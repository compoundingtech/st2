use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

#[derive(Clone, Debug)]
pub struct ObservationRequest {
    pub provider: String,
    pub locator: String,
    pub fields: BTreeSet<String>,
    pub cursor: Option<String>,
    pub previous_facts: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderObservation {
    pub facts: Value,
    pub cursor: Option<String>,
    pub next_check_unix_ms: u128,
}

pub trait ResourceProvider: Send + Sync + 'static {
    fn observe(
        &self,
        request: ObservationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderObservation>> + Send + '_>>;
}

#[derive(Clone, Default)]
pub struct RegisteredResourceProvider;

impl ResourceProvider for RegisteredResourceProvider {
    fn observe(
        &self,
        request: ObservationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderObservation>> + Send + '_>> {
        Box::pin(async move {
            match request.provider.as_str() {
                "github.pull-request" => observe_github_pull_request(request).await,
                "github.repository" => observe_github_repository(request).await,
                "local.file" => observe_local_file(request),
                provider => bail!("resource provider `{provider}` is not registered"),
            }
        })
    }
}

async fn observe_github_repository(request: ObservationRequest) -> Result<ProviderObservation> {
    let (owner, repository) = request
        .locator
        .split_once('/')
        .context("a GitHub repository locator needs OWNER/REPO")?;
    anyhow::ensure!(
        !owner.is_empty() && !repository.is_empty() && !repository.contains('/'),
        "a GitHub repository locator needs OWNER/REPO"
    );
    let client = reqwest::Client::builder()
        .user_agent("st3-resource-observer/0.1")
        .build()?;
    let token = github_token().await;
    let request_json = |url: String| {
        let request = client
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        if let Some(token) = &token {
            request.bearer_auth(token)
        } else {
            request
        }
    };
    let base = format!("https://api.github.com/repos/{owner}/{repository}");
    let pulls: Vec<Value> = request_json(format!("{base}/pulls?state=open&per_page=100"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let issues: Vec<Value> = request_json(format!("{base}/issues?state=open&per_page=100"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let facts = normalize_github_repository(
        request.previous_facts.as_ref(),
        &pulls,
        &issues,
        &request.fields,
    );
    let cursor = Some(hex::encode(Sha256::digest(serde_json::to_vec(&facts)?)));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Ok(ProviderObservation {
        facts,
        cursor,
        next_check_unix_ms: now.saturating_add(60_000),
    })
}

async fn github_token() -> Option<String> {
    if let Some(token) = std::env::var("GH_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("GITHUB_TOKEN")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
    {
        return Some(token);
    }
    let output = tokio::process::Command::new("gh")
        .args(["auth", "token"])
        .kill_on_drop(true)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

fn normalize_github_repository(
    previous: Option<&Value>,
    pulls: &[Value],
    issues: &[Value],
    fields: &BTreeSet<String>,
) -> Value {
    let mut facts = serde_json::Map::new();
    if fields.contains("pull_requests") {
        let mut values = previous
            .and_then(|value| value.get("pull_requests"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for pull in pulls
            .iter()
            .filter(|pull| pull.get("draft").and_then(Value::as_bool) == Some(false))
        {
            let value = json!({
                "number": pull.get("number").cloned().unwrap_or(Value::Null),
                "url": pull.get("html_url").cloned().unwrap_or(Value::Null),
                "title": pull.get("title").cloned().unwrap_or(Value::Null),
                "head": pull.pointer("/head/sha").cloned().unwrap_or(Value::Null),
            });
            let number = value.get("number");
            if !values.iter().any(|old| old.get("number") == number) {
                values.push(value);
            }
        }
        values.sort_by_key(|value| value.get("number").and_then(Value::as_u64));
        facts.insert("pull_requests".into(), Value::Array(values));
    }
    if fields.contains("issues") {
        let mut values = previous
            .and_then(|value| value.get("issues"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for issue in issues
            .iter()
            .filter(|issue| issue.get("pull_request").is_none())
        {
            let value = json!({
                "number": issue.get("number").cloned().unwrap_or(Value::Null),
                "url": issue.get("html_url").cloned().unwrap_or(Value::Null),
                "title": issue.get("title").cloned().unwrap_or(Value::Null),
            });
            let number = value.get("number");
            if !values.iter().any(|old| old.get("number") == number) {
                values.push(value);
            }
        }
        values.sort_by_key(|value| value.get("number").and_then(Value::as_u64));
        facts.insert("issues".into(), Value::Array(values));
    }
    Value::Object(facts)
}

fn observe_local_file(request: ObservationRequest) -> Result<ProviderObservation> {
    let path = Path::new(&request.locator);
    anyhow::ensure!(
        path.is_absolute(),
        "a local file locator must be an absolute path"
    );
    let mut facts = serde_json::Map::new();
    facts.insert("path".into(), Value::String(request.locator.clone()));
    match std::fs::read(path) {
        Ok(bytes) => {
            facts.insert("status".into(), Value::String("ready".into()));
            facts.insert(
                "content_hash".into(),
                Value::String(hex::encode(Sha256::digest(&bytes))),
            );
            facts.insert("size".into(), Value::from(bytes.len() as u64));
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let mode = std::fs::metadata(path)?.permissions().mode() & 0o7777;
                facts.insert("mode".into(), Value::from(mode));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            facts.insert("status".into(), Value::String("missing".into()));
        }
        Err(error) => {
            facts.insert("status".into(), Value::String("unreadable".into()));
            facts.insert("reason".into(), Value::String(error.to_string()));
        }
    }
    facts.retain(|name, _| request.fields.contains(name) || name == "status" || name == "path");
    let facts = Value::Object(facts);
    let cursor = Some(hex::encode(Sha256::digest(serde_json::to_vec(&facts)?)));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Ok(ProviderObservation {
        facts,
        cursor,
        next_check_unix_ms: now.saturating_add(60_000),
    })
}

async fn observe_github_pull_request(request: ObservationRequest) -> Result<ProviderObservation> {
    let (repository, number) = request
        .locator
        .rsplit_once('#')
        .context("a GitHub pull request locator needs OWNER/REPO#NUMBER")?;
    let (owner, repository) = repository
        .split_once('/')
        .context("a GitHub pull request locator needs OWNER/REPO#NUMBER")?;
    let number = number
        .parse::<u64>()
        .context("a GitHub pull request number must be an integer")?;
    let client = reqwest::Client::builder()
        .user_agent("st3-resource-observer/0.1")
        .build()?;
    let token = github_token().await;
    let request_json = |url: String| {
        let request = client
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        if let Some(token) = &token {
            request.bearer_auth(token)
        } else {
            request
        }
    };
    let base = format!("https://api.github.com/repos/{owner}/{repository}");
    let pull: Value = request_json(format!("{base}/pulls/{number}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut facts = serde_json::Map::new();
    if request.fields.contains("head") {
        facts.insert(
            "head".into(),
            pull.pointer("/head/sha").cloned().unwrap_or(Value::Null),
        );
    }
    if request.fields.contains("state") {
        facts.insert(
            "state".into(),
            json!({
                "state": pull.get("state").cloned().unwrap_or(Value::Null),
                "draft": pull.get("draft").cloned().unwrap_or(Value::Null),
                "merged": pull.get("merged").cloned().unwrap_or(Value::Null),
            }),
        );
    }
    if request.fields.contains("review") {
        let reviews: Vec<Value> =
            request_json(format!("{base}/pulls/{number}/reviews?per_page=100"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
        let normalized = reviews
            .into_iter()
            .map(|review| {
                json!({
                    "id": review.get("id").cloned().unwrap_or(Value::Null),
                    "user": review.pointer("/user/login").cloned().unwrap_or(Value::Null),
                    "state": review.get("state").cloned().unwrap_or(Value::Null),
                    "submitted_at": review.get("submitted_at").cloned().unwrap_or(Value::Null),
                    "commit_id": review.get("commit_id").cloned().unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();
        facts.insert("review".into(), Value::Array(normalized));
    }
    if request.fields.contains("checks") {
        let head = pull
            .pointer("/head/sha")
            .and_then(Value::as_str)
            .context("the GitHub pull request has no head SHA")?;
        let checks: Value = request_json(format!("{base}/commits/{head}/check-runs?per_page=100"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let normalized = checks
            .get("check_runs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|check| {
                json!({
                    "id": check.get("id").cloned().unwrap_or(Value::Null),
                    "name": check.get("name").cloned().unwrap_or(Value::Null),
                    "status": check.get("status").cloned().unwrap_or(Value::Null),
                    "conclusion": check.get("conclusion").cloned().unwrap_or(Value::Null),
                    "completed_at": check.get("completed_at").cloned().unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();
        facts.insert("checks".into(), Value::Array(normalized));
    }
    let facts = Value::Object(facts);
    let cursor = Some(hex::encode(Sha256::digest(serde_json::to_vec(&facts)?)));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Ok(ProviderObservation {
        facts,
        cursor,
        next_check_unix_ms: now.saturating_add(60_000),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeProvider;

    impl ResourceProvider for FakeProvider {
        fn observe(
            &self,
            request: ObservationRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderObservation>> + Send + '_>> {
            Box::pin(async move {
                Ok(ProviderObservation {
                    facts: json!({"provider": request.provider, "locator": request.locator}),
                    cursor: Some("fake-cursor".into()),
                    next_check_unix_ms: 42,
                })
            })
        }
    }

    #[tokio::test]
    async fn a_second_provider_uses_the_generic_contract() {
        let observation = FakeProvider
            .observe(ObservationRequest {
                provider: "fake.issue".into(),
                locator: "project/42".into(),
                fields: BTreeSet::from(["state".into()]),
                cursor: None,
                previous_facts: None,
            })
            .await
            .unwrap();
        assert_eq!(observation.cursor.as_deref(), Some("fake-cursor"));
        assert_eq!(observation.facts["provider"], "fake.issue");
    }

    #[tokio::test]
    async fn a_local_file_observer_records_metadata_without_content() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("proof.txt");
        std::fs::write(&path, "secret proof\n").unwrap();
        let observation = RegisteredResourceProvider
            .observe(ObservationRequest {
                provider: "local.file".into(),
                locator: path.display().to_string(),
                fields: BTreeSet::from(["content_hash".into(), "size".into(), "status".into()]),
                cursor: None,
                previous_facts: None,
            })
            .await
            .unwrap();
        assert_eq!(observation.facts["status"], "ready");
        assert_eq!(observation.facts["size"], 13);
        assert!(observation.facts.get("content_hash").is_some());
        assert!(observation.facts.get("content").is_none());
        assert_eq!(observation.facts["path"], path.display().to_string());
    }

    #[tokio::test]
    async fn a_missing_local_file_is_a_distinct_observation() {
        let path = std::env::temp_dir().join("st3-file-that-does-not-exist");
        let observation = RegisteredResourceProvider
            .observe(ObservationRequest {
                provider: "local.file".into(),
                locator: path.display().to_string(),
                fields: BTreeSet::from(["status".into()]),
                cursor: None,
                previous_facts: None,
            })
            .await
            .unwrap();
        assert_eq!(observation.facts["status"], "missing");
    }

    #[test]
    fn repository_discovery_filters_drafts_and_pull_requests_from_issues() {
        let fields = BTreeSet::from(["pull_requests".into(), "issues".into()]);
        let facts = normalize_github_repository(
            None,
            &[
                json!({"number": 1, "draft": true, "title": "draft"}),
                json!({"number": 2, "draft": false, "title": "ready", "head": {"sha": "abc"}}),
            ],
            &[
                json!({"number": 2, "title": "PR", "pull_request": {}}),
                json!({"number": 3, "title": "Issue"}),
            ],
            &fields,
        );
        assert_eq!(facts["pull_requests"].as_array().unwrap().len(), 1);
        assert_eq!(facts["pull_requests"][0]["number"], 2);
        assert_eq!(facts["issues"].as_array().unwrap().len(), 1);
        assert_eq!(facts["issues"][0]["number"], 3);
    }

    #[test]
    fn repository_discovery_retains_old_items_and_adds_a_ready_draft_once() {
        let fields = BTreeSet::from(["pull_requests".into()]);
        let previous = json!({"pull_requests": [{"number": 1, "title": "old"}]});
        let facts = normalize_github_repository(
            Some(&previous),
            &[json!({"number": 2, "draft": false, "title": "now ready"})],
            &[],
            &fields,
        );
        assert_eq!(facts["pull_requests"].as_array().unwrap().len(), 2);
        let repeated = normalize_github_repository(Some(&facts), &[], &[], &fields);
        assert_eq!(repeated, facts);
    }
}
