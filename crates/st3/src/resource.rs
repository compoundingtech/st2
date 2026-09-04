use std::collections::BTreeSet;
use std::future::Future;
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
                provider => bail!("resource provider `{provider}` is not registered"),
            }
        })
    }
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
    let token = std::env::var("GH_TOKEN")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("GITHUB_TOKEN")
                .ok()
                .filter(|value| !value.is_empty())
        });
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
}
