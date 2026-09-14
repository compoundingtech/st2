use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/github-issue",
    world: "github-issue-provider",
    with: {
        "compoundingtech:st2-github-issue/github-issue@0.1.0": generate,
    },
});

use compoundingtech::st2_github_issue::github_issue;
use exports::st2::resource_provider::provider_api;

const SNAPSHOT_SCHEMA: &str = "dev.schickling.github-issue.snapshot.v1";
const TOPICS: [&str; 5] = ["body", "state", "labels", "assignment", "discussion"];
const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "topics": {
      "type": "array",
      "items": { "type": "string" },
      "uniqueItems": true
    }
  },
  "additionalProperties": false
}"#;

struct Component;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selector {
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Clone)]
struct IssueRef {
    owner: String,
    repo: String,
    number: u64,
}

impl IssueRef {
    fn parse_uri(uri: &str) -> Option<Self> {
        let subject = uri.strip_prefix("github-issue://github.com/")?;
        let mut parts = subject.split('/');
        let owner = parts.next()?;
        let repo = parts.next()?;
        if parts.next()? != "issues" {
            return None;
        }
        let number = parts.next()?;
        if parts.next().is_some()
            || !valid_component(owner, 39)
            || !valid_component(repo, 100)
            || number.is_empty()
            || number.len() > 10
            || number.starts_with('0')
            || !number.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        Some(Self {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            number: number.parse().ok()?,
        })
    }
}

#[derive(Deserialize)]
struct IssueResponse {
    title: String,
    body: Option<String>,
    state: String,
    state_reason: Option<IssueStateReason>,
    user: Option<UserResponse>,
    html_url: String,
    locked: bool,
    labels: Vec<LabelResponse>,
    assignees: Vec<AssigneeResponse>,
    milestone: Option<MilestoneResponse>,
    comments: u64,
    created_at: String,
    updated_at: String,
    closed_at: Option<String>,
    pull_request: Option<Value>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum IssueStateReason {
    Completed,
    Duplicate,
    NotPlanned,
    Reopened,
}

#[derive(Deserialize)]
struct LabelResponse {
    name: String,
}

#[derive(Deserialize)]
struct AssigneeResponse {
    login: String,
}

#[derive(Deserialize)]
struct UserResponse {
    login: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MilestoneResponse {
    number: u64,
    title: String,
    state: String,
    #[serde(alias = "html_url")]
    html_url: String,
    #[serde(alias = "due_on")]
    due_on: Option<String>,
}

#[derive(Deserialize)]
struct CommentMetadata {
    updated_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot<'a> {
    schema: &'static str,
    uri: &'a str,
    observed_at: &'a str,
    repository: RepositorySnapshot<'a>,
    number: u64,
    issue: IssueSnapshot,
    discussion: DiscussionSnapshot,
    facets: Facets,
}

#[derive(Serialize)]
struct RepositorySnapshot<'a> {
    owner: &'a str,
    name: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IssueSnapshot {
    title: String,
    body: Option<String>,
    state: String,
    state_reason: Option<IssueStateReason>,
    author: Option<String>,
    html_url: String,
    locked: bool,
    labels: Vec<String>,
    assignees: Vec<String>,
    milestone: Option<MilestoneResponse>,
    created_at: String,
    updated_at: String,
    closed_at: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiscussionSnapshot {
    comment_count: u64,
    latest_updated_at: Option<String>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Facets {
    open: bool,
    closed: bool,
    assigned: bool,
    has_discussion: bool,
}

impl provider_api::Guest for Component {
    fn describe() -> Result<provider_api::ProviderDescriptor, provider_api::DescriptorError> {
        Ok(provider_api::ProviderDescriptor {
            capabilities: vec![provider_api::SchedulingCapability::Demand],
            selector_schema_json: SELECTOR_SCHEMA.into(),
            default_selector_json: "{}".into(),
            topics: TOPICS.iter().map(|topic| (*topic).to_owned()).collect(),
            snapshot_media_type: "application/json".into(),
            snapshot_schema_id: SNAPSHOT_SCHEMA.into(),
        })
    }

    fn observe(request: provider_api::ObserveRequest) -> provider_api::ObservationResult {
        observe(request)
            .unwrap_or_else(|diagnostic| provider_api::ObservationResult::Failed(Some(diagnostic)))
    }
}

fn observe(
    request: provider_api::ObserveRequest,
) -> Result<provider_api::ObservationResult, String> {
    let selector: Selector = serde_json::from_str(&request.selector_json)
        .map_err(|_| "invalid GitHub issue selector".to_owned())?;
    let reference = IssueRef::parse_uri(&request.uri)
        .ok_or_else(|| "invalid canonical GitHub issue URI".to_owned())?;
    let response = github_issue::get(&github_issue::IssueRequest {
        owner: reference.owner.clone(),
        repo: reference.repo.clone(),
        number: reference.number,
    })
    .map_err(map_source_error)?;
    let observation = match response {
        github_issue::IssueResponse::NotModified => {
            return Ok(provider_api::ObservationResult::Unchanged);
        }
        github_issue::IssueResponse::Ok(observation) => observation,
    };

    let current = build_snapshot(&request.uri, &reference, &observation.current)?;
    let previous = observation
        .previous
        .as_ref()
        .map(|source| build_snapshot(&request.uri, &reference, source))
        .transpose()?;
    if previous
        .as_ref()
        .is_some_and(|previous| same_semantics(previous, &current))
    {
        let prior_digest = request
            .prior_digest
            .as_deref()
            .ok_or_else(|| "GitHub prior source lacked a snapshot digest".to_owned())?;
        github_issue::bind_snapshot(prior_digest).map_err(map_source_error)?;
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let bytes = serde_json::to_vec(&current)
        .map_err(|_| "GitHub issue snapshot normalization failed".to_owned())?;
    let digest = Sha256::digest(&bytes);
    github_issue::bind_snapshot(digest.as_slice()).map_err(map_source_error)?;
    if request.prior_digest.as_deref() == Some(digest.as_slice()) {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let publication_topics = topics(previous.as_ref(), &current);
    let facts = facts(previous.as_ref(), &current, reference.number);
    let _ = (request.demand_watermark, selector.topics);
    Ok(provider_api::ObservationResult::Published(
        provider_api::Publication {
            schema_id: SNAPSHOT_SCHEMA.into(),
            media_type: "application/json".into(),
            bytes,
            topics: publication_topics,
            facts: Some(facts),
        },
    ))
}

fn build_snapshot(
    uri: &str,
    reference: &IssueRef,
    source: &github_issue::SourceSnapshot,
) -> Result<Value, String> {
    let issue: IssueResponse = serde_json::from_slice(&source.issue.body)
        .map_err(|_| "GitHub issue response was invalid".to_owned())?;
    if issue.pull_request.is_some() {
        return Err("GitHub issue response described a pull request".into());
    }
    let latest_updated_at = match (source.latest_comment.as_ref(), issue.comments) {
        (None, 0) => None,
        (None, _) => return Err("GitHub issue comment metadata was missing".into()),
        (Some(_), 0) => return Err("GitHub issue comment metadata was unexpected".into()),
        (Some(comment), _) => {
            let comments: Vec<CommentMetadata> = serde_json::from_slice(&comment.body)
                .map_err(|_| "GitHub issue comment metadata was invalid".to_owned())?;
            match comments.as_slice() {
                [comment] => Some(comment.updated_at.clone()),
                _ => return Err("GitHub issue comment metadata was invalid".into()),
            }
        }
    };
    let mut labels: Vec<_> = issue.labels.into_iter().map(|label| label.name).collect();
    labels.sort();
    let mut assignees: Vec<_> = issue
        .assignees
        .into_iter()
        .map(|assignee| assignee.login)
        .collect();
    assignees.sort();
    let facets = Facets {
        open: issue.state == "open",
        closed: issue.state == "closed",
        assigned: !assignees.is_empty(),
        has_discussion: issue.comments > 0,
    };

    serde_json::to_value(Snapshot {
        schema: SNAPSHOT_SCHEMA,
        uri,
        observed_at: &source.observed_at,
        repository: RepositorySnapshot {
            owner: &reference.owner,
            name: &reference.repo,
        },
        number: reference.number,
        issue: IssueSnapshot {
            title: issue.title,
            body: issue.body,
            state: issue.state,
            state_reason: issue.state_reason,
            html_url: issue.html_url,
            author: issue.user.map(|user| user.login),
            locked: issue.locked,
            labels,
            assignees,
            milestone: issue.milestone,
            created_at: issue.created_at,
            updated_at: issue.updated_at,
            closed_at: issue.closed_at,
        },
        discussion: DiscussionSnapshot {
            comment_count: issue.comments,
            latest_updated_at,
        },
        facets,
    })
    .map_err(|_| "GitHub issue snapshot normalization failed".to_owned())
}

fn same_semantics(before: &Value, after: &Value) -> bool {
    let (Some(before), Some(after)) = (before.as_object(), after.as_object()) else {
        return false;
    };
    let before_len = before
        .keys()
        .filter(|key| key.as_str() != "observedAt")
        .count();
    let after_len = after
        .keys()
        .filter(|key| key.as_str() != "observedAt")
        .count();
    before_len == after_len
        && before
            .iter()
            .filter(|(key, _)| key.as_str() != "observedAt")
            .all(|(key, value)| after.get(key) == Some(value))
}

fn topics(previous: Option<&Value>, current: &Value) -> Vec<String> {
    let Some(previous) = previous else {
        return TOPICS.iter().map(|topic| (*topic).to_owned()).collect();
    };
    [
        ("body", vec![["issue", "title"], ["issue", "body"]]),
        ("state", vec![["issue", "state"], ["issue", "stateReason"]]),
        ("labels", vec![["issue", "labels"]]),
        (
            "assignment",
            vec![["issue", "assignees"], ["issue", "milestone"]],
        ),
        ("discussion", vec![["issue", "locked"], ["discussion", ""]]),
    ]
    .into_iter()
    .filter_map(|(topic, paths)| {
        paths
            .into_iter()
            .any(|path| value_at(previous, path) != value_at(current, path))
            .then(|| topic.to_owned())
    })
    .collect()
}

fn value_at<'a>(value: &'a Value, path: [&str; 2]) -> Option<&'a Value> {
    let value = value.get(path[0])?;
    if path[1].is_empty() {
        Some(value)
    } else {
        value.get(path[1])
    }
}

fn facts(previous: Option<&Value>, current: &Value, number: u64) -> Vec<provider_api::Fact> {
    fn state(snapshot: &Value) -> Option<&str> {
        snapshot
            .get("issue")
            .and_then(|issue| issue.get("state"))
            .and_then(Value::as_str)
    }
    fn comments(snapshot: &Value) -> Option<String> {
        snapshot
            .get("discussion")
            .and_then(|discussion| discussion.get("commentCount"))
            .and_then(Value::as_u64)
            .map(|count| count.to_string())
    }
    let current_state = state(current).unwrap_or("unknown");
    let current_comments = comments(current).unwrap_or_else(|| "unknown".to_owned());
    let mut facts = vec![current_fact("issue", format!("#{number}"))];
    match previous {
        Some(previous) => {
            facts.push(transition_fact("state", state(previous), current_state));
            facts.push(transition_fact_owned(
                "comments",
                comments(previous),
                current_comments,
            ));
        }
        None => {
            facts.push(current_fact("state", current_state));
            facts.push(current_fact("comments", current_comments));
        }
    }
    facts
}

fn current_fact(key: &str, value: impl Into<String>) -> provider_api::Fact {
    provider_api::Fact {
        key: key.into(),
        before: provider_api::FactValue::Omitted,
        after: provider_api::FactValue::Value(value.into()),
    }
}

fn transition_fact(key: &str, before: Option<&str>, after: &str) -> provider_api::Fact {
    provider_api::Fact {
        key: key.into(),
        before: before.map_or(provider_api::FactValue::Null, |value| {
            provider_api::FactValue::Value(value.into())
        }),
        after: provider_api::FactValue::Value(after.into()),
    }
}

fn transition_fact_owned(key: &str, before: Option<String>, after: String) -> provider_api::Fact {
    provider_api::Fact {
        key: key.into(),
        before: before.map_or(
            provider_api::FactValue::Null,
            provider_api::FactValue::Value,
        ),
        after: provider_api::FactValue::Value(after),
    }
}

fn valid_component(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn map_source_error(error: github_issue::IssueError) -> String {
    match error {
        github_issue::IssueError::Denied => "GitHub issue scope denied",
        github_issue::IssueError::Unavailable => "GitHub is unavailable",
        github_issue::IssueError::ResourceExhausted => "GitHub response exceeded limits",
        github_issue::IssueError::DeadlineExceeded => "GitHub request deadline exceeded",
    }
    .into()
}

export!(Component);
