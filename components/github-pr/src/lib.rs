use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/github-pr",
    world: "github-pr-provider",
    with: {
        "compoundingtech:st2-github-pr/github-pr@0.1.0": generate,
    },
});

use compoundingtech::st2_github_pr::github_pr;
use exports::st2::resource_provider::provider_api;

const SNAPSHOT_SCHEMA: &str = "dev.schickling.github-pr.snapshot.v1";
const TOPICS: [&str; 4] = [
    "ci.failure",
    "mergeability.conflict",
    "review.requested",
    "terminal",
];
const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "owner": { "type": "string" },
    "repo": { "type": "string" },
    "number": { "type": "integer" },
    "topics": {
      "type": "array",
      "items": { "type": "string" },
      "uniqueItems": true
    }
  },
  "required": ["owner", "repo", "number"],
  "additionalProperties": false
}"#;

struct Component;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Selector {
    owner: String,
    repo: String,
    number: u64,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Deserialize)]
struct PullRequestResponse {
    number: u64,
    #[serde(default = "null")]
    url: Value,
    #[serde(default = "null")]
    html_url: Value,
    #[serde(default = "null")]
    state: Value,
    #[serde(default)]
    draft: Option<bool>,
    #[serde(default)]
    merged: Option<bool>,
    #[serde(default = "null")]
    merged_at: Value,
    #[serde(default = "null")]
    closed_at: Value,
    #[serde(default)]
    mergeable: Option<bool>,
    #[serde(default = "null")]
    mergeable_state: Value,
    head: PullRequestHeadResponse,
    base: PullRequestBaseResponse,
    #[serde(default)]
    requested_reviewers: Option<Vec<RequestedReviewerResponse>>,
    #[serde(default)]
    requested_teams: Option<Vec<RequestedTeamResponse>>,
}

#[derive(Deserialize)]
struct PullRequestHeadResponse {
    sha: String,
    #[serde(default = "null")]
    r#ref: Value,
}

#[derive(Deserialize)]
struct PullRequestBaseResponse {
    #[serde(default = "null")]
    r#ref: Value,
}

#[derive(Deserialize)]
struct RequestedReviewerResponse {
    login: String,
}

#[derive(Deserialize)]
struct RequestedTeamResponse {
    slug: String,
}

#[derive(Deserialize)]
struct CheckRunsResponse {
    #[serde(default)]
    check_runs: Option<Vec<CheckRunResponse>>,
}

#[derive(Deserialize)]
struct CheckRunResponse {
    name: String,
    #[serde(default = "null")]
    status: Value,
    #[serde(default = "null")]
    conclusion: Value,
    #[serde(default = "null")]
    details_url: Value,
}

#[derive(Deserialize)]
struct CombinedStatusResponse {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    statuses: Option<Vec<StatusResponse>>,
}

#[derive(Deserialize)]
struct StatusResponse {
    context: String,
    state: String,
    #[serde(default = "null")]
    target_url: Value,
    #[serde(default = "null")]
    description: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot<'a> {
    schema: &'static str,
    uri: &'a str,
    observed_at: &'a str,
    repository: RepositorySnapshot<'a>,
    number: u64,
    pull_request: PullRequestSnapshot,
    ci: CiSnapshot,
    facets: Facets,
}

#[derive(Serialize)]
struct RepositorySnapshot<'a> {
    owner: &'a str,
    name: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PullRequestSnapshot {
    api_url: Value,
    html_url: Value,
    state: Value,
    draft: bool,
    merged: bool,
    merged_at: Value,
    closed_at: Value,
    mergeable: Option<bool>,
    mergeable_state: Value,
    head: HeadSnapshot,
    base: BaseSnapshot,
    requested_reviewers: Vec<String>,
    requested_teams: Vec<String>,
}

#[derive(Serialize)]
struct HeadSnapshot {
    sha: String,
    r#ref: Value,
}

#[derive(Serialize)]
struct BaseSnapshot {
    r#ref: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CiSnapshot {
    state: String,
    check_runs: Vec<CheckRunSnapshot>,
    statuses: Vec<StatusSnapshot>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckRunSnapshot {
    name: String,
    status: Value,
    conclusion: Value,
    details_url: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusSnapshot {
    context: String,
    state: String,
    target_url: Value,
    description: Value,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Facets {
    review_requested: bool,
    ci_failure: bool,
    merge_conflict: bool,
    terminal: bool,
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
        observe(request).unwrap_or_else(|diagnostic| {
            provider_api::ObservationResult::Failed(Some(diagnostic))
        })
    }
}

fn observe(
    request: provider_api::ObserveRequest,
) -> Result<provider_api::ObservationResult, String> {
    let selector: Selector = serde_json::from_str(&request.selector_json)
        .map_err(|_| "invalid GitHub pull request selector".to_owned())?;
    if selector.owner.is_empty() || selector.repo.is_empty() || selector.number == 0 {
        return Err("GitHub pull request selector fields must be non-empty".into());
    }
    let expected_uri = format!(
        "github-pr://{}/{}/{}",
        selector.owner, selector.repo, selector.number
    );
    if request.uri != expected_uri {
        return Err("GitHub pull request URI did not match the selector".into());
    }
    let response = github_pr::get(&github_pr::PullRequestRequest {
        owner: selector.owner.clone(),
        repo: selector.repo.clone(),
        number: selector.number,
    })
    .map_err(map_source_error)?;
    let observation = match response {
        github_pr::PullRequestResponse::NotModified => {
            return Ok(provider_api::ObservationResult::Unchanged);
        }
        github_pr::PullRequestResponse::Ok(observation) => observation,
    };

    let current = build_snapshot(
        &request.uri,
        &selector,
        &observation.current,
    )?;
    let previous = observation
        .previous
        .as_ref()
        .map(|source| build_snapshot(&request.uri, &selector, source))
        .transpose()?;
    if previous
        .as_ref()
        .is_some_and(|previous| same_semantics(previous, &current))
    {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let bytes = serde_json::to_vec(&current)
        .map_err(|_| "GitHub pull request snapshot normalization failed".to_owned())?;
    let digest = Sha256::digest(&bytes);
    if request.prior_digest.as_deref() == Some(digest.as_slice()) {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let publication_topics = topics(previous.as_ref(), &current);
    let facts = facet_facts(previous.as_ref(), &current);
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
    selector: &Selector,
    source: &github_pr::SourceSnapshot,
) -> Result<Value, String> {
    let pull: PullRequestResponse = serde_json::from_slice(&source.pull_request.body)
        .map_err(|_| "GitHub pull request response was invalid".to_owned())?;
    if pull.number != selector.number || !valid_head_sha(&pull.head.sha) {
        return Err("GitHub response did not match the requested pull request".into());
    }
    let checks: CheckRunsResponse = serde_json::from_slice(&source.check_runs.body)
        .map_err(|_| "GitHub check runs response was invalid".to_owned())?;
    let status: CombinedStatusResponse = serde_json::from_slice(&source.combined_status.body)
        .map_err(|_| "GitHub combined status response was invalid".to_owned())?;

    let mut requested_reviewers: Vec<_> = pull
        .requested_reviewers
        .unwrap_or_default()
        .into_iter()
        .map(|reviewer| reviewer.login)
        .collect();
    requested_reviewers.sort();
    let mut requested_teams: Vec<_> = pull
        .requested_teams
        .unwrap_or_default()
        .into_iter()
        .map(|team| team.slug)
        .collect();
    requested_teams.sort();
    let mut check_runs: Vec<_> = checks
        .check_runs
        .unwrap_or_default()
        .into_iter()
        .map(|check| CheckRunSnapshot {
            name: check.name,
            status: check.status,
            conclusion: check.conclusion,
            details_url: check.details_url,
        })
        .collect();
    check_runs.sort_by(|left, right| left.name.cmp(&right.name));
    let mut statuses: Vec<_> = status
        .statuses
        .unwrap_or_default()
        .into_iter()
        .map(|status| StatusSnapshot {
            context: status.context,
            state: status.state,
            target_url: status.target_url,
            description: status.description,
        })
        .collect();
    statuses.sort_by(|left, right| left.context.cmp(&right.context));

    let check_failure = check_runs.iter().any(|check| {
        check.status.as_str() == Some("completed")
            && matches!(
                check.conclusion.as_str(),
                Some(
                    "failure"
                        | "timed_out"
                        | "cancelled"
                        | "action_required"
                        | "startup_failure"
                        | "stale"
                )
            )
    });
    let combined_state = status.state.unwrap_or_else(|| "pending".to_owned());
    let facets = Facets {
        review_requested: !requested_reviewers.is_empty() || !requested_teams.is_empty(),
        ci_failure: check_failure || matches!(combined_state.as_str(), "failure" | "error"),
        merge_conflict: pull.mergeable == Some(false)
            || pull.mergeable_state.as_str() == Some("dirty"),
        terminal: pull.merged == Some(true) || pull.state.as_str() == Some("closed"),
    };

    serde_json::to_value(Snapshot {
        schema: SNAPSHOT_SCHEMA,
        uri,
        observed_at: &source.observed_at,
        repository: RepositorySnapshot {
            owner: &selector.owner,
            name: &selector.repo,
        },
        number: selector.number,
        pull_request: PullRequestSnapshot {
            api_url: pull.url,
            html_url: pull.html_url,
            state: pull.state,
            draft: pull.draft.unwrap_or(false),
            merged: pull.merged.unwrap_or(false),
            merged_at: pull.merged_at,
            closed_at: pull.closed_at,
            mergeable: pull.mergeable,
            mergeable_state: pull.mergeable_state,
            head: HeadSnapshot {
                sha: pull.head.sha,
                r#ref: pull.head.r#ref,
            },
            base: BaseSnapshot {
                r#ref: pull.base.r#ref,
            },
            requested_reviewers,
            requested_teams,
        },
        ci: CiSnapshot {
            state: combined_state,
            check_runs,
            statuses,
        },
        facets,
    })
    .map_err(|_| "GitHub pull request snapshot normalization failed".to_owned())
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
        ("ciFailure", "ci.failure"),
        ("mergeConflict", "mergeability.conflict"),
        ("reviewRequested", "review.requested"),
        ("terminal", "terminal"),
    ]
    .into_iter()
    .filter_map(|(facet, topic)| {
        (facet_value(previous, facet) != facet_value(current, facet)).then(|| topic.to_owned())
    })
    .collect()
}

fn facet_facts(previous: Option<&Value>, current: &Value) -> Vec<provider_api::Fact> {
    [
        ("facets.ciFailure", "ciFailure"),
        ("facets.mergeConflict", "mergeConflict"),
        ("facets.reviewRequested", "reviewRequested"),
        ("facets.terminal", "terminal"),
    ]
    .into_iter()
    .filter_map(|(key, facet)| {
        let before = previous.and_then(|value| facet_value(value, facet));
        let after = facet_value(current, facet);
        (before != after).then(|| provider_api::Fact {
            key: key.into(),
            before: before.map_or(provider_api::FactValue::Omitted, |value| {
                provider_api::FactValue::Value(value.to_string())
            }),
            after: after.map_or(provider_api::FactValue::Null, |value| {
                provider_api::FactValue::Value(value.to_string())
            }),
        })
    })
    .collect()
}

fn facet_value(snapshot: &Value, facet: &str) -> Option<bool> {
    snapshot
        .get("facets")
        .and_then(|facets| facets.get(facet))
        .and_then(Value::as_bool)
}

fn valid_head_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn null() -> Value {
    Value::Null
}

fn map_source_error(error: github_pr::PullRequestError) -> String {
    match error {
        github_pr::PullRequestError::Denied => "GitHub pull request scope denied",
        github_pr::PullRequestError::Unavailable => "GitHub is unavailable",
        github_pr::PullRequestError::ResourceExhausted => "GitHub response exceeded limits",
        github_pr::PullRequestError::DeadlineExceeded => "GitHub request deadline exceeded",
    }
    .into()
}

export!(Component);
