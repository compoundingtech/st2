use serde::{Deserialize, Serialize};
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

const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "owner": { "type": "string" },
    "repo": { "type": "string" },
    "number": { "type": "integer" },
    "etag": { "type": "string" },
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
    etag: Option<String>,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Deserialize)]
struct GitHubIssue {
    number: u64,
    state: String,
    title: String,
    updated_at: String,
    html_url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Carrier<'a> {
    resource: &'static str,
    owner: &'a str,
    repo: &'a str,
    number: u64,
    state: &'a str,
    title: &'a str,
    updated_at: &'a str,
    html_url: &'a str,
}

impl provider_api::Guest for Component {
    fn describe() -> Result<provider_api::ProviderDescriptor, provider_api::DescriptorError> {
        Ok(provider_api::ProviderDescriptor {
            capabilities: vec![provider_api::SchedulingCapability::Demand],
            selector_schema_json: SELECTOR_SCHEMA.into(),
            default_selector_json: "{}".into(),
            topics: vec!["issue".into()],
            snapshot_media_type: "application/json".into(),
            snapshot_schema_id: "st2.resource.github-issue.v1".into(),
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
        .map_err(|_| "invalid GitHub issue selector".to_owned())?;
    if selector.owner.is_empty() || selector.repo.is_empty() || selector.number == 0 {
        return Err("GitHub issue selector fields must be non-empty".into());
    }
    let response = github_issue::get(&github_issue::IssueRequest {
        owner: selector.owner.clone(),
        repo: selector.repo.clone(),
        number: selector.number,
        etag: selector.etag,
    })
    .map_err(map_source_error)?;
    let (etag, body) = match response {
        github_issue::IssueResponse::NotModified(_) => {
            return Ok(provider_api::ObservationResult::Unchanged);
        }
        github_issue::IssueResponse::Ok(value) => value,
    };
    let issue: GitHubIssue = serde_json::from_slice(&body)
        .map_err(|_| "GitHub response was invalid".to_owned())?;
    if issue.number != selector.number {
        return Err("GitHub response did not match the requested issue".into());
    }
    let bytes = serde_json::to_vec(&Carrier {
        resource: "github-issue",
        owner: &selector.owner,
        repo: &selector.repo,
        number: issue.number,
        state: &issue.state,
        title: &issue.title,
        updated_at: &issue.updated_at,
        html_url: &issue.html_url,
    })
    .map_err(|_| "GitHub response normalization failed".to_owned())?;
    let digest = Sha256::digest(&bytes);
    if request.prior_digest.as_deref() == Some(digest.as_slice()) {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let _ = (request.uri, request.demand_watermark, selector.topics);
    let facts = vec![
        provider_api::Fact {
            key: "state".into(),
            before: provider_api::FactValue::Omitted,
            after: provider_api::FactValue::Value(issue.state),
        },
        provider_api::Fact {
            key: "etag".into(),
            before: provider_api::FactValue::Omitted,
            after: etag.map_or(provider_api::FactValue::Null, provider_api::FactValue::Value),
        },
    ];
    Ok(provider_api::ObservationResult::Published(
        provider_api::Publication {
            schema_id: "st2.resource.github-issue.v1".into(),
            media_type: "application/json".into(),
            bytes,
            topics: vec!["issue".into()],
            facts: Some(facts),
        },
    ))
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
