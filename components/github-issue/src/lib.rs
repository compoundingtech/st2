use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/github-issue",
    world: "github-issue-observer",
});

use compoundingtech::st2_github_issue::github_issue;
use exports::compoundingtech::st2_resource_observer::observation;

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

impl observation::Guest for Component {
    fn observe(request: observation::Request) -> Result<observation::Proposal, observation::ObservationError> {
        let selector: Selector = serde_json::from_str(&request.selector_json)
            .map_err(|_| observation::ObservationError::InvalidRequest("invalid GitHub issue selector".into()))?;
        if selector.owner.is_empty() || selector.repo.is_empty() || selector.number == 0 {
            return Err(observation::ObservationError::InvalidRequest(
                "GitHub issue selector fields must be non-empty".into(),
            ));
        }
        let response = github_issue::get(&github_issue::IssueRequest {
            owner: selector.owner.clone(),
            repo: selector.repo.clone(),
            number: selector.number,
            etag: selector.etag,
        })
        .map_err(map_source_error)?;
        let (etag, body) = match response {
            github_issue::IssueResponse::NotModified(_) => return Ok(observation::Proposal::Unchanged),
            github_issue::IssueResponse::Ok(value) => value,
        };
        let issue: GitHubIssue = serde_json::from_slice(&body)
            .map_err(|_| observation::ObservationError::Unavailable("GitHub response was invalid".into()))?;
        if issue.number != selector.number {
            return Err(observation::ObservationError::Unavailable(
                "GitHub response did not match the requested issue".into(),
            ));
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
        .map_err(|_| observation::ObservationError::Unavailable("GitHub response normalization failed".into()))?;
        let digest = Sha256::digest(&bytes);
        if request.previous_digest.as_deref() == Some(digest.as_slice()) {
            return Ok(observation::Proposal::Unchanged);
        }
        let _ = selector.topics;
        let facts = vec![
            observation::Fact {
                key: "state".into(),
                before: observation::FactValue::Omitted,
                after: observation::FactValue::Value(issue.state),
            },
            observation::Fact {
                key: "etag".into(),
                before: observation::FactValue::Omitted,
                after: etag.map_or(observation::FactValue::Null, observation::FactValue::Value),
            },
        ];
        Ok(observation::Proposal::Published(observation::Publication {
            schema_id: "st2.resource.github-issue.v1".into(),
            media_type: "application/json".into(),
            bytes,
            topics: vec!["issue".into()],
            facts,
        }))
    }
}

fn map_source_error(error: github_issue::IssueError) -> observation::ObservationError {
    match error {
        github_issue::IssueError::Denied => {
            observation::ObservationError::InvalidRequest("GitHub issue scope denied".into())
        }
        github_issue::IssueError::Unavailable => {
            observation::ObservationError::Unavailable("GitHub is unavailable".into())
        }
        github_issue::IssueError::ResourceExhausted => {
            observation::ObservationError::Unavailable("GitHub response exceeded limits".into())
        }
        github_issue::IssueError::DeadlineExceeded => {
            observation::ObservationError::Unavailable("GitHub request deadline exceeded".into())
        }
    }
}

export!(Component);
