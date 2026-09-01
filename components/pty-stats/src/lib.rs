use serde::Serialize;
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/pty-stats",
    world: "pty-stats-observer",
});

use compoundingtech::st2_pty_stats::pty_stats;
use exports::compoundingtech::st2_resource_observer::observation;

struct Component;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Selector {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Carrier<'a> {
    resource: &'static str,
    scope: Scope<'a>,
    stats: &'a serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum Scope<'a> {
    All,
    Session(&'a str),
}

impl observation::Guest for Component {
    fn observe(request: observation::Request) -> Result<observation::Proposal, observation::ObservationError> {
        let selector: Selector = serde_json::from_str(&request.selector_json)
            .map_err(|_| observation::ObservationError::InvalidRequest("invalid PTY stats selector".into()))?;
        if selector.session.as_deref().is_some_and(str::is_empty) {
            return Err(observation::ObservationError::InvalidRequest(
                "PTY session scope must be non-empty".into(),
            ));
        }
        let scope = selector.session.as_ref().map_or(pty_stats::Scope::All, |session| {
            pty_stats::Scope::Session(session.clone())
        });
        let outcome = pty_stats::get(&scope).map_err(map_source_error)?;
        if outcome.stdout_truncated || outcome.stderr_truncated {
            return Err(observation::ObservationError::Unavailable(
                "PTY stats output exceeded limits".into(),
            ));
        }
        match outcome.exit {
            pty_stats::ExitStatus::Code(0) => {}
            pty_stats::ExitStatus::Code(_) | pty_stats::ExitStatus::Signal(_) => {
                return Ok(observation::Proposal::Failed(Some(
                    "pty stats exited unsuccessfully".into(),
                )));
            }
        }
        let stats: serde_json::Value = serde_json::from_slice(&outcome.stdout)
            .map_err(|_| observation::ObservationError::Unavailable("pty stats returned invalid JSON".into()))?;
        let carrier_scope = selector
            .session
            .as_deref()
            .map_or(Scope::All, Scope::Session);
        let scope_fact = selector.session.clone().unwrap_or_else(|| "all".into());
        let bytes = serde_json::to_vec(&Carrier {
            resource: "pty-stats",
            scope: carrier_scope,
            stats: &stats,
        })
        .map_err(|_| observation::ObservationError::Unavailable("PTY stats normalization failed".into()))?;
        let digest = Sha256::digest(&bytes);
        if request.previous_digest.as_deref() == Some(digest.as_slice()) {
            return Ok(observation::Proposal::Unchanged);
        }
        let _ = selector.topics;
        Ok(observation::Proposal::Published(observation::Publication {
            schema_id: "st2.resource.pty-stats.v1".into(),
            media_type: "application/json".into(),
            bytes,
            topics: vec!["stats".into()],
            facts: vec![observation::Fact {
                key: "scope".into(),
                before: observation::FactValue::Omitted,
                after: observation::FactValue::Value(scope_fact),
            }],
        }))
    }
}

fn map_source_error(error: pty_stats::PtyStatsError) -> observation::ObservationError {
    match error {
        pty_stats::PtyStatsError::Denied => {
            observation::ObservationError::InvalidRequest("PTY stats scope denied".into())
        }
        pty_stats::PtyStatsError::Unavailable => {
            observation::ObservationError::Unavailable("PTY stats is unavailable".into())
        }
        pty_stats::PtyStatsError::ResourceExhausted => {
            observation::ObservationError::Unavailable("PTY stats output exceeded limits".into())
        }
        pty_stats::PtyStatsError::DeadlineExceeded => {
            observation::ObservationError::Unavailable("PTY stats deadline exceeded".into())
        }
        pty_stats::PtyStatsError::Cancelled => {
            observation::ObservationError::Unavailable("PTY stats was cancelled".into())
        }
    }
}

export!(Component);
