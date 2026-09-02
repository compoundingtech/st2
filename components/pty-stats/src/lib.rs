use serde::Serialize;
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/pty-stats",
    world: "pty-stats-provider",
    with: {
        "compoundingtech:st2-pty-stats/pty-stats@0.1.0": generate,
    },
});

use compoundingtech::st2_pty_stats::pty_stats;
use exports::st2::resource_provider::provider_api;

const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "session": { "type": "string" },
    "topics": {
      "type": "array",
      "items": { "type": "string" },
      "uniqueItems": true
    }
  },
  "additionalProperties": false
}"#;

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

impl provider_api::Guest for Component {
    fn describe() -> Result<provider_api::ProviderDescriptor, provider_api::DescriptorError> {
        Ok(provider_api::ProviderDescriptor {
            capabilities: vec![provider_api::SchedulingCapability::Demand],
            selector_schema_json: SELECTOR_SCHEMA.into(),
            default_selector_json: "{}".into(),
            topics: vec!["stats".into()],
            snapshot_media_type: "application/json".into(),
            snapshot_schema_id: "st2.resource.pty-stats.v1".into(),
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
        .map_err(|_| "invalid PTY stats selector".to_owned())?;
    if selector.session.as_deref().is_some_and(str::is_empty) {
        return Err("PTY session scope must be non-empty".into());
    }
    let scope = selector.session.as_ref().map_or(pty_stats::Scope::All, |session| {
        pty_stats::Scope::Session(session.clone())
    });
    let outcome = pty_stats::get(&scope).map_err(map_source_error)?;
    if outcome.stdout_truncated || outcome.stderr_truncated {
        return Err("PTY stats output exceeded limits".into());
    }
    match outcome.exit {
        pty_stats::ExitStatus::Code(0) => {}
        pty_stats::ExitStatus::Code(_) | pty_stats::ExitStatus::Signal(_) => {
            return Ok(provider_api::ObservationResult::Failed(Some(
                "pty stats exited unsuccessfully".into(),
            )));
        }
    }
    let stats: serde_json::Value = serde_json::from_slice(&outcome.stdout)
        .map_err(|_| "pty stats returned invalid JSON".to_owned())?;
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
    .map_err(|_| "PTY stats normalization failed".to_owned())?;
    let digest = Sha256::digest(&bytes);
    if request.prior_digest.as_deref() == Some(digest.as_slice()) {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let _ = (request.uri, request.demand_watermark, selector.topics);
    Ok(provider_api::ObservationResult::Published(
        provider_api::Publication {
            schema_id: "st2.resource.pty-stats.v1".into(),
            media_type: "application/json".into(),
            bytes,
            topics: vec!["stats".into()],
            facts: Some(vec![provider_api::Fact {
                key: "scope".into(),
                before: provider_api::FactValue::Omitted,
                after: provider_api::FactValue::Value(scope_fact),
            }]),
        },
    ))
}

fn map_source_error(error: pty_stats::PtyStatsError) -> String {
    match error {
        pty_stats::PtyStatsError::Denied => "PTY stats scope denied",
        pty_stats::PtyStatsError::Unavailable => "PTY stats is unavailable",
        pty_stats::PtyStatsError::ResourceExhausted => "PTY stats output exceeded limits",
        pty_stats::PtyStatsError::DeadlineExceeded => "PTY stats deadline exceeded",
        pty_stats::PtyStatsError::Cancelled => "PTY stats was cancelled",
    }
    .into()
}

export!(Component);
