use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/vista",
    world: "vista-provider",
    with: {
        "compoundingtech:st2-vista/vista@0.1.0": generate,
    },
});

use compoundingtech::st2_vista::vista;
use exports::st2::resource_provider::provider_api;

const SNAPSHOT_SCHEMA: &str = "dev.schickling.vista.snapshot.v1";
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;
const TOPICS: [&str; 4] = ["ready", "updated", "failed", "expired"];
const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "slug": { "type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$" },
    "version": { "type": "integer", "minimum": 1 },
    "topics": {
      "type": "array",
      "items": { "type": "string", "enum": ["ready", "updated", "failed", "expired"] },
      "uniqueItems": true
    }
  },
  "required": ["slug", "version"],
  "additionalProperties": false
}"#;

struct Component;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Selector {
    slug: String,
    version: u64,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ArtifactState {
    Ready,
    Failed,
    Expired,
}

impl ArtifactState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusCounts {
    locked: u64,
    open: u64,
    awaiting: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ArtifactManifest {
    schema_version: u8,
    uri: String,
    slug: String,
    version: u64,
    author: String,
    timestamp: String,
    change_summary: String,
    parent: Option<u64>,
    retired: bool,
    state: ArtifactState,
    canonical_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<StatusCounts>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot<'a> {
    schema: &'static str,
    #[serde(flatten)]
    artifact: &'a ArtifactManifest,
}

impl provider_api::Guest for Component {
    fn describe() -> Result<provider_api::ProviderDescriptor, provider_api::DescriptorError> {
        Ok(provider_api::ProviderDescriptor {
            capabilities: vec![provider_api::SchedulingCapability::Demand],
            selector_schema_json: SELECTOR_SCHEMA.into(),
            default_selector_json: "{}".into(),
            topics: TOPICS.into_iter().map(str::to_owned).collect(),
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
        .map_err(|_| "invalid Vista selector".to_owned())?;
    let Some((uri_slug, uri_version)) = parse_uri(&request.uri) else {
        return Err("invalid Vista URI".into());
    };
    if !valid_slug(&selector.slug)
        || selector.version == 0
        || selector.slug != uri_slug
        || selector.version != uri_version
    {
        return Err("Vista selector does not match URI identity".into());
    }

    let outcome = vista::get(&vista::ArtifactRequest {
        slug: selector.slug.clone(),
        version: selector.version,
    })
    .map_err(map_source_error)?;
    if outcome.stdout_truncated || outcome.stderr_truncated {
        return Err("Vista output exceeded limits".into());
    }
    match outcome.exit {
        vista::ExitStatus::Code(0) => {}
        vista::ExitStatus::Code(_) | vista::ExitStatus::Signal(_) => {
            let detail = bounded_detail(&outcome.stderr);
            let diagnostic = if detail.is_empty() {
                "vista artifact get exited unsuccessfully".into()
            } else {
                format!("vista artifact get failed: {detail}")
            };
            return Ok(provider_api::ObservationResult::Failed(Some(diagnostic)));
        }
    }

    let artifact: ArtifactManifest = serde_json::from_slice(&outcome.stdout)
        .map_err(|error| format!("vista returned invalid manifest: {error}"))?;
    if artifact.schema_version != 1
        || artifact.uri != request.uri
        || artifact.slug != selector.slug
        || artifact.version != selector.version
    {
        return Err("vista returned a different artifact identity".into());
    }

    let state = artifact.state.as_str();
    let bytes = serde_json::to_vec(&Snapshot {
        schema: SNAPSHOT_SCHEMA,
        artifact: &artifact,
    })
    .map_err(|_| "Vista snapshot normalization failed".to_owned())?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("Vista snapshot exceeded limits".into());
    }
    let digest = Sha256::digest(&bytes);
    if request.prior_digest.as_deref() == Some(digest.as_slice()) {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let topics = if request.prior_digest.is_some() {
        vec!["updated".into(), state.into()]
    } else {
        vec![state.into()]
    };
    let _ = (request.demand_watermark, selector.topics);
    Ok(provider_api::ObservationResult::Published(
        provider_api::Publication {
            schema_id: SNAPSHOT_SCHEMA.into(),
            media_type: "application/json".into(),
            bytes,
            topics,
            facts: Some(vec![provider_api::Fact {
                key: "state".into(),
                before: provider_api::FactValue::Omitted,
                after: provider_api::FactValue::Value(state.into()),
            }]),
        },
    ))
}

fn parse_uri(uri: &str) -> Option<(&str, u64)> {
    let subject = uri.strip_prefix("vista://")?;
    let (slug, version) = subject.split_once('/')?;
    let digits = version.strip_prefix('v')?;
    if version.contains('/')
        || !valid_slug(slug)
        || digits.is_empty()
        || digits.len() > 19
        || digits.starts_with('0')
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let version = digits.parse::<u64>().ok()?;
    (version > 0).then_some((slug, version))
}

fn valid_slug(slug: &str) -> bool {
    !slug.is_empty()
        && slug.len() <= 128
        && slug.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || (byte == b'-' && index > 0)
        })
        && !slug.ends_with('-')
        && !slug.contains("--")
}

fn bounded_detail(stderr: &[u8]) -> String {
    const MAX_DETAIL: usize = 4096;
    String::from_utf8_lossy(&stderr[..stderr.len().min(MAX_DETAIL)])
        .trim()
        .to_owned()
}

fn map_source_error(error: vista::VistaError) -> String {
    match error {
        vista::VistaError::Denied => "Vista artifact scope denied",
        vista::VistaError::Unavailable => "Vista is unavailable",
        vista::VistaError::ResourceExhausted => "Vista output exceeded limits",
        vista::VistaError::DeadlineExceeded => "Vista deadline exceeded",
        vista::VistaError::Cancelled => "Vista observation was cancelled",
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vista_uri_identity_grammar_is_strict() {
        assert_eq!(parse_uri("vista://release-notes/v7"), Some(("release-notes", 7)));
        assert_eq!(
            parse_uri("vista://a/v9999999999999999999"),
            Some(("a", 9_999_999_999_999_999_999))
        );
        for invalid in [
            "vista://release-notes/v0",
            "vista://release-notes/v01",
            "vista://release-notes/v10000000000000000000",
            "vista://-release/v1",
            "vista://release-/v1",
            "vista://release--notes/v1",
            "vista://Release/v1",
            "vista://release/v1/extra",
            "vista://release/1",
        ] {
            assert_eq!(parse_uri(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn artifact_manifest_rejects_unknown_fields() {
        let unknown = br#"{
            "schemaVersion": 1,
            "uri": "vista://release/v1",
            "slug": "release",
            "version": 1,
            "author": "agent",
            "timestamp": "2026-09-02T10:00:00Z",
            "changeSummary": "created",
            "parent": null,
            "retired": false,
            "state": "ready",
            "canonicalUrl": "https://vista.example/release/v1",
            "unexpected": true
        }"#;
        assert!(serde_json::from_slice::<ArtifactManifest>(unknown).is_err());
    }
}

export!(Component);
