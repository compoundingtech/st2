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
const MAX_VERSION: u64 = 9_007_199_254_740_991;
const TOPICS: [&str; 4] = ["ready", "updated", "failed", "expired"];
const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "topics": {
      "type": "array",
      "items": { "type": "string" },
      "uniqueItems": true
    }
  },
  "required": ["topics"],
  "additionalProperties": false
}"#;
const DEFAULT_SELECTOR: &str = r#"{"topics":["ready","updated","failed","expired"]}"#;

struct Component;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selector {
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

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusCounts {
    locked: u64,
    open: u64,
    awaiting: u64,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
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

struct NormalizedSource {
    observed_at: String,
    artifact: ArtifactManifest,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot<'a> {
    schema: &'static str,
    observed_at: &'a str,
    #[serde(flatten)]
    artifact: &'a ArtifactManifest,
}

impl provider_api::Guest for Component {
    fn describe() -> Result<provider_api::ProviderDescriptor, provider_api::DescriptorError> {
        Ok(provider_api::ProviderDescriptor {
            capabilities: vec![provider_api::SchedulingCapability::Demand],
            selector_schema_json: SELECTOR_SCHEMA.into(),
            default_selector_json: DEFAULT_SELECTOR.into(),
            topics: TOPICS.into_iter().map(str::to_owned).collect(),
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
        .map_err(|_| "invalid Vista selector".to_owned())?;
    if !valid_topics(&selector.topics) {
        return Err("invalid Vista selector".into());
    }
    let Some((slug, version)) = parse_uri(&request.uri) else {
        return Err("invalid Vista URI".into());
    };
    let response = vista::get(&vista::ArtifactRequest {
        slug: slug.to_owned(),
        version,
    })
    .map_err(map_source_error)?;
    let observation = match response {
        vista::ArtifactResponse::Ok(observation) => observation,
        vista::ArtifactResponse::CommandFailed(failure) => {
            let detail = bounded_detail(&failure.stderr);
            let diagnostic = if detail.is_empty() {
                "vista artifact get exited unsuccessfully".into()
            } else {
                format!("vista artifact get failed: {detail}")
            };
            return Ok(provider_api::ObservationResult::Failed(Some(diagnostic)));
        }
    };

    let current = normalize_source(&request.uri, slug, version, observation.current)?;
    let previous = observation
        .previous
        .map(|source| normalize_source(&request.uri, slug, version, source))
        .transpose()?;
    if previous
        .as_ref()
        .is_some_and(|previous| previous.artifact == current.artifact)
    {
        let prior_digest = request
            .prior_digest
            .as_deref()
            .ok_or_else(|| "Vista prior source lacked a snapshot digest".to_owned())?;
        vista::bind_snapshot(prior_digest).map_err(map_source_error)?;
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let publication_topics = topics(previous.as_ref(), &current);
    let publication_facts = facts(previous.as_ref(), &current, slug, version)?;

    let bytes = serde_json::to_vec(&Snapshot {
        schema: SNAPSHOT_SCHEMA,
        observed_at: &current.observed_at,
        artifact: &current.artifact,
    })
    .map_err(|_| "Vista snapshot normalization failed".to_owned())?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("Vista snapshot exceeded limits".into());
    }
    let digest = Sha256::digest(&bytes);
    vista::bind_snapshot(digest.as_slice()).map_err(map_source_error)?;
    if request.prior_digest.as_deref() == Some(digest.as_slice()) {
        return Ok(provider_api::ObservationResult::Unchanged);
    }
    let _ = request.demand_watermark;
    Ok(provider_api::ObservationResult::Published(
        provider_api::Publication {
            schema_id: SNAPSHOT_SCHEMA.into(),
            media_type: "application/json".into(),
            bytes,
            topics: publication_topics,
            facts: Some(publication_facts),
        },
    ))
}

fn normalize_source(
    uri: &str,
    slug: &str,
    version: u64,
    source: vista::SourceSnapshot,
) -> Result<NormalizedSource, String> {
    let artifact: ArtifactManifest = serde_json::from_slice(&source.manifest_json)
        .map_err(|error| format!("vista returned invalid manifest: {error}"))?;
    if artifact.schema_version != 1
        || artifact.uri != uri
        || artifact.slug != slug
        || artifact.version != version
    {
        return Err("vista returned a different artifact identity".into());
    }
    Ok(NormalizedSource {
        observed_at: source.observed_at,
        artifact,
    })
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
    (1..=MAX_VERSION)
        .contains(&version)
        .then_some((slug, version))
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

fn valid_topics(topics: &[String]) -> bool {
    topics
        .iter()
        .enumerate()
        .all(|(index, topic)| TOPICS.contains(&topic.as_str()) && !topics[..index].contains(topic))
}

fn topics(previous: Option<&NormalizedSource>, current: &NormalizedSource) -> Vec<String> {
    let state = current.artifact.state.as_str();
    let Some(previous) = previous else {
        return vec![state.into()];
    };
    if previous.artifact == current.artifact {
        return Vec::new();
    }
    let mut changed = vec!["updated".into()];
    if previous.artifact.state != current.artifact.state {
        changed.push(state.into());
    }
    changed
}

fn facts(
    previous: Option<&NormalizedSource>,
    current: &NormalizedSource,
    slug: &str,
    version: u64,
) -> Result<Vec<provider_api::Fact>, String> {
    let current_state = current.artifact.state.as_str();
    let current_blocks = block_count(current)?;
    let mut facts = vec![current_fact("artifact", format!("{slug}/v{version}"))];
    match previous {
        Some(previous) => facts.push(transition_fact(
            "state",
            Some(previous.artifact.state.as_str()),
            current_state,
        )),
        None => facts.push(current_fact("state", current_state)),
    }
    if let Some(current_blocks) = current_blocks {
        match previous {
            Some(previous) => {
                let previous_blocks = block_count(previous)?;
                facts.push(transition_fact(
                    "blocks",
                    previous_blocks.as_deref(),
                    &current_blocks,
                ));
            }
            None => facts.push(current_fact("blocks", current_blocks)),
        }
    }
    Ok(facts)
}

fn block_count(source: &NormalizedSource) -> Result<Option<String>, String> {
    let Some(status) = source.artifact.status.as_ref() else {
        return Ok(None);
    };
    let total = status
        .locked
        .checked_add(status.open)
        .and_then(|total| total.checked_add(status.awaiting))
        .ok_or_else(|| "Vista status block count exceeds u64::MAX".to_owned())?;
    Ok(Some(total.to_string()))
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

fn bounded_detail(stderr: &[u8]) -> String {
    const MAX_DETAIL: usize = 4096;
    String::from_utf8_lossy(&stderr[..stderr.len().min(MAX_DETAIL)])
        .trim()
        .to_owned()
}

fn map_source_error(error: vista::VistaError) -> String {
    match error {
        vista::VistaError::Denied => "Vista artifact request denied",
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

    fn source(state: ArtifactState, title: &str, status: Option<StatusCounts>) -> NormalizedSource {
        NormalizedSource {
            observed_at: "2026-09-02T10:00:00Z".into(),
            artifact: ArtifactManifest {
                schema_version: 1,
                uri: "vista://release/v7".into(),
                slug: "release".into(),
                version: 7,
                author: "agent".into(),
                timestamp: "2026-09-02T09:00:00Z".into(),
                change_summary: "created".into(),
                parent: Some(6),
                retired: false,
                state,
                canonical_url: "https://vista.example/release/v7".into(),
                title: Some(title.into()),
                template: Some("architecture-review".into()),
                status,
            },
        }
    }

    #[test]
    fn vista_uri_accepts_only_backend_safe_integer_versions() {
        assert_eq!(
            parse_uri("vista://release-notes/v9007199254740991"),
            Some(("release-notes", MAX_VERSION))
        );
        for invalid in [
            "vista://release-notes/v0",
            "vista://release-notes/v01",
            "vista://release-notes/v9007199254740992",
            "vista://release-notes/v9999999999999999999",
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
    fn source_identity_must_match_the_canonical_request_uri() {
        let manifest_json =
            serde_json::to_vec(&source(ArtifactState::Ready, "Architecture", None).artifact)
                .unwrap();
        let make_source = || vista::SourceSnapshot {
            manifest_json: manifest_json.clone(),
            observed_at: "2026-09-02T10:00:00Z".into(),
        };
        assert!(normalize_source("vista://release/v7", "release", 7, make_source(),).is_ok());
        assert!(normalize_source("vista://other/v7", "other", 7, make_source(),).is_err());
    }

    #[test]
    fn selector_contains_topics_only_and_defaults_to_all_topics() {
        let schema: serde_json::Value = serde_json::from_str(SELECTOR_SCHEMA).unwrap();
        assert_eq!(schema["required"], serde_json::json!(["topics"]));
        assert_eq!(
            schema["properties"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            ["topics"]
        );
        let default: Selector = serde_json::from_str(DEFAULT_SELECTOR).unwrap();
        assert_eq!(default.topics, TOPICS.map(str::to_owned));
        assert!(valid_topics(&TOPICS.map(str::to_owned)));
        assert!(!valid_topics(&["ready".into(), "ready".into()]));
        assert!(!valid_topics(&["unknown".into()]));
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

    #[test]
    fn snapshot_flattens_the_strict_manifest_with_observed_at() {
        let source = source(ArtifactState::Ready, "Architecture", None);
        let snapshot = serde_json::to_value(Snapshot {
            schema: SNAPSHOT_SCHEMA,
            observed_at: &source.observed_at,
            artifact: &source.artifact,
        })
        .unwrap();
        assert_eq!(snapshot["schema"], SNAPSHOT_SCHEMA);
        assert_eq!(snapshot["observedAt"], "2026-09-02T10:00:00Z");
        assert_eq!(snapshot["uri"], "vista://release/v7");
        assert_eq!(snapshot["state"], "ready");
        assert!(snapshot.get("artifact").is_none());
        assert!(snapshot.get("status").is_none());
    }

    #[test]
    fn snapshot_topics_and_facts_match_manifest_transitions() {
        let previous = source(
            ArtifactState::Ready,
            "Architecture",
            Some(StatusCounts {
                locked: 3,
                open: 1,
                awaiting: 2,
            }),
        );
        let current = source(
            ArtifactState::Ready,
            "Architecture corrected",
            Some(StatusCounts {
                locked: 3,
                open: 2,
                awaiting: 2,
            }),
        );
        assert_eq!(topics(Some(&previous), &current), ["updated"]);
        let facts = facts(Some(&previous), &current, "release", 7).unwrap();
        assert_eq!(facts.len(), 3);
        assert_eq!(facts[0].key, "artifact");
        assert!(matches!(
            &facts[0].after,
            provider_api::FactValue::Value(value) if value == "release/v7"
        ));
        assert_eq!(facts[1].key, "state");
        assert!(matches!(
            (&facts[1].before, &facts[1].after),
            (
                provider_api::FactValue::Value(before),
                provider_api::FactValue::Value(after)
            ) if before == "ready" && after == "ready"
        ));
        assert_eq!(facts[2].key, "blocks");
        assert!(matches!(
            (&facts[2].before, &facts[2].after),
            (
                provider_api::FactValue::Value(before),
                provider_api::FactValue::Value(after)
            ) if before == "6" && after == "7"
        ));
    }

    #[test]
    fn topics_emit_current_state_first_and_only_changed_state_later() {
        let ready = source(ArtifactState::Ready, "Architecture", None);
        let same = source(ArtifactState::Ready, "Architecture", None);
        let failed = source(ArtifactState::Failed, "Architecture", None);
        let expired = source(ArtifactState::Expired, "Architecture", None);
        assert_eq!(topics(None, &ready), ["ready"]);
        assert!(topics(Some(&ready), &same).is_empty());
        assert_eq!(topics(Some(&ready), &failed), ["updated", "failed"]);
        assert_eq!(topics(None, &expired), ["expired"]);
    }

    #[test]
    fn block_count_overflow_is_atomic_failure() {
        let overflowing = source(
            ArtifactState::Ready,
            "Architecture",
            Some(StatusCounts {
                locked: u64::MAX,
                open: 1,
                awaiting: 0,
            }),
        );
        assert_eq!(
            block_count(&overflowing),
            Err("Vista status block count exceeds u64::MAX".into())
        );
        let maximum = source(
            ArtifactState::Ready,
            "Architecture",
            Some(StatusCounts {
                locked: u64::MAX,
                open: 0,
                awaiting: 0,
            }),
        );
        assert_eq!(block_count(&maximum), Ok(Some(u64::MAX.to_string())));
    }
}

export!(Component);
