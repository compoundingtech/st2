use std::collections::BTreeSet;

use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};

wit_bindgen::generate!({
    path: "../../wit/pty-stats",
    world: "pty-stats-provider",
    with: {
        "compoundingtech:st2-pty-stats/pty-stats@0.1.0": generate,
    },
});

use compoundingtech::st2_pty_stats::pty_stats as host;
use exports::st2::resource_provider::provider_api;

const SNAPSHOT_SCHEMA: &str = "dev.schickling.pty.snapshot.v1";
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;
const TOPICS: [&str; 3] = ["lifecycle", "metadata", "runtime"];
const SELECTOR_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "topics": {
      "type": "array",
      "items": { "type": "string", "enum": ["lifecycle", "metadata", "runtime"] },
      "minItems": 1,
      "uniqueItems": true
    }
  },
  "required": ["topics"],
  "additionalProperties": false
}"#;
const DEFAULT_SELECTOR: &str = r#"{"topics":["lifecycle","metadata"]}"#;

struct Component;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Selector {
    topics: Vec<String>,
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
        .map_err(|_| "invalid PTY selector".to_owned())?;
    validate_topics(&selector.topics)?;
    let id = parse_uri(&request.uri).ok_or_else(|| "invalid PTY URI".to_owned())?;

    let observation = host::list_session(id).map_err(map_source_error)?;
    let mut current = observation.current;
    if current.id != id {
        return Err("PTY list returned a different session identity".into());
    }
    let previous = observation.previous.filter(|source| source.id == id);

    if matches!(&current.lifecycle, host::Lifecycle::Running) {
        current = host::stats(id).map_err(map_source_error)?;
        if current.id != id {
            return Err("PTY stats returned a different session identity".into());
        }
    }

    let current_snapshot = build_snapshot(&request.uri, id, &current)?;
    let previous_snapshot = previous
        .as_ref()
        .map(|source| build_snapshot(&request.uri, id, source))
        .transpose()?;
    let same_payload_semantics = previous_snapshot
        .as_ref()
        .is_some_and(|prior| same_semantics(prior, &current_snapshot));
    if same_payload_semantics {
        return Ok(provider_api::ObservationResult::Unchanged);
    }

    let topics = changed_topics(previous_snapshot.as_ref(), &current_snapshot);
    let publication_snapshot = if same_payload_semantics {
        previous_snapshot.expect("the semantically equal previous PTY snapshot was present")
    } else {
        current_snapshot
    };
    let bytes = serde_json::to_vec(&publication_snapshot)
        .map_err(|_| "PTY snapshot normalization failed".to_owned())?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("PTY snapshot exceeded limits".into());
    }
    let digest = Sha256::digest(&bytes);
    host::bind_snapshot(digest.as_slice()).map_err(map_source_error)?;

    let topics = if same_payload_semantics {
        Vec::new()
    } else {
        topics
    };
    let facts = facts(previous.as_ref(), &current, id);
    let _ = (request.prior_digest, request.demand_watermark);
    Ok(provider_api::ObservationResult::Published(
        provider_api::Publication {
            schema_id: SNAPSHOT_SCHEMA.into(),
            media_type: "application/json".into(),
            bytes,
            topics,
            facts: Some(facts),
        },
    ))
}

fn validate_topics(topics: &[String]) -> Result<BTreeSet<&str>, String> {
    let selected: BTreeSet<_> = topics.iter().map(String::as_str).collect();
    if topics.is_empty()
        || selected.len() != topics.len()
        || selected.iter().any(|topic| !TOPICS.contains(topic))
    {
        return Err("invalid PTY selector topics".into());
    }
    Ok(selected)
}

fn parse_uri(uri: &str) -> Option<&str> {
    let id = uri.strip_prefix("pty:")?;
    if id.is_empty()
        || id.len() > 255
        || matches!(id, "." | "..")
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return None;
    }
    Some(id)
}

fn build_snapshot(uri: &str, id: &str, source: &host::SessionSource) -> Result<Value, String> {
    let mut snapshot = Map::new();
    snapshot.insert("schema".into(), Value::String(SNAPSHOT_SCHEMA.into()));
    snapshot.insert("uri".into(), Value::String(uri.into()));
    snapshot.insert(
        "observedAt".into(),
        Value::String(source.observed_at.clone()),
    );
    snapshot.insert("id".into(), Value::String(id.into()));
    snapshot.insert(
        "lifecycle".into(),
        Value::String(lifecycle_str(&source.lifecycle).into()),
    );
    snapshot.insert(
        "generation".into(),
        source
            .generation
            .as_ref()
            .map_or(Value::Null, generation_value),
    );
    snapshot.insert(
        "metadata".into(),
        source.metadata.as_ref().map_or(Value::Null, metadata_value),
    );
    snapshot.insert(
        "runtime".into(),
        source
            .runtime
            .as_ref()
            .map_or(Ok(Value::Null), runtime_value)?,
    );
    Ok(Value::Object(snapshot))
}

fn generation_value(generation: &host::Generation) -> Value {
    match generation {
        host::Generation::Number(number) => Value::Number((*number).into()),
        host::Generation::Timestamp(timestamp) => Value::String(timestamp.clone()),
    }
}

fn metadata_value(metadata: &host::Metadata) -> Value {
    let mut value = Map::new();
    insert_string(&mut value, "displayName", metadata.display_name.as_ref());
    insert_string(&mut value, "command", metadata.command.as_ref());
    insert_string(&mut value, "cwd", metadata.cwd.as_ref());
    insert_string(&mut value, "createdAt", metadata.created_at.as_ref());
    if let Some(exit_code) = metadata.exit_code {
        value.insert("exitCode".into(), Value::Number(exit_code.into()));
    }
    insert_string(&mut value, "exitedAt", metadata.exited_at.as_ref());
    if let Some(tags) = &metadata.tags {
        let tags = tags
            .iter()
            .map(|tag| (tag.key.clone(), Value::String(tag.value.clone())))
            .collect();
        value.insert("tags".into(), Value::Object(tags));
    }
    Value::Object(value)
}

fn runtime_value(runtime: &host::Runtime) -> Result<Value, String> {
    let terminal = &runtime.terminal;
    let process = &runtime.process;
    let clients = &runtime.clients;
    let modes = &runtime.modes;
    let mut process_value = Map::new();
    process_value.insert("alive".into(), Value::Bool(process.alive));
    process_value.insert(
        "exitCode".into(),
        process
            .exit_code
            .map_or(Value::Null, |code| Value::Number(code.into())),
    );
    if let Some(resources) = &process.resources {
        let cpu = Number::from_f64(resources.cpu_percent)
            .ok_or_else(|| "PTY stats returned a non-finite CPU percentage".to_owned())?;
        process_value.insert(
            "resources".into(),
            Value::Object(Map::from_iter([
                ("rssKb".into(), Value::Number(resources.rss_kb.into())),
                ("cpuPercent".into(), Value::Number(cpu)),
            ])),
        );
    } else {
        process_value.insert("resources".into(), Value::Null);
    }
    Ok(Value::Object(Map::from_iter([
        (
            "terminal".into(),
            Value::Object(Map::from_iter([
                ("cols".into(), Value::Number(terminal.cols.into())),
                ("rows".into(), Value::Number(terminal.rows.into())),
                ("cursorX".into(), Value::Number(terminal.cursor_x.into())),
                ("cursorY".into(), Value::Number(terminal.cursor_y.into())),
                (
                    "scrollbackUsed".into(),
                    Value::Number(terminal.scrollback_used.into()),
                ),
                (
                    "scrollbackCapacity".into(),
                    Value::Number(terminal.scrollback_capacity.into()),
                ),
            ])),
        ),
        ("process".into(), Value::Object(process_value)),
        (
            "clients".into(),
            Value::Object(Map::from_iter([
                ("total".into(), Value::Number(clients.total.into())),
                ("attached".into(), Value::Number(clients.attached.into())),
                ("readOnly".into(), Value::Number(clients.read_only.into())),
            ])),
        ),
        (
            "modes".into(),
            Value::Object(Map::from_iter([
                ("sgrMouse".into(), Value::Bool(modes.sgr_mouse)),
                ("cursorHidden".into(), Value::Bool(modes.cursor_hidden)),
                ("kittyKeyboard".into(), Value::Bool(modes.kitty_keyboard)),
                (
                    "kittyKeyboardFlags".into(),
                    Value::Array(
                        modes
                            .kitty_keyboard_flags
                            .iter()
                            .map(|flag| Value::Number((*flag).into()))
                            .collect(),
                    ),
                ),
            ])),
        ),
        (
            "uptimeSeconds".into(),
            runtime
                .uptime_seconds
                .map_or(Value::Null, |seconds| Value::Number(seconds.into())),
        ),
    ])))
}

fn insert_string(map: &mut Map<String, Value>, key: &str, value: Option<&String>) {
    if let Some(value) = value {
        map.insert(key.into(), Value::String(value.clone()));
    }
}

fn same_semantics(previous: &Value, current: &Value) -> bool {
    semantic_projection(previous) == semantic_projection(current)
}

fn semantic_projection(snapshot: &Value) -> Value {
    let mut projected = snapshot.clone();
    if let Some(object) = projected.as_object_mut() {
        object.remove("observedAt");
    }
    projected
}

fn changed_topics(previous: Option<&Value>, current: &Value) -> Vec<String> {
    let Some(previous) = previous else {
        return TOPICS.into_iter().map(str::to_owned).collect();
    };
    TOPICS
        .into_iter()
        .filter(|topic| match *topic {
            "lifecycle" => {
                previous.get("lifecycle") != current.get("lifecycle")
                    || previous.get("generation") != current.get("generation")
            }
            "metadata" => previous.get("metadata") != current.get("metadata"),
            "runtime" => previous.get("runtime") != current.get("runtime"),
            _ => false,
        })
        .map(str::to_owned)
        .collect()
}

fn facts(
    previous: Option<&host::SessionSource>,
    current: &host::SessionSource,
    id: &str,
) -> Vec<provider_api::Fact> {
    let mut facts = vec![current_fact("session", id)];
    facts.push(match previous {
        Some(previous) => transition_fact(
            "state",
            Some(lifecycle_str(&previous.lifecycle)),
            lifecycle_str(&current.lifecycle),
        ),
        None => current_fact("state", lifecycle_str(&current.lifecycle)),
    });
    if let Some(exit) = exit_code(current) {
        facts.push(match previous {
            Some(previous) => transition_fact("exit", exit_code(previous).as_deref(), &exit),
            None => current_fact("exit", &exit),
        });
    }
    facts
}

fn exit_code(source: &host::SessionSource) -> Option<String> {
    source
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.exit_code)
        .map(|code| code.to_string())
}

fn current_fact(key: &str, value: &str) -> provider_api::Fact {
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

const fn lifecycle_str(lifecycle: &host::Lifecycle) -> &'static str {
    match lifecycle {
        host::Lifecycle::Running => "running",
        host::Lifecycle::Exited => "exited",
        host::Lifecycle::Vanished => "vanished",
        host::Lifecycle::Absent => "absent",
    }
}

fn map_source_error(error: host::PtyStatsError) -> String {
    match error {
        host::PtyStatsError::Denied => "PTY session request denied",
        host::PtyStatsError::Unavailable => "PTY control plane is unavailable",
        host::PtyStatsError::ResourceExhausted => "PTY control-plane output exceeded limits",
        host::PtyStatsError::DeadlineExceeded => "PTY control-plane deadline exceeded",
        host::PtyStatsError::Cancelled => "PTY control-plane observation was cancelled",
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(lifecycle: host::Lifecycle) -> host::SessionSource {
        host::SessionSource {
            id: "session-1".into(),
            observed_at: "2026-09-02T10:00:00Z".into(),
            lifecycle,
            generation: Some(host::Generation::Timestamp("created".into())),
            metadata: Some(host::Metadata {
                display_name: Some("Session".into()),
                command: Some("agent".into()),
                cwd: Some("/workspace".into()),
                created_at: Some("created".into()),
                exit_code: None,
                exited_at: None,
                tags: Some(vec![host::Tag {
                    key: "owner".into(),
                    value: "agent".into(),
                }]),
            }),
            runtime: Some(host::Runtime {
                terminal: host::Terminal {
                    cols: 120,
                    rows: 40,
                    cursor_x: 10,
                    cursor_y: 4,
                    scrollback_used: 20,
                    scrollback_capacity: 1_000,
                },
                process: host::Process {
                    alive: true,
                    exit_code: None,
                    resources: None,
                },
                clients: host::Clients {
                    total: 2,
                    attached: 1,
                    read_only: 1,
                },
                modes: host::Modes {
                    sgr_mouse: true,
                    cursor_hidden: false,
                    kitty_keyboard: true,
                    kitty_keyboard_flags: vec![1, 2],
                },
                uptime_seconds: Some(60),
            }),
        }
    }

    #[test]
    fn canonical_uri_validation_rejects_aliases_and_paths() {
        assert_eq!(parse_uri("pty:stable.session-1"), Some("stable.session-1"));
        for uri in [
            "pty:",
            "pty://stable.session-1",
            "pty:../session",
            "pty:..",
            "pty:display name",
            "pty:stable%2Esession",
            "PTY:stable.session-1",
        ] {
            assert_eq!(parse_uri(uri), None);
        }
    }

    #[test]
    fn selectors_are_nonempty_unique_and_closed() {
        assert!(validate_topics(&["lifecycle".into(), "metadata".into()]).is_ok());
        assert!(validate_topics(&[]).is_err());
        assert!(validate_topics(&["runtime".into(), "runtime".into()]).is_err());
        assert!(validate_topics(&["transcript".into()]).is_err());
    }

    #[test]
    fn metadata_snapshot_is_transcript_and_process_identity_free() {
        let snapshot = build_snapshot(
            "pty:session-1",
            "session-1",
            &source(host::Lifecycle::Running),
        )
        .unwrap();
        let bytes = serde_json::to_string(&snapshot).unwrap();
        assert!(bytes.contains("\"displayName\":\"Session\""));
        for forbidden in ["transcript", "screen", "lastLines", "pid", "args", "socket"] {
            assert!(!bytes.contains(forbidden));
        }
    }

    #[test]
    fn selector_topics_do_not_change_the_canonical_carrier() {
        fn carrier(topics: &[String], source: &host::SessionSource) -> (Vec<u8>, Vec<u8>) {
            validate_topics(topics).unwrap();
            let snapshot = build_snapshot("pty:session-1", "session-1", source).unwrap();
            let bytes = serde_json::to_vec(&snapshot).unwrap();
            let digest = Sha256::digest(&bytes).to_vec();
            (bytes, digest)
        }

        let source = source(host::Lifecycle::Running);
        let lifecycle = carrier(&["lifecycle".into()], &source);
        let runtime = carrier(&["metadata".into(), "runtime".into()], &source);
        assert_eq!(lifecycle, runtime);

        let snapshot = build_snapshot("pty:session-1", "session-1", &source).unwrap();
        assert_eq!(snapshot["lifecycle"], "running");
        assert!(snapshot.get("metadata").is_some_and(Value::is_object));
        assert!(snapshot.get("runtime").is_some_and(Value::is_object));
        assert_eq!(
            changed_topics(None, &snapshot),
            ["lifecycle", "metadata", "runtime"]
        );
    }

    #[test]
    fn source_topics_and_facts_are_not_filtered_by_selector() {
        validate_topics(&["metadata".into()]).unwrap();
        let before = source(host::Lifecycle::Running);
        let after = source(host::Lifecycle::Exited);
        let before_snapshot = build_snapshot("pty:session-1", "session-1", &before).unwrap();
        let after_snapshot = build_snapshot("pty:session-1", "session-1", &after).unwrap();
        assert!(!same_semantics(&before_snapshot, &after_snapshot));
        assert_eq!(
            changed_topics(Some(&before_snapshot), &after_snapshot),
            ["lifecycle"]
        );
        let facts = facts(Some(&before), &after, "session-1");
        assert!(
            matches!(&facts[1].before, provider_api::FactValue::Value(value) if value == "running")
        );
        assert!(
            matches!(&facts[1].after, provider_api::FactValue::Value(value) if value == "exited")
        );
    }
}

export!(Component);
