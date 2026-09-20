use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use st3::api::AppState;
use st3::store::Store;
use tokio::sync::{Notify, watch};
use tower::ServiceExt as _;

fn asset_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/st3/client-v0")
}

fn json(path: impl AsRef<Path>) -> Value {
    let path = path.as_ref();
    serde_json::from_slice(
        &std::fs::read(path)
            .unwrap_or_else(|error| panic!("read client v0 asset {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("parse client v0 asset {}: {error}", path.display()))
}

fn fixture(name: &str) -> Value {
    json(asset_root().join("fixtures").join(name))
}

#[test]
fn manifest_names_existing_json_fixtures_and_schema_definitions() {
    let root = asset_root();
    let manifest = json(root.join("fixtures/manifest.json"));
    let schema = json(root.join("schemas/client-v0.schema.json"));
    let definitions = schema["$defs"].as_object().expect("schema definitions");
    let mut files = HashSet::new();

    for entry in manifest["fixtures"].as_array().expect("fixture manifest") {
        let file = entry["file"].as_str().expect("fixture file");
        let definition = entry["definition"].as_str().expect("fixture definition");
        assert!(files.insert(file), "duplicate fixture {file}");
        assert!(
            definitions.contains_key(definition),
            "unknown definition {definition}"
        );
        let value = json(root.join("fixtures").join(file));
        assert!(!value.is_null(), "empty fixture {file}");
    }

    for entry in std::fs::read_dir(root.join("fixtures")).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if name != "manifest.json" {
            assert!(files.contains(name.as_str()), "unlisted fixture {name}");
        }
    }

    fn assert_refs_resolve(value: &Value, definitions: &serde_json::Map<String, Value>) {
        match value {
            Value::Object(object) => {
                if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                    && let Some(name) = reference.strip_prefix("#/$defs/")
                {
                    assert!(
                        definitions.contains_key(name),
                        "unresolved schema reference {reference}"
                    );
                }
                for child in object.values() {
                    assert_refs_resolve(child, definitions);
                }
            }
            Value::Array(array) => {
                for child in array {
                    assert_refs_resolve(child, definitions);
                }
            }
            _ => {}
        }
    }
    assert_refs_resolve(&schema, definitions);
}

#[test]
fn operation_manifest_is_launch_only_and_covers_v0_resources_and_actions() {
    let operations = json(asset_root().join("schemas/operations.json"));
    let encoded = serde_json::to_string(&operations).unwrap();
    assert!(
        !encoded.contains("planning"),
        "planning leaked into the client surface"
    );
    assert_eq!(operations["transport"]["public_listener"], false);
    assert_eq!(operations["transport"]["remote_mode"], "online-thin-client");

    let read_ids = operations["reads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|read| read["id"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    for required in [
        "capabilities.get",
        "attention.list",
        "messages.list",
        "launches.list",
        "launch-variants.list",
        "launch-decisions.list",
        "launch-approvals.list",
        "missions.list",
        "work.list",
        "agents.list",
        "runtimes.list",
        "operations.list",
        "history.list",
        "sessions.list",
        "timeline.list",
        "events.list",
        "terminal.screen",
    ] {
        assert!(
            read_ids.contains(required),
            "missing read contract {required}"
        );
    }

    let actions = operations["actions"].as_object().unwrap();
    for required in [
        "attention.resolve",
        "message.send",
        "message.read",
        "message.close",
        "launch.create",
        "launch.revise",
        "launch.preview",
        "launch.approve",
        "launch.cancel",
        "mission.start",
        "mission.revise",
        "mission.approve-revision",
        "mission.cancel",
        "work.claim",
        "work.renew",
        "work.progress",
        "work.complete",
        "work.fail",
        "work.release",
        "work.publish-mission",
        "runtime.stop",
        "runtime.restart",
        "runtime.reset",
        "runtime.context-clear",
        "runtime.signal",
        "terminal.input",
        "terminal.resize",
        "terminal.attach",
        "terminal.detach",
        "pairing.revoke",
    ] {
        assert!(
            actions.contains_key(required),
            "missing action contract {required}"
        );
    }
    let schema = json(asset_root().join("schemas/client-v0.schema.json"));
    let schema_actions = schema["$defs"]["ActionCommon"]["properties"]["type"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|action| action.as_str().unwrap())
        .collect::<BTreeSet<_>>();
    let manifest_actions = actions.keys().map(String::as_str).collect::<BTreeSet<_>>();
    assert_eq!(schema_actions, manifest_actions);
}

#[test]
fn resource_fixture_covers_every_resource_kind_with_stable_unique_ids() {
    let resources = fixture("resources.json");
    let mut ids = HashSet::new();
    let kinds = resources
        .as_array()
        .unwrap()
        .iter()
        .map(|resource| {
            let id = resource["id"].as_str().unwrap();
            assert!(id.contains('/'), "ID has no type prefix: {id}");
            assert!(ids.insert(id), "duplicate stable ID {id}");
            resource["kind"].as_str().unwrap()
        })
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        "agent",
        "attention",
        "history",
        "launch",
        "launch-approval",
        "launch-decision",
        "launch-variant",
        "message",
        "mission",
        "operation",
        "runtime",
        "session",
        "work",
    ]);
    assert_eq!(kinds, expected);
}

#[test]
fn launch_preview_token_is_deterministic() {
    let resources = fixture("resources.json");
    let variant = resources
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["kind"] == "launch-variant")
        .unwrap();
    // RFC 8785 canonical JSON for the normative token input in the fixture.
    let canonical = concat!(
        r#"{"api_version":"st3.client.v0","candidate_revision":2,"diagnostics":[],"launch_id":"launch/release","normalized_mission":{"goals":["Ship release"],"id":"mission/release","steps":[{"id":"build","needs":[]},{"id":"deploy","needs":["build"]}]},"target_generation":null,"variant_id":"launch-variant/release/default"}"#,
    );
    let token = format!("lpv0:{:x}", Sha256::digest(canonical.as_bytes()));
    assert_eq!(variant["preview_token"], token);
}

#[test]
fn action_fixture_is_idempotent_fenced_and_cannot_select_identity() {
    let action = fixture("action.json");
    assert_eq!(action["api_version"], "st3.client.v0");
    assert!(action["idempotency_key"].as_str().unwrap().len() >= 16);
    assert!(action["fence"]["snapshot_id"].is_string());
    assert!(action["fence"]["mission_generation"].is_string());
    assert!(action["fence"]["runtime_incarnation"].is_string());
    let encoded = serde_json::to_string(&action).unwrap();
    for forbidden in [
        "\"actor\"",
        "\"credential\"",
        "fleet_secret",
        "shared_secret",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "forbidden action identity field {forbidden}"
        );
    }
}

#[test]
fn timeline_is_ordered_typed_and_links_tool_results_to_prior_calls() {
    let timeline = fixture("timeline.json");
    let entries = timeline["value"]["items"].as_array().unwrap();
    let mut previous = None;
    let mut calls = HashSet::new();
    let mut types = BTreeSet::new();
    for entry in entries {
        let sequence = entry["sequence"].as_u64().unwrap();
        if let Some(previous) = previous {
            assert!(sequence > previous, "timeline is not strictly ordered");
        }
        previous = Some(sequence);
        let kind = entry["type"].as_str().unwrap();
        types.insert(kind);
        if kind == "tool_call" {
            calls.insert(entry["body"]["call_id"].as_str().unwrap());
        } else if kind == "tool_result" {
            assert!(calls.contains(entry["body"]["call_id"].as_str().unwrap()));
        }
        assert!(matches!(
            entry["role"].as_str().unwrap(),
            "system" | "user" | "assistant" | "tool"
        ));
    }
    assert_eq!(
        types,
        BTreeSet::from([
            "content",
            "error",
            "message",
            "redaction",
            "status",
            "tool_call",
            "tool_result",
            "truncation",
            "usage"
        ])
    );
}

#[test]
fn event_and_terminal_streams_are_contiguous_and_resumable() {
    let events = fixture("events.json");
    let items = events["value"]["items"].as_array().unwrap();
    for pair in items.windows(2) {
        assert_eq!(pair[0]["next_cursor"], pair[1]["previous_cursor"]);
        assert_eq!(
            pair[0]["sequence"].as_u64().unwrap() + 1,
            pair[1]["sequence"].as_u64().unwrap()
        );
    }
    assert_eq!(
        items.last().unwrap()["next_cursor"],
        events["value"]["resume_cursor"]
    );

    let frames = fixture("terminal-frames.json");
    let expected_incarnation = frames["value"]["runtime_incarnation"].as_str().unwrap();
    let frames = frames["value"]["frames"].as_array().unwrap();
    for (index, frame) in frames.iter().enumerate() {
        assert_eq!(frame["runtime_incarnation"], expected_incarnation);
        if index > 0 {
            assert_eq!(
                frames[index - 1]["sequence"].as_u64().unwrap() + 1,
                frame["sequence"].as_u64().unwrap()
            );
        }
    }
}

fn test_state(root: &Path) -> AppState {
    AppState {
        store: Arc::new(Store::open_memory("client-v0-baseline").unwrap()),
        notify: Arc::new(Notify::new()),
        event_notify: watch::channel(0_u64).0,
        node: "client-v0-baseline".into(),
        state_dir: root.to_path_buf(),
        pty_root: root.join("pty"),
        pty_binary: PathBuf::from("pty"),
        fleet_id: None,
        configured_peers: Vec::new(),
    }
}

#[tokio::test]
async fn client_v0_read_routes_conform_to_the_manifest() {
    let root = tempfile::tempdir().unwrap();
    let app = st3::api::router(test_state(root.path()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/client/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let envelope: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(envelope["api_version"], "st3.client.v0");
    assert_eq!(
        envelope["snapshot"]["projection_version"],
        "client-projection.v0"
    );
    assert_eq!(envelope["value"]["kind"], "capabilities");
    assert_eq!(envelope["value"]["transport"], "unix");
    assert_eq!(envelope["value"]["limits"]["max_page_items"], 200);
}

async fn client_json(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, value)
}

#[tokio::test]
async fn operational_lists_share_one_versioned_paginated_shape() {
    let root = tempfile::tempdir().unwrap();
    let state = test_state(root.path());
    let store = state.store.clone();
    for (subject, runtime, incarnation) in [
        ("agent/alpha", "alpha-runtime", "alpha-runtime:i1"),
        ("agent/beta", "beta-runtime", "beta-runtime:i1"),
    ] {
        store
            .append_claim(&st3::model::ClaimInput {
                subject: subject.into(),
                kind: "runtime.observed".into(),
                actor: Some(subject.into()),
                fields: std::collections::BTreeMap::from([
                    ("runtime_id".into(), Value::String(runtime.into())),
                    ("incarnation_id".into(), Value::String(incarnation.into())),
                    ("status".into(), Value::String("running".into())),
                    ("reachability".into(), Value::String("local".into())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .unwrap();
    }
    let app = st3::api::router(state);
    for collection in [
        "attention",
        "messages",
        "launches",
        "work",
        "agents",
        "history",
        "sessions",
    ] {
        let (status, envelope) =
            client_json(app.clone(), &format!("/v1/client/{collection}")).await;
        assert_eq!(status, StatusCode::OK, "{collection}: {envelope}");
        assert_eq!(envelope["api_version"], "st3.client.v0");
        assert_eq!(envelope["value"]["kind"], "page");
        assert_eq!(envelope["value"]["collection"], collection);
        assert!(envelope["value"]["items"].is_array());
        assert_eq!(envelope["value"]["page"]["limit"], 50);
    }

    let (_, first) = client_json(app.clone(), "/v1/client/agents?limit=1").await;
    assert_eq!(first["value"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["value"]["page"]["has_more"], true);
    let cursor = first["value"]["page"]["next_cursor"].as_str().unwrap();
    let uri = format!(
        "/v1/client/agents?limit=1&cursor={}",
        urlencoding::encode(cursor)
    );
    let (_, second) = client_json(app.clone(), &uri).await;
    assert_eq!(first["snapshot"]["id"], second["snapshot"]["id"]);
    assert_ne!(
        first["value"]["items"][0]["id"],
        second["value"]["items"][0]["id"]
    );

    store
        .append_claim(&st3::model::ClaimInput {
            subject: "agent/gamma".into(),
            kind: "runtime.observed".into(),
            actor: Some("agent/gamma".into()),
            fields: std::collections::BTreeMap::from([
                ("runtime_id".into(), Value::String("gamma-runtime".into())),
                (
                    "incarnation_id".into(),
                    Value::String("gamma-runtime:i1".into()),
                ),
                ("status".into(), Value::String("running".into())),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .unwrap();
    let (status, expired) = client_json(app, &uri).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(expired["code"], "page-cursor-expired");
    assert_eq!(expired["error_version"], "st3.client.error.v0");
}

#[tokio::test]
async fn stopped_agents_are_annotated_history_not_default_membership() {
    let root = tempfile::tempdir().unwrap();
    let state = test_state(root.path());
    state
        .store
        .append_claim(&st3::model::ClaimInput {
            subject: "agent/retired".into(),
            kind: "runtime.observed".into(),
            actor: Some("agent/retired".into()),
            fields: std::collections::BTreeMap::from([
                ("runtime_id".into(), Value::String("retired-runtime".into())),
                (
                    "incarnation_id".into(),
                    Value::String("retired-runtime:i1".into()),
                ),
                ("status".into(), Value::String("stopped".into())),
                ("terminal".into(), Value::Bool(true)),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .unwrap();
    let app = st3::api::router(state);
    let (_, current) = client_json(app.clone(), "/v1/client/agents").await;
    assert!(current["value"]["items"].as_array().unwrap().is_empty());
    let (_, history) = client_json(app, "/v1/client/agents?history=true").await;
    let retired = &history["value"]["items"][0];
    assert_eq!(retired["id"], "agent/retired");
    assert_eq!(retired["operational"]["layer"], "history");
    assert_eq!(retired["operational"]["actionable"], false);
    assert!(
        retired["operational"]["reasons"]
            .as_array()
            .unwrap()
            .contains(&Value::String("stopped".into()))
    );
}

#[tokio::test]
#[ignore = "red baseline: enable when fenced client v0 actions are implemented"]
async fn client_v0_action_route_accepts_the_golden_fenced_action() {
    let root = tempfile::tempdir().unwrap();
    let action = std::fs::read(asset_root().join("fixtures/action.json")).unwrap();
    let response = st3::api::router(test_state(root.path()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/client/actions")
                .header("content-type", "application/json")
                .body(Body::from(action))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}
