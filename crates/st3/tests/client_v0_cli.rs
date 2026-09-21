#![cfg(unix)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest as _, Sha256};
use st3::api::AppState;
use st3::model::ClaimInput;
use st3::store::Store;
use tokio::sync::{Notify, watch};

fn test_state(root: &Path) -> AppState {
    AppState {
        store: Arc::new(Store::open_memory("client-v0-cli").unwrap()),
        notify: Arc::new(Notify::new()),
        event_notify: watch::channel(0_u64).0,
        node: "client-v0-cli".into(),
        state_dir: root.to_path_buf(),
        pty_root: root.join("pty"),
        pty_binary: PathBuf::from("pty"),
        fleet_id: None,
        configured_peers: vec!["offline-peer".into()],
    }
}

async fn run_cli(socket: &Path, args: &[&str]) -> Output {
    run_cli_mode(socket, true, args).await
}

async fn run_cli_human(socket: &Path, args: &[&str]) -> Output {
    run_cli_mode(socket, false, args).await
}

async fn run_cli_mode(socket: &Path, json: bool, args: &[&str]) -> Output {
    let binary = assert_cmd::cargo::cargo_bin!("st3").to_path_buf();
    let socket = socket.to_path_buf();
    let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(binary);
        command.arg("--endpoint").arg(socket);
        if json {
            command.arg("--json");
        }
        command.args(args).output().unwrap()
    })
    .await
    .unwrap()
}

fn value(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "decode CLI JSON: {error}; stdout={}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_product_cli_uses_real_client_v0_envelopes_and_fences() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("st3.sock");
    let state = test_state(root.path());
    state
        .store
        .append_claim(&ClaimInput {
            subject: "agent/cli-runtime".into(),
            kind: "runtime.observed".into(),
            actor: Some("agent/cli-runtime".into()),
            fields: BTreeMap::from([
                ("runtime_id".into(), Value::String("cli-runtime".into())),
                (
                    "incarnation_id".into(),
                    Value::String("cli-runtime:i1".into()),
                ),
                ("status".into(), Value::String("running".into())),
                ("reachability".into(), Value::String("local".into())),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .unwrap();
    state
        .store
        .append_claim(&ClaimInput {
            subject: "agent/cli-stopped".into(),
            kind: "runtime.observed".into(),
            actor: Some("agent/cli-stopped".into()),
            fields: BTreeMap::from([
                ("runtime_id".into(), Value::String("cli-stopped".into())),
                (
                    "incarnation_id".into(),
                    Value::String("cli-stopped:i1".into()),
                ),
                ("status".into(), Value::String("stopped".into())),
                ("reachability".into(), Value::String("local".into())),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .unwrap();
    state
        .store
        .append_claim(&ClaimInput {
            subject: "custom/client/cli-device".into(),
            kind: "custom.client.pairing-completed".into(),
            actor: Some("person/nathan".into()),
            fields: BTreeMap::from([
                (
                    "credential_hash".into(),
                    Value::String(hex::encode(Sha256::digest(b"cli credential"))),
                ),
                ("device_id".into(), Value::String("device/cli".into())),
                ("person_id".into(), Value::String("person/nathan".into())),
                (
                    "session_actor".into(),
                    Value::String("person/nathan/session/cli".into()),
                ),
                ("scopes".into(), serde_json::json!(["read.projections"])),
                ("expires_at_unix_ms".into(), serde_json::json!(u64::MAX / 2)),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .unwrap();

    let server_socket = socket.clone();
    let server =
        tokio::spawn(
            async move { st3::api::serve_unix(&server_socket, st3::api::router(state)).await },
        );
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(socket.exists(), "client-v0 test socket did not appear");

    let now = value(&run_cli(&socket, &["now"]).await);
    assert_eq!(now["api_version"], "st3.client.v0");
    assert_eq!(now["value"]["collection"], "now");

    let machines = value(&run_cli(&socket, &["machines"]).await);
    assert_eq!(machines["api_version"], "st3.client.v0");
    assert_eq!(machines["value"]["items"][0]["id"], "machine/client-v0-cli");
    assert_eq!(machines["value"]["items"][0]["kind"], "machine");
    assert_eq!(
        machines["value"]["items"][0]["runtime_ids"][0],
        "runtime/cli-runtime"
    );
    assert_eq!(
        machines["value"]["items"][0]["capacity"]["state"],
        "unknown"
    );
    assert_eq!(
        machines["value"]["items"][0]["occupancy"]["running_runtimes"],
        1
    );
    assert!(
        machines["value"]["items"][0]["projects"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(machines["value"]["items"][1]["id"], "machine/offline-peer");
    assert_eq!(machines["value"]["items"][1]["state"], "indeterminate");
    let machine_history = value(&run_cli(&socket, &["machines", "--all"]).await);
    assert_eq!(
        machine_history["value"]["items"][0]["runtime_ids"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        machine_history["value"]["items"][0]["occupancy"]["running_runtimes"],
        1
    );

    let activity = value(&run_cli(&socket, &["activity", "--limit", "10"]).await);
    assert_eq!(activity["api_version"], "st3.client.v0");
    assert_eq!(activity["value"]["kind"], "event-page");

    let devices = value(&run_cli(&socket, &["devices", "--as", "person/nathan"]).await);
    assert_eq!(devices["value"]["items"][0]["id"], "device/cli");
    assert_eq!(devices["value"]["items"][0]["state"], "active");

    let pairing = value(
        &run_cli(
            &socket,
            &["devices", "--as", "person/nathan", "pair", "test-phone"],
        )
        .await,
    );
    assert_eq!(pairing["api_version"], "st3.client.v0");
    assert_eq!(pairing["value"]["kind"], "pairing-challenge");

    let revoked = value(
        &run_cli(
            &socket,
            &["devices", "--as", "person/nathan", "revoke", "device/cli"],
        )
        .await,
    );
    assert_eq!(revoked["api_version"], "st3.client.v0");
    assert_eq!(revoked["value"]["status"], "completed");

    let current = value(&run_cli(&socket, &["devices", "--as", "person/nathan"]).await);
    assert!(current["value"]["items"].as_array().unwrap().is_empty());
    let history = value(&run_cli(&socket, &["devices", "--as", "person/nathan", "--all"]).await);
    assert_eq!(history["value"]["items"][0]["state"], "revoked");

    for (arguments, heading) in [
        (vec!["now"], "NOW"),
        (vec!["machines"], "MACHINES"),
        (vec!["activity", "--limit", "10"], "ACTIVITY"),
        (vec!["devices", "--as", "person/nathan", "--all"], "DEVICES"),
    ] {
        let output = run_cli_human(&socket, &arguments).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rendered = String::from_utf8(output.stdout).unwrap();
        assert!(rendered.starts_with(heading), "{rendered}");
        assert!(!rendered.contains("api_version"), "{rendered}");
        assert!(!rendered.contains("request_id"), "{rendered}");
        assert!(!rendered.contains("snapshot"), "{rendered}");
    }

    server.abort();
}
