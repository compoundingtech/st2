use std::path::{Path, PathBuf};
use std::sync::Arc;

use st3::api::AppState;
use st3::store::Store;
use st3_client::{
    Capabilities, Client, Envelope, PairedSession, PairingBegin, PairingChallenge, PairingComplete,
};
use tokio::sync::{Notify, watch};

fn state(root: &Path, name: &str) -> AppState {
    AppState {
        store: Arc::new(Store::open_memory(name).unwrap()),
        notify: Arc::new(Notify::new()),
        event_notify: watch::channel(0_u64).0,
        node: name.into(),
        state_dir: root.to_path_buf(),
        pty_root: root.join("pty"),
        pty_binary: PathBuf::from("pty"),
        fleet_id: None,
        configured_peers: Vec::new(),
    }
}

#[tokio::test]
async fn generated_client_conforms_over_the_real_unix_transport() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("st3.sock");
    let server_socket = socket.clone();
    let app = st3::api::router(state(root.path(), "client-unix"));
    let server = tokio::spawn(async move { st3::api::serve_unix(&server_socket, app).await });
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let client = Client::unix(&socket);
    let capabilities = client.capabilities().await.unwrap();
    assert_eq!(
        capabilities.value.transport,
        st3_client::TransportKind::Unix
    );
    assert_eq!(
        capabilities.value.session_actor,
        "person/local/session/unix"
    );
    let work = client.list("work", None, Some(7), false).await.unwrap();
    assert_eq!(work.value.collection, "work");
    assert_eq!(work.value.page.limit, 7);
    let events = client.events(None, Some(10), Some(0)).await.unwrap();
    assert!(!events.value.has_more);
    server.abort();
}

#[tokio::test]
async fn generated_client_conforms_over_paired_loopback_and_rejects_bad_credentials() {
    let root = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = st3::api::router(state(root.path(), "client-loopback"));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{address}");
    let http = reqwest::Client::new();

    let challenge: Envelope<PairingChallenge> = http
        .post(format!("{base}/v1/client/pairings"))
        .json(&PairingBegin {
            api_version: st3_client::API_VERSION.into(),
            device_name: "Conformance phone".into(),
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let pairing_id = challenge.value.pairing_id.trim_start_matches("pairing/");
    let paired: Envelope<PairedSession> = http
        .post(format!("{base}/v1/client/pairings/{pairing_id}/complete"))
        .json(&PairingComplete {
            api_version: st3_client::API_VERSION.into(),
            code: challenge.value.code,
            device_public_key: "conformance-public-key-000000000000000000000000".into(),
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let client = Client::fabric_loopback(&base, &paired.value.credential);
    let capabilities: Envelope<Capabilities> = client.capabilities().await.unwrap();
    assert_eq!(
        capabilities.value.transport,
        st3_client::TransportKind::FabricLoopback
    );
    assert_eq!(capabilities.value.session_actor, paired.value.session_actor);
    assert_eq!(
        client
            .list("operations", None, None, false)
            .await
            .unwrap()
            .value
            .collection,
        "operations"
    );

    let denied = http
        .get(format!("{base}/v1/client/capabilities"))
        .bearer_auth("not-a-real-client-credential")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    let error: st3_client::ErrorEnvelope = denied.json().await.unwrap();
    assert_eq!(error.code, st3_client::ErrorCode::Forbidden);
    server.abort();
}
