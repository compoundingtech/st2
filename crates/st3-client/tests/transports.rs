use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;

use serde_json::Value;
use st3::api::AppState;
use st3::model::{AttentionRequest, ClaimInput};
use st3::store::Store;
use st3_client::{
    AttentionResolveParameters, Capabilities, Client, Envelope, Fence, LaunchVariantParameters,
    PairingBegin, PairingComplete, Resource, TargetParameters, TerminalAttachment,
};
use tokio::sync::{Notify, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

fn state(root: &Path, name: &str) -> AppState {
    let pty_binary = root.join(format!("{name}-fake-pty"));
    std::fs::write(
        &pty_binary,
        "#!/bin/sh\nif [ \"$1\" = peek ]; then printf 'terminal ready\\n$ '; exit 0; fi\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&pty_binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    AppState {
        store: Arc::new(Store::open_memory(name).unwrap()),
        notify: Arc::new(Notify::new()),
        event_notify: watch::channel(0_u64).0,
        node: name.into(),
        state_dir: root.to_path_buf(),
        pty_root: root.join("pty"),
        pty_binary,
        fleet_id: None,
        configured_peers: Vec::new(),
    }
}

fn publish_terminal(state: &AppState, incarnation: &str) {
    state
        .store
        .append_claim(&ClaimInput {
            subject: "agent/terminal-demo".into(),
            kind: "runtime.observed".into(),
            actor: Some("agent/terminal-demo".into()),
            fields: BTreeMap::from([
                (
                    "runtime_id".into(),
                    Value::String("terminal-demo-runtime".into()),
                ),
                ("incarnation_id".into(), Value::String(incarnation.into())),
                ("status".into(), Value::String("running".into())),
                ("terminal".into(), Value::Bool(true)),
                ("reachability".into(), Value::String("local".into())),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })
        .unwrap();
}

async fn wait_for_socket(socket: &Path) {
    for _ in 0..100 {
        if socket.exists() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("Unix server socket {} did not appear", socket.display());
}

async fn attach_terminal(client: &Client, suffix: &str) -> TerminalAttachment {
    let runtimes = client.runtimes_list(None, None, false).await.unwrap();
    let runtime = runtimes
        .value
        .items
        .iter()
        .find_map(|resource| match resource {
            Resource::Runtime(runtime) if runtime.terminal_id.is_some() => Some(runtime),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "terminal-capable runtime advertised by the server: {:?}",
                runtimes.value.items
            )
        });
    let terminal_id = runtime.terminal_id.as_deref().unwrap();
    let incarnation = runtime.incarnation_id.as_deref().unwrap();
    let terminal_sequence = runtime.terminal_sequence.unwrap();
    assert_eq!(runtime.terminal_access.as_ref().unwrap().read, "granted");
    client
        .terminal_attach(
            format!("action/terminal-attach-{suffix}"),
            format!("terminal-attach-{suffix}-00000001"),
            Fence {
                snapshot_id: runtimes.snapshot.id,
                runtime_incarnation: Some(incarnation.into()),
                terminal_sequence: Some(terminal_sequence),
                ..Fence::default()
            },
            TargetParameters {
                target_id: terminal_id.into(),
                ..TargetParameters::default()
            },
        )
        .await
        .unwrap()
        .value
        .terminal_attachment
        .expect("terminal.attach result")
}

async fn detach_terminal(client: &Client, attachment: &TerminalAttachment, suffix: &str) {
    let capabilities = client.capabilities().await.unwrap();
    client
        .terminal_detach(
            format!("action/terminal-detach-{suffix}"),
            format!("terminal-detach-{suffix}-0000001"),
            Fence {
                snapshot_id: capabilities.snapshot.id,
                runtime_incarnation: Some(attachment.runtime_incarnation.clone()),
                terminal_sequence: Some(capabilities.snapshot.store_index),
                ..Fence::default()
            },
            TargetParameters {
                target_id: attachment.attachment_id.clone(),
                ..TargetParameters::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn generated_client_conforms_over_the_real_unix_transport() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("st3.sock");
    let server_socket = socket.clone();
    let state = state(root.path(), "client-unix");
    publish_terminal(&state, "terminal-demo-runtime:i1");
    state
        .store
        .request_attention(
            "attention/nathan-client-proof",
            &AttentionRequest {
                reviewer: "person/nathan".into(),
                title: "Client proof".into(),
                reason: "Resolve through the generated client".into(),
                severity: "error".into(),
                targets: vec!["agent/terminal-demo".into()],
                actor: "daemon/runtime".into(),
                idempotency_key: "attention-nathan-client-proof".into(),
            },
        )
        .unwrap();
    state
        .store
        .request_attention(
            "attention/alex-client-proof",
            &AttentionRequest {
                reviewer: "person/alex".into(),
                title: "Other person's attention".into(),
                reason: "Must not resolve as Nathan".into(),
                severity: "warning".into(),
                targets: Vec::new(),
                actor: "daemon/runtime".into(),
                idempotency_key: "attention-alex-client-proof".into(),
            },
        )
        .unwrap();
    let app = st3::api::router(state.clone());
    let server = tokio::spawn(async move { st3::api::serve_unix(&server_socket, app).await });
    wait_for_socket(&socket).await;

    let client = Client::unix_as(&socket, "person/nathan");
    let read_only = Client::unix(&socket);
    assert_eq!(
        read_only.capabilities().await.unwrap().value.session_actor,
        "client/local/read-only"
    );
    assert!(
        read_only
            .pairing_begin(&PairingBegin {
                api_version: st3_client::API_VERSION.into(),
                device_name: "Unattributed device".into(),
                person_id: "person/nathan".into(),
            })
            .await
            .is_err(),
        "plain Unix clients cannot infer pairing authority"
    );
    let capabilities = client.capabilities().await.unwrap();
    assert_eq!(
        capabilities.value.transport,
        st3_client::TransportKind::Unix
    );
    assert_eq!(capabilities.value.session_actor, "person/nathan");
    let work = client.work_list(None, Some(7), false).await.unwrap();
    assert_eq!(work.value.collection, "work");
    assert_eq!(work.value.page.limit, 7);
    let events = client.events(None, Some(10), Some(0)).await.unwrap();
    assert!(!events.value.has_more);

    let attention_page = client.attention_list(None, None, false).await.unwrap();
    let attention = attention_page
        .value
        .items
        .iter()
        .find_map(|resource| match resource {
            Resource::Attention(attention)
                if attention.header.id == "attention/nathan-client-proof" =>
            {
                Some(attention)
            }
            _ => None,
        })
        .expect("person/nathan attention in generated client page");
    let resolve_fence = Fence {
        snapshot_id: attention_page.snapshot.id.clone(),
        subject_revisions: BTreeMap::from([(
            attention.header.id.clone(),
            attention.header.revision.clone(),
        )]),
        ..Fence::default()
    };
    let resolve_parameters = AttentionResolveParameters {
        attention_id: attention.header.id.clone(),
        outcome: "resolved".into(),
        reason: Some("generated client proof".into()),
    };
    assert!(
        read_only
            .attention_resolve(
                "action/attention-resolve-read-only",
                "attention-resolve-read-only-0001",
                resolve_fence.clone(),
                resolve_parameters.clone(),
            )
            .await
            .is_err(),
        "plain Unix clients cannot resolve person attention"
    );
    assert!(
        read_only
            .launch_approve(
                "action/launch-approve-without-person",
                "launch-approve-no-person-0001",
                Fence {
                    snapshot_id: attention_page.snapshot.id.clone(),
                    ..Fence::default()
                },
                LaunchVariantParameters {
                    launch_id: "launch/example".into(),
                    variant_id: "launch-variant/example/default".into(),
                },
            )
            .await
            .is_err(),
        "plain Unix clients cannot approve launches"
    );
    client
        .attention_resolve(
            "action/attention-resolve-generated-client",
            "attention-resolve-client-0001",
            resolve_fence,
            resolve_parameters,
        )
        .await
        .unwrap();
    assert!(
        state
            .store
            .attention_items(Some("person/nathan"))
            .unwrap()
            .is_empty()
    );
    let alex_page = client.attention_list(None, None, false).await.unwrap();
    let alex = alex_page
        .value
        .items
        .iter()
        .find_map(|resource| match resource {
            Resource::Attention(attention)
                if attention.header.id == "attention/alex-client-proof" =>
            {
                Some(attention)
            }
            _ => None,
        })
        .unwrap();
    assert!(
        client
            .attention_resolve(
                "action/attention-resolve-cross-person",
                "attention-resolve-cross-0001",
                Fence {
                    snapshot_id: alex_page.snapshot.id,
                    subject_revisions: BTreeMap::from([(
                        alex.header.id.clone(),
                        alex.header.revision.clone(),
                    )]),
                    ..Fence::default()
                },
                AttentionResolveParameters {
                    attention_id: alex.header.id.clone(),
                    outcome: "resolved".into(),
                    reason: None,
                },
            )
            .await
            .is_err(),
        "person/nathan cannot resolve person/alex attention"
    );

    let first_attachment = attach_terminal(&client, "unix-first").await;
    let first = client
        .terminal_frames(
            &first_attachment.terminal_id,
            None,
            Some("terminal-demo-runtime:i1"),
            first_attachment.stream_capability.as_deref().unwrap(),
            Some(1_000),
        )
        .await
        .unwrap();
    assert_eq!(
        first.screen.value.runtime_incarnation,
        "terminal-demo-runtime:i1"
    );
    assert_eq!(first.screen.value.lines[0].text, "terminal ready");
    let frames = first.frames.expect("bounded initial frame page");
    assert_eq!(frames.value.frames.len(), 1);
    assert_eq!(
        frames.value.frames[0].frame_type,
        st3_client::TerminalFrameType::Resync
    );
    assert_eq!(
        frames.value.frames[0].runtime_incarnation,
        "terminal-demo-runtime:i1"
    );

    assert!(
        client
            .terminal_frames(
                &first_attachment.terminal_id,
                None,
                Some("terminal-demo-runtime:wrong"),
                attach_terminal(&client, "unix-wrong")
                    .await
                    .stream_capability
                    .as_deref()
                    .unwrap(),
                Some(100),
            )
            .await
            .is_err(),
        "a stale incarnation must be rejected during the WebSocket handshake"
    );

    let stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
    let mut wrong_protocol = "ws://localhost/v1/client/terminals/agent/terminal-demo/stream"
        .into_client_request()
        .unwrap();
    wrong_protocol.headers_mut().insert(
        "sec-websocket-protocol",
        "wrong.terminal.protocol".parse().unwrap(),
    );
    assert!(
        tokio_tungstenite::client_async(wrong_protocol, stream)
            .await
            .is_err(),
        "the Unix WebSocket must reject the wrong subprotocol"
    );

    let replacement_fence = attach_terminal(&client, "unix-replaced").await;
    publish_terminal(&state, "terminal-demo-runtime:i2");
    assert!(
        client
            .terminal_frames(
                &replacement_fence.terminal_id,
                None,
                Some("terminal-demo-runtime:i1"),
                replacement_fence.stream_capability.as_deref().unwrap(),
                Some(100),
            )
            .await
            .is_err(),
        "reconnect to a replaced incarnation must reject the old fence"
    );
    let second_attachment = attach_terminal(&client, "unix-second").await;
    let replacement = client
        .terminal_frames(
            &second_attachment.terminal_id,
            None,
            Some("terminal-demo-runtime:i2"),
            second_attachment.stream_capability.as_deref().unwrap(),
            Some(1_000),
        )
        .await
        .unwrap();
    assert_eq!(
        replacement.screen.value.runtime_incarnation,
        "terminal-demo-runtime:i2"
    );
    assert!(
        client
            .terminal_frames(
                &second_attachment.terminal_id,
                None,
                Some("terminal-demo-runtime:i2"),
                second_attachment.stream_capability.as_deref().unwrap(),
                Some(100),
            )
            .await
            .is_err(),
        "a stream capability must be single use"
    );
    let detached = attach_terminal(&client, "unix-detach").await;
    detach_terminal(&client, &detached, "unix-first").await;
    detach_terminal(&client, &detached, "unix-repeat").await;
    assert!(
        client
            .terminal_frames(
                &detached.terminal_id,
                None,
                Some("terminal-demo-runtime:i2"),
                detached.stream_capability.as_deref().unwrap(),
                Some(100),
            )
            .await
            .is_err(),
        "an idempotently detached viewer must not reconnect"
    );
    server.abort();
}

#[tokio::test]
async fn generated_client_conforms_over_paired_loopback_and_rejects_bad_credentials() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path(), "client-loopback");
    publish_terminal(&state, "terminal-demo-runtime:fabric-i1");

    let socket = root.path().join("st3.sock");
    let server_socket = socket.clone();
    let unix_app = st3::api::router(state.clone());
    let unix_server =
        tokio::spawn(async move { st3::api::serve_unix(&server_socket, unix_app).await });
    wait_for_socket(&socket).await;
    let local = Client::unix_as(&socket, "person/nathan");
    let challenge = local
        .pairing_begin(&PairingBegin {
            api_version: st3_client::API_VERSION.into(),
            device_name: "Conformance phone".into(),
            person_id: "person/nathan".into(),
        })
        .await
        .unwrap();
    let paired = local
        .pairing_complete(
            &challenge.value.pairing_id,
            &PairingComplete {
                api_version: st3_client::API_VERSION.into(),
                code: challenge.value.code,
                device_public_key: "conformance-public-key-000000000000000000000000".into(),
            },
        )
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = st3::api::fabric_router(state);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{address}");
    let http = reqwest::Client::new();

    let client = Client::fabric_loopback(&base, &paired.value.credential);
    let capabilities: Envelope<Capabilities> = client.capabilities().await.unwrap();
    assert_eq!(
        capabilities.value.transport,
        st3_client::TransportKind::FabricLoopback
    );
    assert_eq!(capabilities.value.session_actor, paired.value.session_actor);
    assert_eq!(paired.value.person_id, "person/nathan");
    assert_eq!(
        client
            .operations_list(None, None, false)
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

    let fabric_attachment = attach_terminal(&client, "fabric-first").await;
    let terminal = client
        .terminal_frames(
            &fabric_attachment.terminal_id,
            None,
            Some("terminal-demo-runtime:fabric-i1"),
            fabric_attachment.stream_capability.as_deref().unwrap(),
            Some(1_000),
        )
        .await
        .unwrap();
    assert_eq!(
        terminal.screen.value.runtime_incarnation,
        "terminal-demo-runtime:fabric-i1"
    );
    let bad_attachment = "not-a-real-stream-capability-0000000000";
    assert!(
        Client::fabric_loopback(&base, "not-a-real-client-credential")
            .terminal_frames(
                &fabric_attachment.terminal_id,
                None,
                Some("terminal-demo-runtime:fabric-i1"),
                bad_attachment,
                Some(100),
            )
            .await
            .is_err(),
        "the Fabric WebSocket must authenticate before upgrade"
    );

    let mut wrong_protocol = format!(
        "ws://{address}/v1/client/terminals/agent%2Fterminal-demo/stream?incarnation=terminal-demo-runtime%3Afabric-i1"
    )
    .into_client_request()
    .unwrap();
    wrong_protocol.headers_mut().insert(
        "sec-websocket-protocol",
        "wrong.terminal.protocol".parse().unwrap(),
    );
    wrong_protocol.headers_mut().insert(
        "authorization",
        format!("Bearer {}", paired.value.credential)
            .parse()
            .unwrap(),
    );
    assert!(
        tokio_tungstenite::connect_async(wrong_protocol)
            .await
            .is_err(),
        "the authenticated Fabric WebSocket must reject the wrong subprotocol"
    );

    let revoked_attachment = attach_terminal(&client, "fabric-revoked").await;
    let local_capabilities = local.capabilities().await.unwrap();
    local
        .pairing_revoke(
            "action/pairing-revoke-fabric",
            "pairing-revoke-fabric-000001",
            Fence {
                snapshot_id: local_capabilities.snapshot.id,
                ..Fence::default()
            },
            TargetParameters {
                target_id: paired.value.device_id.clone(),
                ..TargetParameters::default()
            },
        )
        .await
        .unwrap();
    assert!(
        client
            .terminal_frames(
                &revoked_attachment.terminal_id,
                None,
                Some("terminal-demo-runtime:fabric-i1"),
                revoked_attachment.stream_capability.as_deref().unwrap(),
                Some(100),
            )
            .await
            .is_err(),
        "credential revocation must invalidate an unused stream capability"
    );

    server.abort();
    unix_server.abort();
}
