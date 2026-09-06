use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::sync::{Notify, watch};

use crate::config::PeerConfig;
use crate::model::{ApiResponse, ReplicationBatch, ReplicationQuery, ReplicationResponse};
use crate::store::Store;

pub fn start(
    store: Arc<Store>,
    node: String,
    peers: Vec<PeerConfig>,
    notify: Arc<Notify>,
    event_notify: watch::Sender<u64>,
) {
    for peer in peers {
        let store = store.clone();
        let node = node.clone();
        let notify = notify.clone();
        let event_notify = event_notify.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            let mut transport_status: Option<&str> = None;
            loop {
                match exchange(&store, &node, &peer).await {
                    Ok(changed) => {
                        let recovered = transport_status != Some("up");
                        if recovered {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            let _ = store.append_claim(&crate::model::ClaimInput {
                                subject: format!("host/{}", peer.name),
                                kind: "transport.observed".into(),
                                actor: None,
                                fields: std::collections::BTreeMap::from([
                                    ("status".into(), serde_json::Value::String("up".into())),
                                    (
                                        "protocol".into(),
                                        serde_json::Value::String("http-replication".into()),
                                    ),
                                    ("last_success_at".into(), serde_json::Value::from(now)),
                                ]),
                                evidence: Vec::new(),
                                expected_subject: None,
                                idempotency_key: None,
                            });
                            transport_status = Some("up");
                        }
                        if changed || recovered {
                            notify.notify_one();
                            event_notify.send_modify(|generation| {
                                *generation = generation.saturating_add(1)
                            });
                        }
                        backoff = Duration::from_secs(1);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(error) => {
                        if transport_status != Some("down") {
                            let fields = std::collections::BTreeMap::from([
                                ("status".into(), serde_json::Value::String("down".into())),
                                (
                                    "protocol".into(),
                                    serde_json::Value::String("http-replication".into()),
                                ),
                                (
                                    "reason".into(),
                                    serde_json::Value::String(error.to_string()),
                                ),
                            ]);
                            let _ = store.append_claim(&crate::model::ClaimInput {
                                subject: format!("host/{}", peer.name),
                                kind: "transport.observed".into(),
                                actor: None,
                                fields,
                                evidence: Vec::new(),
                                expected_subject: None,
                                idempotency_key: None,
                            });
                            transport_status = Some("down");
                            notify.notify_one();
                            event_notify.send_modify(|generation| {
                                *generation = generation.saturating_add(1)
                            });
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        });
    }
}

async fn exchange(store: &Store, node: &str, peer: &PeerConfig) -> Result<bool> {
    let client = reqwest::Client::new();
    let local_heads = store.replica_heads()?;
    let remote_batches = client
        .post(format!(
            "{}/v1/peer/claims/query",
            peer.url.trim_end_matches('/')
        ))
        .header("x-st3-peer", node)
        .json(&ReplicationQuery {
            replica_heads: local_heads,
        })
        .send()
        .await
        .with_context(|| format!("connect to peer {}", peer.name))?
        .error_for_status()?
        .json::<ApiResponse<ReplicationBatch>>()
        .await?
        .value;
    let pulled = !remote_batches.batches.is_empty();
    if pulled {
        store
            .import_replication(&peer.name, &remote_batches)
            .map_err(anyhow::Error::from)?;
    }

    let local_batches = store.export_replication_for_heads(&remote_batches.replica_heads)?;
    let pushed = !local_batches.batches.is_empty();
    if pushed {
        client
            .post(format!("{}/v1/peer/claims", peer.url.trim_end_matches('/')))
            .header("x-st3-peer", node)
            .json(&local_batches)
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse<ReplicationResponse>>()
            .await?;
    }
    Ok(pulled || pushed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{AppState, router, serve_tcp};
    use crate::model::ClaimInput;
    use std::collections::{BTreeMap, BTreeSet};

    fn free_loopback_address() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    fn state(store: Arc<Store>, node: &str, peer: &str, root: &std::path::Path) -> AppState {
        AppState {
            store,
            notify: Arc::new(Notify::new()),
            event_notify: watch::channel(0_u64).0,
            node: node.into(),
            state_dir: root.join(node),
            pty_root: root.join(format!("{node}-pty")),
            trusted_peers: BTreeSet::from([peer.into()]),
        }
    }

    async fn wait_ready(address: &str) {
        let url = format!("http://{address}/v1/health");
        for _ in 0..100 {
            if reqwest::get(&url).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the peer test server at {address} did not start");
    }

    #[tokio::test]
    async fn two_loopback_daemons_replicate_claims_in_both_directions() {
        let root = tempfile::tempdir().unwrap();
        let store_a = Arc::new(Store::open(&root.path().join("a.sqlite3"), "node-a").unwrap());
        let store_b = Arc::new(Store::open(&root.path().join("b.sqlite3"), "node-b").unwrap());
        let address_a = free_loopback_address();
        let address_b = free_loopback_address();
        let state_a = router(state(store_a.clone(), "node-a", "node-b", root.path()));
        let state_b = router(state(store_b.clone(), "node-b", "node-a", root.path()));
        let server_address_a = address_a.clone();
        let server_address_b = address_b.clone();
        let server_a = tokio::spawn(async move { serve_tcp(&server_address_a, state_a).await });
        let server_b = tokio::spawn(async move { serve_tcp(&server_address_b, state_b).await });
        wait_ready(&address_a).await;
        wait_ready(&address_b).await;

        store_a
            .append_claim(&ClaimInput {
                subject: "custom/test/from-a".into(),
                kind: "custom.test.observed".into(),
                actor: Some("person/test".into()),
                fields: BTreeMap::new(),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("from-a".into()),
            })
            .unwrap();
        exchange(
            &store_a,
            "node-a",
            &PeerConfig {
                name: "node-b".into(),
                url: format!("http://{address_b}"),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            store_b
                .claims_for("custom/test/from-a", None)
                .unwrap()
                .len(),
            1
        );

        store_b
            .append_claim(&ClaimInput {
                subject: "custom/test/from-b".into(),
                kind: "custom.test.observed".into(),
                actor: Some("person/test".into()),
                fields: BTreeMap::new(),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("from-b".into()),
            })
            .unwrap();
        exchange(
            &store_b,
            "node-b",
            &PeerConfig {
                name: "node-a".into(),
                url: format!("http://{address_a}"),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            store_a
                .claims_for("custom/test/from-b", None)
                .unwrap()
                .len(),
            1
        );

        server_a.abort();
        server_b.abort();
    }
}
