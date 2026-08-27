use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::sync::Notify;

use crate::config::PeerConfig;
use crate::model::{ApiResponse, ReplicationBatch, ReplicationQuery, ReplicationResponse};
use crate::store::Store;

pub fn start(
    store: Arc<Store>,
    node: String,
    peers: Vec<PeerConfig>,
    notify: Arc<Notify>,
    event_notify: Arc<Notify>,
) {
    for peer in peers {
        let store = store.clone();
        let node = node.clone();
        let notify = notify.clone();
        let event_notify = event_notify.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match exchange(&store, &node, &peer).await {
                    Ok(changed) => {
                        if changed {
                            notify.notify_one();
                            event_notify.notify_waiters();
                        }
                        backoff = Duration::from_secs(1);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(error) => {
                        let fields = std::collections::BTreeMap::from([
                            ("status".into(), serde_json::Value::String("down".into())),
                            (
                                "reason".into(),
                                serde_json::Value::String(error.to_string()),
                            ),
                        ]);
                        let _ = store.append_claim(&crate::model::ClaimInput {
                            subject: format!("host/{}", peer.name),
                            kind: "transport.peer".into(),
                            actor: None,
                            fields,
                            evidence: Vec::new(),
                            expected_subject: None,
                            idempotency_key: None,
                        });
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
