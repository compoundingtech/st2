//! Durable guard for changes to a catalog's effective PTY registry.
//!
//! A PTY root partitions the observable session universe. If a replacement supervisor starts with
//! a different root, an exact declared task still alive in the previous root looks absent and would
//! be launched a second time. Keep the last effective root per `(catalog, host)`, inspect that known
//! prior registry on a transition, and fail closed before reconciliation while any exact declared
//! PTY task survives there. This guard never kills or adopts sessions.

use std::collections::BTreeSet;
use std::fs;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::reconcile::Session;
use crate::spec::{AgentSpec, TaskKind};

const RECEIPT_VERSION: u32 = 1;
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PtyRootReceipt {
    version: u32,
    pty_root: PathBuf,
}

/// Durable root-selection receipt for one catalog host.
pub fn receipt_path(catalog_root: &Path, host: &str) -> PathBuf {
    catalog_root.join(format!(".st2.{host}.pty-root.json"))
}

/// Establish or verify the effective PTY root before a catalog reconcile pass.
///
/// A changed root is accepted only after the known previous registry can be inspected and contains
/// no live session whose id exactly matches a declared local PTY task. Unknown and prefix-related
/// sessions are left untouched and do not block the transition.
pub fn ensure_safe_transition(
    catalog_root: &Path,
    host: &str,
    effective_root: &Path,
    specs: &[AgentSpec],
    discovery_complete: bool,
    inspect: impl FnOnce(&Path) -> anyhow::Result<Vec<Session>>,
) -> anyhow::Result<()> {
    let receipt = receipt_path(catalog_root, host);
    let requested = normalized_root(effective_root)?;
    let previous = match fs::read(&receipt) {
        Ok(bytes) => {
            let parsed: PtyRootReceipt = serde_json::from_slice(&bytes)
                .with_context(|| format!("reading PTY-root receipt {}", receipt.display()))?;
            if parsed.version != RECEIPT_VERSION {
                anyhow::bail!(
                    "unsupported PTY-root receipt version {} in {} (expected {}); refusing to reconcile",
                    parsed.version,
                    receipt.display(),
                    RECEIPT_VERSION
                );
            }
            normalized_root(&parsed.pty_root)?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            write_receipt(&receipt, &requested)?;
            return Ok(());
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading PTY-root receipt {}", receipt.display()));
        }
    };

    if previous == requested {
        return Ok(());
    }

    if !discovery_complete {
        anyhow::bail!(
            "effective PTY root changed for catalog host '{host}', but agent discovery was incomplete; \
             refusing to reconcile or advance the PTY-root receipt\n  previous: {}\n  requested: {}\n  \
             receipt: {}\nFix every discovery error before retrying. st2 has not killed, adopted, or \
             launched any task.",
            previous.display(),
            requested.display(),
            receipt.display()
        );
    }

    let sessions = inspect(&previous).with_context(|| {
        format!(
            "effective PTY root changed for catalog host '{host}', but the previous registry could \
             not be inspected; refusing to reconcile\n  previous: {}\n  requested: {}\n  receipt: \
             {}\nRestore the previous PTY_ROOT/catalog declaration, or make that registry inspectable \
             before retrying. st2 has not killed, adopted, or launched any task.",
            previous.display(),
            requested.display(),
            receipt.display()
        )
    })?;
    let declared = declared_local_pty_ids(specs, host);
    let conflicts = sessions
        .iter()
        .filter(|session| session.alive && declared.contains(&session.pty_id))
        .map(|session| session.pty_id.clone())
        .collect::<BTreeSet<_>>();

    if !conflicts.is_empty() {
        anyhow::bail!(
            "effective PTY root changed for catalog host '{host}' while declared tasks still survive \
             in the previous registry; refusing to reconcile\n  previous: {}\n  requested: {}\n  \
             live exact task ids: {}\n  receipt: {}\nInspect the previous registry with \
             `PTY_ROOT={} pty list`. Restore the previous root to adopt the survivors, or explicitly \
             stop/migrate those exact tasks before retrying. st2 has not killed, adopted, or launched \
             any task.",
            previous.display(),
            requested.display(),
            conflicts.into_iter().collect::<Vec<_>>().join(", "),
            receipt.display(),
            previous.display()
        );
    }

    write_receipt(&receipt, &requested)
}

fn declared_local_pty_ids(specs: &[AgentSpec], host: &str) -> BTreeSet<String> {
    specs
        .iter()
        .filter(|spec| spec.resolved_host(host) == host)
        .flat_map(|spec| {
            let bus_id = spec.bus_id(host);
            spec.tasks
                .iter()
                .filter(|task| task.kind == TaskKind::Pty)
                .map(move |task| {
                    task.id
                        .clone()
                        .unwrap_or_else(|| format!("{bus_id}.{}", task.name))
                })
        })
        .collect()
}

fn normalized_root(path: &Path) -> anyhow::Result<PathBuf> {
    if let Ok(canonical) = path.canonicalize() {
        return Ok(canonical);
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    Ok(normalized)
}

fn write_receipt(path: &Path, root: &Path) -> anyhow::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".pty-root.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let bytes = serde_json::to_vec_pretty(&PtyRootReceipt {
        version: RECEIPT_VERSION,
        pty_root: root.to_path_buf(),
    })?;
    let result = (|| -> anyhow::Result<()> {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result.with_context(|| format!("atomically writing PTY-root receipt {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{JobType, Task};

    fn specs() -> Vec<AgentSpec> {
        vec![AgentSpec {
            identity: "seat".into(),
            host: Some("h".into()),
            role: None,
            job_type: JobType::Service,
            workspace: None,
            supervisor: None,
            retired: false,
            keep: false,
            restart: None,
            tasks: vec![
                Task {
                    name: "agent".into(),
                    kind: TaskKind::Pty,
                    derived: false,
                    id: None,
                    command: Some("true".into()),
                    cwd: None,
                    tags: Default::default(),
                    env: Default::default(),
                    keep: false,
                },
                Task {
                    name: "worker".into(),
                    id: Some("h.seat.child".into()),
                    kind: TaskKind::Pty,
                    derived: false,
                    command: Some("true".into()),
                    cwd: None,
                    tags: Default::default(),
                    env: Default::default(),
                    keep: false,
                },
                Task {
                    name: "sidecar".into(),
                    kind: TaskKind::Exec,
                    derived: false,
                    id: None,
                    command: Some("true".into()),
                    cwd: None,
                    tags: Default::default(),
                    env: Default::default(),
                    keep: false,
                },
            ],
            path: PathBuf::from("/tmp/agent.kdl"),
        }]
    }

    fn live(id: &str) -> Session {
        Session {
            pty_id: id.into(),
            alive: true,
            exit_code: None,
        }
    }

    #[test]
    fn first_root_persists_and_same_root_never_probes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("a");
        ensure_safe_transition(tmp.path(), "h", &root, &specs(), true, |_| {
            panic!("first selection must not probe another registry")
        })
        .unwrap();
        ensure_safe_transition(tmp.path(), "h", &root, &specs(), true, |_| {
            panic!("same-root adoption must not probe another registry")
        })
        .unwrap();
        let receipt: PtyRootReceipt =
            serde_json::from_slice(&fs::read(receipt_path(tmp.path(), "h")).unwrap()).unwrap();
        assert_eq!(receipt.pty_root, root);
    }

    #[test]
    fn both_transition_directions_refuse_exact_and_explicit_child_survivors() {
        for (from, to, survivor) in [("a", "b", "h.seat.agent"), ("b", "a", "h.seat.child")] {
            let tmp = tempfile::tempdir().unwrap();
            let from = tmp.path().join(from);
            let to = tmp.path().join(to);
            ensure_safe_transition(tmp.path(), "h", &from, &specs(), true, |_| unreachable!())
                .unwrap();
            let error = ensure_safe_transition(tmp.path(), "h", &to, &specs(), true, |probed| {
                assert_eq!(probed, from);
                Ok(vec![live(survivor)])
            })
            .unwrap_err()
            .to_string();
            assert!(error.contains(survivor), "{error}");
            assert!(
                error.contains("has not killed, adopted, or launched"),
                "{error}"
            );

            let receipt: PtyRootReceipt =
                serde_json::from_slice(&fs::read(receipt_path(tmp.path(), "h")).unwrap()).unwrap();
            assert_eq!(
                receipt.pty_root, from,
                "a refused transition must survive restart"
            );
        }
    }

    #[test]
    fn sibling_prefix_and_unknown_sessions_do_not_block_a_clean_transition() {
        let tmp = tempfile::tempdir().unwrap();
        let from = tmp.path().join("a");
        let to = tmp.path().join("b");
        ensure_safe_transition(tmp.path(), "h", &from, &specs(), true, |_| unreachable!()).unwrap();
        ensure_safe_transition(tmp.path(), "h", &to, &specs(), true, |probed| {
            assert_eq!(probed, from);
            Ok(vec![live("h.seat.agent-sibling"), live("h.other.agent")])
        })
        .unwrap();

        let receipt: PtyRootReceipt =
            serde_json::from_slice(&fs::read(receipt_path(tmp.path(), "h")).unwrap()).unwrap();
        assert_eq!(receipt.pty_root, to);
    }

    #[test]
    fn incomplete_discovery_refuses_a_changed_root_without_probing_or_advancing() {
        let tmp = tempfile::tempdir().unwrap();
        let from = tmp.path().join("a");
        let to = tmp.path().join("b");
        ensure_safe_transition(tmp.path(), "h", &from, &specs(), true, |_| unreachable!()).unwrap();

        let error = ensure_safe_transition(tmp.path(), "h", &to, &specs(), false, |_| {
            panic!("incomplete declarations must fail before inspecting or advancing")
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("discovery was incomplete"), "{error}");
        assert!(error.contains("has not killed, adopted, or launched"), "{error}");
        let receipt: PtyRootReceipt =
            serde_json::from_slice(&fs::read(receipt_path(tmp.path(), "h")).unwrap()).unwrap();
        assert_eq!(receipt.pty_root, from);
    }

    #[test]
    fn unreadable_previous_registry_fails_closed_without_advancing() {
        let tmp = tempfile::tempdir().unwrap();
        let from = tmp.path().join("a");
        let to = tmp.path().join("b");
        ensure_safe_transition(tmp.path(), "h", &from, &specs(), true, |_| unreachable!()).unwrap();
        let error = ensure_safe_transition(tmp.path(), "h", &to, &specs(), true, |_| {
            anyhow::bail!("synthetic list failure")
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("could not be inspected"), "{error}");

        let receipt: PtyRootReceipt =
            serde_json::from_slice(&fs::read(receipt_path(tmp.path(), "h")).unwrap()).unwrap();
        assert_eq!(receipt.pty_root, from);
    }
}
