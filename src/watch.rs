//! Filesystem watch filtering shared by the supervisor and DING sidecars.
//!
//! Linux `notify` subscribes to inotify `OPEN` events and reports them as [`EventKind::Access`].
//! Forwarding those events makes a reader self-wake forever: each pass reads its watched tree, that
//! read emits another event, and the next timed wait returns immediately. Only mutations are useful
//! wakeups; timer polling remains the fallback for anything a backend cannot classify.

use std::path::Path;
use std::sync::mpsc::Sender;

use agent_spec::discovery::is_catalog_path_excluded;
use notify::{Event, EventKind, RecursiveMode, Watcher};

/// Watch a directory recursively, forwarding only events that can change reconciled state.
pub(crate) fn watch_recursive_mutations(
    dir: &Path,
    tx: Sender<()>,
) -> Option<notify::RecommendedWatcher> {
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<Event>| {
        if result.is_ok_and(|event| is_mutation(&event)) {
            let _ = tx.send(());
        }
    })
    .ok()?;
    watcher.watch(dir, RecursiveMode::Recursive).ok()?;
    Some(watcher)
}

/// Watch only declaration inputs for the supervisor. Runtime state (PTY registry, bus, logs,
/// locks, inboxes, and generated materializations) must never wake reconciliation.
pub(crate) fn watch_catalog_declarations(
    root: &Path,
    tx: Sender<()>,
) -> Option<notify::RecommendedWatcher> {
    let root = root.to_path_buf();
    let callback_root = root.clone();
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<Event>| {
        if result.is_ok_and(|event| {
            is_mutation(&event)
                && event
                    .paths
                    .iter()
                    .any(|path| is_declaration_path(&callback_root, path))
        }) {
            let _ = tx.send(());
        }
    })
    .ok()?;
    watcher.watch(&root, RecursiveMode::Recursive).ok()?;
    Some(watcher)
}

fn is_declaration_path(root: &Path, path: &Path) -> bool {
    let rel = path.strip_prefix(root).unwrap_or(path);
    let mut components = rel.components();
    if matches!(
        components.next().and_then(|c| c.as_os_str().to_str()),
        Some("_templates")
    ) {
        return true;
    }
    if is_catalog_path_excluded(root, path) {
        return false;
    }
    path.file_name().and_then(|n| n.to_str()) == Some("agent.kdl")
}

fn is_mutation(event: &Event) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{
        AccessKind, AccessMode, CreateKind, DataChange, ModifyKind, RemoveKind, RenameMode,
    };

    #[test]
    fn only_mutations_wake_watch_loops() {
        for kind in [
            EventKind::Create(CreateKind::File),
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
            EventKind::Remove(RemoveKind::File),
        ] {
            assert!(is_mutation(&Event::new(kind)), "{kind:?} must wake");
        }

        for kind in [
            EventKind::Access(AccessKind::Open(AccessMode::Read)),
            EventKind::Access(AccessKind::Close(AccessMode::Read)),
            EventKind::Any,
            EventKind::Other,
        ] {
            assert!(!is_mutation(&Event::new(kind)), "{kind:?} must stay quiet");
        }
    }

    #[test]
    fn declaration_filter_ignores_runtime_state() {
        let root = Path::new("/catalog");
        assert!(is_declaration_path(
            root,
            Path::new("/catalog/team/agent.kdl")
        ));
        assert!(is_declaration_path(
            root,
            Path::new("/catalog/agents/.retired/hetz/old/agent.kdl")
        ));
        assert!(is_declaration_path(
            root,
            Path::new("/catalog/_templates/base.kdl")
        ));
        assert!(is_declaration_path(
            root,
            Path::new("/catalog/_templates/resources/base.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            Path::new("/catalog/.git/agents/hetz/git-state/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            Path::new("/catalog/.st2/agents/hetz/runner-state/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            Path::new("/catalog/agents/hetz/worker/resources/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            Path::new("/catalog/pty/session.json")
        ));
        assert!(!is_declaration_path(
            root,
            Path::new("/catalog/bus/inbox/msg")
        ));
        assert!(!is_declaration_path(
            root,
            Path::new("/catalog/team/rendered.kdl")
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reads_are_silent_but_real_mutations_wake() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("watched");
        std::fs::write(&file, "before").unwrap();

        let (tx, rx) = channel();
        let _watcher = watch_recursive_mutations(dir.path(), tx).expect("start inotify watcher");

        for _ in 0..1_000 {
            assert_eq!(std::fs::read_to_string(&file).unwrap(), "before");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "OPEN/read access must not wake a mutation watcher"
        );

        std::fs::write(&file, "after").unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("content mutation must wake");

        fn assert_wakes(setup: fn(&Path), mutate: fn(&Path)) {
            let dir = tempfile::tempdir().unwrap();
            setup(dir.path());
            let (tx, rx) = channel();
            let _watcher =
                watch_recursive_mutations(dir.path(), tx).expect("start inotify watcher");
            mutate(dir.path());
            rx.recv_timeout(Duration::from_secs(1))
                .expect("filesystem mutation must wake");
        }

        assert_wakes(
            |_| {},
            |dir| std::fs::write(dir.join("created"), "value").unwrap(),
        );
        assert_wakes(
            |dir| std::fs::write(dir.join("renamed-from"), "value").unwrap(),
            |dir| {
                std::fs::rename(dir.join("renamed-from"), dir.join("renamed-to")).unwrap();
            },
        );
        assert_wakes(
            |dir| std::fs::write(dir.join("removed"), "value").unwrap(),
            |dir| std::fs::remove_file(dir.join("removed")).unwrap(),
        );
    }
}
