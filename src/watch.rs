//! Filesystem watch filtering shared by the supervisor and DING sidecars.
//!
//! Linux `notify` subscribes to inotify `OPEN` events and reports them as [`EventKind::Access`].
//! Forwarding those events makes a reader self-wake forever: each pass reads its watched tree, that
//! read emits another event, and the next timed wait returns immediately. Only mutations are useful
//! wakeups; timer polling remains the fallback for anything a backend cannot classify.

use std::path::Path;
use std::sync::mpsc::Sender;

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

/// Watch only the inputs a native delivery pump consumes: the agent's `resources/inbox` subtree
/// and its `status` file. Runtime records written beside them by the pump's own process group —
/// the presence refresh's temp siblings, `harness-state`, stream state — must never wake delivery,
/// or a writer that observes on every turn boundary pumps itself continuously.
pub(crate) fn watch_delivery_inputs(
    agent_dir: &Path,
    tx: Sender<()>,
) -> Option<notify::RecommendedWatcher> {
    let inbox = agent_dir.join("resources").join("inbox");
    let status = agent_dir.join("status");
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<Event>| {
        if result.is_ok_and(|event| {
            is_mutation(&event)
                && event
                    .paths
                    .iter()
                    .any(|path| path.starts_with(&inbox) || *path == status)
        }) {
            let _ = tx.send(());
        }
    })
    .ok()?;
    watcher.watch(agent_dir, RecursiveMode::Recursive).ok()?;
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
    if !agent_spec::is_catalog_path(root, path) {
        return false;
    }
    let rel = path.strip_prefix(root).unwrap_or(path);
    let mut components = rel.components();
    if matches!(
        components.next().and_then(|c| c.as_os_str().to_str()),
        Some("_templates")
    ) {
        return true;
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
    fn declaration_filter_uses_catalog_scoped_namespace_semantics() {
        let catalog = tempfile::tempdir().unwrap();
        let root = catalog.path();
        std::fs::create_dir_all(root.join("agents/h/live")).unwrap();
        std::fs::write(
            root.join("agents/h/live/agent.kdl"),
            r#"agent "live" { host "h"; command "x" }"#,
        )
        .unwrap();

        assert!(is_declaration_path(
            root,
            &root.join("teams/.managed/project/agent.kdl")
        ));
        assert!(is_declaration_path(
            root,
            &root.join("teams/.retired/project/agent.kdl")
        ));
        assert!(is_declaration_path(
            root,
            &root.join("agents/archive/project/agent.kdl")
        ));
        assert!(is_declaration_path(
            root,
            &root.join("agents/resources/project/agent.kdl")
        ));
        assert!(is_declaration_path(root, &root.join("_templates/base.kdl")));
        assert!(!is_declaration_path(
            root,
            &root.join(".git/project/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            &root.join(".st2/project/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            &root.join("organizations/project/.git/nested/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            &root.join("organizations/project/.st2/nested/agent.kdl")
        ));
        assert!(!is_declaration_path(
            root,
            &root.join("pty/project/agent.kdl")
        ));
        for reserved in ["resources", "archive", "inbox"] {
            assert!(!is_declaration_path(
                root,
                &root.join(format!("agents/h/live/{reserved}/project/agent.kdl"))
            ));
        }
        assert!(!is_declaration_path(root, &root.join("team/rendered.kdl")));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn delivery_watcher_ignores_runtime_records_but_wakes_on_inbox_and_status() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let agent_dir = dir.path();
        std::fs::create_dir_all(agent_dir.join("resources/inbox")).unwrap();

        let (tx, rx) = channel();
        let _watcher = watch_delivery_inputs(agent_dir, tx).expect("start inotify watcher");

        // Runtime records the pump's own process group writes must stay silent: the observed
        // harness state, its atomic temp siblings, the presence temp sibling, stream state.
        std::fs::write(agent_dir.join("harness-state"), "{}").unwrap();
        std::fs::write(agent_dir.join(".harness-state.tmp-1-0"), "{}").unwrap();
        std::fs::write(agent_dir.join(".status.tmp-1-0"), "available\n").unwrap();
        std::fs::create_dir_all(agent_dir.join("resources/streams/s")).unwrap();
        std::fs::write(agent_dir.join("resources/streams/s/state.json"), "{}").unwrap();
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "runtime-record writes must not wake the delivery pump"
        );

        // The two genuine delivery inputs wake it: an inbox arrival…
        std::fs::write(agent_dir.join("resources/inbox/0001-msg.md"), "hi").unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("inbox write must wake");
        while rx.try_recv().is_ok() {}

        // …and a presence change, including one landing via atomic tmp+rename.
        std::fs::rename(agent_dir.join(".status.tmp-1-0"), agent_dir.join("status")).unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("status rename must wake");
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
