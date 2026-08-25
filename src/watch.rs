//! Filesystem watch filtering shared by the supervisor and DING sidecars.
//!
//! Linux `notify` subscribes to inotify `OPEN` events and reports them as [`EventKind::Access`].
//! Forwarding those events makes a reader self-wake forever: each pass reads its watched tree, that
//! read emits another event, and the next timed wait returns immediately. Only mutations are useful
//! wakeups; timer polling remains the fallback for anything a backend cannot classify.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

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

/// A shallow subscription over the directories that can contain declarations.
///
/// `notify` implements a recursive Linux watch by eagerly walking the entire tree and allocating
/// one inotify watch per directory BEFORE the first callback ever runs. A production catalog also
/// contains unbounded Resource payload trees, so a recursive subscription spends all of startup
/// walking data the callback would later ignore — and fails outright once the walk exhausts
/// kernel limits. Keep one non-recursive watch per declaration-space directory instead;
/// [`CatalogDeclarationWatcher::refresh`] runs after each reconcile pass so a newly created
/// directory becomes watched before later edits inside it.
pub(crate) struct CatalogDeclarationWatcher {
    root: PathBuf,
    watcher: RecommendedWatcher,
    watched: Arc<Mutex<BTreeMap<PathBuf, Option<DirIdentity>>>>,
    failed: BTreeSet<PathBuf>,
}

/// Identity of a watched directory. An inotify watch attaches to an INODE, not a name, so a
/// directory deleted and recreated at the same pathname is a DIFFERENT directory: matching on
/// identity alone lets `refresh` force re-registration for replacements.
#[cfg(unix)]
type DirIdentity = (u64, u64);
#[cfg(not(unix))]
type DirIdentity = ();

#[cfg(unix)]
fn dir_identity(path: &Path) -> Option<DirIdentity> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).ok().map(|meta| (meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn dir_identity(path: &Path) -> Option<DirIdentity> {
    fs::metadata(path).ok().map(|_| ())
}

impl CatalogDeclarationWatcher {
    fn new(root: &Path, tx: Sender<()>) -> notify::Result<Self> {
        let callback_root = root.to_path_buf();
        let watched = Arc::new(Mutex::new(BTreeMap::new()));
        let invalidator = Arc::clone(&watched);
        let watcher = notify::recommended_watcher(move |result: notify::Result<Event>| {
            if let Ok(event) = result {
                invalidate_removed_dirs(&invalidator, &event);
                if should_wake_catalog(&callback_root, &event) {
                    let _ = tx.send(());
                }
            }
        })?;
        let mut this = Self {
            root: root.to_path_buf(),
            watcher,
            watched,
            failed: BTreeSet::new(),
        };
        this.watcher.watch(root, RecursiveMode::NonRecursive)?;
        let mut watched = this
            .watched
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        watched.insert(root.to_path_buf(), dir_identity(root));
        drop(watched);
        this.refresh();
        Ok(this)
    }

    /// Reconcile the shallow subscriptions with the current declaration namespace.
    pub(crate) fn refresh(&mut self) {
        let desired = declaration_watch_dirs(&self.root);
        let mut watched = self.watched.lock().unwrap_or_else(|p| p.into_inner());

        // An inotify watch dies with its inode. A directory deleted and recreated at the same
        // pathname can even reuse the identity, so `invalidate_removed_dirs` drops removals from
        // this set eagerly; identity comparison here then catches anything the events missed.
        let mut stale = Vec::new();
        watched.retain(|path, identity| match desired.get(path) {
            Some(fresh) if *fresh == *identity => true,
            _ => {
                stale.push(path.clone());
                false
            }
        });
        for path in &stale {
            // Backends normally discard a watch when its directory disappears. An explicit
            // best-effort unwatch also handles moves that leave the watched inode alive elsewhere.
            let _ = self.watcher.unwatch(path);
        }
        for added in desired.into_keys() {
            if watched.contains_key(&added) {
                continue;
            }
            match self.watcher.watch(&added, RecursiveMode::NonRecursive) {
                Ok(()) => {
                    self.failed.remove(&added);
                    match dir_identity(&added) {
                        Some(identity) => {
                            watched.insert(added, Some(identity));
                        }
                        // Vanished between registration and stat: leave it unrecorded so the
                        // next refresh retries from scratch.
                        None => {
                            let _ = self.watcher.unwatch(&added);
                        }
                    }
                }
                Err(error) if self.failed.insert(added.clone()) => {
                    eprintln!(
                        "st2: cannot watch catalog declaration directory '{}': {error}; immediate changes below it are unavailable, continuing with timer polling.",
                        added.display()
                    );
                }
                Err(_) => {}
            }
        }
    }

    #[cfg(test)]
    fn watched_dirs(&self) -> BTreeSet<PathBuf> {
        self.watched
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}

/// Eagerly drop tracked directories the moment the backend reports them removed or renamed
/// away — an inotify watch dies with its inode, and a same-pathname replacement can reuse the
/// old identity, so only event-time invalidation makes the next [`refresh`] re-register
/// deterministically instead of trusting a stat race.
fn invalidate_removed_dirs(watched: &Mutex<BTreeMap<PathBuf, Option<DirIdentity>>>, event: &Event) {
    let torn_down = matches!(
        &event.kind,
        EventKind::Remove(RemoveKind::Folder | RemoveKind::Any | RemoveKind::Other)
            | EventKind::Modify(ModifyKind::Name(RenameMode::From))
    );
    if !torn_down {
        return;
    }
    let mut watched = watched
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for path in &event.paths {
        watched.remove(path);
        // Removing or moving away a parent retires every watch beneath it too.
        watched.retain(|tracked, _| !tracked.starts_with(path));
    }
}

/// Watch only declaration inputs for the supervisor. Runtime state (PTY registry, bus, logs,
/// locks, inboxes, and generated materializations) must never be traversed or wake reconciliation.
pub(crate) fn watch_catalog_declarations(
    root: &Path,
    tx: Sender<()>,
) -> notify::Result<CatalogDeclarationWatcher> {
    CatalogDeclarationWatcher::new(root, tx)
}

/// Every directory that can hold declarations, discovered WITHOUT descending into Resource
/// payloads: recursion is gated by [`agent_spec::is_catalog_path`], so a declaration parent's
/// `resources`/`archive`/`inbox` children and `.git`/`.st2` control dirs prune the walk.
fn declaration_watch_dirs(root: &Path) -> BTreeMap<PathBuf, Option<DirIdentity>> {
    fn collect(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Option<DirIdentity>>) {
        out.insert(dir.to_path_buf(), dir_identity(dir));
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|kind| kind.is_dir())
                && agent_spec::is_catalog_path(root, &path)
            {
                collect(root, &path, out);
            }
        }
    }

    let mut dirs = BTreeMap::new();
    collect(root, root, &mut dirs);
    dirs
}

fn should_wake_catalog(root: &Path, event: &Event) -> bool {
    if !is_mutation(event) {
        return false;
    }
    event.paths.iter().any(|path| {
        is_declaration_path(root, path)
            || (is_directory_topology_mutation(event) && agent_spec::is_catalog_path(root, path))
    })
}

/// Directory-level topology changes must wake even though no `agent.kdl` path exists yet — the
/// created directory may receive one next, and the watcher needs `refresh` anyway. FILE-level
/// create/remove/rename stays silent: a scratch, log, or editor-swap file in declaration space
/// must not wake a full-catalog reconcile. Linux classifies creates/removals by entry type;
/// renames carry no type, so the arrived side is checked against the live tree. A rename AWAY
/// from the catalog has no surviving entry to inspect and stays silent here — an in-catalog
/// rename also emits the arrived side, and a full removal is bounded by the timer plus the
/// next reconciliation's `refresh`.
fn is_directory_topology_mutation(event: &Event) -> bool {
    match &event.kind {
        EventKind::Create(CreateKind::Folder) => true,
        EventKind::Remove(RemoveKind::Folder | RemoveKind::Any | RemoveKind::Other) => true,
        EventKind::Modify(ModifyKind::Name(_)) => event.paths.iter().any(|p| p.is_dir()),
        _ => false,
    }
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

    #[test]
    fn declaration_watch_tree_stops_before_resource_payloads() {
        let catalog = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = catalog.path();
        let agent = root.join("agents/h/live");
        std::fs::create_dir_all(agent.join("resources/cache/a/b/c/d/e")).unwrap();
        std::fs::create_dir_all(agent.join("inbox/archive/a/b/c")).unwrap();
        std::fs::create_dir_all(outside.path().join("worktree/node_modules/a/b/c/d/e")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            outside.path().join("worktree"),
            agent.join("resources/worktree"),
        )
        .unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "h"; command "x" }"#,
        )
        .unwrap();

        let watched = declaration_watch_dirs(root);
        assert_eq!(
            watched.keys().cloned().collect::<BTreeSet<_>>(),
            [
                root.to_path_buf(),
                root.join("agents"),
                root.join("agents/h"),
                agent,
            ]
            .into_iter()
            .collect()
        );
        assert!(
            watched.keys().all(|path| !path.starts_with(outside.path())),
            "Resource worktree links must not escape the catalog watch boundary"
        );
    }

    #[test]
    fn topology_wakes_are_directory_scoped() {
        use notify::event::{AccessKind, AccessMode};

        let file_create = Event::new(EventKind::Create(CreateKind::File))
            .add_path(PathBuf::from("/cat/agents/h/live/scratch.log"));
        let dir_create = Event::new(EventKind::Create(CreateKind::Folder))
            .add_path(PathBuf::from("/cat/agents/h/newdir"));
        let file_remove = Event::new(EventKind::Remove(RemoveKind::File))
            .add_path(PathBuf::from("/cat/agents/h/live/scratch.log"));
        let dir_remove = Event::new(EventKind::Remove(RemoveKind::Folder))
            .add_path(PathBuf::from("/cat/agents/h/gone"));
        let file_rename_in = Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::To)))
            .add_path(PathBuf::from("/cat/agents/h/live/renamed.txt"));
        let rename_away = Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
            .add_path(PathBuf::from("/cat/agents/h/live/.swap.swp"));

        for quiet in [file_create, file_remove, file_rename_in, rename_away] {
            assert!(
                !is_directory_topology_mutation(&quiet),
                "{quiet:?}: a FILE topology event must not wake the catalog"
            );
        }
        for loud in [dir_create, dir_remove] {
            assert!(
                is_directory_topology_mutation(&loud),
                "{loud:?}: a DIRECTORY-level topology event must wake the catalog"
            );
        }

        // Sanity: access events never wake regardless of classification.
        let read = Event::new(EventKind::Access(AccessKind::Open(AccessMode::Read)))
            .add_path(PathBuf::from("/cat/agents/h/newdir"));
        assert!(!is_directory_topology_mutation(&read));
    }

    #[test]
    fn watcher_installation_preserves_backend_errors() {
        let parent = tempfile::tempdir().unwrap();
        let missing_catalog = parent.path().join("missing");
        let (tx, _rx) = std::sync::mpsc::channel();
        assert!(
            watch_catalog_declarations(&missing_catalog, tx).is_err(),
            "a missing root must return the installation error"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn catalog_watch_is_bounded_and_refreshes_new_declaration_directories() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let catalog = tempfile::tempdir().unwrap();
        let root = catalog.path();
        let agent = root.join("agents/h/live");
        std::fs::create_dir_all(agent.join("resources/cache/a/b/c/d/e")).unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "h"; command "x" }"#,
        )
        .unwrap();

        let (tx, rx) = channel();
        let mut watcher = watch_catalog_declarations(root, tx).expect("start catalog watcher");
        assert_eq!(
            watcher.watched_dirs().len(),
            4,
            "Resource depth must not add watches"
        );

        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "h"; command "changed" }"#,
        )
        .unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("declaration update must wake");
        while rx.try_recv().is_ok() {}

        let nested = root.join("teams/new/live");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            nested.join("agent.kdl"),
            r#"agent "live" { host "new"; command "x" }"#,
        )
        .unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("new declaration directory must wake its watched ancestor");
        while rx.try_recv().is_ok() {}
        watcher.refresh();
        assert!(watcher.watched_dirs().contains(&nested));

        std::fs::write(
            nested.join("agent.kdl"),
            r#"agent "live" { host "new"; command "changed" }"#,
        )
        .unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("declaration update after refresh must wake");
        while rx.try_recv().is_ok() {}

        std::fs::remove_file(nested.join("agent.kdl")).unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("declaration removal must wake");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_declaration_file_topology_stays_silent() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let catalog = tempfile::tempdir().unwrap();
        let agent = catalog.path().join("agents/h/live");
        std::fs::create_dir_all(&agent).unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "h"; command "x" }"#,
        )
        .unwrap();

        let (tx, rx) = channel();
        let _watcher = watch_catalog_declarations(catalog.path(), tx).expect("start watcher");

        std::fs::write(agent.join("scratch.log"), "noise").unwrap();
        std::fs::rename(agent.join("scratch.log"), agent.join("scratch2.log")).unwrap();
        std::fs::remove_file(agent.join("scratch2.log")).unwrap();
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "scratch-file create/rename/remove must not wake a full-catalog reconcile"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn replaced_declaration_directory_is_resubscribed_on_refresh() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let catalog = tempfile::tempdir().unwrap();
        let root = catalog.path();
        let agent = root.join("agents/h/live");
        std::fs::create_dir_all(&agent).unwrap();
        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "h"; command "x" }"#,
        )
        .unwrap();

        let (tx, rx) = channel();
        let mut watcher = watch_catalog_declarations(root, tx).expect("start watcher");

        // Delete and recreate at the SAME pathname: the backend watch died with the old inode
        // while `watched` still holds the name, so only identity comparison can catch this.
        std::fs::remove_dir_all(&agent).unwrap();
        std::fs::create_dir_all(&agent).unwrap();
        watcher.refresh();
        while rx.try_recv().is_ok() {}

        std::fs::write(
            agent.join("agent.kdl"),
            r#"agent "live" { host "h"; command "changed" }"#,
        )
        .unwrap();
        rx.recv_timeout(Duration::from_secs(1))
            .expect("a replaced directory must be resubscribed by refresh");
    }
}
