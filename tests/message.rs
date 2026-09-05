//! M2.1 integration: the native message bus resolves recipients against a *discovered* catalog and
//! lands each message in the right agent's `resources/inbox` (VRS §5). Unit-level send/parse/archive
//! mechanics live in `src/message.rs`; this covers the catalog-resolution composition the CLI relies
//! on — send by bus id, list, reply-threading, archive.

use std::fs;
use std::path::Path;

use st2::AgentSelector;
use st2::message::{
    archive_dir, archive_msg, collect_thread, inbox_dir, list_dir, read_msg, reply_subject,
    resolve_agent_dir, select_agent_dir, send_to_inbox, send_to_resolved_inbox,
};

fn write(root: &Path, rel: &str, contents: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn agent_kdl(identity: &str, host: &str) -> String {
    format!(
        r#"agent "{identity}" {{
  identity "{identity}"
  host "{host}"
  type "service"
  pty "agent" {{
    command "exec claude boot"
  }}
}}
"#
    )
}

/// A migrated declaration: an explicit catalog-global immutable `id` and a mutable `address`.
fn migrated_agent_kdl(identity: &str, host: &str, id: &str, address: &str) -> String {
    format!(
        r#"agent "{identity}" {{
  identity "{identity}"
  id "{id}"
  address "{address}"
  host "{host}"
  type "service"
  pty "agent" {{
    command "exec claude boot"
  }}
}}
"#
    )
}

/// A two-agent catalog on host `hetz`. Send to one by bus id → lands in *its* `resources/inbox`,
/// never the other's. List, then archive.
#[test]
fn send_by_agent_id_lands_in_recipient_inbox() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "hetz/st2-claude/agent.kdl",
        &agent_kdl("st2-claude", "hetz"),
    );
    write(
        root,
        "hetz/cos-claude/agent.kdl",
        &agent_kdl("cos-claude", "hetz"),
    );

    // Resolve by the subject's exact immutable ID. For an unmigrated declaration that ID is
    // exactly its `<host>.<identity>` bytes, which is why migration moves no state.
    let dir_by_id = resolve_agent_dir(root, "hetz.st2-claude", "hetz")
        .unwrap()
        .expect("resolve by immutable id");
    // The same subject as an ordinary human reference, through the address algorithm.
    let dir_by_address = select_agent_dir(root, &AgentSelector::address("st2-claude"), "hetz")
        .unwrap()
        .expect("resolve by address");
    assert_eq!(dir_by_id, dir_by_address);
    assert_eq!(dir_by_id, root.join("hetz/st2-claude"));
    let dir_by_bus = dir_by_id;

    // Send lands under the recipient's resources/inbox — and nowhere near the other agent.
    let inbox = inbox_dir(&dir_by_bus);
    let f = send_to_inbox(&inbox, "hetz.cos-claude", Some("kick"), None, &[], "do M2").unwrap();
    assert!(inbox.join(&f).exists());
    let other = inbox_dir(&root.join("hetz/cos-claude"));
    assert!(
        list_dir(&other).unwrap().is_empty(),
        "must not leak into the other agent's inbox"
    );

    let listed = list_dir(&inbox).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].from.as_deref(), Some("hetz.cos-claude"));
    assert_eq!(read_msg(&inbox, &f).unwrap().body.trim_end(), "do M2");

    // Archive moves it out of the inbox and into resources/archive.
    archive_msg(&inbox, &archive_dir(&dir_by_bus), &f).unwrap();
    assert!(list_dir(&inbox).unwrap().is_empty());
    assert_eq!(list_dir(&archive_dir(&dir_by_bus)).unwrap().len(), 1);
}

/// An unknown recipient resolves to `None` (the CLI turns this into a clear error).
#[test]
fn unknown_recipient_does_not_resolve() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "hetz/st2-claude/agent.kdl",
        &agent_kdl("st2-claude", "hetz"),
    );
    assert!(
        resolve_agent_dir(root, "hetz.nobody", "hetz")
            .unwrap()
            .is_none()
    );
}

#[test]
fn archive_rejects_noncanonical_leaf_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("inbox");
    let archive = tmp.path().join("archive");
    fs::create_dir_all(&inbox).unwrap();
    fs::write(tmp.path().join("outside.md"), "unchanged").unwrap();
    assert!(archive_msg(&inbox, &archive, "../outside.md").is_err());
    assert_eq!(
        fs::read_to_string(tmp.path().join("outside.md")).unwrap(),
        "unchanged"
    );
}

/// `message thread` walks the reply chain ACROSS agents — a two-party conversation lives in both
/// boxes (alice's message in bob's inbox; bob's reply in alice's inbox). The walk finds the whole
/// thread from any member, in reply-tree order with depth.
#[test]
fn thread_walks_the_reply_chain_across_both_agents() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "h/alice-claude/agent.kdl",
        &agent_kdl("alice-claude", "h"),
    );
    write(
        root,
        "h/bob-claude/agent.kdl",
        &agent_kdl("bob-claude", "h"),
    );
    let alice = resolve_agent_dir(root, "h.alice-claude", "h")
        .unwrap()
        .unwrap();
    let bob = resolve_agent_dir(root, "h.bob-claude", "h")
        .unwrap()
        .unwrap();

    // alice → bob (root), bob → alice (reply), alice → bob (reply-to-reply).
    let f1 = send_to_inbox(
        &inbox_dir(&bob),
        "h.alice-claude",
        Some("plan"),
        None,
        &[],
        "kickoff",
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let f2 = send_to_inbox(
        &inbox_dir(&alice),
        "h.bob-claude",
        Some("re: plan"),
        Some(&f1),
        &[],
        "on it",
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let f3 = send_to_inbox(
        &inbox_dir(&bob),
        "h.alice-claude",
        Some("re: plan"),
        Some(&f2),
        &[],
        "go",
    )
    .unwrap();

    // From ANY member, the whole thread comes back, root-first, in reply order.
    for start in [&f1, &f2, &f3] {
        let thread = collect_thread(root, start).unwrap();
        let names: Vec<&str> = thread.iter().map(|e| e.filename.as_str()).collect();
        assert_eq!(
            names,
            [f1.as_str(), f2.as_str(), f3.as_str()],
            "thread from {start}"
        );
        // Depth increases down the reply chain.
        assert_eq!(
            thread.iter().map(|e| e.depth).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }

    // An unknown filename → empty thread.
    assert!(
        collect_thread(root, "0000000000000-nope00.md")
            .unwrap()
            .is_empty()
    );
}

/// The reply flow: read an inbound message, derive recipient (its `from`) + threading, and land the
/// reply in the *original sender's* inbox with an `in-reply-to` back-pointer.
#[test]
fn reply_threads_back_to_the_original_sender() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "hetz/st2-claude/agent.kdl",
        &agent_kdl("st2-claude", "hetz"),
    );
    write(
        root,
        "hetz/cos-claude/agent.kdl",
        &agent_kdl("cos-claude", "hetz"),
    );

    let me = resolve_agent_dir(root, "hetz.st2-claude", "hetz")
        .unwrap()
        .unwrap();
    let cos = resolve_agent_dir(root, "hetz.cos-claude", "hetz")
        .unwrap()
        .unwrap();

    // cos → me
    let original = send_to_inbox(
        &inbox_dir(&me),
        "hetz.cos-claude",
        Some("M2 kick"),
        None,
        &[],
        "go",
    )
    .unwrap();

    // me replies: recipient + subject + in-reply-to derived from the original in my inbox.
    let inbound = read_msg(&inbox_dir(&me), &original).unwrap();
    let to = inbound.from.as_deref().unwrap();
    assert_eq!(to, "hetz.cos-claude");
    let subject = reply_subject(inbound.subject.as_deref());
    assert_eq!(subject.as_deref(), Some("re: M2 kick"));
    let reply = send_to_inbox(
        &inbox_dir(&cos),
        "hetz.st2-claude",
        subject.as_deref(),
        Some(&original),
        &[],
        "on it",
    )
    .unwrap();

    let got = read_msg(&inbox_dir(&cos), &reply).unwrap();
    assert_eq!(got.from.as_deref(), Some("hetz.st2-claude"));
    assert_eq!(got.in_reply_to.as_deref(), Some(original.as_str()));
    assert_eq!(got.subject.as_deref(), Some("re: M2 kick"));
}

/// Concurrent readers see either no canonical entry or the complete message. The large body widens
/// the old direct-write window; a temporary sibling never matches the bus filename grammar.
#[test]
fn a_concurrent_reader_never_observes_a_half_written_message() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let body = "x".repeat(4 * 1024 * 1024);
    let sends = 40usize;
    let cursor = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let reader = {
        let root = root.clone();
        let cursor = Arc::clone(&cursor);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut incomplete = 0usize;
            while !done.load(Ordering::Relaxed) {
                let inbox = root.join(format!("inbox-{}", cursor.load(Ordering::Relaxed)));
                for message in list_dir(&inbox).unwrap() {
                    if message.from.is_none() || message.subject.is_none() {
                        incomplete += 1;
                    }
                }
            }
            incomplete
        })
    };

    for i in 0..sends {
        let inbox = root.join(format!("inbox-{i}"));
        fs::create_dir_all(&inbox).unwrap();
        cursor.store(i, Ordering::Relaxed);
        send_to_inbox(&inbox, "alice", Some(&format!("big {i}")), None, &[], &body).unwrap();
    }
    done.store(true, Ordering::Relaxed);

    assert_eq!(
        reader.join().unwrap(),
        0,
        "reader observed a canonical message before it was complete"
    );
}

/// The ordinary reference algorithm, end to end: a bare address, a host-qualified bus address, and
/// a reference that names two distinct subjects, which must fail closed rather than pick one.
#[test]
fn an_ordinary_reference_resolves_by_address_and_fails_closed_when_ambiguous() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "hetz/worker/agent.kdl", &agent_kdl("worker", "hetz"));
    write(root, "dev4/worker/agent.kdl", &agent_kdl("worker", "dev4"));
    write(root, "hetz/solo/agent.kdl", &agent_kdl("solo", "hetz"));

    // A bare address unique across the catalog resolves.
    assert_eq!(
        select_agent_dir(root, &AgentSelector::address("solo"), "hetz")
            .unwrap()
            .expect("a unique bare address resolves"),
        root.join("hetz/solo")
    );

    // A host-qualified reference picks exactly that host's subject, including a remote one.
    assert_eq!(
        select_agent_dir(root, &AgentSelector::address("dev4.worker"), "hetz")
            .unwrap()
            .expect("a qualified bus address resolves"),
        root.join("dev4/worker")
    );

    // Pinning the caller's host disambiguates the same bare address.
    assert_eq!(
        select_agent_dir(root, &AgentSelector::address_on_host("worker", "hetz"), "hetz")
            .unwrap()
            .expect("a host-pinned address resolves"),
        root.join("hetz/worker")
    );

    // Unpinned, `worker` names two distinct subjects. Silently picking one would deliver a
    // message to the wrong agent, so this is an error and not an absent subject.
    let error = select_agent_dir(root, &AgentSelector::address("worker"), "hetz")
        .expect_err("an ambiguous reference must fail closed");
    let rendered = format!("{error}");
    assert!(rendered.contains("dev4.worker"), "{rendered}");
    assert!(rendered.contains("hetz.worker"), "{rendered}");
}

/// ID and address are separate typed namespaces. An exact-ID selection must never fall through to
/// address lookup, and an ordinary reference must never reach the ID namespace.
#[test]
fn an_exact_id_selection_never_falls_through_to_address_lookup() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "hetz/worker/agent.kdl",
        &migrated_agent_kdl("worker", "hetz", "worker-uuid", "chat"),
    );

    assert_eq!(
        resolve_agent_dir(root, "worker-uuid", "hetz")
            .unwrap()
            .expect("the exact immutable ID resolves"),
        root.join("hetz/worker")
    );
    // The subject's current route is `hetz.chat`; asking for it as an ID must not find it.
    assert!(resolve_agent_dir(root, "chat", "hetz").unwrap().is_none());
    assert!(
        resolve_agent_dir(root, "hetz.chat", "hetz")
            .unwrap()
            .is_none()
    );
    // And the ID is not a route: an address lookup must not reach the ID namespace.
    assert!(
        select_agent_dir(root, &AgentSelector::address("worker-uuid"), "hetz")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        select_agent_dir(root, &AgentSelector::address("chat"), "hetz")
            .unwrap()
            .expect("the address resolves as an address"),
        root.join("hetz/worker")
    );
}

/// An address change is an immediate atomic cutover of the ROUTE only. Durable state does not
/// move: the inbox, the archive, and every reply that targets the persisted endpoint still land
/// in the same boxes, and the old route stops resolving with no alias or redirect left behind.
#[test]
fn an_address_change_preserves_inbox_archive_and_reply_targeting() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let worker = root.join("hetz/worker");
    write(
        root,
        "hetz/sender/agent.kdl",
        &migrated_agent_kdl("sender", "hetz", "sender-uuid", "sender"),
    );
    write(
        root,
        "hetz/worker/agent.kdl",
        &migrated_agent_kdl("worker", "hetz", "worker-uuid", "chat"),
    );

    // Send by the recipient's current route.
    let first = send_to_resolved_inbox(
        root, "chat", "hetz", "sender-uuid", Some("before"), None, &[], "one", None, None,
    )
    .unwrap();
    assert!(inbox_dir(&worker).join(&first).exists());
    archive_msg(&inbox_dir(&worker), &archive_dir(&worker), &first).unwrap();

    // Cut the address over. Nothing else about the declaration changes.
    write(
        root,
        "hetz/worker/agent.kdl",
        &migrated_agent_kdl("worker", "hetz", "worker-uuid", "renamed"),
    );

    // Durable state stayed exactly where it was — the archive receipt is still the subject's.
    assert_eq!(
        resolve_agent_dir(root, "worker-uuid", "hetz")
            .unwrap()
            .expect("the ID still resolves after the cutover"),
        worker
    );
    let archived = list_dir(&archive_dir(&worker)).unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].filename, first);

    // The released address is gone immediately: no alias, no redirect, no history.
    assert!(
        select_agent_dir(root, &AgentSelector::address("chat"), "hetz")
            .unwrap()
            .is_none()
    );

    // A reply targeting the persisted canonical endpoint still lands in the same inbox, and so
    // does a send by the new route.
    let reply = send_to_inbox(
        &inbox_dir(&worker),
        "hetz.sender",
        Some("re: before"),
        Some(&first),
        &[],
        "two",
    )
    .unwrap();
    let second = send_to_resolved_inbox(
        root, "renamed", "hetz", "sender-uuid", Some("after"), None, &[], "three", None, None,
    )
    .unwrap();

    let inbox = list_dir(&inbox_dir(&worker)).unwrap();
    let mut names = inbox.iter().map(|m| m.filename.clone()).collect::<Vec<_>>();
    names.sort();
    let mut expected = vec![reply, second];
    expected.sort();
    assert_eq!(names, expected);
    assert_eq!(
        read_msg(&inbox_dir(&worker), &expected[0]).unwrap().ts_ms > 0,
        true
    );
}
