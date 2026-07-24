//! M2.1 integration: the native message bus resolves recipients against a *discovered* catalog and
//! lands each message in the right agent's `resources/inbox` (VRS §5). Unit-level send/parse/archive
//! mechanics live in `src/message.rs`; this covers the catalog-resolution composition the CLI relies
//! on — send by bus id, list, reply-threading, archive.

use std::fs;
use std::path::Path;

use st2::message::{
    archive_dir, archive_msg, collect_thread, inbox_dir, list_dir, read_msg, reply_subject,
    resolve_agent_dir, send_to_inbox,
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

/// A two-agent catalog on host `hetz`. Send to one by bus id → lands in *its* `resources/inbox`,
/// never the other's. List, then archive.
#[test]
fn send_by_bus_id_lands_in_recipient_inbox() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "hetz/st2-claude/agent.kdl", &agent_kdl("st2-claude", "hetz"));
    write(root, "hetz/cos-claude/agent.kdl", &agent_kdl("cos-claude", "hetz"));

    // Resolve the recipient's agent folder by its bus id, then by bare identity — both must match.
    let dir_by_bus = resolve_agent_dir(root, "hetz.st2-claude", "hetz").expect("resolve by bus id");
    let dir_by_ident = resolve_agent_dir(root, "st2-claude", "hetz").expect("resolve by identity");
    assert_eq!(dir_by_bus, dir_by_ident);
    assert_eq!(dir_by_bus, root.join("hetz/st2-claude"));

    // Send lands under the recipient's resources/inbox — and nowhere near the other agent.
    let inbox = inbox_dir(&dir_by_bus);
    let f = send_to_inbox(&inbox, "hetz.cos-claude", Some("kick"), None, &[], "do M2").unwrap();
    assert!(inbox.join(&f).exists());
    let other = inbox_dir(&root.join("hetz/cos-claude"));
    assert!(list_dir(&other).unwrap().is_empty(), "must not leak into the other agent's inbox");

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
    write(root, "hetz/st2-claude/agent.kdl", &agent_kdl("st2-claude", "hetz"));
    assert!(resolve_agent_dir(root, "hetz.nobody", "hetz").is_none());
}

/// `message thread` walks the reply chain ACROSS agents — a two-party conversation lives in both
/// boxes (alice's message in bob's inbox; bob's reply in alice's inbox). The walk finds the whole
/// thread from any member, in reply-tree order with depth.
#[test]
fn thread_walks_the_reply_chain_across_both_agents() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "h/alice-claude/agent.kdl", &agent_kdl("alice-claude", "h"));
    write(root, "h/bob-claude/agent.kdl", &agent_kdl("bob-claude", "h"));
    let alice = resolve_agent_dir(root, "h.alice-claude", "h").unwrap();
    let bob = resolve_agent_dir(root, "h.bob-claude", "h").unwrap();

    // alice → bob (root), bob → alice (reply), alice → bob (reply-to-reply).
    let f1 = send_to_inbox(&inbox_dir(&bob), "h.alice-claude", Some("plan"), None, &[], "kickoff").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let f2 = send_to_inbox(&inbox_dir(&alice), "h.bob-claude", Some("re: plan"), Some(&f1), &[], "on it").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let f3 = send_to_inbox(&inbox_dir(&bob), "h.alice-claude", Some("re: plan"), Some(&f2), &[], "go").unwrap();

    // From ANY member, the whole thread comes back, root-first, in reply order.
    for start in [&f1, &f2, &f3] {
        let thread = collect_thread(root, start);
        let names: Vec<&str> = thread.iter().map(|e| e.filename.as_str()).collect();
        assert_eq!(names, [f1.as_str(), f2.as_str(), f3.as_str()], "thread from {start}");
        // Depth increases down the reply chain.
        assert_eq!(thread.iter().map(|e| e.depth).collect::<Vec<_>>(), [0, 1, 2]);
    }

    // An unknown filename → empty thread.
    assert!(collect_thread(root, "0000000000000-nope00.md").is_empty());
}

/// The reply flow: read an inbound message, derive recipient (its `from`) + threading, and land the
/// reply in the *original sender's* inbox with an `in-reply-to` back-pointer.
#[test]
fn reply_threads_back_to_the_original_sender() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "hetz/st2-claude/agent.kdl", &agent_kdl("st2-claude", "hetz"));
    write(root, "hetz/cos-claude/agent.kdl", &agent_kdl("cos-claude", "hetz"));

    let me = resolve_agent_dir(root, "hetz.st2-claude", "hetz").unwrap();
    let cos = resolve_agent_dir(root, "hetz.cos-claude", "hetz").unwrap();

    // cos → me
    let original =
        send_to_inbox(&inbox_dir(&me), "hetz.cos-claude", Some("M2 kick"), None, &[], "go").unwrap();

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
