//! M2.1 integration: the native message bus resolves recipients against a *discovered* catalog and
//! lands each message in the right agent's `resources/inbox` (VRS §5). Unit-level send/parse/archive
//! mechanics live in `src/message.rs`; this covers the catalog-resolution composition the CLI relies
//! on — send by bus id, list, reply-threading, archive.

use std::fs;
use std::path::Path;

use st2::identity::AgentSelector;
use st2::message::{
    LegacyAttribution, archive_dir, archive_msg, attribute_endpoint, collect_thread, inbox_dir,
    list_dir, list_sent, read_msg, reply_recipient, reply_subject, resolve_agent_dir,
    resolve_selected_agent_dir, send_selected_to_resolved_inbox, send_to_inbox,
    send_to_resolved_inbox,
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

/// `agent_kdl` plus extra declaration fields, one per line, already indented.
fn agent_kdl_with(identity: &str, host: &str, extra: &str) -> String {
    format!(
        r#"agent "{identity}" {{
  identity "{identity}"
  host "{host}"
{extra}  type "service"
  pty "agent" {{
    command "exec claude boot"
  }}
}}
"#
    )
}

const SENDER_ID: &str = "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1";
const RECIPIENT_ID: &str = "0199b8f4-b48d-75c0-baa2-5e0fe2a1f8a3";

/// The one committed Sent row of a sender, as raw JSON.
fn sent_row(agent_dir: &Path, filename: &str) -> serde_json::Value {
    let path = agent_dir
        .join("resources/sent/messages")
        .join(format!("{filename}.json"));
    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap()
}

/// An ordinary reference is an *address*: a dotted bare address and its host-qualified spelling
/// name the same subject, and a declared address replaces the positional identity fallback.
#[test]
fn an_ordinary_reference_resolves_through_the_address_algorithm() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "dev3/verifier/agent.kdl",
        &agent_kdl_with(
            "verifier",
            "dev3",
            "  address \"dotfiles.fractal.keymap.verifier\"\n",
        ),
    );

    let expected = root.join("dev3/verifier");
    assert_eq!(
        resolve_agent_dir(root, "dotfiles.fractal.keymap.verifier", "dev3")
            .unwrap()
            .expect("the dotted bare address resolves"),
        expected
    );
    assert_eq!(
        resolve_agent_dir(root, "dev3.dotfiles.fractal.keymap.verifier", "dev3")
            .unwrap()
            .expect("the host-qualified spelling resolves to the same subject"),
        expected
    );
    assert!(
        resolve_agent_dir(root, "verifier", "dev3").unwrap().is_none(),
        "the positional identity is only the fallback address, not a second route"
    );
}

/// Absence and ambiguity are different answers. Two subjects sharing one address make an
/// unqualified reference undecidable, and the refusal names them instead of reporting "not found".
#[test]
fn an_ambiguous_address_refuses_and_names_the_surviving_subjects() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "dev3/chat/agent.kdl", &agent_kdl("chat", "dev3"));
    write(root, "dev4/chat/agent.kdl", &agent_kdl("chat", "dev4"));

    let error = resolve_agent_dir(root, "chat", "dev3").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("is ambiguous"), "{rendered}");
    assert!(rendered.contains("dev3.chat"), "{rendered}");
    assert!(rendered.contains("dev4.chat"), "{rendered}");

    // Qualifying it is exact, and an absent address is still a plain absence.
    assert!(
        resolve_agent_dir(root, "dev3.chat", "dev3")
            .unwrap()
            .is_some()
    );
    assert!(resolve_agent_dir(root, "ghost", "dev3").unwrap().is_none());
}

/// An exact ID selector performs only ID lookup: equal bytes in the two namespaces do not collide,
/// so an address never answers an ID and an ID never answers an address.
#[test]
fn an_exact_agent_id_bypasses_address_lookup() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "dev3/keymap/agent.kdl",
        &agent_kdl_with(
            "keymap",
            "dev3",
            &format!("  id \"{SENDER_ID}\"\n  address \"chat\"\n"),
        ),
    );
    let expected = root.join("dev3/keymap");

    assert_eq!(
        resolve_selected_agent_dir(root, &AgentSelector::Id(SENDER_ID.to_owned()), "dev3")
            .unwrap()
            .expect("the exact id resolves"),
        expected
    );
    assert!(
        resolve_selected_agent_dir(root, &AgentSelector::Id("chat".to_owned()), "dev3")
            .unwrap()
            .is_none(),
        "an address must not be readable as an id"
    );
    assert_eq!(
        resolve_selected_agent_dir(root, &AgentSelector::Address("chat".to_owned()), "dev3")
            .unwrap()
            .expect("the address resolves"),
        expected
    );
}

/// An address cutover takes effect immediately, and the released address is reusable by another
/// subject: the old address stops resolving to the subject that gave it up.
#[test]
fn an_address_cutover_is_immediate_and_the_old_address_is_reusable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let first = |address: &str| {
        agent_kdl_with(
            "first",
            "dev3",
            &format!("  id \"{SENDER_ID}\"\n  address \"{address}\"\n"),
        )
    };
    let second = |address: &str| {
        agent_kdl_with(
            "second",
            "dev3",
            &format!("  id \"{RECIPIENT_ID}\"\n  address \"{address}\"\n"),
        )
    };
    write(root, "dev3/first/agent.kdl", &first("chat"));
    write(root, "dev3/second/agent.kdl", &second("notes"));
    assert_eq!(
        resolve_agent_dir(root, "chat", "dev3").unwrap().unwrap(),
        root.join("dev3/first")
    );

    write(root, "dev3/first/agent.kdl", &first("chat-v2"));
    write(root, "dev3/second/agent.kdl", &second("chat"));

    assert_eq!(
        resolve_agent_dir(root, "chat", "dev3").unwrap().unwrap(),
        root.join("dev3/second"),
        "the released address routes to its new holder, with no ambiguity from the old one"
    );
    assert_eq!(
        resolve_agent_dir(root, "chat-v2", "dev3").unwrap().unwrap(),
        root.join("dev3/first")
    );
    // The immutable ID is unaffected by either subject's address change.
    assert_eq!(
        resolve_selected_agent_dir(root, &AgentSelector::Id(SENDER_ID.to_owned()), "dev3")
            .unwrap()
            .unwrap(),
        root.join("dev3/first")
    );

    // Both endpoints selected by exact ID: delivery follows the subject, not its address, and the
    // rendered sender shows the address it holds *now*.
    let filename = send_selected_to_resolved_inbox(
        root,
        &AgentSelector::Id(SENDER_ID.to_owned()),
        "dev3",
        &AgentSelector::Id(RECIPIENT_ID.to_owned()),
        Some("by id"),
        None,
        &[],
        "hi",
        None,
        None,
    )
    .unwrap();
    let delivered =
        fs::read_to_string(inbox_dir(&root.join("dev3/first")).join(&filename)).unwrap();
    assert!(
        delivered.starts_with(&format!("---\nfrom: dev3.chat\nfrom-id: {RECIPIENT_ID}\n")),
        "{delivered}"
    );
}

/// A fully migrated catalog publishes version-2 rows: `from`/`to` are immutable IDs, the address
/// fields are publication-time display snapshots, and the endpoint kinds are explicit. The
/// rendered message carries the address for humans and the ID as authority.
#[test]
fn a_migrated_catalog_publishes_version_two_rows_with_ids_and_address_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "dev3/sender/agent.kdl",
        &agent_kdl_with(
            "sender",
            "dev3",
            &format!("  id \"{SENDER_ID}\"\n  address \"chat\"\n"),
        ),
    );
    write(
        root,
        "dev3/recipient/agent.kdl",
        &agent_kdl_with("recipient", "dev3", &format!("  id \"{RECIPIENT_ID}\"\n")),
    );

    let filename = send_to_resolved_inbox(
        root,
        "recipient",
        "dev3",
        "chat",
        Some("hello"),
        None,
        &[],
        "body",
        None,
        None,
    )
    .unwrap();

    let delivered = fs::read_to_string(
        inbox_dir(&root.join("dev3/recipient")).join(&filename),
    )
    .unwrap();
    assert!(
        delivered.starts_with(&format!("---\nfrom: dev3.chat\nfrom-id: {SENDER_ID}\n")),
        "{delivered}"
    );

    let row = sent_row(&root.join("dev3/sender"), &filename);
    assert_eq!(row["version"], 2);
    assert_eq!(row["from"], SENDER_ID);
    assert_eq!(row["to"], RECIPIENT_ID);
    assert_eq!(row["fromAddress"], "dev3.chat");
    assert_eq!(row["toAddress"], "dev3.recipient");
    assert_eq!(row["fromKind"], "agent");
    assert_eq!(row["toKind"], "agent");

    // The ledger reader verifies every content, row, and node digest, so this also proves the
    // version-2 row's digest relations hold. The public `message sent --json` row projects the
    // canonical recipient ID plus the display-only address snapshot and the explicit kind.
    let view = list_sent(&root.join("dev3/sender"), false).unwrap();
    assert_eq!(view.messages.len(), 1);
    assert_eq!(view.messages[0].to, RECIPIENT_ID);
    assert_eq!(view.messages[0].to_address.as_deref(), Some("dev3.recipient"));
    assert_eq!(view.messages[0].to_kind.as_deref(), Some("agent"));
}

/// One unmigrated subject keeps legacy behavior normative for the whole catalog: the row stays
/// version 1, carries legacy bus identities, and grows no version-2 key — the pending record's
/// filename is the digest of its canonical JSON, so a stray key would break retry and recovery.
#[test]
fn a_partially_migrated_catalog_still_publishes_version_one_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "dev3/sender/agent.kdl",
        &agent_kdl_with(
            "sender",
            "dev3",
            &format!("  id \"{SENDER_ID}\"\n  address \"chat\"\n"),
        ),
    );
    write(
        root,
        "dev3/recipient/agent.kdl",
        &agent_kdl("recipient", "dev3"),
    );

    let filename = send_to_resolved_inbox(
        root,
        "recipient",
        "dev3",
        "chat",
        Some("hello"),
        None,
        &[],
        "body",
        None,
        None,
    )
    .unwrap();

    let delivered =
        fs::read_to_string(inbox_dir(&root.join("dev3/recipient")).join(&filename)).unwrap();
    assert!(
        delivered.starts_with("---\nfrom: dev3.sender\nsubject: hello\n"),
        "{delivered}"
    );

    let row = sent_row(&root.join("dev3/sender"), &filename);
    assert_eq!(row["version"], 1);
    assert_eq!(row["from"], "dev3.sender");
    assert_eq!(row["to"], "dev3.recipient");
    let mut keys = row
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "body",
            "filename",
            "from",
            "idempotencyKey",
            "inReplyTo",
            "priority",
            "renderedMessage",
            "subject",
            "tags",
            "to",
            "ts",
            "version",
        ]
    );
    // The digest chain still verifies, so the legacy record's bytes are unchanged.
    assert_eq!(
        list_sent(&root.join("dev3/sender"), false)
            .unwrap()
            .messages
            .len(),
        1
    );
    // A version-1 row declared neither, and the public row says so rather than defaulting.
    let view = list_sent(&root.join("dev3/sender"), false).unwrap();
    assert_eq!(view.messages[0].to_address, None);
    assert_eq!(view.messages[0].to_kind, None, "absent means absent");
}

/// A reassigned legacy bus identity denotes two subjects. Only the record's own state owner proves
/// which one, so every other endpoint is a historical address with no reply or automation
/// authority — never the ID of the subject that kept the bytes (`MESSAGE-R04`).
#[test]
fn a_reassigned_legacy_endpoint_is_attributed_only_for_its_state_owner() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "dev3/chat/agent.kdl", &agent_kdl("chat", "dev3"));
    write(root, "dev3/reader/agent.kdl", &agent_kdl("reader", "dev3"));
    write(
        root,
        ".st2/agent-id-migration.json",
        &format!(
            r#"{{"schema":"st2.agent-id-migration.v1","migratedAtMs":1,"reassigned":[{{"legacyBusIdentity":"dev3.chat","keptByAgentId":"dev3.chat","keptByPlane":"live","reassignedAgentId":"{RECIPIENT_ID}","reassignedHost":"dev3","reassignedIdentity":"chat","reassignedPlane":"archived"}}]}}"#
        ),
    );
    let migration = st2::catalog_migrate_ids::read_migration_record(root)
        .unwrap()
        .expect("the durable collision record");

    // The sender of a sender-owned row is its state owner, so those bytes are attributable.
    assert_eq!(
        attribute_endpoint(Some(&migration), "dev3.chat", Some("dev3.chat")),
        LegacyAttribution::Owned {
            id: "dev3.chat".to_owned()
        }
    );
    // The same bytes at any other endpoint are not.
    assert_eq!(
        attribute_endpoint(Some(&migration), "dev3.chat", Some("dev3.reader")),
        LegacyAttribution::Historical {
            address: "dev3.chat".to_owned()
        }
    );
    // An untouched legacy identity is its own frozen ID and needs no attribution at all.
    assert_eq!(
        attribute_endpoint(Some(&migration), "dev3.reader", Some("dev3.reader")),
        LegacyAttribution::Frozen {
            reference: "dev3.reader".to_owned()
        }
    );

    // An inbox row's state owner is its recipient, so a colliding sender is unattributable and a
    // reply to it is refused rather than delivered to the subject that kept the bytes.
    let reader_inbox = inbox_dir(&root.join("dev3/reader"));
    let colliding =
        send_to_inbox(&reader_inbox, "dev3.chat", Some("ping"), None, &[], "hi").unwrap();
    let colliding = read_msg(&reader_inbox, &colliding).unwrap();
    let refusal = reply_recipient(root, Some("dev3.reader"), &colliding).unwrap_err();
    assert!(
        format!("{refusal:#}").contains("historical address"),
        "{refusal:#}"
    );

    // An untouched sender still replies as an ordinary address, exactly as before migration.
    let plain =
        send_to_inbox(&reader_inbox, "dev3.other", Some("ping"), None, &[], "hi").unwrap();
    let plain = read_msg(&reader_inbox, &plain).unwrap();
    assert_eq!(
        reply_recipient(root, Some("dev3.reader"), &plain).unwrap(),
        AgentSelector::Address("dev3.other".to_owned())
    );

    // A version-2 message states its sender's ID, so the reply is an exact-ID send and no
    // attribution is needed.
    let published = st2::message::send_to_inbox(
        &reader_inbox,
        "dev3.chat",
        Some("ping"),
        None,
        &[],
        "hi",
    )
    .unwrap();
    let mut published = read_msg(&reader_inbox, &published).unwrap();
    published.from_id = Some(SENDER_ID.to_owned());
    assert_eq!(
        reply_recipient(root, Some("dev3.reader"), &published).unwrap(),
        AgentSelector::Id(SENDER_ID.to_owned())
    );
}

/// A two-agent catalog on host `hetz`. Send to one by bus id → lands in *its* `resources/inbox`,
/// never the other's. List, then archive.
#[test]
fn send_by_bus_id_lands_in_recipient_inbox() {
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

    // Resolve the recipient's agent folder by its bus id, then by bare identity — both must match.
    let dir_by_bus = resolve_agent_dir(root, "hetz.st2-claude", "hetz")
        .unwrap()
        .expect("resolve by bus id");
    let dir_by_ident = resolve_agent_dir(root, "st2-claude", "hetz")
        .unwrap()
        .expect("resolve by identity");
    assert_eq!(dir_by_bus, dir_by_ident);
    assert_eq!(dir_by_bus, root.join("hetz/st2-claude"));

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
