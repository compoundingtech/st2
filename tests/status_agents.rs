//! M2.3 integration: presence status + the agent roster over a discovered catalog. Unit mechanics
//! (state parse, staleness, atomic set/refresh) live in `src/status.rs`; this covers the composition
//! `st2 agents` relies on — enumerate specs, read each agent's status/presentation/inbox, project the
//! roster, and derive `unknown` from staleness.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime};

use st2::agents::roster;
use st2::message::send_to_inbox;
use st2::status::{State, set_state, status_path};

fn write(root: &Path, rel: &str, contents: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn agent_kdl(identity: &str, host: &str) -> String {
    format!(
        "agent \"{identity}\" {{\n  identity \"{identity}\"\n  host \"{host}\"\n  \
         type \"service\"\n  resource \"work\" _tag=\"vendor-specific\" uri=\"issue://example/{identity}\"\n  \
         pty \"agent\" {{ command \"exec claude boot\" }}\n}}\n"
    )
}

fn presented_agent_kdl(identity: &str, host: &str) -> String {
    agent_kdl(identity, host).replace(
        "  type \"service\"\n",
        "  type \"service\"\n  name \"st2 owner\"\n  description \"Own st2 delivery\"\n",
    )
}

fn retired_agent_kdl(identity: &str, host: &str) -> String {
    agent_kdl(identity, host).replace(
        "  type \"service\"\n",
        "  type \"service\"\n  retired #true\n",
    )
}

/// The roster enumerates every catalog agent by bus id (sorted), projects each one's presence, and —
/// with enrich data — its inbox count and last-activity.
#[test]
fn roster_projects_presence_name_and_enrich_across_the_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "hetz/st2-claude/agent.kdl",
        &presented_agent_kdl("st2-claude", "hetz"),
    );
    write(
        root,
        "hetz/cos-claude/agent.kdl",
        &agent_kdl("cos-claude", "hetz"),
    );
    write(
        root,
        "silber/fabric-claude/agent.kdl",
        &agent_kdl("fabric-claude", "silber"),
    );

    // Presence: st2-claude busy, cos-claude available, fabric-claude unset (→ offline). Presentation
    // metadata and an inbox message belong to st2-claude's declaration.
    set_state(&status_path(&root.join("hetz/st2-claude")), State::Busy).unwrap();
    set_state(
        &status_path(&root.join("hetz/cos-claude")),
        State::Available,
    )
    .unwrap();
    send_to_inbox(
        &st2::message::inbox_dir(&root.join("hetz/st2-claude")),
        "hetz.cos-claude",
        None,
        None,
        &[],
        "hi",
    )
    .unwrap();
    let restored = send_to_inbox(
        &st2::message::inbox_dir(&root.join("hetz/st2-claude")),
        "hetz.cos-claude",
        Some("already handled"),
        None,
        &[],
        "must not count twice",
    )
    .unwrap();
    let archive = st2::message::archive_dir(&root.join("hetz/st2-claude"));
    fs::create_dir_all(&archive).unwrap();
    fs::copy(
        st2::message::inbox_dir(&root.join("hetz/st2-claude")).join(&restored),
        archive.join(&restored),
    )
    .unwrap();

    let rows = roster(root, "hetz");
    // Sorted by bus id, spanning hosts.
    let ids: Vec<&str> = rows.iter().map(|r| r.identity.as_str()).collect();
    assert_eq!(
        ids,
        ["hetz.cos-claude", "hetz.st2-claude", "silber.fabric-claude"]
    );

    let st2c = rows
        .iter()
        .find(|r| r.identity == "hetz.st2-claude")
        .unwrap();
    assert_eq!(st2c.status, State::Busy);
    assert_eq!(st2c.name.as_deref(), Some("st2 owner"));
    assert_eq!(st2c.description.as_deref(), Some("Own st2 delivery"));
    assert!(!st2c.retired);
    assert_eq!(
        st2c.inbox, 1,
        "the same-filename archive receipt suppresses the raw inbox duplicate"
    );
    assert!(
        st2c.last_activity_ms.is_some(),
        "an agent with a status + inbox has activity"
    );

    let cos = rows
        .iter()
        .find(|r| r.identity == "hetz.cos-claude")
        .unwrap();
    assert_eq!(cos.status, State::Available);
    assert_eq!(cos.name, None);
    assert!(!cos.retired);
    assert_eq!(cos.inbox, 0);

    // fabric-claude: no status file, nothing touched → offline, no activity.
    let fab = rows
        .iter()
        .find(|r| r.identity == "silber.fabric-claude")
        .unwrap();
    assert_eq!(fab.status, State::Offline);
    assert!(!fab.retired);
    assert_eq!(fab.last_activity_ms, None);
}

#[test]
fn roster_json_and_human_output_distinguish_retirement_from_presence() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "h/live/agent.kdl", &agent_kdl("live", "h"));
    write(
        root,
        "h/retired/agent.kdl",
        &retired_agent_kdl("retired", "h"),
    );
    set_state(&status_path(&root.join("h/live")), State::Available).unwrap();
    set_state(&status_path(&root.join("h/retired")), State::Busy).unwrap();

    let rows = roster(root, "h");
    assert!(
        !rows
            .iter()
            .find(|row| row.identity == "h.live")
            .unwrap()
            .retired
    );
    let retired = rows.iter().find(|row| row.identity == "h.retired").unwrap();
    assert!(retired.retired);
    assert_eq!(
        retired.status,
        State::Busy,
        "retirement is declaration state, not a replacement presence value"
    );

    let json = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--json", "--enrich"])
        .output()
        .unwrap();
    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.live");
    assert_eq!(rows[0]["retired"], false);
    assert_eq!(
        rows[0]["resources"],
        serde_json::json!([{
            "name": "work",
            "_tag": "vendor-specific",
            "uri": "issue://example/live"
        }])
    );
    assert_eq!(rows[1]["identity"], "h.retired");
    assert_eq!(rows[1]["status"], "busy");
    assert_eq!(rows[1]["retired"], true);

    let human = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h"])
        .output()
        .unwrap();
    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    assert_eq!(
        String::from_utf8(human.stdout).unwrap(),
        "h.live\tavailable\t\t\nh.retired\tbusy\t\t\t[retired]\n"
    );
}

/// A status file older than the stale window projects as `unknown` in the roster, no matter its value.
#[test]
fn roster_derives_unknown_from_a_stale_status() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "hetz/idle/agent.kdl", &agent_kdl("idle", "hetz"));
    let sp = status_path(&root.join("hetz/idle"));
    set_state(&sp, State::Available).unwrap();

    // Fresh → available.
    assert_eq!(roster(root, "hetz")[0].status, State::Available);

    // Backdate the status file past the stale window → unknown.
    let old = SystemTime::now() - st2::status::STATUS_STALE - Duration::from_secs(60);
    fs::File::open(&sp).unwrap().set_modified(old).unwrap();
    assert_eq!(roster(root, "hetz")[0].status, State::Unknown);
}
