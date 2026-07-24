//! M2.3 integration: presence status + the agent roster over a discovered catalog. Unit mechanics
//! (state parse, staleness, atomic set/refresh) live in `src/status.rs`; this covers the composition
//! `st2 agents` relies on — enumerate specs, read each agent's `status`/`name`/inbox, project the
//! roster, and derive `unknown` from staleness.

use std::fs;
use std::path::Path;
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
         type \"service\"\n  pty \"agent\" {{ command \"exec claude boot\" }}\n}}\n"
    )
}

/// The roster enumerates every catalog agent by bus id (sorted), projects each one's presence, and —
/// with enrich data — its inbox count and last-activity.
#[test]
fn roster_projects_presence_name_and_enrich_across_the_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "hetz/st2-claude/agent.kdl", &agent_kdl("st2-claude", "hetz"));
    write(root, "hetz/cos-claude/agent.kdl", &agent_kdl("cos-claude", "hetz"));
    write(root, "silber/fabric-claude/agent.kdl", &agent_kdl("fabric-claude", "silber"));

    // Presence: st2-claude busy, cos-claude available, fabric-claude unset (→ offline). A display name
    // and an inbox message for st2-claude.
    set_state(&status_path(&root.join("hetz/st2-claude")), State::Busy).unwrap();
    set_state(&status_path(&root.join("hetz/cos-claude")), State::Available).unwrap();
    write(root, "hetz/st2-claude/name", "st2 owner\n");
    send_to_inbox(&st2::message::inbox_dir(&root.join("hetz/st2-claude")), "hetz.cos-claude", None, None, &[], "hi")
        .unwrap();

    let rows = roster(root, "hetz");
    // Sorted by bus id, spanning hosts.
    let ids: Vec<&str> = rows.iter().map(|r| r.identity.as_str()).collect();
    assert_eq!(ids, ["hetz.cos-claude", "hetz.st2-claude", "silber.fabric-claude"]);

    let st2c = rows.iter().find(|r| r.identity == "hetz.st2-claude").unwrap();
    assert_eq!(st2c.status, State::Busy);
    assert_eq!(st2c.name.as_deref(), Some("st2 owner"));
    assert_eq!(st2c.inbox, 1);
    assert!(st2c.last_activity_ms.is_some(), "an agent with a status + inbox has activity");

    let cos = rows.iter().find(|r| r.identity == "hetz.cos-claude").unwrap();
    assert_eq!(cos.status, State::Available);
    assert_eq!(cos.name, None);
    assert_eq!(cos.inbox, 0);

    // fabric-claude: no status file, nothing touched → offline, no activity.
    let fab = rows.iter().find(|r| r.identity == "silber.fabric-claude").unwrap();
    assert_eq!(fab.status, State::Offline);
    assert_eq!(fab.last_activity_ms, None);
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
