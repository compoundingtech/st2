//! M2.3 integration: presence status + the agent roster over a discovered catalog. Unit mechanics
//! (state parse, staleness, atomic set/refresh) live in `src/status.rs`; this covers the composition
//! `st2 agents` relies on — enumerate specs, read each agent's status/presentation/inbox, project the
//! roster, and derive `unknown` from staleness.

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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

fn suspended_agent_kdl(identity: &str, host: &str) -> String {
    agent_kdl(identity, host).replace(
        "  type \"service\"\n",
        "  type \"service\"\n  desired-state \"suspended\" reason=\"Waiting for capacity\"\n",
    )
}

#[test]
fn roster_keeps_presence_separate_from_suspended_desired_state() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "h/worker/agent.kdl",
        &suspended_agent_kdl("worker", "h"),
    );
    set_state(&status_path(&root.join("h/worker")), State::Available).unwrap();

    let rows = roster(root, "h");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, State::Available);
    assert!(!rows[0].retired);
    assert_eq!(rows[0].desired_state, "suspended");
    assert_eq!(
        rows[0].desired_state_reason.as_deref(),
        Some("Waiting for capacity")
    );
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

    let selected = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--identity", "h.live", "--json"])
        .output()
        .unwrap();
    assert!(
        selected.status.success(),
        "{}",
        String::from_utf8_lossy(&selected.stderr)
    );
    let selected: serde_json::Value = serde_json::from_slice(&selected.stdout).unwrap();
    assert_eq!(selected.as_array().unwrap().len(), 1);
    assert_eq!(selected[0]["identity"], "h.live");

    let absent = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--identity", "h.missing", "--json"])
        .output()
        .unwrap();
    assert!(!absent.status.success());
    assert!(
        String::from_utf8_lossy(&absent.stderr)
            .contains("expected exactly one Agent Spec with identity `h.missing`, found 0")
    );

    let filtered_out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args([
            "--host",
            "h",
            "--identity",
            "h.live",
            "--status",
            "busy",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!filtered_out.status.success());
    assert!(
        String::from_utf8_lossy(&filtered_out.stderr)
            .contains("Agent Spec `h.live` does not match status `busy`")
    );

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

#[test]
fn exact_identity_rejects_duplicates_before_status_filtering() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "declarations/one/agent.kdl",
        &agent_kdl("worker", "h"),
    );
    write(
        root,
        "declarations/two/agent.kdl",
        &agent_kdl("worker", "h"),
    );
    set_state(
        &status_path(&root.join("declarations/one")),
        State::Busy,
    )
    .unwrap();
    set_state(
        &status_path(&root.join("declarations/two")),
        State::Available,
    )
    .unwrap();

    let selected = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args([
            "--host",
            "h",
            "--identity",
            "h.worker",
            "--status",
            "busy",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!selected.status.success());
    assert!(
        String::from_utf8_lossy(&selected.stderr)
            .contains("expected exactly one Agent Spec with identity `h.worker`, found 2")
    );
}

#[test]
fn exact_identity_rejects_incomplete_catalog_discovery() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(root, "valid/worker.kdl", &agent_kdl("worker", "h"));
    write(root, "h/worker/agent.kdl", "this is malformed KDL {");

    let selected = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--identity", "h.worker", "--json"])
        .output()
        .unwrap();
    assert!(!selected.status.success());
    let stderr = String::from_utf8_lossy(&selected.stderr);
    assert!(
        stderr.contains("cannot select an exact Agent Spec while catalog discovery has 1 error")
    );
    assert!(stderr.contains("h/worker/agent.kdl"));

    let ordinary = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--json"])
        .output()
        .unwrap();
    assert!(ordinary.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&ordinary.stdout).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
}

#[cfg(unix)]
#[test]
fn exact_identity_rejects_incomplete_catalog_traversal() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let unreadable = root.join("unreadable");
    write(root, "valid/worker.kdl", &agent_kdl("worker", "h"));
    write(root, "unreadable/hidden.kdl", &agent_kdl("worker", "h"));
    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read_dir(&unreadable).is_ok() {
        // Root or CAP_DAC_OVERRIDE can bypass mode 000, so this environment cannot induce the
        // traversal failure the test is intended to exercise.
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }

    let selected = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--identity", "h.worker", "--json"])
        .output()
        .unwrap();

    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(!selected.status.success());
    let stderr = String::from_utf8_lossy(&selected.stderr);
    assert!(
        stderr.contains("cannot select an exact Agent Spec while catalog discovery has 1 error")
    );
    assert!(stderr.contains("unreadable"));
    assert!(stderr.contains("catalog directory traversal failed"));
}

#[test]
fn exact_identity_selects_one_of_multiple_agent_specs_in_one_declaration() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "h/pair.kdl",
        r#"
agent "one" {
  host "h"
  name "First Agent Spec"
  pty "agent" { command "true" }
}
agent "two" {
  host "h"
  name "Second Agent Spec"
  pty "agent" { command "true" }
}
"#,
    );

    let selected = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("agents")
        .arg(root)
        .args(["--host", "h", "--identity", "h.two", "--json"])
        .output()
        .unwrap();
    assert!(
        selected.status.success(),
        "{}",
        String::from_utf8_lossy(&selected.stderr)
    );
    let selected: serde_json::Value = serde_json::from_slice(&selected.stdout).unwrap();
    assert_eq!(selected.as_array().unwrap().len(), 1);
    assert_eq!(selected[0]["identity"], "h.two");
    assert_eq!(selected[0]["name"], "Second Agent Spec");
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
