//! M1 correctness net: discovery + lowering of VRS `agent.kdl` jobs (spec.md §1–2, §4).
//!
//! Builds throwaway catalog folders, writes real job files (KDL/TOML/JSON, services), and
//! asserts they lower per the spec: `pty`/`exec` task split, `restart{}`, `type`, `workspace`,
//! `supervisor`; render-only fields ignored; content/path precedence; malformed → error, not halt.

use std::fs;
use std::path::Path;
use std::time::Duration;

use agent_spec::spec::TaskKind;
use agent_spec::{AgentSpec, JobType, Task, discover};

fn write(root: &Path, rel: &str, contents: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn find<'a>(specs: &'a [AgentSpec], identity: &str) -> &'a AgentSpec {
    specs
        .iter()
        .find(|s| s.identity == identity)
        .unwrap_or_else(|| panic!("expected a spec with identity '{identity}'"))
}

fn argv(task: &Task) -> Vec<&str> {
    task.argv
        .as_deref()
        .unwrap()
        .iter()
        .map(String::as_str)
        .collect()
}

const FULL_KDL: &str = r####"
// The agent is the "job"; pty/exec are the tasks.
agent "fabric-claude" {
  identity  "fabric-claude"
  host      "silber"
  type      "service"
  workspace "/repos/fabric"
  supervisor "cos"
  retired   #false

  restart { attempts 5; interval "90s"; delay "5s"; mode "fail" }

  // Render-only fields — must be ignored by st2:
  harness "claude"
  model   "opus"
  role    "worker"
  persona "worker"

  pty "agent" {
    id      "silber.fabric-claude"
    command #"exec claude --permission-mode bypassPermissions 'boot'"#
    tags role="agent" env="prod"
    env {
      ST_AGENT "silber.fabric-claude"
      ST_ROOT  "$CATALOG"
    }
  }

  exec "ding" {
    command #"st2 ding silber.fabric --identity silber.fabric-claude"#
    keep #true
  }

  meta { tier "worker" }
}
"####;

#[test]
fn parses_full_kdl_service_job() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/silber/fabric-claude/agent.kdl",
        FULL_KDL,
    );

    let found = discover(tmp.path());
    assert!(
        found.errors.is_empty(),
        "unexpected errors: {:?}",
        found.errors
    );
    assert_eq!(found.specs.len(), 1);
    let s = &found.specs[0];

    assert_eq!(s.identity, "fabric-claude");
    assert_eq!(s.host.as_deref(), Some("silber"));
    assert_eq!(s.job_type, JobType::Service);
    assert_eq!(s.workspace.as_deref(), Some("/repos/fabric"));
    assert_eq!(s.supervisor.as_deref(), Some("cos"));
    assert!(!s.retired);

    // restart{} parsed with duration strings
    let r = s.restart.clone().unwrap();
    assert_eq!(r.attempts, 5);
    assert_eq!(r.interval, Duration::from_secs(90));
    assert_eq!(r.delay, Duration::from_secs(5));
    assert_eq!(r.mode, agent_spec::RestartMode::Fail);

    // tasks: pty "agent" + exec "ding" (sorted by name)
    assert_eq!(s.tasks.len(), 2);
    let agent = s.tasks.iter().find(|t| t.name == "agent").unwrap();
    assert_eq!(agent.kind, TaskKind::Pty);
    assert_eq!(agent.id.as_deref(), Some("silber.fabric-claude"));
    assert!(agent.command.as_deref().unwrap().starts_with("exec claude"));
    assert_eq!(agent.tags.get("role").map(String::as_str), Some("agent"));
    assert_eq!(
        agent.env.get("ST_ROOT").map(String::as_str),
        Some("$CATALOG")
    );

    let ding = s.tasks.iter().find(|t| t.name == "ding").unwrap();
    assert_eq!(ding.kind, TaskKind::Exec);
    assert!(ding.keep);
    assert!(ding.command.as_deref().unwrap().starts_with("st2 ding"));

    assert!(s.is_runnable());
}

#[test]
fn compact_agent_command_and_ding_lower_to_native_tasks() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/Silber/cos/agent.kdl",
        r#"
agent "cos" {
  host "Silber"
  workspace "/Volumes/SSD/src/github.com/myobie/cos"
  env { ST_AGENT "Silber.cos" }
  command "exec codex boot"
  ding
}
"#,
    );

    let found = discover(tmp.path());
    assert!(
        found.errors.is_empty(),
        "unexpected errors: {:?}",
        found.errors
    );
    let spec = &found.specs[0];
    assert_eq!(spec.tasks.len(), 2);

    let agent = spec.tasks.iter().find(|t| t.name == "agent").unwrap();
    assert_eq!(agent.kind, TaskKind::Pty);
    assert!(!agent.derived);
    assert_eq!(agent.id.as_deref(), Some("Silber.cos"));
    assert_eq!(agent.command.as_deref(), Some("exec codex boot"));
    assert_eq!(
        agent.env.get("ST_AGENT").map(String::as_str),
        Some("Silber.cos")
    );

    let ding = spec.tasks.iter().find(|t| t.name == "ding").unwrap();
    assert_eq!(ding.kind, TaskKind::Exec);
    assert!(ding.derived);
    assert_eq!(ding.id.as_deref(), Some("Silber.cos.ding"));
    assert_eq!(
        ding.command.as_deref(),
        Some("st2 ding --identity Silber.cos --root $ST_ROOT")
    );
    assert_eq!(
        ding.env.get("ST_AGENT").map(String::as_str),
        Some("Silber.cos")
    );
}

#[test]
fn direct_argv_lowers_for_compact_and_explicit_kdl_tasks() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/compact/agent.kdl",
        r#"agent "compact" {
  host "h"
  argv "axe" "agent" "exec" "--" "claude" "--resume" "session id"
}"#,
    );
    write(
        tmp.path(),
        "agents/h/explicit/agent.kdl",
        r#"agent "explicit" {
  host "h"
  pty "agent" { argv "/opt/bin/codex" "--model" "gpt 5" }
  exec "probe" { argv "printf" "%s" "$CATALOG" }
}"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);

    let compact = find(&found.specs, "compact");
    assert_eq!(
        argv(&compact.tasks[0]),
        [
            "axe",
            "agent",
            "exec",
            "--",
            "claude",
            "--resume",
            "session id"
        ]
    );
    assert!(compact.tasks[0].command.is_none());

    let explicit = find(&found.specs, "explicit");
    assert_eq!(
        argv(
            explicit
                .tasks
                .iter()
                .find(|task| task.name == "agent")
                .unwrap()
        ),
        ["/opt/bin/codex", "--model", "gpt 5"]
    );
    assert_eq!(
        argv(
            explicit
                .tasks
                .iter()
                .find(|task| task.name == "probe")
                .unwrap()
        ),
        ["printf", "%s", "$CATALOG"]
    );
}

#[test]
fn direct_argv_lowers_from_toml_and_json() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/toml/agent.toml",
        r#"identity = "toml"
host = "h"
argv = ["claude", "--resume", "session id"]
"#,
    );
    write(
        tmp.path(),
        "agents/h/json/agent.json",
        r#"{"identity":"json","host":"h","pty":{"agent":{"argv":["codex","resume","abc"]}}}"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert_eq!(
        argv(&find(&found.specs, "toml").tasks[0]),
        ["claude", "--resume", "session id"]
    );
    assert_eq!(
        argv(&find(&found.specs, "json").tasks[0]),
        ["codex", "resume", "abc"]
    );
}

#[test]
fn empty_or_ambiguous_argv_is_rejected_in_every_task_shape() {
    let tmp = tempfile::tempdir().unwrap();
    for (identity, body) in [
        ("empty-compact", r#"argv"#),
        ("empty-program-compact", r#"argv """#),
        (
            "both-compact",
            r#"command "true"
  argv "true""#,
        ),
        ("empty-explicit", r#"pty "agent" { argv }"#),
        (
            "empty-program-explicit",
            r#"pty "agent" { argv "" }"#,
        ),
        (
            "both-explicit",
            r#"pty "agent" { command "true"; argv "true" }"#,
        ),
    ] {
        write(
            tmp.path(),
            &format!("agents/h/{identity}/agent.kdl"),
            &format!("agent \"{identity}\" {{ host \"h\"; {body} }}"),
        );
    }

    let found = discover(tmp.path());
    assert_eq!(found.errors.len(), 6, "{:?}", found.errors);
    let messages = found
        .errors
        .iter()
        .map(|error| error.message.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.ends_with("empty `argv`"))
            .count(),
        2
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.contains("both `command` and `argv`"))
            .count(),
        2
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.contains("empty `argv` program"))
            .count(),
        2
    );
}

#[test]
fn compact_and_explicit_agent_task_forms_cannot_be_mixed() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/ambiguous/agent.kdl",
        r#"agent "ambiguous" {
  host "hetz"
  command "compact"
  pty "agent" { command "explicit" }
}"#,
    );
    let found = discover(tmp.path());
    assert_eq!(found.errors.len(), 1);
    assert!(
        found.errors[0]
            .message
            .contains("declares both a compact launch")
    );
}

#[test]
fn parses_toml_service_job_with_pty_and_exec_tables() {
    let tmp = tempfile::tempdir().unwrap();
    let toml = r#"
identity = "fetcher"
host     = "hetz"
type     = "service"
workspace = "/repos/fetcher"

[restart]
attempts = 3
interval = "60s"
mode = "delay"

[pty.agent]
id = "hetz.fetcher"
command = "exec claude 'boot'"

[exec.ding]
command = "st2 ding hetz.fetcher"
"#;
    write(tmp.path(), "agents/hetz/fetcher/agent.toml", toml);

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert_eq!(found.specs.len(), 1);
    let s = &found.specs[0];
    assert_eq!(s.identity, "fetcher");
    assert_eq!(s.job_type, JobType::Service);
    assert_eq!(
        s.restart.clone().unwrap().mode,
        agent_spec::RestartMode::Delay
    );
    assert_eq!(s.tasks.len(), 2);
    assert_eq!(
        s.tasks.iter().find(|t| t.name == "agent").unwrap().kind,
        TaskKind::Pty
    );
    assert_eq!(
        s.tasks.iter().find(|t| t.name == "ding").unwrap().kind,
        TaskKind::Exec
    );
}

#[test]
fn parses_json_service_job() {
    let tmp = tempfile::tempdir().unwrap();
    let json = r#"{
        "identity": "reporter", "host": "hetz", "type": "service",
        "pty":  { "agent": { "command": "exec claude 'boot'" } },
        "exec": { "ding":  { "command": "st2 ding hetz.reporter" } }
    }"#;
    write(tmp.path(), "agents/hetz/reporter/agent.json", json);

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert_eq!(found.specs.len(), 1);
    assert_eq!(found.specs[0].tasks.len(), 2);
}

#[test]
fn type_defaults_to_service() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/svc/agent.toml",
        "identity=\"svc\"\n[pty.agent]\ncommand=\"x\"\n",
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert_eq!(find(&found.specs, "svc").job_type, JobType::Service); // omitted → service
}

#[test]
fn path_supplies_identity_and_host_when_content_omits_them() {
    let tmp = tempfile::tempdir().unwrap();
    let minimal = "type \"service\"\npty \"agent\" { command \"exec claude 'boot'\" }";
    write(
        tmp.path(),
        "agents/hetz/st2-claude/agent.kdl",
        &format!("agent {{\n{minimal}\n}}"),
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert_eq!(found.specs.len(), 1);
    let s = &found.specs[0];
    assert_eq!(s.identity, "st2-claude");
    assert_eq!(s.host.as_deref(), Some("hetz"));
    assert_eq!(s.bus_id("fallback"), "hetz.st2-claude");
}

#[test]
fn content_wins_over_path_and_mismatch_warns() {
    let tmp = tempfile::tempdir().unwrap();
    let spec = r#"
identity = "real-name"
host     = "real-host"
[pty.agent]
command = "exec claude 'boot'"
"#;
    write(tmp.path(), "agents/wrong-host/wrong-name/agent.toml", spec);

    let found = discover(tmp.path());
    assert!(found.errors.is_empty());
    let s = &found.specs[0];
    assert_eq!(s.identity, "real-name");
    assert_eq!(s.host.as_deref(), Some("real-host"));
    assert_eq!(found.warnings.len(), 2);
    assert!(
        found
            .warnings
            .iter()
            .any(|w| w.contains("identity mismatch"))
    );
    assert!(found.warnings.iter().any(|w| w.contains("host mismatch")));
}

#[test]
fn malformed_file_is_collected_as_error_and_does_not_halt_the_walk() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/good/agent.toml",
        "identity=\"good\"\n[pty.agent]\ncommand=\"x\"\n",
    );
    write(
        tmp.path(),
        "agents/hetz/bad/agent.toml",
        "identity = \"broken\"\nthis is not valid =",
    );

    let found = discover(tmp.path());
    assert_eq!(found.specs.len(), 1);
    assert_eq!(found.specs[0].identity, "good");
    assert_eq!(found.errors.len(), 1);
    assert!(found.errors[0].message.contains("TOML parse error"));
}

#[test]
fn malformed_kdl_is_collected_as_error() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/bad/agent.kdl",
        "agent \"broken\" { this is { not valid",
    );
    let found = discover(tmp.path());
    assert!(found.specs.is_empty());
    assert_eq!(found.errors.len(), 1);
    assert!(found.errors[0].message.contains("KDL parse error"));
}

#[test]
fn non_spec_files_are_skipped_silently() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/x/package.json",
        r#"{"name":"x","version":"1.0.0"}"#,
    );
    write(
        tmp.path(),
        "agents/hetz/x/agent.toml",
        "identity=\"x\"\n[pty.agent]\ncommand=\"c\"\n",
    );

    let found = discover(tmp.path());
    assert_eq!(found.specs.len(), 1, "only the real job, not package.json");
    assert_eq!(found.specs[0].identity, "x");
    assert!(found.errors.is_empty());
}

#[test]
fn render_only_fields_are_ignored_not_errored() {
    let tmp = tempfile::tempdir().unwrap();
    // A job carrying every render-only field — st2 must parse it cleanly, ignoring them.
    let toml = r#"
identity = "rendered"
host = "hetz"
harness = "claude"
model = "opus"
role = "worker"
persona = "worker"
transport = "ding"
strategy = "permanent"
[permissions]
read = ["a", "b"]
[meta]
tier = "worker"
[pty.agent]
command = "exec claude 'boot'"
"#;
    write(tmp.path(), "agents/hetz/rendered/agent.toml", toml);

    let found = discover(tmp.path());
    assert!(
        found.errors.is_empty(),
        "render-only fields must not error: {:?}",
        found.errors
    );
    assert_eq!(found.specs.len(), 1);
    assert_eq!(found.specs[0].tasks.len(), 1);
}

#[test]
fn unrendered_job_without_command_is_flagged_not_runnable() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/needs-render/agent.toml",
        "identity=\"needs-render\"\ntype=\"service\"\n",
    );
    let found = discover(tmp.path());
    assert_eq!(found.specs.len(), 1);
    assert!(!found.specs[0].is_runnable());
}

#[test]
fn keep_and_retired_are_parsed() {
    let tmp = tempfile::tempdir().unwrap();
    let toml = r#"
identity = "old"
retired = true
keep = true
[pty.agent]
command = "x"
"#;
    write(tmp.path(), "agents/hetz/old/agent.toml", toml);
    let found = discover(tmp.path());
    let s = &found.specs[0];
    assert!(s.retired);
    assert!(s.keep);
}

#[test]
fn multiple_agent_nodes_in_one_kdl_file_yield_multiple_specs() {
    let tmp = tempfile::tempdir().unwrap();
    let kdl = r#"
agent "one" { host "hetz" pty "agent" { command "x" } }
agent "two" { host "hetz" pty "agent" { command "y" } }
"#;
    write(tmp.path(), "agents/hetz/pair.kdl", kdl);
    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert_eq!(found.specs.len(), 2);
    assert!(found.specs.iter().any(|s| s.identity == "one"));
    assert!(found.specs.iter().any(|s| s.identity == "two"));
}

#[test]
fn nonexistent_root_yields_empty_not_error() {
    let found = discover(Path::new("/no/such/catalog/anywhere"));
    assert!(found.specs.is_empty());
    assert!(found.errors.is_empty());
}

#[test]
fn hidden_runner_state_and_resources_are_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/a/agent.toml",
        "identity=\"a\"\n[pty.agent]\ncommand=\"x\"\n",
    );
    // dot-prefixed runner state (R03) + a resource message — neither is a spec.
    write(tmp.path(), ".st2.hetz.lock", "12345");
    write(
        tmp.path(),
        "agents/hetz/a/resources/inbox/1784-abc.md",
        "a message",
    );
    // The canonical PTY_ROOT is `<catalog>/pty`. Its session JSON contains command/cwd fields and
    // must never be mistaken for a catalog agent declaration.
    write(
        tmp.path(),
        "pty/hetz.a.json",
        r#"{"name":"hetz.a","status":"running","command":"sh -c x","cwd":"/tmp"}"#,
    );

    let found = discover(tmp.path());
    assert_eq!(found.specs.len(), 1);
    assert_eq!(found.specs[0].identity, "a");
}
