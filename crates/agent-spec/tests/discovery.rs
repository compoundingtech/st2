//! M1 correctness net: discovery + lowering of VRS `agent.kdl` jobs (spec.md §1–2, §4).
//!
//! Builds throwaway catalog folders, writes real job files (KDL/TOML/JSON, services), and
//! asserts they lower per the spec: `pty`/`exec` task split, `restart{}`, `type`, `workspace`,
//! `supervisor`; render-only fields ignored; content/path precedence; malformed → error, not halt.

use std::fs;
use std::path::Path;
use std::time::Duration;

use agent_spec::spec::{
    ClaudeDriver, CodexDriver, DeliveryTransport, Driver, PiDriver, TaskKind, TaskLifecycle,
};
use agent_spec::{
    AgentDesiredState, AgentSpec, JobType, Resource, Task, discover, discover_strict,
};

#[test]
fn desired_state_is_typed_and_legacy_retirement_remains_readable() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/suspended/agent.kdl",
        r#"agent "suspended" { desired-state "suspended" reason="Waiting for capacity"; argv "true" }"#,
    );
    write(
        tmp.path(),
        "agents/h/retired/agent.kdl",
        r#"agent "retired" { retired #true; argv "true" }"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert!(matches!(
        &find(&found.specs, "suspended").desired_state,
        AgentDesiredState::Suspended { reason } if reason == "Waiting for capacity"
    ));
    assert!(matches!(
        &find(&found.specs, "retired").desired_state,
        AgentDesiredState::Retired { reason: None }
    ));
}

#[test]
fn desired_state_rejects_illegal_state_reason_combinations() {
    for (name, lifecycle) in [
        ("missing-reason", "desired-state \"suspended\""),
        ("running-reason", "desired-state \"running\" reason=\"no\""),
        ("unknown", "desired-state \"paused\" reason=\"no\""),
        ("non-string-state", "desired-state #true"),
        (
            "non-string-reason",
            "desired-state \"suspended\" reason=#true",
        ),
        ("unknown-property", "desired-state \"running\" because=\"no\""),
        (
            "duplicate-reason",
            "desired-state \"suspended\" reason=\"one\" reason=\"two\"",
        ),
        ("typed", "(state)desired-state \"running\""),
        ("empty-reason", "desired-state \"suspended\" reason=\"\""),
        (
            "line-separator",
            "desired-state \"suspended\" reason=\"left right\"",
        ),
        (
            "mixed",
            "retired #false; desired-state \"suspended\" reason=\"no\"",
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            &format!("agents/h/{name}/agent.kdl"),
            &format!("agent \"{name}\" {{ {lifecycle}; argv \"true\" }}"),
        );
        let found = discover(tmp.path());
        assert_eq!(found.errors.len(), 1, "{name}: {:?}", found.errors);
    }
}

#[test]
fn desired_state_has_equivalent_toml_and_json_lowering() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/toml/agent.toml",
        "identity = \"toml\"\nhost = \"h\"\ndesired_state = \"suspended\"\ndesired_state_reason = \"Waiting for capacity\"\nargv = [\"true\"]\n",
    );
    write(
        tmp.path(),
        "agents/h/json/agent.json",
        r#"{"identity":"json","host":"h","desired_state":"retired","desired_state_reason":"Mission complete","argv":["true"]}"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert!(matches!(
        &find(&found.specs, "toml").desired_state,
        AgentDesiredState::Suspended { reason } if reason == "Waiting for capacity"
    ));
    assert!(matches!(
        &find(&found.specs, "json").desired_state,
        AgentDesiredState::Retired { reason: Some(reason) } if reason == "Mission complete"
    ));
}

#[test]
fn lifecycle_fields_make_path_placed_files_agent_candidates() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/suspended/agent.toml",
        "desired_state = \"suspended\"\ndesired_state_reason = \"Waiting for capacity\"\n",
    );
    write(
        tmp.path(),
        "agents/h/retired/agent.json",
        r#"{"retired":true}"#,
    );
    write(
        tmp.path(),
        "agents/h/orphan-reason/agent.json",
        r#"{"desired_state_reason":"Missing state"}"#,
    );

    let found = discover(tmp.path());
    assert!(matches!(
        &find(&found.specs, "suspended").desired_state,
        AgentDesiredState::Suspended { reason } if reason == "Waiting for capacity"
    ));
    assert!(find(&found.specs, "retired").desired_state.is_retired());
    assert_eq!(found.errors.len(), 1, "{:?}", found.errors);
    assert!(
        found.errors[0]
            .message
            .contains("lifecycle `reason` requires `desired-state`"),
        "{:?}",
        found.errors
    );
}

#[test]
fn explicit_json_null_fields_are_rejected_instead_of_granting_default_behavior() {
    for (name, lifecycle) in [
        ("null-retired", r#""retired":null"#),
        ("null-state", r#""desired_state":null"#),
        ("null-deliver", r#""deliver":null"#),
        (
            "null-reason",
            r#""desired_state":"suspended","desired_state_reason":null"#,
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            &format!("agents/h/{name}/agent.json"),
            &format!(
                r#"{{"identity":"{name}","host":"h",{lifecycle},"argv":["true"]}}"#
            ),
        );
        let found = discover(tmp.path());
        assert!(found.specs.is_empty(), "{name}: {:?}", found.specs);
        assert_eq!(found.errors.len(), 1, "{name}: {:?}", found.errors);
        assert!(
            found.errors[0].message.contains("must not be null"),
            "{name}: {:?}",
            found.errors
        );
    }
}

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
    lifecycle "adopt-only"
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
    assert!(!s.desired_state.is_retired());

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
    assert_eq!(agent.lifecycle, TaskLifecycle::AdoptOnly);
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
    assert!(spec.delivery.is_none());
    assert!(spec.has_delivery_transport());
}

#[test]
fn deliver_is_typed_without_lowering_to_the_legacy_ding_task() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/claude/agent.kdl",
        r#"agent "claude" { host "h"; command "claude"; deliver "mcp" }"#,
    );
    write(
        tmp.path(),
        "agents/h/codex/agent.kdl",
        r#"agent "codex" { host "h"; command "codex"; deliver "app-server" }"#,
    );
    write(
        tmp.path(),
        "agents/h/pi/agent.kdl",
        r#"agent "pi" { host "h"; command "pi"; deliver "pi-channel" }"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let claude = find(&found.specs, "claude");
    let codex = find(&found.specs, "codex");
    let pi = find(&found.specs, "pi");
    assert_eq!(claude.delivery, Some(DeliveryTransport::Mcp));
    assert_eq!(codex.delivery, Some(DeliveryTransport::AppServer));
    assert_eq!(pi.delivery, Some(DeliveryTransport::PiChannel));
    assert_eq!(claude.delivery.unwrap().as_str(), "mcp");
    assert_eq!(codex.delivery.unwrap().as_str(), "app-server");
    assert_eq!(pi.delivery.unwrap().as_str(), "pi-channel");
    for spec in [claude, codex, pi] {
        assert!(spec.has_delivery_transport());
        assert_eq!(spec.tasks.len(), 1);
        assert!(spec.tasks.iter().all(|task| !task.derived));
    }
}

#[test]
fn deliver_rejects_unknown_duplicate_mixed_and_malformed_declarations() {
    for (name, declaration, expected) in [
        (
            "unknown",
            r#"agent "worker" { command "true"; deliver "socket" }"#,
            "unsupported `deliver` value 'socket'",
        ),
        (
            "duplicate",
            r#"agent "worker" { command "true"; deliver "mcp"; deliver "app-server" }"#,
            "declares `deliver` more than once",
        ),
        (
            "mixed",
            r#"agent "worker" { command "true"; ding; deliver "mcp" }"#,
            "declares both `ding` and `deliver`",
        ),
        (
            "missing",
            r#"agent "worker" { command "true"; deliver }"#,
            "must contain exactly one positional string",
        ),
        (
            "non-string",
            r#"agent "worker" { command "true"; deliver #true }"#,
            "value must be a string",
        ),
        (
            "property",
            r#"agent "worker" { command "true"; deliver "mcp" mode="extra" }"#,
            "must contain exactly one positional string",
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            &format!("agents/h/{name}/agent.kdl"),
            declaration,
        );
        let found = discover(tmp.path());
        assert!(found.specs.is_empty(), "{name}: {:?}", found.specs);
        assert_eq!(found.errors.len(), 1, "{name}: {:?}", found.errors);
        assert!(
            found.errors[0].message.contains(expected),
            "{name}: expected {expected:?}, got {:?}",
            found.errors[0]
        );
    }
}

#[test]
fn compact_adopt_only_lifecycle_lowers_to_the_generated_agent_task() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/migrant/agent.kdl",
        r#"
agent "migrant" {
  host "h"
  lifecycle "adopt-only"
  command "codex"
}
"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert_eq!(found.specs[0].tasks[0].lifecycle, TaskLifecycle::AdoptOnly);
}

#[test]
fn unknown_task_lifecycle_is_rejected_instead_of_falling_back_to_service() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/unsafe/agent.kdl",
        r#"
agent "unsafe" {
  host "h"
  lifecycle "replace-maybe"
  command "codex"
}
"#,
    );

    let found = discover(tmp.path());
    assert!(found.specs.is_empty());
    assert_eq!(found.errors.len(), 1);
    assert!(found.errors[0].message.contains("unknown lifecycle"));
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
fn typed_driver_blocks_lower_with_kdl_toml_and_json_parity() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/claude-kdl/agent.kdl",
        r#"agent "claude-kdl" {
  claude {
    model "opus"
    effort "xhigh"
    dev-channels #true
    prompt "Start the assigned work."
    args "--permission-mode" "bypassPermissions"
  }
}"#,
    );
    write(
        tmp.path(),
        "agents/h/claude-toml/agent.toml",
        r#"identity = "claude-toml"

[claude]
model = "opus"
effort = "xhigh"
dev-channels = true
prompt = "Start the assigned work."
args = ["--permission-mode", "bypassPermissions"]
"#,
    );
    write(
        tmp.path(),
        "agents/h/claude-json/agent.json",
        r#"{
  "identity": "claude-json",
  "claude": {
    "model": "opus",
    "effort": "xhigh",
    "dev-channels": true,
    "prompt": "Start the assigned work.",
    "args": ["--permission-mode", "bypassPermissions"]
  }
}"#,
    );
    write(
        tmp.path(),
        "agents/h/codex-kdl/agent.kdl",
        r#"agent "codex-kdl" {
  codex {
    model "gpt-5.6-sol"
    effort "xhigh"
    prompt "Start the assigned work."
    args "--dangerously-bypass-approvals-and-sandbox"
  }
}"#,
    );
    write(
        tmp.path(),
        "agents/h/codex-toml/agent.toml",
        r#"identity = "codex-toml"

[codex]
model = "gpt-5.6-sol"
effort = "xhigh"
prompt = "Start the assigned work."
args = ["--dangerously-bypass-approvals-and-sandbox"]
"#,
    );
    write(
        tmp.path(),
        "agents/h/codex-json/agent.json",
        r#"{
  "identity": "codex-json",
  "codex": {
    "model": "gpt-5.6-sol",
    "effort": "xhigh",
    "prompt": "Start the assigned work.",
    "args": ["--dangerously-bypass-approvals-and-sandbox"]
  }
}"#,
    );

    write(
        tmp.path(),
        "agents/h/pi-kdl/agent.kdl",
        r#"agent "pi-kdl" {
  pi {
    model "anthropic/claude-opus-5"
    effort "high"
    prompt "Start the assigned work."
    args "--tools" "read,bash,edit,write"
  }
}"#,
    );
    write(
        tmp.path(),
        "agents/h/pi-toml/agent.toml",
        r#"identity = "pi-toml"

[pi]
model = "anthropic/claude-opus-5"
effort = "high"
prompt = "Start the assigned work."
args = ["--tools", "read,bash,edit,write"]
"#,
    );
    write(
        tmp.path(),
        "agents/h/pi-json/agent.json",
        r#"{
  "identity": "pi-json",
  "pi": {
    "model": "anthropic/claude-opus-5",
    "effort": "high",
    "prompt": "Start the assigned work.",
    "args": ["--tools", "read,bash,edit,write"]
  }
}"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let claude = Driver::Claude(ClaudeDriver {
        model: Some("opus".into()),
        effort: Some("xhigh".into()),
        dev_channels: true,
        prompt: "Start the assigned work.".into(),
        args: vec!["--permission-mode".into(), "bypassPermissions".into()],
    });
    for identity in ["claude-kdl", "claude-toml", "claude-json"] {
        let spec = find(&found.specs, identity);
        assert_eq!(spec.driver.as_ref(), Some(&claude));
        assert!(!spec.is_runnable());
    }
    let codex = Driver::Codex(CodexDriver {
        model: Some("gpt-5.6-sol".into()),
        effort: Some("xhigh".into()),
        prompt: "Start the assigned work.".into(),
        args: vec!["--dangerously-bypass-approvals-and-sandbox".into()],
    });
    for identity in ["codex-kdl", "codex-toml", "codex-json"] {
        let spec = find(&found.specs, identity);
        assert_eq!(spec.driver.as_ref(), Some(&codex));
        assert!(!spec.is_runnable());
    }
    let pi = Driver::Pi(PiDriver {
        model: Some("anthropic/claude-opus-5".into()),
        effort: Some("high".into()),
        prompt: "Start the assigned work.".into(),
        args: vec!["--tools".into(), "read,bash,edit,write".into()],
    });
    for identity in ["pi-kdl", "pi-toml", "pi-json"] {
        let spec = find(&found.specs, identity);
        assert_eq!(spec.driver.as_ref(), Some(&pi));
        assert!(!spec.is_runnable());
    }
}

#[test]
fn driver_blocks_reject_ambiguous_providers_and_untyped_fields() {
    for (name, body) in [
        (
            "both",
            r#"claude { prompt "go" }; codex { prompt "go" }"#,
        ),
        ("claude-and-pi", r#"claude { prompt "go" }; pi { prompt "go" }"#),
        ("codex-and-pi", r#"codex { prompt "go" }; pi { prompt "go" }"#),
        ("pi-dev", r#"pi { dev-channels #true; prompt "go" }"#),
        ("pi-missing-prompt", r#"pi { model "anthropic/x" }"#),
        ("missing-prompt", r#"claude { model "opus" }"#),
        ("wrong-bool", r#"claude { dev-channels "yes"; prompt "go" }"#),
        ("codex-dev", r#"codex { dev-channels #true; prompt "go" }"#),
        ("unknown", r#"claude { presence #true; prompt "go" }"#),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            &format!("agents/h/{name}/agent.kdl"),
            &format!("agent \"{name}\" {{ {body} }}"),
        );
        let found = discover(tmp.path());
        assert!(found.specs.is_empty(), "accepted {name}");
        assert_eq!(found.errors.len(), 1, "{name}: {:?}", found.errors);
    }
}

#[test]
fn presentation_metadata_lowers_from_kdl_toml_and_json_without_changing_identity() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/kdl/agent.kdl",
        r#"agent "kdl" {
  host "h"
  name "Display label"
  description "Enduring responsibility"
  command "true"
}"#,
    );
    write(
        tmp.path(),
        "agents/h/toml/agent.toml",
        r#"identity = "toml"
host = "h"
name = "Display label"
description = "Enduring responsibility"
command = "true"
"#,
    );
    write(
        tmp.path(),
        "agents/h/json/agent.json",
        r#"{"identity":"json","host":"h","name":"Display label","description":"Enduring responsibility","command":"true"}"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    for identity in ["kdl", "toml", "json"] {
        let spec = find(&found.specs, identity);
        assert_eq!(spec.identity, identity);
        assert_eq!(spec.name.as_deref(), Some("Display label"));
        assert_eq!(spec.description.as_deref(), Some("Enduring responsibility"));
    }
}

#[test]
fn malformed_or_duplicate_kdl_presentation_is_rejected() {
    for (case, body) in [
        ("duplicate", "name \"one\"; name \"two\""),
        ("wrong-type", "description 42"),
        ("children", "description { nested \"no\" }"),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            &format!("agents/h/{case}/agent.kdl"),
            &format!("agent {case:?} {{ host \"h\"; {body}; command \"true\" }}"),
        );
        let found = discover(tmp.path());
        assert!(found.specs.is_empty(), "{case}: {:?}", found.specs);
        assert_eq!(found.errors.len(), 1, "{case}: {:?}", found.errors);
        assert!(
            found.errors[0].message.contains("must contain")
                || found.errors[0].message.contains("more than once"),
            "{case}: {}",
            found.errors[0].message
        );
    }
}

#[test]
fn presentation_bounds_count_unicode_scalars_and_reject_noncanonical_values() {
    use agent_spec::spec::{
        AGENT_DESCRIPTION_MAX_CHARS, AGENT_NAME_MAX_CHARS, validate_presentation,
    };

    let name_at_limit = "é".repeat(AGENT_NAME_MAX_CHARS);
    let description_at_limit = "界".repeat(AGENT_DESCRIPTION_MAX_CHARS);
    assert!(validate_presentation("name", Some(&name_at_limit), AGENT_NAME_MAX_CHARS).is_ok());
    assert!(
        validate_presentation(
            "description",
            Some(&description_at_limit),
            AGENT_DESCRIPTION_MAX_CHARS,
        )
        .is_ok()
    );
    assert!(
        validate_presentation(
            "name",
            Some(&format!("{name_at_limit}x")),
            AGENT_NAME_MAX_CHARS,
        )
        .is_err()
    );
    assert!(
        validate_presentation(
            "description",
            Some(&format!("{description_at_limit}x")),
            AGENT_DESCRIPTION_MAX_CHARS,
        )
        .is_err()
    );
    for (field, max_chars) in [
        ("name", AGENT_NAME_MAX_CHARS),
        ("description", AGENT_DESCRIPTION_MAX_CHARS),
    ] {
        assert!(validate_presentation(field, Some(r"slash/name\path"), max_chars).is_ok());
        for invalid in [
            "",
            " leading",
            "trailing ",
            "two\nlines",
            "control\u{7f}",
            "line\u{2028}separator",
            "paragraph\u{2029}separator",
        ] {
            assert!(
                validate_presentation(field, Some(invalid), max_chars).is_err(),
                "accepted {field} {invalid:?}"
            );
        }
    }
}

#[test]
fn presentation_parser_rejects_unicode_line_and_paragraph_separators() {
    for field in ["name", "description"] {
        for separator in ['\u{2028}', '\u{2029}'] {
            let tmp = tempfile::tempdir().unwrap();
            write(
                tmp.path(),
                "agents/h/worker/agent.kdl",
                &format!(
                    "agent \"worker\" {{\n  host \"h\"\n  type \"service\"\n  {field} \"left{separator}right\"\n  pty \"agent\" {{ command \"true\" }}\n}}\n"
                ),
            );
            let found = discover(tmp.path());
            assert!(
                found.specs.is_empty(),
                "accepted {field} U+{:04X}",
                separator as u32
            );
            assert_eq!(found.errors.len(), 1, "{field}: {:?}", found.errors);
        }
    }
}

#[test]
fn named_resource_bindings_are_uri_identities_and_order_independent() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/kdl/agent.kdl",
        r#"agent "kdl" {
  host "h"
  resource "source" uri="worktree://github.com/example/project/main" relation="uses" reason="Primary checkout."
  resource "work" uri="github-issue://example/project/41" relation="current-work" reason="Current implementation task."
  command "true"
}"#,
    );
    write(
        tmp.path(),
        "agents/h/json/agent.json",
        r#"{
  "identity": "json",
  "host": "h",
  "resource": {
    "work": {"uri": "github-issue://example/project/41", "relation": "current-work", "reason": "Current implementation task."},
    "source": {"uri": "worktree://github.com/example/project/main", "relation": "uses", "reason": "Primary checkout."}
  },
  "command": "true"
}"#,
    );
    write(
        tmp.path(),
        "agents/h/toml/agent.toml",
        r#"identity = "toml"
host = "h"
command = "true"

[resource.work]
uri = "github-issue://example/project/41"
relation = "current-work"
reason = "Current implementation task."

[resource.source]
uri = "worktree://github.com/example/project/main"
relation = "uses"
reason = "Primary checkout."
"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let expected = vec![
        Resource::new_with_relation_reason(
            "source".into(),
            "worktree://github.com/example/project/main".into(),
            "uses".into(),
            "Primary checkout.".into(),
        )
        .unwrap(),
        Resource::new_with_relation_reason(
            "work".into(),
            "github-issue://example/project/41".into(),
            "current-work".into(),
            "Current implementation task.".into(),
        )
        .unwrap(),
    ];
    for identity in ["json", "kdl", "toml"] {
        assert_eq!(find(&found.specs, identity).resources, expected);
    }

    let json = serde_json::to_string(&expected).unwrap();
    assert_eq!(
        json,
        r#"[{"name":"source","uri":"worktree://github.com/example/project/main","relation":"uses","reason":"Primary checkout."},{"name":"work","uri":"github-issue://example/project/41","relation":"current-work","reason":"Current implementation task."}]"#
    );
    assert_eq!(
        serde_json::from_str::<Vec<Resource>>(&json).unwrap(),
        expected
    );
}

#[test]
fn resources_without_context_remain_valid_and_relation_reason_are_an_optional_pair() {
    let resource = Resource::new("source".into(), "worktree://example/project".into()).unwrap();
    assert_eq!(resource.relation(), None);
    assert_eq!(resource.reason(), None);
    assert_eq!(
        serde_json::to_string(&resource).unwrap(),
        r#"{"name":"source","uri":"worktree://example/project"}"#
    );

    for descriptor in [
        r#"{"name":"work","uri":"issue://one","relation":"uses"}"#,
        r#"{"name":"work","uri":"issue://one","reason":"Needed here."}"#,
    ] {
        assert!(
            serde_json::from_str::<Resource>(descriptor).is_err(),
            "{descriptor}"
        );
    }
}

#[test]
fn malformed_relation_and_reason_values_are_rejected_causally() {
    let tmp = tempfile::tempdir().unwrap();
    for (identity, relation, reason) in [
        ("missing-reason", " relation=\"uses\"", ""),
        ("missing-relation", "", " reason=\"Needed here.\""),
        (
            "relation-uppercase",
            " relation=\"Uses\"",
            " reason=\"Needed here.\"",
        ),
        (
            "relation-double-hyphen",
            " relation=\"current--work\"",
            " reason=\"Needed here.\"",
        ),
        (
            "reason-leading-space",
            " relation=\"uses\"",
            " reason=\" Needed here.\"",
        ),
        (
            "reason-line-separator",
            " relation=\"uses\"",
            " reason=\"Needed\u{2028}here.\"",
        ),
    ] {
        write(
            tmp.path(),
            &format!("agents/h/{identity}/agent.kdl"),
            &format!(
                "agent \"{identity}\" {{\n  host \"h\"\n  resource \"work\" uri=\"issue://one\"{relation}{reason}\n  command \"true\"\n}}"
            ),
        );
    }

    let found = discover(tmp.path());
    assert!(found.specs.is_empty(), "{:?}", found.specs);
    assert_eq!(found.errors.len(), 6, "{:?}", found.errors);
    let messages = found
        .errors
        .iter()
        .map(|error| error.message.as_str())
        .collect::<Vec<_>>();
    assert!(
        messages
            .iter()
            .any(|error| error.contains("must also declare string `reason`"))
    );
    assert!(
        messages
            .iter()
            .any(|error| error.contains("must also declare string `relation`"))
    );
    assert!(
        messages
            .iter()
            .any(|error| error.contains("ASCII kebab-case"))
    );
    assert!(
        messages
            .iter()
            .any(|error| error.contains("surrounding Unicode whitespace"))
    );
}

#[test]
fn relation_and_reason_byte_bounds_are_enforced() {
    let valid_relation = "a".repeat(64);
    let too_long_relation = "a".repeat(65);
    let valid_reason = "x".repeat(160);
    let multibyte_too_long_reason = "é".repeat(81);

    assert!(
        Resource::new_with_relation_reason(
            "work".into(),
            "issue://one".into(),
            valid_relation,
            valid_reason,
        )
        .is_ok()
    );
    assert!(
        Resource::new_with_relation_reason(
            "work".into(),
            "issue://one".into(),
            too_long_relation,
            "Needed here.".into(),
        )
        .is_err()
    );
    assert!(
        Resource::new_with_relation_reason(
            "work".into(),
            "issue://one".into(),
            "uses".into(),
            multibyte_too_long_reason,
        )
        .is_err()
    );
}

#[test]
fn malformed_resource_envelopes_are_rejected_without_defining_downstream_types() {
    let tmp = tempfile::tempdir().unwrap();
    for (identity, resource) in [
        (
            "duplicate",
            r#"resource "work" uri="issue://one"
  resource "work" uri="pull-request://two""#,
        ),
        (
            "unexpected-tag",
            r#"resource "work" _tag="issue" uri="issue://example/1""#,
        ),
        ("missing-uri", r#"resource "work""#),
        (
            "relative-uri",
            r#"resource "work" uri="./issue/1""#,
        ),
        (
            "policy",
            r#"resource "work" uri="issue://example/1" required=#true"#,
        ),
        (
            "payload",
            r#"resource "work" uri="issue://example/1" { token "secret" }"#,
        ),
    ] {
        write(
            tmp.path(),
            &format!("agents/h/{identity}/agent.kdl"),
            &format!("agent \"{identity}\" {{\n  host \"h\"\n  {resource}\n  command \"true\"\n}}"),
        );
    }

    let found = discover(tmp.path());
    assert!(found.specs.is_empty(), "{:?}", found.specs);
    assert_eq!(found.errors.len(), 6, "{:?}", found.errors);
    let errors = found
        .errors
        .iter()
        .map(|error| error.message.as_str())
        .collect::<Vec<_>>();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("duplicate resource binding"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("unsupported property `_tag`"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("needs string `uri`"))
    );
    assert!(errors.iter().any(|error| error.contains("absolute URI")));
    assert!(
        errors
            .iter()
            .any(|error| error.contains("unsupported property `required`"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("cannot have children"))
    );
}

#[test]
fn duplicate_json_resource_names_are_rejected_instead_of_last_write_winning() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/dup/agent.json",
        r#"{
  "identity": "dup",
  "command": "true",
  "resource": {
    "work": {"uri": "issue://one"},
    "work": {"uri": "issue://two"}
  }
}"#,
    );
    let found = discover(tmp.path());
    assert!(found.specs.is_empty());
    assert_eq!(found.errors.len(), 1);
    assert!(
        found.errors[0]
            .message
            .contains("duplicate resource binding 'work'")
    );
}

#[test]
fn public_resource_json_deserialization_enforces_the_catalog_invariants() {
    for descriptor in [
        r#"{"name":"","uri":"issue://one"}"#,
        r#"{"name":"work","uri":"./relative"}"#,
        r#"{"name":"work","uri":"issue://one","_tag":"issue"}"#,
        r#"{"name":"work","uri":"issue://one","required":true}"#,
    ] {
        assert!(
            serde_json::from_str::<Resource>(descriptor).is_err(),
            "{descriptor}"
        );
    }
}

#[test]
fn forbidden_raw_uri_characters_are_rejected_across_declaration_formats() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/h/kdl/agent.kdl",
        r##"agent "kdl" {
  command "true"
  resource "work" uri=#"thing://bad\slash"#
}"##,
    );
    write(
        tmp.path(),
        "agents/h/json/agent.json",
        r#"{
  "identity": "json",
  "command": "true",
  "resource": {"work": {"uri": "thing://bad<left"}}
}"#,
    );
    write(
        tmp.path(),
        "agents/h/toml/agent.toml",
        r#"identity = "toml"
command = "true"

[resource.work]
uri = 'thing://bad"quote'
"#,
    );

    let found = discover(tmp.path());
    assert!(found.specs.is_empty(), "{:?}", found.specs);
    assert_eq!(found.errors.len(), 3, "{:?}", found.errors);
    assert!(
        found
            .errors
            .iter()
            .all(|error| error.message.contains("must be an exact absolute URI"))
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
        ("empty-program-explicit", r#"pty "agent" { argv "" }"#),
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
fn an_explicit_identity_with_an_implicit_host_keeps_the_path_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let spec = r#"
identity = "real-name"
[pty.agent]
command = "exec claude 'boot'"
"#;
    write(tmp.path(), "agents/wrong-host/wrong-name/agent.toml", spec);

    let found = discover(tmp.path());
    assert!(found.errors.is_empty());
    let s = &found.specs[0];
    assert_eq!(s.identity, "real-name");
    assert_eq!(s.host.as_deref(), Some("wrong-host"));
    assert_eq!(found.warnings.len(), 1);
    assert!(
        found
            .warnings
            .iter()
            .any(|w| w.contains("identity mismatch"))
    );
}

#[test]
fn an_explicit_host_with_an_implicit_identity_keeps_the_path_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let spec = r#"
host = "real-host"
[pty.agent]
command = "exec claude 'boot'"
"#;
    write(tmp.path(), "agents/wrong-host/path-name/agent.toml", spec);

    let found = discover(tmp.path());
    assert!(found.errors.is_empty());
    let s = &found.specs[0];
    assert_eq!(s.identity, "path-name");
    assert_eq!(s.host.as_deref(), Some("real-host"));
    assert_eq!(found.warnings.len(), 1);
    assert!(found.warnings.iter().any(|w| w.contains("host mismatch")));
}

#[test]
fn an_explicit_identity_and_host_are_path_independent() {
    let tmp = tempfile::tempdir().unwrap();
    let rel = "teams/.managed/groups/archive/project/declaration/agent.kdl";
    write(
        tmp.path(),
        rel,
        r#"agent "stable-agent" { host "stable-host"; command "exec codex" }"#,
    );

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert!(found.warnings.is_empty(), "warnings: {:?}", found.warnings);
    assert_eq!(found.specs.len(), 1);
    let spec = &found.specs[0];
    assert_eq!(spec.identity, "stable-agent");
    assert_eq!(spec.host.as_deref(), Some("stable-host"));
    assert_eq!(
        spec.path.parent(),
        Some(
            tmp.path()
                .join("teams/.managed/groups/archive/project/declaration")
                .as_path()
        ),
        "the declaration parent remains the state/resource anchor"
    );
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
fn adjacent_non_agent_kdl_is_outside_strict_agent_admission() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "themes/layout.kdl",
        r#"layout { pane "sidebar"; pane "main" }"#,
    );
    write(
        tmp.path(),
        "agents/hetz/x/agent.kdl",
        r#"agent "x" { host "hetz"; command "true" }"#,
    );

    let found = discover(tmp.path());
    assert_eq!(found.specs.len(), 1);
    assert_eq!(found.declarations.len(), 1);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
}

#[test]
fn discovery_retains_the_immutable_parse_that_it_lowered() {
    let tmp = tempfile::tempdir().unwrap();
    let path = "agents/hetz/x/agent.kdl";
    let source = r#"agent "x" { host "hetz"; command "true" }"#;
    write(tmp.path(), path, source);

    let found = discover(tmp.path());
    write(tmp.path(), path, "this is no longer valid KDL {");

    let parsed = found.declarations[0].parse.as_ref().unwrap();
    assert_eq!(parsed.document.as_ref().unwrap().source, source);
    assert!(parsed.is_valid());
    assert_eq!(found.specs[0].identity, "x");
}

#[test]
fn shape_invalid_declarations_never_become_runnable_specs() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/x/agent.kdl",
        r#"agent "x" {
  host "hetz"
  schedule "daily" { command "true" }
}"#,
    );

    let found = discover(tmp.path());
    assert!(found.specs.is_empty(), "shape-invalid spec was lowered");
    assert_eq!(found.errors.len(), 1);
    assert!(found.errors[0].message.contains("unsupported-schedule"));
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
    assert!(s.desired_state.is_retired());
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

#[cfg(unix)]
#[test]
fn strict_discovery_reports_unobservable_declaration_entries() {
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;

    // A SHORT root, not `tempfile::tempdir()`, which honours TMPDIR. This test
    // binds a unix socket inside the directory it then scans, and a socket
    // address is capped at 104 bytes (Darwin; Linux allows 108, so the portable
    // bound is the smaller). Under `cargo test` in a session that points TMPDIR
    // at a long per-agent directory the bind fails with "path must be shorter
    // than SUN_LEN", reported from inside a discovery assertion rather than at
    // the socket. The socket has to live in the scanned directory, so the root
    // itself is what must stay short.
    let tmp = tempfile::Builder::new()
        .prefix("st2-discovery-")
        .tempdir_in("/tmp")
        .unwrap();
    write(
        tmp.path(),
        "agents/hetz/live/agent.kdl",
        r#"agent "live" { host "hetz"; command "x" }"#,
    );
    let dangling = tmp.path().join("dangling.kdl");
    symlink(tmp.path().join("missing.kdl"), &dangling).unwrap();
    let socket = tmp.path().join("socket.kdl");
    let _listener = UnixListener::bind(&socket).unwrap();

    let ordinary = discover(tmp.path());
    assert_eq!(ordinary.specs.len(), 1);
    assert!(ordinary.errors.is_empty(), "{:?}", ordinary.errors);

    let strict = discover_strict(tmp.path());
    assert_eq!(strict.specs.len(), 1);
    assert_eq!(strict.errors.len(), 2, "{:?}", strict.errors);
    assert!(strict.errors.iter().any(|error| error.path == dangling));
    assert!(strict.errors.iter().any(|error| error.path == socket));
    assert!(
        strict
            .errors
            .iter()
            .all(|error| error.message.contains("unobservable declaration entry"))
    );
}

#[cfg(unix)]
#[test]
fn strict_discovery_reports_a_directory_symlink_that_can_hide_declarations() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    write(
        external.path(),
        "nested/agent.kdl",
        r#"agent "hidden" { host "hetz"; command "x" }"#,
    );
    let linked = tmp.path().join("linked-catalog");
    symlink(external.path(), &linked).unwrap();

    let ordinary = discover(tmp.path());
    assert!(ordinary.specs.is_empty());
    assert!(ordinary.errors.is_empty(), "{:?}", ordinary.errors);

    let strict = discover_strict(tmp.path());
    assert!(strict.specs.is_empty());
    assert_eq!(strict.errors.len(), 1, "{:?}", strict.errors);
    assert_eq!(strict.errors[0].path, linked);
}

#[cfg(unix)]
#[test]
fn strict_discovery_allows_symlinks_that_cannot_hide_declarations() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    std::fs::write(external.path().join("python"), "binary").unwrap();
    let venv = tmp.path().join("workspace/.venv");
    std::fs::create_dir_all(venv.join("bin")).unwrap();
    std::fs::create_dir_all(venv.join("lib")).unwrap();
    symlink(external.path().join("python"), venv.join("bin/python")).unwrap();
    symlink("lib", venv.join("lib64")).unwrap();
    write(
        tmp.path(),
        "real/agent.kdl",
        r#"agent "visible" { host "hetz"; command "x" }"#,
    );
    symlink("real", tmp.path().join("alias")).unwrap();

    let strict = discover_strict(tmp.path());
    assert!(strict.errors.is_empty(), "{:?}", strict.errors);
    assert_eq!(strict.specs.len(), 1);
    assert_eq!(strict.specs[0].identity, "visible");
}

#[test]
fn only_contextually_reserved_namespaces_are_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/live/agent.kdl",
        r#"agent "live" { host "hetz"; command "x" }"#,
    );
    for (path, identity) in [
        (".managed/team/agent.kdl", "managed"),
        (".retired/team/agent.kdl", "dot-retired"),
        ("agents/archive/project/agent.kdl", "archive-project"),
        ("agents/resources/project/agent.kdl", "resources-project"),
        ("agents/inbox/project/agent.kdl", "inbox-project"),
    ] {
        write(
            tmp.path(),
            path,
            &format!(r#"agent "{identity}" {{ host "h"; command "x" }}"#),
        );
    }

    for path in [
        ".git/project/agent.kdl",
        ".st2/project/agent.kdl",
        "organizations/project/.git/nested/agent.kdl",
        "organizations/project/.st2/nested/agent.kdl",
        "pty/project/agent.kdl",
        "agents/hetz/live/resources/project/agent.kdl",
        "agents/hetz/live/archive/project/agent.kdl",
        "agents/hetz/live/inbox/project/agent.kdl",
    ] {
        write(
            tmp.path(),
            path,
            r#"agent "excluded" { host "h"; command "x" }"#,
        );
    }

    let found = discover(tmp.path());
    assert!(found.errors.is_empty(), "errors: {:?}", found.errors);
    assert_eq!(found.specs.len(), 6, "specs: {:?}", found.specs);
    for identity in [
        "live",
        "managed",
        "dot-retired",
        "archive-project",
        "resources-project",
        "inbox-project",
    ] {
        assert!(
            found.specs.iter().any(|spec| spec.identity == identity),
            "{identity} must remain discoverable"
        );
    }
    assert!(
        !find(&found.specs, "dot-retired").desired_state.is_retired(),
        "a .retired folder has no lifecycle meaning"
    );
    assert!(
        found.specs.iter().all(|spec| spec.identity != "excluded"),
        "reserved control/state namespaces must not become declarations"
    );
}
