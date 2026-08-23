//! `st2 validate` — the contract-check a second renderer (Johannes's nix → catalog) uses to confirm
//! it hit the spec. Each test builds a minimal catalog exercising one failure mode and asserts the
//! exact issue code + severity; a clean catalog (and our shipped `examples/`) must validate spotless.

use st2::validate::{Report, Severity, validate, validate_for_host};

/// Write a set of `(relative-path, body)` files into a fresh temp catalog.
fn catalog(files: &[(&str, &str)]) -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    for (rel, body) in files {
        let p = d.path().join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }
    d
}

fn has(r: &Report, code: &str, sev: Severity) -> bool {
    r.issues.iter().any(|i| i.code == code && i.severity == sev)
}

// ---- clean cases -----------------------------------------------------------------------------

#[test]
fn a_well_formed_service_catalog_is_clean() {
    let c = catalog(&[(
        "hetz/worker/agent.kdl",
        r#"agent "worker" { host "hetz"; type "service"; pty "agent" { command "claude" } }"#,
    )]);
    let r = validate(c.path());
    assert_eq!(r.errors(), 0, "unexpected issues: {:?}", r.issues);
    assert_eq!(r.warnings(), 0, "unexpected warnings: {:?}", r.issues);
    assert_eq!(r.agents, 1);
}

#[test]
fn compact_agent_catalog_is_clean() {
    let c = catalog(&[(
        "Silber/cos/agent.kdl",
        r#"agent "cos" {
  host "Silber"
  env { ST_AGENT "Silber.cos" }
  command "codex"
  ding
}"#,
    )]);
    let r = validate(c.path());
    assert_eq!(r.errors(), 0, "unexpected issues: {:?}", r.issues);
    assert_eq!(r.warnings(), 0, "unexpected warnings: {:?}", r.issues);
}

#[test]
fn adjacent_non_agent_kdl_is_not_subject_to_agent_shape_policy() {
    let c = catalog(&[
        (
            "hetz/worker/agent.kdl",
            r#"agent "worker" { host "hetz"; command "true" }"#,
        ),
        (
            "themes/layout.kdl",
            r#"layout { pane "sidebar"; pane "main" }"#,
        ),
    ]);

    let report = validate(c.path());
    assert_eq!(report.errors(), 0, "unexpected issues: {:?}", report.issues);
}

#[test]
fn opaque_resource_bindings_are_structurally_valid() {
    let c = catalog(&[(
        "Silber/cos/agent.kdl",
        r#"agent "cos" {
  host "Silber"
  resource "work" uri="vendor+thing://authority/exact%20identity" reason="example vendor work item"
  command "codex"
}"#,
    )]);
    let r = validate(c.path());
    assert_eq!(r.errors(), 0, "unexpected issues: {:?}", r.issues);
    assert_eq!(r.warnings(), 0, "unexpected warnings: {:?}", r.issues);
}

#[test]
fn active_agents_may_share_an_opaque_resource_uri() {
    let c = catalog(&[
        (
            "h/reviewer/agent.kdl",
            r#"agent "reviewer" {
  host "h"
  resource "subject" uri="git-commit://github.com/example/project/0123456789abcdef" reason="reviewed example commit"
  command "true"
}"#,
        ),
        (
            "h/integrator/agent.kdl",
            r#"agent "integrator" {
  host "h"
  resource "subject" uri="git-commit://github.com/example/project/0123456789abcdef" reason="reviewed example commit"
  command "true"
}"#,
        ),
    ]);

    let r = validate(c.path());
    assert_eq!(r.errors(), 0, "unexpected issues: {:?}", r.issues);
    assert_eq!(r.agents, 2);
}

#[test]
fn duplicate_bus_ids_remain_an_error_when_resources_are_shared() {
    let c = catalog(&[
        (
            "h/one/agent.kdl",
            r#"agent "worker" {
  host "h"
  resource "subject" uri="git-commit://github.com/example/project/0123456789abcdef" reason="reviewed example commit"
  command "true"
}"#,
        ),
        (
            "h/two/agent.kdl",
            r#"agent "worker" {
  host "h"
  resource "subject" uri="git-commit://github.com/example/project/0123456789abcdef" reason="reviewed example commit"
  command "true"
}"#,
        ),
    ]);

    let r = validate(c.path());
    assert!(has(&r, "dup-id", Severity::Error), "{:?}", r.issues);
}

#[test]
fn an_invalid_resource_binding_is_a_parse_error() {
    let c = catalog(&[(
        "Silber/cos/agent.kdl",
        r#"agent "cos" {
  host "Silber"
  resource "work" uri="not-an-absolute-uri"
  command "codex"
}"#,
    )]);
    assert!(has(&validate(c.path()), "parse-error", Severity::Error));
}

#[test]
fn shared_workspace_render_conflict_is_an_error() {
    let c = tempfile::tempdir().unwrap();
    let workspace = c.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(c.path().join("_templates")).unwrap();
    std::fs::write(c.path().join("_templates/a"), "a\n").unwrap();
    std::fs::write(c.path().join("_templates/b"), "b\n").unwrap();
    for (identity, template) in [("a", "a"), ("b", "b")] {
        let path = c.path().join(format!("h/{identity}/agent.kdl"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            format!(
                "agent \"{identity}\" {{ host \"h\"; workspace \"{}\"; command \"true\"; render {{ copy \"_templates/{template}\" \".st2/PERSONA.md\" }} }}",
                workspace.display()
            ),
        )
        .unwrap();
    }

    let r = validate_for_host(c.path(), "h");

    assert!(
        has(&r, "render-owner-conflict", Severity::Error),
        "{:?}",
        r.issues
    );
}

// ---- errors ----------------------------------------------------------------------------------

#[test]
fn a_driver_block_and_deliver_are_two_conflicting_launch_sources() {
    let c = catalog(&[(
        "h/worker/agent.kdl",
        r#"agent "worker" {
  host "h"
  deliver "app-server"
  claude { prompt "Start work." }
  argv "claude" "Start work."
}"#,
    )]);

    let report = validate(c.path());
    let issue = report
        .issues
        .iter()
        .find(|issue| issue.code == "driver-deliver-conflict")
        .unwrap();
    assert_eq!(issue.severity, Severity::Error);
    assert_eq!(
        issue.message,
        "agent 'worker' declares both a driver block and `deliver`; choose one launch source"
    );
}

#[test]
fn type_batch_is_retired_and_flagged_unknown() {
    // `type = batch` is retired (native `st2 eval` replaces it) — a lingering batch spec is now an
    // unknown type, not a silently-accepted service.
    let c = catalog(&[(
        "demo/eval/agent.kdl",
        r#"agent "eval" { host "demo"; type "batch"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(&validate(c.path()), "unknown-type", Severity::Error));
}

#[test]
fn unknown_type_is_an_error() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "srvice"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(&validate(c.path()), "unknown-type", Severity::Error));
}

#[test]
fn a_spec_with_no_identity_is_an_error() {
    // Generic `agent.kdl` at the catalog root: no folder to borrow an identity from, none in content.
    let c = catalog(&[("agent.kdl", r#"agent { type "service" }"#)]);
    assert!(has(&validate(c.path()), "no-identity", Severity::Error));
}

#[test]
fn a_malformed_file_is_a_parse_error() {
    let c = catalog(&[("hetz/w/agent.kdl", r#"agent "w" { host "hetz""#)]);
    assert!(has(&validate(c.path()), "parse-error", Severity::Error));
}

#[test]
fn a_relative_path_is_an_error() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; workspace "some/rel/dir"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(&validate(c.path()), "bad-path", Severity::Error));
}

#[test]
fn canonical_relative_workspace_and_task_cwd_are_clean() {
    let c = catalog(&[
        (
            "hetz/w/agent.kdl",
            r#"agent "w" {
  host "hetz"
  workspace ".workspace"
  pty "agent" { cwd ".workspace"; command "x" }
}"#,
        ),
        ("hetz/w/.workspace/.keep", ""),
    ]);
    assert!(!has(&validate(c.path()), "bad-path", Severity::Error));
}

#[test]
fn normalized_relative_workspace_is_clean_but_indeterminate_relative_fails_closed() {
    let temp = tempfile::tempdir().unwrap();
    for (identity, workspace) in [
        ("dotted", "./.workspace"),
        ("indeterminate", "$ST2_TEST_UNSET_WORKSPACE"),
    ] {
        let bundle = temp.path().join(format!("agents/host/{identity}"));
        std::fs::create_dir_all(bundle.join(".workspace")).unwrap();
        std::fs::write(
            bundle.join("agent.kdl"),
            format!(
                "agent \"{identity}\" {{\n  host \"host\"\n  workspace \"{workspace}\"\n  argv \"true\"\n}}\n"
            ),
        )
        .unwrap();
    }

    let report = validate(temp.path());
    let bad_paths = report
        .issues
        .iter()
        .filter(|issue| issue.code == "bad-path")
        .collect::<Vec<_>>();
    assert_eq!(bad_paths.len(), 1, "{:#?}", report.issues);
    assert!(
        bad_paths
            .iter()
            .all(|issue| issue.message.contains("unresolved environment variable"))
    );
}

#[test]
fn environment_expanded_canonical_relative_workspace_is_clean() {
    let c = catalog(&[
        (
            "hetz/w/agent.kdl",
            r#"agent "w" { host "hetz"; workspace "$ST2_TEST_WORKSPACE"; argv "true" }"#,
        ),
        ("hetz/w/.workspace/.keep", ""),
    ]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(c.path())
        .args(["validate", "--json"])
        .env("ST2_TEST_WORKSPACE", ".workspace")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn a_missing_catalog_rooted_path_is_an_error() {
    // The renderer's own output — its absence is a real render bug.
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; workspace "$CATALOG/not-emitted"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(&validate(c.path()), "bad-path", Severity::Error));
}

#[test]
fn a_missing_external_path_is_only_a_warning() {
    // The workspace repo may not be checked out on the host running validate (≠ the run host) — a
    // nix build gate legitimately validates a catalog whose workspace lives elsewhere.
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; workspace "/no/such/dir/xyz123"; pty "agent" { command "x" } }"#,
    )]);
    let r = validate(c.path());
    assert!(has(&r, "bad-path", Severity::Warn));
    assert_eq!(
        r.errors(),
        0,
        "a missing external workspace must not be an error: {:?}",
        r.issues
    );
}

#[test]
fn a_remote_hosts_missing_external_path_is_not_a_local_warning() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; workspace "/no/such/dir/xyz123"; pty "agent" { command "x" } }"#,
    )]);
    let r = validate_for_host(c.path(), "Silber");
    assert_eq!(r.errors(), 0, "unexpected errors: {:?}", r.issues);
    assert_eq!(
        r.warnings(),
        0,
        "remote-host filesystem facts must not dirty local strict validation: {:?}",
        r.issues
    );
}

#[test]
fn an_environment_expanded_absolute_path_is_checked_normally() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; workspace "$HOME/repo"; pty "agent" { command "x" } }"#,
    )]);
    assert!(!has(&validate(c.path()), "bad-path", Severity::Error));
}

#[test]
fn a_catalog_rooted_path_that_exists_is_clean() {
    let c = catalog(&[
        (
            "hetz/w/agent.kdl",
            r#"agent "w" { host "hetz"; type "service"; workspace "$CATALOG/repo"; pty "agent" { command "x" } }"#,
        ),
        ("repo/.keep", ""),
    ]);
    assert!(!has(&validate(c.path()), "bad-path", Severity::Error));
}

#[test]
fn a_duplicate_bus_id_is_an_error() {
    let c = catalog(&[
        (
            "hetz/one/agent.kdl",
            r#"agent "twin" { host "hetz"; type "service"; pty "agent" { command "a" } }"#,
        ),
        (
            "hetz/two/agent.kdl",
            r#"agent "twin" { host "hetz"; type "service"; pty "agent" { command "b" } }"#,
        ),
    ]);
    assert!(has(&validate(c.path()), "dup-id", Severity::Error));
}

#[test]
fn explicit_identity_and_host_are_path_independent_but_still_unique() {
    let c = catalog(&[(
        "organization/.managed/archive/arbitrary/declaration/agent.kdl",
        r#"agent "stable" { host "pinned"; command "x" }"#,
    )]);
    let r = validate(c.path());
    assert_eq!(r.errors(), 0, "unexpected errors: {:?}", r.issues);
    assert_eq!(r.warnings(), 0, "unexpected warnings: {:?}", r.issues);
    assert_eq!(r.agents, 1);
}

#[test]
fn an_unrendered_service_is_not_runnable() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service" }"#,
    )]);
    assert!(has(&validate(c.path()), "not-runnable", Severity::Error));
}

#[test]
fn fleet_validation_compiles_remote_driver_launches() {
    let c = catalog(&[(
        "Silber/worker/agent.kdl",
        r#"agent "worker" {
  host "Silber"
  workspace "/tmp"
  claude { prompt "boot" }
}"#,
    )]);
    let found = st2::discover(c.path());
    assert!(!found.specs[0].is_runnable());

    for report in [validate(c.path()), validate_for_host(c.path(), "droppy")] {
        assert_eq!(report.errors(), 0, "unexpected issues: {:?}", report.issues);
        assert_eq!(
            report.warnings(),
            0,
            "unexpected issues: {:?}",
            report.issues
        );
    }
}

#[test]
fn validation_reports_shared_task_compiler_errors() {
    let c = catalog(&[(
        "h/worker/agent.kdl",
        r#"agent "worker" {
  host "h"
  workspace "/tmp"
  command "codex"
  deliver "app-server"
}"#,
    )]);

    assert!(has(
        &validate_for_host(c.path(), "h"),
        "launch-compile-error",
        Severity::Error
    ));
}

#[test]
fn a_generated_ding_sidecar_is_not_authored_runnable_work() {
    let c = catalog(&[("hetz/w/agent.kdl", r#"agent "w" { host "hetz"; ding }"#)]);
    assert!(has(&validate(c.path()), "not-runnable", Severity::Error));
}

#[test]
fn ls_marks_a_generated_ding_only_agent_as_unrendered() {
    let c = catalog(&[("hetz/w/agent.kdl", r#"agent "w" { host "hetz"; ding }"#)]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(c.path())
        .arg("ls")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("[UNRENDERED: no task launch]"),
        "stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn ls_compiles_driver_launches_before_display() {
    let c = catalog(&[(
        "h/worker/agent.kdl",
        r#"agent "worker" { host "h"; claude { prompt "boot" } }"#,
    )]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(c.path())
        .arg("ls")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("UNRENDERED"), "{stdout}");
    assert!(
        stdout.contains(r#""driver", "claude-session""#)
            && stdout.contains(r#""--", "claude", "boot"]"#),
        "{stdout}"
    );
}

#[test]
fn a_missing_render_source_is_an_error() {
    let workspace = tempfile::tempdir().unwrap();
    let agent = format!(
        r#"agent "w" {{
  host "hetz"
  workspace "{}"
  command "x"
  render {{ copy "_templates/missing.md" "AGENTS.md" }}
}}"#,
        workspace.path().display()
    );
    let c = catalog(&[("hetz/w/agent.kdl", &agent)]);
    assert!(has(&validate(c.path()), "render-error", Severity::Error));
}

#[test]
fn a_nameless_task_is_an_error() {
    // A `pty` block with no name is silently dropped by the parser — the task never runs.
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; pty { command "claude" } }"#,
    )]);
    assert!(has(
        &validate(c.path()),
        "unknown-task-kind",
        Severity::Error
    ));
}

#[test]
fn the_future_schedule_preview_is_explicitly_rejected() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" {
  host "hetz"
  command "x"
  schedule "local-health" {
    every "2h"
    ding "Run the local health check."
  }
}"#,
    )]);
    assert!(has(
        &validate(c.path()),
        "unsupported-schedule",
        Severity::Error
    ));
}

// ---- warnings --------------------------------------------------------------------------------

#[test]
fn a_dangling_supervisor_is_a_warning() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; supervisor "ghost"; pty "agent" { command "x" } }"#,
    )]);
    let r = validate(c.path());
    assert!(has(&r, "dangling-supervisor", Severity::Warn));
    assert_eq!(
        r.errors(),
        0,
        "a dangling supervisor must not be an error: {:?}",
        r.issues
    );
}

#[test]
fn a_fully_qualified_supervisor_in_the_catalog_is_clean() {
    let c = catalog(&[
        (
            "Silber/cos/agent.kdl",
            r#"agent "cos" { host "Silber"; command "x" }"#,
        ),
        (
            "hetz/w/agent.kdl",
            r#"agent "w" { host "hetz"; supervisor "Silber.cos"; command "x" }"#,
        ),
    ]);
    let r = validate(c.path());
    assert_eq!(r.errors(), 0, "unexpected errors: {:?}", r.issues);
    assert_eq!(
        r.warnings(),
        0,
        "runtime-routable qualified supervisors must validate: {:?}",
        r.issues
    );
}

#[test]
fn an_identity_folder_mismatch_is_a_warning() {
    let c = catalog(&[(
        "hetz/folder-name/agent.kdl",
        r#"agent "content-name" { type "service"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(&validate(c.path()), "id-path-mismatch", Severity::Warn));
}

#[test]
fn a_host_folder_mismatch_is_a_warning() {
    let c = catalog(&[(
        "folderhost/w/agent.kdl",
        r#"agent { host "confighost"; type "service"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(
        &validate(c.path()),
        "host-path-mismatch",
        Severity::Warn
    ));
}

#[test]
fn a_dangling_overlay_import_is_a_warning() {
    let d = tempfile::tempdir().unwrap();
    // Workspace with the native overlay shape, but the imported persona file is missing.
    let ws = d.path().join("ws");
    std::fs::create_dir_all(ws.join(".claude/rules")).unwrap();
    std::fs::write(ws.join(".claude/rules/st2.md"), "@../../.st2/PERSONA.md\n").unwrap();
    let agent = format!(
        r#"agent "w" {{ host "hetz"; type "service"; workspace {:?}; pty "agent" {{ command "x" }} }}"#,
        ws.display()
    );
    std::fs::create_dir_all(d.path().join("hetz/w")).unwrap();
    std::fs::write(d.path().join("hetz/w/agent.kdl"), agent).unwrap();
    assert!(has(&validate(d.path()), "dangling-import", Severity::Warn));
}

// ---- CLI: exit codes, --strict, --json, native example ---------------------------------------

fn run_validate(args: &[&std::ffi::OsStr]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("validate")
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn cli_exits_zero_on_a_clean_catalog() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; pty "agent" { command "x" } }"#,
    )]);
    let out = run_validate(&[c.path().as_os_str()]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

#[test]
fn cli_host_scope_keeps_remote_paths_out_of_strict_validation() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; workspace "/no/such/dir/xyz123"; command "x" }"#,
    )]);
    let out = run_validate(&[
        c.path().as_os_str(),
        std::ffi::OsStr::new("--host"),
        std::ffi::OsStr::new("Silber"),
        std::ffi::OsStr::new("--strict"),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn cli_exits_nonzero_on_an_error_and_strict_promotes_warnings() {
    // An error catalog exits non-zero without --strict.
    let err = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "srvice" }"#,
    )]);
    assert!(!run_validate(&[err.path().as_os_str()]).status.success());

    // A warning-only catalog exits 0 normally, 1 under --strict.
    let warn = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service"; supervisor "ghost"; pty "agent" { command "x" } }"#,
    )]);
    assert!(run_validate(&[warn.path().as_os_str()]).status.success());
    let strict = std::ffi::OsStr::new("--strict");
    assert!(
        !run_validate(&[warn.path().as_os_str(), strict])
            .status
            .success()
    );
}

#[test]
fn cli_json_is_well_formed() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "srvice"; pty "agent" { command "x" } }"#,
    )]);
    let out = run_validate(&[c.path().as_os_str(), std::ffi::OsStr::new("--json")]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid JSON");
    assert_eq!(v["schema"], "st2.validate.v2");
    assert_eq!(v["policyProfile"], "st2.core+catalog.v1");
    let revision = v["agentSpecRevision"]
        .as_str()
        .expect("agentSpecRevision string");
    let clean_revision = revision.len() == 40
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    assert!(
        clean_revision
            || revision.starts_with("local-dirty.")
            || revision.starts_with("nix-dirty.")
            || revision == "local.unknown"
    );
    assert_eq!(v["errors"], 1);
    assert_eq!(v["issues"][0]["code"], "unknown-type");
    assert_eq!(v["issues"][0]["severity"], "error");
}

#[test]
fn a_hand_authored_native_catalog_validates_without_errors() {
    let workspace = tempfile::tempdir().unwrap();
    let catalog = tempfile::tempdir().unwrap();
    let declaration = include_str!("../examples/native/agent-codex.kdl")
        .replace("<identity>", "worker")
        .replace("<host>", "h")
        .replace("<workspace>", workspace.path().to_str().unwrap());
    let path = catalog.path().join("agents/h/worker/agent.kdl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, declaration).unwrap();
    std::fs::create_dir_all(catalog.path().join("agents/h/worker/assets")).unwrap();
    std::fs::write(
        catalog.path().join("agents/h/worker/assets/AGENTS.md"),
        "# Worker\n",
    )
    .unwrap();

    let result = validate(catalog.path());
    assert_eq!(
        result.errors(),
        0,
        "native catalog has errors: {:?}",
        result.issues
    );
}
