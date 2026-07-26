//! `st2 validate` — the contract-check a second renderer (Johannes's nix → catalog) uses to confirm
//! it hit the spec. Each test builds a minimal catalog exercising one failure mode and asserts the
//! exact issue code + severity; a clean catalog (and our shipped `examples/`) must validate spotless.

use st2::validate::{Report, Severity, validate};

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

// ---- errors ----------------------------------------------------------------------------------

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
fn a_path_bearing_another_var_is_skipped() {
    // SD3: an unset var is a literal token — do not guess, do not flag.
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
fn an_unrendered_service_is_not_runnable() {
    let c = catalog(&[(
        "hetz/w/agent.kdl",
        r#"agent "w" { host "hetz"; type "service" }"#,
    )]);
    assert!(has(&validate(c.path()), "not-runnable", Severity::Error));
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
        String::from_utf8_lossy(&output.stdout).contains("[UNRENDERED: no task command]"),
        "stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
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
fn an_identity_folder_mismatch_is_a_warning() {
    let c = catalog(&[(
        "hetz/folder-name/agent.kdl",
        r#"agent "content-name" { host "hetz"; type "service"; pty "agent" { command "x" } }"#,
    )]);
    assert!(has(&validate(c.path()), "id-path-mismatch", Severity::Warn));
}

#[test]
fn a_host_folder_mismatch_is_a_warning() {
    let c = catalog(&[(
        "folderhost/w/agent.kdl",
        r#"agent "w" { host "confighost"; type "service"; pty "agent" { command "x" } }"#,
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
    std::fs::create_dir_all(catalog.path().join("_templates")).unwrap();
    std::fs::write(
        catalog.path().join("_templates/h.worker.AGENTS.md"),
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
