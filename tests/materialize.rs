use std::fs;
use std::path::Path;
use std::process::Command;

use st2::materialize::{materialize_catalog, materialize_catalog_against, parse_plan};
use st2::{AgentSpec, discover};

fn write(path: &Path, contents: impl AsRef<[u8]>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

#[test]
fn task_selector_refusal_initializes_only_the_persistent_coordination_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up", "--catalog"])
        .arg(tmp.path())
        .args([
            "--host",
            "host",
            "--materialize-only",
            "--task",
            "host.missing.task",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let root_entries = fs::read_dir(tmp.path())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(root_entries.len(), 2);
    let host_lock = root_entries
        .iter()
        .find(|entry| entry.file_name() == ".st2.host.lock")
        .unwrap();
    let host_lock = fs::symlink_metadata(host_lock.path()).unwrap();
    assert!(host_lock.is_file() && !host_lock.file_type().is_symlink());
    let control = root_entries
        .iter()
        .find(|entry| entry.file_name() == ".st2")
        .unwrap();
    let control_entries = fs::read_dir(control.path())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(control_entries.len(), 1);
    assert_eq!(control_entries[0].file_name(), "catalog-authoring.lock");
    let lock = fs::symlink_metadata(control_entries[0].path()).unwrap();
    assert!(lock.is_file() && !lock.file_type().is_symlink());
}

#[test]
fn task_selector_materializes_only_owning_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let owner = tmp.path().join("owner");
    let sibling = tmp.path().join("sibling");
    fs::create_dir_all(&owner).unwrap();
    fs::create_dir_all(&sibling).unwrap();
    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(&owner, r#"    copy "_templates/owner" "OWNER.txt""#),
    );
    write(
        &catalog.join("agents/Silber/pty/agent.kdl"),
        agent_kdl(&sibling, r#"    copy "_templates/sibling" "SIBLING.txt""#)
            .replace("agent \"cos\"", "agent \"pty\"")
            .replace("Silber.cos", "Silber.pty"),
    );
    write(&catalog.join("_templates/owner"), "owner");
    write(&catalog.join("_templates/sibling"), "sibling");
    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up", "--catalog"])
        .arg(&catalog)
        .args([
            "--host",
            "Silber",
            "--materialize-only",
            "--task",
            "Silber.cos.agent",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        fs::read_to_string(owner.join("OWNER.txt")).unwrap(),
        "owner"
    );
    assert!(!sibling.join("SIBLING.txt").exists());
    assert!(String::from_utf8_lossy(&out.stdout).contains("materialized 1 operation"));
}

#[test]
fn shared_workspace_conflicting_copy_targets_fail_before_any_write() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("shared-workspace");
    fs::create_dir_all(&workspace).unwrap();
    write(
        &catalog.join("agents/Silber/worker/agent.kdl"),
        agent_kdl(
            &workspace,
            r#"    copy "_templates/worker.md" ".st2/PERSONA.md""#,
        ),
    );
    write(
        &catalog.join("agents/Silber/orchestrator/agent.kdl"),
        agent_kdl(
            &workspace,
            r#"    copy "_templates/orchestrator.md" ".st2/PERSONA.md""#,
        )
        .replace("agent \"cos\"", "agent \"orchestrator\"")
        .replace("Silber.cos", "Silber.orchestrator"),
    );
    write(&catalog.join("_templates/worker.md"), "worker\n");
    write(
        &catalog.join("_templates/orchestrator.md"),
        "orchestrator\n",
    );

    let found = discover(&catalog);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let report = materialize_catalog(&catalog, &found.specs, "Silber");

    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(
        report.errors[0].contains("conflicting render ownership"),
        "{:?}",
        report.errors
    );
    assert_eq!(report.failed_agents.len(), 2);
    assert!(
        !workspace.join(".st2/PERSONA.md").exists(),
        "a conflicting fleet must be rejected before either owner writes"
    );
}

#[test]
fn selected_owner_cannot_bypass_a_shared_workspace_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("shared-workspace");
    fs::create_dir_all(&workspace).unwrap();
    write(
        &catalog.join("agents/Silber/worker/agent.kdl"),
        agent_kdl(
            &workspace,
            r#"    copy "_templates/worker.md" ".st2/PERSONA.md""#,
        ),
    );
    write(
        &catalog.join("agents/Silber/orchestrator/agent.kdl"),
        agent_kdl(
            &workspace,
            r#"    copy "_templates/orchestrator.md" ".st2/PERSONA.md""#,
        )
        .replace("agent \"cos\"", "agent \"orchestrator\"")
        .replace("Silber.cos", "Silber.orchestrator"),
    );
    write(&catalog.join("_templates/worker.md"), "worker\n");
    write(
        &catalog.join("_templates/orchestrator.md"),
        "orchestrator\n",
    );
    let found = discover(&catalog);
    let worker = found
        .specs
        .iter()
        .find(|spec| spec.identity == "cos")
        .unwrap();
    let report = materialize_catalog_against(
        &catalog,
        std::slice::from_ref(worker),
        &found.specs,
        "Silber",
    );

    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.failed_agents.contains("Silber.cos"));
    assert!(!workspace.join(".st2/PERSONA.md").exists());
}

#[test]
fn shared_workspace_byte_identical_claims_are_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("shared-workspace");
    fs::create_dir_all(&workspace).unwrap();
    let render = r#"    copy "_templates/shared.md" ".st2/bus.md""#;
    write(
        &catalog.join("agents/Silber/a/agent.kdl"),
        agent_kdl(&workspace, render),
    );
    write(
        &catalog.join("agents/Silber/b/agent.kdl"),
        agent_kdl(&workspace, render)
            .replace("agent \"cos\"", "agent \"b\"")
            .replace("Silber.cos", "Silber.b"),
    );
    write(&catalog.join("_templates/shared.md"), "shared\n");
    let found = discover(&catalog);

    let report = materialize_catalog(&catalog, &found.specs, "Silber");

    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(report.failed_agents.is_empty());
    assert_eq!(
        fs::read_to_string(workspace.join(".st2/bus.md")).unwrap(),
        "shared\n"
    );
}

#[test]
fn task_selector_ambiguous_refuses_without_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    fs::create_dir_all(&a).unwrap();
    fs::create_dir_all(&b).unwrap();
    let kdl = |id: &str, ws: &Path, marker: &str| {
        format!(
            "agent \"{id}\" {{\n host \"Silber\"\n type \"service\"\n workspace \"{}\"\n pty \"agent\" {{\n  id \"dup\"\n  command \"true\"\n }}\n render {{ file \"MARKER.txt\" \"{marker}\" }}\n}}\n",
            ws.display()
        )
    };
    write(
        &catalog.join("agents/Silber/a/agent.kdl"),
        kdl("a", &a, "a"),
    );
    write(
        &catalog.join("agents/Silber/b/agent.kdl"),
        kdl("b", &b, "b"),
    );
    let found = st2::discover(&catalog);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert_eq!(found.specs.len(), 2);
    assert!(
        found
            .specs
            .iter()
            .all(|s| s.tasks.len() == 1 && s.tasks[0].id.as_deref() == Some("dup"))
    );
    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up", "--catalog"])
        .arg(&catalog)
        .args(["--host", "Silber", "--materialize-only", "--task", "dup"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("ambiguous"));
    assert!(!a.join("MARKER.txt").exists() && !b.join("MARKER.txt").exists());
}

#[test]
fn task_selector_wrong_host_refuses_without_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let target = tmp.path().join("hetz-target");
    fs::create_dir_all(&target).unwrap();
    let text = format!(
        "agent \"remote\" {{\n host \"Hetz\"\n type \"service\"\n workspace \"{}\"\n pty \"agent\" {{ id \"hetz.task\" command \"true\" }}\n render {{ file \"MARKER.txt\" \"remote\" }}\n}}\n",
        target.display()
    );
    write(&catalog.join("agents/Hetz/remote/agent.kdl"), text);
    let found = st2::discover(&catalog);
    assert!(found.errors.is_empty());
    assert_eq!(found.specs[0].tasks[0].id.as_deref(), Some("hetz.task"));
    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up", "--catalog"])
        .arg(&catalog)
        .args([
            "--host",
            "Silber",
            "--materialize-only",
            "--task",
            "hetz.task",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("did not resolve"));
    assert!(!target.join("MARKER.txt").exists());
}

#[test]
fn task_selector_cli_modes_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    for args in [
        vec![
            "up",
            "--catalog",
            tmp.path().to_str().unwrap(),
            "--task",
            "host.a.x",
        ],
        vec![
            "up",
            "--catalog",
            tmp.path().to_str().unwrap(),
            "--materialize-only",
            "--task",
            "host.a.x",
            "--agent",
            "a",
        ],
        vec![
            "up",
            "--catalog",
            tmp.path().to_str().unwrap(),
            "--agent",
            "a",
        ],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_st2"))
            .args(args)
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(!String::from_utf8_lossy(&out.stderr).trim().is_empty());
    }
}

#[test]
fn task_selector_single_file_modes_refuse_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let spec = tmp.path().join("spec.kdl");
    fs::write(&spec, "agent \"a\" { host \"Silber\" command \"true\" }\n").unwrap();
    let before = fs::read_to_string(&spec).unwrap();
    for extra in [
        ["--materialize-only", "--task", "Silber.a.agent"],
        ["--once", "--task", "Silber.a.agent"],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_st2"))
            .args(["up", spec.to_str().unwrap()])
            .args(extra)
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(!String::from_utf8_lossy(&out.stderr).is_empty());
    }
    assert_eq!(fs::read_to_string(&spec).unwrap(), before);
}

fn spec(catalog: &Path, identity: &str) -> AgentSpec {
    discover(catalog)
        .specs
        .into_iter()
        .find(|spec| spec.identity == identity)
        .unwrap()
}

fn agent_kdl(workspace: &Path, render: &str) -> String {
    format!(
        r##"agent "cos" {{
  host "Silber"
  workspace "{}"
  env {{ ST_AGENT "Silber.cos" }}
  command "true"
  ding
  render {{
{render}
  }}
}}
"##,
        workspace.display()
    )
}

fn init_git(workspace: &Path) {
    let status = Command::new("git")
        .args(["init", "-q"])
        .arg(workspace)
        .status()
        .unwrap();
    assert!(status.success());
}

fn track(workspace: &Path, path: &str) {
    let status = Command::new("git")
        .args(["-C"])
        .arg(workspace)
        .args(["add", "--", path])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn every_directive_materializes_in_order_and_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let init = Command::new("git")
        .args(["init", "-q"])
        .arg(&workspace)
        .status()
        .unwrap();
    assert!(init.success());

    write(
        &catalog.join("_templates/AGENTS.md"),
        b"catalog-owned brief\n",
    );
    write(
        &workspace.join(".codex/hooks.json"),
        r#"{"keep":true,"nested":{"user":"value"}}"#,
    );
    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(
            &workspace,
            r##"    copy "_templates/AGENTS.md" "AGENTS.md"
    file ".st2/env.txt" { content #"agent=$ST_AGENT root=$ST_ROOT"# }
    json-upsert ".codex/hooks.json" {
      content #"{"nested":{"st2":"value"},"hooks":{"Stop":[]}}"#
    }
    ensure-line ".codex/loader.md" "first"
    ensure-line ".codex/loader.md" "second"
    git-exclude "AGENTS.md" ".st2/"
"##,
        ),
    );

    let found = discover(&catalog);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let plan = parse_plan(&found.specs[0]).unwrap();
    assert_eq!(plan.ops.len(), 7);

    for _ in 0..2 {
        let report = materialize_catalog(&catalog, &found.specs, "Silber");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    }

    assert_eq!(
        fs::read(workspace.join("AGENTS.md")).unwrap(),
        b"catalog-owned brief\n"
    );
    let env = fs::read_to_string(workspace.join(".st2/env.txt")).unwrap();
    assert!(env.contains("agent=Silber.cos"));
    assert!(env.contains(&format!("root={}", catalog.display())));

    let json: serde_json::Value =
        serde_json::from_slice(&fs::read(workspace.join(".codex/hooks.json")).unwrap()).unwrap();
    assert_eq!(json["keep"], true);
    assert_eq!(json["nested"]["user"], "value");
    assert_eq!(json["nested"]["st2"], "value");
    assert_eq!(json["hooks"]["Stop"], serde_json::json!([]));

    assert_eq!(
        fs::read_to_string(workspace.join(".codex/loader.md")).unwrap(),
        "first\nsecond\n"
    );
    let exclude = fs::read_to_string(workspace.join(".git/info/exclude")).unwrap();
    assert_eq!(
        exclude.lines().filter(|line| *line == "AGENTS.md").count(),
        1
    );
    assert_eq!(exclude.lines().filter(|line| *line == ".st2/").count(), 1);
}

#[test]
fn every_content_directive_refuses_to_change_a_tracked_target_before_any_write() {
    for (name, initial, directive, template) in [
        (
            "copy",
            "old\n",
            r#"copy "_templates/replacement" "target""#,
            Some("new\n"),
        ),
        ("file", "old\n", r#"file "target" "new\n""#, None),
        (
            "json-upsert",
            "{\"keep\":true}\n",
            r##"json-upsert "target" #"{"added":true}"#"##,
            None,
        ),
        (
            "ensure-line",
            "old\n",
            r#"ensure-line "target" "new""#,
            None,
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let workspace = tmp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        init_git(&workspace);
        write(&workspace.join("target"), initial);
        track(&workspace, "target");
        if let Some(contents) = template {
            write(&catalog.join("_templates/replacement"), contents);
        }
        write(
            &catalog.join("agents/Silber/cos/agent.kdl"),
            agent_kdl(
                &workspace,
                &format!("    {directive}\n    file \"must-not-exist\" \"blocked with {name}\""),
            ),
        );

        let found = discover(&catalog);
        let report = materialize_catalog(&catalog, &found.specs, "Silber");
        assert_eq!(report.errors.len(), 1, "{name}: {:?}", report.errors);
        assert!(
            report.errors[0].contains("generated materialization would change Git-tracked target")
                && report.errors[0].contains("choose an untracked overlay target"),
            "{name}: {:?}",
            report.errors
        );
        assert_eq!(
            fs::read_to_string(workspace.join("target")).unwrap(),
            initial,
            "{name} changed the tracked target"
        );
        assert!(
            !workspace.join("must-not-exist").exists(),
            "{name} wrote a later operation after the gate failed"
        );
    }
}

#[test]
fn byte_identical_tracked_target_is_allowed_without_modification() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    init_git(&workspace);
    write(&workspace.join("AGENTS.md"), "same\n");
    track(&workspace, "AGENTS.md");
    write(&catalog.join("_templates/AGENTS.md"), "same\n");
    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(&workspace, r#"    copy "_templates/AGENTS.md" "AGENTS.md""#),
    );

    let found = discover(&catalog);
    let before = fs::metadata(workspace.join("AGENTS.md"))
        .unwrap()
        .modified()
        .unwrap();
    let report = materialize_catalog(&catalog, &found.specs, "Silber");
    let after = fs::metadata(workspace.join("AGENTS.md"))
        .unwrap()
        .modified()
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(before, after, "identical target was needlessly rewritten");
}

#[test]
fn untracked_and_non_git_targets_remain_materializable() {
    for git in [true, false] {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let workspace = tmp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        if git {
            init_git(&workspace);
        }
        write(&workspace.join("AGENTS.md"), "old\n");
        write(&catalog.join("_templates/AGENTS.md"), "new\n");
        write(
            &catalog.join("agents/Silber/cos/agent.kdl"),
            agent_kdl(&workspace, r#"    copy "_templates/AGENTS.md" "AGENTS.md""#),
        );

        let found = discover(&catalog);
        let report = materialize_catalog(&catalog, &found.specs, "Silber");
        assert!(report.is_clean(), "git={git}: {:?}", report.errors);
        assert_eq!(
            fs::read_to_string(workspace.join("AGENTS.md")).unwrap(),
            "new\n"
        );
    }
}

#[test]
fn missing_git_executable_fails_closed_before_workspace_write() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let hooks_root = tmp.path().join("hooks");
    let empty_path = tmp.path().join("empty-path");
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&empty_path).unwrap();
    init_git(&workspace);
    write(&workspace.join("AGENTS.md"), "old\n");
    track(&workspace, "AGENTS.md");
    write(&catalog.join("_templates/AGENTS.md"), "new\n");
    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(&workspace, r#"    copy "_templates/AGENTS.md" "AGENTS.md""#),
    );
    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("up")
        .arg(&catalog)
        .args(["--host", "Silber", "--materialize-only"])
        .env("ST_HOOKS", &hooks_root)
        .env("PATH", &empty_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("git rev-parse for materialization safety"),
        "{stderr}"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("AGENTS.md")).unwrap(),
        "old\n"
    );
}

#[test]
fn a_gating_failure_blocks_only_that_agent_but_git_exclude_is_advisory() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let bad_workspace = tmp.path().join("bad-workspace");
    let okay_workspace = tmp.path().join("okay-workspace");
    fs::create_dir_all(&bad_workspace).unwrap();
    fs::create_dir_all(&okay_workspace).unwrap();

    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(
            &bad_workspace,
            r#"    copy "_templates/missing.md" "AGENTS.md""#,
        ),
    );
    let okay = agent_kdl(
        &okay_workspace,
        r#"    file "AGENTS.md" "okay"
    git-exclude "AGENTS.md""#,
    )
    .replacen(r#"agent "cos""#, r#"agent "worker""#, 1)
    .replace("Silber.cos", "Silber.worker");
    write(&catalog.join("agents/Silber/worker/agent.kdl"), okay);

    let found = discover(&catalog);
    let report = materialize_catalog(&catalog, &found.specs, "Silber");
    assert_eq!(report.errors.len(), 1);
    assert!(report.failed_agents.contains("Silber.cos"));
    assert!(!report.failed_agents.contains("Silber.worker"));
    assert_eq!(
        fs::read_to_string(okay_workspace.join("AGENTS.md")).unwrap(),
        "okay"
    );
    assert_eq!(report.warnings.len(), 1, "non-Git exclude is advisory");
}

#[test]
fn source_can_be_relative_to_the_agent_file_for_blessed_catalog_compatibility() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    write(&catalog.join("_templates/brief.md"), "brief\n");
    write(
        &catalog.join("Silber/cos/agent.kdl"),
        agent_kdl(
            &workspace,
            r#"    copy "../../_templates/brief.md" "AGENTS.md""#,
        ),
    );
    let agent = spec(&catalog, "cos");
    let report = materialize_catalog(&catalog, &[agent], "Silber");
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(
        fs::read_to_string(workspace.join("AGENTS.md")).unwrap(),
        "brief\n"
    );
}

#[test]
fn up_materialize_only_writes_the_overlay_without_needing_pty() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    let hooks_root = tmp.path().join("hooks");
    fs::create_dir_all(&workspace).unwrap();
    write(&catalog.join("_templates/brief.md"), "brief\n");
    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(&workspace, r#"    copy "_templates/brief.md" "AGENTS.md""#),
    );
    let git = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|entry| entry.join("git"))
        .find(|candidate| candidate.is_file())
        .unwrap();
    let tools = tmp.path().join("tools");
    fs::create_dir(&tools).unwrap();
    std::os::unix::fs::symlink(git, tools.join("git")).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up"])
        .arg(&catalog)
        .args(["--host", "Silber", "--materialize-only"])
        // Proves this path never tries the runtime's external pty backend.
        .env("ST_HOOKS", &hooks_root)
        .env("PATH", &tools)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(workspace.join("AGENTS.md")).unwrap(),
        "brief\n"
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("materialized 1 operation"));
}
