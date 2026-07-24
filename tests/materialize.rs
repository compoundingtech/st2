use std::fs;
use std::path::Path;
use std::process::Command;

use st2::materialize::{materialize_catalog, parse_plan};
use st2::{AgentSpec, discover};

fn write(path: &Path, contents: impl AsRef<[u8]>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
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
  command "codex"
  ding
  render {{
{render}
  }}
}}
"##,
        workspace.display()
    )
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
    fs::create_dir_all(&workspace).unwrap();
    write(&catalog.join("_templates/brief.md"), "brief\n");
    write(
        &catalog.join("agents/Silber/cos/agent.kdl"),
        agent_kdl(&workspace, r#"    copy "_templates/brief.md" "AGENTS.md""#),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["up"])
        .arg(&catalog)
        .args(["--host", "Silber", "--materialize-only"])
        // Proves this path never tries the runtime's external pty backend.
        .env("PATH", "/usr/bin:/bin")
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
