use std::fs;
use std::path::Path;
use std::process::Command;

use st2::materialize::materialize_agent;
use st2::reconcile::{TaskCompileContext, compile_generated_tasks};
use st2::{discover, driver::expand_driver};

fn assert_snapshot(input: &str, expected: &str) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("agents/host/worker/agent.kdl");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, input).unwrap();

    let found = discover(temp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert_eq!(found.specs.len(), 1);
    let actual = expand_driver(&found.specs[0], "unused").unwrap().to_string();
    assert_eq!(actual, expected);
    expected.parse::<kdl::KdlDocument>().unwrap();
}

#[test]
fn claude_kdl_expansion_matches_snapshot() {
    assert_snapshot(
        include_str!("fixtures/driver/claude.in.kdl"),
        include_str!("fixtures/driver/claude.out.kdl"),
    );
}

#[test]
fn codex_kdl_expansion_matches_snapshot() {
    assert_snapshot(
        include_str!("fixtures/driver/codex.in.kdl"),
        include_str!("fixtures/driver/codex.out.kdl"),
    );
}

#[test]
fn cli_prints_each_snapshot_without_changing_its_input() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/driver");
    for provider in ["claude", "codex"] {
        let input = fixtures.join(format!("{provider}.in.kdl"));
        let before = fs::read(&input).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_st2"))
            .args(["--catalog"])
            .arg(&fixtures)
            .args(["driver", "expand"])
            .arg(&input)
            .args(
                (provider == "claude")
                    .then_some(["--agent", "Silber.fabric"])
                    .into_iter()
                    .flatten(),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.stdout,
            fs::read(fixtures.join(format!("{provider}.out.kdl"))).unwrap()
        );
        assert_eq!(fs::read(&input).unwrap(), before);
    }
}

#[test]
fn claude_driver_matches_deliver_after_normalizing_only_resolution_and_the_alias() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let legacy_workspace = temp.path().join("legacy-workspace");
    let driver_workspace = temp.path().join("driver-workspace");
    fs::create_dir_all(&legacy_workspace).unwrap();
    fs::create_dir_all(&driver_workspace).unwrap();
    let legacy_path = catalog.join("legacy.kdl");
    let driver_path = catalog.join("driver.kdl");
    fs::create_dir_all(&catalog).unwrap();
    fs::write(
        &legacy_path,
        format!(
            r#"agent "worker" {{
  host "h"
  workspace "{}"
  deliver "mcp"
  argv "claude" "--model" "opus" "--effort" "xhigh" "--dangerously-load-development-channels=server:st2" "--permission-mode" "bypassPermissions" "boot"
}}
"#,
            legacy_workspace.display()
        ),
    )
    .unwrap();
    fs::write(
        &driver_path,
        format!(
            r#"agent "worker" {{
  host "h"
  workspace "{}"
  claude {{
    model "opus"
    effort "xhigh"
    dev-channels #true
    prompt "boot"
    args "--permission-mode" "bypassPermissions"
  }}
}}
"#,
            driver_workspace.display()
        ),
    )
    .unwrap();
    let (legacy, _) = st2::discover_file(&catalog, &legacy_path).unwrap();
    let (driver, _) = st2::discover_file(&catalog, &driver_path).unwrap();
    let mut legacy = legacy.into_iter().next().unwrap();
    let mut driver = driver.into_iter().next().unwrap();
    let executable = catalog.join("bin/st2");
    fs::create_dir_all(executable.parent().unwrap()).unwrap();
    fs::write(&executable, "test binary").unwrap();
    let context = TaskCompileContext::new(catalog.clone(), executable).unwrap();

    compile_generated_tasks(std::slice::from_mut(&mut legacy), "h", &context).unwrap();
    compile_generated_tasks(std::slice::from_mut(&mut driver), "h", &context).unwrap();
    let legacy_task = legacy
        .tasks
        .iter()
        .find(|task| task.name == "agent")
        .unwrap();
    let driver_task = driver
        .tasks
        .iter()
        .find(|task| task.name == "agent")
        .unwrap();
    assert_eq!(driver_task, legacy_task);

    materialize_agent(&catalog, &legacy, "h").unwrap();
    materialize_agent(&catalog, &driver, "h").unwrap();
    let legacy_mcp: serde_json::Value =
        serde_json::from_slice(&fs::read(legacy_workspace.join(".mcp.json")).unwrap()).unwrap();
    let mut driver_mcp: serde_json::Value =
        serde_json::from_slice(&fs::read(driver_workspace.join(".mcp.json")).unwrap()).unwrap();
    assert_eq!(driver_mcp["mcpServers"]["st2"]["command"], "st2");
    driver_mcp["mcpServers"]["st2"]["command"] =
        legacy_mcp["mcpServers"]["st2"]["command"].clone();
    let args = driver_mcp["mcpServers"]["st2"]["args"]
        .as_array_mut()
        .unwrap();
    assert_eq!(&args[2..4], ["driver", "claude"]);
    args.splice(2..4, [serde_json::Value::String("claude-mcp".into())]);
    assert_eq!(driver_mcp, legacy_mcp);
}
