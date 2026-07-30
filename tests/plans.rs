use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn fixture() -> tempfile::TempDir {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "agent.kdl",
        r#"
agent "worker" {
  plan-ref "file:plans/shared/plan.kdl"
  plan "local" {
    version "0000" resource="file:plans/local.md"
  }
}
"#,
    );
    write(root, "plans/local.md", "# Local\n");
    write(root, "plans/shared/0000.md", "# Initial\n");
    write(root, "plans/shared/0001.md", "# Left\n");
    write(root, "plans/shared/0002.md", "# Right\n");
    write(
        root,
        "plans/shared/plan.kdl",
        r#"
plan "shared" {
  owner "cos"
  version "0000" resource="file:0000.md"
  version "0001" resource="file:0001.md" {
    parent "0000"
    why "Left branch."
  }
  version "0002" resource="file:0002.md" {
    parent "0000"
    why "Right branch."
  }
}
"#,
    );
    temporary
}

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}

fn success_json(root: &Path, args: &[&str]) -> serde_json::Value {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn cli_validates_lists_shows_and_inspects_the_same_read_only_plan_model() {
    let temporary = fixture();
    let root = temporary.path();
    let agent_before = fs::read(root.join("agent.kdl")).unwrap();
    let plan_before = fs::read(root.join("plans/shared/plan.kdl")).unwrap();

    let valid = success_json(root, &["plan", "validate", "--json"]);
    assert_eq!(valid["result"], "valid");
    assert_eq!(valid["plans"], 2);

    let listed = success_json(root, &["plan", "list", "--json"]);
    assert_eq!(listed[0]["identity"], "local");
    assert_eq!(listed[1]["identity"], "shared");
    assert_eq!(listed[1]["frontier"], serde_json::json!(["0001", "0002"]));

    let shown = success_json(root, &["plan", "show", "shared", "--json"]);
    assert_eq!(shown["owner"], "cos");
    assert_eq!(shown["frontier"], serde_json::json!(["0001", "0002"]));
    assert!(shown.get("source").is_none());
    assert!(shown["versions"][0].get("resolvedResource").is_none());

    let inspected = success_json(root, &["plan", "inspect", "shared", "--json"]);
    assert_eq!(inspected["sourceKind"], "external");
    assert_eq!(inspected["referencedBy"], serde_json::json!(["worker"]));
    assert!(
        inspected["versions"][0]["resolvedResource"]
            .as_str()
            .unwrap()
            .ends_with("/plans/shared/0000.md")
    );

    assert_eq!(fs::read(root.join("agent.kdl")).unwrap(), agent_before);
    assert_eq!(
        fs::read(root.join("plans/shared/plan.kdl")).unwrap(),
        plan_before
    );
    assert!(!root.join(".st2").exists());
}

#[test]
fn cli_validation_is_nonzero_and_classified_for_out_of_scope_plan_fields() {
    let temporary = tempfile::tempdir().unwrap();
    write(temporary.path(), "v0.md", "# Initial\n");
    write(
        temporary.path(),
        "plan.kdl",
        r#"
plan "not-read-only" {
  owner "cos"
  current "0000"
  version "0000" resource="file:v0.md"
}
"#,
    );

    let output = run(temporary.path(), &["plan", "validate", "--json"]);
    assert!(!output.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["result"], "invalid");
    assert_eq!(receipt["code"], "unsupported-plan-field");
}
