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
  resource "shared-role" _tag="plan" uri="file:plans/shared/plan.kdl"
  resource "local-role" _tag="plan" uri="file:plans/local/plan.kdl"
}
"#,
    );
    write(
        root,
        "plans/local/plan.kdl",
        r#"
plan "local" {
  owner "cos"
  version "0000" {
    intent "Keep the complete local intent in plan.kdl."
  }
}
"#,
    );
    write(root, "plans/shared/0000.md", "# Initial\n");
    write(root, "plans/shared/0001.md", "# Left\n");
    write(root, "plans/shared/0002.md", "# Right\n");
    write(
        root,
        "plans/shared/plan.kdl",
        r#"
plan "shared" {
  owner "cos"
  version "0000" content="file:0000.md"
  version "0001" content="file:0001.md" {
    parent "0000"
    why "Left branch."
  }
  version "0002" content="file:0002.md" {
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
    assert_eq!(shown["versions"][0]["content"], "file:0000.md");
    assert!(shown["versions"][0].get("resolvedContent").is_none());

    let inline = success_json(root, &["plan", "show", "local", "--json"]);
    assert_eq!(
        inline["versions"][0]["intent"],
        "Keep the complete local intent in plan.kdl."
    );
    assert!(inline["versions"][0].get("content").is_none());

    let inspected = success_json(root, &["plan", "inspect", "shared", "--json"]);
    assert_eq!(inspected["sourceKind"], "external");
    assert_eq!(inspected["referencedBy"], serde_json::json!(["worker"]));
    assert!(
        inspected["versions"][0]["resolvedContent"]
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
  version "0000" content="file:v0.md"
}
"#,
    );

    let output = run(temporary.path(), &["plan", "validate", "--json"]);
    assert!(!output.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["result"], "invalid");
    assert_eq!(receipt["code"], "unsupported-plan-field");
}

#[test]
fn cli_rejects_agent_owned_plan_truth_and_childful_plan_resources() {
    let temporary = tempfile::tempdir().unwrap();
    write(
        temporary.path(),
        "agent.kdl",
        r#"
agent "worker" {
  plan-ref "file:plan.kdl"
}
"#,
    );
    let old_form = run(temporary.path(), &["plan", "validate", "--json"]);
    assert!(!old_form.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&old_form.stdout).unwrap();
    assert_eq!(receipt["code"], "unsupported-agent-plan-form");

    write(
        temporary.path(),
        "agent.kdl",
        r#"
agent "worker" {
  resource "local-role" _tag="plan" uri="file:plan.kdl" {
    owner "forbidden"
  }
}
"#,
    );
    let childful = run(temporary.path(), &["plan", "validate", "--json"]);
    assert!(!childful.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&childful.stdout).unwrap();
    assert_eq!(receipt["code"], "invalid-plan-resource-binding");
}
