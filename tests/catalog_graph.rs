use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::process::{Command, Output};

fn write(root: &Path, relative: &str, body: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

fn st2(root: &Path, args: &[&str], pty_root: Option<&Path>) -> Output {
    let home = root.join("home");
    fs::create_dir_all(&home).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_st2"));
    command
        .args(["--catalog", root.to_str().unwrap()])
        .args(args)
        .env("HOME", &home)
        .env("XDG_STATE_HOME", home.join("state"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT");
    if let Some(pty_root) = pty_root {
        command.env("PTY_ROOT", pty_root);
    }
    command.output().unwrap()
}

fn json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON ({error}):\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn graph_preserves_valid_rows_broken_sources_conflicts_and_incompleteness() {
    let catalog = tempfile::tempdir().unwrap();
    let root = catalog.path();
    fs::create_dir_all(root.join("agents/h/lead/.workspace")).unwrap();
    write(
        root,
        "agents/h/lead/agent.kdl",
        "agent \"lead\" {\n  host \"h\"\n  role \"worker\"\n  supervisor \"h.boss\"\n  workspace \"./.workspace\"\n  session-driver \"claude\"\n  desired-state \"suspended\" reason=\"Waiting for capacity\"\n  command \"true\"\n}\n",
    );
    write(
        root,
        "alternate/lead.kdl",
        "agent \"lead\" { host \"h\"; command \"true\" }\n",
    );
    write(
        root,
        "broken/agent.kdl",
        "agent \"broken\" { host \"h\"; pty { command \"true\" } }\n",
    );
    symlink(root.join("missing-declaration.kdl"), root.join("unreadable.kdl")).unwrap();

    let output = st2(root, &["catalog", "graph", "--host", "h", "--json"], None);
    assert_eq!(output.status.code(), Some(1));
    let graph = json(&output);
    assert_eq!(graph["schema"], "st2.catalog-graph.v1");
    assert_eq!(graph["complete"], false);
    assert_eq!(graph["roots"]["ptyRoot"], root.join("pty").display().to_string());

    let rows = graph["agents"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "valid duplicate rows remain visible: {graph:#}");
    let lead = rows
        .iter()
        .find(|row| row["source"]["path"] == "agents/h/lead/agent.kdl")
        .unwrap();
    assert_eq!(lead["id"], "h.lead");
    assert_eq!(lead["supervisor"], "h.boss");
    assert_eq!(lead["persona"], "worker");
    assert_eq!(lead["workspace"], "./.workspace");
    assert_eq!(lead["resolvedWorkspace"], root.join("agents/h/lead/.workspace").display().to_string());
    assert_eq!(lead["effectiveSessionDriver"], "claude");
    assert!(lead["parentId"].is_null());
    assert!(lead["rootId"].is_null());
    assert!(lead["depth"].is_null());
    assert!(lead["ancestorIds"].is_null());
    assert_eq!(lead["desiredState"], "suspended");
    assert_eq!(lead["desiredStateReason"], "Waiting for capacity");
    assert_eq!(lead["source"]["identityProvenance"], "declaration");
    assert_eq!(lead["source"]["hostProvenance"], "declaration");
    assert!(lead["runtime"]["observedState"].is_null());

    let broken = graph["declarations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["path"] == "broken/agent.kdl")
        .unwrap();
    assert_eq!(broken["status"], "invalid");
    assert_eq!(broken["agents"][0]["identity"], "broken");
    assert!(graph["conflicts"].as_array().unwrap().iter().any(|conflict| {
        conflict["kind"] == "duplicateIdentity" && conflict["identity"] == "h.lead"
    }));
    let issue_codes = graph["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|issue| issue["code"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(issue_codes.contains(&"dup-id"));
    assert!(issue_codes.contains(&"catalog-incomplete"));
    assert!(issue_codes.contains(&"unknown-task-kind"));
}

#[test]
fn graph_exposes_admitted_topology_and_delivery_readiness_facts() {
    let catalog = tempfile::tempdir().unwrap();
    let root = catalog.path();
    write(
        root,
        "agents/h/root/agent.kdl",
        r#"agent "root" { host "h"; command "true" }"#,
    );
    write(
        root,
        "agents/h/worker/agent.kdl",
        r#"agent "worker" {
  host "h"
  supervisor "h.root"
  argv "axe" "agent" "launch"
  session-driver "omp"
  delivery-readiness "anonymous" "zeta" "alpha" "zeta" harness="omp"
}"#,
    );

    let output = st2(root, &["catalog", "graph", "--host", "h", "--json"], None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let graph = json(&output);
    assert_eq!(graph["complete"], true, "{graph:#}");
    let rows = graph["agents"].as_array().unwrap();
    let root_row = rows.iter().find(|row| row["id"] == "h.root").unwrap();
    assert!(root_row["parentId"].is_null());
    assert_eq!(root_row["rootId"], "h.root");
    assert_eq!(root_row["depth"], 0);
    assert_eq!(root_row["ancestorIds"], serde_json::json!([]));

    let worker = rows.iter().find(|row| row["id"] == "h.worker").unwrap();
    assert_eq!(worker["effectiveSessionDriver"], "omp");
    assert_eq!(worker["parentId"], "h.root");
    assert_eq!(worker["rootId"], "h.root");
    assert_eq!(worker["depth"], 1);
    assert_eq!(worker["ancestorIds"], serde_json::json!(["h.root"]));
    assert_eq!(
        worker["deliveryReadiness"],
        serde_json::json!({
            "kind": "anonymous",
            "harness": "omp",
            "models": ["alpha", "zeta"]
        })
    );
}


#[test]
fn graph_rejects_missing_cycle_depth_and_per_host_root_count() {
    let issue_codes = |root: &Path| {
        let output = st2(root, &["catalog", "graph", "--host", "h", "--json"], None);
        assert_eq!(output.status.code(), Some(1));
        let graph = json(&output);
        assert_eq!(graph["complete"], false);
        graph["issues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|issue| issue["code"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };

    let missing = tempfile::tempdir().unwrap();
    write(
        missing.path(),
        "agents/h/root/agent.kdl",
        r#"agent "root" { host "h"; command "true" }"#,
    );
    write(
        missing.path(),
        "agents/h/worker/agent.kdl",
        r#"agent "worker" { host "h"; supervisor "h.absent"; command "true" }"#,
    );
    assert!(issue_codes(missing.path()).contains(&"supervisor-missing".to_owned()));

    let cycle = tempfile::tempdir().unwrap();
    write(
        cycle.path(),
        "agents/h/one/agent.kdl",
        r#"agent "one" { host "h"; supervisor "h.two"; command "true" }"#,
    );
    write(
        cycle.path(),
        "agents/h/two/agent.kdl",
        r#"agent "two" { host "h"; supervisor "h.one"; command "true" }"#,
    );
    let cycle_codes = issue_codes(cycle.path());
    assert!(cycle_codes.contains(&"supervisor-cycle".to_owned()));
    assert!(cycle_codes.contains(&"root-count".to_owned()));

    let deep = tempfile::tempdir().unwrap();
    for index in 0..=64 {
        let supervisor = if index == 0 {
            String::new()
        } else {
            format!(" supervisor \"h.node{}\";", index - 1)
        };
        write(
            deep.path(),
            &format!("agents/h/node{index}/agent.kdl"),
            &format!(
                "agent \"node{index}\" {{ host \"h\";{supervisor} command \"true\" }}\n"
            ),
        );
    }
    assert!(issue_codes(deep.path()).contains(&"supervisor-depth".to_owned()));

    let roots = tempfile::tempdir().unwrap();
    for identity in ["one", "two"] {
        write(
            roots.path(),
            &format!("agents/h/{identity}/agent.kdl"),
            &format!("agent \"{identity}\" {{ host \"h\"; command \"true\" }}\n"),
        );
    }
    assert!(issue_codes(roots.path()).contains(&"root-count".to_owned()));
}
#[test]
fn candidate_overlay_reports_conflict_on_stdout_and_never_publishes() {
    let catalog = tempfile::tempdir().unwrap();
    let root = catalog.path();
    write(
        root,
        "legacy/worker.kdl",
        "agent { host \"h\"; command \"true\" }\n",
    );
    let candidate_dir = tempfile::tempdir().unwrap();
    let candidate = candidate_dir.path().join("agent.kdl.candidate");
    fs::write(
        &candidate,
        "agent \"worker\" { host \"h\"; command \"true\" }\n",
    )
    .unwrap();

    let output = st2(
        root,
        &[
            "validate",
            "--host",
            "h",
            "--candidate",
            candidate.to_str().unwrap(),
            "--strict",
            "--json",
        ],
        None,
    );
    assert_eq!(output.status.code(), Some(1));
    let receipt = json(&output);
    assert_eq!(receipt["schema"], "st2.validate.v2");
    assert!(receipt["issues"].as_array().unwrap().iter().any(|issue| {
        issue["code"] == "dup-id" && issue["agent"] == "worker"
    }));
    assert!(!root.join("agents/h/worker/agent.kdl").exists());
}

#[test]
fn graph_reports_default_relative_absolute_and_ambient_pty_roots() {
    let catalog = tempfile::tempdir().unwrap();
    let root = catalog.path();
    write(
        root,
        "agents/h/worker/agent.kdl",
        "agent \"worker\" { host \"h\"; command \"true\" }\n",
    );

    let default = st2(root, &["catalog", "graph", "--host", "h", "--json"], None);
    assert!(default.status.success(), "{}", String::from_utf8_lossy(&default.stderr));
    assert_eq!(json(&default)["roots"]["ptyRoot"], root.join("pty").display().to_string());

    write(root, "catalog.kdl", "catalog { pty-root \"shared\" }\n");
    let relative = st2(root, &["catalog", "graph", "--host", "h", "--json"], None);
    assert!(relative.status.success());
    assert_eq!(json(&relative)["roots"]["ptyRoot"], root.join("shared").display().to_string());

    let absolute_root = root.join("absolute-registry");
    write(
        root,
        "catalog.kdl",
        &format!("catalog {{ pty-root \"{}\" }}\n", absolute_root.display()),
    );
    let absolute = st2(root, &["catalog", "graph", "--host", "h", "--json"], None);
    assert!(absolute.status.success());
    assert_eq!(json(&absolute)["roots"]["ptyRoot"], absolute_root.display().to_string());

    let ambient_root = root.join("ambient-shared-registry");
    let ambient = st2(
        root,
        &["catalog", "graph", "--host", "h", "--json"],
        Some(&ambient_root),
    );
    assert!(ambient.status.success());
    assert_eq!(json(&ambient)["roots"]["ptyRoot"], ambient_root.display().to_string());
}
