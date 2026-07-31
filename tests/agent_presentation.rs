use std::fs;
use std::path::Path;
use std::process::Command;

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn declaration(identity: &str, supervisor: Option<&str>, managed_by: &str) -> String {
    let supervisor = supervisor
        .map(|value| format!("  supervisor {value:?}\n"))
        .unwrap_or_default();
    format!(
        "// unrelated comment\nagent {identity:?} {{\n  host \"h\"\n  meta {{ managed-by {managed_by:?}; keep \"unchanged\" }}\n{supervisor}  command \"sleep 300\"\n}}\n"
    )
}

fn run(root: &Path, command: &str, args: &[&str], actor: Option<&str>) -> std::process::Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_st2"));
    process
        .args(["--catalog", root.to_str().unwrap(), command])
        .args(args)
        .env_remove("ST_AGENT");
    if let Some(actor) = actor {
        process.env("ST_AGENT", actor);
    }
    process.output().unwrap()
}

#[test]
fn cli_sets_replaces_and_clears_fields_without_changing_identity_or_other_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = declaration("worker", None, "catalog");
    write(root, "h/worker/agent.kdl", &initial);
    write(root, "h/worker/name", "obsolete sibling authority\n");

    for (command, field, value) in [
        ("rename", "name", "Build owner"),
        ("describe", "description", "Own build delivery"),
    ] {
        let output = run(
            root,
            command,
            &["h.worker", value, "--host", "h", "--json"],
            None,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["result"], "changed");
        assert_eq!(receipt["identity"], "h.worker");
        assert_eq!(receipt["field"], field);
        assert_eq!(receipt["value"], value);
    }

    let found = st2::discover(root);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    let spec = &found.specs[0];
    assert_eq!(spec.identity, "worker");
    assert_eq!(spec.name.as_deref(), Some("Build owner"));
    assert_eq!(spec.description.as_deref(), Some("Own build delivery"));
    assert_eq!(
        fs::read_to_string(root.join("h/worker/name")).unwrap(),
        "obsolete sibling authority\n",
        "the retired sibling source is ignored, not rewritten or consulted"
    );

    let roster = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "--catalog",
            root.to_str().unwrap(),
            "agents",
            "--host",
            "h",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(roster.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&roster.stdout).unwrap();
    assert_eq!(rows[0]["identity"], "h.worker");
    assert_eq!(rows[0]["name"], "Build owner");
    assert_eq!(rows[0]["description"], "Own build delivery");

    let repeat = run(
        root,
        "rename",
        &["worker", "Build owner", "--host", "h", "--json"],
        None,
    );
    assert!(repeat.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&repeat.stdout).unwrap()["result"],
        "unchanged"
    );

    for command in ["rename", "describe"] {
        let clear = run(
            root,
            command,
            &["h.worker", "--clear", "--host", "h", "--json"],
            None,
        );
        assert!(clear.status.success());
    }
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        initial
    );
}

#[test]
fn cli_enforces_agent_authority_and_nix_and_format_refusals() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/root/agent.kdl",
        &declaration("root", None, "catalog"),
    );
    write(
        root,
        "h/child/agent.kdl",
        &declaration("child", Some("root"), "catalog"),
    );
    write(
        root,
        "h/sibling/agent.kdl",
        &declaration("sibling", Some("root"), "catalog"),
    );
    write(
        root,
        "h/nix/agent.kdl",
        &declaration("nix", Some("root"), "nix"),
    );
    write(
        root,
        "h/json/agent.json",
        r#"{"identity":"json","host":"h","command":"sleep 300"}"#,
    );

    for (command, target, actor, code) in [
        (
            "rename",
            "h.sibling",
            Some("h.child"),
            "presentation-not-authorized",
        ),
        (
            "describe",
            "h.nix",
            Some("h.root"),
            "nix-managed-declaration",
        ),
        ("describe", "h.json", None, "unsupported-declaration-format"),
    ] {
        let output = run(
            root,
            command,
            &[target, "refused", "--host", "h", "--json"],
            actor,
        );
        assert!(!output.status.success());
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["result"], "error");
        assert_eq!(receipt["code"], code);
    }

    let allowed = run(
        root,
        "describe",
        &["h.child", "Owned by root", "--host", "h", "--json"],
        Some("h.root"),
    );
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
}
