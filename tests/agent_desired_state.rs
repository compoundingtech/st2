use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn author(root: &Path, state: &str, reason: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_st2"));
    command.args([
        "--catalog",
        root.to_str().unwrap(),
        "agent",
        "desired-state",
        "h.worker",
        state,
        "--host",
        "h",
        "--json",
    ]);
    if let Some(reason) = reason {
        command.args(["--reason", reason]);
    }
    command.env_remove("ST_AGENT").output().unwrap()
}

fn author_as(root: &Path, actor: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "--catalog",
            root.to_str().unwrap(),
            "agent",
            "desired-state",
            "h.worker",
            "suspended",
            "--reason",
            "Waiting for capacity",
            "--host",
            "h",
            "--json",
        ])
        .env("ST_AGENT", actor)
        .output()
        .unwrap()
}

#[test]
fn cli_suspends_resumes_and_retires_without_rewriting_unrelated_source() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = "// keep this comment\nagent \"worker\" {\n  host \"h\"\n  command \"sleep 300\"\n  meta { owner \"ops\" }\n}\n";
    write(root, "h/worker/agent.kdl", initial);

    let suspended = author(root, "suspended", Some("Waiting for capacity"));
    assert!(
        suspended.status.success(),
        "{}",
        String::from_utf8_lossy(&suspended.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&suspended.stdout).unwrap();
    assert_eq!(receipt["result"], "changed");
    assert_eq!(receipt["desired_state"], "suspended");
    assert_eq!(receipt["reason"], "Waiting for capacity");
    let authored = fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap();
    assert!(authored.contains("  desired-state \"suspended\" reason=\"Waiting for capacity\"\n"));
    assert!(authored.starts_with("// keep this comment\n"));
    assert!(authored.contains("  meta { owner \"ops\" }\n"));

    let repeat = author(root, "suspended", Some("Waiting for capacity"));
    assert!(repeat.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&repeat.stdout).unwrap()["result"],
        "unchanged"
    );

    let running = author(root, "running", None);
    assert!(
        running.status.success(),
        "{}",
        String::from_utf8_lossy(&running.stderr)
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        initial
    );

    let retired = author(root, "retired", Some("Mission complete"));
    assert!(retired.status.success());
    let found = st2::discover(root);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert!(found.specs[0].desired_state.is_retired());
    assert_eq!(
        found.specs[0].desired_state.reason(),
        Some("Mission complete")
    );
}

#[test]
fn cli_resume_preserves_same_line_leading_comment() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = "agent \"worker\" {\n  host \"h\"\n  /* operator note */ desired-state \"suspended\" reason=\"Waiting for capacity\"\n  command \"true\"\n}\n";
    write(root, "h/worker/agent.kdl", initial);

    let running = author(root, "running", None);
    assert!(
        running.status.success(),
        "{}",
        String::from_utf8_lossy(&running.stderr)
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        "agent \"worker\" {\n  host \"h\"\n  /* operator note */\n  command \"true\"\n}\n"
    );
}

#[test]
fn cli_authors_a_canonical_path_derived_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = "agent {\n  command \"true\"\n}\n";
    write(root, "h/worker/agent.kdl", initial);

    let suspended = author(root, "suspended", Some("Waiting for capacity"));
    assert!(
        suspended.status.success(),
        "{}",
        String::from_utf8_lossy(&suspended.stderr)
    );
    assert!(
        fs::read_to_string(root.join("h/worker/agent.kdl"))
            .unwrap()
            .contains("desired-state \"suspended\" reason=\"Waiting for capacity\"")
    );

    let running = author(root, "running", None);
    assert!(
        running.status.success(),
        "{}",
        String::from_utf8_lossy(&running.stderr)
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        initial
    );
}

#[test]
fn cli_rejects_invalid_reason_contract_without_mutation() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let initial = "agent \"worker\" { host \"h\"; command \"true\" }\n";
    write(root, "h/worker/agent.kdl", initial);

    for (state, reason) in [
        ("suspended", None),
        ("retired", None),
        ("running", Some("not allowed")),
        ("suspended", Some(" surrounding ")),
    ] {
        let output = author(root, state, reason);
        assert!(
            !output.status.success(),
            "{state} {reason:?} unexpectedly succeeded"
        );
        assert_eq!(
            fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
            initial
        );
    }
}

#[test]
fn cli_canonicalizes_legacy_retirement_and_refuses_nix_owned_declarations() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        "agent \"worker\" { host \"h\"; retired #true; command \"true\" }\n",
    );
    let output = author(root, "suspended", Some("May return"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let authored = fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap();
    assert!(!authored.contains("retired"));
    assert!(authored.contains("desired-state \"suspended\" reason=\"May return\""));

    write(
        root,
        "h/worker/agent.kdl",
        "agent \"worker\" { host \"h\"; command \"true\"; meta { managed-by \"nix\" } }\n",
    );
    let before = fs::read(root.join("h/worker/agent.kdl")).unwrap();
    let refused = author(root, "suspended", Some("Maintenance"));
    assert!(!refused.status.success());
    assert_eq!(fs::read(root.join("h/worker/agent.kdl")).unwrap(), before);
}

#[test]
fn cli_applies_the_existing_self_or_descendant_authority_guardrail() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/root/agent.kdl",
        "agent \"root\" { host \"h\"; command \"true\" }\n",
    );
    write(
        root,
        "h/worker/agent.kdl",
        "agent \"worker\" { host \"h\"; supervisor \"h.root\"; command \"true\" }\n",
    );
    write(
        root,
        "h/sibling/agent.kdl",
        "agent \"sibling\" { host \"h\"; command \"true\" }\n",
    );

    let allowed = author_as(root, "h.root");
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
    assert!(author(root, "running", None).status.success());
    let refused = author_as(root, "h.sibling");
    assert!(!refused.status.success());
    let receipt: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(receipt["code"], "desired-state-not-authorized");
    assert!(
        st2::discover(root)
            .specs
            .iter()
            .any(|spec| spec.identity == "worker" && spec.desired_state.is_running())
    );
}
