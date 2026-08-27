//! `st2 resource` operates on declared Agent Spec Resource bindings: `ls`/`read` project them,
//! and `add`/`remove`/`rename` mutate one binding through mediated CAS publication without the
//! caller rendering KDL.

use std::fs;
use std::path::Path;
use std::process::Command;

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

/// A declaration with `bindings` spliced in verbatim, so tests control exact bytes.
fn declaration(identity: &str, managed_by: &str, bindings: &str) -> String {
    format!(
        "// unrelated comment\nagent {identity:?} {{\n  host \"h\"\n  meta {{ managed-by {managed_by:?} }}\n{bindings}  command \"sleep 300\"\n}}\n"
    )
}

fn run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(["--catalog", root.to_str().unwrap()])
        .args(args)
        .env_remove("ST_AGENT")
        .output()
        .unwrap()
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn ok(root: &Path, args: &[&str]) -> String {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "expected success from {args:?}\nstdout: {}\nstderr: {}",
        stdout(&output),
        stderr(&output)
    );
    stdout(&output)
}

fn spec(root: &Path) -> String {
    fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap()
}

#[test]
fn ls_projects_declared_bindings_and_reports_none_without_pointing_at_another_store() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "catalog", ""),
    );

    let empty = ok(root, &["resource", "ls", "worker"]);
    assert!(
        empty.contains("0 resource"),
        "empty roster should report zero bindings, got: {empty}"
    );

    write(
        root,
        "h/worker/agent.kdl",
        &declaration(
            "worker",
            "catalog",
            "  resource \"notes\" reason=\"Durable notes.\" uri=\"agent-notes://h/worker\"\n\
             resource \"work\" reason=\"PR under preparation.\" uri=\"github-pr://github.com/o/r/pull/42\"\n",
        ),
    );

    let listed = ok(root, &["resource", "ls", "worker"]);
    assert!(listed.contains("2 resource"), "got: {listed}");
    assert!(listed.contains("notes"), "got: {listed}");
    assert!(
        listed.contains("agent-notes://h/worker"),
        "binding uri must appear: {listed}"
    );
    assert!(
        listed.contains("github-pr://github.com/o/r/pull/42"),
        "binding uri must appear: {listed}"
    );
}

#[test]
fn ls_json_and_read_expose_every_declared_field() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration(
            "worker",
            "catalog",
            "  resource \"work\" reason=\"PR under preparation.\" uri=\"github-pr://github.com/o/r/pull/42\" inactive-reason=\"Superseded by #43.\"\n",
        ),
    );

    let listed = ok(root, &["resource", "ls", "worker", "--json"]);
    let rows: serde_json::Value = serde_json::from_str(&listed).unwrap();
    let row = &rows.as_array().expect("json array")[0];
    assert_eq!(row["name"], "work");
    assert_eq!(row["uri"], "github-pr://github.com/o/r/pull/42");
    assert_eq!(row["reason"], "PR under preparation.");
    // `inactive_reason`, not `inactiveReason`: this is the descriptor `st2 agents --json`
    // already emits, and INVARIANTS.md pins that surface to preserve its field names. The
    // snake_case is inconsistent with sibling roster fields but predates this change.
    assert_eq!(row["inactive_reason"], "Superseded by #43.");

    let read = ok(root, &["resource", "read", "worker", "work"]);
    assert!(
        read.contains("github-pr://github.com/o/r/pull/42"),
        "got: {read}"
    );
    assert!(read.contains("PR under preparation."), "got: {read}");
    assert!(read.contains("Superseded by #43."), "got: {read}");
}

#[test]
fn add_publishes_one_binding_and_is_idempotent_on_identical_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "catalog", ""),
    );

    let added = ok(
        root,
        &[
            "resource",
            "add",
            "work",
            "--agent",
            "worker",
            "--uri",
            "github-pr://github.com/o/r/pull/42",
            "--reason",
            "PR under preparation.",
            "--json",
        ],
    );
    let receipt: serde_json::Value = serde_json::from_str(&added).unwrap();
    assert_eq!(receipt["result"], "changed");

    let after = spec(root);
    assert!(after.contains("resource \"work\""), "got:\n{after}");
    assert!(
        after.contains("// unrelated comment"),
        "unrelated bytes must survive:\n{after}"
    );
    assert!(
        after.contains("command \"sleep 300\""),
        "unrelated bytes must survive:\n{after}"
    );

    let repeated = ok(
        root,
        &[
            "resource",
            "add",
            "work",
            "--agent",
            "worker",
            "--uri",
            "github-pr://github.com/o/r/pull/42",
            "--reason",
            "PR under preparation.",
            "--json",
        ],
    );
    let receipt: serde_json::Value = serde_json::from_str(&repeated).unwrap();
    assert_eq!(
        receipt["result"], "unchanged",
        "re-adding identical bytes must not republish"
    );
    assert_eq!(spec(root), after, "unchanged add must not rewrite the file");
}

#[test]
fn add_preserves_the_uri_byte_for_byte_without_normalization() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "catalog", ""),
    );

    // Percent-encoding, mixed case, and a trailing slash must all survive verbatim.
    let exact = "vendor+Thing://Authority/exact%20identity/";
    ok(
        root,
        &[
            "resource",
            "add",
            "odd",
            "--agent",
            "worker",
            "--uri",
            exact,
            "--reason",
            "Exactness probe.",
        ],
    );

    assert!(
        spec(root).contains(exact),
        "uri must round-trip unchanged, got:\n{}",
        spec(root)
    );
    assert!(
        ok(root, &["resource", "ls", "worker"]).contains(exact),
        "uri must project unchanged"
    );
}

#[test]
fn remove_is_idempotent_and_rename_refuses_absent_and_colliding_names() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration(
            "worker",
            "catalog",
            "  resource \"notes\" reason=\"Durable notes.\" uri=\"agent-notes://h/worker\"\n\
             resource \"work\" reason=\"PR under preparation.\" uri=\"github-pr://github.com/o/r/pull/42\"\n",
        ),
    );

    let renamed = ok(
        root,
        &[
            "resource",
            "rename",
            "work",
            "current-work",
            "--agent",
            "worker",
            "--json",
        ],
    );
    let receipt: serde_json::Value = serde_json::from_str(&renamed).unwrap();
    assert_eq!(receipt["result"], "changed");
    assert!(spec(root).contains("resource \"current-work\""));
    assert!(!spec(root).contains("resource \"work\""));

    let absent = run(
        root,
        &["resource", "rename", "work", "other", "--agent", "worker"],
    );
    assert!(
        !absent.status.success(),
        "renaming an absent binding must fail"
    );

    let collision = run(
        root,
        &[
            "resource",
            "rename",
            "notes",
            "current-work",
            "--agent",
            "worker",
        ],
    );
    assert!(
        !collision.status.success(),
        "renaming onto an existing name must fail; names are unique per agent"
    );

    let removed = ok(
        root,
        &[
            "resource",
            "remove",
            "current-work",
            "--agent",
            "worker",
            "--json",
        ],
    );
    let receipt: serde_json::Value = serde_json::from_str(&removed).unwrap();
    assert_eq!(receipt["result"], "changed");

    let again = ok(
        root,
        &[
            "resource",
            "remove",
            "current-work",
            "--agent",
            "worker",
            "--json",
        ],
    );
    let receipt: serde_json::Value = serde_json::from_str(&again).unwrap();
    assert_eq!(
        receipt["result"], "unchanged",
        "removing an absent binding is an idempotent success"
    );
}

#[test]
fn mutation_refuses_an_invalid_uri_an_empty_reason_and_a_nix_managed_declaration() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "catalog", ""),
    );

    let relative = run(
        root,
        &[
            "resource",
            "add",
            "bad",
            "--agent",
            "worker",
            "--uri",
            "not-absolute",
            "--reason",
            "Probe.",
        ],
    );
    assert!(
        !relative.status.success(),
        "a non-absolute URI must be refused"
    );

    let blank = run(
        root,
        &[
            "resource",
            "add",
            "bad",
            "--agent",
            "worker",
            "--uri",
            "https://example.test/x",
            "--reason",
            "",
        ],
    );
    assert!(!blank.status.success(), "an empty reason must be refused");
    assert!(
        !spec(root).contains("resource \"bad\""),
        "a refused mutation must not write"
    );

    write(root, "h/nixed/agent.kdl", &declaration("nixed", "nix", ""));
    let nixed = run(
        root,
        &[
            "resource",
            "add",
            "work",
            "--agent",
            "nixed",
            "--uri",
            "https://example.test/x",
            "--reason",
            "Probe.",
        ],
    );
    assert!(
        !nixed.status.success(),
        "a Nix-owned declaration must refuse a runtime binding edit"
    );
}

#[test]
fn the_retired_link_record_plane_is_gone() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "catalog", ""),
    );

    // `add` took a bare URL under the retired plane. It now requires a name plus --uri/--reason,
    // so the old invocation must not silently succeed and write a link record.
    let legacy = run(root, &["resource", "add", "https://example.test/output"]);
    assert!(
        !legacy.status.success(),
        "the retired link-record invocation must not be accepted"
    );
    assert!(
        !root.join("h/worker/resources/links").exists(),
        "no link-record store may be created"
    );
}

/// A hand-authored binding may carry a trailing `//` comment explaining it. Removing the binding
/// removes that explanation with it, and updating one keeps the separator before it.
/// Regression: `remove` previously refused with `unsafe-source-shape`, and `add` glued the
/// rendered node onto the comment.
#[test]
fn a_binding_with_a_trailing_line_comment_is_removable_and_updatable() {
    let commented = "// unrelated comment\nagent \"worker\" {\n  host \"h\"\n  meta { managed-by \"catalog\" }\n  resource \"work\" uri=\"github-pr://github.com/o/r/pull/42\" reason=\"PR under preparation.\" // why it is here\n  resource \"notes\" uri=\"agent-notes://h/worker\" reason=\"Durable notes.\"\n  command \"sleep 300\"\n}\n";

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/worker/agent.kdl", commented);

    ok(
        root,
        &[
            "resource", "add", "work",
            "--agent", "worker",
            "--uri", "https://example.test/x",
            "--reason", "Changed.",
        ],
    );
    let updated = spec(root);
    assert!(
        updated.contains(r#"reason="Changed." // why it is here"#),
        "the blank before a trailing comment must survive an update:\n{updated}"
    );

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/worker/agent.kdl", commented);

    ok(root, &["resource", "remove", "work", "--agent", "worker"]);
    let after = spec(root);
    assert!(!after.contains("resource \"work\""), "got:\n{after}");
    assert!(
        !after.contains("why it is here"),
        "the binding's own trailing comment goes with it:\n{after}"
    );
    assert!(
        after.contains("resource \"notes\""),
        "the sibling binding must survive:\n{after}"
    );
    assert!(
        after.contains("// unrelated comment") && after.contains("command \"sleep 300\""),
        "unrelated bytes must survive:\n{after}"
    );
}
