use std::ffi::CString;
use std::fs;
use std::io::Write as _;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use sha2::{Digest as _, Sha256};

fn st2() -> Command {
    Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn agent(identity: &str, retired: bool) -> String {
    format!("agent \"{identity}\" {{\n  host \"host\"\n  retired #{retired}\n  argv \"true\"\n}}\n")
}

fn agent_dir(catalog: &Path, identity: &str) -> PathBuf {
    catalog.join("agents/host").join(identity)
}

fn write_agent(catalog: &Path, identity: &str, retired: bool) {
    write_agent_for_host(catalog, "host", identity, retired);
}

fn write_agent_for_host(catalog: &Path, host: &str, identity: &str, retired: bool) {
    let dir = catalog.join("agents").join(host).join(identity);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("agent.kdl"),
        format!(
            "agent \"{identity}\" {{\n  host \"{host}\"\n  retired #{retired}\n  argv \"true\"\n}}\n"
        ),
    )
    .unwrap();
}

fn ensure_external_pty_config(catalog: &Path) {
    let config = catalog.join("catalog.kdl");
    if !config.exists() {
        fs::write(
            config,
            "catalog { pty-root \"/tmp/st2-catalog-transaction-test-pty\" }\n",
        )
        .unwrap();
    }
}

fn snapshot(catalog: &Path, output: &Path) -> Value {
    ensure_external_pty_config(catalog);
    let result = st2()
        .args([
            "catalog",
            "snapshot",
            "--catalog",
            catalog.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "snapshot stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

fn apply(catalog: &Path, prepared: &Path, expected: &str) -> Output {
    st2()
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--prepared",
            prepared.to_str().unwrap(),
            "--expect-sha256",
            expected,
            "--json",
        ])
        .output()
        .unwrap()
}

fn resume(catalog: &Path) -> Output {
    st2()
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--resume",
            "--json",
        ])
        .output()
        .unwrap()
}

#[test]
fn catalog_apply_cli_exposes_exactly_the_three_closed_modes() {
    let help = st2().args(["catalog", "apply", "--help"]).output().unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    for flag in [
        "--prepared",
        "--projection-bundle",
        "--projection-child",
        "--expect-bundle-sha256",
        "--expect-sha256",
        "--resume",
        "--json",
    ] {
        assert!(help.contains(flag), "catalog apply help omitted {flag}");
    }
    assert!(!help.contains("--expect-absent"));

    for args in [
        vec!["catalog", "apply"],
        vec!["catalog", "apply", "--prepared", "/tmp/prepared"],
        vec![
            "catalog",
            "apply",
            "--resume",
            "--prepared",
            "/tmp/prepared",
        ],
        vec![
            "catalog",
            "apply",
            "--resume",
            "--expect-sha256",
            "0000000000000000000000000000000000000000000000000000000000000000",
        ],
        vec![
            "catalog",
            "apply",
            "--projection-bundle",
            "/tmp/bundle",
            "--projection-child",
            "provider-witness",
            "--expect-bundle-sha256",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--expect-sha256",
            "0000000000000000000000000000000000000000000000000000000000000000",
        ],
    ] {
        let rejected = st2().args(args).output().unwrap();
        assert!(
            !rejected.status.success(),
            "catalog apply accepted an incomplete or ambiguous mode"
        );
    }
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn send(catalog: &Path, recipient: &str, body: &str) -> Output {
    let mut child = st2()
        .args([
            "message",
            "send",
            recipient,
            "--catalog",
            catalog.to_str().unwrap(),
            "--as",
            "host.sender",
            "--host",
            "host",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn run_with_stdin(args: &[&str], body: &str) -> Output {
    let mut child = st2()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn paused_apply(
    catalog: &Path,
    prepared: &Path,
    expected: &str,
    point: &str,
    ready: &Path,
    release: &Path,
) -> Child {
    let mut command = st2();
    command
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--prepared",
            prepared.to_str().unwrap(),
            "--expect-sha256",
            expected,
            "--json",
        ])
        .env("ST2_TEST_CATALOG_APPLY_PAUSE_AT", point)
        .env("ST2_TEST_CATALOG_APPLY_READY", ready)
        .env("ST2_TEST_CATALOG_APPLY_RELEASE", release)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.spawn().unwrap()
}

#[test]
fn snapshot_is_typed_deterministic_and_excludes_state_and_workspaces() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let dir = agent_dir(&catalog, "worker");
    let workspace = temp.path().join("external-workspace");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir_all(catalog.join("_templates")).unwrap();
    fs::write(catalog.join("_templates/prompt.md"), "prompt").unwrap();
    fs::write(
        dir.join("agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n  render {{ copy \"_templates/prompt.md\" \"prompt.md\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    fs::create_dir_all(dir.join("resources/inbox")).unwrap();
    fs::write(dir.join("resources/inbox/live.md"), "state").unwrap();
    fs::write(dir.join("status"), "busy").unwrap();
    fs::create_dir_all(dir.join("assets")).unwrap();
    fs::write(dir.join("assets/tool.sh"), "#!/bin/sh\ntrue\n").unwrap();
    fs::create_dir_all(catalog.join("workspaces/repo")).unwrap();
    fs::write(catalog.join("workspaces/repo/owned.txt"), "workspace").unwrap();

    let output = temp.path().join("snapshot");
    let first = snapshot(&catalog, &output);
    assert_eq!(first["schema"], "st2.catalog-snapshot.v1");
    assert_eq!(first["status"], "created");
    assert!(output.join("agents/host/worker/agent.kdl").is_file());
    assert!(output.join("agents/host/worker/assets/tool.sh").is_file());
    assert!(output.join("_templates/prompt.md").is_file());
    assert!(!output.join("agents/host/worker/resources").exists());
    assert!(!output.join("agents/host/worker/status").exists());
    assert!(!output.join("workspaces").exists());

    let second = snapshot(&catalog, &output);
    assert_eq!(second["status"], "unchanged");
    assert_eq!(second["rootSha256"], first["rootSha256"]);
}

#[test]
fn complete_template_library_survives_unused_apply_and_supports_a_later_reference() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    fs::create_dir_all(catalog.join("_templates/nested")).unwrap();
    fs::write(catalog.join("_templates/used.md"), "used").unwrap();
    fs::write(catalog.join("_templates/future.md"), "future-v1").unwrap();
    fs::write(catalog.join("_templates/nested/alias.md"), "alias").unwrap();

    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    assert_eq!(
        fs::read_to_string(prepared.join("_templates/future.md")).unwrap(),
        "future-v1"
    );
    assert_eq!(
        fs::read_to_string(prepared.join("_templates/nested/alias.md")).unwrap(),
        "alias"
    );

    fs::write(prepared.join("_templates/future.md"), "future-v2").unwrap();
    fs::write(prepared.join("_templates/unreferenced.md"), "keep").unwrap();
    let applied = apply(&catalog, &prepared, before["rootSha256"].as_str().unwrap());
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    assert_eq!(
        fs::read_to_string(catalog.join("_templates/unreferenced.md")).unwrap(),
        "keep"
    );

    let workspace = temp.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let next = temp.path().join("next");
    let current = snapshot(&catalog, &next);
    fs::write(
        next.join("agents/host/worker/agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n  render {{ copy \"_templates/future.md\" \"future.md\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    let later = apply(&catalog, &next, current["rootSha256"].as_str().unwrap());
    assert!(
        later.status.success(),
        "{}",
        String::from_utf8_lossy(&later.stderr)
    );
    assert_eq!(
        fs::read_to_string(catalog.join("_templates/future.md")).unwrap(),
        "future-v2"
    );
}

#[test]
fn template_library_rejects_malicious_nodes_and_every_explicit_bound() {
    let temp = tempfile::tempdir().unwrap();
    for case in [
        "symlink",
        "hardlink",
        "fifo",
        "depth",
        "files",
        "file-bytes",
        "total-bytes",
    ] {
        let catalog = temp.path().join(format!("catalog-{case}"));
        write_agent(&catalog, "worker", false);
        let prepared = temp.path().join(format!("prepared-{case}"));
        let before = snapshot(&catalog, &prepared);
        fs::create_dir(prepared.join("_templates")).unwrap();
        match case {
            "symlink" => {
                std::os::unix::fs::symlink(temp.path(), prepared.join("_templates/escape.md"))
                    .unwrap();
            }
            "hardlink" => {
                fs::write(prepared.join("_templates/original.md"), "same inode").unwrap();
                fs::hard_link(
                    prepared.join("_templates/original.md"),
                    prepared.join("_templates/alias.md"),
                )
                .unwrap();
            }
            "fifo" => {
                let fifo = prepared.join("_templates/fifo");
                let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
            }
            "depth" => {
                let mut dir = prepared.join("_templates");
                for index in 0..9 {
                    dir.push(format!("d{index}"));
                    fs::create_dir(&dir).unwrap();
                }
                fs::write(dir.join("too-deep.md"), "deep").unwrap();
            }
            "files" => {
                for index in 0..257 {
                    fs::write(prepared.join(format!("_templates/{index}.md")), "").unwrap();
                }
            }
            "file-bytes" => {
                let file = fs::File::create(prepared.join("_templates/large.md")).unwrap();
                file.set_len(1024 * 1024 + 1).unwrap();
            }
            "total-bytes" => {
                for index in 0..33 {
                    let file =
                        fs::File::create(prepared.join(format!("_templates/large-{index}.md")))
                            .unwrap();
                    file.set_len(1024 * 1024).unwrap();
                }
            }
            _ => unreachable!(),
        }
        let output = apply(&catalog, &prepared, before["rootSha256"].as_str().unwrap());
        assert!(
            !output.status.success(),
            "template case {case} unexpectedly succeeded"
        );
        assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
    }
}

#[test]
fn apply_is_cas_guarded_idempotent_and_preserves_orphan_state() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let dir = agent_dir(&catalog, "worker");
    fs::create_dir_all(dir.join("resources/inbox")).unwrap();
    fs::write(dir.join("resources/inbox/live.md"), "keep").unwrap();
    fs::write(dir.join("status"), "busy").unwrap();
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    fs::remove_file(prepared.join("agents/host/worker/agent.kdl")).unwrap();
    fs::remove_dir_all(prepared.join("agents/host/worker")).unwrap();
    fs::remove_dir(prepared.join("agents/host")).unwrap();
    fs::remove_dir(prepared.join("agents")).unwrap();

    let stale = apply(&catalog, &prepared, &"0".repeat(64));
    assert!(!stale.status.success());
    assert!(dir.join("agent.kdl").exists());
    assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());

    let applied = apply(&catalog, &prepared, before["rootSha256"].as_str().unwrap());
    assert!(
        applied.status.success(),
        "apply stderr: {}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let applied: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(applied["schema"], "st2.catalog-apply.v1");
    assert_eq!(applied["status"], "applied");
    assert!(!dir.join("agent.kdl").exists());
    assert_eq!(
        fs::read_to_string(dir.join("resources/inbox/live.md")).unwrap(),
        "keep"
    );
    assert_eq!(fs::read_to_string(dir.join("status")).unwrap(), "busy");

    let after = snapshot(&catalog, &temp.path().join("after"));
    assert_eq!(after["rootSha256"], applied["afterSha256"]);
    let unchanged = apply(&catalog, &prepared, after["rootSha256"].as_str().unwrap());
    assert!(unchanged.status.success());
    let unchanged: Value = serde_json::from_slice(&unchanged.stdout).unwrap();
    assert_eq!(unchanged["status"], "unchanged");
}

#[test]
fn workspace_facts_are_empty_in_prepared_admitted_against_live_and_never_applied() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let workspace = catalog.join("agents/host/worker/.workspace");
    fs::create_dir_all(&workspace).unwrap();
    fs::write(workspace.join("live.txt"), "preserve").unwrap();
    fs::write(
        catalog.join("agents/host/worker/agent.kdl"),
        "agent \"worker\" {\n  host \"host\"\n  workspace \".workspace\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let task_workspace = catalog.join("agents/host/tasker/.workspace");
    fs::create_dir_all(&task_workspace).unwrap();
    fs::write(task_workspace.join("task-live.txt"), "task preserve").unwrap();
    fs::write(
        catalog.join("agents/host/tasker/agent.kdl"),
        "agent \"tasker\" {\n  host \"host\"\n  pty \"work\" {\n    cwd \".workspace\"\n    argv \"true\"\n  }\n}\n",
    )
    .unwrap();

    for case in ["missing", "content"] {
        let invalid = temp.path().join(format!("invalid-{case}"));
        let current = snapshot(&catalog, &invalid);
        let fact = invalid.join("agents/host/worker/.workspace");
        if case == "missing" {
            fs::remove_dir(&fact).unwrap();
        } else {
            let secret = temp.path().join("workspace-secret");
            fs::write(&secret, "must never be opened or copied").unwrap();
            std::os::unix::fs::symlink(&secret, fact.join("forbidden-link")).unwrap();
        }
        let rejected = apply(&catalog, &invalid, current["rootSha256"].as_str().unwrap());
        assert!(
            !rejected.status.success(),
            "prepared workspace {case} unexpectedly succeeded"
        );
        if case == "content" {
            assert!(
                String::from_utf8_lossy(&rejected.stderr)
                    .contains("prepared workspace fact must be empty"),
                "{}",
                String::from_utf8_lossy(&rejected.stderr)
            );
        }
        assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
        assert_eq!(
            fs::read_to_string(workspace.join("live.txt")).unwrap(),
            "preserve"
        );
    }

    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    let prepared_workspace = prepared.join("agents/host/worker/.workspace");
    assert!(prepared_workspace.is_dir());
    assert!(prepared_workspace.read_dir().unwrap().next().is_none());
    let prepared_task_workspace = prepared.join("agents/host/tasker/.workspace");
    assert!(prepared_task_workspace.is_dir());
    assert!(prepared_task_workspace.read_dir().unwrap().next().is_none());

    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        "agent \"worker\" {\n  host \"host\"\n  role \"updated\"\n  workspace \"./.workspace\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let applied = apply(&catalog, &prepared, before["rootSha256"].as_str().unwrap());
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    assert_eq!(
        fs::read_to_string(workspace.join("live.txt")).unwrap(),
        "preserve"
    );
    assert_eq!(
        fs::read_to_string(task_workspace.join("task-live.txt")).unwrap(),
        "task preserve"
    );
    let unchanged = apply(
        &catalog,
        &prepared,
        serde_json::from_slice::<Value>(&applied.stdout).unwrap()["afterSha256"]
            .as_str()
            .unwrap(),
    );
    assert!(
        unchanged.status.success(),
        "{}",
        String::from_utf8_lossy(&unchanged.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&unchanged.stdout).unwrap()["status"],
        "unchanged"
    );

    let next_workspace = catalog.join("agents/host/renamed/.workspace");
    let next = temp.path().join("next");
    let current = snapshot(&catalog, &next);
    fs::create_dir_all(&next_workspace).unwrap();
    fs::write(next_workspace.join("next.txt"), "also preserve").unwrap();
    fs::remove_dir(next.join("agents/host/worker/.workspace")).unwrap();
    fs::remove_file(next.join("agents/host/worker/agent.kdl")).unwrap();
    fs::remove_dir(next.join("agents/host/worker")).unwrap();
    fs::create_dir_all(next.join("agents/host/renamed/.workspace")).unwrap();
    fs::write(
        next.join("agents/host/renamed/agent.kdl"),
        "agent \"renamed\" {\n  host \"host\"\n  workspace \".workspace\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let changed = apply(&catalog, &next, current["rootSha256"].as_str().unwrap());
    assert!(
        changed.status.success(),
        "{}",
        String::from_utf8_lossy(&changed.stderr)
    );
    assert_eq!(
        fs::read_to_string(workspace.join("live.txt")).unwrap(),
        "preserve"
    );
    assert_eq!(
        fs::read_to_string(next_workspace.join("next.txt")).unwrap(),
        "also preserve"
    );
    let after = temp.path().join("after-workspace-move");
    snapshot(&catalog, &after);
    assert!(!after.join("agents/host/worker/.workspace").exists());
    assert!(
        after
            .join("agents/host/renamed/.workspace")
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn environment_expanded_relative_workspace_is_projected_and_admitted_consistently() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let workspace = catalog.join("agents/host/worker/.workspace");
    fs::create_dir_all(&workspace).unwrap();
    fs::write(workspace.join("live.txt"), "preserve").unwrap();
    fs::write(
        catalog.join("agents/host/worker/agent.kdl"),
        "agent \"worker\" {\n  host \"host\"\n  workspace \".workspace\"\n  argv \"true\"\n}\n",
    )
    .unwrap();

    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        "agent \"worker\" {\n  host \"host\"\n  role \"updated\"\n  workspace \"$ST2_TEST_WORKSPACE\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let applied = st2()
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--prepared",
            prepared.to_str().unwrap(),
            "--expect-sha256",
            before["rootSha256"].as_str().unwrap(),
            "--json",
        ])
        .env("ST2_TEST_WORKSPACE", ".workspace")
        .output()
        .unwrap();
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    assert_eq!(
        fs::read_to_string(workspace.join("live.txt")).unwrap(),
        "preserve"
    );
}

#[test]
fn workspace_fact_symlink_ancestry_fails_before_publication() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target");
    let old_workspace = temp.path().join("old-workspace");
    fs::create_dir(&old_workspace).unwrap();
    fs::create_dir_all(target.join("agents/host/worker")).unwrap();
    fs::write(
        target.join("agents/host/worker/agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n}}\n",
            old_workspace.display()
        ),
    )
    .unwrap();
    let prepared = temp.path().join("prepared");
    let before = snapshot(&target, &prepared);

    let external = temp.path().join("external");
    fs::create_dir(&external).unwrap();
    std::os::unix::fs::symlink(&external, target.join("agents/host/worker/.workspace")).unwrap();
    let workspace = target.join("agents/host/worker/.workspace");

    fs::create_dir_all(prepared.join("agents/host/worker/.workspace")).unwrap();
    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    let output = apply(&target, &prepared, before["rootSha256"].as_str().unwrap());
    assert!(!output.status.success());
    assert!(
        fs::read_to_string(target.join("agents/host/worker/agent.kdl"))
            .unwrap()
            .contains(old_workspace.to_str().unwrap())
    );
    assert!(!target.join(".st2/catalog-apply-incomplete").exists());
}

#[test]
fn prepared_state_symlinks_and_pty_root_changes_fail_before_a_marker() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let original = snapshot(&catalog, &temp.path().join("original"));
    let expected = original["rootSha256"].as_str().unwrap();

    for case in ["state", "workspace", "symlink", "fifo", "pty-root"] {
        let prepared = temp.path().join(format!("prepared-{case}"));
        snapshot(&catalog, &prepared);
        match case {
            "state" => {
                fs::create_dir_all(prepared.join("agents/host/worker/resources/inbox")).unwrap();
                fs::write(
                    prepared.join("agents/host/worker/resources/inbox/message.md"),
                    "forbidden",
                )
                .unwrap();
            }
            "workspace" => {
                fs::create_dir_all(prepared.join("workspaces/repo")).unwrap();
                fs::write(prepared.join("workspaces/repo/file"), "not declarations").unwrap();
            }
            "symlink" => {
                std::os::unix::fs::symlink("/tmp", prepared.join("agents/host/worker/assets-link"))
                    .unwrap();
            }
            "fifo" => {
                let fifo = prepared.join("agents/host/worker/fifo");
                let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
            }
            "pty-root" => {
                fs::write(
                    prepared.join("catalog.kdl"),
                    "catalog { pty-root \"/tmp/other-pty\" }\n",
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let result = apply(&catalog, &prepared, expected);
        assert!(
            !result.status.success(),
            "case {case} unexpectedly succeeded"
        );
        assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
    }
}

#[test]
fn apply_v1_requires_a_declared_pty_root_outside_the_catalog() {
    let temp = tempfile::tempdir().unwrap();
    for case in ["default", "relative", "catalog-variable"] {
        let catalog = temp.path().join(format!("catalog-{case}"));
        write_agent(&catalog, "worker", false);
        match case {
            "default" => {}
            "relative" => fs::write(
                catalog.join("catalog.kdl"),
                "catalog { pty-root \"registry\" }\n",
            )
            .unwrap(),
            "catalog-variable" => fs::write(
                catalog.join("catalog.kdl"),
                "catalog { pty-root \"$CATALOG/../catalog-catalog-variable/pty\" }\n",
            )
            .unwrap(),
            _ => unreachable!(),
        }
        let prepared = temp.path().join(format!("prepared-{case}"));
        let captured = st2()
            .args([
                "catalog",
                "snapshot",
                "--catalog",
                catalog.to_str().unwrap(),
                "--output",
                prepared.to_str().unwrap(),
                "--json",
            ])
            .output()
            .unwrap();
        assert!(captured.status.success());
        let captured: Value = serde_json::from_slice(&captured.stdout).unwrap();
        let rejected = apply(
            &catalog,
            &prepared,
            captured["rootSha256"].as_str().unwrap(),
        );
        assert!(
            !rejected.status.success(),
            "{case} unexpectedly admitted: {}",
            String::from_utf8_lossy(&rejected.stderr)
        );
        assert!(
            String::from_utf8_lossy(&rejected.stderr).contains("requires pty-root outside")
                || String::from_utf8_lossy(&rejected.stderr)
                    .contains("requires an explicit external pty-root")
        );
        assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
    }
}

#[test]
fn retained_capture_refuses_a_destination_inside_its_source_before_traversal() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = temp.path().join("bundle");
    fs::create_dir(&bundle).unwrap();
    fs::write(bundle.join("agent.kdl"), agent("worker", false)).unwrap();
    let digest = st2()
        .args(["agent", "digest", "--bundle"])
        .arg(&bundle)
        .env("TMPDIR", &bundle)
        .output()
        .unwrap();
    assert!(!digest.status.success());
    assert!(
        String::from_utf8_lossy(&digest.stderr).contains("is contained by source"),
        "{}",
        String::from_utf8_lossy(&digest.stderr)
    );
    assert_eq!(bundle.read_dir().unwrap().count(), 1);

    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    let result = st2()
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--prepared",
            prepared.to_str().unwrap(),
            "--expect-sha256",
            before["rootSha256"].as_str().unwrap(),
        ])
        .env("TMPDIR", &prepared)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("is contained by source"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
}

#[test]
fn concurrent_control_creation_fsyncs_both_the_creator_and_racing_observer_paths() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let release = temp.path().join("release");
    let mut children = Vec::new();
    for index in 0..2 {
        let ready = temp.path().join(format!("ready-{index}"));
        let branch = temp.path().join(format!("branch-{index}"));
        let output = temp.path().join(format!("snapshot-{index}"));
        let mut command = st2();
        command
            .args([
                "catalog",
                "snapshot",
                "--catalog",
                catalog.to_str().unwrap(),
                "--output",
                output.to_str().unwrap(),
                "--json",
            ])
            .env("ST2_TEST_CATALOG_CONTROL_READY", &ready)
            .env("ST2_TEST_CATALOG_CONTROL_RELEASE", &release)
            .env("ST2_TEST_CATALOG_CONTROL_BRANCH", &branch)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        children.push((ready, branch, command.spawn().unwrap()));
    }
    wait_for(&children[0].0);
    wait_for(&children[1].0);
    fs::write(&release, "").unwrap();

    let mut branches = Vec::new();
    for (_, branch, child) in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        branches.push(fs::read_to_string(branch).unwrap());
    }
    branches.sort();
    assert_eq!(branches, ["created", "raced"]);
    assert!(catalog.join(".st2/catalog-authoring.lock").is_file());

    // A third contender can first observe the directory after mkdir but before the creator's
    // parent fsync. The observer must independently fsync the catalog before using the control dir.
    let observed_catalog = temp.path().join("observed-catalog");
    write_agent(&observed_catalog, "worker", false);
    let created_ready = temp.path().join("created-ready");
    let created_release = temp.path().join("created-release");
    let creator_branch = temp.path().join("creator-branch");
    let creator = st2()
        .args([
            "catalog",
            "snapshot",
            "--catalog",
            observed_catalog.to_str().unwrap(),
            "--output",
            temp.path().join("creator-snapshot").to_str().unwrap(),
            "--json",
        ])
        .env("ST2_TEST_CATALOG_CONTROL_CREATED_READY", &created_ready)
        .env("ST2_TEST_CATALOG_CONTROL_CREATED_RELEASE", &created_release)
        .env("ST2_TEST_CATALOG_CONTROL_BRANCH", &creator_branch)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for(&created_ready);

    let observer_branch = temp.path().join("observer-branch");
    let observer = st2()
        .args([
            "catalog",
            "snapshot",
            "--catalog",
            observed_catalog.to_str().unwrap(),
            "--output",
            temp.path().join("observer-snapshot").to_str().unwrap(),
            "--json",
        ])
        .env("ST2_TEST_CATALOG_CONTROL_BRANCH", &observer_branch)
        .output()
        .unwrap();
    assert!(
        observer.status.success(),
        "{}",
        String::from_utf8_lossy(&observer.stderr)
    );
    assert_eq!(fs::read_to_string(observer_branch).unwrap(), "observed");

    fs::write(&created_release, "").unwrap();
    let creator = creator.wait_with_output().unwrap();
    assert!(
        creator.status.success(),
        "{}",
        String::from_utf8_lossy(&creator.stderr)
    );
    assert_eq!(fs::read_to_string(creator_branch).unwrap(), "created");
}

#[test]
fn crashes_recover_from_the_durable_stage_without_rechecking_partial_live_state() {
    for point in [
        "marker-created",
        "mid-write",
        "before-verify",
        "before-clear",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let catalog = temp.path().join("catalog");
        write_agent(&catalog, "worker", false);
        let prepared = temp.path().join("prepared");
        let before = snapshot(&catalog, &prepared);
        fs::write(
            prepared.join("agents/host/worker/agent.kdl"),
            agent("worker", true),
        )
        .unwrap();

        let crashed = st2()
            .args([
                "catalog",
                "apply",
                "--catalog",
                catalog.to_str().unwrap(),
                "--prepared",
                prepared.to_str().unwrap(),
                "--expect-sha256",
                before["rootSha256"].as_str().unwrap(),
                "--json",
            ])
            .env("ST2_TEST_CATALOG_APPLY_CRASH_AT", point)
            .output()
            .unwrap();
        assert!(
            !crashed.status.success(),
            "crash point {point} did not abort"
        );
        assert!(catalog.join(".st2/catalog-apply-incomplete").is_file());

        let competing = apply(&catalog, &prepared, before["rootSha256"].as_str().unwrap());
        assert!(!competing.status.success());
        assert!(
            String::from_utf8_lossy(&competing.stderr)
                .contains("recover only with `catalog apply --resume`")
        );
        fs::remove_dir_all(&prepared).unwrap();
        let recovered = resume(&catalog);
        assert!(
            recovered.status.success(),
            "recovery {point}: {}",
            String::from_utf8_lossy(&recovered.stderr)
        );
        let recovered: Value = serde_json::from_slice(&recovered.stdout).unwrap();
        assert_eq!(recovered["status"], "applied");
        assert_eq!(recovered["recovered"], true);
        assert!(recovered["prepared"].is_null());
        assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
        assert_eq!(
            fs::read_to_string(agent_dir(&catalog, "worker").join("agent.kdl")).unwrap(),
            agent("worker", true)
        );
    }
}

#[test]
fn recovery_does_not_need_a_partially_broken_old_render_graph() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let workspace = temp.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir_all(catalog.join("_templates")).unwrap();
    fs::write(catalog.join("_templates/old.md"), "old").unwrap();
    fs::write(
        agent_dir(&catalog, "worker").join("agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n  render {{ copy \"_templates/old.md\" \"prompt.md\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    fs::write(prepared.join("_templates/new.md"), "new").unwrap();
    fs::remove_file(prepared.join("_templates/old.md")).unwrap();
    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n  render {{ copy \"_templates/new.md\" \"prompt.md\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();

    let crashed = st2()
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--prepared",
            prepared.to_str().unwrap(),
            "--expect-sha256",
            before["rootSha256"].as_str().unwrap(),
        ])
        .env("ST2_TEST_CATALOG_APPLY_CRASH_AT", "mid-delete")
        .output()
        .unwrap();
    assert!(!crashed.status.success());
    assert!(!catalog.join("_templates/old.md").exists());

    fs::remove_dir_all(&prepared).unwrap();
    let recovered = resume(&catalog);
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    assert_eq!(
        fs::read_to_string(catalog.join("_templates/new.md")).unwrap(),
        "new"
    );
    assert!(
        fs::read_to_string(agent_dir(&catalog, "worker").join("agent.kdl"))
            .unwrap()
            .contains("_templates/new.md")
    );
}

#[test]
fn mismatched_recovery_and_malformed_markers_remain_fenced_without_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        agent("worker", true),
    )
    .unwrap();
    let crashed = st2()
        .args([
            "catalog",
            "apply",
            "--catalog",
            catalog.to_str().unwrap(),
            "--prepared",
            prepared.to_str().unwrap(),
            "--expect-sha256",
            before["rootSha256"].as_str().unwrap(),
        ])
        .env("ST2_TEST_CATALOG_APPLY_CRASH_AT", "marker-created")
        .output()
        .unwrap();
    assert!(!crashed.status.success());
    let marker = catalog.join(".st2/catalog-apply-incomplete");
    assert!(marker.is_file());
    let marker_json: Value = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
    assert_eq!(marker_json["schema"], "st2.catalog-apply-incomplete.v1");
    assert_eq!(marker_json["expectedRootSha256"], before["rootSha256"]);
    assert!(
        marker_json["stageName"]
            .as_str()
            .unwrap()
            .starts_with("catalog-apply-stage-")
    );
    assert!(marker_json["preparedRootSha256"].as_str().is_some());
    assert!(marker_json["originalPaths"].is_array());

    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        agent("worker", false),
    )
    .unwrap();
    let mismatched = apply(&catalog, &prepared, before["rootSha256"].as_str().unwrap());
    assert!(!mismatched.status.success());
    assert!(marker.is_file());
    assert_eq!(
        fs::read_to_string(agent_dir(&catalog, "worker").join("agent.kdl")).unwrap(),
        agent("worker", false)
    );

    let live_workspace = agent_dir(&catalog, "worker").join(".workspace");
    fs::create_dir_all(&live_workspace).unwrap();
    let live_file = live_workspace.join("live.txt");
    fs::write(&live_file, "must survive forged recovery").unwrap();
    let mut forged = marker_json.clone();
    let original_paths = forged["originalPaths"].as_array_mut().unwrap();
    original_paths.push(Value::String(
        "agents/host/worker/.workspace/live.txt".to_owned(),
    ));
    original_paths.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    fs::write(&marker, serde_json::to_vec(&forged).unwrap()).unwrap();
    let forged_recovery = resume(&catalog);
    assert!(!forged_recovery.status.success());
    assert!(
        String::from_utf8_lossy(&forged_recovery.stderr).contains("workspace or state-plane path"),
        "{}",
        String::from_utf8_lossy(&forged_recovery.stderr)
    );
    assert_eq!(
        fs::read_to_string(&live_file).unwrap(),
        "must survive forged recovery"
    );
    assert!(marker.is_file());

    fs::write(&marker, "{broken").unwrap();
    let malformed = resume(&catalog);
    assert!(!malformed.status.success());
    assert_eq!(fs::read_to_string(&marker).unwrap(), "{broken");
}

#[test]
fn marker_time_state_routes_existing_orphans_but_never_flat_falls_back_for_new_agents() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "old", false);
    let old = agent_dir(&catalog, "old");
    fs::create_dir_all(old.join("resources/inbox")).unwrap();
    write_agent(&catalog, "mix.sup", false);
    fs::create_dir_all(agent_dir(&catalog, "mix.sup").join("resources/inbox")).unwrap();
    write_agent(&catalog, "remote.worker", false);
    fs::create_dir_all(agent_dir(&catalog, "remote.worker").join("resources/inbox")).unwrap();
    write_agent_for_host(&catalog, "remote", "worker", false);
    fs::create_dir_all(catalog.join("agents/remote/worker/resources/inbox")).unwrap();
    let trap = agent_dir(&catalog, "trap");
    fs::create_dir_all(&trap).unwrap();
    let external = temp.path().join("external-state");
    fs::create_dir(&external).unwrap();
    std::os::unix::fs::symlink(&external, trap.join("resources")).unwrap();
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    write_agent(&prepared, "new", false);
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let child = paused_apply(
        &catalog,
        &prepared,
        before["rootSha256"].as_str().unwrap(),
        "marker-created",
        &ready,
        &release,
    );
    wait_for(&ready);

    let existing = send(&catalog, "host.old", "during apply");
    assert!(
        existing.status.success(),
        "{}",
        String::from_utf8_lossy(&existing.stderr)
    );
    assert!(
        old.join("resources/inbox")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    let hierarchical = send(&catalog, "mix.sup", "hierarchical");
    assert!(
        hierarchical.status.success(),
        "{}",
        String::from_utf8_lossy(&hierarchical.stderr)
    );
    assert!(
        agent_dir(&catalog, "mix.sup")
            .join("resources/inbox")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    let ambiguous = send(&catalog, "remote.worker", "ambiguous");
    assert!(!ambiguous.status.success());
    let trapped = send(&catalog, "trap", "must not escape");
    assert!(!trapped.status.success());
    assert!(external.read_dir().unwrap().next().is_none());

    let catalog_text = catalog.to_str().unwrap();
    let context = run_with_stdin(
        &[
            "context",
            "write",
            "host.old",
            "--catalog",
            catalog_text,
            "--host",
            "host",
        ],
        "working during apply",
    );
    assert!(
        context.status.success(),
        "{}",
        String::from_utf8_lossy(&context.stderr)
    );
    let resource = st2()
        .args([
            "resource",
            "add",
            "https://example.test/result",
            "--catalog",
            catalog_text,
            "--as",
            "host.old",
            "--host",
            "host",
        ])
        .output()
        .unwrap();
    assert!(
        resource.status.success(),
        "{}",
        String::from_utf8_lossy(&resource.stderr)
    );
    let status = st2()
        .args([
            "status",
            "host.old",
            "--set",
            "busy",
            "--catalog",
            catalog_text,
            "--host",
            "host",
        ])
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert_eq!(
        fs::read_to_string(old.join("resources/context/now.md")).unwrap(),
        "working during apply"
    );
    assert!(
        old.join("resources/links")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    assert_eq!(fs::read_to_string(old.join("status")).unwrap(), "busy\n");

    let phantom = send(&catalog, "host.new", "too early");
    assert!(!phantom.status.success());
    assert!(!catalog.join("host.new").exists());
    assert!(!agent_dir(&catalog, "new").exists());

    fs::write(&release, "").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(agent_dir(&catalog, "new").join("agent.kdl").is_file());

    let dotted = temp.path().join("dotted-catalog");
    for path in [
        "agents/a/b.c/resources/inbox",
        "agents/a.b/c/resources/inbox",
        "agents/a.b/only/resources/inbox",
    ] {
        fs::create_dir_all(dotted.join(path)).unwrap();
    }
    fs::create_dir_all(dotted.join(".st2")).unwrap();
    fs::write(dotted.join(".st2/catalog-apply-incomplete"), "{}").unwrap();
    let ambiguous_qualified = send(&dotted, "a.b.c", "ambiguous qualified");
    assert!(!ambiguous_qualified.status.success());
    let dotted_host = send(&dotted, "a.b.only", "dotted host");
    assert!(
        dotted_host.status.success(),
        "{}",
        String::from_utf8_lossy(&dotted_host.stderr)
    );
    assert!(
        dotted
            .join("agents/a.b/only/resources/inbox")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
}

#[test]
fn state_remains_addressable_after_its_spec_is_deleted_mid_apply() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "old", false);
    let old = agent_dir(&catalog, "old");
    fs::create_dir_all(old.join("resources/inbox")).unwrap();
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    fs::remove_file(prepared.join("agents/host/old/agent.kdl")).unwrap();
    fs::remove_dir_all(prepared.join("agents")).unwrap();
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let child = paused_apply(
        &catalog,
        &prepared,
        before["rootSha256"].as_str().unwrap(),
        "deleted-spec",
        &ready,
        &release,
    );
    wait_for(&ready);
    assert!(!old.join("agent.kdl").exists());

    let sent = send(&catalog, "host.old", "after delete");
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    assert!(
        old.join("resources/inbox")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    assert!(!catalog.join("host.old").exists());

    fs::write(&release, "").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn agent_publish_serializes_behind_apply_and_rechecks_its_leaf_cas() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    write_agent(&catalog, "worker", false);
    let old = agent("worker", false);
    let prepared = temp.path().join("prepared");
    let before = snapshot(&catalog, &prepared);
    fs::write(
        prepared.join("agents/host/worker/agent.kdl"),
        agent("worker", true),
    )
    .unwrap();
    let publisher_spec = temp.path().join("publisher.kdl");
    fs::write(
        &publisher_spec,
        "agent \"worker\" {\n  host \"host\"\n  role \"publisher\"\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let digest = st2()
        .args(["agent", "digest", "--spec"])
        .arg(&publisher_spec)
        .arg("--json")
        .output()
        .unwrap();
    assert!(digest.status.success());
    let digest: Value = serde_json::from_slice(&digest.stdout).unwrap();
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let apply_child = paused_apply(
        &catalog,
        &prepared,
        before["rootSha256"].as_str().unwrap(),
        "marker-created",
        &ready,
        &release,
    );
    wait_for(&ready);

    let mut publisher = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            publisher_spec.to_str().unwrap(),
            "--input-sha256",
            digest["sha256"].as_str().unwrap(),
            "--expect-sha256",
            &format!("{:x}", Sha256::digest(old.as_bytes())),
            "--json",
        ])
        .env(
            "ST2_TEST_CATALOG_LOCK_ATTEMPT",
            temp.path().join("publisher-waiting"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for(&temp.path().join("publisher-waiting"));
    assert!(publisher.try_wait().unwrap().is_none());

    fs::write(&release, "").unwrap();
    let apply_output = apply_child.wait_with_output().unwrap();
    assert!(
        apply_output.status.success(),
        "{}",
        String::from_utf8_lossy(&apply_output.stderr)
    );
    let publisher = publisher.wait_with_output().unwrap();
    assert!(!publisher.status.success());
    assert!(
        String::from_utf8_lossy(&publisher.stderr).contains("precondition failed"),
        "{}",
        String::from_utf8_lossy(&publisher.stderr)
    );
    assert_eq!(
        fs::read_to_string(agent_dir(&catalog, "worker").join("agent.kdl")).unwrap(),
        agent("worker", true)
    );
}
