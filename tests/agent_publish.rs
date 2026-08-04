use std::fs;
use std::io::Write as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use sha2::{Digest, Sha256};

fn st2() -> Command {
    Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn valid_spec(retired: bool) -> String {
    format!("agent \"worker\" {{\n  host \"host\"\n  retired #{retired}\n  argv \"true\"\n}}\n")
}

fn publish(catalog: &Path, spec: &Path, expectation: &[&str]) -> Output {
    let input_sha256 = sha256(&fs::read(spec).unwrap());
    st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            spec.to_str().unwrap(),
        ])
        .args(["--input-sha256", &input_sha256])
        .args(expectation)
        .arg("--json")
        .output()
        .unwrap()
}

fn source_digest(flag: &str, path: &Path) -> String {
    let output = st2()
        .args(["agent", "digest", flag])
        .arg(path)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt = serde_json::from_slice::<Value>(&output.stdout).unwrap();
    assert_eq!(receipt["schema"], "st2.agent-source-digest.v1");
    receipt["sha256"].as_str().unwrap().to_string()
}

fn assert_agent_spec_revision(value: &Value) {
    let revision = value.as_str().expect("agentSpecRevision string");
    let clean_revision = revision.len() == 40
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    assert!(
        clean_revision
            || revision.starts_with("local-dirty.")
            || revision.starts_with("nix-dirty.")
            || revision == "local.unknown",
        "unexpected agentSpecRevision: {revision}"
    );
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn target(catalog: &Path) -> PathBuf {
    catalog.join("agents/host/worker/agent.kdl")
}

#[test]
fn spec_create_is_typed_and_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    fs::create_dir(&catalog).unwrap();
    let spec = temp.path().join("candidate.kdl");
    fs::write(&spec, valid_spec(false)).unwrap();

    let first = publish(&catalog, &spec, &["--expect-absent"]);
    assert!(
        first.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first["schema"], "st2.agent-publish.v2");
    assert_eq!(first["policyProfile"], "st2.core+catalog.v1");
    assert_agent_spec_revision(&first["agentSpecRevision"]);
    assert_eq!(first["status"], "published");
    assert_eq!(first["busId"], "host.worker");
    assert_eq!(first["inputSha256"], sha256(valid_spec(false).as_bytes()));
    assert_eq!(first["afterSha256"], sha256(valid_spec(false).as_bytes()));
    assert_eq!(
        fs::read_to_string(target(&catalog)).unwrap(),
        valid_spec(false)
    );

    let second = publish(&catalog, &spec, &["--expect-absent"]);
    assert!(
        second.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(second["status"], "unchanged");
}

#[test]
fn caller_source_digest_rejects_mutation_and_symlink_swaps_before_publication() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    fs::create_dir(&catalog).unwrap();
    let spec = temp.path().join("candidate.kdl");
    fs::write(&spec, valid_spec(false)).unwrap();
    let reserved = source_digest("--spec", &spec);

    fs::write(&spec, valid_spec(true)).unwrap();
    let changed = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            spec.to_str().unwrap(),
            "--input-sha256",
            &reserved,
            "--expect-absent",
        ])
        .output()
        .unwrap();
    assert!(!changed.status.success());
    assert!(
        String::from_utf8_lossy(&changed.stderr).contains("input precondition failed"),
        "{}",
        String::from_utf8_lossy(&changed.stderr)
    );
    assert!(!target(&catalog).exists());

    let replacement = temp.path().join("replacement.kdl");
    fs::write(&replacement, valid_spec(false)).unwrap();
    fs::remove_file(&spec).unwrap();
    std::os::unix::fs::symlink(&replacement, &spec).unwrap();
    let swapped = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            spec.to_str().unwrap(),
            "--input-sha256",
            &reserved,
            "--expect-absent",
        ])
        .output()
        .unwrap();
    assert!(!swapped.status.success());
    assert!(!target(&catalog).exists());

    fs::remove_file(&spec).unwrap();
    let bundle = temp.path().join("bundle");
    fs::create_dir(&bundle).unwrap();
    fs::write(bundle.join("agent.kdl"), valid_spec(false)).unwrap();
    fs::write(bundle.join("asset.txt"), "reserved").unwrap();
    let bundle_reserved = source_digest("--bundle", &bundle);
    fs::write(bundle.join("asset.txt"), "changed").unwrap();
    let changed_bundle = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--bundle",
            bundle.to_str().unwrap(),
            "--input-sha256",
            &bundle_reserved,
            "--expect-absent",
        ])
        .output()
        .unwrap();
    assert!(!changed_bundle.status.success());
    assert!(
        String::from_utf8_lossy(&changed_bundle.stderr).contains("input precondition failed"),
        "{}",
        String::from_utf8_lossy(&changed_bundle.stderr)
    );
    assert!(!target(&catalog).exists());

    let external_asset = temp.path().join("external-asset");
    fs::write(&external_asset, "reserved").unwrap();
    fs::remove_file(bundle.join("asset.txt")).unwrap();
    std::os::unix::fs::symlink(&external_asset, bundle.join("asset.txt")).unwrap();
    let symlink_bundle = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--bundle",
            bundle.to_str().unwrap(),
            "--input-sha256",
            &bundle_reserved,
            "--expect-absent",
        ])
        .output()
        .unwrap();
    assert!(!symlink_bundle.status.success());
    assert!(!target(&catalog).exists());
}

#[test]
fn cas_rejects_stale_writers_and_preserves_resources() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(agent.join("resources/inbox")).unwrap();
    let old = valid_spec(false);
    fs::write(agent.join("agent.kdl"), &old).unwrap();
    fs::write(agent.join("resources/inbox/message.md"), "keep me").unwrap();
    fs::write(agent.join("status"), "busy").unwrap();
    fs::write(agent.join(".status.tmp-live"), "busy").unwrap();
    fs::create_dir_all(agent.join("assets")).unwrap();
    fs::write(agent.join("assets/PERSONA.md"), "static input").unwrap();
    let workspace = temp.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let replacement = format!(
        "agent \"worker\" {{\n  host \"host\"\n  retired #true\n  workspace \"{}\"\n  argv \"true\"\n  render {{\n    copy \"assets/PERSONA.md\" \"PERSONA.md\"\n  }}\n}}\n",
        workspace.display()
    );
    let candidate = temp.path().join("retired.kdl");
    fs::write(&candidate, &replacement).unwrap();

    let rejected = publish(&catalog, &candidate, &["--expect-sha256", &"0".repeat(64)]);
    assert!(!rejected.status.success());
    assert_eq!(fs::read_to_string(agent.join("agent.kdl")).unwrap(), old);

    let accepted = publish(
        &catalog,
        &candidate,
        &["--expect-sha256", &sha256(old.as_bytes())],
    );
    assert!(
        accepted.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let accepted: Value = serde_json::from_slice(&accepted.stdout).unwrap();
    assert_eq!(accepted["status"], "published");
    assert_eq!(
        fs::read_to_string(agent.join("resources/inbox/message.md")).unwrap(),
        "keep me"
    );
    assert_eq!(fs::read_to_string(agent.join("status")).unwrap(), "busy");
    assert_eq!(
        fs::read_to_string(agent.join(".status.tmp-live")).unwrap(),
        "busy"
    );
    assert_eq!(
        fs::read_to_string(agent.join("assets/PERSONA.md")).unwrap(),
        "static input"
    );
}

#[test]
fn unchanged_still_fails_closed_on_an_invalid_full_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let spec = temp.path().join("candidate.kdl");
    fs::write(&spec, valid_spec(false)).unwrap();
    fs::write(agent.join("agent.kdl"), valid_spec(false)).unwrap();
    let broken = catalog.join("agents/host/broken/agent.kdl");
    fs::create_dir_all(broken.parent().unwrap()).unwrap();
    fs::write(&broken, "agent \"broken\" {").unwrap();

    let output = publish(&catalog, &spec, &["--expect-absent"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("full-catalog validation"));
}

#[test]
fn full_catalog_admission_rejects_same_host_render_ownership_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let workspace = temp.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let sibling = catalog.join("agents/host/sibling/agent.kdl");
    fs::create_dir_all(sibling.parent().unwrap()).unwrap();
    fs::write(
        &sibling,
        format!(
            "agent \"sibling\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n  render {{ file \"shared\" \"one\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(
        &candidate,
        format!(
            "agent \"worker\" {{\n  host \"host\"\n  workspace \"{}\"\n  argv \"true\"\n  render {{ file \"shared\" \"two\" }}\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();

    let output = publish(&catalog, &candidate, &["--expect-absent"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("render-owner-conflict"));
}

#[test]
fn incomplete_apply_marker_blocks_declarations_but_not_the_state_plane() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let active = valid_spec(false);
    fs::write(agent.join("agent.kdl"), &active).unwrap();
    fs::create_dir_all(catalog.join(".st2")).unwrap();
    fs::write(
        st2::catalog_lock::apply_marker_path(&catalog),
        "malformed still means incomplete",
    )
    .unwrap();

    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, &active).unwrap();
    let publication = publish(&catalog, &candidate, &["--expect-absent"]);
    assert!(!publication.status.success());
    assert!(String::from_utf8_lossy(&publication.stderr).contains("apply is incomplete"));

    let reconcile = st2()
        .args([
            "up",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "host",
            "--once",
        ])
        .output()
        .unwrap();
    assert!(!reconcile.status.success());
    assert!(String::from_utf8_lossy(&reconcile.stderr).contains("apply is incomplete"));

    let bin = temp.path().join("marker-bin");
    fs::create_dir(&bin).unwrap();
    let runtime_called = temp.path().join("runtime-called");
    let fake_pty = bin.join("pty");
    fs::write(
        &fake_pty,
        "#!/bin/sh\n: > \"$ST2_TEST_RUNTIME_CALLED\"\nexit 1\n",
    )
    .unwrap();
    let mut permissions = fs::metadata(&fake_pty).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    fs::set_permissions(&fake_pty, permissions).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut resident = st2()
        .args([
            "up",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "host",
            "--interval",
            "1",
        ])
        .env("PATH", path)
        .env("ST2_TEST_RUNTIME_CALLED", &runtime_called)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(150));
    assert!(
        resident.try_wait().unwrap().is_none(),
        "resident supervisor restart-stormed on an incomplete apply"
    );
    unsafe {
        libc::kill(resident.id() as i32, libc::SIGTERM);
    }
    let resident = resident.wait_with_output().unwrap();
    assert!(
        resident.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&resident.stderr)
    );
    assert!(
        !runtime_called.exists(),
        "incomplete apply reached runtime observation/action"
    );

    fs::write(
        st2::catalog_lock::apply_marker_path(&catalog),
        format!(
            "{{\"schema\":\"st2.catalog-apply-incomplete.v1\",\"stageName\":\"catalog-apply-stage-{hash}\",\"expectedRootSha256\":\"{hash}\",\"preparedRootSha256\":\"{hash}\",\"originalPaths\":[\"agents/host/worker/agent.kdl\"]}}\n",
            hash = "0".repeat(64)
        ),
    )
    .unwrap();

    let status = st2()
        .args([
            "status",
            "host.worker",
            "--set",
            "busy",
            "--catalog",
            catalog.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );

    let mut context = st2()
        .args([
            "context",
            "write",
            "host.worker",
            "--catalog",
            catalog.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    context
        .stdin
        .take()
        .unwrap()
        .write_all(b"durable context\n")
        .unwrap();
    let context = context.wait_with_output().unwrap();
    assert!(
        context.status.success(),
        "{}",
        String::from_utf8_lossy(&context.stderr)
    );

    let resource = st2()
        .args([
            "resource",
            "add",
            "https://example.invalid/result",
            "--as",
            "host.worker",
            "--catalog",
            catalog.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        resource.status.success(),
        "{}",
        String::from_utf8_lossy(&resource.stderr)
    );

    let message = st2()
        .args([
            "message",
            "send",
            "host.worker",
            "--message",
            "still live",
            "--as",
            "host.worker",
            "--catalog",
            catalog.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        message.status.success(),
        "{}",
        String::from_utf8_lossy(&message.stderr)
    );
}

#[test]
fn malformed_multiple_and_implicit_specs_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    fs::create_dir(&catalog).unwrap();
    for (name, contents) in [
        ("malformed.kdl", "agent \"worker\" {"),
        (
            "multiple.kdl",
            "agent \"one\" { host \"host\" argv \"true\" }\nagent \"two\" { host \"host\" argv \"true\" }\n",
        ),
        ("implicit.kdl", "agent \"worker\" { argv \"true\" }\n"),
        (
            "extra-top-level.kdl",
            "agent \"worker\" { host \"host\" argv \"true\" }\nmeta {}\n",
        ),
    ] {
        let spec = temp.path().join(name);
        fs::write(&spec, contents).unwrap();
        let output = publish(&catalog, &spec, &["--expect-absent"]);
        assert!(
            !output.status.success(),
            "{name} unexpectedly published: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    assert!(!catalog.join("agents").exists());
}

#[test]
fn duplicate_routing_fields_are_not_publishable() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    fs::create_dir(&catalog).unwrap();
    let spec = temp.path().join("duplicate-host.kdl");
    fs::write(
        &spec,
        r#"agent "worker" {
  host "first"
  host "second"
  argv "true"
}
"#,
    )
    .unwrap();

    let output = publish(&catalog, &spec, &["--expect-absent"]);
    assert!(
        !output.status.success(),
        "duplicate routing fields unexpectedly published: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!catalog.join("agents").exists());
}

#[test]
fn catalog_control_names_and_path_traversal_are_not_publishable_placements() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    fs::create_dir(&catalog).unwrap();
    for (name, placement) in [("traversal.kdl", "../escape"), ("control-host.kdl", ".st2")] {
        let spec = temp.path().join(name);
        fs::write(
            &spec,
            format!("agent \"worker\" {{\n  host \"{placement}\"\n  argv \"true\"\n}}\n"),
        )
        .unwrap();
        assert!(
            !publish(&catalog, &spec, &["--expect-absent"])
                .status
                .success()
        );
    }
    assert!(!catalog.join("agents").exists());
}

#[test]
fn a_symlinked_target_ancestor_is_rejected_before_shadow_writes() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let outside = temp.path().join("outside");
    fs::create_dir_all(catalog.join("agents")).unwrap();
    fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, catalog.join("agents/host")).unwrap();
    let spec = temp.path().join("candidate.kdl");
    fs::write(&spec, valid_spec(false)).unwrap();

    let output = publish(&catalog, &spec, &["--expect-absent"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not a real directory"));
    assert!(!outside.join("worker").exists());
}

#[test]
fn bundle_is_atomic_create_only_and_retry_checks_the_full_payload() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let bundle = temp.path().join("bundle");
    fs::create_dir(&catalog).unwrap();
    fs::create_dir_all(bundle.join("resources/inbox")).unwrap();
    fs::write(bundle.join("agent.kdl"), valid_spec(false)).unwrap();
    fs::write(bundle.join("resources/inbox/kickoff.md"), "start").unwrap();
    let bundle_digest = source_digest("--bundle", &bundle);

    let first = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--bundle",
            bundle.to_str().unwrap(),
            "--input-sha256",
            &bundle_digest,
            "--expect-absent",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        fs::read_to_string(catalog.join("agents/host/worker/resources/inbox/kickoff.md")).unwrap(),
        "start"
    );

    let retry = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--bundle",
            bundle.to_str().unwrap(),
            "--input-sha256",
            &bundle_digest,
            "--expect-absent",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(retry.status.success());
    let retry: Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(retry["status"], "unchanged");

    let target_payload = catalog.join("agents/host/worker/resources/inbox/kickoff.md");
    let external = temp.path().join("external-kickoff");
    fs::write(&external, "start").unwrap();
    fs::remove_file(&target_payload).unwrap();
    std::os::unix::fs::symlink(&external, &target_payload).unwrap();
    let unsafe_retry = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--bundle",
            bundle.to_str().unwrap(),
            "--input-sha256",
            &bundle_digest,
            "--expect-absent",
        ])
        .output()
        .unwrap();
    assert!(!unsafe_retry.status.success());
    fs::remove_file(&target_payload).unwrap();
    fs::write(&target_payload, "start").unwrap();

    fs::write(bundle.join("resources/inbox/kickoff.md"), "different").unwrap();
    let changed_digest = source_digest("--bundle", &bundle);
    let mismatch = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--bundle",
            bundle.to_str().unwrap(),
            "--input-sha256",
            &changed_digest,
            "--expect-absent",
        ])
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
}

#[test]
fn spec_publish_crash_stages_only_in_the_control_plane() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let original = valid_spec(false);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, valid_spec(true)).unwrap();
    let digest = source_digest("--spec", &candidate);
    let crashed = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &digest,
            "--expect-sha256",
            &sha256(original.as_bytes()),
        ])
        .env("ST2_TEST_AGENT_PUBLISH_CRASH_AFTER_TEMP", "1")
        .output()
        .unwrap();
    assert!(!crashed.status.success());
    assert_eq!(crashed.status.signal(), Some(libc::SIGABRT));
    assert_eq!(
        fs::read_to_string(agent.join("agent.kdl")).unwrap(),
        original
    );
    assert!(!catalog.join(".st2/catalog-generation").exists());
    assert!(catalog.join(".st2/catalog-generation-incomplete").is_file());
    assert!(fs::read_dir(&agent).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("agent-publish-leaf-")
    }));
    let retry = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &digest,
            "--expect-sha256",
            &sha256(original.as_bytes()),
        ])
        .output()
        .unwrap();
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(
        fs::read_to_string(catalog.join(".st2/catalog-generation")).unwrap(),
        "2\n"
    );
}

#[test]
fn publish_post_commit_generation_failure_is_fenced_and_recovered() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let original = valid_spec(false);
    let desired = valid_spec(true);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, &desired).unwrap();
    let digest = source_digest("--spec", &candidate);
    let failed = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &digest,
            "--expect-sha256",
            &sha256(original.as_bytes()),
        ])
        .env("ST2_TEST_GENERATION_FAIL_AFTER_COMMIT", "1")
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert_eq!(
        fs::read_to_string(agent.join("agent.kdl")).unwrap(),
        desired
    );
    assert!(catalog.join(".st2/catalog-generation-incomplete").is_file());
    assert!(!catalog.join(".st2/catalog-generation").exists());
    let shared = st2()
        .args(["agents", "--catalog", catalog.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert!(!shared.status.success());

    let recovered = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &digest,
            "--expect-sha256",
            &sha256(desired.as_bytes()),
        ])
        .output()
        .unwrap();
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    assert_eq!(
        fs::read_to_string(catalog.join(".st2/catalog-generation")).unwrap(),
        "1\n"
    );
    assert!(!catalog.join(".st2/catalog-generation-incomplete").exists());
}

#[test]
fn success_receipt_requires_exact_locked_readback() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let original = valid_spec(false);
    let desired = valid_spec(true);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, &desired).unwrap();
    let ready = temp.path().join("readback-ready");
    let release = temp.path().join("readback-release");
    let publisher = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &sha256(desired.as_bytes()),
            "--expect-sha256",
            &sha256(original.as_bytes()),
            "--json",
        ])
        .env("ST2_TEST_AGENT_PUBLISH_READBACK_READY", &ready)
        .env("ST2_TEST_AGENT_PUBLISH_READBACK_RELEASE", &release)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&ready);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    fs::write(&release, "").unwrap();

    let publisher = publisher.wait_with_output().unwrap();
    assert!(!publisher.status.success());
    assert!(publisher.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&publisher.stderr).contains("readback mismatch"),
        "{}",
        String::from_utf8_lossy(&publisher.stderr)
    );
    assert!(catalog.join(".st2/catalog-generation-incomplete").is_file());
    assert!(!catalog.join(".st2/catalog-generation").exists());
}

#[test]
fn success_receipt_requires_locked_full_catalog_readmission() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let original = valid_spec(false);
    let desired = valid_spec(true);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, &desired).unwrap();
    let ready = temp.path().join("readmission-ready");
    let release = temp.path().join("readmission-release");
    let publisher = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &sha256(desired.as_bytes()),
            "--expect-sha256",
            &sha256(original.as_bytes()),
            "--json",
        ])
        .env("ST2_TEST_AGENT_PUBLISH_READBACK_READY", &ready)
        .env("ST2_TEST_AGENT_PUBLISH_READBACK_RELEASE", &release)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&ready);
    let adjacent = catalog.join("agents/host/adjacent");
    fs::create_dir(&adjacent).unwrap();
    fs::write(adjacent.join("agent.kdl"), valid_spec(false)).unwrap();
    fs::write(&release, "").unwrap();

    let publisher = publisher.wait_with_output().unwrap();
    assert!(!publisher.status.success());
    assert!(publisher.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&publisher.stderr).contains("locked core/catalog re-admission"),
        "{}",
        String::from_utf8_lossy(&publisher.stderr)
    );
    assert_eq!(
        fs::read_to_string(agent.join("agent.kdl")).unwrap(),
        desired
    );
    assert!(catalog.join(".st2/catalog-generation-incomplete").is_file());
    assert!(!catalog.join(".st2/catalog-generation").exists());
}

#[test]
fn control_directory_swap_cannot_redirect_publication_staging() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let original = valid_spec(false);
    let desired = valid_spec(true);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, &desired).unwrap();
    let digest = source_digest("--spec", &candidate);
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let publisher = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &digest,
            "--expect-sha256",
            &sha256(original.as_bytes()),
        ])
        .env("ST2_TEST_CATALOG_LOCK_HELD_READY", &ready)
        .env("ST2_TEST_CATALOG_LOCK_HELD_RELEASE", &release)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&ready);
    let retained = temp.path().join("retained-control");
    fs::rename(catalog.join(".st2"), &retained).unwrap();
    let outside = temp.path().join("outside-control");
    fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, catalog.join(".st2")).unwrap();
    fs::write(&release, "").unwrap();
    let publisher = publisher.wait_with_output().unwrap();
    assert!(
        publisher.status.success(),
        "{}",
        String::from_utf8_lossy(&publisher.stderr)
    );
    assert_eq!(
        fs::read_to_string(agent.join("agent.kdl")).unwrap(),
        desired
    );
    assert!(outside.read_dir().unwrap().next().is_none());
    assert!(retained.join("catalog-generation").is_file());
}

#[test]
fn intermediate_host_swap_cannot_redirect_publication_outside_the_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let original = valid_spec(false);
    fs::write(agent.join("agent.kdl"), &original).unwrap();
    let candidate = temp.path().join("candidate.kdl");
    fs::write(&candidate, valid_spec(true)).unwrap();
    let digest = source_digest("--spec", &candidate);
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let publisher = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            candidate.to_str().unwrap(),
            "--input-sha256",
            &digest,
            "--expect-sha256",
            &sha256(original.as_bytes()),
        ])
        .env("ST2_TEST_AGENT_PUBLISH_READY", &ready)
        .env("ST2_TEST_AGENT_PUBLISH_RELEASE", &release)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&ready);

    let retained_host = temp.path().join("retained-host");
    fs::rename(catalog.join("agents/host"), &retained_host).unwrap();
    let outside = temp.path().join("outside-host");
    fs::create_dir_all(outside.join("worker")).unwrap();
    std::os::unix::fs::symlink(&outside, catalog.join("agents/host")).unwrap();
    fs::write(&release, "").unwrap();
    let publisher = publisher.wait_with_output().unwrap();
    assert!(
        !publisher.status.success(),
        "publication unexpectedly succeeded: {}",
        String::from_utf8_lossy(&publisher.stdout)
    );
    assert!(!outside.join("worker/agent.kdl").exists());
    assert_eq!(
        fs::read_to_string(retained_host.join("worker/agent.kdl")).unwrap(),
        original
    );
    assert!(!catalog.join(".st2/catalog-generation").exists());
}

#[test]
fn compile_agent_is_not_a_cli_writer_anymore() {
    let output = st2().args(["compile-agent", "--help"]).output().unwrap();
    assert!(!output.status.success());
    let help = st2().arg("--help").output().unwrap();
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(!help.contains("compile-agent"));
    assert!(help.contains("agent"));

    let completions = st2().args(["completions", "bash"]).output().unwrap();
    assert!(completions.status.success());
    let completions = String::from_utf8(completions.stdout).unwrap();
    for flag in [
        "--spec",
        "--bundle",
        "--expect-absent",
        "--expect-sha256",
        "--input-sha256",
        "--prepared",
        "--resume",
        "--json",
    ] {
        assert!(
            completions.contains(flag),
            "generated completion omitted {flag}"
        );
    }
    for command in ["digest", "catalog", "snapshot", "apply"] {
        assert!(
            completions.contains(command),
            "generated completion omitted {command}"
        );
    }
    assert!(!completions.contains("compile-agent"));
}

#[test]
fn concurrent_publishers_serialize_and_only_one_wins_the_cas() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let old = valid_spec(false);
    fs::write(agent.join("agent.kdl"), &old).unwrap();
    let one = temp.path().join("one.kdl");
    let two = temp.path().join("two.kdl");
    fs::write(
        &one,
        "agent \"worker\" {\n  host \"host\"\n  retired #true\n  argv \"true\"\n}\n",
    )
    .unwrap();
    fs::write(
        &two,
        "agent \"worker\" {\n  host \"host\"\n  role \"other\"\n  retired #true\n  argv \"true\"\n}\n",
    )
    .unwrap();
    let expected = sha256(old.as_bytes());

    // Hold EX while both real CLI processes start, so neither can finish before the other is
    // contending on the same persistent lock.
    let gate = st2::CatalogLock::exclusive(&catalog).unwrap();
    let first_attempt = temp.path().join("first-publisher-lock-attempt");
    let mut first = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            one.to_str().unwrap(),
            "--expect-sha256",
            &expected,
            "--input-sha256",
            &sha256(fs::read(&one).unwrap().as_slice()),
            "--json",
        ])
        .env("ST2_TEST_CATALOG_LOCK_ATTEMPT", &first_attempt)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let second_attempt = temp.path().join("second-publisher-lock-attempt");
    let mut second = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            two.to_str().unwrap(),
            "--expect-sha256",
            &expected,
            "--input-sha256",
            &sha256(fs::read(&two).unwrap().as_slice()),
            "--json",
        ])
        .env("ST2_TEST_CATALOG_LOCK_ATTEMPT", &second_attempt)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&first_attempt);
    wait_for_path(&second_attempt);
    assert!(first.try_wait().unwrap().is_none());
    assert!(second.try_wait().unwrap().is_none());
    drop(gate);

    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert_eq!(
        usize::from(first.status.success()) + usize::from(second.status.success()),
        1,
        "first stderr: {}\nsecond stderr: {}",
        String::from_utf8_lossy(&first.stderr),
        String::from_utf8_lossy(&second.stderr)
    );
}

#[test]
fn retirement_cannot_commit_between_reconcile_discovery_and_launch() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let agent = catalog.join("agents/host/worker");
    fs::create_dir_all(&agent).unwrap();
    let active = valid_spec(false);
    fs::write(agent.join("agent.kdl"), &active).unwrap();
    let retired = temp.path().join("retired.kdl");
    fs::write(&retired, valid_spec(true)).unwrap();

    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let fake_pty = bin.join("pty");
    fs::write(
        &fake_pty,
        "#!/bin/sh\n\
         if [ \"$1\" = list ]; then\n\
           printf '[]\\n'\n\
           exit 0\n\
         fi\n\
         if [ \"$1\" = run ]; then\n\
           : > \"$ST2_TEST_RUN_ENTERED\"\n\
           while [ ! -e \"$ST2_TEST_RUN_RELEASE\" ]; do sleep 0.01; done\n\
           printf 'launch\\n' >> \"$ST2_TEST_LAUNCH_LOG\"\n\
           exit 0\n\
         fi\n\
         exit 0\n",
    )
    .unwrap();
    let mut permissions = fs::metadata(&fake_pty).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    fs::set_permissions(&fake_pty, permissions).unwrap();
    let entered = temp.path().join("run-entered");
    let release = temp.path().join("run-release");
    let launches = temp.path().join("launches");
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let reconcile = st2()
        .args([
            "up",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "host",
            "--once",
        ])
        .env("PATH", &path)
        .env_remove("XDG_RUNTIME_DIR")
        .env("ST2_TEST_RUN_ENTERED", &entered)
        .env("ST2_TEST_RUN_RELEASE", &release)
        .env("ST2_TEST_LAUNCH_LOG", &launches)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&entered);

    let mut publisher = st2()
        .args([
            "agent",
            "publish",
            "--catalog",
            catalog.to_str().unwrap(),
            "--spec",
            retired.to_str().unwrap(),
            "--expect-sha256",
            &sha256(active.as_bytes()),
            "--input-sha256",
            &sha256(fs::read(&retired).unwrap().as_slice()),
            "--json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(
        publisher.try_wait().unwrap().is_none(),
        "publisher committed while reconcile still held its snapshot"
    );
    assert_eq!(fs::read_to_string(agent.join("agent.kdl")).unwrap(), active);

    fs::write(&release, "").unwrap();
    let reconcile = reconcile.wait_with_output().unwrap();
    assert!(
        reconcile.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&reconcile.stdout),
        String::from_utf8_lossy(&reconcile.stderr)
    );
    assert_eq!(fs::read_to_string(&launches).unwrap(), "launch\n");

    let publisher = publisher.wait_with_output().unwrap();
    assert!(
        publisher.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&publisher.stderr)
    );
    assert_eq!(
        fs::read_to_string(agent.join("agent.kdl")).unwrap(),
        valid_spec(true)
    );
}

fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}
