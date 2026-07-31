use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use agent_spec::spec::{TaskKind, TaskLifecycle};
use serde_json::Value;

fn st2() -> Command {
    Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn fixture(root: &Path) -> PathBuf {
    let catalog = root.join("catalog");
    write(
        &catalog.join("catalog.kdl"),
        "catalog { pty-root \"/tmp/st2-catalog-projection-test-pty\" }\n",
    );
    write(&catalog.join("agents/h/.foreign-author.lock"), "mutable\n");
    write(
        &catalog.join("agents/h/active/agent.kdl"),
        r#"
agent "active" {
  host "h"
  role "worker"
  resource "issue" _tag="github-issue" uri="github-issue://example/project/1"
  argv "provider" "--literal=$HOME" "embedded ' quote" "line\nbreak"
  ding
}
"#,
    );
    write(
        &catalog.join("agents/h/explicit/agent.kdl"),
        r#"
agent "explicit" {
  host "h"
  pty "agent" {
    id "custom.provider"
    argv "provider-2" "--model" "open"
    lifecycle "service"
  }
  exec "metrics" {
    argv "metrics" "--agent" "explicit"
  }
}
"#,
    );
    write(
        &catalog.join("agents/h/retired/agent.kdl"),
        r#"
agent "retired" {
  host "h"
  retired #true
  argv "retired-provider"
  ding
}
"#,
    );
    write(
        &catalog.join("agents/foreign/observer/agent.kdl"),
        r#"
agent "observer" {
  host "foreign"
  resource "fleet" _tag="catalog" uri="st2-catalog://fleet/main"
  exec "observe" {
    argv "observe" "--read-only"
  }
}
"#,
    );
    catalog
}

fn snapshot(catalog: &Path, output: &Path) -> Value {
    let result = st2()
        .args(["catalog", "snapshot", "--catalog"])
        .arg(catalog)
        .args(["--output"])
        .arg(output)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

fn project(catalog: &Path, snapshot: &Path, hash: &str, output: &Path) -> Output {
    st2()
        .args(["catalog", "project", "--catalog"])
        .arg(catalog)
        .arg("--snapshot")
        .arg(snapshot)
        .args(["--expect-sha256", hash, "--output"])
        .arg(output)
        .arg("--json")
        .output()
        .unwrap()
}

fn task_argvs(catalog: &Path, kind: TaskKind) -> Vec<(String, String, Option<Vec<String>>)> {
    let mut result = st2::discover(catalog)
        .specs
        .into_iter()
        .flat_map(|spec| {
            let identity = spec.identity;
            spec.tasks
                .into_iter()
                .filter(move |task| task.kind == kind)
                .map(move |task| (identity.clone(), task.name, task.argv))
        })
        .collect::<Vec<_>>();
    result.sort();
    result
}

fn resources(catalog: &Path) -> Vec<(String, String, String, String)> {
    let mut result = st2::discover(catalog)
        .specs
        .into_iter()
        .flat_map(|spec| {
            let identity = spec.identity;
            spec.resources.into_iter().map(move |resource| {
                (
                    identity.clone(),
                    resource.name().to_owned(),
                    resource.tag().to_owned(),
                    resource.uri().to_owned(),
                )
            })
        })
        .collect::<Vec<_>>();
    result.sort();
    result
}

#[test]
fn projection_is_one_atomic_restart_safe_bundle_with_exact_identity_partition() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path());
    let retained = temp.path().join("retained");
    let snapshot_receipt = snapshot(&catalog, &retained);
    let source_hash = snapshot_receipt["rootSha256"].as_str().unwrap();

    // A foreign mutable live declaration cannot affect the retained capability.
    fs::write(
        catalog.join("agents/foreign/observer/agent.kdl"),
        "agent \"changed-after-snapshot\" { host \"foreign\" argv \"false\" }\n",
    )
    .unwrap();

    let output = temp.path().join("projection");
    let projected = project(&catalog, &retained, source_hash, &output);
    assert!(
        projected.status.success(),
        "{}",
        String::from_utf8_lossy(&projected.stderr)
    );
    let result: Value = serde_json::from_slice(&projected.stdout).unwrap();
    assert_eq!(result["schema"], "st2.catalog-projection.v1");
    assert_eq!(result["receipt"]["sourceRootSha256"], source_hash);
    assert_eq!(
        fs::read_to_string(output.join("bundle.sha256"))
            .unwrap()
            .trim(),
        result["bundleSha256"].as_str().unwrap()
    );
    assert_eq!(
        fs::read(output.join("service/agents/h/active/agent.kdl")).unwrap(),
        fs::read(retained.join("agents/h/active/agent.kdl")).unwrap()
    );
    assert!(!retained.join("agents/h/.foreign-author.lock").exists());

    let service = output.join("service");
    let adopt = output.join("adopt-only");
    let witness = output.join("provider-witness");
    let service_agents = st2::discover(&service)
        .specs
        .iter()
        .map(|spec| spec.bus_id(spec.host.as_deref().unwrap()))
        .collect::<Vec<_>>();
    let witness_agents = st2::discover(&witness)
        .specs
        .iter()
        .map(|spec| spec.bus_id(spec.host.as_deref().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(service_agents, witness_agents);
    assert!(witness_agents.contains(&"foreign.observer".to_string()));

    // Direct argv values are semantic data, never reconstructed through a shell.
    assert_eq!(
        task_argvs(&service, TaskKind::Pty),
        task_argvs(&adopt, TaskKind::Pty)
    );
    let source_active = task_argvs(&service, TaskKind::Pty)
        .into_iter()
        .filter(|(agent, _, _)| agent != "retired")
        .collect::<Vec<_>>();
    assert_eq!(source_active, task_argvs(&witness, TaskKind::Pty));
    assert_eq!(
        task_argvs(&service, TaskKind::Exec),
        task_argvs(&adopt, TaskKind::Exec)
    );
    assert!(task_argvs(&witness, TaskKind::Exec).is_empty());
    assert_eq!(resources(&service), resources(&adopt));
    assert_eq!(resources(&service), resources(&witness));

    for root in [&adopt, &witness] {
        for spec in st2::discover(root).specs {
            for task in spec.tasks {
                if task.kind == TaskKind::Pty {
                    assert_eq!(task.lifecycle, TaskLifecycle::AdoptOnly);
                }
            }
        }
    }
    let receipt: Value =
        serde_json::from_slice(&fs::read(output.join("receipt.json")).unwrap()).unwrap();
    assert_eq!(
        receipt["service"]["providerTaskIds"],
        receipt["adoptOnly"]["providerTaskIds"]
    );
    assert_eq!(
        receipt["service"]["activeProviderTaskIds"],
        receipt["providerWitness"]["providerTaskIds"]
    );
    assert_eq!(
        receipt["service"]["retiredAbsentProviderTaskIds"],
        serde_json::json!(["h.retired"])
    );
    assert_eq!(
        receipt["providerWitness"]["retiredAbsentProviderTaskIds"],
        serde_json::json!([])
    );
    assert_eq!(
        receipt["providerWitness"]["agentIds"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn projection_refuses_stale_authority_aliases_unsupported_shapes_and_retries() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path());
    let retained = temp.path().join("retained");
    let snapshot_receipt = snapshot(&catalog, &retained);
    let source_hash = snapshot_receipt["rootSha256"].as_str().unwrap();
    let output = temp.path().join("projection");

    let stale = project(&catalog, &retained, &"0".repeat(64), &output);
    assert!(!stale.status.success());
    assert!(!output.exists());

    let alias = temp.path().join("snapshot-alias");
    std::os::unix::fs::symlink(&retained, &alias).unwrap();
    let aliased = project(&catalog, &alias, source_hash, &output);
    assert!(!aliased.status.success());
    assert!(!output.exists());

    let output_parent_alias = temp.path().join("output-parent-alias");
    std::os::unix::fs::symlink(&retained, &output_parent_alias).unwrap();
    let nested = project(
        &catalog,
        &retained,
        source_hash,
        &output_parent_alias.join("projection"),
    );
    assert!(!nested.status.success());
    assert!(!retained.join("projection").exists());

    let in_catalog = project(
        &catalog,
        &retained,
        source_hash,
        &catalog.join("projection"),
    );
    assert!(!in_catalog.status.success());
    assert!(!catalog.join("projection").exists());
    let catalog_parent_alias = temp.path().join("catalog-parent-alias");
    std::os::unix::fs::symlink(&catalog, &catalog_parent_alias).unwrap();
    let aliased_catalog_output = project(
        &catalog,
        &retained,
        source_hash,
        &catalog_parent_alias.join("projection"),
    );
    assert!(!aliased_catalog_output.status.success());
    assert!(!catalog.join("projection").exists());

    let snapshot_parent = temp.path().join("snapshot-parent");
    fs::create_dir(&snapshot_parent).unwrap();
    let nested_snapshot = snapshot_parent.join("retained");
    let nested_receipt = snapshot(&catalog, &nested_snapshot);
    let snapshot_parent_alias = temp.path().join("snapshot-parent-alias");
    std::os::unix::fs::symlink(&snapshot_parent, &snapshot_parent_alias).unwrap();
    let parent_aliased_snapshot = project(
        &catalog,
        &snapshot_parent_alias.join("retained"),
        nested_receipt["rootSha256"].as_str().unwrap(),
        &output,
    );
    assert!(!parent_aliased_snapshot.status.success());
    assert!(
        String::from_utf8_lossy(&parent_aliased_snapshot.stderr).contains("canonical absolute")
    );
    assert!(!output.exists());

    let malformed = temp.path().join("malformed");
    fs::create_dir_all(malformed.join("agents/h/bad")).unwrap();
    fs::copy(retained.join("catalog.kdl"), malformed.join("catalog.kdl")).unwrap();
    write(
        &malformed.join("agents/h/bad/agent.kdl"),
        "agent \"bad\" { host \"h\"; pty \"agent\" }\n",
    );
    let malformed_receipt = snapshot(&malformed, &temp.path().join("malformed-snapshot"));
    let rejected = project(
        &malformed,
        &temp.path().join("malformed-snapshot"),
        malformed_receipt["rootSha256"].as_str().unwrap(),
        &output,
    );
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("not-runnable"));
    assert!(!output.exists());

    let duplicate_retired = temp.path().join("duplicate-retired");
    fs::create_dir_all(duplicate_retired.join("agents/h/bad")).unwrap();
    fs::copy(
        retained.join("catalog.kdl"),
        duplicate_retired.join("catalog.kdl"),
    )
    .unwrap();
    write(
        &duplicate_retired.join("agents/h/bad/agent.kdl"),
        "agent \"bad\" { host \"h\"; retired #false; retired #true; argv \"provider\" }\n",
    );
    let duplicate_snapshot = temp.path().join("duplicate-retired-snapshot");
    let duplicate_receipt = snapshot(&duplicate_retired, &duplicate_snapshot);
    let rejected = project(
        &duplicate_retired,
        &duplicate_snapshot,
        duplicate_receipt["rootSha256"].as_str().unwrap(),
        &output,
    );
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("retired more than once"));
    assert!(!output.exists());

    let first = project(&catalog, &retained, source_hash, &output);
    assert!(first.status.success());
    let retry = project(&catalog, &retained, source_hash, &output);
    assert!(!retry.status.success());
    assert!(String::from_utf8_lossy(&retry.stderr).contains("create-only"));
}

#[test]
fn typed_projection_apply_verifies_target_receipt_digest_and_materialized_child() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = fixture(temp.path());
    let retained = temp.path().join("retained");
    let snapshot_receipt = snapshot(&catalog, &retained);
    let source_hash = snapshot_receipt["rootSha256"].as_str().unwrap();
    let bundle = temp.path().join("bundle");
    let projected = project(&catalog, &retained, source_hash, &bundle);
    assert!(
        projected.status.success(),
        "{}",
        String::from_utf8_lossy(&projected.stderr)
    );
    let bundle_hash = serde_json::from_slice::<Value>(&projected.stdout).unwrap()["bundleSha256"]
        .as_str()
        .unwrap()
        .to_owned();

    let wrong_target = temp.path().join("wrong-target");
    fs::create_dir(&wrong_target).unwrap();
    fs::copy(
        catalog.join("catalog.kdl"),
        wrong_target.join("catalog.kdl"),
    )
    .unwrap();
    let wrong = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&wrong_target)
        .args(["--projection-bundle"])
        .arg(&bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &bundle_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!wrong.status.success());
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("does not match apply target"));

    let wrong_lineage = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &bundle_hash,
            "--expect-sha256",
            &"0".repeat(64),
        ])
        .output()
        .unwrap();
    assert!(!wrong_lineage.status.success());
    assert!(String::from_utf8_lossy(&wrong_lineage.stderr).contains("source root"));

    let wrong_bundle_capability = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &"0".repeat(64),
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!wrong_bundle_capability.status.success());
    assert!(String::from_utf8_lossy(&wrong_bundle_capability.stderr).contains("caller expected"));

    let extra_bundle = temp.path().join("extra-bundle");
    let extra_projection = project(&catalog, &retained, source_hash, &extra_bundle);
    assert!(extra_projection.status.success());
    let extra_hash =
        serde_json::from_slice::<Value>(&extra_projection.stdout).unwrap()["bundleSha256"]
            .as_str()
            .unwrap()
            .to_owned();
    fs::write(extra_bundle.join("foreign.mutable"), "not in the bundle\n").unwrap();
    let extra = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&extra_bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &extra_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!extra.status.success());
    assert!(String::from_utf8_lossy(&extra.stderr).contains("must contain exactly"));

    let unselected_bundle = temp.path().join("unselected-bundle");
    let unselected_projection = project(&catalog, &retained, source_hash, &unselected_bundle);
    assert!(unselected_projection.status.success());
    let unselected_hash =
        serde_json::from_slice::<Value>(&unselected_projection.stdout).unwrap()["bundleSha256"]
            .as_str()
            .unwrap()
            .to_owned();
    let unselected_path = unselected_bundle.join("provider-witness/agents/h/active/agent.kdl");
    let mut unselected_bytes = fs::read(&unselected_path).unwrap();
    unselected_bytes.push(b'\n');
    fs::write(&unselected_path, unselected_bytes).unwrap();
    let unselected = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&unselected_bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &unselected_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!unselected.status.success());
    assert!(
        String::from_utf8_lossy(&unselected.stderr)
            .contains("provider-witness does not match its receipt")
    );

    let oversized_bundle = temp.path().join("oversized-bundle");
    let oversized_projection = project(&catalog, &retained, source_hash, &oversized_bundle);
    assert!(oversized_projection.status.success());
    let oversized_hash =
        serde_json::from_slice::<Value>(&oversized_projection.stdout).unwrap()["bundleSha256"]
            .as_str()
            .unwrap()
            .to_owned();
    fs::OpenOptions::new()
        .write(true)
        .open(oversized_bundle.join("provider-witness/agents/h/active/agent.kdl"))
        .unwrap()
        .set_len(64 * 1024 * 1024 + 1)
        .unwrap();
    let oversized = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&oversized_bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &oversized_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!oversized.status.success());
    assert!(String::from_utf8_lossy(&oversized.stderr).contains("file exceeds"));

    let fanout_bundle = temp.path().join("fanout-bundle");
    let fanout_projection = project(&catalog, &retained, source_hash, &fanout_bundle);
    assert!(fanout_projection.status.success());
    let fanout_hash =
        serde_json::from_slice::<Value>(&fanout_projection.stdout).unwrap()["bundleSha256"]
            .as_str()
            .unwrap()
            .to_owned();
    let fanout = fanout_bundle.join("provider-witness/fanout");
    fs::create_dir(&fanout).unwrap();
    for index in 0..4097 {
        fs::create_dir(fanout.join(format!("{index:04}"))).unwrap();
    }
    let fanout = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&fanout_bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &fanout_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!fanout.status.success());
    assert!(String::from_utf8_lossy(&fanout.stderr).contains("filesystem entries"));

    let receipt_tamper_bundle = temp.path().join("receipt-tamper-bundle");
    let receipt_tamper_projection =
        project(&catalog, &retained, source_hash, &receipt_tamper_bundle);
    assert!(receipt_tamper_projection.status.success());
    let receipt_tamper_hash = serde_json::from_slice::<Value>(&receipt_tamper_projection.stdout)
        .unwrap()["bundleSha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let receipt_path = receipt_tamper_bundle.join("receipt.json");
    let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["admission"]["applyAdmissible"] =
        serde_json::json!(!receipt["admission"]["applyAdmissible"].as_bool().unwrap());
    fs::write(
        &receipt_path,
        format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
    )
    .unwrap();
    let tampered_receipt = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&receipt_tamper_bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &receipt_tamper_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!tampered_receipt.status.success());
    assert!(String::from_utf8_lossy(&tampered_receipt.stderr).contains("bundle sha256 mismatch"));

    let before = snapshot(&catalog, &temp.path().join("before-tamper"));
    let tampered_path = bundle.join("adopt-only/agents/h/active/agent.kdl");
    let mut tampered_bytes = fs::read(&tampered_path).unwrap();
    tampered_bytes.push(b'\n');
    fs::write(&tampered_path, tampered_bytes).unwrap();
    let tampered = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &bundle_hash,
            "--expect-sha256",
            source_hash,
        ])
        .output()
        .unwrap();
    assert!(!tampered.status.success());
    assert!(String::from_utf8_lossy(&tampered.stderr).contains("does not match its receipt"));
    let after = snapshot(&catalog, &temp.path().join("after-tamper"));
    assert_eq!(before["rootSha256"], after["rootSha256"]);

    // A fresh, independently verified bundle composes into ordinary full admission and CAS.
    let clean_bundle = temp.path().join("clean-bundle");
    let projected = project(&catalog, &retained, source_hash, &clean_bundle);
    assert!(projected.status.success());
    let clean_bundle_hash =
        serde_json::from_slice::<Value>(&projected.stdout).unwrap()["bundleSha256"]
            .as_str()
            .unwrap()
            .to_owned();
    let applied = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--projection-bundle"])
        .arg(&clean_bundle)
        .args([
            "--projection-child",
            "adopt-only",
            "--expect-bundle-sha256",
            &clean_bundle_hash,
            "--expect-sha256",
            source_hash,
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let applied: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(
        applied["afterSha256"],
        serde_json::from_slice::<Value>(&fs::read(clean_bundle.join("receipt.json")).unwrap())
            .unwrap()["adoptOnly"]["rootSha256"]
    );
}

#[test]
fn render_owner_conflicts_are_explicit_projection_evidence_not_apply_authority() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = temp.path().join("catalog");
    let workspace = temp.path().join("shared-workspace");
    fs::create_dir(&workspace).unwrap();
    write(
        &catalog.join("catalog.kdl"),
        "catalog { pty-root \"/tmp/st2-catalog-projection-test-pty\" }\n",
    );
    for (identity, content) in [("one", "first"), ("two", "second")] {
        write(
            &catalog.join(format!("agents/h/{identity}/agent.kdl")),
            &format!(
                "agent \"{identity}\" {{\n  host \"h\"\n  workspace {:?}\n  argv \"provider\"\n  render {{ file \"shared\" \"{content}\" }}\n}}\n",
                workspace.display().to_string()
            ),
        );
    }
    let retained = temp.path().join("retained");
    let snapshot_receipt = snapshot(&catalog, &retained);
    let source_hash = snapshot_receipt["rootSha256"].as_str().unwrap();
    let output = temp.path().join("projection");
    let projected = project(&catalog, &retained, source_hash, &output);
    assert!(
        projected.status.success(),
        "{}",
        String::from_utf8_lossy(&projected.stderr)
    );
    let receipt: Value =
        serde_json::from_slice(&fs::read(output.join("receipt.json")).unwrap()).unwrap();
    assert_eq!(receipt["admission"]["applyAdmissible"], false);
    assert_eq!(
        receipt["admission"]["exceptions"][0]["code"],
        "render-owner-conflict"
    );
    assert_eq!(receipt["admission"]["exceptions"][0]["count"], 1);
    assert_eq!(
        receipt["admission"]["exceptions"][0]["evidence"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let apply = st2()
        .args(["catalog", "apply", "--catalog"])
        .arg(&catalog)
        .args(["--prepared"])
        .arg(output.join("adopt-only"))
        .args(["--expect-sha256", source_hash, "--json"])
        .output()
        .unwrap();
    assert!(!apply.status.success());
    assert!(String::from_utf8_lossy(&apply.stderr).contains("render-owner-conflict"));
}
