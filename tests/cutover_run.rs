//! CLI-level proofs for the sole durable cutover mutation driver.

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};
use st2::cutover_admission::{
    CutoverAction, ExternalCheckpointKind, GateId, HostId, LaunchPromptAuthority,
    ProviderFleetEntry, ProviderFleetProofAction,
};
use st2::cutover_driver::{
    CUTOVER_REQUEST_SCHEMA, CutoverCheckpointInput, CutoverPredecessorRetirementInput,
    CutoverRequest, canonical_request_bytes,
};
use st2::ding_reconcile::{DingDesiredExec, DingReconcileAction};
use st2::exec_retirement::{
    ExecRetirementReceipt, ExecRetirementStatus, LegacySuccessorTask, RetiredDisposition,
    RetiredRecordEvidence, RetiredTarget, RetirementAuthorityKind, SuccessorDesiredState,
};

fn st2() -> std::process::Command {
    std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn predecessor_receipt(catalog: &Path, host: &str, catalog_sha256: &str) -> Vec<u8> {
    let legacy_partition = Some(vec![LegacySuccessorTask {
        runtime_id: format!("{host}.legacy.ding"),
        agent: format!("{host}.legacy"),
        task: "ding".to_owned(),
        desired_state: SuccessorDesiredState::AbsentRetired,
    }]);
    let mut partition_bytes = serde_json::to_vec(&legacy_partition).unwrap();
    partition_bytes.push(b'\n');
    let mut partition_hash = Sha256::new();
    partition_hash.update(b"st2.exec-retirement-legacy-partition.v1\0");
    partition_hash.update(partition_bytes);
    let record = RetiredRecordEvidence {
        relative_path: format!("{host}.legacy.ding.pid"),
        device: 1,
        inode: 2,
        length: 3,
        modified_unix_ns: 4,
        sha256: digest(4),
    };
    let receipt = ExecRetirementReceipt {
        schema: "st2.exec-retirement.v1".to_owned(),
        request_sha256: digest(1),
        plan_sha256: digest(2),
        catalog: catalog.to_path_buf(),
        host: host.to_owned(),
        catalog_sha256: catalog_sha256.to_owned(),
        state_dir_device: 1,
        state_dir_inode: 2,
        journal_schema: "st2.exec-retirement-journal.v1".to_owned(),
        journal_sha256: digest(3),
        journal_status: "completed".to_owned(),
        status: ExecRetirementStatus::Completed,
        completed_at_unix_ms: 1,
        census_sha256: digest(5),
        forward_only_started: true,
        legacy_partition_sha256: format!("{:x}", partition_hash.finalize()),
        legacy_partition,
        targets: vec![RetiredTarget {
            runtime_id: format!("{host}.legacy.ding"),
            generation_id: None,
            authority_kind: RetirementAuthorityKind::StaleRecordOnly,
            disposition: RetiredDisposition::StaleRecordOnly,
            pid: 42,
            start_time_ticks: None,
            cgroup_path: None,
            scope_unit: None,
            cgroup_device: None,
            cgroup_inode: None,
            legacy_scope: None,
            membership: Vec::new(),
            freeze_observed: false,
            cgroup_outcome: None,
            durable_phase: "record-retired".to_owned(),
            record_before: record.clone(),
            record_after: RetiredRecordEvidence {
                relative_path: ".retirements/record".to_owned(),
                ..record
            },
        }],
    };
    let mut bytes = serde_json::to_vec(&receipt).unwrap();
    bytes.push(b'\n');
    bytes
}

fn checkpoint_request(catalog: &Path, root: &Path) -> CutoverRequest {
    let argv = vec![
        "/nix/store/axe/bin/axe".to_owned(),
        "agent".to_owned(),
        "launch".to_owned(),
        "--persona".to_owned(),
        "worker".to_owned(),
        "--harness".to_owned(),
        "codex".to_owned(),
        "--model".to_owned(),
        "gpt-5".to_owned(),
        "--effort".to_owned(),
        "high".to_owned(),
        "--mode".to_owned(),
        "managed-unattended".to_owned(),
        "--boot".to_owned(),
        "managed-v1".to_owned(),
    ];
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let provider_argv = argv
        .iter()
        .map(|argument| format!("{argument:?}"))
        .collect::<Vec<_>>()
        .join(" ");
    let provider_declaration = catalog.join("agents/testhost/worker-a/agent.kdl");
    fs::create_dir_all(provider_declaration.parent().unwrap()).unwrap();
    fs::write(
        provider_declaration,
        format!(
            r#"agent "worker-a" {{
  identity "worker-a"
  host "testhost"
  workspace {workspace:?}
  pty "agent" {{
    lifecycle "adopt-only"
    argv {provider_argv}
    env {{
      AGENT_PERSONA "worker"
      AGENT_RUNTIME_PROFILE "/nix/store/profile.json"
    }}
  }}
  exec "ding" {{
    argv "st2" "ding" "--identity" "testhost.worker-a" "--root" "$ST_ROOT"
  }}
}}
"#
        ),
    )
    .unwrap();
    let mut provider = ProviderFleetEntry {
        identity: "worker-a".to_owned(),
        host: HostId::parse("testhost").unwrap(),
        provider: "codex".to_owned(),
        account: "account-a".to_owned(),
        persona: "worker".to_owned(),
        workspace: workspace.clone(),
        prompt: LaunchPromptAuthority {
            runtime_profile_path: PathBuf::from("/nix/store/profile.json"),
            runtime_profile_sha256: digest(3),
            persona_prompt_path: PathBuf::from("/nix/store/personas/worker.md"),
            persona_prompt_sha256: digest(4),
            launch_receipt_path: PathBuf::from("/run/axe/receipts/worker.json"),
            launch_receipt_sha256: digest(5),
            injection_kind: st2::cutover_admission::PromptInjectionKind::CodexDeveloperInstructions,
        },
        canonical_argv: argv.clone(),
        argv_sha256: st2::cutover_admission::candidate_argv_sha256(&argv),
        profile_sha256: digest(3),
        harness: "codex".to_owned(),
        model: "gpt-5".to_owned(),
        effort: "high".to_owned(),
        mode: "managed-unattended".to_owned(),
        boot_contract: "managed-v1".to_owned(),
        launch_generation_id: "axe-generation-a".to_owned(),
        runtime_generation_id: "generation-a".to_owned(),
        trajectory_sha256: String::new(),
    };
    provider.trajectory_sha256 =
        st2::cutover_admission::provider_trajectory_sha256(&provider).unwrap();
    let providers = vec![provider];
    let mut ding = DingDesiredExec {
        runtime_id: "testhost.worker-a.ding".to_owned(),
        canonical_argv: vec![
            "st2".to_owned(),
            "ding".to_owned(),
            "--identity".to_owned(),
            "testhost.worker-a".to_owned(),
            "--root".to_owned(),
            catalog.canonicalize().unwrap().display().to_string(),
        ],
        canonical_cwd: workspace,
        canonical_env: Default::default(),
        launch_sha256: String::new(),
    };
    ding.launch_sha256 = st2::ding_reconcile::launch_sha256(&ding).unwrap();
    let desired = vec![ding];
    let source_catalog_sha256 =
        st2::catalog_transaction::declaration_root_sha256_locked(catalog).unwrap();
    let retirement_receipt = predecessor_receipt(
        &catalog.canonicalize().unwrap(),
        "testhost",
        &source_catalog_sha256,
    );
    let retirement_receipt_path = root.join("predecessor-retirement.json");
    fs::write(&retirement_receipt_path, &retirement_receipt).unwrap();
    CutoverRequest {
        schema: CUTOVER_REQUEST_SCHEMA.to_owned(),
        canonical_catalog: catalog.canonicalize().unwrap(),
        host: HostId::parse("testhost").unwrap(),
        gate_id: GateId::parse("gate-a").unwrap(),
        source_catalog_sha256,
        program: vec![
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::Cleanup,
                input_sha256: digest(7),
            },
            CutoverAction::ProviderFleetProof(ProviderFleetProofAction {
                providers_sha256: st2::cutover_admission::provider_entries_sha256(&providers)
                    .unwrap(),
                providers,
            }),
            CutoverAction::DingReconcile(DingReconcileAction {
                generation_id: "ding-generation-a".to_owned(),
                desired_sha256: st2::ding_reconcile::desired_set_sha256(&desired).unwrap(),
                desired,
            }),
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::BusContinuity,
                input_sha256: digest(9),
            },
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::FinalProof,
                input_sha256: digest(8),
            },
        ],
        predecessor_retirement: CutoverPredecessorRetirementInput {
            receipt: retirement_receipt_path,
            expect_sha256: sha256(&retirement_receipt),
        },
        catalog_inputs: Vec::new(),
        checkpoint_inputs: vec![
            CutoverCheckpointInput {
                action_index: 0,
                receipt: root.join("cleanup.json"),
            },
            CutoverCheckpointInput {
                action_index: 3,
                receipt: root.join("bus.json"),
            },
            CutoverCheckpointInput {
                action_index: 4,
                receipt: root.join("final.json"),
            },
        ],
    }
}

#[test]
fn run_surface_has_only_the_two_request_authority_flags() {
    let out = st2().args(["cutover", "run", "--help"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(help.contains("--request <FILE>"));
    assert!(help.contains("--expect-request-sha256 <HEX>"));
    assert!(!help.contains("--once"));
    assert!(!help.contains("--force"));
    assert!(!help.contains("--resume"));
}

#[test]
fn wrong_request_digest_refuses_before_any_catalog_or_runtime_state_mutation() {
    let root = tempfile::tempdir().unwrap();
    let request = root.path().join("request.json");
    let catalog = root.path().join("catalog");
    let decoy = root.path().join("decoy");
    let state = root.path().join("state");
    fs::create_dir(&catalog).unwrap();
    fs::create_dir(&decoy).unwrap();
    fs::create_dir(&state).unwrap();
    fs::write(&request, b"{}\n").unwrap();

    let before_request = fs::read(&request).unwrap();
    let out = st2()
        .args([
            "cutover",
            "run",
            "--request",
            request.to_str().unwrap(),
            "--expect-request-sha256",
            &"00".repeat(32),
        ])
        .env("CATALOG", &decoy)
        .env("XDG_STATE_HOME", &state)
        .output()
        .unwrap();

    assert!(!out.status.success());
    assert!(out.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("cutover request digest mismatch"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(fs::read(&request).unwrap(), before_request);
    assert_eq!(fs::read_dir(&catalog).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&state).unwrap().count(), 0);
}

#[test]
fn candidate_start_failure_preserves_the_ordinary_supervisor_and_no_fence() {
    let root = tempfile::tempdir().unwrap();
    let catalog = root.path().join("catalog");
    fs::create_dir(&catalog).unwrap();
    let legacy = catalog.join("agents/testhost/legacy/agent.kdl");
    fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    fs::write(
        legacy,
        "agent \"legacy\" {\n  host \"testhost\"\n  retired #true\n  exec \"ding\" {\n    command \"st2 ding --identity testhost.legacy --root $ST_ROOT\"\n  }\n}\n",
    )
    .unwrap();
    let request = checkpoint_request(&catalog, root.path());
    let bytes = canonical_request_bytes(&request).unwrap();
    let request_sha256 = sha256(&bytes);
    let request_path = root.path().join("request.json");
    fs::write(&request_path, bytes).unwrap();

    let config = root.path().join("config");
    let unit_dir = config.join("systemd/user");
    fs::create_dir_all(&unit_dir).unwrap();
    let ordinary = unit_dir.join("st2.service");
    fs::write(&ordinary, b"ordinary-supervisor\n").unwrap();
    let bin = root.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let systemctl = bin.join("systemctl");
    fs::write(
        &systemctl,
        r#"#!/bin/sh
if [ "$2" = enable ]; then exit 23; fi
if [ "$2" = show ]; then
  printf '%s\n' \
    'MainPID=1' \
    'ActiveState=active' \
    'LoadState=loaded' \
    'Restart=always' \
    'RestartUSec=2s' \
    "FragmentPath=$XDG_CONFIG_HOME/systemd/user/$3" \
    'DropInPaths=' \
    'NeedDaemonReload=no' \
    'UnitFileState=enabled' \
    'Transient=no'
fi
exit 0
"#,
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = st2()
        .args([
            "cutover",
            "install",
            "--request",
            request_path.to_str().unwrap(),
            "--expect-request-sha256",
            &request_sha256,
        ])
        .env("XDG_CONFIG_HOME", &config)
        .env("PATH", path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary-supervisor\n");
    assert!(!catalog.join(".st2/cutover/active.json").exists());

    let candidate = st2::service::CutoverCandidateServiceSpec::new(
        PathBuf::from(env!("CARGO_BIN_EXE_st2"))
            .canonicalize()
            .unwrap(),
        catalog.canonicalize().unwrap(),
        request_path.canonicalize().unwrap(),
        request_sha256.clone(),
        "testhost".to_owned(),
        "gate-a".to_owned(),
    )
    .unwrap();
    let forged = st2()
        .args([
            "cutover",
            "run",
            "--request",
            request_path.to_str().unwrap(),
            "--expect-request-sha256",
            &request_sha256,
        ])
        .env("XDG_CONFIG_HOME", &config)
        .env("PATH", format!("{}:", bin.display()))
        .env(st2::service::CUTOVER_CANDIDATE_ENV, candidate.unit_name)
        .env("INVOCATION_ID", "forged")
        .output()
        .unwrap();
    assert!(!forged.status.success());
    assert!(
        String::from_utf8_lossy(&forged.stderr).contains("MainPID mismatch"),
        "{}",
        String::from_utf8_lossy(&forged.stderr)
    );
    assert!(!catalog.join(".st2/cutover/active.json").exists());
}

#[test]
fn missing_checkpoint_boundary_is_durable_and_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let catalog = root.path().join("catalog");
    let state = root.path().join("state");
    fs::create_dir(&catalog).unwrap();
    fs::create_dir(&state).unwrap();
    let exec_state = state.join("st2/testhost/exec");
    fs::create_dir_all(&exec_state).unwrap();
    fs::write(exec_state.join("testhost.legacy.ding.pid"), b"2000000000\n").unwrap();
    let legacy = catalog.join("agents/testhost/legacy/agent.kdl");
    fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    fs::write(
        legacy,
        "agent \"legacy\" {\n  host \"testhost\"\n  retired #true\n  exec \"ding\" {\n    command \"st2 ding --identity testhost.legacy --root $ST_ROOT\"\n  }\n}\n",
    )
    .unwrap();
    let request = checkpoint_request(&catalog, root.path());
    let bytes = canonical_request_bytes(&request).unwrap();
    let request_sha256 = sha256(&bytes);
    let request_path = root.path().join("request.json");
    fs::write(&request_path, bytes).unwrap();
    let candidate = st2::service::CutoverCandidateServiceSpec::new(
        PathBuf::from(env!("CARGO_BIN_EXE_st2"))
            .canonicalize()
            .unwrap(),
        catalog.canonicalize().unwrap(),
        request_path.canonicalize().unwrap(),
        request_sha256.clone(),
        "testhost".to_owned(),
        "gate-a".to_owned(),
    )
    .unwrap();
    let config = root.path().join("config");
    let unit_dir = config.join("systemd/user");
    fs::create_dir_all(&unit_dir).unwrap();
    fs::write(
        unit_dir.join(&candidate.unit_name),
        st2::service::render_cutover_candidate_unit(&candidate),
    )
    .unwrap();
    let bin = root.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let systemctl = bin.join("systemctl");
    fs::write(
        &systemctl,
        r#"#!/bin/sh
if [ "$2" != show ]; then exit 1; fi
printf '%s\n' \
  "MainPID=$PPID" \
  'ActiveState=active' \
  'LoadState=loaded' \
  'Restart=always' \
  'RestartUSec=2s' \
  "FragmentPath=$XDG_CONFIG_HOME/systemd/user/$3" \
  'DropInPaths=' \
  'NeedDaemonReload=no' \
  'UnitFileState=enabled' \
  'Transient=no'
"#,
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let invoke = || {
        st2()
            .args([
                "cutover",
                "run",
                "--request",
                request_path.to_str().unwrap(),
                "--expect-request-sha256",
                &request_sha256,
            ])
            .env("XDG_STATE_HOME", &state)
            .env("XDG_CONFIG_HOME", &config)
            .env(st2::service::CUTOVER_CANDIDATE_ENV, &candidate.unit_name)
            .env("INVOCATION_ID", "test-invocation")
            .env("PATH", &path)
            .output()
            .unwrap()
    };
    let first = invoke();
    assert!(!first.status.success());
    assert!(
        !first.stdout.is_empty(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first_json["outcome"], "needs-checkpoint");
    assert_eq!(first_json["actionIndex"], 0);
    assert_eq!(first_json["kind"], "cleanup");
    assert_eq!(first_json["inputSha256"], digest(7));
    assert_eq!(
        first_json["receipt"],
        root.path().join("cleanup.json").display().to_string()
    );
    let marker = catalog.join(".st2/cutover/active.json");
    let marker_before = fs::read(&marker).unwrap();
    let receipt_path = root.path().join("predecessor-retirement.json");
    fs::remove_file(&receipt_path).unwrap();

    let second = invoke();
    assert!(!second.status.success());
    assert_eq!(second.stdout, first.stdout);
    assert_eq!(fs::read(marker).unwrap(), marker_before);
    assert!(!receipt_path.exists());
}
