//! Live crash-recovery proofs for the exact production cutover candidate service.
//!
//! These tests are ignored by the ordinary suite because they require a reachable user systemd
//! manager. Each case uses the real `st2 cutover install` and its exact
//! `st2 cutover run --request ... --expect-request-sha256 ...` unit command. The catalog, PTY root,
//! XDG config/state, request, and every evidence artifact are temporary. A unique runtime unit link
//! makes that temporary unit visible to the real user manager; cleanup restores the complete
//! pre-existing `st2-cutover-e2e-*` inventory.

#![cfg(all(target_os = "linux", debug_assertions))]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest as _, Sha256};
use st2::catalog_transaction::declaration_root_sha256_locked;
use st2::catalog_transaction::{SnapshotRequest, snapshot};
use st2::cutover_admission::{
    CUTOVER_TRANSACTION_SCHEMA, CatalogTransition, CompletedCheckpoint, CompletedDingReconcile,
    CutoverAction, CutoverMarker, EXTERNAL_CHECKPOINT_EVIDENCE_SCHEMA, ExternalCheckpointEvidence,
    ExternalCheckpointKind, ExternalCheckpointPayload, ExternalCheckpointReceipt, GateId, HostId,
    LaunchPromptAuthority, PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA,
    PROVIDER_FLEET_PROOF_EVIDENCE_SCHEMA, PredecessorRetirementEvidence, PromptInjectionKind,
    ProviderFleetEntry, ProviderFleetProofAction, ProviderFleetProofEvidence,
    candidate_argv_sha256, provider_entries_sha256, provider_launch_receipts_sha256,
    provider_trajectory_sha256,
};
use st2::cutover_driver::{
    CUTOVER_REQUEST_SCHEMA, CutoverCatalogInput, CutoverCheckpointInput,
    CutoverPredecessorRetirementInput, CutoverRequest, canonical_request_bytes,
};
use st2::ding_reconcile::{
    DING_RECONCILE_RECEIPT_SCHEMA, DingDesiredExec, DingReconcileAction, DingReconcileReceipt,
    desired_set_sha256, launch_sha256,
};
use st2::exec_retirement::{ExecRetirementReceipt, ExecRetirementStatus, LegacySuccessorTask};
use st2::host_lock::HostOwnership;
use st2::service::CutoverCandidateServiceSpec;

const TEST_BOUNDARY_ENV: &str = "ST2_TEST_CUTOVER_BOUNDARY";
const TEST_SENTINEL_ENV: &str = "ST2_TEST_CUTOVER_SENTINEL";
const HOST: &str = "e2e-host";
const GATE: &str = "e2e-gate";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

#[derive(Clone, Copy)]
enum CrashBoundary {
    BeforeRun,
    AfterFinalize,
    AfterFinalizedHistoryBeforeActive,
}

impl CrashBoundary {
    fn as_str(self) -> &'static str {
        match self {
            Self::BeforeRun => "before-run",
            Self::AfterFinalize => "after-finalize",
            Self::AfterFinalizedHistoryBeforeActive => {
                "after-finalized-history-before-active-finalized"
            }
        }
    }
}

#[test]
#[ignore = "live user-systemd crash recovery; run explicitly"]
fn restart_always_recovers_a_crash_before_successor_readiness() {
    live_restart_proof(CrashBoundary::BeforeRun, false);
}

#[test]
#[ignore = "live user-systemd crash recovery; run explicitly"]
fn restart_always_recovers_finalize_before_readiness_without_duplicate_supervisor() {
    live_restart_proof(CrashBoundary::AfterFinalize, false);
}

#[test]
#[ignore = "live user-systemd crash recovery; run explicitly"]
fn restart_always_reacquires_history_only_supervision_after_readiness() {
    live_restart_proof(CrashBoundary::BeforeRun, true);
}

#[test]
#[ignore = "live user-systemd crash recovery; run explicitly"]
fn restart_always_recovers_history_before_active_finalization() {
    live_restart_proof(CrashBoundary::AfterFinalizedHistoryBeforeActive, false);
}

#[test]
fn replay_fixture_is_preflight_valid_and_catalog_transition_is_non_vacuous() {
    let root = tempfile::tempdir().unwrap();
    let catalog = root.path().join("catalog");
    let artifacts = root.path().join("artifacts");
    fs::create_dir(&catalog).unwrap();
    fs::create_dir(&artifacts).unwrap();
    let fixture = write_replay_fixture(&catalog, &artifacts);

    st2::cutover_driver::LoadedCutoverRequest::load(&fixture.request, &fixture.request_sha256)
        .unwrap()
        .preflight()
        .unwrap();
    assert_ne!(
        fixture.source_catalog_sha256, fixture.final_catalog_sha256,
        "the replay proof must contain a real catalog declaration transition"
    );
}

fn live_restart_proof(boundary: CrashBoundary, kill_after_readiness: bool) {
    require_user_systemd();
    let _serial = ProcessLock::acquire();
    let before = LiveSentinels::capture();
    let root = tempfile::Builder::new()
        .prefix(".st2-cutover-e2e-")
        .tempdir_in("/tmp")
        .unwrap();
    let catalog = root.path().join("catalog");
    let pty_root = root.path().join("pty");
    let config = xdg_config_home();
    let state = xdg_state_home();
    let artifacts = root.path().join("artifacts");
    for directory in [&catalog, &pty_root, &artifacts] {
        fs::create_dir(directory).unwrap();
    }
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let sentinel = root.path().join("crash-sentinel");
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&sentinel)
        .unwrap()
        .sync_all()
        .unwrap();

    let fixture = write_replay_fixture(&catalog, &artifacts);
    st2::cutover_driver::LoadedCutoverRequest::load(&fixture.request, &fixture.request_sha256)
        .unwrap()
        .preflight()
        .unwrap();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_st2"))
        .canonicalize()
        .unwrap();
    let ordinary_unit = format!(
        "st2-cutover-e2e-ordinary-{}.service",
        &fixture.request_sha256[..16]
    );
    let ordinary_unit_path = artifacts.join(&ordinary_unit);
    fs::write(
        &ordinary_unit_path,
        format!(
            "[Unit]\nDescription=harmless ordinary supervisor cutover E2E sentinel\n\n\
             [Service]\nType=simple\nExecStart={} infinity\nSuccessExitStatus=SIGTERM\n",
            systemd_quote(&which("sleep").display().to_string())
        ),
    )
    .unwrap();
    let spec = CutoverCandidateServiceSpec::new(
        exe.clone(),
        catalog.canonicalize().unwrap(),
        fixture.request.canonicalize().unwrap(),
        fixture.request_sha256.clone(),
        HOST.to_owned(),
        GATE.to_owned(),
    )
    .unwrap()
    .with_test_ordinary_unit(ordinary_unit.clone())
    .unwrap();
    let unit_dir = config.join("systemd/user");
    fs::create_dir_all(&unit_dir).unwrap();
    let unit_path = unit_dir.join(&spec.unit_name);
    assert!(
        !unit_path.exists(),
        "live E2E candidate unit path must be absent before the exact install"
    );
    let host_state_path = state.join("st2").join(HOST);
    assert!(
        !host_state_path.exists(),
        "live E2E exact host runner state must be absent before the run"
    );
    let unit_bytes = st2::service::render_cutover_candidate_unit(&spec);
    let runtime_unit_dir =
        PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").unwrap()).join("systemd/user");
    let mut cleanup = UnitCleanup {
        unit: spec.unit_name.clone(),
        ordinary_unit: ordinary_unit.clone(),
        exe: exe.clone(),
        catalog: catalog.clone(),
        config: config.clone(),
        state: state.clone(),
        pty_root: pty_root.clone(),
        ding_exe: fixture.ding_exe.clone(),
        ding_argv: fixture.ding_argv.clone(),
        unit_path: unit_path.clone(),
        host_state_path,
        runtime_ordinary_unit_path: runtime_unit_dir.join(&ordinary_unit),
        before,
        armed: true,
    };
    systemctl(&["link", "--runtime", ordinary_unit_path.to_str().unwrap()]);
    systemctl(&["daemon-reload"]);
    systemctl(&["start", &ordinary_unit]);
    assert_eq!(
        systemctl_value(&ordinary_unit, "ActiveState"),
        "active",
        "harmless ordinary test supervisor must be live before cutover"
    );
    let mut manager_environment = ManagerEnvironment::set(&[
        (TEST_BOUNDARY_ENV, boundary.as_str()),
        (TEST_SENTINEL_ENV, sentinel.to_str().unwrap()),
    ]);

    let install = Command::new(&exe)
        .args([
            "cutover",
            "install",
            "--request",
            fixture.request.to_str().unwrap(),
            "--expect-request-sha256",
            &fixture.request_sha256,
        ])
        .env(st2::service::CUTOVER_TEST_ORDINARY_UNIT_ENV, &ordinary_unit)
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "real cutover install failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&install.stdout),
        String::from_utf8_lossy(&install.stderr)
    );

    let phase_path = root.path().join("crash-sentinel.phase");
    let phase = wait_for_file(&phase_path, Duration::from_secs(20));
    let old_pid: u32 = phase
        .split_ascii_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        phase.split_ascii_whitespace().nth(1),
        Some(boundary.as_str()),
        "candidate stopped at the wrong crash boundary"
    );
    let active = catalog.join(".st2/cutover/active.json");
    let history = catalog.join(format!(".st2/cutover/history/{HOST}/{GATE}.json"));
    let active_at_crash: CutoverMarker =
        serde_json::from_slice(&fs::read(&active).unwrap()).unwrap();
    match boundary {
        CrashBoundary::BeforeRun => {
            assert!(!active_at_crash.finalized);
            assert!(!history.exists());
        }
        CrashBoundary::AfterFinalize => {
            assert!(active_at_crash.finalized);
            let history_at_crash: CutoverMarker =
                serde_json::from_slice(&fs::read(&history).unwrap()).unwrap();
            assert!(history_at_crash.finalized);
        }
        CrashBoundary::AfterFinalizedHistoryBeforeActive => {
            assert!(!active_at_crash.finalized);
            let history_at_crash: CutoverMarker =
                serde_json::from_slice(&fs::read(&history).unwrap()).unwrap();
            assert!(history_at_crash.finalized);
        }
    }
    assert_eq!(
        systemctl_value(&spec.unit_name, "MainPID"),
        old_pid.to_string(),
        "controller must kill the exact stopped boundary process"
    );
    fs::remove_file(&sentinel).unwrap();
    systemctl(&[
        "kill",
        "--kill-who=main",
        "--signal=SIGKILL",
        &spec.unit_name,
    ]);

    wait_until(Duration::from_secs(25), || {
        !active.exists()
            && history.is_file()
            && systemctl_value(&spec.unit_name, "ActiveState") == "active"
    });
    let mut ready_pid: u32 = systemctl_value(&spec.unit_name, "MainPID").parse().unwrap();
    assert_ne!(ready_pid, old_pid, "systemd must start a fresh process");
    assert_eq!(systemctl_value(&spec.unit_name, "Restart"), "always");
    assert_eq!(
        systemctl_value(&spec.unit_name, "RestartUSec"),
        format!("{}s", st2::service::CUTOVER_RESTART_SEC)
    );
    assert!(
        systemctl_value(&spec.unit_name, "NRestarts")
            .parse::<u64>()
            .unwrap()
            >= 1,
        "systemd must report at least one automatic restart"
    );
    assert_eq!(
        fs::read(&unit_path).unwrap(),
        unit_bytes.as_bytes(),
        "the exact production candidate unit artifact changed"
    );
    let exec_start = systemctl_value(&spec.unit_name, "ExecStart");
    assert!(exec_start.contains("cutover run"));
    assert!(exec_start.contains(fixture.request.to_str().unwrap()));
    assert!(exec_start.contains(&fixture.request_sha256));
    let expected_argv = vec![
        exe.display().to_string(),
        "--catalog".to_owned(),
        catalog.canonicalize().unwrap().display().to_string(),
        "cutover".to_owned(),
        "run".to_owned(),
        "--request".to_owned(),
        fixture
            .request
            .canonicalize()
            .unwrap()
            .display()
            .to_string(),
        "--expect-request-sha256".to_owned(),
        fixture.request_sha256.clone(),
    ];
    assert_eq!(
        unit_supervisors(&spec.unit_name, &exe, &expected_argv),
        vec![ready_pid],
        "the candidate cgroup must contain exactly one exact st2 cutover supervisor; transient observer children are permitted"
    );
    let lock_error = match HostOwnership::acquire(&catalog, HOST) {
        Ok(_) => panic!("the restarted supervisor must retain exact host ownership"),
        Err(error) => error,
    };
    assert_eq!(lock_error.kind(), std::io::ErrorKind::WouldBlock);

    if kill_after_readiness {
        let history_before = fs::read(&history).unwrap();
        let catalog_digest_before = declaration_root_sha256_locked(&catalog).unwrap();
        let declaration_before = fs::read(catalog.join("catalog.kdl")).unwrap();
        let restarts_before: u64 = systemctl_value(&spec.unit_name, "NRestarts")
            .parse()
            .unwrap();
        let first_ready_pid = ready_pid;
        systemctl(&[
            "kill",
            "--kill-who=main",
            "--signal=SIGKILL",
            &spec.unit_name,
        ]);
        wait_until(Duration::from_secs(25), || {
            let Ok(observed_pid) = systemctl_value(&spec.unit_name, "MainPID").parse::<u32>()
            else {
                return false;
            };
            observed_pid != 0
                && observed_pid != first_ready_pid
                && systemctl_value(&spec.unit_name, "ActiveState") == "active"
                && HostOwnership::acquire(&catalog, HOST)
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        });
        ready_pid = systemctl_value(&spec.unit_name, "MainPID").parse().unwrap();
        assert_ne!(ready_pid, first_ready_pid);
        assert!(
            systemctl_value(&spec.unit_name, "NRestarts")
                .parse::<u64>()
                .unwrap()
                > restarts_before,
            "history-only successor death must increment the automatic restart count"
        );
        assert_eq!(
            fs::read(&history).unwrap(),
            history_before,
            "history-only replay must preserve the exact finalized receipt"
        );
        assert_eq!(
            declaration_root_sha256_locked(&catalog).unwrap(),
            catalog_digest_before,
            "history-only replay must not replay the completed catalog transition"
        );
        assert_eq!(
            fs::read(catalog.join("catalog.kdl")).unwrap(),
            declaration_before
        );
        assert!(!active.exists());
        assert!(!catalog.join(".st2/catalog-apply-incomplete").exists());
        assert_eq!(
            unit_supervisors(&spec.unit_name, &exe, &expected_argv),
            vec![ready_pid],
            "history-only reacquisition must leave exactly one exact successor supervisor"
        );
    }

    assert_ne!(
        systemctl_value(&ordinary_unit, "ActiveState"),
        "active",
        "readiness must retire the harmless test-owned ordinary supervisor"
    );
    assert_eq!(systemctl_value(&ordinary_unit, "MainPID"), "0");

    cleanup.clean_checked();
    manager_environment.restore_checked();
    assert_eq!(
        LiveSentinels::capture(),
        cleanup.before,
        "live E2E must restore unit inventory and the ordinary supervisor exactly"
    );
}

struct ReplayFixture {
    request: PathBuf,
    request_sha256: String,
    source_catalog_sha256: String,
    final_catalog_sha256: String,
    ding_exe: PathBuf,
    ding_argv: Vec<String>,
}

fn write_replay_fixture(catalog: &Path, artifacts: &Path) -> ReplayFixture {
    let canonical_catalog = catalog.canonicalize().unwrap();
    let source = declaration_root_sha256_locked(&canonical_catalog).unwrap();
    let retirement = predecessor_retirement_receipt(&canonical_catalog, &source);
    let mut retirement_bytes = serde_json::to_vec(&retirement).unwrap();
    retirement_bytes.push(b'\n');
    let mut retirement_roundtrip = serde_json::to_vec(
        &serde_json::from_slice::<ExecRetirementReceipt>(&retirement_bytes).unwrap(),
    )
    .unwrap();
    retirement_roundtrip.push(b'\n');
    assert_eq!(
        retirement_roundtrip, retirement_bytes,
        "fixture retirement receipt must be canonical"
    );
    let retirement_sha256 = sha256(&retirement_bytes);
    let retirement_path = artifacts.join("predecessor-retirement.json");
    fs::write(&retirement_path, retirement_bytes).unwrap();
    let workspace = canonical_catalog.parent().unwrap().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let host = HostId::parse(HOST).unwrap();
    let gate_id = GateId::parse(GATE).unwrap();
    let provider = provider(&workspace, host.clone());
    let provider_argv = provider
        .canonical_argv
        .iter()
        .map(|argument| format!("{argument:?}"))
        .collect::<Vec<_>>()
        .join(" ");
    fs::write(
        canonical_catalog.join("catalog.kdl"),
        format!(
            "catalog {{ pty-root {:?} }}\n",
            canonical_catalog.parent().unwrap().join("pty")
        ),
    )
    .unwrap();
    let declaration = canonical_catalog.join(format!("agents/{HOST}/worker/agent.kdl"));
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        declaration,
        format!(
            r#"agent "worker" {{
  identity "worker"
  host "{HOST}"
  workspace {workspace:?}
  pty "agent" {{
    lifecycle "adopt-only"
    argv {provider_argv}
    env {{
      AGENT_PERSONA "worker"
      AGENT_RUNTIME_PROFILE "/nix/store/e2e-profile.json"
    }}
  }}
  exec "ding" {{
    id "{HOST}.worker.ding"
    argv "st2" "ding" "--identity" "{HOST}.worker" "--root" "$ST_ROOT"
  }}
}}
"#,
        ),
    )
    .unwrap();
    let prepared = artifacts.join("prepared-catalog");
    let after = snapshot(SnapshotRequest {
        catalog: canonical_catalog.clone(),
        output: prepared.clone(),
    })
    .unwrap()
    .root_sha256;

    let providers = vec![provider];
    let provider_action = ProviderFleetProofAction {
        providers_sha256: provider_entries_sha256(&providers).unwrap(),
        providers,
    };
    let ding_argv = vec![
        "st2".to_owned(),
        "ding".to_owned(),
        "--identity".to_owned(),
        format!("{HOST}.worker"),
        "--root".to_owned(),
        canonical_catalog.display().to_string(),
    ];
    let mut desired = DingDesiredExec {
        runtime_id: format!("{HOST}.worker.ding"),
        canonical_argv: ding_argv.clone(),
        canonical_cwd: workspace,
        canonical_env: BTreeMap::new(),
        launch_sha256: String::new(),
    };
    desired.launch_sha256 = launch_sha256(&desired).unwrap();
    let ding_action = DingReconcileAction {
        generation_id: "ding-generation-e2e".to_owned(),
        desired_sha256: desired_set_sha256(std::slice::from_ref(&desired)).unwrap(),
        desired: vec![desired],
    };
    let program = vec![
        CutoverAction::CatalogTransition(CatalogTransition {
            before_sha256: source.clone(),
            after_sha256: after.clone(),
        }),
        CutoverAction::ProviderFleetProof(provider_action.clone()),
        CutoverAction::DingReconcile(ding_action.clone()),
        CutoverAction::ExternalCheckpoint {
            kind: ExternalCheckpointKind::BusContinuity,
            input_sha256: C.to_owned(),
        },
        CutoverAction::ExternalCheckpoint {
            kind: ExternalCheckpointKind::FinalProof,
            input_sha256: B.to_owned(),
        },
    ];
    let request = CutoverRequest {
        schema: CUTOVER_REQUEST_SCHEMA.to_owned(),
        canonical_catalog: canonical_catalog.clone(),
        host: host.clone(),
        gate_id: gate_id.clone(),
        source_catalog_sha256: source.clone(),
        program: program.clone(),
        predecessor_retirement: CutoverPredecessorRetirementInput {
            receipt: retirement_path.clone(),
            expect_sha256: retirement_sha256.clone(),
        },
        catalog_inputs: vec![CutoverCatalogInput {
            action_index: 0,
            prepared,
            expect_sha256: source.clone(),
        }],
        checkpoint_inputs: vec![
            CutoverCheckpointInput {
                action_index: 3,
                receipt: artifacts.join("bus.json"),
            },
            CutoverCheckpointInput {
                action_index: 4,
                receipt: artifacts.join("final.json"),
            },
        ],
    };
    let request_bytes = canonical_request_bytes(&request).unwrap();
    let request_sha256 = sha256(&request_bytes);
    let request_path = artifacts.join("request.json");
    fs::write(&request_path, request_bytes).unwrap();

    let metadata = fs::metadata(&canonical_catalog).unwrap();
    let provider_proof = ProviderFleetProofEvidence {
        schema: PROVIDER_FLEET_PROOF_EVIDENCE_SCHEMA.to_owned(),
        providers_sha256: provider_action.providers_sha256.clone(),
        launch_receipts_sha256: provider_launch_receipts_sha256(&provider_action.providers)
            .unwrap(),
        ding_partition_sha256: C.to_owned(),
        result_sha256: D.to_owned(),
    };
    let runtime_ids = vec![ding_action.desired[0].runtime_id.clone()];
    let exec_generation_ids = vec!["exec-generation-e2e".to_owned()];
    let ding_receipt = DingReconcileReceipt {
        schema: DING_RECONCILE_RECEIPT_SCHEMA.to_owned(),
        gate_id: GATE.to_owned(),
        action_index: 2,
        generation_id: ding_action.generation_id,
        desired_sha256: ding_action.desired_sha256,
        runtime_ids: runtime_ids.clone(),
        exec_generation_ids: exec_generation_ids.clone(),
        observed_sha256: ding_observation_sha256(&runtime_ids, &exec_generation_ids),
    };
    let mut marker = CutoverMarker {
        schema: CUTOVER_TRANSACTION_SCHEMA.to_owned(),
        canonical_catalog: canonical_catalog.clone(),
        catalog_device: metadata.dev(),
        catalog_inode: metadata.ino(),
        host,
        gate_id,
        request_sha256: request_sha256.clone(),
        source_catalog_sha256: source.clone(),
        program,
        cursor: 5,
        predecessor_retirement: PredecessorRetirementEvidence {
            schema: PREDECESSOR_RETIREMENT_EVIDENCE_SCHEMA.to_owned(),
            receipt_sha256: retirement_sha256,
            plan_sha256: retirement.plan_sha256,
            catalog_sha256: source.clone(),
            host: HostId::parse(HOST).unwrap(),
            census_sha256: retirement.census_sha256,
            journal_sha256: retirement.journal_sha256,
            legacy_partition_sha256: retirement.legacy_partition_sha256,
            legacy_partition: Vec::new(),
        },
        completed_checkpoints: Vec::new(),
        completed_ding_reconciles: vec![CompletedDingReconcile {
            action_index: 2,
            receipt: ding_receipt.clone(),
        }],
        provider_fleet_proof: Some(provider_proof.clone()),
        finalized: false,
    };
    marker.completed_checkpoints.push(checkpoint(
        &marker,
        3,
        ExternalCheckpointKind::BusContinuity,
        ExternalCheckpointPayload::BusContinuity {
            bus_id: "e2e-bus".to_owned(),
            probe_sha256: D.to_owned(),
        },
    ));
    marker.completed_checkpoints.push(checkpoint(
        &marker,
        4,
        ExternalCheckpointKind::FinalProof,
        ExternalCheckpointPayload::FinalProof {
            final_catalog_sha256: after.clone(),
            providers_sha256: provider_proof.providers_sha256,
            launch_receipts_sha256: provider_proof.launch_receipts_sha256,
            ding_partition_sha256: provider_proof.ding_partition_sha256,
            ding_reconcile_sha256: sha256(&canonical_json(&ding_receipt)),
            validation_sha256: C.to_owned(),
            runtime_inventory_sha256: provider_proof.result_sha256,
        },
    ));
    let cutover = canonical_catalog.join(".st2/cutover");
    fs::create_dir_all(cutover.join("history").join(HOST)).unwrap();
    fs::write(cutover.join("active.json"), canonical_json(&marker)).unwrap();

    ReplayFixture {
        request: request_path,
        request_sha256,
        source_catalog_sha256: source,
        final_catalog_sha256: after,
        ding_exe: which("st2"),
        ding_argv,
    }
}

fn predecessor_retirement_receipt(catalog: &Path, source: &str) -> ExecRetirementReceipt {
    let legacy_partition = Some(Vec::<LegacySuccessorTask>::new());
    let mut partition_hash = Sha256::new();
    partition_hash.update(b"st2.exec-retirement-legacy-partition.v1\0");
    let mut partition_bytes = serde_json::to_vec(&legacy_partition).unwrap();
    partition_bytes.push(b'\n');
    partition_hash.update(partition_bytes);
    ExecRetirementReceipt {
        schema: "st2.exec-retirement.v1".to_owned(),
        request_sha256: B.to_owned(),
        plan_sha256: C.to_owned(),
        catalog: catalog.to_path_buf(),
        host: HOST.to_owned(),
        catalog_sha256: source.to_owned(),
        state_dir_device: 1,
        state_dir_inode: 1,
        journal_schema: "st2.exec-retirement-journal.v1".to_owned(),
        journal_sha256: D.to_owned(),
        journal_status: "completed".to_owned(),
        status: ExecRetirementStatus::Completed,
        completed_at_unix_ms: 1,
        census_sha256: B.to_owned(),
        forward_only_started: true,
        legacy_partition_sha256: format!("{:x}", partition_hash.finalize()),
        legacy_partition,
        targets: Vec::new(),
    }
}

fn provider(workspace: &Path, host: HostId) -> ProviderFleetEntry {
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
    let mut provider = ProviderFleetEntry {
        identity: "worker".to_owned(),
        host,
        provider: "codex".to_owned(),
        account: "account-e2e".to_owned(),
        persona: "worker".to_owned(),
        workspace: workspace.to_path_buf(),
        prompt: LaunchPromptAuthority {
            runtime_profile_path: PathBuf::from("/nix/store/e2e-profile.json"),
            runtime_profile_sha256: B.to_owned(),
            persona_prompt_path: PathBuf::from("/nix/store/e2e-worker.md"),
            persona_prompt_sha256: C.to_owned(),
            launch_receipt_path: PathBuf::from("/run/user/e2e-launch-receipt.json"),
            launch_receipt_sha256: D.to_owned(),
            injection_kind: PromptInjectionKind::CodexDeveloperInstructions,
        },
        canonical_argv: argv.clone(),
        argv_sha256: candidate_argv_sha256(&argv),
        profile_sha256: B.to_owned(),
        harness: "codex".to_owned(),
        model: "gpt-5".to_owned(),
        effort: "high".to_owned(),
        mode: "managed-unattended".to_owned(),
        boot_contract: "managed-v1".to_owned(),
        launch_generation_id: "axe-generation-e2e".to_owned(),
        runtime_generation_id: "pty-generation-e2e".to_owned(),
        trajectory_sha256: String::new(),
    };
    provider.trajectory_sha256 = provider_trajectory_sha256(&provider).unwrap();
    provider
}

fn checkpoint(
    marker: &CutoverMarker,
    action_index: usize,
    kind: ExternalCheckpointKind,
    payload: ExternalCheckpointPayload,
) -> CompletedCheckpoint {
    let CutoverAction::ExternalCheckpoint { input_sha256, .. } = &marker.program[action_index]
    else {
        panic!("checkpoint index must address a checkpoint action");
    };
    let receipt = ExternalCheckpointReceipt {
        schema: EXTERNAL_CHECKPOINT_EVIDENCE_SCHEMA.to_owned(),
        canonical_catalog: marker.canonical_catalog.clone(),
        catalog_device: marker.catalog_device,
        catalog_inode: marker.catalog_inode,
        host: marker.host.clone(),
        gate_id: marker.gate_id.clone(),
        request_sha256: marker.request_sha256.clone(),
        action_index,
        kind,
        input_sha256: input_sha256.clone(),
        payload,
    };
    CompletedCheckpoint {
        action_index,
        evidence: ExternalCheckpointEvidence {
            receipt_sha256: sha256(&canonical_json(&receipt)),
            receipt,
        },
    }
}

fn canonical_json<T: serde::Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn ding_observation_sha256(runtime_ids: &[String], generation_ids: &[String]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"st2.cutover-ding-observation.v1\0");
    for values in [runtime_ids, generation_ids] {
        hash.update((values.len() as u64).to_be_bytes());
        for value in values {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
    }
    format!("{:x}", hash.finalize())
}

fn wait_for_file(path: &Path, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(value) = fs::read_to_string(path)
            && !value.trim().is_empty()
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        thread::sleep(Duration::from_millis(50));
    }
}

fn require_user_systemd() {
    let output = Command::new("systemctl")
        .args(["--user", "show-environment"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "live E2E requires a reachable user systemd manager: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn systemctl(args: &[&str]) -> Output {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "systemctl --user {} failed\nstdout: {}\nstderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn systemctl_value(unit: &str, property: &str) -> String {
    String::from_utf8(
        systemctl(&["show", unit, &format!("--property={property}"), "--value"]).stdout,
    )
    .unwrap()
    .trim()
    .to_owned()
}

fn unit_processes(unit: &str) -> Vec<u32> {
    let control_group = systemctl_value(unit, "ControlGroup");
    let mut pids = fs::read_to_string(
        Path::new("/sys/fs/cgroup")
            .join(control_group.trim_start_matches('/'))
            .join("cgroup.procs"),
    )
    .unwrap()
    .lines()
    .map(|line| line.parse::<u32>().unwrap())
    .collect::<Vec<_>>();
    pids.sort_unstable();
    pids
}

fn unit_supervisors(unit: &str, exe: &Path, expected_argv: &[String]) -> Vec<u32> {
    unit_processes(unit)
        .into_iter()
        .filter(|pid| {
            let process = PathBuf::from(format!("/proc/{pid}"));
            let Ok(observed_exe) = fs::read_link(process.join("exe")) else {
                return false;
            };
            if observed_exe != exe {
                return false;
            }
            let Ok(command) = fs::read(process.join("cmdline")) else {
                return false;
            };
            let argv = command
                .split(|byte| *byte == 0)
                .filter(|argument| !argument.is_empty())
                .map(|argument| String::from_utf8_lossy(argument).into_owned())
                .collect::<Vec<_>>();
            argv == expected_argv
        })
        .collect()
}

fn exact_processes(exe: &Path, expected_argv: &[String]) -> Vec<u32> {
    let mut matches = Vec::new();
    for entry in fs::read_dir("/proc").unwrap() {
        let Ok(entry) = entry else {
            continue;
        };
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let process = entry.path();
        let Ok(metadata) = fs::metadata(&process) else {
            continue;
        };
        if metadata.uid() != unsafe { libc::geteuid() } {
            continue;
        }
        let Ok(observed_exe) = fs::read_link(process.join("exe")) else {
            continue;
        };
        if observed_exe != exe {
            continue;
        }
        let Ok(command) = fs::read(process.join("cmdline")) else {
            continue;
        };
        let argv = command
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect::<Vec<_>>();
        if argv.len() == expected_argv.len() && argv[1..] == expected_argv[1..] {
            matches.push(pid);
        }
    }
    matches.sort_unstable();
    matches
}

fn stop_exact_ding_scopes(exe: &Path, expected_argv: &[String], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut quiet_since = None;
    loop {
        let processes = exact_processes(exe, expected_argv);
        if processes.is_empty() {
            let quiet = quiet_since.get_or_insert_with(Instant::now);
            if quiet.elapsed() >= Duration::from_millis(500) {
                return true;
            }
        } else {
            quiet_since = None;
            for pid in processes {
                let Ok(cgroup) = fs::read_to_string(format!("/proc/{pid}/cgroup")) else {
                    continue;
                };
                let Some(scope) = cgroup
                    .lines()
                    .find_map(|line| line.strip_prefix("0::/"))
                    .and_then(|path| path.rsplit('/').next())
                    .filter(|unit| {
                        unit.starts_with("st2-e2e-host.worker.ding-")
                            && unit.ends_with(".scope")
                            && unit.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric()
                                    || matches!(byte, b'.' | b'_' | b'-' | b'@')
                            })
                    })
                else {
                    continue;
                };
                let _ = Command::new("systemctl")
                    .args(["--user", "stop", scope])
                    .output();
            }
        }
        if Instant::now() >= deadline {
            return exact_processes(exe, expected_argv).is_empty();
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LiveSentinels {
    e2e_units: Vec<u8>,
    ordinary_main_pid: String,
    ordinary_control_group: String,
    ordinary_fragment_path: String,
    ordinary_fragment_bytes: Vec<u8>,
}

impl LiveSentinels {
    fn capture() -> Self {
        let mut e2e_units = Vec::new();
        for args in [
            &[
                "list-unit-files",
                "st2-cutover-*",
                "--no-legend",
                "--no-pager",
            ][..],
            &[
                "list-units",
                "st2-cutover-*",
                "--all",
                "--no-legend",
                "--no-pager",
            ][..],
        ] {
            let output = Command::new("systemctl")
                .arg("--user")
                .args(args)
                .output()
                .unwrap();
            e2e_units.extend(output.status.code().unwrap_or(-1).to_be_bytes());
            e2e_units.extend(output.stdout);
            e2e_units.push(0);
            e2e_units.extend(output.stderr);
            e2e_units.push(0);
        }
        let ordinary_fragment_path = systemctl_value("st2.service", "FragmentPath");
        let ordinary_fragment_bytes = if ordinary_fragment_path.is_empty() {
            Vec::new()
        } else {
            fs::read(&ordinary_fragment_path).unwrap()
        };
        Self {
            e2e_units,
            ordinary_main_pid: systemctl_value("st2.service", "MainPID"),
            ordinary_control_group: systemctl_value("st2.service", "ControlGroup"),
            ordinary_fragment_path,
            ordinary_fragment_bytes,
        }
    }
}

fn which(name: &str) -> PathBuf {
    let output = Command::new("sh")
        .args(["-c", "command -v \"$1\"", "sh", name])
        .output()
        .unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .canonicalize()
        .unwrap()
}

fn xdg_config_home() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap()).join(".config"))
}

fn xdg_state_home() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap()).join(".local/state"))
}

fn systemd_quote(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('$', "$$")
            .replace('%', "%%")
    )
}

struct ManagerEnvironment {
    previous: Vec<(String, Option<String>)>,
    armed: bool,
}

impl ManagerEnvironment {
    fn set(values: &[(&str, &str)]) -> Self {
        let environment = String::from_utf8(systemctl(&["show-environment"]).stdout).unwrap();
        let previous = values
            .iter()
            .map(|(key, _)| {
                let prefix = format!("{key}=");
                (
                    (*key).to_owned(),
                    environment
                        .lines()
                        .find_map(|line| line.strip_prefix(&prefix).map(str::to_owned)),
                )
            })
            .collect::<Vec<_>>();
        for (key, value) in values {
            systemctl(&["set-environment", &format!("{key}={value}")]);
        }
        Self {
            previous,
            armed: true,
        }
    }

    fn restore_checked(&mut self) {
        if !self.armed {
            return;
        }
        for (key, value) in &self.previous {
            match value {
                Some(value) => {
                    systemctl(&["set-environment", &format!("{key}={value}")]);
                }
                None => {
                    systemctl(&["unset-environment", key]);
                }
            }
        }
        self.armed = false;
    }

    fn restore_best_effort(&mut self) {
        if !self.armed {
            return;
        }
        for (key, value) in &self.previous {
            let mut command = Command::new("systemctl");
            command.arg("--user");
            match value {
                Some(value) => {
                    command.args(["set-environment", &format!("{key}={value}")]);
                }
                None => {
                    command.args(["unset-environment", key]);
                }
            }
            let _ = command.output();
        }
        self.armed = false;
    }
}

impl Drop for ManagerEnvironment {
    fn drop(&mut self) {
        self.restore_best_effort();
    }
}

struct UnitCleanup {
    unit: String,
    ordinary_unit: String,
    exe: PathBuf,
    catalog: PathBuf,
    config: PathBuf,
    state: PathBuf,
    pty_root: PathBuf,
    ding_exe: PathBuf,
    ding_argv: Vec<String>,
    unit_path: PathBuf,
    host_state_path: PathBuf,
    runtime_ordinary_unit_path: PathBuf,
    before: LiveSentinels,
    armed: bool,
}

impl UnitCleanup {
    fn clean_checked(&mut self) {
        let (down, ding_quiescent) = self.clean_inner();
        assert!(
            down.status.success(),
            "exact temporary catalog teardown failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&down.stdout),
            String::from_utf8_lossy(&down.stderr)
        );
        assert!(
            ding_quiescent,
            "exact temporary Ding did not quiesce after the candidate stopped and temporary catalog teardown completed"
        );
    }

    fn clean_inner(&mut self) -> (Output, bool) {
        if !self.armed {
            return (Command::new("true").output().unwrap(), true);
        }
        let _ = Command::new("systemctl")
            .args(["--user", "disable", "--now", &self.unit])
            .output();
        let _ = Command::new("systemctl")
            .args(["--user", "reset-failed", &self.unit])
            .output();
        let _ = Command::new("systemctl")
            .args(["--user", "unlink", &self.unit])
            .output();
        let down = Command::new(&self.exe)
            .args([
                "--catalog",
                self.catalog.to_str().unwrap(),
                "down",
                "--host",
                HOST,
            ])
            .env("XDG_CONFIG_HOME", &self.config)
            .env("XDG_STATE_HOME", &self.state)
            .env("PTY_ROOT", &self.pty_root)
            .output()
            .unwrap();
        let ding_quiescent =
            stop_exact_ding_scopes(&self.ding_exe, &self.ding_argv, Duration::from_secs(5));
        let _ = Command::new("systemctl")
            .args(["--user", "disable", "--now", &self.ordinary_unit])
            .output();
        let _ = Command::new("systemctl")
            .args(["--user", "reset-failed", &self.ordinary_unit])
            .output();
        let _ = Command::new("systemctl")
            .args(["--user", "unlink", &self.ordinary_unit])
            .output();
        let _ = fs::remove_file(&self.unit_path);
        let _ = fs::remove_file(&self.runtime_ordinary_unit_path);
        let _ = fs::remove_dir_all(&self.host_state_path);
        let _ = Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .output();
        self.armed = false;
        (down, ding_quiescent)
    }
}

impl Drop for UnitCleanup {
    fn drop(&mut self) {
        let _ = self.clean_inner();
    }
}

struct ProcessLock(File);

impl ProcessLock {
    fn acquire() -> Self {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open("/tmp/st2-cutover-service-e2e.lock")
            .unwrap();
        // SAFETY: the descriptor is retained by the returned guard.
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        Self(file)
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // SAFETY: this only releases the advisory lock retained by this guard.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
