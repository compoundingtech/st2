//! CLI-level proofs for the sole durable cutover mutation driver.

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};
use st2::cutover_admission::{
    CutoverAction, ExternalCheckpointKind, GateId, HostId, LaunchPromptAuthority,
    ProviderFleetEntry, ProviderFleetProofAction,
};
use st2::cutover_driver::{
    CUTOVER_REQUEST_SCHEMA, CutoverCheckpointInput, CutoverRequest, CutoverRetirement,
    CutoverRetirementSelector, canonical_request_bytes,
};
use st2::ding_reconcile::{DingDesiredExec, DingReconcileAction};

fn st2() -> std::process::Command {
    std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
}

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
    let mut provider = ProviderFleetEntry {
        identity: "worker-a".to_owned(),
        host: HostId::parse("testhost").unwrap(),
        provider: "codex".to_owned(),
        account: "account-a".to_owned(),
        persona: "worker".to_owned(),
        workspace: root.join("workspace"),
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
        canonical_argv: vec!["/nix/store/st2/bin/st2".to_owned(), "ding".to_owned()],
        canonical_cwd: root.join("workspace"),
        canonical_env: Default::default(),
        launch_sha256: String::new(),
    };
    ding.launch_sha256 = st2::ding_reconcile::launch_sha256(&ding).unwrap();
    let desired = vec![ding];
    let source_catalog_sha256 =
        st2::catalog_transaction::declaration_root_sha256_locked(catalog).unwrap();
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
                kind: ExternalCheckpointKind::FinalProof,
                input_sha256: digest(8),
            },
            CutoverAction::ExternalCheckpoint {
                kind: ExternalCheckpointKind::BusContinuity,
                input_sha256: digest(9),
            },
        ],
        retirement: CutoverRetirement {
            selector: CutoverRetirementSelector::LegacySet,
            plan_output: root.join("retirement-plan.json"),
        },
        catalog_inputs: Vec::new(),
        checkpoint_inputs: vec![
            CutoverCheckpointInput {
                action_index: 0,
                receipt: root.join("cleanup.json"),
            },
            CutoverCheckpointInput {
                action_index: 3,
                receipt: root.join("final.json"),
            },
            CutoverCheckpointInput {
                action_index: 4,
                receipt: root.join("bus.json"),
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
        "agent \"legacy\" {\n  host \"testhost\"\n  retired #true\n  command \"true\"\n  ding\n}\n",
    )
    .unwrap();
    let request = checkpoint_request(&catalog, root.path());
    let bytes = canonical_request_bytes(&request).unwrap();
    let request_sha256 = sha256(&bytes);
    let request_path = root.path().join("request.json");
    fs::write(&request_path, bytes).unwrap();

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
    let plan_before = fs::read(root.path().join("retirement-plan.json")).unwrap();

    let second = invoke();
    assert!(!second.status.success());
    assert_eq!(second.stdout, first.stdout);
    assert_eq!(fs::read(marker).unwrap(), marker_before);
    assert_eq!(
        fs::read(root.path().join("retirement-plan.json")).unwrap(),
        plan_before
    );
}
