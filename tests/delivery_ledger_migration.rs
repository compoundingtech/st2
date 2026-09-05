//! The delivery ledger's migration and rollback contract, asserted from outside the drivers.
//!
//! Two failure classes are load-bearing here, and both were measured before this code existed:
//!
//! * **An in-place schema bump has no rollback at all.** v1's Codex loader `ensure!`s its schema
//!   and denies unknown fields, and that error propagates through `CodexInboxDelivery::new` into
//!   the control connection's startup — a v2 body at `delivery-state.json` makes the old binary
//!   refuse to start, deterministically, at every crash position. v1's OpenCode loader instead
//!   discards a body it cannot read and then POSTs before requerying, appending the same
//!   `messageID`'s parts a second time. So the ledger must live in a fresh namespace and the v1
//!   path must only ever hold a v1 body.
//! * **A delivery started by the new binary has no v1 record.** Rolled back, the old OpenCode pump
//!   takes its `state == None` path and re-POSTs (9 of 9 crash positions duplicated). The
//!   v1-*shaped* `Attempted` floor removes that class — but only if it passes v1's own load
//!   filter, which recomputes the derived correlation and silently discards a record that differs.
//!
//! These tests therefore mirror both v1 record types verbatim, including `deny_unknown_fields`,
//! and recompute both derived correlations independently of the driver modules.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use st2::delivery_ledger::{
    self, Begin, CODEX_LEGACY_SCHEMA, Correlation, Evidence, Harness, HoldReason, LEDGER_FILE,
    LEDGER_SCHEMA, LEGACY_FILE, Ledger, NegativeReceipt, OPENCODE_LEGACY_SCHEMA, Phase, Retention,
    RetryDecision,
};

const AGENT: &str = "h.worker";
const FILE_A: &str = "1786380000000-aaa111.md";
const THREAD: &str = "thread-main";
const SESSION: &str = "ses_target";
const INCARNATION: &str = "incarnation-1";

/// v1's `CodexDeliveryState`, mirrored verbatim. `deny_unknown_fields` is the point: it is why an
/// additively-extended record at this path is an outage rather than a degraded read.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CodexV1 {
    schema: String,
    agent: String,
    runtime_id: String,
    runtime_incarnation: String,
    thread_id: String,
    filename: String,
    client_id: String,
    phase: String,
}

/// v1's OpenCode `DeliveryState`, mirrored verbatim.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OpenCodeV1 {
    schema: String,
    agent: String,
    runtime_id: String,
    session_id: String,
    filename: String,
    message_id: String,
    phase: String,
}

fn digest(domain: &[u8], parts: [&str; 3]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    for value in parts {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

/// Recomputed here, independently of `codex_app_server`: v1 validates the client ID against this
/// exact derivation, so a floor that does not match is a floor v1 refuses.
fn codex_client_id(thread: &str, filename: &str) -> String {
    format!(
        "st2:{}",
        digest(b"st2.codex-client-user-message.v1", [AGENT, thread, filename])
    )
}

/// Recomputed here, independently of `opencode_session`: v1 recomputes this and silently discards
/// a record whose `messageId` differs, which would put the duplicate-POST class straight back.
fn opencode_message_id(session: &str, filename: &str) -> String {
    format!(
        "msg{:.26}",
        digest(b"st2.opencode-client-message.v1", [AGENT, session, filename])
    )
}

fn legacy_path(dir: &Path) -> PathBuf {
    dir.join("state").join(LEGACY_FILE)
}

fn ledger_path(dir: &Path) -> PathBuf {
    dir.join("state").join(LEDGER_FILE)
}

fn open(dir: &Path, harness: Harness) -> Ledger {
    match harness {
        Harness::Codex => Ledger::open(
            &legacy_path(dir),
            harness.profile(),
            AGENT,
            AGENT,
            |thread, filename| codex_client_id(thread, filename),
        ),
        Harness::OpenCode => Ledger::open(
            &legacy_path(dir),
            harness.profile(),
            AGENT,
            AGENT,
            |session, filename| opencode_message_id(session, filename),
        ),
    }
}

fn begin(ledger: &mut Ledger, harness: Harness) {
    let begin = match harness {
        Harness::Codex => {
            let client_id = codex_client_id(THREAD, FILE_A);
            Begin {
                filename: FILE_A.to_string(),
                binding: THREAD.to_string(),
                correlation: Correlation::native(client_id.clone()),
                incarnation: Some(INCARNATION.to_string()),
                legacy_floor: delivery_ledger::codex_floor(
                    AGENT,
                    AGENT,
                    INCARNATION,
                    THREAD,
                    FILE_A,
                    &client_id,
                ),
            }
        }
        Harness::OpenCode => {
            let message_id = opencode_message_id(SESSION, FILE_A);
            Begin {
                filename: FILE_A.to_string(),
                binding: SESSION.to_string(),
                correlation: Correlation::native(message_id.clone()),
                incarnation: None,
                legacy_floor: delivery_ledger::opencode_floor(
                    AGENT, AGENT, SESSION, FILE_A, &message_id,
                ),
            }
        }
    };
    ledger.begin(begin).unwrap();
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

/// v1's own load filter for each harness, applied to whatever is at the v1 path. `Ok(phase)` means
/// the old binary would load this record and act on it; `Err` means it would refuse to start
/// (Codex) or silently discard it and re-POST (OpenCode).
fn v1_would_load(path: &Path, harness: Harness) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    match harness {
        Harness::Codex => {
            let state: CodexV1 =
                serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
            if state.schema != CODEX_LEGACY_SCHEMA {
                return Err(format!("unsupported schema '{}'", state.schema));
            }
            if state.agent != AGENT || state.runtime_id != AGENT {
                return Err("belongs to a different runtime".to_string());
            }
            if state.runtime_incarnation.is_empty() || state.thread_id.is_empty() {
                return Err("invalid runtime binding".to_string());
            }
            if state.client_id != codex_client_id(&state.thread_id, &state.filename) {
                return Err("client ID does not match its binding".to_string());
            }
            Ok(state.phase)
        }
        Harness::OpenCode => {
            let state: OpenCodeV1 =
                serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
            if state.schema != OPENCODE_LEGACY_SCHEMA
                || state.agent != AGENT
                || state.message_id != opencode_message_id(&state.session_id, &state.filename)
            {
                return Err("discarded by the v1 filter".to_string());
            }
            Ok(state.phase)
        }
    }
}

#[test]
fn in_place_schema_bump_is_rejected_by_construction() {
    for harness in [Harness::Codex, Harness::OpenCode] {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), harness);
        begin(&mut ledger, harness);
        ledger.record(FILE_A, Evidence::TransportAccepted).unwrap();
        match harness {
            Harness::Codex => ledger.record(FILE_A, Evidence::Consumed).unwrap(),
            Harness::OpenCode => ledger.record(FILE_A, Evidence::Persisted).unwrap(),
        };

        // Authority is in the fresh namespace, under the ledger's own schema.
        assert_eq!(read_json(&ledger_path(tmp.path()))["schema"], LEDGER_SCHEMA);

        // And the v1 path never holds a ledger body — the shape that makes Codex refuse to start
        // and OpenCode duplicate. It holds a v1 body or nothing at all.
        if legacy_path(tmp.path()).exists() {
            let v1 = read_json(&legacy_path(tmp.path()));
            assert_eq!(v1["schema"], harness.legacy_schema());
            assert!(
                v1.get("entries").is_none() && v1.get("harness").is_none(),
                "no ledger field ever reaches the v1 path: {v1}"
            );
        }
    }
}

#[test]
fn old_binary_rollback_neither_duplicates_nor_refuses_to_start() {
    // A delivery STARTED by the new binary: the case with no pre-existing v1 record, and the one
    // that duplicated at every crash position without a floor.
    for harness in [Harness::Codex, Harness::OpenCode] {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = open(tmp.path(), harness);
        begin(&mut ledger, harness);

        // Boundary 1: crash right after the first transport was owned.
        assert_eq!(
            v1_would_load(&legacy_path(tmp.path()), harness),
            Ok("attempted".to_string()),
            "the rolled-back binary loads a true lower bound and reconciles"
        );
        let exact = read_json(&legacy_path(tmp.path()));

        // Boundary 2: the transport landed. The floor still says only `attempted`, so the old
        // binary reconciles rather than treating it as acceptance.
        ledger.record(FILE_A, Evidence::TransportAccepted).unwrap();
        assert_eq!(read_json(&legacy_path(tmp.path())), exact);

        // Boundary 3: the harness's own receipt. Codex proves consumption and releases; OpenCode
        // proves only storage, holds, and keeps the floor for a rollback.
        match harness {
            Harness::Codex => {
                ledger.record(FILE_A, Evidence::Consumed).unwrap();
                assert_eq!(ledger.retention(FILE_A), Retention::Release);
                assert!(
                    !legacy_path(tmp.path()).exists(),
                    "release clears the floor: nothing is outstanding to protect"
                );
            }
            Harness::OpenCode => {
                ledger.record(FILE_A, Evidence::Persisted).unwrap();
                assert_eq!(
                    ledger.retention(FILE_A),
                    Retention::Hold(HoldReason::UnreadReceipt)
                );
                assert_eq!(read_json(&legacy_path(tmp.path())), exact);
            }
        }

        // At no boundary does a rolled-back binary see something it must refuse to start on.
        if legacy_path(tmp.path()).exists() {
            assert!(v1_would_load(&legacy_path(tmp.path()), harness).is_ok());
        }
    }
}

#[test]
fn a_lost_ledger_write_after_the_floor_recovers_as_an_ambiguous_attempt() {
    // The exact interleaving the sweep found: the floor landed, the ledger's own record did not.
    // Adoption reads the floor back as the ambiguous attempt it is — held, surfaced, never
    // replayed — instead of starting a second delivery.
    for harness in [Harness::Codex, Harness::OpenCode] {
        let tmp = tempfile::tempdir().unwrap();
        let floor = match harness {
            Harness::Codex => delivery_ledger::codex_floor(
                AGENT,
                AGENT,
                INCARNATION,
                THREAD,
                FILE_A,
                &codex_client_id(THREAD, FILE_A),
            ),
            Harness::OpenCode => delivery_ledger::opencode_floor(
                AGENT,
                AGENT,
                SESSION,
                FILE_A,
                &opencode_message_id(SESSION, FILE_A),
            ),
        };
        fs::create_dir_all(tmp.path().join("state")).unwrap();
        fs::write(
            legacy_path(tmp.path()),
            serde_json::to_vec(&floor).unwrap(),
        )
        .unwrap();

        let mut recovered = open(tmp.path(), harness);
        assert_eq!(recovered.entry(FILE_A).unwrap().phase, Phase::Attempted);
        assert_eq!(
            recovered.retry(FILE_A),
            RetryDecision::Hold(HoldReason::AdoptedWithoutFreshEvidence)
        );

        // Only fresh evidence about the world moves it, and then it is a first-class attempt.
        recovered.negative(FILE_A, NegativeReceipt::Absent).unwrap();
        assert_eq!(recovered.retry(FILE_A), RetryDecision::Retry);
    }
}

#[test]
fn adoption_downgrades_each_v1_label_to_the_evidence_it_proved() {
    // Codex `Accepted` came from the typed completed user message: consumption, and its true
    // ceiling, so it releases.
    let codex = tempfile::tempdir().unwrap();
    fs::create_dir_all(codex.path().join("state")).unwrap();
    fs::write(
        legacy_path(codex.path()),
        serde_json::to_vec(&json!({
            "schema": CODEX_LEGACY_SCHEMA,
            "agent": AGENT,
            "runtimeId": AGENT,
            "runtimeIncarnation": INCARNATION,
            "threadId": THREAD,
            "filename": FILE_A,
            "clientId": codex_client_id(THREAD, FILE_A),
            "phase": "accepted",
        }))
        .unwrap(),
    )
    .unwrap();
    let ledger = open(codex.path(), Harness::Codex);
    assert_eq!(ledger.quarantined(), None);
    assert_eq!(ledger.entry(FILE_A).unwrap().phase, Phase::Consumed);
    assert_eq!(ledger.retention(FILE_A), Retention::Release);

    // OpenCode `Accepted` came from a `GET 200`: storage. Mapping it to consumption would make
    // the stored-but-never-admitted class permanently unretryable, so it adopts as persisted and
    // keeps holding.
    let opencode = tempfile::tempdir().unwrap();
    fs::create_dir_all(opencode.path().join("state")).unwrap();
    fs::write(
        legacy_path(opencode.path()),
        serde_json::to_vec(&json!({
            "schema": OPENCODE_LEGACY_SCHEMA,
            "agent": AGENT,
            "runtimeId": AGENT,
            "sessionId": SESSION,
            "filename": FILE_A,
            "messageId": opencode_message_id(SESSION, FILE_A),
            "phase": "accepted",
        }))
        .unwrap(),
    )
    .unwrap();
    let ledger = open(opencode.path(), Harness::OpenCode);
    assert_eq!(ledger.entry(FILE_A).unwrap().phase, Phase::Persisted);
    assert_eq!(
        ledger.retention(FILE_A),
        Retention::Hold(HoldReason::UnreadReceipt),
        "ownership is held because admission is unread, not because adoption is unproven"
    );
    // Two reasons apply to this entry — storage without admission, and a carried-forward claim
    // with no fresh evidence — and the retry gate names the stricter one. Retention describes what
    // the harness has; retry describes what st2 may do about it, and a v1 record is never a reason
    // to transport.
    assert_eq!(
        ledger.retry(FILE_A),
        RetryDecision::Hold(HoldReason::AdoptedWithoutFreshEvidence),
        "adoption alone authorizes no transport"
    );
}

#[test]
fn the_ledger_never_moves_an_inbox_file() {
    // The ledger's whole vocabulary is retention: may-resend, hold and surface, release FIFO
    // ownership. Archive stays the recipient agent's act, so the ledger touches nothing but its
    // own two files.
    let tmp = tempfile::tempdir().unwrap();
    let inbox = tmp.path().join("agents/h/worker/resources/inbox");
    fs::create_dir_all(&inbox).unwrap();
    fs::write(inbox.join(FILE_A), "body").unwrap();

    let mut ledger = open(tmp.path(), Harness::Codex);
    begin(&mut ledger, Harness::Codex);
    ledger.record(FILE_A, Evidence::Consumed).unwrap();
    assert_eq!(ledger.retention(FILE_A), Retention::Release);
    assert!(
        inbox.join(FILE_A).is_file(),
        "released ownership is not an archive"
    );

    // Only the recipient archiving it retires the entry, and even then only the ledger's own
    // record changes.
    fs::remove_file(inbox.join(FILE_A)).unwrap();
    ledger.prune(|_| false).unwrap();
    assert!(ledger.entries().is_empty());
    assert_eq!(read_json(&ledger_path(tmp.path()))["entries"], json!([]));
}
