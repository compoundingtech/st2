//! The join between resync ([`06-resync`](../docs/vrs/06-resync/spec.md)) and DING delivery.
//!
//! [`tests/resync.rs`] proves the ingress half and defers the wake to "the existing delivery
//! suite". That suite proves the wake for a *publicly* emitted stream event
//! ([`tests/event_e2e.rs::event_emit_cli_returns_a_stable_json_receipt_and_ding_marks_the_record`]),
//! which reaches the inbox through a different admission path than a built-in resync record, and
//! reads the inbox with `message::list_inbox` rather than the arrival scan the live loop uses.
//!
//! So nothing proved that a resync record survives DING's own `new_arrivals` scan — the place a
//! stream predicate would silently swallow it — or that it renders as stream work. This file
//! closes that seam against the real supervisor ingress.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn write_agent(root: &Path) -> PathBuf {
    let dir = root.join("agents/hetz/worker");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("agent.kdl"),
        r#"agent "worker" {
  host "hetz"
  command "agent"
  resource "goal" uri="resources/goal.md" reason="Mission."
}"#,
    )
    .unwrap();
    st2::event::publish_owner_binding_for_test(root, "hetz").unwrap();
    dir
}

fn resync_records(inbox: &Path) -> usize {
    fs::read_dir(inbox)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| {
                    fs::read_to_string(entry.path())
                        .is_ok_and(|contents| contents.contains("stream: resync"))
                })
                .count()
        })
        .unwrap_or(0)
}

fn wait_for(condition: impl Fn() -> usize, expected: usize) -> bool {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if condition() >= expected {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    condition() >= expected
}

/// A carrier change becomes a `[DING]` notice: the record is a new arrival to DING's own scan, and
/// it renders as stream work rather than as a message from an unknown peer.
#[test]
fn a_resync_record_is_a_ding_arrival_and_renders_as_stream_work() {
    let catalog = tempfile::tempdir().unwrap();
    let agent_dir = write_agent(catalog.path());
    let inbox = agent_dir.join("resources/inbox");

    let supervisor =
        st2::resync::ResyncSupervisor::spawn(catalog.path().to_path_buf(), "hetz".to_owned());
    assert!(
        supervisor
            .refresh(
                &st2::discover_strict(catalog.path()).specs,
                "hetz",
                &[],
                &[],
            )
            .is_empty()
    );
    std::thread::sleep(Duration::from_millis(300));

    // Start DING's seen-set from the current unread set, exactly as `run_ding` does at startup, so
    // the arrival below is the only thing this scan can report.
    let mut seen: HashSet<String> = HashSet::new();
    let backlog = st2::ding::new_arrivals(&inbox, &mut seen);
    assert!(
        backlog.is_empty(),
        "seeding is silent, so there is no backlog"
    );

    let goal = agent_dir.join("resources/goal.md");
    fs::create_dir_all(goal.parent().unwrap()).unwrap();
    fs::write(&goal, "ship the thing\n").unwrap();
    assert!(
        wait_for(|| resync_records(&inbox), 1),
        "the goal carrier change must reach the inbox"
    );

    let arrivals = st2::ding::new_arrivals(&inbox, &mut seen);
    assert_eq!(
        arrivals.len(),
        1,
        "the resync record must reach DING's arrival scan, not be filtered out of it"
    );
    let record = &arrivals[0];
    assert_eq!(record.stream.as_deref(), Some("resync"));
    assert!(
        record.event_id.is_some(),
        "a resync record carries an event id"
    );

    let notice = st2::ding::poke_text(catalog.path(), "hetz", "hetz.worker", record);
    assert!(
        notice.starts_with("[DING] » hetz.worker/resync: resource goal changed"),
        "a resync record renders as stream work: {notice}"
    );

    // The arrival is consumed exactly once: a second scan re-poking it would duplicate the wake.
    assert!(
        st2::ding::new_arrivals(&inbox, &mut seen).is_empty(),
        "a delivered resync record must not be reported as a new arrival again"
    );
}
