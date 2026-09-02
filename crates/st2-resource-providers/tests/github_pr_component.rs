use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use st2_resource_protocol::{ObservationResult, SnapshotDigest};
use st2_resource_wasip2::{
    CapabilityContext, CapabilityModule, Executor, InterruptionReason, InvocationControl,
    InvocationStore, ObservationRequest, ObserveError, RuntimeConfig,
};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/github-pr",
        world: "github-pr-provider",
    });
}

use bindings::compoundingtech::st2_github_pr::github_pr::{
    Host, PullRequestError, PullRequestRequest, PullRequestResponse, SourceObject,
    SourceObservation, SourceSnapshot,
};

const IMPORT_NAME: &str = "compoundingtech:st2-github-pr/github-pr@0.1.0";
const HEAD_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone, Default)]
struct FixtureModule {
    calls: Arc<AtomicUsize>,
}

struct FixtureInvocation {
    calls: Arc<AtomicUsize>,
}

impl CapabilityModule for FixtureModule {
    type Invocation = FixtureInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[IMPORT_NAME]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        bindings::GithubPrProvider::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, _context: CapabilityContext<'_>) -> Self::Invocation {
        FixtureInvocation {
            calls: Arc::clone(&self.calls),
        }
    }
}

impl Host for InvocationStore<FixtureInvocation> {
    fn get(
        &mut self,
        request: PullRequestRequest,
    ) -> Result<PullRequestResponse, PullRequestError> {
        assert_eq!(request.owner, "example");
        assert_eq!(request.repo, "demo");
        assert_eq!(request.number, 389);
        let call = self.capability().calls.fetch_add(1, Ordering::SeqCst);
        Ok(match call {
            0 => PullRequestResponse::Ok(SourceObservation {
                current: source(false, "2026-08-30T12:34:56Z"),
                previous: None,
            }),
            1 => PullRequestResponse::Ok(SourceObservation {
                current: source(true, "2026-08-30T12:35:56Z"),
                previous: Some(source(false, "2026-08-30T12:34:56Z")),
            }),
            _ => PullRequestResponse::Ok(SourceObservation {
                current: source(true, "2026-08-30T12:36:56Z"),
                previous: Some(source(true, "2026-08-30T12:35:56Z")),
            }),
        })
    }
}

#[derive(Clone)]
struct BlockingModule {
    entered: Arc<Barrier>,
}

struct BlockingInvocation {
    entered: Arc<Barrier>,
    control: InvocationControl,
}

impl CapabilityModule for BlockingModule {
    type Invocation = BlockingInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[IMPORT_NAME]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        bindings::GithubPrProvider::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, context: CapabilityContext<'_>) -> Self::Invocation {
        BlockingInvocation {
            entered: Arc::clone(&self.entered),
            control: context.control().clone(),
        }
    }
}

impl Host for InvocationStore<BlockingInvocation> {
    fn get(
        &mut self,
        _request: PullRequestRequest,
    ) -> Result<PullRequestResponse, PullRequestError> {
        let capability = self.capability();
        capability.entered.wait();
        match capability.control.wait_for_interruption() {
            InterruptionReason::Cancelled => Err(PullRequestError::Unavailable),
            InterruptionReason::TimedOut => Err(PullRequestError::DeadlineExceeded),
        }
    }
}

#[test]
fn component_preserves_snapshot_facets_delta_topics_and_semantic_replay() {
    let executor = Executor::new(RuntimeConfig::default(), None, FixtureModule::default()).unwrap();
    let bytes = fs::read(component()).unwrap();
    let loaded = executor.load(&bytes).unwrap();
    let descriptor = executor.describe(&loaded, None).unwrap();
    assert_eq!(
        descriptor.topics,
        [
            "ci.failure",
            "mergeability.conflict",
            "review.requested",
            "terminal"
        ]
    );
    assert_eq!(
        descriptor.snapshot_schema_id,
        "dev.schickling.github-pr.snapshot.v1"
    );
    assert_eq!(descriptor.snapshot_media_type, "application/json");

    let first = executor
        .observe(&loaded, &request(1, None), None)
        .unwrap();
    let first = match first {
        ObservationResult::Published { publication } => publication,
        other => panic!("first observation must publish, got {other:?}"),
    };
    assert_eq!(
        first.topics,
        [
            "ci.failure",
            "mergeability.conflict",
            "review.requested",
            "terminal"
        ]
    );
    let snapshot: Value = serde_json::from_slice(first.bytes.as_slice()).unwrap();
    assert_eq!(
        snapshot,
        json!({
            "ci": {
                "checkRuns": [
                    {"conclusion": "failure", "detailsUrl": "https://example.invalid/a", "name": "a-build", "status": "completed"},
                    {"conclusion": "success", "detailsUrl": "https://example.invalid/z", "name": "z-test", "status": "completed"}
                ],
                "state": "failure",
                "statuses": [
                    {"context": "a/build", "description": "failed", "state": "failure", "targetUrl": "https://example.invalid/a"},
                    {"context": "z/lint", "description": null, "state": "success", "targetUrl": "https://example.invalid/z"}
                ]
            },
            "facets": {"ciFailure": true, "mergeConflict": false, "reviewRequested": true, "terminal": false},
            "number": 389,
            "observedAt": "2026-08-30T12:34:56Z",
            "pullRequest": {
                "apiUrl": "https://api.github.com/repos/example/demo/pulls/389",
                "base": {"ref": "main"},
                "closedAt": null,
                "draft": false,
                "head": {"ref": "resources", "sha": HEAD_SHA},
                "htmlUrl": "https://github.com/example/demo/pull/389",
                "mergeable": true,
                "mergeableState": "clean",
                "merged": false,
                "mergedAt": null,
                "requestedReviewers": ["a-reviewer", "z-reviewer"],
                "requestedTeams": ["team-a", "team-b"],
                "state": "open"
            },
            "repository": {"name": "demo", "owner": "example"},
            "schema": "dev.schickling.github-pr.snapshot.v1",
            "uri": "github-pr://example/demo/389"
        })
    );

    let second = executor
        .observe(
            &loaded,
            &request(2, Some(SnapshotDigest::of(first.bytes.as_slice()))),
            None,
        )
        .unwrap();
    let second = match second {
        ObservationResult::Published { publication } => publication,
        other => panic!("terminal transition must publish, got {other:?}"),
    };
    assert_eq!(second.topics, ["terminal"]);
    assert_eq!(
        second
            .facts
            .as_ref()
            .unwrap()
            .iter()
            .map(|fact| fact.key())
            .collect::<Vec<_>>(),
        ["facets.terminal"]
    );

    let third = executor
        .observe(
            &loaded,
            &request(3, Some(SnapshotDigest::of(second.bytes.as_slice()))),
            None,
        )
        .unwrap();
    assert_eq!(third, ObservationResult::Unchanged);
}

#[test]
fn component_import_cancellation_is_deterministic() {
    assert!(matches!(
        interrupt_blocked_import(BlockedInterruption::Cancel),
        ObserveError::Cancelled
    ));
}

#[test]
fn component_import_deadline_is_deterministic() {
    assert!(matches!(
        interrupt_blocked_import(BlockedInterruption::TimeOut),
        ObserveError::TimedOut
    ));
}

enum BlockedInterruption {
    Cancel,
    TimeOut,
}

fn interrupt_blocked_import(interruption: BlockedInterruption) -> ObserveError {
    let entered = Arc::new(Barrier::new(2));
    let executor = Executor::new(
        RuntimeConfig::default(),
        None,
        BlockingModule {
            entered: Arc::clone(&entered),
        },
    )
    .unwrap();
    let bytes = fs::read(component()).unwrap();
    let loaded = executor.load(&bytes).unwrap();
    let handle = executor.interruption_handle();
    let trigger = handle.clone();
    let observing =
        std::thread::spawn(move || executor.observe(&loaded, &request(4, None), Some(&handle)));
    entered.wait();
    match interruption {
        BlockedInterruption::Cancel => assert!(trigger.cancel()),
        BlockedInterruption::TimeOut => assert!(trigger.time_out()),
    }
    observing.join().unwrap().unwrap_err()
}

fn request(invocation_id: u64, prior_digest: Option<SnapshotDigest>) -> ObservationRequest {
    ObservationRequest {
        invocation_id,
        uri: "github-pr://example/demo/389".into(),
        selector: json!({
            "owner": "example",
            "repo": "demo",
            "number": 389,
            "topics": ["ci.failure", "terminal"]
        }),
        prior_digest,
        demand_watermark: Some(invocation_id),
    }
}

fn source(terminal: bool, observed_at: &str) -> SourceSnapshot {
    let state = if terminal { "closed" } else { "open" };
    SourceSnapshot {
        pull_request: object(
            "\"pull-v1\"",
            json!({
                "number": 389,
                "url": "https://api.github.com/repos/example/demo/pulls/389",
                "html_url": "https://github.com/example/demo/pull/389",
                "state": state,
                "draft": false,
                "merged": terminal,
                "merged_at": if terminal { Some("2026-08-30T12:35:00Z") } else { None },
                "closed_at": if terminal { Some("2026-08-30T12:35:00Z") } else { None },
                "mergeable": true,
                "mergeable_state": "clean",
                "head": {"sha": HEAD_SHA, "ref": "resources"},
                "base": {"ref": "main"},
                "requested_reviewers": [{"login": "z-reviewer"}, {"login": "a-reviewer"}],
                "requested_teams": [{"slug": "team-b"}, {"slug": "team-a"}]
            }),
        ),
        check_runs: object(
            "\"checks-v1\"",
            json!({
                "check_runs": [
                    {"name": "z-test", "status": "completed", "conclusion": "success", "details_url": "https://example.invalid/z"},
                    {"name": "a-build", "status": "completed", "conclusion": "failure", "details_url": "https://example.invalid/a"}
                ]
            }),
        ),
        combined_status: object(
            "\"status-v1\"",
            json!({
                "state": "failure",
                "statuses": [
                    {"context": "z/lint", "state": "success", "target_url": "https://example.invalid/z", "description": null},
                    {"context": "a/build", "state": "failure", "target_url": "https://example.invalid/a", "description": "failed"}
                ]
            }),
        ),
        observed_at: observed_at.into(),
    }
}

fn object(etag: &str, value: Value) -> SourceObject {
    SourceObject {
        etag: Some(etag.into()),
        body: serde_json::to_vec(&value).unwrap(),
    }
}

fn component() -> PathBuf {
    PathBuf::from(
        std::env::var_os("ST2_GITHUB_PR_COMPONENT")
            .expect("ST2_GITHUB_PR_COMPONENT is not set"),
    )
}
