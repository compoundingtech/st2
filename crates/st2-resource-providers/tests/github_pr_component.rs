use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

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
    Host, PullRequestError, PullRequestRequest, SourceObservation, SourceSnapshot,
};

const IMPORT_NAME: &str = "compoundingtech:st2-github-pr/github-pr@0.1.0";
const HEAD_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone, Default)]
struct FixtureModule {
    calls: Arc<AtomicUsize>,
    bindings: Arc<AtomicUsize>,
}

struct FixtureInvocation {
    calls: Arc<AtomicUsize>,
    bindings: Arc<AtomicUsize>,
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
            bindings: Arc::clone(&self.bindings),
        }
    }
}

impl Host for InvocationStore<FixtureInvocation> {
    fn get(&mut self, request: PullRequestRequest) -> Result<SourceObservation, PullRequestError> {
        assert_eq!(request.owner, "example");
        assert_eq!(request.repo, "demo");
        assert_eq!(request.number, 389);
        let call = self.capability().calls.fetch_add(1, Ordering::SeqCst);
        Ok(match call {
            0 => SourceObservation {
                current: source(false, "2026-08-30T12:34:56Z"),
                previous: None,
            },
            1 => SourceObservation {
                current: source(true, "2026-08-30T12:35:56Z"),
                previous: Some(source(false, "2026-08-30T12:34:56Z")),
            },
            _ => SourceObservation {
                current: source(true, "2026-08-30T12:36:56Z"),
                previous: Some(source(true, "2026-08-30T12:35:56Z")),
            },
        })
    }

    fn bind_snapshot(&mut self, digest: Vec<u8>) -> Result<(), PullRequestError> {
        assert_eq!(digest.len(), 32);
        self.capability().bindings.fetch_add(1, Ordering::SeqCst);
        Ok(())
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
    fn get(&mut self, _request: PullRequestRequest) -> Result<SourceObservation, PullRequestError> {
        let capability = self.capability();
        capability.entered.wait();
        match capability.control.wait_for_interruption() {
            InterruptionReason::Cancelled => Err(PullRequestError::Unavailable),
            InterruptionReason::TimedOut => Err(PullRequestError::DeadlineExceeded),
        }
    }

    fn bind_snapshot(&mut self, _digest: Vec<u8>) -> Result<(), PullRequestError> {
        Ok(())
    }
}

#[test]
fn component_pins_approved_snapshot_facts_topics_and_atomic_results() {
    let module = FixtureModule::default();
    let calls = Arc::clone(&module.calls);
    let bindings = Arc::clone(&module.bindings);
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
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
    let selector_properties = descriptor.selector_schema["properties"]
        .as_object()
        .unwrap();
    assert!(selector_properties.contains_key("topics"));
    assert!(!selector_properties.contains_key("owner"));

    let first = executor.observe(&loaded, &request(1, None), None).unwrap();
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
    assert_eq!(
        first
            .facts
            .as_ref()
            .unwrap()
            .iter()
            .map(|fact| (fact.key(), fact.before(), fact.after()))
            .collect::<Vec<_>>(),
        [
            ("pr", None, Some(Some("#389"))),
            ("state", None, Some(Some("open"))),
            ("ci", None, Some(Some("failure"))),
        ]
    );
    let snapshot: Value = serde_json::from_slice(first.bytes.as_slice()).unwrap();
    assert_eq!(
        snapshot,
        json!({
            "schema": "dev.schickling.github-pr.snapshot.v1",
            "uri": "github-pr://github.com/example/demo/pull/389",
            "observedAt": "2026-08-30T12:34:56Z",
            "repository": {"owner": "example", "name": "demo"},
            "number": 389,
            "pullRequest": {
                "apiUrl": "https://api.github.com/repos/example/demo/pulls/389",
                "htmlUrl": "https://github.com/example/demo/pull/389",
                "title": "Canonical pull request",
                "body": "Authoritative PR body",
                "state": "open",
                "author": "octocat",
                "draft": false,
                "merged": false,
                "mergedAt": null,
                "closedAt": null,
                "mergeable": true,
                "mergeableState": "clean",
                "head": {"sha": HEAD_SHA, "ref": "resources"},
                "base": {"ref": "main"},
                "reviewDecision": "REVIEW_REQUIRED",
                "requestedReviewers": ["copilot-pull-request-reviewer", "former-reviewer", "z-reviewer"],
                "requestedTeams": ["team-a"],
                "reviewRequestTotalCount": 4,
                "reviewRequestsTruncated": false
            },
            "ci": {
                "state": "failure",
                "totalCount": 101,
                "truncated": true,
                "checkRuns": [
                    {"name": "a-build", "status": "completed", "conclusion": "failure", "detailsUrl": "https://github.com/example/demo/actions/a"},
                    {"name": "z-test", "status": "completed", "conclusion": "success", "detailsUrl": "https://github.com/example/demo/actions/z"}
                ],
                "statuses": [
                    {"context": "a/build", "state": "failure", "targetUrl": "https://ci.example/a", "description": "failed"}
                ]
            },
            "facets": {"reviewRequested": true, "ciFailure": true, "mergeConflict": false, "terminal": false}
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
        ["pr", "state", "ci"]
    );

    let third = executor
        .observe(
            &loaded,
            &request(3, Some(SnapshotDigest::of(second.bytes.as_slice()))),
            None,
        )
        .unwrap();
    assert_eq!(third, ObservationResult::Unchanged);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(bindings.load(Ordering::SeqCst), 3);
}

#[test]
fn component_rejects_old_short_uri_before_import_as_one_failed_result() {
    let module = FixtureModule::default();
    let calls = Arc::clone(&module.calls);
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
    let loaded = executor.load(&fs::read(component()).unwrap()).unwrap();
    let result = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 9,
                uri: "github-pr://example/demo/389".into(),
                selector: json!({}),
                prior_digest: None,
                demand_watermark: Some(9),
            },
            None,
        )
        .unwrap();
    assert!(matches!(
        &result,
        ObservationResult::Failed {
            diagnostic: Some(diagnostic)
        } if diagnostic == "invalid canonical GitHub pull request URI"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
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
        std::thread::spawn(move || executor.observe(&loaded, &request(10, None), Some(&handle)));
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
        uri: "github-pr://github.com/example/demo/pull/389".into(),
        selector: json!({"topics": ["ci.failure", "terminal"]}),
        prior_digest,
        demand_watermark: Some(invocation_id),
    }
}

fn source(terminal: bool, observed_at: &str) -> SourceSnapshot {
    let mut data = graphql_data();
    let pull = &mut data["repository"]["pullRequest"];
    if terminal {
        pull["state"] = json!("CLOSED");
        pull["merged"] = json!(true);
        pull["mergedAt"] = json!("2026-08-30T12:35:00Z");
        pull["closedAt"] = json!("2026-08-30T12:35:00Z");
    }
    SourceSnapshot {
        graphql_data: serde_json::to_vec(&data).unwrap(),
        observed_at: observed_at.into(),
    }
}

fn graphql_data() -> Value {
    json!({
        "repository": {
            "pullRequest": {
                "url": "https://github.com/example/demo/pull/389",
                "title": "Canonical pull request",
                "body": "Authoritative PR body",
                "state": "OPEN",
                "isDraft": false,
                "merged": false,
                "mergedAt": null,
                "closedAt": null,
                "mergeable": "MERGEABLE",
                "author": {"login": "octocat"},
                "headRefOid": HEAD_SHA,
                "headRefName": "resources",
                "baseRefName": "main",
                "reviewDecision": "REVIEW_REQUIRED",
                "reviewRequests": {
                    "totalCount": 4,
                    "nodes": [
                        {"requestedReviewer": {"__typename": "User", "login": "z-reviewer"}},
                        {"requestedReviewer": {"__typename": "Team", "slug": "team-a"}},
                        {"requestedReviewer": {"__typename": "Bot", "login": "copilot-pull-request-reviewer"}},
                        {"requestedReviewer": {"__typename": "Mannequin", "login": "former-reviewer"}}
                    ]
                },
                "commits": {
                    "nodes": [{
                        "commit": {
                            "statusCheckRollup": {
                                "state": "FAILURE",
                                "contexts": {
                                    "totalCount": 101,
                                    "nodes": [
                                        {"__typename": "CheckRun", "name": "z-test", "status": "COMPLETED", "conclusion": "SUCCESS", "detailsUrl": "https://github.com/example/demo/actions/z"},
                                        {"__typename": "CheckRun", "name": "a-build", "status": "COMPLETED", "conclusion": "FAILURE", "detailsUrl": "https://github.com/example/demo/actions/a"},
                                        {"__typename": "StatusContext", "context": "a/build", "state": "FAILURE", "targetUrl": "https://ci.example/a", "description": "failed"}
                                    ]
                                }
                            }
                        }
                    }]
                }
            }
        }
    })
}

fn component() -> PathBuf {
    PathBuf::from(
        std::env::var_os("ST2_GITHUB_PR_COMPONENT").expect("ST2_GITHUB_PR_COMPONENT is not set"),
    )
}
