use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use st2_resource_protocol::{ObservationResult, SnapshotDigest};
use st2_resource_wasip2::{
    CapabilityContext, CapabilityModule, Executor, InvocationStore, ObservationRequest,
    RuntimeConfig,
};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/github-issue",
        world: "github-issue-provider",
    });
}

use bindings::compoundingtech::st2_github_issue::github_issue::{
    Host, IssueError, IssueRequest, IssueResponse, SourceObject, SourceObservation, SourceSnapshot,
};

const IMPORT_NAME: &str = "compoundingtech:st2-github-issue/github-issue@0.1.0";

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
        bindings::GithubIssueProvider::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, _context: CapabilityContext<'_>) -> Self::Invocation {
        FixtureInvocation {
            calls: Arc::clone(&self.calls),
            bindings: Arc::clone(&self.bindings),
        }
    }
}

impl Host for InvocationStore<FixtureInvocation> {
    fn get(&mut self, request: IssueRequest) -> Result<IssueResponse, IssueError> {
        assert_eq!(request.owner, "example");
        assert_eq!(request.repo, "demo");
        assert_eq!(request.number, 42);
        let call = self.capability().calls.fetch_add(1, Ordering::SeqCst);
        match call {
            0 => Ok(IssueResponse::Ok(SourceObservation {
                current: source(2, "2026-08-30T11:22:33Z", "2026-08-30T12:34:56Z"),
                previous: None,
            })),
            1 => Ok(IssueResponse::Ok(SourceObservation {
                current: source(3, "2026-08-30T12:22:33Z", "2026-08-30T12:35:56Z"),
                previous: Some(source(2, "2026-08-30T11:22:33Z", "2026-08-30T12:34:56Z")),
            })),
            2 => Ok(IssueResponse::Ok(SourceObservation {
                current: source(3, "2026-08-30T12:22:33Z", "2026-08-30T12:36:56Z"),
                previous: Some(source(3, "2026-08-30T12:22:33Z", "2026-08-30T12:35:56Z")),
            })),
            3 => Ok(IssueResponse::NotModified),
            _ => Err(IssueError::Unavailable),
        }
    }

    fn bind_snapshot(&mut self, digest: Vec<u8>) -> Result<(), IssueError> {
        assert_eq!(digest.len(), 32);
        self.capability().bindings.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn component_pins_approved_snapshot_facts_topics_and_atomic_results() {
    let module = FixtureModule::default();
    let calls = Arc::clone(&module.calls);
    let bindings = Arc::clone(&module.bindings);
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
    let loaded = executor.load(&fs::read(component()).unwrap()).unwrap();
    let descriptor = executor.describe(&loaded, None).unwrap();
    assert_eq!(
        descriptor.topics,
        ["body", "state", "labels", "assignment", "discussion"]
    );
    assert_eq!(
        descriptor.snapshot_schema_id,
        "dev.schickling.github-issue.snapshot.v1"
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
        ["body", "state", "labels", "assignment", "discussion"]
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
            ("issue", None, Some(Some("#42"))),
            ("state", None, Some(Some("open"))),
            ("comments", None, Some(Some("2"))),
        ]
    );
    let snapshot: Value = serde_json::from_slice(first.bytes.as_slice()).unwrap();
    assert_eq!(
        snapshot,
        json!({
            "schema": "dev.schickling.github-issue.snapshot.v1",
            "uri": "github-issue://github.com/example/demo/issues/42",
            "observedAt": "2026-08-30T12:34:56Z",
            "repository": {"owner": "example", "name": "demo"},
            "number": 42,
            "issue": {
                "title": "Canonical issue",
                "body": "Authoritative body",
                "state": "open",
                "stateReason": null,
                "author": "octocat",
                "htmlUrl": "https://github.com/example/demo/issues/42",
                "locked": false,
                "labels": ["a-first", "z-last"],
                "assignees": ["a-user", "z-user"],
                "milestone": {
                    "number": 7,
                    "title": "Ship it",
                    "state": "open",
                    "htmlUrl": "https://github.com/example/demo/milestone/7",
                    "dueOn": "2026-09-01T00:00:00Z"
                },
                "createdAt": "2026-08-28T09:00:00Z",
                "updatedAt": "2026-08-30T11:23:00Z",
                "closedAt": null
            },
            "discussion": {"commentCount": 2, "latestUpdatedAt": "2026-08-30T11:22:33Z"},
            "facets": {"open": true, "closed": false, "assigned": true, "hasDiscussion": true}
        })
    );
    assert!(!String::from_utf8_lossy(first.bytes.as_slice()).contains("COMMENT BODY"));

    let second = executor
        .observe(
            &loaded,
            &request(2, Some(SnapshotDigest::of(first.bytes.as_slice()))),
            None,
        )
        .unwrap();
    let second = match second {
        ObservationResult::Published { publication } => publication,
        other => panic!("discussion transition must publish, got {other:?}"),
    };
    assert_eq!(second.topics, ["discussion"]);
    let comments = &second.facts.as_ref().unwrap()[2];
    assert_eq!(comments.key(), "comments");
    assert_eq!(comments.before(), Some(Some("2")));
    assert_eq!(comments.after(), Some(Some("3")));

    let third = executor
        .observe(
            &loaded,
            &request(3, Some(SnapshotDigest::of(second.bytes.as_slice()))),
            None,
        )
        .unwrap();
    assert_eq!(third, ObservationResult::Unchanged);
    let fourth = executor
        .observe(
            &loaded,
            &request(4, Some(SnapshotDigest::of(second.bytes.as_slice()))),
            None,
        )
        .unwrap();
    assert_eq!(fourth, ObservationResult::Unchanged);
    let fifth = executor
        .observe(
            &loaded,
            &request(5, Some(SnapshotDigest::of(second.bytes.as_slice()))),
            None,
        )
        .unwrap();
    assert!(matches!(
        &fifth,
        ObservationResult::Failed {
            diagnostic: Some(diagnostic)
        } if diagnostic == "GitHub is unavailable"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 5);
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
                uri: "github-issue://example/demo/42".into(),
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
        } if diagnostic == "invalid canonical GitHub issue URI"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

fn request(invocation_id: u64, prior_digest: Option<SnapshotDigest>) -> ObservationRequest {
    ObservationRequest {
        invocation_id,
        uri: "github-issue://github.com/example/demo/issues/42".into(),
        selector: json!({"topics": ["discussion"]}),
        prior_digest,
        demand_watermark: Some(invocation_id),
    }
}

fn source(comments: u64, latest_updated_at: &str, observed_at: &str) -> SourceSnapshot {
    SourceSnapshot {
        issue: object(
            "\"issue-v1\"",
            json!({
                "title": "Canonical issue",
                "body": "Authoritative body",
                "state": "open",
                "state_reason": null,
                "user": {"login": "octocat"},
                "html_url": "https://github.com/example/demo/issues/42",
                "locked": false,
                "labels": [{"name": "z-last", "color": "ffffff"}, {"name": "a-first", "color": "000000"}],
                "assignees": [{"login": "z-user"}, {"login": "a-user"}],
                "milestone": {
                    "number": 7,
                    "title": "Ship it",
                    "state": "open",
                    "html_url": "https://github.com/example/demo/milestone/7",
                    "due_on": "2026-09-01T00:00:00Z"
                },
                "comments": comments,
                "created_at": "2026-08-28T09:00:00Z",
                "updated_at": "2026-08-30T11:23:00Z",
                "closed_at": null
            }),
        ),
        latest_comment: Some(object(
            "\"comment-v1\"",
            json!([{"updated_at": latest_updated_at}]),
        )),
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
        std::env::var_os("ST2_GITHUB_ISSUE_COMPONENT")
            .expect("ST2_GITHUB_ISSUE_COMPONENT is not set"),
    )
}
