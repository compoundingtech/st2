#![cfg(all(unix, feature = "wasip2-provider-runtime"))]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use st2_resource_protocol::{ObservationResult, SnapshotDigest};
use st2_resource_providers::{
    GitHubIssueConfig, GitHubIssueModule, GitHubPrConfig, GitHubPrModule, PtyStatsConfig,
    PtyStatsModule, PtyStatsScope, VistaConfig, VistaModule,
};
use st2_resource_wasip2::{Executor, ObservationRequest, RuntimeConfig};

#[test]
fn pty_component_observes_replays_and_enforces_capability_scope() {
    let temporary = tempfile::tempdir().unwrap();
    let executable = temporary.path().join("pty-stats");
    write_executable(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' '{\"sessions\":2,\"bytes\":64}'\n",
    );

    let module = PtyStatsModule::new(
        PtyStatsConfig::resolve(
            &executable,
            temporary.path().to_path_buf(),
            PtyStatsScope::All,
            Duration::from_secs(5),
        )
        .unwrap(),
    );
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
    let component_bytes = fs::read(component("ST2_PTY_STATS_COMPONENT")).unwrap();
    let loaded = executor.load(&component_bytes).unwrap();
    let descriptor = executor.describe(&loaded, None).unwrap();
    assert_eq!(descriptor.topics, ["stats"]);
    assert_eq!(descriptor.snapshot_schema_id, "st2.resource.pty-stats.v1");
    assert_eq!(descriptor.snapshot_media_type, "application/json");


    let first = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 1,
                uri: "dev.st2.pty-stats://all".into(),
                selector: json!({ "topics": ["stats"] }),
                prior_digest: None,
                demand_watermark: Some(1),
            },
            None,
        )
        .unwrap();
    let publication = match first {
        ObservationResult::Published { publication } => publication,
        other => panic!("first observation must publish, got {other:?}"),
    };
    assert_eq!(publication.schema_id, "st2.resource.pty-stats.v1");
    assert_eq!(publication.topics, ["stats"]);
    assert_eq!(
        publication
            .facts
            .as_ref()
            .unwrap()
            .iter()
            .map(|fact| fact.key())
            .collect::<Vec<_>>(),
        ["scope"]
    );
    let prior = SnapshotDigest::of(publication.bytes.as_slice());

    let replay = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 2,
                uri: "dev.st2.pty-stats://all".into(),
                selector: json!({ "topics": ["stats"] }),
                prior_digest: Some(prior),
                demand_watermark: Some(2),
            },
            None,
        )
        .unwrap();
    assert_eq!(replay, ObservationResult::Unchanged);

    let denied = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 3,
                uri: "dev.st2.pty-stats://session/other".into(),
                selector: json!({ "session": "other", "topics": ["stats"] }),
                prior_digest: None,
                demand_watermark: Some(3),
            },
            None,
        )
        .unwrap();
    assert!(matches!(
        &denied,
        ObservationResult::Failed {
            diagnostic: Some(diagnostic)
        } if diagnostic.contains("PTY stats scope denied")
    ));
}

#[test]
fn github_pr_component_describes_and_denies_out_of_scope_before_transport() {
    let module = GitHubPrModule::new(GitHubPrConfig {
        owner: "example".into(),
        repo: "demo".into(),
        number: 389,
        connect_timeout: Duration::from_secs(3),
        total_timeout: Duration::from_secs(10),
    })
    .unwrap();
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
    let component_bytes = fs::read(component("ST2_GITHUB_PR_COMPONENT")).unwrap();
    let loaded = executor.load(&component_bytes).unwrap();
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

    let denied = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 1,
                uri: "github-pr://other/demo/389".into(),
                selector: json!({
                    "owner": "other",
                    "repo": "demo",
                    "number": 389,
                    "topics": ["ci.failure"]
                }),
                prior_digest: None,
                demand_watermark: Some(1),
            },
            None,
        )
        .unwrap();
    assert!(matches!(
        &denied,
        ObservationResult::Failed {
            diagnostic: Some(diagnostic)
        } if diagnostic.contains("GitHub pull request scope denied")
    ));
}

#[test]
fn vista_component_observes_replays_and_enforces_identity_before_spawn() {
    let temporary = tempfile::tempdir().unwrap();
    let executable = temporary.path().join("vista");
    write_executable(
        &executable,
        r#"#!/bin/sh
set -eu
if [ "$#" -ne 6 ] || [ "$1" != artifact ] || [ "$2" != get ] || [ "$3" != release-notes ] || [ "$4" != v7 ] || [ "$5" != --output ] || [ "$6" != json ]; then
  printf 'unexpected argv: %s\n' "$*" >&2
  exit 64
fi
printf '%s\n' "$*" >> "$PWD/invocations"
IFS= read -r mode < "$PWD/mode"
case "$mode" in
  ready)
    printf '%s\n' '{"schemaVersion":1,"uri":"vista://release-notes/v7","slug":"release-notes","version":7,"author":"agent","timestamp":"2026-09-02T10:00:00Z","changeSummary":"created","parent":null,"retired":false,"state":"ready","canonicalUrl":"https://vista.example/release-notes/v7","title":"Release notes","status":{"locked":1,"open":2,"awaiting":3}}'
    ;;
  changed)
    printf '%s\n' '{"schemaVersion":1,"uri":"vista://release-notes/v7","slug":"release-notes","version":7,"author":"agent","timestamp":"2026-09-02T10:00:00Z","changeSummary":"revised","parent":null,"retired":false,"state":"ready","canonicalUrl":"https://vista.example/release-notes/v7","title":"Revised release notes","status":{"locked":1,"open":1,"awaiting":2}}'
    ;;
  mismatch)
    printf '%s\n' '{"schemaVersion":1,"uri":"vista://different/v7","slug":"different","version":7,"author":"agent","timestamp":"2026-09-02T10:00:00Z","changeSummary":"wrong","parent":null,"retired":false,"state":"ready","canonicalUrl":"https://vista.example/different/v7"}'
    ;;
  unknown-field)
    printf '%s\n' '{"schemaVersion":1,"uri":"vista://release-notes/v7","slug":"release-notes","version":7,"author":"agent","timestamp":"2026-09-02T10:00:00Z","changeSummary":"wrong","parent":null,"retired":false,"state":"ready","canonicalUrl":"https://vista.example/release-notes/v7","extra":true}'
    ;;
  nonzero)
    printf 'artifact unavailable for release-notes\n' >&2
    exit 7
    ;;
esac
"#,
    );
    fs::write(temporary.path().join("mode"), "ready\n").unwrap();

    let module = VistaModule::new(
        VistaConfig::resolve(
            &executable,
            temporary.path().to_path_buf(),
            "release-notes".into(),
            7,
            Duration::from_secs(5),
        )
        .unwrap(),
    );
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
    let component_bytes = fs::read(component("ST2_VISTA_COMPONENT")).unwrap();
    let loaded = executor.load(&component_bytes).unwrap();
    let descriptor = executor.describe(&loaded, None).unwrap();
    assert_eq!(descriptor.capabilities.len(), 1);
    assert_eq!(
        descriptor.topics,
        ["ready", "updated", "failed", "expired"]
    );
    assert_eq!(
        descriptor.snapshot_schema_id,
        "dev.schickling.vista.snapshot.v1"
    );
    assert_eq!(descriptor.snapshot_media_type, "application/json");

    let request = |invocation_id, prior_digest| ObservationRequest {
        invocation_id,
        uri: "vista://release-notes/v7".into(),
        selector: json!({
            "slug": "release-notes",
            "version": 7,
            "topics": ["ready", "updated", "failed", "expired"]
        }),
        prior_digest,
        demand_watermark: Some(invocation_id),
    };
    let first = executor.observe(&loaded, &request(1, None), None).unwrap();
    let publication = match first {
        ObservationResult::Published { publication } => publication,
        other => panic!("first Vista observation must publish, got {other:?}"),
    };
    assert_eq!(publication.topics, ["ready"]);
    assert_eq!(
        publication
            .facts
            .as_ref()
            .unwrap()
            .iter()
            .map(|fact| fact.key())
            .collect::<Vec<_>>(),
        ["state"]
    );
    let carrier: serde_json::Value =
        serde_json::from_slice(publication.bytes.as_slice()).unwrap();
    assert_eq!(
        carrier.get("schema").and_then(serde_json::Value::as_str),
        Some("dev.schickling.vista.snapshot.v1")
    );
    assert!(carrier.get("observedAt").is_none());
    let prior = SnapshotDigest::of(publication.bytes.as_slice());

    assert_eq!(
        executor
            .observe(&loaded, &request(2, Some(prior)), None)
            .unwrap(),
        ObservationResult::Unchanged
    );

    fs::write(temporary.path().join("mode"), "changed\n").unwrap();
    let changed = executor.observe(&loaded, &request(3, Some(prior)), None).unwrap();
    assert!(matches!(
        changed,
        ObservationResult::Published { publication }
            if publication.topics == ["updated", "ready"]
    ));

    let invocations_before = fs::read_to_string(temporary.path().join("invocations")).unwrap();
    let malformed = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 4,
                uri: "vista://bad--slug/v7".into(),
                selector: json!({ "slug": "bad--slug", "version": 7 }),
                prior_digest: None,
                demand_watermark: Some(4),
            },
            None,
        )
        .unwrap();
    assert!(matches!(malformed, ObservationResult::Failed { .. }));
    assert_eq!(
        fs::read_to_string(temporary.path().join("invocations")).unwrap(),
        invocations_before
    );

    for (mode, expected) in [
        ("mismatch", "different artifact identity"),
        ("unknown-field", "invalid manifest"),
        ("nonzero", "artifact unavailable for release-notes"),
    ] {
        fs::write(temporary.path().join("mode"), format!("{mode}\n")).unwrap();
        let result = executor
            .observe(&loaded, &request(5, None), None)
            .unwrap();
        assert!(matches!(
            &result,
            ObservationResult::Failed {
                diagnostic: Some(diagnostic)
            } if diagnostic.contains(expected)
        ));
    }
}

#[test]
#[ignore = "explicit read-only public GitHub smoke; requires network and ST2_GITHUB_ISSUE_COMPONENT"]
fn github_component_public_read_only_smoke() {
    let module = GitHubIssueModule::new(GitHubIssueConfig {
        owner: "rust-lang".into(),
        repo: "rust".into(),
        number: 1,
        connect_timeout: Duration::from_secs(3),
        total_timeout: Duration::from_secs(10),
    })
    .unwrap();
    let executor = Executor::new(RuntimeConfig::default(), None, module).unwrap();
    let component_bytes = fs::read(component("ST2_GITHUB_ISSUE_COMPONENT")).unwrap();
    let loaded = executor.load(&component_bytes).unwrap();
    let descriptor = executor.describe(&loaded, None).unwrap();
    assert_eq!(descriptor.topics, ["issue"]);
    assert_eq!(
        descriptor.snapshot_schema_id,
        "st2.resource.github-issue.v1"
    );
    assert_eq!(descriptor.snapshot_media_type, "application/json");

    let observed = executor
        .observe(
            &loaded,
            &ObservationRequest {
                invocation_id: 1,
                uri: "dev.st2.github-issue://rust-lang/rust/1".into(),
                selector: json!({
                    "owner": "rust-lang",
                    "repo": "rust",
                    "number": 1,
                    "topics": ["issue"]
                }),
                prior_digest: None,
                demand_watermark: Some(1),
            },
            None,
        )
        .unwrap();
    assert!(matches!(observed, ObservationResult::Published { .. }));
}

fn component(variable: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(variable).unwrap_or_else(|| panic!("{variable} is not set")))
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}
