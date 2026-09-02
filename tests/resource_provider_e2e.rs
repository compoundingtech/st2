#![cfg(all(unix, feature = "wasip2-provider-runtime"))]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use st2_resource_protocol::{ObservationResult, SnapshotDigest};
use st2_resource_providers::{
    GitHubIssueConfig, GitHubIssueModule, GitHubPrConfig, GitHubPrModule, PtyStatsConfig,
    PtyStatsModule, PtyStatsScope,
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
