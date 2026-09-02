#![cfg(all(unix, feature = "wasip2-provider-runtime"))]

use parking_lot::Mutex;
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use st2::resource_observe::{ObserveReceipt, ObserveReceiptStatus, ObserveRequest, submit_request};
use st2::resource_profile_supervisor::ResourceProfileSupervisor;

static STATE_ENV: Mutex<()> = Mutex::new(());
const WAIT: Duration = Duration::from_secs(30);

#[test]
fn supervisor_compatibility_contract_uses_the_production_pty_component() {
    // The component executor replaces native protocol frames. Executor import admission and
    // resource limits live in st2-resource-wasip2 tests; this test maps the applicable supervisor
    // contract to real component changed/unchanged/failed proposals, health, and restart recovery.
    let _guard = STATE_ENV.lock();
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };
    let executable = temporary.path().join("fixture-pty");
    write_executable(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' '[{\"name\":\"subject\",\"status\":\"exited\",\"generation\":1}]'\n",
    );
    let pty = ProviderFixture::new(
        temporary.path().join("pty"),
        "pty",
        component("ST2_PTY_STATS_COMPONENT"),
        r#"{"topics":["lifecycle","metadata"]}"#,
        &format!(
            "pty-stats executable={:?} cwd={:?} deadline-ms=10000",
            executable,
            temporary.path()
        ),
        "dev.schickling.pty.snapshot.v1",
        &["lifecycle", "metadata", "runtime"],
    );
    let first = pty.observe(None);
    assert_eq!(
        first.status,
        ObserveReceiptStatus::SettledChanged,
        "{first:?}"
    );
    let first_bytes = fs::read(pty.snapshot()).unwrap();
    let first_snapshot: serde_json::Value = serde_json::from_slice(&first_bytes).unwrap();
    assert_eq!(
        first_snapshot
            .get("schema")
            .and_then(serde_json::Value::as_str),
        Some("dev.schickling.pty.snapshot.v1")
    );
    assert_eq!(
        first_snapshot
            .get("uri")
            .and_then(serde_json::Value::as_str),
        Some("pty:subject")
    );
    let replay = pty.observe(first.digest);
    assert_eq!(replay.status, ObserveReceiptStatus::SettledUnchanged);
    assert_eq!(fs::read(pty.snapshot()).unwrap(), first_bytes);

    write_executable(&executable, "#!/bin/sh\nexit 7\n");
    let failed = pty.observe(first.digest);
    assert_eq!(failed.status, ObserveReceiptStatus::SettledFailed);
    assert_eq!(fs::read(pty.snapshot()).unwrap(), first_bytes);
    assert!(
        pty.supervisor
            .health()
            .iter()
            .any(|health| health.binding.as_deref() == Some("observed")
                && health.state == st2::resource_profile::RuntimeHealthState::Degraded)
    );

    write_executable(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' '[{\"name\":\"subject\",\"status\":\"exited\",\"generation\":2}]'\n",
    );
    let recovered = pty.observe(first.digest);
    assert_eq!(recovered.status, ObserveReceiptStatus::SettledChanged);
    assert!(
        pty.supervisor
            .health()
            .iter()
            .any(|health| health.binding.as_deref() == Some("observed")
                && health.state == st2::resource_profile::RuntimeHealthState::Ready)
    );
    let recovered_bytes = fs::read(pty.snapshot()).unwrap();
    drop(pty);

    let restarted = ProviderFixture::new(
        temporary.path().join("pty"),
        "pty",
        component("ST2_PTY_STATS_COMPONENT"),
        r#"{"topics":["lifecycle","metadata"]}"#,
        &format!(
            "pty-stats executable={:?} cwd={:?} deadline-ms=10000",
            executable,
            temporary.path()
        ),
        "dev.schickling.pty.snapshot.v1",
        &["lifecycle", "metadata", "runtime"],
    );
    let observed_after_restart = restarted.observe(recovered.digest);
    assert_eq!(
        observed_after_restart.status,
        ObserveReceiptStatus::SettledChanged
    );
    let restarted_snapshot: serde_json::Value =
        serde_json::from_slice(&fs::read(restarted.snapshot()).unwrap()).unwrap();
    assert_eq!(
        restarted_snapshot
            .get("schema")
            .and_then(serde_json::Value::as_str),
        Some("dev.schickling.pty.snapshot.v1")
    );
}

#[test]
fn supervisor_spawns_vista_capability_and_preserves_stable_snapshot() {
    let _guard = STATE_ENV.lock();
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };
    let executable = temporary.path().join("vista");
    write_executable(
        &executable,
        r#"#!/bin/sh
if [ "$#" -ne 6 ] || [ "$1" != artifact ] || [ "$2" != get ] || [ "$3" != release-notes ] || [ "$4" != v7 ] || [ "$5" != --output ] || [ "$6" != json ]; then
  exit 64
fi
printf '%s\n' '{"schemaVersion":1,"uri":"vista://release-notes/v7","slug":"release-notes","version":7,"author":"agent","timestamp":"2026-09-02T10:00:00Z","changeSummary":"created","parent":null,"retired":false,"state":"ready","canonicalUrl":"https://vista.example/release-notes/v7"}'
"#,
    );
    let selector = r#"{"topics":["ready","updated","failed","expired"]}"#;
    let vista = ProviderFixture::new_with_uri(
        temporary.path().join("catalog"),
        "vista",
        "vista://release-notes/v7",
        component("ST2_VISTA_COMPONENT"),
        selector,
        &format!(
            "vista executable={:?} cwd={:?} deadline-ms=10000",
            executable,
            temporary.path()
        ),
        "dev.schickling.vista.snapshot.v1",
        &["ready", "updated", "failed", "expired"],
    );

    let first = vista.observe(None);
    assert_eq!(
        first.status,
        ObserveReceiptStatus::SettledChanged,
        "{first:?}"
    );
    let first_bytes = fs::read(vista.snapshot()).unwrap();
    let snapshot: serde_json::Value = serde_json::from_slice(&first_bytes).unwrap();
    assert_eq!(
        snapshot.get("schema").and_then(serde_json::Value::as_str),
        Some("dev.schickling.vista.snapshot.v1")
    );
    assert!(snapshot.get("observedAt").is_some());
    let replay = vista.observe(first.digest);
    assert_eq!(replay.status, ObserveReceiptStatus::SettledUnchanged);
    assert_eq!(fs::read(vista.snapshot()).unwrap(), first_bytes);
}

#[test]
fn production_component_preserves_resync_filter_catch_up_and_scope_isolation() {
    let _guard = STATE_ENV.lock();
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let primary_control = temporary.path().join("primary-control");
    fs::create_dir_all(&primary_control).unwrap();
    let primary_payload = primary_control.join("payload.json");
    fs::write(
        &primary_payload,
        r#"[{"name":"subject","status":"exited","generation":1}]"#,
    )
    .unwrap();
    let primary_executable = primary_control.join("fixture-pty");
    write_executable(
        &primary_executable,
        "#!/bin/sh\nread payload < payload.json\nprintf '%s\\n' \"$payload\"\n",
    );
    let primary = ProviderFixture::new(
        temporary.path().join("primary"),
        "pty",
        component("ST2_PTY_STATS_COMPONENT"),
        r#"{"topics":["lifecycle"]}"#,
        &format!(
            "pty-stats executable={:?} cwd={:?} deadline-ms=10000",
            primary_executable, primary_control
        ),
        "dev.schickling.pty.snapshot.v1",
        &["lifecycle", "metadata", "runtime"],
    );
    let first = primary.observe(None);
    assert_eq!(
        first.status,
        ObserveReceiptStatus::SettledChanged,
        "{first:?}"
    );
    let first_snapshot = fs::read(primary.snapshot()).unwrap();
    let first_inbox = wait_until("first resync record", || {
        let inbox = resync_inbox(&primary.agent);
        (!inbox.is_empty()).then_some(inbox)
    });
    assert_eq!(first_inbox.len(), 1);
    assert!(
        first_inbox[0].contains("subject: observed · session=subject; state=exited [lifecycle]")
    );
    assert!(first_inbox[0].contains(
        r#""facts":[{"key":"session","after":"subject"},{"key":"state","after":"exited"}]"#
    ));

    let equal = primary.observe(first.digest);
    assert_eq!(equal.status, ObserveReceiptStatus::SettledUnchanged);
    assert_eq!(fs::read(primary.snapshot()).unwrap(), first_snapshot);
    assert_eq!(resync_inbox(&primary.agent), first_inbox);

    fs::write(
        &primary_payload,
        r#"[{"name":"subject","status":"exited","generation":2}]"#,
    )
    .unwrap();
    primary.rewrite_selector(r#"{"topics":["metadata"]}"#);
    primary.refresh();
    let filtered = primary.observe(first.digest);
    assert_eq!(filtered.status, ObserveReceiptStatus::SettledChanged);
    assert_ne!(fs::read(primary.snapshot()).unwrap(), first_snapshot);
    assert_eq!(
        resync_inbox(&primary.agent),
        first_inbox,
        "a selector-excluded lifecycle transition does not invalidate the binding"
    );

    primary.rewrite_selector(r#"{"topics":["lifecycle"]}"#);
    primary.refresh();
    fs::write(
        &primary_payload,
        r#"[{"name":"subject","status":"exited","generation":3}]"#,
    )
    .unwrap();
    let catch_up_fifo = primary_control.join("catch-up.fifo");
    let fifo_c = CString::new(catch_up_fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: `fifo_c` is a live NUL-terminated pathname for this call.
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
    write_executable(
        &primary_executable,
        "#!/bin/sh\nread release < catch-up.fifo\nread payload < payload.json\nprintf '%s\\n' \"$payload\"\n",
    );
    let pending_request = primary.request(1, filtered.digest);
    let pending_client = submit_request(&primary.root, &primary.host, &pending_request).unwrap();
    wait_receipt_status(
        &primary,
        &pending_request.request_id,
        ObserveReceiptStatus::Accepted,
    );
    fs::remove_file(primary.owner_binding_path()).unwrap();
    release_fifo(&catch_up_fifo);
    let pending = pending_client
        .wait_for_terminal(WAIT)
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(pending.status, ObserveReceiptStatus::SettledChanged);
    assert_eq!(resync_inbox(&primary.agent), first_inbox);
    st2::event::publish_owner_binding_for_test(&primary.root, &primary.host).unwrap();
    primary.refresh();
    let caught_up = wait_until("pending resync replay", || {
        let inbox = resync_inbox(&primary.agent);
        (inbox != first_inbox).then_some(inbox)
    });
    assert_eq!(caught_up.len(), 1);
    let caught_up_tree = file_tree(&primary.agent.join("resources"));
    primary.refresh();
    assert_eq!(
        file_tree(&primary.agent.join("resources")),
        caught_up_tree,
        "acknowledged catch-up does not replay"
    );

    let isolated_control = temporary.path().join("isolated-control");
    fs::create_dir_all(&isolated_control).unwrap();
    let isolated_payload = isolated_control.join("payload.json");
    fs::write(
        &isolated_payload,
        r#"[{"name":"subject","status":"exited","generation":10}]"#,
    )
    .unwrap();
    let isolated_executable = isolated_control.join("fixture-pty");
    write_executable(
        &isolated_executable,
        "#!/bin/sh\nread payload < payload.json\nprintf '%s\\n' \"$payload\"\n",
    );
    let isolated = ProviderFixture::new(
        temporary.path().join("isolated"),
        "pty",
        component("ST2_PTY_STATS_COMPONENT"),
        r#"{"topics":["lifecycle"]}"#,
        &format!(
            "pty-stats executable={:?} cwd={:?} deadline-ms=10000",
            isolated_executable, isolated_control
        ),
        "dev.schickling.pty.snapshot.v1",
        &["lifecycle", "metadata", "runtime"],
    );
    let isolated_first = isolated.observe(None);
    assert_eq!(isolated_first.status, ObserveReceiptStatus::SettledChanged);
    let isolated_before = file_tree(&isolated.agent.join("resources"));
    drop(primary);
    assert_eq!(
        file_tree(&isolated.agent.join("resources")),
        isolated_before,
        "dropping one catalog scope must not mutate another"
    );
    fs::write(
        &isolated_payload,
        r#"[{"name":"subject","status":"exited","generation":11}]"#,
    )
    .unwrap();
    let isolated_second = isolated.observe(isolated_first.digest);
    assert_eq!(isolated_second.status, ObserveReceiptStatus::SettledChanged);
    assert_ne!(
        file_tree(&isolated.agent.join("resources")),
        isolated_before,
        "the surviving catalog remains live"
    );
}

#[test]
fn production_demand_jobs_coalesce_queue_disconnect_and_fence_generation() {
    let _guard = STATE_ENV.lock();
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };
    let control = temporary.path().join("control");
    fs::create_dir_all(&control).unwrap();
    fs::write(
        control.join("payload.json"),
        r#"[{"name":"subject","status":"exited","generation":1}]"#,
    )
    .unwrap();
    let fifo = control.join("release.fifo");
    let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: `fifo_c` is a live NUL-terminated pathname for this call.
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
    let executable = control.join("fixture-pty");
    write_executable(
        &executable,
        "#!/bin/sh\nread release < release.fifo\nread payload < payload.json\nprintf '%s\\n' \"$payload\"\n",
    );
    let fixture = ProviderFixture::new(
        temporary.path().join("catalog"),
        "pty",
        component("ST2_PTY_STATS_COMPONENT"),
        r#"{"topics":["lifecycle"]}"#,
        &format!(
            "pty-stats executable={:?} cwd={:?} deadline-ms=10000",
            executable, control
        ),
        "dev.schickling.pty.snapshot.v1",
        &["lifecycle", "metadata", "runtime"],
    );

    let leading = fixture.request(1, None);
    let leading_client = submit_request(&fixture.root, &fixture.host, &leading).unwrap();
    let leading_accepted = wait_receipt_status(
        &fixture,
        &leading.request_id,
        ObserveReceiptStatus::Accepted,
    );
    assert_eq!(leading_accepted.demand_watermark, Some(1));
    let trailing_a = fixture.request(1, None);
    let trailing_b = fixture.request(1, None);
    let trailing_a_path = fixture
        .observe_request_dir()
        .join(format!("{}.json", trailing_a.request_id));
    let trailing_b_path = fixture
        .observe_request_dir()
        .join(format!("{}.json", trailing_b.request_id));
    let trailing_a_client = submit_request(&fixture.root, &fixture.host, &trailing_a).unwrap();
    let trailing_b_client = submit_request(&fixture.root, &fixture.host, &trailing_b).unwrap();
    wait_until("coalesced trailing requests", || {
        (trailing_a_path.is_file() && trailing_b_path.is_file()).then_some(())
    });
    release_fifo(&fifo);
    assert_eq!(
        leading_client
            .wait_for_terminal(WAIT)
            .unwrap()
            .receipt
            .unwrap()
            .demand_watermark,
        Some(1)
    );
    for request in [&trailing_a, &trailing_b] {
        let accepted = wait_receipt_status(
            &fixture,
            &request.request_id,
            ObserveReceiptStatus::Accepted,
        );
        assert_eq!(accepted.demand_watermark, Some(2));
    }
    release_fifo(&fifo);
    for client in [trailing_a_client, trailing_b_client] {
        let receipt = client.wait_for_terminal(WAIT).unwrap().receipt.unwrap();
        assert_eq!(receipt.demand_watermark, Some(2));
    }
    wait_until("durable coalesced request cleanup", || {
        (!trailing_a_path.exists() && !trailing_b_path.exists()).then_some(())
    });

    let disconnected = fixture.request(1, None);
    let disconnected_id = disconnected.request_id.clone();
    let disconnected_client = submit_request(&fixture.root, &fixture.host, &disconnected).unwrap();
    wait_receipt_status(&fixture, &disconnected_id, ObserveReceiptStatus::Accepted);
    drop(disconnected_client);
    release_fifo(&fifo);
    let disconnected_receipt = wait_until("receipt after client disconnect", || {
        st2::resource_observe::read_receipt(&fixture.observe_receipt_dir(), &disconnected_id)
            .ok()
            .flatten()
            .filter(|receipt| receipt.status.is_terminal())
    });
    assert!(
        matches!(
            disconnected_receipt.status,
            ObserveReceiptStatus::SettledChanged | ObserveReceiptStatus::SettledUnchanged
        ),
        "{disconnected_receipt:?}"
    );

    let future = fixture.request(2, disconnected_receipt.digest);
    let future_path = fixture
        .observe_request_dir()
        .join(format!("{}.json", future.request_id));
    let future_client = submit_request(&fixture.root, &fixture.host, &future).unwrap();
    fixture.refresh_generation(1);
    assert!(future_path.is_file());
    assert!(
        st2::resource_observe::read_receipt(&fixture.observe_receipt_dir(), &future.request_id,)
            .unwrap()
            .is_none()
    );
    fixture.refresh_generation(2);
    wait_receipt_status(&fixture, &future.request_id, ObserveReceiptStatus::Accepted);
    release_fifo(&fifo);
    // A generation change discards the provider's semantic cache, so the fenced request may
    // republish the same source with a new observation timestamp.
    let future_receipt = future_client
        .wait_for_terminal(WAIT)
        .unwrap()
        .receipt
        .unwrap();
    assert!(
        matches!(
            future_receipt.status,
            ObserveReceiptStatus::SettledChanged | ObserveReceiptStatus::SettledUnchanged
        ),
        "{future_receipt:?}"
    );

    let stale = fixture.request(2, None);
    let stale_client = submit_request(&fixture.root, &fixture.host, &stale).unwrap();
    wait_receipt_status(&fixture, &stale.request_id, ObserveReceiptStatus::Accepted);
    fixture.refresh_generation(3);
    assert_eq!(
        stale_client
            .wait_for_terminal(WAIT)
            .unwrap()
            .receipt
            .unwrap()
            .status,
        ObserveReceiptStatus::StaleGeneration
    );

    let config_path = st2::catalog::config_path(&fixture.root);
    let without_demand = fs::read_to_string(&config_path)
        .unwrap()
        .replace("    demand #true\n", "    demand #false\n");
    fs::write(config_path, without_demand).unwrap();
    fixture.refresh_generation(4);
    let absent = fixture.request(4, None);
    let absent_receipt = submit_request(&fixture.root, &fixture.host, &absent)
        .unwrap()
        .wait_for_terminal(WAIT)
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(absent_receipt.status, ObserveReceiptStatus::AbsentBinding);
}

#[test]
fn durable_admission_retains_the_256_request_boundary_without_a_runtime() {
    use st2::resource_observe::{MAX_PENDING_OBSERVE_REQUESTS, ObserveAdmissionBackpressure};
    let _guard = STATE_ENV.lock();

    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };
    let root = temporary.path();
    let host = "capacity";
    st2::event::publish_owner_binding_for_test(root, host).unwrap();
    let barrier = Arc::new(Barrier::new(MAX_PENDING_OBSERVE_REQUESTS + 1));
    let submissions = (0..=MAX_PENDING_OBSERVE_REQUESTS)
        .map(|index| {
            let root = root.to_path_buf();
            let host = host.to_owned();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let request = ObserveRequest::new(
                    format!("{host}.worker-{index}"),
                    "observed".into(),
                    Some(1),
                    None,
                )
                .unwrap();
                barrier.wait();
                submit_request(&root, &host, &request).map_err(|error| {
                    error
                        .downcast_ref::<ObserveAdmissionBackpressure>()
                        .is_some_and(|pressure| pressure.limit() == MAX_PENDING_OBSERVE_REQUESTS)
                })
            })
        })
        .collect::<Vec<_>>();
    let results = submissions
        .into_iter()
        .map(|submission| submission.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 256);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(true)))
            .count(),
        1
    );
}

fn release_fifo(path: &Path) {
    let mut writer = OpenOptions::new().write(true).open(path).unwrap();
    writer.write_all(b"go\n").unwrap();
}

fn wait_receipt_status(
    fixture: &ProviderFixture,
    request_id: &str,
    status: ObserveReceiptStatus,
) -> ObserveReceipt {
    wait_until(status.wire_str(), || {
        st2::resource_observe::read_receipt(&fixture.observe_receipt_dir(), request_id)
            .ok()
            .flatten()
            .filter(|receipt| receipt.status == status)
    })
}

fn wait_until<T>(description: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        std::thread::yield_now();
    }
}

fn resync_inbox(agent: &Path) -> Vec<String> {
    let mut records = fs::read_dir(agent.join("resources/inbox"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| fs::read_to_string(entry.path()).ok())
        .filter(|contents| contents.contains("stream: resync"))
        .collect::<Vec<_>>();
    records.sort();
    records
}

fn file_tree(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn visit(base: &Path, directory: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
        let Ok(entries) = fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                visit(base, &path, files);
            } else if file_type.is_file() {
                files.push((
                    path.strip_prefix(base).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                ));
            }
        }
    }
    let mut files = Vec::new();
    visit(root, root, &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn component(variable: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(variable).unwrap_or_else(|| panic!("{variable} is not set")))
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

struct ProviderFixture {
    root: PathBuf,
    agent: PathBuf,
    host: String,
    uri: String,
    selector: Mutex<String>,
    supervisor: ResourceProfileSupervisor,
}

impl ProviderFixture {
    #[allow(clippy::too_many_arguments)]
    fn new(
        root: PathBuf,
        scheme: &str,
        component: PathBuf,
        selector: &str,
        capability: &str,
        schema_id: &str,
        topics: &[&str],
    ) -> Self {
        Self::new_with_uri(
            root,
            scheme,
            &format!("{scheme}:subject"),
            component,
            selector,
            capability,
            schema_id,
            topics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_uri(
        root: PathBuf,
        scheme: &str,
        uri: &str,
        component: PathBuf,
        selector: &str,
        capability: &str,
        schema_id: &str,
        topics: &[&str],
    ) -> Self {
        let host = "e2e".to_owned();
        let agent = root.join("agents/e2e/worker");
        fs::create_dir_all(agent.join("resources")).unwrap();
        fs::create_dir_all(root.join("providers")).unwrap();
        let installed_component = root.join("providers/provider.component.wasm");
        if installed_component.exists() {
            fs::remove_file(&installed_component).unwrap();
        }
        fs::copy(component, installed_component).unwrap();
        fs::write(
            root.join("resolver.wasm"),
            observable_resolver_wasm(schema_id, topics, selector),
        )
        .unwrap();
        fs::write(
            st2::catalog::config_path(&root),
            format!(
                "profile {scheme:?} {{\n  wasm \"resolver.wasm\"\n  class \"immediate\"\n  runtime {{\n    component \"providers/provider.component.wasm\"\n    demand #true\n    {capability}\n  }}\n}}\n"
            ),
        )
        .unwrap();
        write_agent(&agent, &host, uri, selector);
        st2::event::publish_owner_binding_for_test(&root, &host).unwrap();
        let supervisor = ResourceProfileSupervisor::new(root.clone(), host.clone()).unwrap();
        let fixture = Self {
            root,
            agent,
            host,
            uri: uri.to_owned(),
            selector: Mutex::new(selector.to_owned()),
            supervisor,
        };
        fixture.refresh();
        fixture
    }

    fn refresh(&self) {
        self.refresh_generation(1);
    }

    fn refresh_generation(&self, generation: u64) {
        let (config, profiles) = st2::catalog::declared_profile_catalog(&self.root).unwrap();
        let discovery = st2::discover_strict(&self.root);
        assert!(discovery.errors.is_empty(), "{:?}", discovery.errors);
        let report =
            self.supervisor
                .refresh(&config, &profiles, Some(generation), &discovery.specs);
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    }

    fn rewrite_selector(&self, selector: &str) {
        *self.selector.lock() = selector.to_owned();
        write_agent(&self.agent, &self.host, &self.uri, selector);
    }

    fn observe(&self, prior: Option<st2::resource_profile::SnapshotDigest>) -> ObserveReceipt {
        self.observe_generation(1, prior)
    }

    fn observe_generation(
        &self,
        generation: u64,
        prior: Option<st2::resource_profile::SnapshotDigest>,
    ) -> ObserveReceipt {
        let request = ObserveRequest::new(
            format!("{}.worker", self.host),
            "observed".into(),
            Some(generation),
            prior,
        )
        .unwrap();
        let wait = submit_request(&self.root, &self.host, &request)
            .unwrap()
            .wait_for_terminal(WAIT)
            .unwrap();
        assert!(!wait.timed_out);
        wait.receipt.unwrap()
    }

    fn request(
        &self,
        generation: u64,
        prior: Option<st2::resource_profile::SnapshotDigest>,
    ) -> ObserveRequest {
        ObserveRequest::new(
            format!("{}.worker", self.host),
            "observed".into(),
            Some(generation),
            prior,
        )
        .unwrap()
    }

    fn snapshot(&self) -> PathBuf {
        self.agent.join("resources/snapshot.json")
    }

    fn observe_request_dir(&self) -> PathBuf {
        st2::park::SupervisorScope::current(&self.root, &self.host)
            .unwrap()
            .park_dir()
            .parent()
            .unwrap()
            .join("observe-requests")
    }

    fn observe_receipt_dir(&self) -> PathBuf {
        st2::park::SupervisorScope::current(&self.root, &self.host)
            .unwrap()
            .park_dir()
            .parent()
            .unwrap()
            .join("observe-receipts")
    }

    fn owner_binding_path(&self) -> PathBuf {
        st2::park::SupervisorScope::current(&self.root, &self.host)
            .unwrap()
            .park_dir()
            .parent()
            .unwrap()
            .join("stream-owner.json")
    }
}

fn write_agent(agent: &Path, host: &str, uri: &str, selector: &str) {
    fs::write(
        agent.join("agent.kdl"),
        format!(
            "agent \"worker\" {{\n  host {host:?}\n  command \"true\"\n  resource \"observed\" uri={uri:?} reason=\"Observed state.\" selector=#\"{selector}\"#\n}}\n"
        ),
    )
    .unwrap();
}

fn observable_resolver_wasm(schema_id: &str, topics: &[&str], selector: &str) -> Vec<u8> {
    let selector_value: serde_json::Value = serde_json::from_str(selector).unwrap();
    let descriptor = serde_json::to_vec(&serde_json::json!({
        "abiVersion": 3,
        "capabilities": ["resolve", "read", "observe"],
        "selectorSchema": { "type": "object", "additionalProperties": true },
        "defaultSelector": selector_value,
        "topics": topics.iter().map(|name| serde_json::json!({"name": name})).collect::<Vec<_>>(),
        "runtime": {"topology": "shared"},
        "snapshot": {"mediaType": "application/json", "schemaId": schema_id}
    }))
    .unwrap();
    const RESOLUTION: &[u8] = br#"{"path":"resources/snapshot.json","class":"observable"}"#;
    const DESCRIPTOR_PTR: i64 = 1024;
    const RESOLUTION_PTR: i64 = 8192;
    let mut module = b"\0asm\x01\0\0\0".to_vec();
    let mut types = vec![3, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 4];
    types.extend([0x7f, 0x7f, 0x7f, 0x7f, 1, 0x7e]);
    types.extend([0x60, 0, 1, 0x7e]);
    push_section(&mut module, 1, &types);
    push_section(&mut module, 3, &[3, 0, 1, 2]);
    push_section(&mut module, 5, &[1, 0, 1]);
    let mut exports = vec![4];
    push_export(&mut exports, "memory", 0x02, 0);
    push_export(&mut exports, "alloc", 0x00, 0);
    push_export(&mut exports, "resolve", 0x00, 1);
    push_export(&mut exports, "describe", 0x00, 2);
    push_section(&mut module, 7, &exports);
    let mut code = vec![3];
    push_body(&mut code, 0x41, 16384);
    push_body(
        &mut code,
        0x42,
        (RESOLUTION_PTR << 32) | RESOLUTION.len() as i64,
    );
    push_body(
        &mut code,
        0x42,
        (DESCRIPTOR_PTR << 32) | descriptor.len() as i64,
    );
    push_section(&mut module, 10, &code);
    let mut data = vec![2];
    push_data(&mut data, DESCRIPTOR_PTR, &descriptor);
    push_data(&mut data, RESOLUTION_PTR, RESOLUTION);
    push_section(&mut module, 11, &data);
    module
}

fn push_section(module: &mut Vec<u8>, id: u8, payload: &[u8]) {
    module.push(id);
    push_u32(module, payload.len() as u32);
    module.extend_from_slice(payload);
}

fn push_export(section: &mut Vec<u8>, name: &str, kind: u8, index: u32) {
    push_u32(section, name.len() as u32);
    section.extend_from_slice(name.as_bytes());
    section.push(kind);
    push_u32(section, index);
}

fn push_body(section: &mut Vec<u8>, constant_opcode: u8, value: i64) {
    let mut body = vec![0, constant_opcode];
    push_i64(&mut body, value);
    body.push(0x0b);
    push_u32(section, body.len() as u32);
    section.extend(body);
}

fn push_data(section: &mut Vec<u8>, offset: i64, bytes: &[u8]) {
    section.extend([0, 0x41]);
    push_i64(section, offset);
    section.push(0x0b);
    push_u32(section, bytes.len() as u32);
    section.extend_from_slice(bytes);
}

fn push_u32(bytes: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn push_i64(bytes: &mut Vec<u8>, mut value: i64) {
    loop {
        let byte = (value as u8) & 0x7f;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        bytes.push(if done { byte } else { byte | 0x80 });
        if done {
            return;
        }
    }
}
