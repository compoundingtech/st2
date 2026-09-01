#![cfg(all(unix, feature = "wasm-resolver"))]

use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use st2::resource_observe::{
    MAX_PENDING_OBSERVE_REQUESTS, ObserveAdmissionBackpressure, ObserveReceiptStatus,
    ObserveRequest,
};
use st2::resource_profile::{
    BindingId, HostMessage, ObservationResult, Publication, RegistrationToken, ResourceFact,
    RuntimeHealthState, RuntimeMessage, RuntimeOwner, SnapshotBytes, SnapshotDigest,
    decode_host_line, encode_runtime_line,
};
use st2::resource_profile_supervisor::ResourceProfileSupervisor;

const SCHEME: &str = "dev.example.observable";
const SCHEMA_ID: &str = "dev.example.observable.snapshot.v1";
static TEST_ENV_LOCK: Mutex<()> = Mutex::new(());
const MEDIA_TYPE: &str = "application/json";

#[derive(Clone)]
struct Registration {
    owner: RuntimeOwner,
    binding_id: BindingId,
    registration: RegistrationToken,
}

#[derive(Clone)]
struct Observation {
    registration: Registration,
    demand_watermark: u64,
}

struct RuntimeControl {
    register_path: PathBuf,
    observe_path: PathBuf,
    output: File,
}

impl RuntimeControl {
    fn registration(&self) -> Registration {
        wait_until("runtime register message", || {
            let line = fs::read(&self.register_path).ok()?;
            let message = decode_host_line(&line).ok()?;
            let HostMessage::Register {
                owner,
                binding_id,
                registration,
                ..
            } = message
            else {
                return None;
            };
            Some(Registration {
                owner,
                binding_id,
                registration,
            })
        })
    }

    fn observations(&self) -> Vec<Observation> {
        fs::read_to_string(&self.observe_path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| {
                let message = decode_host_line(format!("{line}\n").as_bytes()).ok()?;
                let HostMessage::Observe {
                    owner,
                    binding_id,
                    registration,
                    demand_watermark,
                } = message
                else {
                    return None;
                };
                Some(Observation {
                    registration: Registration {
                        owner,
                        binding_id,
                        registration,
                    },
                    demand_watermark,
                })
            })
            .collect()
    }

    fn wait_for_observation(&self, count: usize) -> Observation {
        wait_until("runtime observe message", || {
            self.observations().get(count.saturating_sub(1)).cloned()
        })
    }

    fn unchanged(&self, observation: &Observation) {
        self.result(observation, ObservationResult::Unchanged);
    }

    fn failed(&self, observation: &Observation, diagnostic: &str) {
        self.result(
            observation,
            ObservationResult::Failed {
                diagnostic: Some(diagnostic.to_owned()),
            },
        );
    }

    fn publish_observation(
        &self,
        observation: &Observation,
        bytes: &[u8],
        topics: &[&str],
        fact: &str,
    ) {
        self.result(
            observation,
            ObservationResult::Published {
                publication: publication(bytes, topics, fact),
            },
        );
    }

    fn result(&self, observation: &Observation, result: ObservationResult) {
        let registration = &observation.registration;
        let message = RuntimeMessage::ObservationResult {
            owner: registration.owner.clone(),
            binding_id: registration.binding_id.clone(),
            registration: registration.registration.clone(),
            demand_watermark: observation.demand_watermark,
            result,
        };
        let mut output = &self.output;
        output
            .write_all(&encode_runtime_line(&message).unwrap())
            .unwrap();
        output.flush().unwrap();
    }

    fn publish(
        &self,
        registration: &Registration,
        bytes: &[u8],
        topics: &[&str],
        health_marker: &str,
    ) {
        let publication = RuntimeMessage::Publish {
            owner: registration.owner.clone(),
            binding_id: registration.binding_id.clone(),
            registration: registration.registration.clone(),
            publication: publication(bytes, topics, health_marker),
        };
        let health = RuntimeMessage::Health {
            owner: registration.owner.clone(),
            binding_id: Some(registration.binding_id.clone()),
            registration: Some(registration.registration.clone()),
            state: RuntimeHealthState::Ready,
            detail: Some(health_marker.to_owned()),
        };
        let mut output = &self.output;
        output
            .write_all(&encode_runtime_line(&publication).unwrap())
            .unwrap();
        output
            .write_all(&encode_runtime_line(&health).unwrap())
            .unwrap();
        output.flush().unwrap();
    }
}

fn publication(bytes: &[u8], topics: &[&str], fact: &str) -> Publication {
    Publication {
        schema_id: SCHEMA_ID.to_owned(),
        media_type: MEDIA_TYPE.to_owned(),
        bytes: SnapshotBytes::new(bytes.to_vec()).unwrap(),
        topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
        facts: Some(vec![ResourceFact::current("revision", fact).unwrap()]),
    }
}

struct CatalogFixture {
    root: PathBuf,
    host: String,
    agent_dir: PathBuf,
    runtime: RuntimeControl,
}

impl CatalogFixture {
    fn new(root: PathBuf, host: &str) -> Self {
        fs::create_dir_all(&root).unwrap();
        let agent_dir = root.join("agents").join(host).join("worker");
        fs::create_dir_all(&agent_dir).unwrap();
        fs::create_dir_all(agent_dir.join("resources")).unwrap();
        fs::write(
            agent_dir.join("agent.kdl"),
            format!(
                r##"agent "worker" {{
  host "{host}"
  command "true"
  resource "observed" uri="{SCHEME}://subject" reason="Observed state." selector=#"{{"topics":["selected"]}}"#
}}
"##,
            ),
        )
        .unwrap();

        let resolver = root.join("observable-resolver.wasm");
        fs::write(&resolver, observable_resolver_wasm()).unwrap();
        let control_dir = root.join("runtime-control");
        fs::create_dir_all(&control_dir).unwrap();
        let register_path = control_dir.join("register.ndjson");
        let observe_path = control_dir.join("observe.ndjson");
        let fifo_path = control_dir.join("runtime-output.fifo");
        let fifo = CString::new(fifo_path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        let output = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fifo_path)
            .unwrap();

        let runtime = root.join("fake-observable-runtime");
        fs::write(
            &runtime,
            "#!/bin/sh\nset -eu\nexec 3<&0\n(\n  while IFS= read -r frame; do\n    case \"$frame\" in\n      *'\"type\":\"register\"'*) printf '%s\\n' \"$frame\" > \"$1/register.ndjson\" ;;\n      *'\"type\":\"observe\"'*) printf '%s\\n' \"$frame\" >> \"$1/observe.ndjson\" ;;\n    esac\n  done\n) <&3 &\nexec cat \"$1/runtime-output.fifo\"\n",
        )
        .unwrap();
        fs::set_permissions(&runtime, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(
            st2::catalog::config_path(&root),
            format!(
                r#"profile "{SCHEME}" {{
  wasm "observable-resolver.wasm"
  class "immediate"
  runtime {{
    argv "{}" "{}"
    capability "demand"
  }}
}}
"#,
                runtime.display(),
                control_dir.display()
            ),
        )
        .unwrap();

        Self {
            root,
            host: host.to_owned(),
            agent_dir,
            runtime: RuntimeControl {
                register_path,
                observe_path,
                output,
            },
        }
    }

    fn supervisor(&self) -> ResourceProfileSupervisor {
        let supervisor =
            ResourceProfileSupervisor::new(self.root.clone(), self.host.clone()).unwrap();
        self.refresh(&supervisor);
        supervisor
    }

    fn refresh(&self, supervisor: &ResourceProfileSupervisor) {
        self.refresh_generation(supervisor, 1);
    }

    fn refresh_generation(&self, supervisor: &ResourceProfileSupervisor, generation: u64) {
        let (config, profiles) = st2::catalog::declared_profile_catalog(&self.root).unwrap();
        let discovery = st2::discover_strict(&self.root);
        assert!(
            discovery.errors.is_empty(),
            "fixture catalog must be valid: {:?}",
            discovery.errors
        );
        let report = supervisor.refresh(&config, &profiles, Some(generation), &discovery.specs);
        assert!(
            report.warnings.is_empty(),
            "Resource Profile refresh warnings: {:?}",
            report.warnings
        );
    }

    fn snapshot_path(&self) -> PathBuf {
        self.agent_dir.join("resources/snapshot.json")
    }

    fn owner_binding_path(&self) -> PathBuf {
        let park_dir = st2::park::SupervisorScope::current(&self.root, &self.host)
            .unwrap()
            .park_dir();
        park_dir.parent().unwrap().join("stream-owner.json")
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

    fn request(&self) -> ObserveRequest {
        ObserveRequest::new(
            format!("{}.worker", self.host),
            "observed".to_owned(),
            Some(1),
            None,
        )
        .unwrap()
    }
}

#[test]
fn observable_publication_reaches_builtin_resync_with_filter_catch_up_and_scope_isolation() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    unsafe { std::env::set_var("XDG_STATE_HOME", &state) };

    let primary = CatalogFixture::new(temporary.path().join("catalog-primary"), "alpha");
    st2::event::publish_owner_binding_for_test(&primary.root, &primary.host).unwrap();
    let primary_supervisor = primary.supervisor();
    let primary_registration = primary.runtime.registration();

    let first = br#"{"revision":1}"#;
    primary
        .runtime
        .publish(&primary_registration, first, &["selected"], "primary-first");
    wait_for_health(&primary_supervisor, "primary-first");
    assert_eq!(fs::read(primary.snapshot_path()).unwrap(), first);
    let first_inbox = resync_inbox(&primary.agent_dir);
    assert_eq!(
        first_inbox.len(),
        1,
        "the first selected publication must create one built-in resync record"
    );
    assert!(
        first_inbox[0].contains("subject: observed · revision=primary-first [selected]"),
        "{}",
        first_inbox[0]
    );
    assert!(
        first_inbox[0].contains(r#""facts":[{"key":"revision","after":"primary-first"}]"#),
        "{}",
        first_inbox[0]
    );

    primary
        .runtime
        .publish(&primary_registration, first, &["selected"], "primary-equal");
    wait_for_health(&primary_supervisor, "primary-equal");
    assert_eq!(fs::read(primary.snapshot_path()).unwrap(), first);
    assert_eq!(
        resync_inbox(&primary.agent_dir),
        first_inbox,
        "an equal publication must not invalidate the inbox"
    );

    let filtered = br#"{"revision":2}"#;
    primary.runtime.publish(
        &primary_registration,
        filtered,
        &["ignored"],
        "primary-filtered",
    );
    wait_for_health(&primary_supervisor, "primary-filtered");
    assert_eq!(fs::read(primary.snapshot_path()).unwrap(), filtered);
    assert_eq!(
        resync_inbox(&primary.agent_dir),
        first_inbox,
        "an unselected topic must update the canonical snapshot without invalidation"
    );

    fs::remove_file(primary.owner_binding_path()).unwrap();
    let caught_up = br#"{"revision":3}"#;
    primary.runtime.publish(
        &primary_registration,
        caught_up,
        &["selected"],
        "delivery-unavailable",
    );
    wait_until("failed runtime after unavailable delivery", || {
        (fs::read(primary.snapshot_path()).ok().as_deref() == Some(caught_up)
            && primary_supervisor.health().is_empty())
        .then_some(())
    });
    assert_eq!(
        resync_inbox(&primary.agent_dir),
        first_inbox,
        "failed delivery must remain pending rather than forging a local inbox write"
    );

    st2::event::publish_owner_binding_for_test(&primary.root, &primary.host).unwrap();
    primary.refresh(&primary_supervisor);
    let caught_up_inbox = resync_inbox(&primary.agent_dir);
    assert_eq!(
        caught_up_inbox.len(),
        1,
        "supersession keeps one unread head"
    );
    assert_ne!(
        caught_up_inbox, first_inbox,
        "restoring delivery must replace the old head with the pending digest"
    );
    assert!(
        caught_up_inbox[0].contains("revision=delivery-unavailable"),
        "{}",
        caught_up_inbox[0]
    );
    let caught_up_projection = file_tree(&primary.agent_dir.join("resources"));
    primary.refresh(&primary_supervisor);
    assert_eq!(
        file_tree(&primary.agent_dir.join("resources")),
        caught_up_projection,
        "an acknowledged catch-up must not replay on a later equal refresh"
    );

    let isolated = CatalogFixture::new(temporary.path().join("catalog-isolated"), "beta");
    st2::event::publish_owner_binding_for_test(&isolated.root, &isolated.host).unwrap();
    let isolated_supervisor = isolated.supervisor();
    let isolated_registration = isolated.runtime.registration();
    let isolated_first = br#"{"catalog":"isolated","revision":1}"#;
    isolated.runtime.publish(
        &isolated_registration,
        isolated_first,
        &["selected"],
        "isolated-first",
    );
    wait_for_health(&isolated_supervisor, "isolated-first");
    assert_eq!(fs::read(isolated.snapshot_path()).unwrap(), isolated_first);
    assert_eq!(
        resync_inbox(&isolated.agent_dir).len(),
        1,
        "the isolated scope must own a real snapshot and inbox head before teardown"
    );
    let isolated_before_drop = file_tree(&isolated.agent_dir.join("resources"));

    drop(primary_supervisor);
    assert_eq!(
        file_tree(&isolated.agent_dir.join("resources")),
        isolated_before_drop,
        "tearing down one catalog+host supervisor must not mutate another scope"
    );

    let isolated_second = br#"{"catalog":"isolated","revision":2}"#;
    isolated.runtime.publish(
        &isolated_registration,
        isolated_second,
        &["selected"],
        "isolated-after-primary-drop",
    );
    wait_for_health(&isolated_supervisor, "isolated-after-primary-drop");
    assert_eq!(
        fs::read(isolated.snapshot_path()).unwrap(),
        isolated_second,
        "the isolated supervisor must remain live after the other scope tears down"
    );
    assert_ne!(
        file_tree(&isolated.agent_dir.join("resources")),
        isolated_before_drop,
        "the surviving scope must still publish and invalidate"
    );
}
#[test]
fn demand_observation_settlement_matrix_is_atomic_and_preserves_facts() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let fixture = CatalogFixture::new(temporary.path().join("catalog"), "alpha");
    st2::event::publish_owner_binding_for_test(&fixture.root, &fixture.host).unwrap();
    let supervisor = fixture.supervisor();
    fixture.runtime.registration();

    let unchanged = fixture.request();
    let unchanged_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &unchanged).unwrap();
    let unchanged_observation = fixture.runtime.wait_for_observation(1);
    assert_eq!(unchanged_observation.demand_watermark, 1);
    fixture.runtime.unchanged(&unchanged_observation);
    let unchanged_receipt = unchanged_client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(
        unchanged_receipt.status,
        ObserveReceiptStatus::SettledUnchanged
    );
    assert_eq!(unchanged_receipt.demand_watermark, Some(1));
    assert_eq!(unchanged_receipt.digest, None);

    let changed = fixture.request();
    let changed_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &changed).unwrap();
    let changed_observation = fixture.runtime.wait_for_observation(2);
    let changed_bytes = br#"{"demand":"changed"}"#;
    assert_eq!(changed_observation.demand_watermark, 2);
    fixture.runtime.publish_observation(
        &changed_observation,
        changed_bytes,
        &["selected"],
        "demand-changed",
    );
    let changed_receipt = changed_client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(changed_receipt.status, ObserveReceiptStatus::SettledChanged);
    assert_eq!(
        changed_receipt.digest,
        Some(SnapshotDigest::of(changed_bytes))
    );
    assert_eq!(fs::read(fixture.snapshot_path()).unwrap(), changed_bytes);
    let changed_inbox = resync_inbox(&fixture.agent_dir);
    assert_eq!(changed_inbox.len(), 1);
    assert!(
        changed_inbox[0].contains(r#""facts":[{"key":"revision","after":"demand-changed"}]"#),
        "{}",
        changed_inbox[0]
    );

    let failed = fixture.request();
    let failed_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &failed).unwrap();
    let failed_observation = fixture.runtime.wait_for_observation(3);
    assert_eq!(failed_observation.demand_watermark, 3);
    fixture
        .runtime
        .failed(&failed_observation, "provider refused");
    let failed_receipt = failed_client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(failed_receipt.status, ObserveReceiptStatus::SettledFailed);
    assert_eq!(
        failed_receipt.diagnostic.as_deref(),
        Some("provider refused")
    );
    drop(supervisor);
}

#[test]
fn demand_observation_coalesces_and_fences_watermarks() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let fixture = CatalogFixture::new(temporary.path().join("catalog"), "alpha");
    st2::event::publish_owner_binding_for_test(&fixture.root, &fixture.host).unwrap();
    let supervisor = fixture.supervisor();
    fixture.runtime.registration();

    let leading = fixture.request();
    let leading_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &leading).unwrap();
    let leading_observation = fixture.runtime.wait_for_observation(1);
    let trailing_a = fixture.request();
    let trailing_a_path = fixture
        .observe_request_dir()
        .join(format!("{}.json", trailing_a.request_id));
    let trailing_a_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &trailing_a).unwrap();
    let trailing_b = fixture.request();
    let trailing_b_path = fixture
        .observe_request_dir()
        .join(format!("{}.json", trailing_b.request_id));
    let trailing_b_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &trailing_b).unwrap();
    wait_until("trailing request retention", || {
        (trailing_a_path.exists() && trailing_b_path.exists()).then_some(())
    });
    assert_eq!(
        fixture.runtime.observations().len(),
        1,
        "one in-flight demand permits only one coalesced trailing batch"
    );
    assert_eq!(leading_observation.demand_watermark, 1);
    fixture.runtime.unchanged(&leading_observation);
    let trailing_observation = fixture.runtime.wait_for_observation(2);
    assert_eq!(trailing_observation.demand_watermark, 2);
    fixture.runtime.unchanged(&trailing_observation);
    assert_eq!(
        leading_client
            .wait_for_terminal(Duration::from_secs(2))
            .unwrap()
            .receipt
            .unwrap()
            .demand_watermark,
        Some(1)
    );
    for client in [trailing_a_client, trailing_b_client] {
        assert_eq!(
            client
                .wait_for_terminal(Duration::from_secs(2))
                .unwrap()
                .receipt
                .unwrap()
                .demand_watermark,
            Some(2)
        );
    }
    wait_until("durable request cleanup after terminal receipts", || {
        (!trailing_a_path.exists() && !trailing_b_path.exists()).then_some(())
    });

    let mismatched = fixture.request();
    let mismatched_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &mismatched).unwrap();
    let mut mismatched_observation = fixture.runtime.wait_for_observation(3);
    assert_eq!(mismatched_observation.demand_watermark, 3);
    mismatched_observation.demand_watermark = 2;
    fixture.runtime.unchanged(&mismatched_observation);
    let mismatched_receipt = mismatched_client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(
        mismatched_receipt.status,
        ObserveReceiptStatus::ProviderUnavailable,
        "a stale watermark must fail the runtime rather than settle the current demand"
    );

    fixture.refresh(&supervisor);

    let stale = fixture.request();
    let stale_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &stale).unwrap();
    fixture.runtime.wait_for_observation(4);
    fixture.refresh_generation(&supervisor, 2);
    let stale_receipt = stale_client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(stale_receipt.status, ObserveReceiptStatus::StaleGeneration);
}

#[test]
fn demand_observation_survives_restart_disconnect_and_denies_missing_capability() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let fixture = CatalogFixture::new(temporary.path().join("catalog"), "alpha");
    st2::event::publish_owner_binding_for_test(&fixture.root, &fixture.host).unwrap();
    let initial = fixture.supervisor();
    fixture.runtime.registration();
    let restart_request = fixture.request();
    let restart_path = fixture
        .observe_request_dir()
        .join(format!("{}.json", restart_request.request_id));
    let restart_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &restart_request)
            .unwrap();
    let interrupted_observation = fixture.runtime.wait_for_observation(1);
    assert!(
        restart_path.is_file(),
        "enqueue must not remove the restart-recoverable durable request"
    );
    drop(initial);
    let restarted = fixture.supervisor();
    let restart_observation = fixture.runtime.wait_for_observation(2);
    assert_eq!(restart_observation.demand_watermark, 1);
    assert_ne!(
        restart_observation.registration.owner, interrupted_observation.registration.owner,
        "restart recovery must redispatch through the new runtime owner"
    );
    fixture.runtime.unchanged(&restart_observation);
    assert_eq!(
        restart_client
            .wait_for_terminal(Duration::from_secs(2))
            .unwrap()
            .receipt
            .unwrap()
            .status,
        ObserveReceiptStatus::SettledUnchanged
    );

    let disconnected = fixture.request();
    let disconnected_id = disconnected.request_id.clone();
    let disconnected_client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &disconnected).unwrap();
    let disconnected_observation = fixture.runtime.wait_for_observation(3);
    drop(disconnected_client);
    assert_eq!(disconnected_observation.demand_watermark, 2);
    fixture.runtime.unchanged(&disconnected_observation);
    let disconnected_receipt = wait_until("receipt after client disconnect", || {
        st2::resource_observe::read_receipt(&fixture.observe_receipt_dir(), &disconnected_id)
            .ok()
            .flatten()
            .filter(|receipt| receipt.status.is_terminal())
    });
    assert_eq!(
        disconnected_receipt.status,
        ObserveReceiptStatus::SettledUnchanged
    );

    let config_path = st2::catalog::config_path(&fixture.root);
    let without_demand = fs::read_to_string(&config_path)
        .unwrap()
        .replace("    capability \"demand\"\n", "");
    fs::write(&config_path, without_demand).unwrap();
    fixture.refresh_generation(&restarted, 2);
    let gated = ObserveRequest::new(
        format!("{}.worker", fixture.host),
        "observed".to_owned(),
        None,
        None,
    )
    .unwrap();
    let before_gate = fixture.runtime.observations().len();
    let gated_receipt = st2::resource_observe::submit_request(&fixture.root, &fixture.host, &gated)
        .unwrap()
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(gated_receipt.status, ObserveReceiptStatus::AbsentBinding);
    assert_eq!(
        fixture.runtime.observations().len(),
        before_gate,
        "a runtime without declared demand capability received an observe frame"
    );
}

#[test]
fn durable_observe_admission_rejects_the_concurrent_257th_request() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let fixture = CatalogFixture::new(temporary.path().join("catalog"), "alpha");
    st2::event::publish_owner_binding_for_test(&fixture.root, &fixture.host).unwrap();
    let supervisor = fixture.supervisor();
    fixture.runtime.registration();
    drop(supervisor);

    let submissions = (0..=MAX_PENDING_OBSERVE_REQUESTS)
        .map(|_| {
            let root = fixture.root.clone();
            let host = fixture.host.clone();
            std::thread::spawn(move || {
                let request = ObserveRequest::new(
                    format!("{host}.worker"),
                    "observed".to_owned(),
                    Some(1),
                    None,
                )
                .unwrap();
                st2::resource_observe::submit_request(&root, &host, &request)
                    .map(|_| true)
                    .map_err(|error| {
                        error
                            .downcast_ref::<ObserveAdmissionBackpressure>()
                            .is_some()
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
        1,
        "the only failed submission must expose structured durable backpressure"
    );
    let durable_count = fs::read_dir(fixture.observe_request_dir())
        .unwrap()
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(durable_count, MAX_PENDING_OBSERVE_REQUESTS);
}

#[test]
fn newer_client_generation_stays_queued_until_the_supervisor_refreshes() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let fixture = CatalogFixture::new(temporary.path().join("catalog"), "alpha");
    st2::event::publish_owner_binding_for_test(&fixture.root, &fixture.host).unwrap();
    let supervisor = fixture.supervisor();
    fixture.runtime.registration();

    let request = ObserveRequest::new(
        format!("{}.worker", fixture.host),
        "observed".to_owned(),
        Some(2),
        None,
    )
    .unwrap();
    let client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &request).unwrap();
    fixture.refresh_generation(&supervisor, 1);
    let _ = supervisor.health();
    assert!(
        fixture.runtime.observations().is_empty(),
        "a client-ahead generation was dispatched against an older resident catalog"
    );
    assert!(
        st2::resource_observe::read_receipt(&fixture.observe_receipt_dir(), &request.request_id)
            .unwrap()
            .is_none(),
        "a client-ahead generation was incorrectly terminalized as stale"
    );

    fixture.refresh_generation(&supervisor, 2);
    let observation = fixture.runtime.wait_for_observation(1);
    fixture.runtime.unchanged(&observation);
    let receipt = client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(receipt.status, ObserveReceiptStatus::SettledUnchanged);
}

#[test]
fn demanded_publication_settles_changed_before_resync_delivery_failure() {
    let _guard = TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temporary = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("XDG_STATE_HOME", temporary.path().join("state")) };

    let fixture = CatalogFixture::new(temporary.path().join("catalog"), "alpha");
    st2::event::publish_owner_binding_for_test(&fixture.root, &fixture.host).unwrap();
    let _supervisor = fixture.supervisor();
    fixture.runtime.registration();

    let request = fixture.request();
    let client =
        st2::resource_observe::submit_request(&fixture.root, &fixture.host, &request).unwrap();
    let observation = fixture.runtime.wait_for_observation(1);
    fs::remove_file(fixture.owner_binding_path()).unwrap();
    let published = br#"{"demand":"accepted-before-delivery"}"#;
    fixture
        .runtime
        .publish_observation(&observation, published, &["selected"], "delivery-failed");
    let receipt = client
        .wait_for_terminal(Duration::from_secs(2))
        .unwrap()
        .receipt
        .unwrap();
    assert_eq!(receipt.status, ObserveReceiptStatus::SettledChanged);
    assert_eq!(receipt.digest, Some(SnapshotDigest::of(published)));
    assert_eq!(fs::read(fixture.snapshot_path()).unwrap(), published);
}

fn wait_for_health(supervisor: &ResourceProfileSupervisor, marker: &str) {
    wait_until(marker, || {
        supervisor
            .health()
            .iter()
            .any(|health| health.detail.as_deref() == Some(marker))
            .then_some(())
    });
}

fn wait_until<T>(description: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
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

fn resync_inbox(agent_dir: &Path) -> Vec<String> {
    let inbox = agent_dir.join("resources/inbox");
    let mut records = fs::read_dir(inbox)
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

fn observable_resolver_wasm() -> Vec<u8> {
    const DESCRIPTOR: &[u8] = br#"{"abiVersion":3,"capabilities":["resolve","read","observe"],"selectorSchema":{"type":"object","properties":{"topics":{"type":"array","items":{"type":"string"},"uniqueItems":true}},"required":["topics"],"additionalProperties":false},"defaultSelector":{"topics":["selected"]},"topics":[{"name":"selected"},{"name":"ignored"}],"runtime":{"topology":"shared"},"snapshot":{"mediaType":"application/json","schemaId":"dev.example.observable.snapshot.v1"}}"#;
    const RESOLUTION: &[u8] = br#"{"path":"resources/snapshot.json","class":"observable"}"#;
    const DESCRIPTOR_PTR: i64 = 1024;
    const RESOLUTION_PTR: i64 = 4096;

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
    push_body(&mut code, 0x41, 8192);
    push_body(
        &mut code,
        0x42,
        (RESOLUTION_PTR << 32) | RESOLUTION.len() as i64,
    );
    push_body(
        &mut code,
        0x42,
        (DESCRIPTOR_PTR << 32) | DESCRIPTOR.len() as i64,
    );
    push_section(&mut module, 10, &code);

    let mut data = vec![2];
    push_data(&mut data, DESCRIPTOR_PTR, DESCRIPTOR);
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
    section.push(0);
    section.push(0x41);
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
