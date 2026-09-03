use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use notify::Watcher as _;
use serde_json::Value;
use sha2::Digest as _;
use tokio::sync::Notify;

use crate::model::{
    CheckpointSpec, ClaimInput, DependencySpec, DesiredSubject, GateSpec, LaunchSpec, MemberKind,
    MemberLifecycle, MemberSpec, PlanRunRequest, PlanRunView, PlanSpec, PlanState,
    RestartIntensity, RestartType, StepSpec, UsedPlanSpec,
};
use crate::store::Store;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeObservation {
    pub runtime_id: String,
    pub terminal: bool,
    pub status: String,
    pub exit_code: Option<i64>,
    pub incarnation_id: Option<String>,
}

pub trait RuntimeControl: Send + Sync + 'static {
    fn snapshot_ptys(&self) -> Result<Vec<RuntimeObservation>>;
    fn observe_exec(&self, runtime_id: &str) -> Result<Option<RuntimeObservation>>;
    fn start(&self, member: &MemberSpec) -> Result<()>;
    fn stop(
        &self,
        runtime_id: &str,
        terminal: bool,
        expected_incarnation: Option<&str>,
    ) -> Result<()>;
    fn kill(
        &self,
        runtime_id: &str,
        terminal: bool,
        expected_incarnation: Option<&str>,
    ) -> Result<()>;
    fn attach(&self, runtime_id: &str) -> Result<()>;
    fn send(&self, runtime_id: &str, text: &str) -> Result<()>;
    fn screen(&self, runtime_id: &str) -> Result<String>;
    fn send_key(&self, runtime_id: &str, key: &str) -> Result<()>;
    fn read_exec_log(&self, runtime_id: &str) -> Result<Option<String>>;
}

pub struct NativeRuntime {
    pty: st_runtime::PtyRuntime,
    exec: st_runtime::ExecRuntime,
}

impl NativeRuntime {
    pub fn new(state_dir: &Path, pty_root: Option<&Path>) -> Self {
        Self {
            pty: st_runtime::PtyRuntime::new(
                pty_root
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| state_dir.join("pty")),
            ),
            exec: st_runtime::ExecRuntime::new(state_dir.join("exec"), state_dir.join("logs")),
        }
    }
}

impl RuntimeControl for NativeRuntime {
    fn snapshot_ptys(&self) -> Result<Vec<RuntimeObservation>> {
        self.pty
            .snapshot()?
            .into_iter()
            .map(|item| {
                let incarnation_id = match (&item.pid, &item.created_at) {
                    (Some(pid), Some(created)) => Some(format!("{pid}:{created}")),
                    _ => None,
                };
                Ok(RuntimeObservation {
                    runtime_id: item.name,
                    terminal: true,
                    status: item.status,
                    exit_code: item.exit_code,
                    incarnation_id,
                })
            })
            .collect()
    }

    fn observe_exec(&self, runtime_id: &str) -> Result<Option<RuntimeObservation>> {
        Ok(self
            .exec
            .observe(runtime_id)?
            .map(|observation| match observation {
                st_runtime::ExecObservation::Running(generation) => RuntimeObservation {
                    runtime_id: runtime_id.into(),
                    terminal: false,
                    status: "running".into(),
                    exit_code: None,
                    incarnation_id: Some(generation.generation_id),
                },
                st_runtime::ExecObservation::Exited(generation) => RuntimeObservation {
                    runtime_id: runtime_id.into(),
                    terminal: false,
                    status: "exited".into(),
                    exit_code: generation.exit_code.map(i64::from),
                    incarnation_id: Some(generation.generation_id),
                },
                st_runtime::ExecObservation::Indeterminate(reason) => RuntimeObservation {
                    runtime_id: runtime_id.into(),
                    terminal: false,
                    status: "indeterminate".into(),
                    exit_code: None,
                    incarnation_id: Some(reason),
                },
            }))
    }

    fn start(&self, member: &MemberSpec) -> Result<()> {
        let launch = st_runtime::Launch::from(&member.launch);
        let cwd = PathBuf::from(&member.cwd);
        if member.terminal {
            self.pty.spawn(
                &member.runtime_id,
                &launch,
                &cwd,
                &member.environment,
                member.display_name.as_deref(),
                &member.tags,
            )
        } else {
            self.exec
                .spawn(&member.runtime_id, &launch, &cwd, &member.environment)
                .map(|_| ())
        }
    }

    fn stop(
        &self,
        runtime_id: &str,
        terminal: bool,
        expected_incarnation: Option<&str>,
    ) -> Result<()> {
        if terminal {
            self.pty.stop_if(runtime_id, expected_incarnation)
        } else {
            self.exec.stop_if(runtime_id, expected_incarnation)
        }
    }

    fn kill(
        &self,
        runtime_id: &str,
        terminal: bool,
        expected_incarnation: Option<&str>,
    ) -> Result<()> {
        if terminal {
            self.pty.kill_if(runtime_id, expected_incarnation)
        } else {
            self.exec.kill_if(runtime_id, expected_incarnation)
        }
    }

    fn attach(&self, runtime_id: &str) -> Result<()> {
        self.pty.attach(runtime_id)
    }

    fn send(&self, runtime_id: &str, text: &str) -> Result<()> {
        self.pty.send_line(runtime_id, text)
    }

    fn screen(&self, runtime_id: &str) -> Result<String> {
        self.pty.screen(runtime_id)
    }

    fn send_key(&self, runtime_id: &str, key: &str) -> Result<()> {
        self.pty.send_key(runtime_id, key)
    }

    fn read_exec_log(&self, runtime_id: &str) -> Result<Option<String>> {
        self.exec.read_log(runtime_id)
    }
}

pub struct Reconciler<R = NativeRuntime> {
    store: Arc<Store>,
    runtime: Arc<R>,
    host: String,
    endpoint: String,
    driver_state_dir: PathBuf,
    runtime_environment: BTreeMap<String, String>,
    notify: Arc<Notify>,
    event_notify: Arc<Notify>,
    armed_schedules: Arc<Mutex<std::collections::HashSet<String>>>,
    delayed_restarts: Arc<Mutex<HashMap<String, u128>>>,
    file_watchers: Arc<Mutex<HashMap<String, notify::RecommendedWatcher>>>,
}

impl Reconciler<NativeRuntime> {
    pub fn native(
        store: Arc<Store>,
        state_dir: &Path,
        pty_root: Option<&Path>,
        host: String,
        endpoint: String,
        notify: Arc<Notify>,
        event_notify: Arc<Notify>,
    ) -> Self {
        let selected_pty_root = pty_root
            .map(Path::to_path_buf)
            .unwrap_or_else(|| state_dir.join("pty"));
        Self {
            store,
            runtime: Arc::new(NativeRuntime::new(state_dir, Some(&selected_pty_root))),
            host,
            endpoint,
            driver_state_dir: state_dir.join("drivers"),
            runtime_environment: BTreeMap::from([(
                "PTY_ROOT".into(),
                selected_pty_root.to_string_lossy().into_owned(),
            )]),
            notify,
            event_notify,
            armed_schedules: Arc::new(Mutex::new(std::collections::HashSet::new())),
            delayed_restarts: Arc::new(Mutex::new(HashMap::new())),
            file_watchers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<R: RuntimeControl> Reconciler<R> {
    pub fn new(store: Arc<Store>, runtime: Arc<R>, host: String, notify: Arc<Notify>) -> Self {
        Self {
            store,
            runtime,
            host,
            endpoint: "unused-test-endpoint".into(),
            driver_state_dir: std::env::temp_dir().join("st3-test-drivers"),
            runtime_environment: BTreeMap::new(),
            notify,
            event_notify: Arc::new(Notify::new()),
            armed_schedules: Arc::new(Mutex::new(std::collections::HashSet::new())),
            delayed_restarts: Arc::new(Mutex::new(HashMap::new())),
            file_watchers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(self: Arc<Self>) {
        self.notify.notify_one();
        loop {
            self.notify.notified().await;
            if let Err(error) = self.reconcile_once() {
                let _ = self.record_once(
                    "host/reconciler",
                    "harness.error",
                    BTreeMap::from([
                        ("status".into(), Value::String("unreachable".into())),
                        ("reason".into(), Value::String(error.to_string())),
                    ]),
                );
            }
            self.event_notify.notify_waiters();
        }
    }

    fn signal_changed(&self) {
        signal_changed(&self.notify, &self.event_notify);
    }

    pub fn reconcile_once(&self) -> Result<()> {
        let desired = self.store.desired_subjects()?;
        let ptys = match self.runtime.snapshot_ptys() {
            Ok(snapshot) => snapshot
                .into_iter()
                .map(|item| (item.runtime_id.clone(), item))
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                self.record_once(
                    "host/runtime",
                    "harness.error",
                    BTreeMap::from([
                        ("status".into(), Value::String("indeterminate".into())),
                        ("reason".into(), Value::String(error.to_string())),
                    ]),
                )?;
                HashMap::new()
            }
        };

        let active = self.active_subjects(&desired)?;
        let stopped_scopes = active
            .iter()
            .filter(|subject| subject.kind == "scope-stop")
            .map(|subject| subject.subject.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for subject in &active {
            if subject.kind == "scope-stop" {
                self.reconcile_scope_stop(subject, &desired, &ptys)?;
                continue;
            }
            if subject.kind == "stop" {
                self.reconcile_stop(subject, &ptys)?;
                continue;
            }
            let Some(member) = &subject.member else {
                continue;
            };
            if subject
                .scopes
                .iter()
                .any(|scope| stopped_scopes.contains(scope))
            {
                continue;
            }
            if member.host != self.host {
                continue;
            }
            if self.link_blocks(subject, &desired)? {
                continue;
            }
            let observed = if member.terminal {
                ptys.get(&member.runtime_id).cloned()
            } else {
                self.runtime.observe_exec(&member.runtime_id)?
            };
            match observed {
                Some(observation) if observation.status == "running" => {
                    self.record_member(subject, &observation, true)?;
                    self.reconcile_gates(subject, member, &desired)?;
                }
                Some(observation)
                    if matches!(observation.status.as_str(), "exited" | "vanished") =>
                {
                    self.record_member(subject, &observation, false)?;
                    if !self.member_was_launched_for_selected_desired(&subject.subject)? {
                        self.perform_start(subject, member, "the desired member revision changed")?;
                        continue;
                    }
                    let restart = match member.restart {
                        RestartType::Always => true,
                        RestartType::OnFailure => observation.exit_code != Some(0),
                        RestartType::Never => false,
                    };
                    if restart && member.lifecycle == MemberLifecycle::Service {
                        self.reconcile_restart(subject, member, &observation)?;
                    }
                }
                Some(observation) => {
                    self.record_member(subject, &observation, false)?;
                }
                None if member.lifecycle == MemberLifecycle::AdoptOnly => {
                    self.record_once(
                        &subject.subject,
                        "member.observed",
                        member_fields(member, "absent", None, false),
                    )?;
                }
                None => {
                    let prior = self.store.latest_actual_value(&subject.subject)?;
                    if prior.is_some()
                        && !self.member_was_launched_for_selected_desired(&subject.subject)?
                    {
                        self.perform_start(subject, member, "the desired member revision changed")?;
                        continue;
                    }
                    if prior.as_ref().is_some_and(|actual| {
                        matches!(
                            actual_field(actual, "status").and_then(Value::as_str),
                            Some(
                                "running"
                                    | "ready"
                                    | "working"
                                    | "idle"
                                    | "starting"
                                    | "exited"
                                    | "vanished"
                            )
                        )
                    }) {
                        let observation = RuntimeObservation {
                            runtime_id: member.runtime_id.clone(),
                            terminal: member.terminal,
                            status: "vanished".into(),
                            exit_code: prior
                                .as_ref()
                                .and_then(|actual| actual_field(actual, "exit_code"))
                                .and_then(Value::as_i64),
                            incarnation_id: prior
                                .as_ref()
                                .and_then(|actual| actual_field(actual, "incarnation_id"))
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        };
                        self.record_member(subject, &observation, false)?;
                        let restart = match member.restart {
                            RestartType::Always => true,
                            RestartType::OnFailure => observation.exit_code != Some(0),
                            RestartType::Never => false,
                        };
                        if restart {
                            self.reconcile_restart(subject, member, &observation)?;
                        }
                    } else {
                        self.perform_start(subject, member, "the desired member is absent")?;
                    }
                }
            }
        }
        self.reconcile_schedules(&desired)?;
        self.deliver_messages(&desired)?;
        self.evaluate_plan_runs()?;
        self.evaluate_checkpoints(&desired)?;
        Ok(())
    }

    fn member_was_launched_for_selected_desired(&self, subject: &str) -> Result<bool> {
        let Some(desired_token) = self.store.selected_desired_token(subject)? else {
            return Ok(false);
        };
        Ok(self
            .store
            .claims_for(subject, Some("member.launch"))?
            .into_iter()
            .rev()
            .any(|claim| {
                claim
                    .body
                    .pointer("/fields/desired_token")
                    .and_then(Value::as_str)
                    == Some(desired_token.as_str())
            }))
    }

    pub fn attach(&self, runtime_id: &str) -> Result<()> {
        self.runtime.attach(runtime_id)
    }

    fn active_subjects<'a>(
        &self,
        desired: &'a [DesiredSubject],
    ) -> Result<Vec<&'a DesiredSubject>> {
        let mut reached = HashMap::new();
        let mut final_ordinals = HashMap::<String, u32>::new();
        for stage in desired
            .iter()
            .filter(|subject| subject.kind == "checkpoint-stage")
        {
            if let Ok(stage) = serde_json::from_value::<CheckpointSpec>(stage.desired.clone()) {
                final_ordinals
                    .entry(stage.sequence)
                    .and_modify(|ordinal| *ordinal = (*ordinal).max(stage.ordinal))
                    .or_insert(stage.ordinal);
            }
        }
        for activation in desired
            .iter()
            .filter_map(|subject| subject.activation.as_ref())
        {
            if !reached.contains_key(&activation.sequence) {
                let terminal = self
                    .store
                    .latest_claim(&activation.sequence, Some("checkpoint.failed"))?
                    .is_some();
                reached.insert(
                    activation.sequence.clone(),
                    (
                        self.current_checkpoint_reached(&activation.sequence, desired)?,
                        terminal,
                    ),
                );
            }
        }
        Ok(desired
            .iter()
            .filter(|subject| match &subject.activation {
                None => true,
                Some(activation) => {
                    let next =
                        reached
                            .get(&activation.sequence)
                            .map_or(0, |(reached, terminal)| {
                                if *terminal {
                                    final_ordinals
                                        .get(&activation.sequence)
                                        .copied()
                                        .unwrap_or(0)
                                } else {
                                    reached.map_or(0, |ordinal| ordinal.saturating_add(1))
                                }
                            });
                    activation.ordinal <= next
                }
            })
            .collect())
    }

    fn reconcile_stop(
        &self,
        subject: &DesiredSubject,
        ptys: &HashMap<String, RuntimeObservation>,
    ) -> Result<()> {
        let Some(actual) = self.store.latest_actual_value(&subject.subject)? else {
            return Ok(());
        };
        let fields = actual.get("fields").unwrap_or(&actual);
        let status = fields.get("status").and_then(Value::as_str);
        if matches!(status, Some("stopped" | "absent")) {
            return Ok(());
        }
        let Some(runtime_id) = fields.get("runtime_id").and_then(Value::as_str) else {
            return Ok(());
        };
        let terminal = fields
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let observation = if terminal {
            ptys.get(runtime_id).cloned()
        } else {
            self.runtime.observe_exec(runtime_id)?
        };
        let incarnation = observation
            .as_ref()
            .and_then(|value| value.incarnation_id.as_deref())
            .or_else(|| fields.get("incarnation_id").and_then(Value::as_str));
        let timeout = fields
            .get("shutdown_timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(5_000);
        self.reconcile_runtime_stop(
            &subject.subject,
            runtime_id,
            terminal,
            incarnation,
            timeout,
            observation.as_ref(),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_runtime_stop(
        &self,
        subject: &str,
        runtime_id: &str,
        terminal: bool,
        incarnation: Option<&str>,
        timeout_ms: u64,
        observation: Option<&RuntimeObservation>,
    ) -> Result<bool> {
        if observation.is_none_or(|observation| observation.status != "running") {
            self.record_once(
                subject,
                "member.observed",
                BTreeMap::from([
                    ("status".into(), Value::String("stopped".into())),
                    ("runtime_id".into(), Value::String(runtime_id.into())),
                    ("terminal".into(), Value::Bool(terminal)),
                    ("reachability".into(), Value::String("reachable".into())),
                    ("reason".into(), Value::Null),
                ]),
            )?;
            return Ok(true);
        }
        let incarnation = incarnation.unwrap_or("unknown");
        let requests = self.store.claims_for(subject, Some("action.requested"))?;
        let request = requests.into_iter().rev().find(|claim| {
            claim.body.pointer("/fields/action").and_then(Value::as_str) == Some("terminate")
                && claim
                    .body
                    .pointer("/fields/incarnation_id")
                    .and_then(Value::as_str)
                    == Some(incarnation)
        });
        let Some(request) = request else {
            let deadline = now_ms().saturating_add(timeout_ms as u128);
            let request = self.store.append_claim(&ClaimInput {
                subject: subject.into(),
                kind: "action.requested".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("action".into(), Value::String("terminate".into())),
                    ("runtime_id".into(), Value::String(runtime_id.into())),
                    ("terminal".into(), Value::Bool(terminal)),
                    ("incarnation_id".into(), Value::String(incarnation.into())),
                    (
                        "deadline_unix_ms".into(),
                        Value::String(deadline.to_string()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!("terminate:{subject}:{incarnation}")),
            })?;
            if let Err(error) = self.runtime.stop(runtime_id, terminal, Some(incarnation)) {
                self.store.append_claim(&ClaimInput {
                    subject: subject.into(),
                    kind: "action.failed".into(),
                    actor: None,
                    fields: BTreeMap::from([
                        ("action".into(), Value::String("terminate".into())),
                        ("reason".into(), Value::String(error.to_string())),
                    ]),
                    evidence: vec![request.id],
                    expected_subject: None,
                    idempotency_key: Some(format!("terminate-failed:{subject}:{incarnation}")),
                })?;
                return Err(error);
            }
            self.arm_restart(&format!("stop:{subject}"), now_ms().saturating_add(100));
            return Ok(false);
        };
        let deadline = request
            .body
            .pointer("/fields/deadline_unix_ms")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<u128>().ok())
            .unwrap_or(
                request
                    .accepted_at_unix_ms
                    .saturating_add(timeout_ms as u128),
            );
        if now_ms() < deadline {
            self.arm_restart(&format!("stop:{subject}"), deadline);
            return Ok(false);
        }
        let deadline_key = format!("stop-deadline:{subject}:{incarnation}");
        let deadline_record = self.store.append_claim(&ClaimInput {
            subject: subject.into(),
            kind: "deadline.reached".into(),
            actor: None,
            fields: BTreeMap::from([
                ("action".into(), Value::String("terminate".into())),
                ("incarnation_id".into(), Value::String(incarnation.into())),
            ]),
            evidence: vec![request.id],
            expected_subject: None,
            idempotency_key: Some(deadline_key.clone()),
        })?;
        if self
            .store
            .claims_for(subject, Some("action.completed"))?
            .iter()
            .any(|claim| {
                claim
                    .body
                    .pointer("/fields/deadline_key")
                    .and_then(Value::as_str)
                    == Some(deadline_key.as_str())
            })
        {
            self.record_once(
                subject,
                "supervision.decision",
                BTreeMap::from([
                    ("decision".into(), Value::String("raise".into())),
                    ("reachability".into(), Value::String("unreachable".into())),
                    (
                        "reason".into(),
                        Value::String("the recorded incarnation survived SIGKILL".into()),
                    ),
                ]),
            )?;
            return Ok(false);
        }
        self.runtime.kill(runtime_id, terminal, Some(incarnation))?;
        self.store.append_claim(&ClaimInput {
            subject: subject.into(),
            kind: "action.completed".into(),
            actor: None,
            fields: BTreeMap::from([
                ("action".into(), Value::String("kill".into())),
                ("deadline_key".into(), Value::String(deadline_key)),
                ("incarnation_id".into(), Value::String(incarnation.into())),
            ]),
            evidence: vec![deadline_record.id],
            expected_subject: None,
            idempotency_key: Some(format!("kill:{subject}:{incarnation}")),
        })?;
        self.arm_restart(&format!("stop:{subject}"), now_ms().saturating_add(100));
        Ok(false)
    }

    fn reconcile_scope_stop(
        &self,
        scope: &DesiredSubject,
        desired: &[DesiredSubject],
        ptys: &HashMap<String, RuntimeObservation>,
    ) -> Result<()> {
        let mut live = Vec::new();
        for member in desired
            .iter()
            .filter(|member| member.scopes.contains(&scope.subject))
            .filter_map(|member| member.member.as_ref().map(|spec| (member, spec)))
        {
            let (subject, spec) = member;
            let Some(actual) = self.store.latest_actual_value(&subject.subject)? else {
                continue;
            };
            let observation = if spec.terminal {
                ptys.get(&spec.runtime_id).cloned()
            } else {
                self.runtime.observe_exec(&spec.runtime_id)?
            };
            let incarnation = observation
                .as_ref()
                .and_then(|value| value.incarnation_id.as_deref())
                .or_else(|| actual_field(&actual, "incarnation_id").and_then(Value::as_str));
            if !self.reconcile_runtime_stop(
                &subject.subject,
                &spec.runtime_id,
                spec.terminal,
                incarnation,
                spec.shutdown_timeout_ms,
                observation.as_ref(),
            )? {
                live.push(Value::String(subject.subject.clone()));
            }
        }
        let status = if live.is_empty() {
            "stopped"
        } else {
            "stopping"
        };
        self.record_once(
            &scope.subject,
            "scope.members",
            BTreeMap::from([
                ("status".into(), Value::String(status.into())),
                ("members".into(), Value::Array(live)),
            ]),
        )?;
        Ok(())
    }

    fn perform_start(
        &self,
        subject: &DesiredSubject,
        member: &MemberSpec,
        reason: &str,
    ) -> Result<()> {
        for warning in
            crate::render::apply(&self.store, &subject.desired, Path::new(&member.workspace))?
        {
            self.record_once(
                &subject.subject,
                "harness.error",
                BTreeMap::from([
                    ("status".into(), Value::String("warning".into())),
                    ("reason".into(), Value::String(warning)),
                ]),
            )?;
        }
        let mut launch_member = member.clone();
        for (key, value) in &self.runtime_environment {
            launch_member
                .environment
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
        launch_member
            .environment
            .insert("ST3_ENDPOINT".into(), self.endpoint.clone());
        launch_member.environment.insert(
            "ST3_DRIVER_STATE_DIR".into(),
            self.driver_state_dir.to_string_lossy().into_owned(),
        );
        launch_member
            .environment
            .insert("ST_AGENT".into(), subject.subject.clone());
        let executable = std::env::current_exe()?;
        prepend_executable_dir(&mut launch_member.environment, &executable)?;
        if let crate::model::LaunchSpec::Argv(argv) = &mut launch_member.launch
            && argv.first().map(String::as_str) == Some("st3")
        {
            argv[0] = executable.to_string_lossy().into_owned();
        }
        if !launch_member.terminal {
            let original = match &launch_member.launch {
                crate::model::LaunchSpec::Shell(source) => {
                    vec!["sh".into(), "-c".into(), source.clone()]
                }
                crate::model::LaunchSpec::Argv(argv) => argv.clone(),
            };
            let mut wrapper = vec![
                executable.to_string_lossy().into_owned(),
                "driver".into(),
                "exec".into(),
                "--subject".into(),
                subject.subject.clone(),
                "--".into(),
            ];
            wrapper.extend(original);
            launch_member.launch = crate::model::LaunchSpec::Argv(wrapper);
        }
        self.perform_action(&subject.subject, "start", || {
            self.runtime.start(&launch_member)
        })?;
        let desired_token = self
            .store
            .selected_desired_token(&subject.subject)?
            .unwrap_or_default();
        self.store.append_claim(&ClaimInput {
            subject: subject.subject.clone(),
            kind: "member.launch".into(),
            actor: None,
            fields: BTreeMap::from([
                ("desired_token".into(), Value::String(desired_token)),
                (
                    "runtime_id".into(),
                    Value::String(member.runtime_id.clone()),
                ),
                ("reason".into(), Value::String(reason.into())),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: None,
        })?;
        self.record_once(
            &subject.subject,
            "member.observed",
            member_fields(member, "starting", None, false),
        )?;
        self.record_once(
            &subject.subject,
            "action.completed",
            BTreeMap::from([
                ("reason".into(), Value::String(reason.into())),
                (
                    "runtime_id".into(),
                    Value::String(member.runtime_id.clone()),
                ),
            ]),
        )?;
        self.signal_changed();
        Ok(())
    }

    fn reconcile_restart(
        &self,
        subject: &DesiredSubject,
        member: &MemberSpec,
        observation: &RuntimeObservation,
    ) -> Result<()> {
        match self.restart_decision(subject, member, observation)? {
            RestartDecision::Start => {
                self.delayed_restarts
                    .lock()
                    .expect("restart mutex poisoned")
                    .remove(&subject.subject);
                self.perform_start(subject, member, "the prior generation exited")
            }
            RestartDecision::Wait { until, reason } => {
                self.record_once(
                    &subject.subject,
                    "supervision.decision",
                    BTreeMap::from([
                        ("decision".into(), Value::String("wait".into())),
                        ("reachability".into(), Value::String("reachable".into())),
                        ("reason".into(), Value::String(reason)),
                        (
                            "restart_at_unix_ms".into(),
                            Value::String(until.to_string()),
                        ),
                    ]),
                )?;
                self.arm_restart(&subject.subject, until);
                Ok(())
            }
            RestartDecision::Fail { reason } => self.record_once(
                &subject.subject,
                "supervision.decision",
                BTreeMap::from([
                    ("decision".into(), Value::String("raise".into())),
                    ("reachability".into(), Value::String("unreachable".into())),
                    ("reason".into(), Value::String(reason)),
                ]),
            ),
        }
    }

    fn restart_decision(
        &self,
        subject: &DesiredSubject,
        member: &MemberSpec,
        observation: &RuntimeObservation,
    ) -> Result<RestartDecision> {
        let now = now_ms();
        let desired_token = self
            .store
            .selected_desired_token(&subject.subject)?
            .unwrap_or_default();
        let mut launches = self
            .store
            .claims_for(&subject.subject, Some("member.launch"))?
            .into_iter()
            .filter(|claim| {
                claim
                    .body
                    .pointer("/fields/desired_token")
                    .and_then(Value::as_str)
                    == Some(desired_token.as_str())
            })
            .collect::<Vec<_>>();
        let resets = self
            .store
            .claims_for(&subject.subject, Some("member.restart-reset"))?;
        let reset_index = resets
            .iter()
            .filter(|claim| {
                claim
                    .body
                    .pointer("/fields/desired_token")
                    .and_then(Value::as_str)
                    == Some(desired_token.as_str())
            })
            .map(|claim| claim.store_index)
            .max()
            .unwrap_or(0);
        launches.retain(|claim| claim.store_index > reset_index);

        if member.restart_intensity.mode == "fail" {
            if let Some(last) = launches.last()
                && now.saturating_sub(last.accepted_at_unix_ms)
                    >= member.restart_intensity.interval_ms as u128
            {
                let incarnation = observation.incarnation_id.as_deref().unwrap_or("unknown");
                self.store.append_claim(&ClaimInput {
                    subject: subject.subject.clone(),
                    kind: "member.restart-reset".into(),
                    actor: None,
                    fields: BTreeMap::from([
                        ("desired_token".into(), Value::String(desired_token.clone())),
                        ("incarnation_id".into(), Value::String(incarnation.into())),
                    ]),
                    evidence: vec![last.id.clone()],
                    expected_subject: None,
                    idempotency_key: Some(format!(
                        "restart-reset:{}:{desired_token}:{incarnation}",
                        subject.subject
                    )),
                })?;
                launches.clear();
            }
            if launches.len() >= member.restart_intensity.attempts as usize {
                return Ok(RestartDecision::Fail {
                    reason: format!(
                        "the member used {} launches without a stable {}ms interval",
                        member.restart_intensity.attempts, member.restart_intensity.interval_ms
                    ),
                });
            }
        }

        let mut wait_until = now;
        let mut reasons = Vec::new();
        if member.restart_intensity.delay_ms > 0 {
            let observed_at = self
                .store
                .claims_for(&subject.subject, Some("member.observed"))?
                .into_iter()
                .rev()
                .find(|claim| {
                    matches!(
                        claim.body.pointer("/fields/status").and_then(Value::as_str),
                        Some("exited" | "vanished")
                    ) && observation
                        .incarnation_id
                        .as_deref()
                        .is_none_or(|incarnation| {
                            claim
                                .body
                                .pointer("/fields/incarnation_id")
                                .and_then(Value::as_str)
                                == Some(incarnation)
                        })
                })
                .map(|claim| claim.accepted_at_unix_ms)
                .unwrap_or(now);
            let delayed = observed_at.saturating_add(member.restart_intensity.delay_ms as u128);
            if delayed > wait_until {
                wait_until = delayed;
                reasons.push(format!(
                    "the restart delay is {}ms",
                    member.restart_intensity.delay_ms
                ));
            }
        }
        if member.restart_intensity.mode == "delay" {
            let window_start = now.saturating_sub(member.restart_intensity.interval_ms as u128);
            let recent = launches
                .iter()
                .filter(|claim| claim.accepted_at_unix_ms > window_start)
                .collect::<Vec<_>>();
            if recent.len() >= member.restart_intensity.attempts as usize {
                let available = recent[0]
                    .accepted_at_unix_ms
                    .saturating_add(member.restart_intensity.interval_ms as u128);
                if available > wait_until {
                    wait_until = available;
                    reasons.push(format!(
                        "the {} launch limit applies for {}ms",
                        member.restart_intensity.attempts, member.restart_intensity.interval_ms
                    ));
                }
            }
        }
        if wait_until > now {
            Ok(RestartDecision::Wait {
                until: wait_until,
                reason: reasons.join("; "),
            })
        } else {
            Ok(RestartDecision::Start)
        }
    }

    fn arm_restart(&self, subject: &str, until: u128) {
        let mut armed = self
            .delayed_restarts
            .lock()
            .expect("restart mutex poisoned");
        if armed.get(subject).is_some_and(|current| *current == until) {
            return;
        }
        armed.insert(subject.into(), until);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let notify = self.notify.clone();
        let delayed = self.delayed_restarts.clone();
        let subject = subject.to_owned();
        handle.spawn(async move {
            let delay = until.saturating_sub(now_ms()).min(u64::MAX as u128) as u64;
            tokio::time::sleep(Duration::from_millis(delay)).await;
            delayed
                .lock()
                .expect("restart mutex poisoned")
                .remove(&subject);
            notify.notify_one();
        });
    }

    fn perform_action(
        &self,
        subject: &str,
        action: &str,
        effect: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let operation = format!("{subject}:{action}");
        self.record_once(
            subject,
            "action.requested",
            BTreeMap::from([
                ("action".into(), Value::String(action.into())),
                ("operation".into(), Value::String(operation.clone())),
            ]),
        )?;
        match effect() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.record_once(
                    subject,
                    "action.failed",
                    BTreeMap::from([
                        ("action".into(), Value::String(action.into())),
                        ("operation".into(), Value::String(operation)),
                        ("reason".into(), Value::String(error.to_string())),
                    ]),
                )?;
                Err(error)
            }
        }
    }

    fn record_member(
        &self,
        subject: &DesiredSubject,
        observation: &RuntimeObservation,
        adopted: bool,
    ) -> Result<()> {
        let member = subject
            .member
            .as_ref()
            .context("member observation lacks member")?;
        let mut fields = member_fields(
            member,
            &observation.status,
            observation.incarnation_id.as_deref(),
            adopted,
        );
        if let Some(exit_code) = observation.exit_code {
            fields.insert("exit_code".into(), Value::from(exit_code));
        }
        self.record_once(&subject.subject, "member.observed", fields)
    }

    fn record_once(
        &self,
        subject: &str,
        kind: &str,
        fields: BTreeMap<String, Value>,
    ) -> Result<()> {
        if self
            .store
            .latest_claim(subject, Some(kind))?
            .is_some_and(|claim| {
                claim.body.get("fields")
                    == Some(&serde_json::to_value(&fields).unwrap_or(Value::Null))
            })
        {
            return Ok(());
        }
        self.store
            .append_claim(&ClaimInput {
                subject: subject.into(),
                kind: kind.into(),
                actor: None,
                fields,
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: None,
            })
            .map_err(Into::into)
            .map(|_| ())
    }

    fn evaluate_plan_runs(&self) -> Result<()> {
        let runs = self.store.active_plan_runs()?;
        let mut changed = false;
        for run in runs {
            if run.phase == "revision-draining"
                && self.store.apply_drained_revision(&run.id)?.is_some()
            {
                changed = true;
                continue;
            }
            let plan_id = run.plan.strip_prefix("plan/").unwrap_or(&run.plan);
            let Some(plan) = self.store.plan_spec(plan_id, Some(&run.revision))? else {
                changed |= self.store.set_plan_run_state(
                    &run.id,
                    "blocked",
                    &run.phase,
                    Some("the selected plan revision is unavailable"),
                )?;
                continue;
            };
            changed |= self.evaluate_plan_run(&run, &plan)?;
        }
        if changed {
            self.signal_changed();
        }
        Ok(())
    }

    fn evaluate_plan_run(&self, run: &PlanRunView, plan: &PlanSpec) -> Result<bool> {
        let mut changed = false;
        changed |= self.retire_predecessor_generation(run)?;
        changed |= self.materialize_plan_subgraph(run, plan)?;
        let flat = flatten_plan_steps(plan);
        let views = run
            .steps
            .iter()
            .map(|step| (step.step.as_str(), step))
            .collect::<HashMap<_, _>>();
        if run.phase == "normal" {
            let admitted = flat.iter().filter(|step| !step.spec.finally).any(|step| {
                views.get(step.spec.path.as_str()).is_some_and(|view| {
                    view.attempt > 1
                        || matches!(
                            view.status.as_str(),
                            "ready"
                                | "claimed"
                                | "working"
                                | "verifying"
                                | "completed"
                                | "failed"
                                | "cancelled"
                        )
                })
            });
            if !admitted {
                let variables = crate::store::plan_run_variables(run, &run.revision);
                for baseline in &plan.baselines {
                    for gate in &baseline.gates {
                        if !matches!(
                            self.evaluate_context_gate(
                                run,
                                &run.subject,
                                &plan.id,
                                &plan.revision,
                                1,
                                gate,
                                &variables,
                            )?,
                            GateOutcome::Pass
                        ) {
                            changed |= self.store.set_plan_run_state(
                                &run.id,
                                "blocked",
                                "normal",
                                Some(&format!("plan baseline `{}` does not hold", baseline.name)),
                            )?;
                            return Ok(changed);
                        }
                    }
                }
            }
            let blocked_by_baseline = run.status == "blocked"
                && self
                    .store
                    .latest_claim(&run.subject, Some("plan-run.state"))?
                    .and_then(|claim| {
                        claim
                            .body
                            .pointer("/fields/reason")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .is_some_and(|reason| reason.starts_with("plan baseline `"));
            if blocked_by_baseline {
                changed |= self
                    .store
                    .set_plan_run_state(&run.id, "running", "normal", None)?;
            } else if run.status == "blocked" {
                return Ok(changed);
            }
        }
        let mut normal_failed = flat.iter().any(|step| {
            !step.spec.finally
                && views.get(step.spec.path.as_str()).is_some_and(|view| {
                    view.status == "failed" && view.attempt >= step.spec.retry.attempts
                })
        });
        let mut normal_failure_reason = normal_failed.then(|| "a normal step failed".to_owned());
        let normal_complete = flat.iter().filter(|step| !step.spec.finally).all(|step| {
            views
                .get(step.spec.path.as_str())
                .is_some_and(|view| view.status == "completed")
        });
        if run.phase == "normal" && normal_complete && !normal_failed {
            let variables = crate::store::plan_run_variables(run, &run.revision);
            if !self.products_hold_with_variables(&plan.products, &variables)? {
                return Ok(changed);
            }
            for gate in &plan.gates {
                match self.evaluate_context_gate(
                    run,
                    &run.subject,
                    &plan.id,
                    &plan.revision,
                    1,
                    gate,
                    &variables,
                )? {
                    GateOutcome::Pass => {}
                    GateOutcome::Pending => return Ok(changed),
                    GateOutcome::Fail(reason) => {
                        normal_failed = true;
                        normal_failure_reason = Some(reason);
                        break;
                    }
                }
            }
        }
        if run.phase == "normal" && (normal_failed || normal_complete) {
            for step in flat.iter().filter(|step| !step.spec.finally) {
                let view = views[step.spec.path.as_str()];
                if !matches!(view.status.as_str(), "completed" | "failed" | "cancelled") {
                    changed |= self.store.set_step_state(
                        &view.subject,
                        "cancelled",
                        Some(if normal_failed {
                            "another step failed"
                        } else {
                            "the normal phase completed"
                        }),
                    )?;
                }
            }
            if flat.iter().any(|step| step.spec.finally) {
                changed |= self.store.set_plan_run_state(
                    &run.id,
                    "running",
                    "final",
                    normal_failure_reason.as_deref(),
                )?;
            } else {
                changed |= self.store.set_plan_run_state(
                    &run.id,
                    if normal_failed { "failed" } else { "completed" },
                    "terminal",
                    normal_failure_reason.as_deref(),
                )?;
            }
            return Ok(changed);
        }

        if run.phase == "final" {
            let final_terminal = flat.iter().filter(|step| step.spec.finally).all(|step| {
                views.get(step.spec.path.as_str()).is_some_and(|view| {
                    matches!(view.status.as_str(), "completed" | "failed" | "cancelled")
                })
            });
            if final_terminal {
                let failed = run.steps.iter().any(|step| {
                    !step.step.is_empty() && matches!(step.status.as_str(), "failed" | "cancelled")
                });
                changed |= self.store.set_plan_run_state(
                    &run.id,
                    if failed { "failed" } else { "completed" },
                    "terminal",
                    failed.then_some("one or more plan steps failed"),
                )?;
                return Ok(changed);
            }
        }

        for step in flat {
            let Some(view) = views.get(step.spec.path.as_str()).copied() else {
                continue;
            };
            let eligible_phase = ((run.phase == "normal" || run.phase == "revision-draining")
                && !step.spec.finally)
                || (run.phase == "final" && step.spec.finally);
            if !eligible_phase {
                continue;
            }
            if run.phase == "revision-draining"
                && matches!(
                    view.status.as_str(),
                    "pending" | "ready" | "blocked" | "failed" | "completed" | "cancelled"
                )
            {
                continue;
            }
            if view.status == "failed" {
                if view.attempt < step.spec.retry.attempts {
                    changed |= self.store.retry_step(
                        &view.subject,
                        "the step retry policy permits another attempt",
                        step.spec.retry.backoff_ms,
                    )?;
                }
                continue;
            }
            if matches!(view.status.as_str(), "completed" | "cancelled") {
                continue;
            }
            if let Some(expiry) = view.lease_expires_at_unix_ms
                && expiry <= now_ms()
                && matches!(view.status.as_str(), "claimed" | "working")
            {
                changed |= self.store.set_step_state(
                    &view.subject,
                    "ready",
                    Some("the worker lease expired"),
                )?;
                continue;
            }
            let assignment_blocked = view.status == "blocked"
                && view
                    .blocked_reason
                    .as_deref()
                    .is_some_and(|reason| reason.starts_with("the assigned agent `"));
            let baseline_blocked = view.status == "blocked"
                && view
                    .blocked_reason
                    .as_deref()
                    .is_some_and(|reason| reason.starts_with("step baseline `"));
            if view.status == "pending" || assignment_blocked || baseline_blocked {
                if view
                    .not_before_unix_ms
                    .is_some_and(|not_before| not_before > now_ms())
                {
                    continue;
                }
                if !self.step_dependencies_hold(run, &step, &views)? {
                    continue;
                }
                let mut baseline_holds = true;
                for baseline in &step.spec.baselines {
                    for gate in &baseline.gates {
                        if !matches!(
                            self.evaluate_plan_gate(run, &step, view, gate)?,
                            GateOutcome::Pass
                        ) {
                            changed |= self.store.set_step_state(
                                &view.subject,
                                "blocked",
                                Some(&format!("step baseline `{}` does not hold", baseline.name)),
                            )?;
                            baseline_holds = false;
                            break;
                        }
                    }
                    if !baseline_holds {
                        break;
                    }
                }
                if !baseline_holds {
                    continue;
                }
                if baseline_blocked {
                    changed |= self.store.set_step_state(&view.subject, "pending", None)?;
                }
                if let Some(assignee) = &view.assignee
                    && self.store.selected_desired_revision(assignee)?.is_none()
                {
                    changed |= self.store.set_step_state(
                        &view.subject,
                        "blocked",
                        Some(&format!(
                            "the assigned agent `{assignee}` is not present in the desired graph"
                        )),
                    )?;
                    continue;
                }
                changed |= self.store.set_step_state(&view.subject, "ready", None)?;
                continue;
            }
            if !matches!(
                view.status.as_str(),
                "ready" | "claimed" | "working" | "verifying" | "blocked"
            ) {
                continue;
            }
            changed |= self.materialize_step_subgraph(run, &step, view)?;
            if self.step_timed_out(view, &step)? {
                changed |= self.store.set_step_state(
                    &view.subject,
                    "failed",
                    Some("the step timeout expired"),
                )?;
                continue;
            }
            if !self.step_subgraph_holds(&view.subject)? {
                continue;
            }
            if view.assignee.is_some() && !view.worker_reported {
                continue;
            }
            if let Some(nested) = &step.spec.nested_plan {
                let nested_prefix = format!("{}/{}/", step.spec.path, nested.id);
                if !views
                    .iter()
                    .filter(|(path, _)| path.starts_with(&nested_prefix))
                    .all(|(_, child)| child.status == "completed")
                {
                    continue;
                }
            }
            if step.spec.produces_plan.is_some() && !self.produced_plan_holds(step.spec, view)? {
                continue;
            }
            if step.spec.uses_plan.is_some() {
                let (use_changed, outcome) = self.evaluate_used_plan(run, &step, view, &views)?;
                changed |= use_changed;
                match outcome {
                    UsedPlanOutcome::Pending => continue,
                    UsedPlanOutcome::Completed => {}
                    UsedPlanOutcome::Failed(reason) => {
                        changed |=
                            self.store
                                .set_step_state(&view.subject, "failed", Some(&reason))?;
                        continue;
                    }
                }
            }
            if !self.products_hold(run, &step, view)? {
                continue;
            }
            let mut gates_pass = true;
            for gate in &step.spec.gates {
                match self.evaluate_plan_gate(run, &step, view, gate)? {
                    GateOutcome::Pass => {}
                    GateOutcome::Pending => {
                        gates_pass = false;
                        break;
                    }
                    GateOutcome::Fail(reason) => {
                        changed |=
                            self.store
                                .set_step_state(&view.subject, "failed", Some(&reason))?;
                        gates_pass = false;
                        break;
                    }
                }
            }
            if gates_pass {
                changed |= self
                    .store
                    .set_step_state(&view.subject, "completed", None)?;
            }
        }
        Ok(changed)
    }

    fn produced_plan_holds(
        &self,
        step: &StepSpec,
        view: &crate::model::StepRunView,
    ) -> Result<bool> {
        let Some(expected) = &step.produces_plan else {
            return Ok(true);
        };
        let Some(output) =
            self.store
                .plan_output(&view.subject, view.attempt, &view.definition_hash)?
        else {
            return Ok(false);
        };
        if output.plan.strip_prefix("plan/").unwrap_or(&output.plan) != expected {
            return Ok(false);
        }
        Ok(self
            .store
            .plan_spec(expected, Some(&output.revision))?
            .is_some_and(|plan| plan.state == PlanState::Ready))
    }

    fn evaluate_used_plan(
        &self,
        run: &PlanRunView,
        step: &RuntimeStep<'_>,
        view: &crate::model::StepRunView,
        views: &HashMap<&str, &crate::model::StepRunView>,
    ) -> Result<(bool, UsedPlanOutcome)> {
        let (plan, revision) = match step
            .spec
            .uses_plan
            .as_ref()
            .expect("a used plan was checked")
        {
            UsedPlanSpec::Revision { plan, revision } => (plan.clone(), revision.clone()),
            UsedPlanSpec::StepOutput { step: producer } => {
                let path = if step.dependency_prefix.is_empty() {
                    producer.clone()
                } else {
                    format!("{}/{}", step.dependency_prefix, producer)
                };
                let Some(producer) = views.get(path.as_str()).copied() else {
                    return Ok((false, UsedPlanOutcome::Pending));
                };
                let Some(output) = self.store.plan_output(
                    &producer.subject,
                    producer.attempt,
                    &producer.definition_hash,
                )?
                else {
                    return Ok((false, UsedPlanOutcome::Pending));
                };
                (
                    output
                        .plan
                        .strip_prefix("plan/")
                        .unwrap_or(&output.plan)
                        .to_owned(),
                    output.revision,
                )
            }
        };
        let Some(selected) = self.store.plan_spec(&plan, Some(&revision))? else {
            return Ok((
                false,
                UsedPlanOutcome::Failed(format!(
                    "the exact used plan `plan/{plan}@{revision}` is unavailable"
                )),
            ));
        };
        if selected.state != PlanState::Ready {
            return Ok((
                false,
                UsedPlanOutcome::Failed(format!(
                    "the exact used plan `plan/{plan}@{revision}` is not ready"
                )),
            ));
        }
        let (child, changed) =
            if let Some(child) = self.store.plan_run_for_parent_step(&view.subject)? {
                (child, false)
            } else {
                let child = self.store.create_child_plan_run(
                    &PlanRunRequest {
                        plan: plan.clone(),
                        revision: Some(revision.clone()),
                        workspace: run.workspace.clone(),
                        requester: Some(run.requester.clone()),
                        mode: Some(run.mode.clone()),
                        idempotency_key: format!(
                            "uses-plan:{}:{}:plan/{plan}@{revision}",
                            view.subject, view.attempt
                        ),
                    },
                    run,
                    &view.subject,
                    view.assignee.as_deref(),
                )?;
                (child, true)
            };
        if child.plan != format!("plan/{plan}") || child.revision != revision {
            return Ok((
                changed,
                UsedPlanOutcome::Failed(
                    "the step already started a different exact plan revision".into(),
                ),
            ));
        }
        let outcome = match child.status.as_str() {
            "completed" => UsedPlanOutcome::Completed,
            "failed" | "cancelled" => UsedPlanOutcome::Failed(format!(
                "the used plan run `{}` is {}",
                child.subject, child.status
            )),
            _ => UsedPlanOutcome::Pending,
        };
        Ok((changed, outcome))
    }

    fn step_dependencies_hold(
        &self,
        run: &PlanRunView,
        step: &RuntimeStep<'_>,
        views: &HashMap<&str, &crate::model::StepRunView>,
    ) -> Result<bool> {
        if let Some(parent) = &step.parent {
            let Some(parent) = views.get(parent.as_str()) else {
                return Ok(false);
            };
            if !matches!(parent.status.as_str(), "claimed" | "working" | "verifying") {
                return Ok(false);
            }
        }
        for dependency in &step.spec.dependencies {
            match dependency {
                DependencySpec::Step {
                    step: target,
                    state,
                } => {
                    let path = if step.dependency_prefix.is_empty() {
                        target.clone()
                    } else {
                        format!("{}/{target}", step.dependency_prefix)
                    };
                    let Some(target) = views.get(path.as_str()) else {
                        return Ok(false);
                    };
                    let holds = match state.as_str() {
                        "completed" => target.status == "completed",
                        "failed" => target.status == "failed",
                        "terminal" => {
                            matches!(target.status.as_str(), "completed" | "failed" | "cancelled")
                        }
                        _ => false,
                    };
                    if !holds {
                        return Ok(false);
                    }
                }
                DependencySpec::Predicate { gate } => {
                    let fake = crate::model::StepRunView {
                        subject: format!(
                            "step-run/{}/{}",
                            run.generation
                                .strip_prefix("run-generation/")
                                .unwrap_or(&run.generation),
                            step.spec.path
                        ),
                        run: run.subject.clone(),
                        generation: run.generation.clone(),
                        step: step.spec.path.clone(),
                        definition_hash: step.spec.definition_hash.clone(),
                        status: "pending".into(),
                        attempt: 1,
                        assignee: None,
                        title: None,
                        goals: Vec::new(),
                        under: Vec::new(),
                        worker_reported: false,
                        lease_owner: None,
                        lease_incarnation: None,
                        lease_expires_at_unix_ms: None,
                        blocked_reason: None,
                        not_before_unix_ms: None,
                        created_at_unix_ms: run.created_at_unix_ms,
                        updated_at_unix_ms: run.updated_at_unix_ms,
                    };
                    if !matches!(
                        self.evaluate_plan_gate(run, step, &fake, gate)?,
                        GateOutcome::Pass
                    ) {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn materialize_step_subgraph(
        &self,
        run: &PlanRunView,
        step: &RuntimeStep<'_>,
        view: &crate::model::StepRunView,
    ) -> Result<bool> {
        let Some(source) = &step.spec.subgraph_kdl else {
            return Ok(false);
        };
        let variables = run_variables(run, step, view);
        let source = crate::plan::interpolate(source, &variables)?;
        let mut intent = crate::graph::parse_intent(&source, &self.host)?;
        let step_scope = format!("scope/{}", view.subject);
        let generation_scope = format!("scope/{}", run.generation);
        for subject in intent.subjects.values_mut() {
            subject.scopes.insert(step_scope.clone());
            subject.scopes.insert(generation_scope.clone());
            if let Some(scope) = &run.run_scope {
                subject.scopes.insert(scope.clone());
            }
            if let Some(member) = subject.member.as_mut() {
                let workspace = PathBuf::from(&member.workspace);
                if workspace.is_relative() {
                    member.workspace = Path::new(&run.workspace)
                        .join(&workspace)
                        .to_string_lossy()
                        .into_owned();
                }
                let cwd = PathBuf::from(&member.cwd);
                if cwd.is_relative() {
                    member.cwd = Path::new(&run.workspace)
                        .join(&cwd)
                        .to_string_lossy()
                        .into_owned();
                }
                member.environment.extend(variables.clone());
                member
                    .environment
                    .insert("ST3_RUN_DIR".into(), run.workspace.clone());
                member
                    .environment
                    .insert("ST3_ENDPOINT".into(), self.endpoint.clone());
                member
                    .environment
                    .insert("ST_AGENT".into(), member.runtime_id.clone());
            }
        }
        if let Some(scope) = &run.run_scope {
            intent
                .subjects
                .entry(scope.clone())
                .or_insert_with(|| DesiredSubject {
                    subject: scope.clone(),
                    kind: "scope".into(),
                    desired: serde_json::json!({"scope": scope, "retention": "temporary"}),
                    member: None,
                    activation: None,
                    scopes: BTreeSet::new(),
                });
        }
        let response = self.store.apply_internal(
            &intent,
            &format!("materialize:{}:{}", view.subject, view.attempt),
        )?;
        Ok(response.changed)
    }

    fn retire_predecessor_generation(&self, run: &PlanRunView) -> Result<bool> {
        let Some(predecessor) = self
            .store
            .run_generation(&run.generation)?
            .and_then(|generation| generation.predecessor)
        else {
            return Ok(false);
        };
        let mut generations = vec![predecessor.clone()];
        generations.extend(self.store.descendant_run_generations(&predecessor)?);
        generations.sort();
        generations.dedup();
        let stops = generations
            .iter()
            .map(|generation| format!("scope {generation:?} {{ stop }}"))
            .collect::<Vec<_>>()
            .join("\n");
        let source = format!("version 2\nsubgraph {{\n{stops}\n}}");
        let intent = crate::graph::parse_intent(&source, &self.host)?;
        let response = self
            .store
            .apply_internal(&intent, &format!("retire-generation:{}", run.generation))?;
        Ok(response.changed)
    }

    fn materialize_plan_subgraph(&self, run: &PlanRunView, plan: &PlanSpec) -> Result<bool> {
        let Some(source) = &plan.subgraph_kdl else {
            return Ok(false);
        };
        let variables = crate::store::plan_run_variables(run, &plan.revision);
        let source = crate::plan::interpolate(source, &variables)?;
        let mut intent = crate::graph::parse_intent(&source, &self.host)?;
        let generation_scope = format!("scope/{}", run.generation);
        for subject in intent.subjects.values_mut() {
            subject.scopes.insert(generation_scope.clone());
            if let Some(scope) = &run.run_scope {
                subject.scopes.insert(scope.clone());
            }
            if let Some(member) = subject.member.as_mut() {
                let workspace = PathBuf::from(&member.workspace);
                if workspace.is_relative() {
                    member.workspace = Path::new(&run.workspace)
                        .join(&workspace)
                        .to_string_lossy()
                        .into_owned();
                }
                let cwd = PathBuf::from(&member.cwd);
                if cwd.is_relative() {
                    member.cwd = Path::new(&run.workspace)
                        .join(&cwd)
                        .to_string_lossy()
                        .into_owned();
                }
                member.environment.extend(variables.clone());
                member
                    .environment
                    .insert("ST3_RUN_DIR".into(), run.workspace.clone());
                member
                    .environment
                    .insert("ST3_ENDPOINT".into(), self.endpoint.clone());
                member
                    .environment
                    .insert("ST_AGENT".into(), member.runtime_id.clone());
            }
        }
        let response = self
            .store
            .apply_internal(&intent, &format!("materialize:{}", run.generation))?;
        Ok(response.changed)
    }

    fn step_subgraph_holds(&self, step_subject: &str) -> Result<bool> {
        let scope = format!("scope/{step_subject}");
        for subject in self
            .store
            .desired_subjects()?
            .into_iter()
            .filter(|subject| subject.scopes.contains(&scope))
        {
            let status = self
                .store
                .latest_actual_value(&subject.subject)?
                .as_ref()
                .and_then(|actual| actual_field(actual, "status"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let holds = match subject.kind.as_str() {
                "scope-stop" | "stop" => {
                    matches!(status.as_deref(), Some("stopped" | "absent" | "exited"))
                }
                "message" => matches!(status.as_deref(), Some("delivered" | "accepted" | "closed")),
                _ => match subject.member.as_ref() {
                    Some(member) if member.driver.is_some() => {
                        matches!(status.as_deref(), Some("ready" | "working" | "idle"))
                    }
                    Some(_) => matches!(
                        status.as_deref(),
                        Some("running" | "ready" | "working" | "idle" | "exited")
                    ),
                    None => true,
                },
            };
            if !holds {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn products_hold(
        &self,
        run: &PlanRunView,
        step: &RuntimeStep<'_>,
        view: &crate::model::StepRunView,
    ) -> Result<bool> {
        let variables = run_variables(run, step, view);
        self.products_hold_with_variables(&step.spec.products, &variables)
    }

    fn products_hold_with_variables(
        &self,
        products: &[crate::model::ProductSpec],
        variables: &BTreeMap<String, String>,
    ) -> Result<bool> {
        for product in products {
            let subject = crate::plan::interpolate(&product.subject, variables)?;
            let Some(actual) = self.store.latest_actual_value(&subject)? else {
                return Ok(false);
            };
            for (field, expected) in &product.fields {
                if actual_field(&actual, field) != Some(expected) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn evaluate_plan_gate(
        &self,
        run: &PlanRunView,
        step: &RuntimeStep<'_>,
        view: &crate::model::StepRunView,
        gate: &GateSpec,
    ) -> Result<GateOutcome> {
        let variables = run_variables(run, step, view);
        self.evaluate_context_gate(
            run,
            &view.subject,
            step.spec.title.as_deref().unwrap_or(&step.spec.path),
            &view.definition_hash,
            view.attempt,
            gate,
            &variables,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_context_gate(
        &self,
        run: &PlanRunView,
        subject: &str,
        title: &str,
        definition_hash: &str,
        attempt: u32,
        gate: &GateSpec,
        variables: &BTreeMap<String, String>,
    ) -> Result<GateOutcome> {
        let mut gate = gate.clone();
        expand_gate(&mut gate, variables, &run.workspace)?;
        if let GateSpec::Human {
            reviewer,
            question,
            review_targets,
            ..
        } = &gate
        {
            return self.evaluate_plan_human_gate(
                run,
                subject,
                title,
                definition_hash,
                attempt,
                reviewer,
                question.as_deref(),
                review_targets,
            );
        }
        let stage = CheckpointSpec {
            subject: subject.to_owned(),
            sequence: run.subject.clone(),
            name: title.to_owned(),
            ordinal: 0,
            gates: vec![gate.clone()],
        };
        self.evaluate_gate(&stage, &gate)
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_plan_human_gate(
        &self,
        run: &PlanRunView,
        subject: &str,
        title: &str,
        definition_hash: &str,
        attempt: u32,
        reviewer: &str,
        question: Option<&str>,
        review_targets: &[String],
    ) -> Result<GateOutcome> {
        let question = question
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Approve {title}?"));
        let fields = BTreeMap::from([
            ("reviewer".into(), Value::String(reviewer.into())),
            ("question".into(), Value::String(question)),
            (
                "review_targets".into(),
                Value::Array(review_targets.iter().cloned().map(Value::String).collect()),
            ),
            (
                "decisions".into(),
                Value::Array(
                    ["approved", "rejected", "revise"]
                        .into_iter()
                        .map(|value| Value::String(value.into()))
                        .collect(),
                ),
            ),
            ("plan_revision".into(), Value::String(run.revision.clone())),
            (
                "step_definition".into(),
                Value::String(definition_hash.to_owned()),
            ),
            ("attempt".into(), Value::from(attempt)),
        ]);
        let request_hash = hex::encode(sha2::Sha256::digest(serde_json::to_vec(&fields)?));
        let request = self.store.append_claim(&ClaimInput {
            subject: subject.to_owned(),
            kind: "review.requested".into(),
            actor: None,
            fields,
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(format!(
                "review-request:{}:{}",
                subject,
                &request_hash[..24]
            )),
        })?;
        let decision = self.store.latest_claim(subject, Some("review.decision"))?;
        match decision.as_ref().and_then(|claim| {
            (claim.actor.as_deref() == Some(reviewer)
                && claim
                    .body
                    .pointer("/fields/request")
                    .and_then(Value::as_str)
                    == Some(request.id.as_str()))
            .then(|| {
                claim
                    .body
                    .pointer("/fields/decision")
                    .and_then(Value::as_str)
            })
            .flatten()
        }) {
            Some("approved") => Ok(GateOutcome::Pass),
            Some("rejected") => Ok(GateOutcome::Fail(
                "the human reviewer rejected the work".into(),
            )),
            _ => Ok(GateOutcome::Pending),
        }
    }

    fn step_timed_out(
        &self,
        view: &crate::model::StepRunView,
        step: &RuntimeStep<'_>,
    ) -> Result<bool> {
        let Some(timeout) = step.spec.timeout_ms else {
            return Ok(false);
        };
        let Some(active) = self
            .store
            .latest_claim(&view.subject, Some("step-run.state"))?
        else {
            return Ok(false);
        };
        let elapsed = now_ms().saturating_sub(active.accepted_at_unix_ms);
        if elapsed >= timeout as u128 {
            return Ok(true);
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let notify = self.notify.clone();
            let remaining = (timeout as u128).saturating_sub(elapsed) as u64;
            handle.spawn(async move {
                tokio::time::sleep(Duration::from_millis(remaining)).await;
                notify.notify_one();
            });
        }
        Ok(false)
    }

    fn evaluate_checkpoints(&self, desired: &[DesiredSubject]) -> Result<()> {
        let mut stages = desired
            .iter()
            .filter(|subject| subject.kind == "checkpoint-stage")
            .filter_map(|subject| {
                serde_json::from_value::<CheckpointSpec>(subject.desired.clone())
                    .ok()
                    .map(|stage| (stage, subject.scopes.clone()))
            })
            .collect::<Vec<_>>();
        stages.sort_by_key(|(stage, _)| (stage.sequence.clone(), stage.ordinal));
        let final_ordinals =
            stages
                .iter()
                .fold(HashMap::<String, u32>::new(), |mut map, (stage, _)| {
                    map.entry(stage.sequence.clone())
                        .and_modify(|ordinal| *ordinal = (*ordinal).max(stage.ordinal))
                        .or_insert(stage.ordinal);
                    map
                });
        for (stage, scopes) in stages {
            let reached = self.current_checkpoint_reached(&stage.sequence, desired)?;
            let terminal = self
                .store
                .latest_claim(&stage.sequence, Some("checkpoint.failed"))?
                .is_some();
            let next = if terminal {
                final_ordinals.get(&stage.sequence).copied().unwrap_or(0)
            } else {
                reached.map_or(0, |ordinal| ordinal.saturating_add(1))
            };
            if stage.ordinal != next {
                continue;
            }
            self.record_once(
                &stage.subject,
                "checkpoint.active",
                BTreeMap::from([
                    ("sequence".into(), Value::String(stage.sequence.clone())),
                    ("ordinal".into(), Value::from(stage.ordinal)),
                ]),
            )?;
            let mut all_pass = true;
            let deadline_failure = stage
                .gates
                .iter()
                .filter(|gate| matches!(gate, GateSpec::Deadline { .. }))
                .map(|gate| self.evaluate_gate(&stage, gate))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .find_map(|outcome| match outcome {
                    GateOutcome::Fail(reason) => Some(reason),
                    _ => None,
                });
            if let Some(reason) = deadline_failure {
                self.fail_checkpoint(&stage, &scopes, &reason)?;
                continue;
            }
            if !self.checkpoint_subgraph_holds(&stage, desired)? {
                continue;
            }
            for gate in stage
                .gates
                .iter()
                .filter(|gate| !matches!(gate, GateSpec::Deadline { .. }))
            {
                match self.evaluate_gate(&stage, gate)? {
                    GateOutcome::Pass => {}
                    GateOutcome::Pending => {
                        all_pass = false;
                        break;
                    }
                    GateOutcome::Fail(reason) => {
                        self.fail_checkpoint(&stage, &scopes, &reason)?;
                        all_pass = false;
                        break;
                    }
                }
            }
            if all_pass {
                let definition_revision = self
                    .store
                    .selected_desired_revision(&stage.subject)?
                    .context("a checkpoint stage has no selected definition revision")?;
                self.record_once(
                    &stage.sequence,
                    "checkpoint.reached",
                    BTreeMap::from([
                        ("ordinal".into(), Value::from(stage.ordinal)),
                        ("name".into(), Value::String(stage.name)),
                        (
                            "definition_revision".into(),
                            Value::String(definition_revision),
                        ),
                    ]),
                )?;
                let final_ordinal = final_ordinals.get(&stage.sequence).copied();
                let establishes_verdict = final_ordinal == Some(stage.ordinal)
                    || final_ordinal == Some(stage.ordinal.saturating_add(1));
                if establishes_verdict
                    && let Some(scope) = scopes.iter().next()
                    && self
                        .store
                        .latest_claim(scope, Some("eval.verdict"))?
                        .is_none()
                {
                    self.record_once(
                        scope,
                        "eval.verdict",
                        BTreeMap::from([
                            ("verdict".into(), Value::String("pass".into())),
                            ("sequence".into(), Value::String(stage.sequence.clone())),
                        ]),
                    )?;
                }
                self.signal_changed();
            }
        }
        Ok(())
    }

    fn checkpoint_subgraph_holds(
        &self,
        stage: &CheckpointSpec,
        desired: &[DesiredSubject],
    ) -> Result<bool> {
        for subject in desired.iter().filter(|subject| {
            subject.activation.as_ref().is_some_and(|activation| {
                activation.sequence == stage.sequence && activation.ordinal == stage.ordinal
            })
        }) {
            let status = self
                .store
                .latest_actual_value(&subject.subject)?
                .as_ref()
                .and_then(|actual| actual_field(actual, "status"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let holds = match subject.kind.as_str() {
                "scope-stop" | "stop" => {
                    matches!(status.as_deref(), Some("stopped" | "absent" | "exited"))
                }
                "message" => matches!(status.as_deref(), Some("delivered" | "accepted" | "closed")),
                _ => match subject.member.as_ref() {
                    Some(member) if member.lifecycle == MemberLifecycle::AdoptOnly => matches!(
                        status.as_deref(),
                        Some("absent" | "running" | "ready" | "working" | "idle" | "exited")
                    ),
                    Some(member) if member.driver.is_some() => {
                        matches!(status.as_deref(), Some("ready" | "working" | "idle"))
                    }
                    Some(_) => matches!(
                        status.as_deref(),
                        Some("running" | "ready" | "working" | "idle" | "exited")
                    ),
                    None => true,
                },
            };
            if !holds {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn current_checkpoint_reached(
        &self,
        sequence: &str,
        desired: &[DesiredSubject],
    ) -> Result<Option<u32>> {
        let claims = self
            .store
            .claims_for(sequence, Some("checkpoint.reached"))?;
        let mut stages = desired
            .iter()
            .filter(|subject| subject.kind == "checkpoint-stage")
            .filter_map(|subject| {
                serde_json::from_value::<CheckpointSpec>(subject.desired.clone()).ok()
            })
            .filter(|stage| stage.sequence == sequence)
            .collect::<Vec<_>>();
        stages.sort_by_key(|stage| stage.ordinal);
        let mut reached = None;
        for stage in stages {
            let Some(revision) = self.store.selected_desired_revision(&stage.subject)? else {
                break;
            };
            let has_current_pass = claims.iter().any(|claim| {
                claim
                    .body
                    .pointer("/fields/ordinal")
                    .and_then(Value::as_u64)
                    == Some(stage.ordinal as u64)
                    && claim
                        .body
                        .pointer("/fields/definition_revision")
                        .and_then(Value::as_str)
                        == Some(revision.as_str())
            });
            if !has_current_pass {
                break;
            }
            let still_passes = stage
                .gates
                .iter()
                .filter(|gate| !matches!(gate, GateSpec::Deadline { .. }))
                .map(|gate| self.evaluate_gate(&stage, gate))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .all(|outcome| matches!(outcome, GateOutcome::Pass));
            if !still_passes {
                break;
            }
            reached = Some(stage.ordinal);
        }
        Ok(reached)
    }

    fn fail_checkpoint(
        &self,
        stage: &CheckpointSpec,
        scopes: &std::collections::BTreeSet<String>,
        reason: &str,
    ) -> Result<()> {
        self.record_once(
            &stage.sequence,
            "checkpoint.failed",
            BTreeMap::from([
                ("ordinal".into(), Value::from(stage.ordinal)),
                ("reason".into(), Value::String(reason.into())),
            ]),
        )?;
        if let Some(scope) = scopes.iter().next()
            && self
                .store
                .latest_claim(scope, Some("eval.verdict"))?
                .is_none()
        {
            self.record_once(
                scope,
                "eval.verdict",
                BTreeMap::from([
                    ("verdict".into(), Value::String("fail".into())),
                    ("reason".into(), Value::String(reason.into())),
                    ("sequence".into(), Value::String(stage.sequence.clone())),
                ]),
            )?;
        }
        self.signal_changed();
        Ok(())
    }

    fn deliver_messages(&self, desired: &[DesiredSubject]) -> Result<()> {
        for message in self.store.messages(None, false)? {
            if message.status != "sent" {
                continue;
            }
            let recipient = if message.to.starts_with("agent/") {
                message.to.clone()
            } else {
                format!("agent/{}", message.to)
            };
            let driver = desired
                .iter()
                .find(|item| item.subject == recipient)
                .and_then(|item| item.member.as_ref())
                .and_then(|member| member.driver.as_deref());
            if matches!(driver, Some("claude" | "codex")) {
                continue;
            }
            let Some(actual) = self.store.latest_actual_value(&recipient)? else {
                continue;
            };
            let Some(runtime_id) = actual_field(&actual, "runtime_id").and_then(Value::as_str)
            else {
                continue;
            };
            let status = actual_field(&actual, "status").and_then(Value::as_str);
            if !matches!(status, Some("running" | "ready" | "working" | "idle")) {
                continue;
            }
            let id = message
                .subject
                .strip_prefix("message/")
                .unwrap_or(&message.subject);
            let wake = format!(
                "[DING] new st3 message: [id:{id}] {} (from {}); run `st3 message ls`",
                message.title.as_deref().unwrap_or("message"),
                message.from
            );
            self.perform_action(&message.subject, "deliver", || {
                self.runtime.send(runtime_id, &wake)
            })?;
            self.record_once(
                &message.subject,
                "message.delivered",
                BTreeMap::from([
                    ("status".into(), Value::String("delivered".into())),
                    ("runtime_id".into(), Value::String(runtime_id.into())),
                ]),
            )?;
            if let Some(root) = desired
                .iter()
                .find(|item| item.subject == recipient)
                .and_then(|item| item.member.as_ref())
                .and_then(|member| member.environment.get("ST3_MESSAGE_ROOT"))
            {
                crate::projection::export_messages(
                    Path::new(root),
                    &self.store.messages(None, true)?,
                )?;
            }
        }
        Ok(())
    }

    fn link_blocks(&self, subject: &DesiredSubject, desired: &[DesiredSubject]) -> Result<bool> {
        for link in desired.iter().filter(|item| item.kind == "link") {
            let Some(spec) = crate::graph::link_spec(&link.desired) else {
                continue;
            };
            if !spec.required || spec.from != subject.subject {
                continue;
            }
            let target = self.store.latest_actual_value(&spec.to)?;
            let reachable = target.as_ref().is_some_and(|value| {
                let status = actual_field(value, "status").and_then(Value::as_str);
                let reachability = actual_field(value, "reachability").and_then(Value::as_str);
                !matches!(
                    status,
                    Some("absent" | "stopped" | "exited" | "unreachable" | "indeterminate")
                ) && !matches!(reachability, Some("unreachable" | "indeterminate"))
            });
            if reachable {
                continue;
            }
            self.record_once(
                &subject.subject,
                "supervision.decision",
                BTreeMap::from([
                    ("decision".into(), Value::String("hold".into())),
                    (
                        "reason".into(),
                        Value::String(format!(
                            "required link `{}` cannot reach `{}`",
                            link.subject, spec.to
                        )),
                    ),
                    ("reachability".into(), Value::String("unreachable".into())),
                ]),
            )?;
            if spec.on_unreachable == "void"
                && let Some(scope) = subject.scopes.iter().next()
            {
                self.record_once(
                    scope,
                    "eval.verdict",
                    BTreeMap::from([
                        ("verdict".into(), Value::String("void".into())),
                        (
                            "reason".into(),
                            Value::String(format!(
                                "required link `{}` is unreachable",
                                link.subject
                            )),
                        ),
                    ]),
                )?;
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn reconcile_gates(
        &self,
        subject: &DesiredSubject,
        member: &MemberSpec,
        desired: &[DesiredSubject],
    ) -> Result<()> {
        let Some(driver) = member.driver.as_deref() else {
            return Ok(());
        };
        let Some(supervisor) = desired
            .iter()
            .find(|item| item.subject == member.supervisor && item.kind == "supervisor")
        else {
            return Ok(());
        };
        let screen = match self.runtime.screen(&member.runtime_id) {
            Ok(screen) => screen,
            Err(_) => return Ok(()),
        };
        let normalized = screen.lines().map(str::trim).collect::<Vec<_>>().join("\n");
        for gate in crate::graph::supervisor_terminal_controls(&supervisor.desired)
            .into_iter()
            .filter(|gate| gate.driver == driver)
        {
            let matches = gate
                .contains
                .iter()
                .all(|needle| normalized.contains(needle))
                && gate
                    .selected
                    .as_ref()
                    .is_none_or(|line| normalized.lines().any(|candidate| candidate == line));
            if !matches {
                continue;
            }
            let prior = self
                .store
                .claims_for(&subject.subject, Some("supervision.decision"))?
                .into_iter()
                .filter(|claim| {
                    claim.body.pointer("/fields/gate").and_then(Value::as_str) == Some(&gate.name)
                })
                .count() as u32;
            if prior >= gate.max_inputs {
                self.record_once(
                    &subject.subject,
                    "supervision.decision",
                    BTreeMap::from([
                        ("decision".into(), Value::String("raise".into())),
                        ("gate".into(), Value::String(gate.name)),
                        ("reachability".into(), Value::String("unreachable".into())),
                        (
                            "reason".into(),
                            Value::String("the gate stayed visible after its input limit".into()),
                        ),
                    ]),
                )?;
                return Ok(());
            }
            let key = gate
                .keys
                .get(prior as usize)
                .or_else(|| gate.keys.last())
                .context("a validated gate has no key")?;
            self.perform_action(&subject.subject, "gate-input", || {
                self.runtime.send_key(&member.runtime_id, key)
            })?;
            self.store.append_claim(&ClaimInput {
                subject: subject.subject.clone(),
                kind: "supervision.decision".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("decision".into(), Value::String("input".into())),
                    ("gate".into(), Value::String(gate.name)),
                    ("key".into(), Value::String(key.clone())),
                    ("input_number".into(), Value::from(prior.saturating_add(1))),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!(
                    "gate:{}:{}:{}",
                    subject.subject, supervisor.subject, prior
                )),
            })?;
            self.signal_changed();
            return Ok(());
        }
        Ok(())
    }

    fn reconcile_schedules(&self, desired: &[DesiredSubject]) -> Result<()> {
        for schedule in desired.iter().filter(|item| item.kind == "schedule") {
            let Some(spec) = crate::graph::schedule_spec(&schedule.desired, &self.host) else {
                continue;
            };
            if spec.stopped || spec.host != self.host {
                continue;
            }
            let Some(revision) = self.store.selected_desired_revision(&schedule.subject)? else {
                continue;
            };
            let reached = self
                .store
                .claims_for(&schedule.subject, Some("clock.reached"))?;
            let last = reached
                .iter()
                .filter(|claim| {
                    claim
                        .body
                        .pointer("/fields/revision")
                        .and_then(Value::as_str)
                        == Some(&revision)
                })
                .filter_map(|claim| {
                    claim
                        .body
                        .pointer("/fields/occurrence")
                        .and_then(Value::as_u64)
                })
                .max();
            let now = now_ms() as i64;
            let (occurrence, scheduled_at) = if let Some(at) = spec.at_unix_ms {
                if last.is_some() {
                    continue;
                }
                (0_u64, at)
            } else {
                let Some(interval) = spec.every_ms else {
                    continue;
                };
                let Some(anchor) = spec.anchor_unix_ms else {
                    continue;
                };
                let current = if now < anchor {
                    0
                } else {
                    ((now - anchor) as u64) / interval
                };
                let mut next = last.map_or(0, |value| value.saturating_add(1));
                if now >= anchor && next <= current {
                    match spec.catch_up.as_str() {
                        "latest" => next = current,
                        "skip" => next = current.saturating_add(1),
                        "all" => {
                            let remaining = current.saturating_sub(next).saturating_add(1);
                            if remaining > spec.max_catch_up.unwrap_or(0) as u64 {
                                self.record_once(
                                    &schedule.subject,
                                    "supervision.decision",
                                    BTreeMap::from([
                                        ("decision".into(), Value::String("raise".into())),
                                        (
                                            "reachability".into(),
                                            Value::String("unreachable".into()),
                                        ),
                                        (
                                            "reason".into(),
                                            Value::String(
                                                "the schedule exceeds max-catch-up".into(),
                                            ),
                                        ),
                                    ]),
                                )?;
                                continue;
                            }
                        }
                        _ => continue,
                    }
                }
                let offset = interval
                    .checked_mul(next)
                    .context("schedule occurrence overflow")?;
                let scheduled = anchor
                    .checked_add(offset as i64)
                    .context("schedule timestamp overflow")?;
                (next, scheduled)
            };
            let operation = format!("{}:{revision}:{occurrence}", schedule.subject);
            if !self
                .armed_schedules
                .lock()
                .expect("schedule mutex poisoned")
                .insert(operation.clone())
            {
                continue;
            }
            let request = self.store.append_claim(&ClaimInput {
                subject: schedule.subject.clone(),
                kind: "clock.wake.requested".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("revision".into(), Value::String(revision.clone())),
                    ("occurrence".into(), Value::from(occurrence)),
                    (
                        "scheduled_at_unix_ms".into(),
                        Value::String(scheduled_at.to_string()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some(format!("clock-wake:{operation}")),
            })?;
            self.event_notify.notify_waiters();
            let template = spec.message.clone();
            let store = self.store.clone();
            let notify = self.notify.clone();
            let event_notify = self.event_notify.clone();
            let armed = self.armed_schedules.clone();
            let schedule_subject = schedule.subject.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let delay = scheduled_at.saturating_sub(now_ms() as i64).max(0) as u64;
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    if store
                        .selected_desired_revision(&schedule_subject)
                        .ok()
                        .flatten()
                        .as_deref()
                        != Some(revision.as_str())
                    {
                        let _ = store.append_claim(&ClaimInput {
                            subject: schedule_subject.clone(),
                            kind: "clock.wake.cancel.requested".into(),
                            actor: None,
                            fields: BTreeMap::from([
                                ("revision".into(), Value::String(revision.clone())),
                                ("occurrence".into(), Value::from(occurrence)),
                                (
                                    "reason".into(),
                                    Value::String("the schedule revision changed".into()),
                                ),
                            ]),
                            evidence: vec![request.id.clone()],
                            expected_subject: None,
                            idempotency_key: Some(format!("clock-cancel:{operation}")),
                        });
                        armed
                            .lock()
                            .expect("schedule mutex poisoned")
                            .remove(&operation);
                        signal_changed(&notify, &event_notify);
                        return;
                    }
                    let reached = store.append_claim(&ClaimInput {
                        subject: schedule_subject.clone(),
                        kind: "clock.reached".into(),
                        actor: None,
                        fields: BTreeMap::from([
                            ("revision".into(), Value::String(revision.clone())),
                            ("occurrence".into(), Value::from(occurrence)),
                            (
                                "scheduled_at_unix_ms".into(),
                                Value::String(scheduled_at.to_string()),
                            ),
                        ]),
                        evidence: vec![request.id],
                        expected_subject: None,
                        idempotency_key: Some(format!("clock-reached:{operation}")),
                    });
                    if let (Ok(reached), Some(template)) = (reached, template) {
                        let message_hash = hex::encode(sha2::Sha256::digest(operation.as_bytes()));
                        let message_subject = format!("message/schedule-{}", &message_hash[..20]);
                        let _ = store.append_claim(&ClaimInput {
                            subject: message_subject,
                            kind: "message.sent".into(),
                            actor: None,
                            fields: BTreeMap::from([
                                ("from".into(), Value::String(template.from)),
                                ("to".into(), Value::String(template.to)),
                                ("content".into(), Value::String(template.content)),
                                ("status".into(), Value::String("sent".into())),
                            ]),
                            evidence: vec![reached.id],
                            expected_subject: None,
                            idempotency_key: Some(format!("schedule-message:{operation}")),
                        });
                    }
                    armed
                        .lock()
                        .expect("schedule mutex poisoned")
                        .remove(&operation);
                    signal_changed(&notify, &event_notify);
                });
            } else {
                self.armed_schedules
                    .lock()
                    .expect("schedule mutex poisoned")
                    .remove(&operation);
            }
        }
        Ok(())
    }

    fn evaluate_gate(&self, stage: &CheckpointSpec, gate: &GateSpec) -> Result<GateOutcome> {
        let outcome = match gate {
            GateSpec::Exists { subject, .. } => {
                self.ensure_file_observation(subject)?;
                if self
                    .store
                    .latest_actual_value(subject)?
                    .is_some_and(|actual| {
                        actual_field(&actual, "status").and_then(Value::as_str)
                            != Some("unreadable")
                    })
                {
                    GateOutcome::Pass
                } else {
                    GateOutcome::Pending
                }
            }
            GateSpec::Empty { subject, .. } => {
                let members = self
                    .store
                    .desired_subjects()?
                    .into_iter()
                    .filter(|item| item.scopes.contains(subject) && item.member.is_some())
                    .collect::<Vec<_>>();
                let mut empty = true;
                for member in members {
                    if self
                        .store
                        .latest_actual_value(&member.subject)?
                        .is_some_and(|value| {
                            !matches!(
                                actual_field(&value, "status").and_then(Value::as_str),
                                Some("absent" | "stopped" | "exited")
                            )
                        })
                    {
                        empty = false;
                    }
                }
                if empty {
                    GateOutcome::Pass
                } else {
                    GateOutcome::Pending
                }
            }
            GateSpec::Field {
                path,
                subject,
                operator,
                value,
                ..
            } => {
                self.ensure_file_observation(subject)?;
                let Some(actual) = self.store.latest_actual_value(subject)? else {
                    return Ok(GateOutcome::Pending);
                };
                let found = if subject.starts_with("file/") {
                    actual_field(&actual, "content")
                        .and_then(Value::as_str)
                        .and_then(|content| serde_json::from_str::<Value>(content).ok())
                        .and_then(|content| owned_field(content, path))
                } else {
                    actual_field(&actual, path).cloned()
                };
                let Some(found) = found.as_ref() else {
                    return Ok(GateOutcome::Pending);
                };
                if compare_value(found, operator, value) {
                    GateOutcome::Pass
                } else {
                    GateOutcome::Pending
                }
            }
            GateSpec::Has { subject, text, .. } | GateSpec::Lacks { subject, text, .. } => {
                self.ensure_file_observation(subject)?;
                let Some(content) = self.subject_text(subject)? else {
                    return Ok(GateOutcome::Pending);
                };
                let contains = content.contains(text);
                let pass = matches!(gate, GateSpec::Has { .. }) == contains;
                if pass {
                    GateOutcome::Pass
                } else {
                    GateOutcome::Pending
                }
            }
            GateSpec::Deadline { duration_ms, .. } => {
                let Some(active) = self
                    .store
                    .latest_claim(&stage.subject, Some("checkpoint.active"))?
                else {
                    return Ok(GateOutcome::Pending);
                };
                let elapsed = now_ms().saturating_sub(active.accepted_at_unix_ms);
                if elapsed >= *duration_ms as u128 {
                    GateOutcome::Fail(format!("deadline expired after {duration_ms}ms"))
                } else {
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let notify = self.notify.clone();
                        let remaining = (*duration_ms as u128).saturating_sub(elapsed) as u64;
                        handle.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(remaining)).await;
                            notify.notify_one();
                        });
                    }
                    GateOutcome::Pass
                }
            }
            GateSpec::Mechanical {
                name,
                command,
                host,
                workspace,
                environment,
                time_limit_ms,
                ..
            } => self.run_mechanical(
                stage,
                name,
                command,
                host,
                workspace,
                environment,
                *time_limit_ms,
            )?,
            GateSpec::Llm {
                name,
                model,
                host,
                workspace,
                tools,
                environment,
                token_budget,
                time_limit_ms,
                prompt,
            } => self.run_llm_gate(
                stage,
                name,
                model,
                host,
                workspace,
                tools,
                environment,
                *token_budget,
                *time_limit_ms,
                prompt,
            )?,
            GateSpec::Human { reviewer, .. } => {
                let decision = self
                    .store
                    .latest_claim(&stage.subject, Some("review.decision"))?;
                match decision.as_ref().and_then(|claim| {
                    (claim.actor.as_deref() == Some(reviewer.as_str()))
                        .then(|| {
                            claim
                                .body
                                .pointer("/fields/decision")
                                .and_then(Value::as_str)
                        })
                        .flatten()
                }) {
                    Some("approved") => GateOutcome::Pass,
                    Some("rejected") => {
                        GateOutcome::Fail("the human reviewer rejected the work".into())
                    }
                    _ => GateOutcome::Pending,
                }
            }
        };
        if matches!(outcome, GateOutcome::Pass | GateOutcome::Fail(_)) {
            let name = crate::graph::gate_name(gate);
            let digest = hex::encode(sha2::Sha256::digest(
                format!("{}:{name}", stage.subject).as_bytes(),
            ));
            let (verdict, reason) = match &outcome {
                GateOutcome::Pass => ("pass", None),
                GateOutcome::Fail(reason) => ("fail", Some(reason.clone())),
                GateOutcome::Pending => unreachable!(),
            };
            let mut fields = BTreeMap::from([
                ("stage".into(), Value::String(stage.subject.clone())),
                ("gate".into(), Value::String(name.to_owned())),
                ("verdict".into(), Value::String(verdict.into())),
            ]);
            if let Some(reason) = reason {
                fields.insert("reason".into(), Value::String(reason));
            }
            self.record_once(
                &format!("gate-result/{}", &digest[..32]),
                "gate.result",
                fields,
            )?;
        }
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_mechanical(
        &self,
        stage: &CheckpointSpec,
        name: &str,
        command: &str,
        host: &str,
        workspace: &str,
        environment: &BTreeMap<String, String>,
        time_limit_ms: u64,
    ) -> Result<GateOutcome> {
        let result_subject = gate_operation_subject(
            stage,
            name,
            &serde_json::json!({
                "type": "mechanical",
                "command": command,
                "host": host,
                "workspace": workspace,
                "environment": environment,
                "time_limit_ms": time_limit_ms,
            }),
        )?;
        let runtime_id = result_subject.replace('/', ".");
        if let Some(result) = self
            .store
            .latest_claim(&result_subject, Some("gate.result"))?
        {
            let verdict = result
                .body
                .pointer("/fields/verdict")
                .and_then(Value::as_str)
                .unwrap_or("fail");
            let reason = result
                .body
                .pointer("/fields/reason")
                .and_then(Value::as_str)
                .unwrap_or("the mechanical gate failed");
            return Ok(if verdict == "pass" {
                GateOutcome::Pass
            } else {
                GateOutcome::Fail(reason.into())
            });
        }
        if host != self.host {
            return Ok(GateOutcome::Pending);
        }
        if let Some(requested) = self
            .store
            .latest_claim(&result_subject, Some("gate.requested"))?
        {
            let elapsed = now_ms().saturating_sub(requested.accepted_at_unix_ms);
            if elapsed >= time_limit_ms as u128 {
                self.stop_gate_runner(&result_subject, true)?;
                let reason = format!("mechanical gate `{name}` exceeded {time_limit_ms}ms");
                self.record_once(
                    &result_subject,
                    "gate.result",
                    BTreeMap::from([
                        ("verdict".into(), Value::String("fail".into())),
                        ("reason".into(), Value::String(reason.clone())),
                    ]),
                )?;
                return Ok(GateOutcome::Fail(reason));
            }
            match self.runtime.observe_exec(&runtime_id)? {
                Some(observation) if observation.status == "running" => {
                    self.arm_gate_poll();
                    return Ok(GateOutcome::Pending);
                }
                Some(observation) if observation.status == "exited" => {
                    let verdict = if observation.exit_code == Some(0) {
                        "pass"
                    } else {
                        "fail"
                    };
                    let reason = format!("mechanical gate `{name}` {verdict}");
                    self.record_once(
                        &result_subject,
                        "gate.result",
                        BTreeMap::from([
                            ("verdict".into(), Value::String(verdict.into())),
                            ("reason".into(), Value::String(reason.clone())),
                        ]),
                    )?;
                    return Ok(if verdict == "pass" {
                        GateOutcome::Pass
                    } else {
                        GateOutcome::Fail(reason)
                    });
                }
                _ => {
                    self.arm_gate_poll();
                    return Ok(GateOutcome::Pending);
                }
            }
        }

        self.record_once(
            &result_subject,
            "gate.requested",
            BTreeMap::from([
                ("status".into(), Value::String("requested".into())),
                ("runner".into(), Value::String("exec".into())),
            ]),
        )?;
        let member = MemberSpec {
            kind: MemberKind::Exec,
            host: host.into(),
            runtime_id,
            workspace: workspace.into(),
            cwd: workspace.into(),
            terminal: false,
            launch: LaunchSpec::Shell(command.into()),
            environment: environment.clone(),
            tags: BTreeMap::new(),
            display_name: None,
            lifecycle: MemberLifecycle::Service,
            restart: RestartType::Never,
            restart_intensity: RestartIntensity::default(),
            shutdown_timeout_ms: 5_000,
            driver: Some("mechanical-gate".into()),
            supervisor: "supervisor/root".into(),
        };
        let desired = DesiredSubject {
            subject: result_subject,
            kind: "gate".into(),
            desired: Value::Null,
            member: Some(member.clone()),
            activation: None,
            scopes: Default::default(),
        };
        self.perform_start(&desired, &member, "the mechanical gate was requested")?;
        self.arm_gate_poll();
        Ok(GateOutcome::Pending)
    }

    fn arm_gate_poll(&self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let notify = self.notify.clone();
            handle.spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                notify.notify_one();
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_llm_gate(
        &self,
        stage: &CheckpointSpec,
        name: &str,
        model: &str,
        host: &str,
        workspace: &str,
        tools: &[String],
        environment: &BTreeMap<String, String>,
        token_budget: u64,
        time_limit_ms: u64,
        prompt: &str,
    ) -> Result<GateOutcome> {
        let result_subject = gate_operation_subject(
            stage,
            name,
            &serde_json::json!({
                "type": "llm",
                "model": model,
                "host": host,
                "workspace": workspace,
                "tools": tools,
                "environment": environment,
                "token_budget": token_budget,
                "time_limit_ms": time_limit_ms,
                "prompt": prompt,
            }),
        )?;
        let runtime_id = result_subject.replace('/', ".");
        if let Some(result) = self.store.latest_actual_value(&result_subject)? {
            let verdict = actual_field(&result, "verdict")
                .and_then(Value::as_str)
                .unwrap_or("fail");
            let reason = actual_field(&result, "reason")
                .and_then(Value::as_str)
                .unwrap_or("the LLM gate failed");
            if let Some(token_usage) = actual_field(&result, "token_usage").and_then(Value::as_u64)
            {
                self.stop_gate_runner(&result_subject, false)?;
                if token_usage > token_budget {
                    return Ok(GateOutcome::Fail(format!(
                        "LLM gate `{name}` used {token_usage} tokens, above its {token_budget} token budget"
                    )));
                }
                return Ok(if verdict == "pass" {
                    GateOutcome::Pass
                } else {
                    GateOutcome::Fail(reason.into())
                });
            }
            match self.runtime.observe_exec(&runtime_id)? {
                Some(observation) if observation.status == "running" => {
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let notify = self.notify.clone();
                        handle.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            notify.notify_one();
                        });
                    }
                    return Ok(GateOutcome::Pending);
                }
                Some(observation) if observation.status == "indeterminate" => {
                    return Ok(GateOutcome::Pending);
                }
                _ => {}
            }
            let token_usage = self
                .runtime
                .read_exec_log(&runtime_id)?
                .as_deref()
                .and_then(structured_token_usage);
            let (verdict, reason, token_usage) = match token_usage {
                Some(token_usage) if token_usage > token_budget => (
                    "fail",
                    format!(
                        "LLM gate `{name}` used {token_usage} tokens, above its {token_budget} token budget"
                    ),
                    token_usage,
                ),
                Some(token_usage) => (verdict, reason.into(), token_usage),
                None => (
                    "fail",
                    format!("LLM gate `{name}` did not report structured token usage"),
                    0,
                ),
            };
            self.record_once(
                &result_subject,
                "gate.result",
                BTreeMap::from([
                    ("verdict".into(), Value::String(verdict.into())),
                    ("reason".into(), Value::String(reason.clone())),
                    ("token_usage".into(), Value::from(token_usage)),
                ]),
            )?;
            return Ok(if verdict == "pass" {
                GateOutcome::Pass
            } else {
                GateOutcome::Fail(reason)
            });
        }
        if host != self.host {
            return Ok(GateOutcome::Pending);
        }
        if let Some(requested) = self
            .store
            .latest_claim(&result_subject, Some("gate.requested"))?
        {
            let elapsed = now_ms().saturating_sub(requested.accepted_at_unix_ms);
            if elapsed >= time_limit_ms as u128 {
                self.stop_gate_runner(&result_subject, true)?;
                self.record_once(
                    &result_subject,
                    "gate.result",
                    BTreeMap::from([
                        ("verdict".into(), Value::String("fail".into())),
                        (
                            "reason".into(),
                            Value::String(format!("LLM gate `{name}` exceeded {time_limit_ms}ms")),
                        ),
                        ("token_usage".into(), Value::from(0)),
                    ]),
                )?;
                return Ok(GateOutcome::Fail(format!(
                    "LLM gate `{name}` exceeded {time_limit_ms}ms"
                )));
            }
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let notify = self.notify.clone();
                let remaining = (time_limit_ms as u128).saturating_sub(elapsed) as u64;
                handle.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(remaining)).await;
                    notify.notify_one();
                });
            }
            return Ok(GateOutcome::Pending);
        }

        let (capability, capability_expires_at) =
            self.store
                .issue_capability("gate-result", &result_subject, None, time_limit_ms)?;
        self.record_once(
            &result_subject,
            "gate.requested",
            BTreeMap::from([
                ("status".into(), Value::String("requested".into())),
                ("model".into(), Value::String(model.into())),
                ("token_budget".into(), Value::from(token_budget)),
                (
                    "tools".into(),
                    Value::Array(tools.iter().cloned().map(Value::String).collect()),
                ),
                (
                    "capability_hash".into(),
                    Value::String(hex::encode(sha2::Sha256::digest(capability.as_bytes()))),
                ),
                (
                    "capability_expires_at".into(),
                    Value::String(capability_expires_at.to_string()),
                ),
            ]),
        )?;
        let instruction = format!(
            "{prompt}\n\nYou are a held-out st3 gate. Inspect only the declared workspace and tools. When you decide, run exactly one of these commands:\n  st3 gate-result pass --reason 'REASON'\n  st3 gate-result fail --reason 'REASON'\nDo not finish without posting a gate-result."
        );
        let argv = if model.starts_with("claude") {
            vec![
                "claude".into(),
                "-p".into(),
                "--model".into(),
                model.into(),
                "--permission-mode".into(),
                "bypassPermissions".into(),
                "--output-format".into(),
                "json".into(),
                instruction,
            ]
        } else {
            vec![
                "codex".into(),
                "exec".into(),
                "--dangerously-bypass-approvals-and-sandbox".into(),
                "--model".into(),
                model.into(),
                "--json".into(),
                instruction,
            ]
        };
        let mut environment = environment.clone();
        environment.insert("ST_GATE_SUBJECT".into(), result_subject.clone());
        environment.insert("ST_GATE_CAPABILITY".into(), capability);
        environment.insert("ST3_TOKEN_BUDGET".into(), token_budget.to_string());
        let member = MemberSpec {
            kind: MemberKind::Exec,
            host: host.into(),
            runtime_id: result_subject.replace('/', "."),
            workspace: workspace.into(),
            cwd: workspace.into(),
            terminal: false,
            launch: LaunchSpec::Argv(argv),
            environment,
            tags: BTreeMap::new(),
            display_name: None,
            lifecycle: MemberLifecycle::Service,
            restart: RestartType::Never,
            restart_intensity: RestartIntensity::default(),
            shutdown_timeout_ms: 5_000,
            driver: Some("llm-gate".into()),
            supervisor: "supervisor/root".into(),
        };
        let desired = DesiredSubject {
            subject: result_subject,
            kind: "gate".into(),
            desired: Value::Null,
            member: Some(member.clone()),
            activation: None,
            scopes: Default::default(),
        };
        self.perform_start(&desired, &member, "the LLM gate was requested")?;
        Ok(GateOutcome::Pending)
    }

    fn stop_gate_runner(&self, subject: &str, hard: bool) -> Result<()> {
        let runtime_id = subject.replace('/', ".");
        let Some(observation) = self.runtime.observe_exec(&runtime_id)? else {
            return Ok(());
        };
        if observation.status != "running" {
            return Ok(());
        }
        let incarnation = observation.incarnation_id.as_deref();
        let action = if hard { "kill-gate" } else { "stop-gate" };
        if self
            .store
            .claims_for(subject, Some("action.completed"))?
            .iter()
            .any(|claim| {
                claim.body.pointer("/fields/action").and_then(Value::as_str) == Some(action)
                    && claim
                        .body
                        .pointer("/fields/incarnation_id")
                        .and_then(Value::as_str)
                        == incarnation
            })
        {
            return Ok(());
        }
        let request = self.store.append_claim(&ClaimInput {
            subject: subject.into(),
            kind: "action.requested".into(),
            actor: None,
            fields: BTreeMap::from([
                ("action".into(), Value::String(action.into())),
                ("runtime_id".into(), Value::String(runtime_id.clone())),
                (
                    "incarnation_id".into(),
                    incarnation.map_or(Value::Null, |value| Value::String(value.into())),
                ),
            ]),
            evidence: Vec::new(),
            expected_subject: None,
            idempotency_key: Some(format!(
                "{action}:{subject}:{}",
                incarnation.unwrap_or("unknown")
            )),
        })?;
        if hard {
            self.runtime.kill(&runtime_id, false, incarnation)?;
        } else {
            self.runtime.stop(&runtime_id, false, incarnation)?;
        }
        self.store.append_claim(&ClaimInput {
            subject: subject.into(),
            kind: "action.completed".into(),
            actor: None,
            fields: BTreeMap::from([
                ("action".into(), Value::String(action.into())),
                (
                    "incarnation_id".into(),
                    incarnation.map_or(Value::Null, |value| Value::String(value.into())),
                ),
            ]),
            evidence: vec![request.id],
            expected_subject: None,
            idempotency_key: Some(format!(
                "{action}-complete:{subject}:{}",
                incarnation.unwrap_or("unknown")
            )),
        })?;
        Ok(())
    }

    fn subject_text(&self, subject: &str) -> Result<Option<String>> {
        if subject.starts_with("doc/") {
            if let Some((name, hash)) = subject.rsplit_once('@') {
                return self
                    .store
                    .get_document(name, hash)?
                    .map(|bytes| String::from_utf8(bytes).map_err(Into::into))
                    .transpose();
            }
            let Some(hash) = self.store.latest_document_hash(subject)? else {
                return Ok(None);
            };
            return self
                .store
                .get_document(subject, &hash)?
                .map(|bytes| String::from_utf8(bytes).map_err(Into::into))
                .transpose();
        }
        let Some(actual) = self.store.latest_actual_value(subject)? else {
            return Ok(None);
        };
        if let Some(content) = actual_field(&actual, "content").and_then(Value::as_str) {
            return Ok(Some(content.into()));
        }
        let Some(hash) = actual_field(&actual, "blob_hash").and_then(Value::as_str) else {
            return Ok(None);
        };
        self.store
            .get_blob(hash)?
            .map(|bytes| String::from_utf8(bytes).map_err(Into::into))
            .transpose()
    }

    fn ensure_file_observation(&self, subject: &str) -> Result<()> {
        let Some(rest) = subject.strip_prefix("file/") else {
            return Ok(());
        };
        let Some((host, path)) = rest.split_once(':') else {
            return Ok(());
        };
        if host != self.host {
            return Ok(());
        }
        self.ensure_file_watch(subject, Path::new(path))?;
        match std::fs::read(path) {
            Ok(bytes) => {
                let blob_hash = self.store.put_blob(&bytes)?;
                let content_hash = hex::encode(sha2::Sha256::digest(&bytes));
                let mode = std::fs::metadata(path).ok().map(|metadata| {
                    use std::os::unix::fs::PermissionsExt as _;
                    metadata.permissions().mode() & 0o7777
                });
                let mut fields = BTreeMap::from([
                    ("status".into(), Value::String("observed".into())),
                    ("path".into(), Value::String(path.into())),
                    ("content_hash".into(), Value::String(content_hash)),
                    ("blob_hash".into(), Value::String(blob_hash)),
                    (
                        "content".into(),
                        Value::String(String::from_utf8_lossy(&bytes).into_owned()),
                    ),
                ]);
                if let Some(mode) = mode {
                    fields.insert("mode".into(), Value::from(mode));
                }
                self.record_once(subject, "resource.file-observed", fields)?;
            }
            Err(error) => {
                self.record_once(
                    subject,
                    "resource.file-observed",
                    BTreeMap::from([
                        ("status".into(), Value::String("unreadable".into())),
                        ("path".into(), Value::String(path.into())),
                        ("reason".into(), Value::String(error.to_string())),
                    ]),
                )?;
            }
        }
        Ok(())
    }

    fn ensure_file_watch(&self, subject: &str, path: &Path) -> Result<()> {
        let mut watchers = self
            .file_watchers
            .lock()
            .expect("file watcher mutex poisoned");
        if watchers.contains_key(subject) {
            return Ok(());
        }
        let notify = self.notify.clone();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                if event.is_ok() {
                    notify.notify_one();
                }
            })?;
        let watched = path
            .ancestors()
            .find(|candidate| candidate.exists())
            .unwrap_or(path);
        watcher.watch(watched, notify::RecursiveMode::NonRecursive)?;
        watchers.insert(subject.into(), watcher);
        Ok(())
    }
}

struct RuntimeStep<'a> {
    spec: &'a StepSpec,
    dependency_prefix: String,
    parent: Option<String>,
}

fn flatten_plan_steps(plan: &PlanSpec) -> Vec<RuntimeStep<'_>> {
    fn append<'a>(
        plan: &'a PlanSpec,
        dependency_prefix: String,
        parent: Option<String>,
        output: &mut Vec<RuntimeStep<'a>>,
    ) {
        for id in &plan.display_order {
            let step = &plan.steps[id];
            output.push(RuntimeStep {
                spec: step,
                dependency_prefix: dependency_prefix.clone(),
                parent: parent.clone(),
            });
            if let Some(nested) = &step.nested_plan {
                append(
                    nested,
                    format!("{}/{}", step.path, nested.id),
                    Some(step.path.clone()),
                    output,
                );
            }
        }
    }
    let mut output = Vec::new();
    append(plan, String::new(), None, &mut output);
    output
}

fn run_variables(
    run: &PlanRunView,
    step: &RuntimeStep<'_>,
    view: &crate::model::StepRunView,
) -> BTreeMap<String, String> {
    let parent_step_run = step
        .parent
        .as_ref()
        .map(|path| {
            format!(
                "step-run/{}/{}",
                run.generation
                    .strip_prefix("run-generation/")
                    .unwrap_or(&run.generation),
                path
            )
        })
        .or_else(|| run.parent_step_run.clone())
        .unwrap_or_default();
    BTreeMap::from([
        (
            "ST_PLAN".into(),
            run.plan.strip_prefix("plan/").unwrap_or(&run.plan).into(),
        ),
        ("ST_PLAN_REVISION".into(), run.revision.clone()),
        ("ST_PLAN_RUN".into(), run.id.clone()),
        (
            "ST_RUN_GENERATION".into(),
            run.generation
                .strip_prefix("run-generation/")
                .unwrap_or(&run.generation)
                .into(),
        ),
        ("ST_SCOPE".into(), run.run_scope.clone().unwrap_or_default()),
        ("ST_WORKSPACE".into(), run.workspace.clone()),
        ("ST_STEP".into(), step.spec.path.clone()),
        ("ST_STEP_RUN".into(), view.subject.clone()),
        ("ST_ATTEMPT".into(), view.attempt.to_string()),
        (
            "ST_ASSIGNEE".into(),
            view.assignee.clone().unwrap_or_default(),
        ),
        ("ST_REQUESTER".into(), run.requester.clone()),
        ("ST_PARENT_STEP_RUN".into(), parent_step_run),
        ("ST_ROOT_PLAN_RUN".into(), run.root_plan_run.clone()),
    ])
}

fn expand_gate(
    gate: &mut GateSpec,
    variables: &BTreeMap<String, String>,
    run_workspace: &str,
) -> Result<()> {
    let expand = |value: &mut String| -> Result<()> {
        *value = crate::plan::interpolate(value, variables)?;
        Ok(())
    };
    match gate {
        GateSpec::Exists { subject, .. }
        | GateSpec::Empty { subject, .. }
        | GateSpec::Field { subject, .. }
        | GateSpec::Has { subject, .. }
        | GateSpec::Lacks { subject, .. } => expand(subject)?,
        GateSpec::Mechanical {
            name,
            command,
            host,
            workspace,
            environment,
            ..
        } => {
            expand(command)?;
            expand(host)?;
            expand(workspace)?;
            if Path::new(workspace).is_relative() {
                *workspace = Path::new(run_workspace)
                    .join(&*workspace)
                    .to_string_lossy()
                    .into_owned();
            }
            for value in environment.values_mut() {
                expand(value)?;
            }
            environment.extend(variables.clone());
            environment.insert("ST_GATE".into(), name.clone());
        }
        GateSpec::Llm {
            name,
            model,
            host,
            workspace,
            environment,
            prompt,
            ..
        } => {
            expand(model)?;
            expand(host)?;
            expand(workspace)?;
            expand(prompt)?;
            if Path::new(workspace).is_relative() {
                *workspace = Path::new(run_workspace)
                    .join(&*workspace)
                    .to_string_lossy()
                    .into_owned();
            }
            for value in environment.values_mut() {
                expand(value)?;
            }
            environment.extend(variables.clone());
            environment.insert("ST_GATE".into(), name.clone());
        }
        GateSpec::Human {
            reviewer,
            question,
            review_targets,
            ..
        } => {
            expand(reviewer)?;
            if let Some(question) = question {
                expand(question)?;
            }
            for target in review_targets {
                expand(target)?;
            }
        }
        GateSpec::Deadline { .. } => {}
    }
    Ok(())
}

fn signal_changed(reconcile_notify: &Notify, event_notify: &Notify) {
    reconcile_notify.notify_one();
    event_notify.notify_waiters();
}

fn structured_token_usage(log: &str) -> Option<u64> {
    let mut usages = Vec::new();
    let trimmed = log.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        collect_token_usage(&value, &mut usages);
    } else {
        for line in log.lines().map(str::trim).filter(|line| !line.is_empty()) {
            if let Ok(value) = serde_json::from_str::<Value>(line) {
                collect_token_usage(&value, &mut usages);
            }
        }
    }
    usages.into_iter().max()
}

fn collect_token_usage(value: &Value, usages: &mut Vec<u64>) {
    match value {
        Value::Object(object) => {
            if let Some(Value::Object(usage)) = object.get("usage")
                && let Some(total) = token_usage_total(usage)
            {
                usages.push(total);
            }
            for nested in object.values() {
                collect_token_usage(nested, usages);
            }
        }
        Value::Array(values) => {
            for nested in values {
                collect_token_usage(nested, usages);
            }
        }
        _ => {}
    }
}

fn token_usage_total(usage: &serde_json::Map<String, Value>) -> Option<u64> {
    for key in ["total_tokens", "totalTokens"] {
        if let Some(total) = usage.get(key).and_then(Value::as_u64) {
            return Some(total);
        }
    }
    let keys = [
        "input_tokens",
        "inputTokens",
        "output_tokens",
        "outputTokens",
        "cache_creation_input_tokens",
        "cacheCreationInputTokens",
        "cache_read_input_tokens",
        "cacheReadInputTokens",
    ];
    let mut found = false;
    let total = keys.into_iter().fold(0_u64, |total, key| {
        usage
            .get(key)
            .and_then(Value::as_u64)
            .map_or(total, |value| {
                found = true;
                total.saturating_add(value)
            })
    });
    found.then_some(total)
}

enum GateOutcome {
    Pass,
    Pending,
    Fail(String),
}

enum UsedPlanOutcome {
    Pending,
    Completed,
    Failed(String),
}

fn member_fields(
    member: &MemberSpec,
    status: &str,
    incarnation_id: Option<&str>,
    adopted: bool,
) -> BTreeMap<String, Value> {
    let mut fields = BTreeMap::from([
        ("status".into(), Value::String(status.into())),
        (
            "runtime_id".into(),
            Value::String(member.runtime_id.clone()),
        ),
        ("terminal".into(), Value::Bool(member.terminal)),
        ("host".into(), Value::String(member.host.clone())),
        ("adopted".into(), Value::Bool(adopted)),
        (
            "shutdown_timeout_ms".into(),
            Value::from(member.shutdown_timeout_ms),
        ),
        ("reachability".into(), Value::String("reachable".into())),
        ("reason".into(), Value::Null),
    ]);
    if let Some(incarnation_id) = incarnation_id {
        fields.insert(
            "incarnation_id".into(),
            Value::String(incarnation_id.into()),
        );
    }
    fields
}

enum RestartDecision {
    Start,
    Wait { until: u128, reason: String },
    Fail { reason: String },
}

fn actual_field<'a>(actual: &'a Value, path: &str) -> Option<&'a Value> {
    let mut value = actual.get("fields").unwrap_or(actual);
    for segment in path.split('.') {
        value = value.get(segment)?;
    }
    Some(value)
}

fn owned_field(mut value: Value, path: &str) -> Option<Value> {
    for segment in path.split('.') {
        value = value.get(segment)?.clone();
    }
    Some(value)
}

fn compare_value(found: &Value, operator: &str, expected: &Value) -> bool {
    match operator {
        "is" => found == expected,
        "starts-with" => found
            .as_str()
            .zip(expected.as_str())
            .is_some_and(|(found, expected)| found.starts_with(expected)),
        "contains" => match (found, expected) {
            (Value::String(found), Value::String(expected)) => found.contains(expected),
            (Value::Array(found), expected) => found.contains(expected),
            _ => false,
        },
        _ => false,
    }
}

fn gate_operation_subject(
    stage: &CheckpointSpec,
    name: &str,
    definition: &Value,
) -> Result<String> {
    let bytes = serde_json::to_vec(&(stage.subject.as_str(), name, definition))?;
    let hash = hex::encode(sha2::Sha256::digest(bytes));
    Ok(format!("{}/gate/{}", stage.subject, &hash[..24]))
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn prepend_executable_dir(
    environment: &mut BTreeMap<String, String>,
    executable: &Path,
) -> Result<()> {
    let Some(directory) = executable.parent() else {
        return Ok(());
    };
    let current = environment
        .get("PATH")
        .map(std::ffi::OsString::from)
        .or_else(|| std::env::var_os("PATH"))
        .unwrap_or_default();
    let paths = std::iter::once(directory.to_path_buf())
        .chain(std::env::split_paths(&current).filter(|path| path != directory));
    environment.insert(
        "PATH".into(),
        std::env::join_paths(paths)?.to_string_lossy().into_owned(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::{SecondsFormat, Utc};

    use super::*;
    use crate::graph::parse_intent;

    #[derive(Default)]
    struct FakeRuntime {
        ptys: Mutex<Vec<RuntimeObservation>>,
        execs: Mutex<HashMap<String, RuntimeObservation>>,
        logs: Mutex<HashMap<String, String>>,
        starts: Mutex<Vec<String>>,
        started_members: Mutex<Vec<MemberSpec>>,
        stops: Mutex<Vec<String>>,
        kills: Mutex<Vec<String>>,
        screen: Mutex<String>,
        keys: Mutex<Vec<String>>,
        sent_lines: Mutex<Vec<(String, String)>>,
    }

    impl RuntimeControl for FakeRuntime {
        fn snapshot_ptys(&self) -> Result<Vec<RuntimeObservation>> {
            Ok(self.ptys.lock().unwrap().clone())
        }
        fn observe_exec(&self, runtime_id: &str) -> Result<Option<RuntimeObservation>> {
            Ok(self.execs.lock().unwrap().get(runtime_id).cloned())
        }
        fn start(&self, member: &MemberSpec) -> Result<()> {
            self.starts.lock().unwrap().push(member.runtime_id.clone());
            self.started_members.lock().unwrap().push(member.clone());
            Ok(())
        }
        fn stop(
            &self,
            runtime_id: &str,
            _terminal: bool,
            _expected_incarnation: Option<&str>,
        ) -> Result<()> {
            self.stops.lock().unwrap().push(runtime_id.into());
            Ok(())
        }
        fn kill(
            &self,
            runtime_id: &str,
            _terminal: bool,
            _expected_incarnation: Option<&str>,
        ) -> Result<()> {
            self.kills.lock().unwrap().push(runtime_id.into());
            Ok(())
        }
        fn attach(&self, _runtime_id: &str) -> Result<()> {
            Ok(())
        }
        fn send(&self, runtime_id: &str, text: &str) -> Result<()> {
            self.sent_lines
                .lock()
                .unwrap()
                .push((runtime_id.into(), text.into()));
            Ok(())
        }
        fn screen(&self, _runtime_id: &str) -> Result<String> {
            Ok(self.screen.lock().unwrap().clone())
        }
        fn send_key(&self, _runtime_id: &str, key: &str) -> Result<()> {
            self.keys.lock().unwrap().push(key.into());
            Ok(())
        }
        fn read_exec_log(&self, runtime_id: &str) -> Result<Option<String>> {
            Ok(self.logs.lock().unwrap().get(runtime_id).cloned())
        }
    }

    fn apply_source(store: &Store, source: &str, idempotency_key: &str) {
        let intent = parse_intent(source, "node").unwrap();
        let plan = store
            .plan(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store
            .apply(&intent, &plan.subject_tokens, idempotency_key)
            .unwrap();
    }

    #[test]
    fn reads_claude_structured_token_usage() {
        let log = r#"{"type":"result","usage":{"input_tokens":120,"cache_creation_input_tokens":30,"cache_read_input_tokens":40,"output_tokens":10}}"#;
        assert_eq!(structured_token_usage(log), Some(200));
    }

    #[test]
    fn reads_codex_jsonl_token_usage() {
        let log = concat!(
            "{\"type\":\"turn.started\"}\n",
            "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":120,\"cached_input_tokens\":80,\"output_tokens\":30}}\n",
        );
        assert_eq!(structured_token_usage(log), Some(150));
    }

    #[test]
    fn a_plan_run_executes_parallel_roots_and_an_all_of_join() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
version 2
subgraph {
  plan "dag" state="ready" {
    goal "Complete plan dag."
    step "one" { }
    step "two" { }
    step "join" {
      depends-on {
        step "one" completed
        step "two" completed
      }
    }
  }
}
"#;
        apply_source(&store, source, "publish-dag");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "dag".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-dag".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        for _ in 0..8 {
            reconciler.reconcile_once().unwrap();
        }
        let run = store.plan_run(&run.id).unwrap().unwrap();
        assert_eq!(run.status, "completed");
        assert!(run.steps.iter().all(|step| step.status == "completed"));
    }

    #[test]
    fn plan_and_step_baselines_block_before_work_and_recheck_retries() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              plan "baseline" state="ready" {
                goal "Run only from an admitted baseline."
                baseline "the release is open" {
                  field "state" "resource/release" is "open"
                }
                step "work" {
                  baseline "the source is clean" {
                    field "state" "resource/source" is "clean"
                  }
                  retry { attempts 2 }
                }
              }
            }
        "#;
        apply_source(&store, source, "baseline-plan");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "baseline".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "baseline-run".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        let blocked = store.plan_run(&run.id).unwrap().unwrap();
        assert_eq!(blocked.status, "blocked");
        assert_eq!(blocked.steps[0].status, "pending");

        for (subject, state, key) in [
            ("resource/release", "open", "release-open"),
            ("resource/source", "clean", "source-clean"),
        ] {
            store
                .append_claim(&ClaimInput {
                    subject: subject.into(),
                    kind: "resource.binding".into(),
                    actor: None,
                    fields: BTreeMap::from([("state".into(), Value::String(state.into()))]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: Some(key.into()),
                })
                .unwrap();
        }
        reconciler.reconcile_once().unwrap();
        let admitted = store.plan_run(&run.id).unwrap().unwrap();
        assert_eq!(admitted.status, "running");
        assert_eq!(admitted.steps[0].status, "ready");

        store
            .append_claim(&ClaimInput {
                subject: "resource/release".into(),
                kind: "resource.binding".into(),
                actor: None,
                fields: BTreeMap::from([("state".into(), Value::String("closed".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("release-closed-after-admission".into()),
            })
            .unwrap();
        store
            .set_step_state(&admitted.steps[0].subject, "failed", Some("test failure"))
            .unwrap();
        store
            .append_claim(&ClaimInput {
                subject: "resource/source".into(),
                kind: "resource.binding".into(),
                actor: None,
                fields: BTreeMap::from([("state".into(), Value::String("dirty".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("source-dirty".into()),
            })
            .unwrap();
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        let retried = store.plan_run(&run.id).unwrap().unwrap();
        assert_eq!(retried.status, "running");
        assert_eq!(retried.steps[0].attempt, 2);
        assert_eq!(retried.steps[0].status, "blocked");
        assert!(
            retried.steps[0]
                .blocked_reason
                .as_deref()
                .unwrap()
                .contains("source is clean")
        );
    }

    #[test]
    fn plan_products_and_gates_hold_completion_and_record_evidence() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              plan "release" state="ready" {
                goal "Publish an approved result."
                produces {
                  resource "result" { state "published" }
                }
                gate "the result is approved" {
                  field "approval" "resource/result" is "yes"
                }
                step "work" { }
              }
            }
        "#;
        apply_source(&store, source, "release-plan");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "release".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "release-run".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        let waiting = store.plan_run(&run.id).unwrap().unwrap();
        assert_eq!(waiting.steps[0].status, "completed");
        assert_eq!(waiting.status, "running");

        store
            .append_claim(&ClaimInput {
                subject: "resource/result".into(),
                kind: "resource.binding".into(),
                actor: None,
                fields: BTreeMap::from([("state".into(), Value::String("published".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("result-published".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(store.plan_run(&run.id).unwrap().unwrap().status, "running");

        store
            .append_claim(&ClaimInput {
                subject: "resource/result".into(),
                kind: "resource.binding".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("state".into(), Value::String("published".into())),
                    ("approval".into(), Value::String("yes".into())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("result-approved".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().status,
            "completed"
        );
        let evidence = store
            .claims_page(None, None, 0, None, false, 500)
            .unwrap()
            .claims
            .into_iter()
            .find(|claim| {
                claim.kind == "gate.result"
                    && claim.body.pointer("/fields/gate").and_then(Value::as_str)
                        == Some("the result is approved")
            })
            .expect("the gate did not record evidence");
        assert_eq!(
            evidence
                .body
                .pointer("/fields/verdict")
                .and_then(Value::as_str),
            Some("pass")
        );
    }

    #[test]
    fn step_members_and_gates_receive_automatic_st_context() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              plan "context" state="ready" {
                goal "Expose the run context."
                step "work" {
                  subgraph {
                    exec "task" {
                      command "true"
                      env { CUSTOM_RUN "${ST_PLAN_RUN}" }
                    }
                  }
                  gate "verify context" {
                    exec "true"
                    host "node"
                    workspace "."
                    time-limit "1m"
                  }
                }
              }
            }
        "#;
        apply_source(&store, source, "context-plan");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "context".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "context-run".into(),
            })
            .unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store,
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        let task = runtime
            .started_members
            .lock()
            .unwrap()
            .iter()
            .find(|member| member.driver.is_none())
            .cloned()
            .expect("the step task did not start");
        for name in [
            "ST_PLAN",
            "ST_PLAN_REVISION",
            "ST_PLAN_RUN",
            "ST_RUN_GENERATION",
            "ST_ROOT_PLAN_RUN",
            "ST_SCOPE",
            "ST_WORKSPACE",
            "ST_REQUESTER",
            "ST_STEP",
            "ST_STEP_RUN",
            "ST_ATTEMPT",
            "ST_ASSIGNEE",
            "ST_PARENT_STEP_RUN",
            "ST_AGENT",
        ] {
            assert!(task.environment.contains_key(name), "missing {name}");
        }
        assert_eq!(task.environment["CUSTOM_RUN"], run.id);

        runtime.execs.lock().unwrap().insert(
            task.runtime_id.clone(),
            RuntimeObservation {
                runtime_id: task.runtime_id,
                terminal: false,
                status: "exited".into(),
                exit_code: Some(0),
                incarnation_id: Some("task-one".into()),
            },
        );
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        let gate = runtime
            .started_members
            .lock()
            .unwrap()
            .iter()
            .find(|member| member.driver.as_deref() == Some("mechanical-gate"))
            .cloned()
            .expect("the mechanical gate did not start");
        assert_eq!(gate.environment["ST_GATE"], "verify context");
        assert_eq!(gate.environment["ST_PLAN_RUN"], run.id);
        assert_eq!(gate.environment["ST_STEP"], "work");
    }

    #[tokio::test]
    async fn a_materialized_step_subgraph_wakes_member_reconciliation() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
version 2
subgraph {
  plan "wake" state="ready" {
    goal "Complete plan wake."
    step "team" {
      subgraph {
        agent "worker" { workspace "/tmp"; command "true"; restart "never" }
      }
    }
  }
}
"#;
        apply_source(&store, source, "publish-wake");
        store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "wake".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-wake".into(),
            })
            .unwrap();
        let notify = Arc::new(Notify::new());
        let reconciler = Reconciler::new(
            store,
            Arc::new(FakeRuntime::default()),
            "node".into(),
            notify.clone(),
        );
        reconciler.reconcile_once().unwrap();
        notify.notified().await;
        reconciler.reconcile_once().unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(50), notify.notified())
            .await
            .expect("the materialized subgraph did not request another reconcile pass");
    }

    #[test]
    fn a_successor_generation_retires_members_left_in_its_predecessor() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let publish = |source: &str, key: &str| {
            let intent = parse_intent(source, "node").unwrap();
            let planned = store
                .plan(
                    &intent,
                    crate::model::IntentInput {
                        kdl: source.into(),
                        source_name: None,
                    },
                )
                .unwrap();
            store.apply(&intent, &planned.subject_tokens, key).unwrap();
            intent
                .plans
                .values()
                .next()
                .expect("the fixture has one plan")
                .clone()
        };
        let first = publish(
            r#"
version 2
subgraph {
  plan "retire" state="ready" {
    goal "Retire superseded generation members."
    step "team" {
      goal "Use the first team."
      subgraph {
        agent "worker" { workspace "/tmp"; command "true"; restart "never" }
      }
    }
  }
}
"#,
            "retire-first",
        );
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: first.id,
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "retire-run".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        let predecessor_scope = format!("scope/{}", run.generation);
        let worker = store
            .desired_subjects()
            .unwrap()
            .into_iter()
            .find(|subject| subject.subject == "agent/node.worker")
            .expect("the first generation member is materialized");
        assert!(worker.scopes.contains(&predecessor_scope));

        let child_plan = publish(
            r#"
version 2
subgraph {
  plan "child" state="ready" {
    goal "Keep one child run active."
    step "work" { goal "Wait for child work." }
  }
}
"#,
            "retire-child",
        );
        let child = store
            .create_child_plan_run(
                &crate::model::PlanRunRequest {
                    plan: child_plan.id,
                    revision: None,
                    workspace: "/tmp".into(),
                    requester: Some("person/test".into()),
                    mode: Some("run".into()),
                    idempotency_key: "retire-child-run".into(),
                },
                &run,
                &run.steps[0].subject,
                None,
            )
            .unwrap();
        let grandchild_plan = publish(
            r#"
version 2
subgraph {
  plan "grandchild" state="ready" {
    goal "Keep one grandchild run active."
    step "work" { goal "Wait for grandchild work." }
  }
}
"#,
            "retire-grandchild",
        );
        let grandchild = store
            .create_child_plan_run(
                &crate::model::PlanRunRequest {
                    plan: grandchild_plan.id,
                    revision: None,
                    workspace: "/tmp".into(),
                    requester: Some("person/test".into()),
                    mode: Some("run".into()),
                    idempotency_key: "retire-grandchild-run".into(),
                },
                &child,
                &child.steps[0].subject,
                None,
            )
            .unwrap();

        let second = publish(
            r#"
version 2
subgraph {
  plan "retire" state="ready" {
    goal "Retire superseded generation members."
    step "team" { goal "Use the replacement team." }
  }
}
"#,
            "retire-second",
        );
        let revised = store
            .adopt_plan_revision(
                &run.id,
                &second,
                "person/test",
                "the old team is no longer part of the plan",
                "retire-cutover",
            )
            .unwrap();
        reconciler.reconcile_once().unwrap();

        let desired = store.desired_subjects().unwrap();
        for scope in [
            predecessor_scope,
            format!("scope/{}", child.generation),
            format!("scope/{}", grandchild.generation),
        ] {
            let stop = desired
                .iter()
                .find(|subject| subject.subject == scope)
                .expect("the predecessor lineage has a teardown scope");
            assert_eq!(stop.kind, "scope-stop");
        }
        assert_eq!(
            store.plan_run(&child.id).unwrap().unwrap().status,
            "cancelled"
        );
        assert_eq!(
            store.plan_run(&grandchild.id).unwrap().unwrap().status,
            "cancelled"
        );
        assert_ne!(revised.generation, run.generation);
    }

    #[test]
    fn a_worker_report_waits_for_the_declared_product() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
version 2
subgraph {
  agent "worker" { workspace "/tmp"; command "true"; restart "never" }
  plan "product" state="ready" {
    goal "Complete plan product."
    step "publish" {
      assigned-to "agent/worker"
      produces {
        resource "plan-run/${ST_PLAN_RUN}/change" {
          kind "vcs.revision"
          state "published"
        }
      }
    }
  }
}
"#;
        apply_source(&store, source, "publish-product");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "product".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-product".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        let step = store.plan_run(&run.id).unwrap().unwrap().steps.remove(0);
        assert_eq!(step.status, "ready");
        for action in ["claim", "complete"] {
            store
                .work_action(
                    &step.subject,
                    action,
                    &crate::model::WorkRequest {
                        actor: Some("agent/node.worker".into()),
                        incarnation: Some("test".into()),
                        summary: Some("published the change".into()),
                        reason: None,
                        evidence: Vec::new(),
                        idempotency_key: format!("{action}-product"),
                    },
                )
                .unwrap();
        }
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "verifying"
        );
        store
            .append_claim(&ClaimInput {
                subject: format!("resource/plan-run/{}/change", run.id),
                kind: "resource.binding".into(),
                actor: Some("agent/worker".into()),
                fields: BTreeMap::from([
                    ("kind".into(), Value::String("vcs.revision".into())),
                    ("state".into(), Value::String("published".into())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("product-binding".into()),
            })
            .unwrap();
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().status,
            "completed"
        );
    }

    #[test]
    fn a_step_uses_the_exact_plan_revision_produced_by_an_earlier_step() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let bootstrap = r#"
version 2
subgraph {
  agent "planner" { workspace "/tmp"; command "true"; restart "never" }
  plan "bootstrap" state="ready" {
    goal "Complete plan bootstrap."
    step "compile" {
      assigned-to "agent/planner"
      produces-plan "project/work"
    }
    step "execute" {
      assigned-to "agent/planner"
      depends-on { step "compile" completed }
      uses-plan output-of="compile"
    }
  }
}
"#;
        let first_work = r#"
version 2
subgraph {
  plan "project/work" state="ready" {
    goal "Complete plan project/work."
    step "inspect" { title "Inspect the fixture" }
    step "finish" { depends-on { step "inspect" completed } }
  }
}
"#;
        apply_source(&store, bootstrap, "publish-bootstrap");
        apply_source(&store, first_work, "publish-first-work");
        let first = store
            .plan_spec("project/work", None)
            .unwrap()
            .expect("first work plan");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "bootstrap".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-bootstrap".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        let compile = store
            .plan_run(&run.id)
            .unwrap()
            .unwrap()
            .steps
            .into_iter()
            .find(|step| step.step == "compile")
            .unwrap();
        store
            .work_action(
                &compile.subject,
                "claim",
                &crate::model::WorkRequest {
                    actor: Some("agent/node.planner".into()),
                    incarnation: Some("test".into()),
                    summary: None,
                    reason: None,
                    evidence: Vec::new(),
                    idempotency_key: "claim-compile".into(),
                },
            )
            .unwrap();
        let output = store
            .record_plan_output(
                &compile.subject,
                "agent/node.planner",
                Some("test"),
                "project/work",
                &first,
                "bind-first-work",
            )
            .unwrap();
        store
            .work_action(
                &compile.subject,
                "complete",
                &crate::model::WorkRequest {
                    actor: Some("agent/node.planner".into()),
                    incarnation: Some("test".into()),
                    summary: Some("published the complete plan".into()),
                    reason: None,
                    evidence: vec![output.claim_id.clone()],
                    idempotency_key: "complete-compile".into(),
                },
            )
            .unwrap();

        let second_work = r#"
version 2
subgraph {
  plan "project/work" state="ready" {
    goal "Complete plan project/work."
    step "replacement" { title "A later plan revision" }
  }
}
"#;
        apply_source(&store, second_work, "publish-second-work");
        for _ in 0..4 {
            reconciler.reconcile_once().unwrap();
        }
        let execute = store
            .plan_run(&run.id)
            .unwrap()
            .unwrap()
            .steps
            .into_iter()
            .find(|step| step.step == "execute")
            .unwrap();
        assert_eq!(execute.status, "ready");
        for action in ["claim", "complete"] {
            store
                .work_action(
                    &execute.subject,
                    action,
                    &crate::model::WorkRequest {
                        actor: Some("agent/node.planner".into()),
                        incarnation: Some("test".into()),
                        summary: None,
                        reason: None,
                        evidence: Vec::new(),
                        idempotency_key: format!("{action}-execute"),
                    },
                )
                .unwrap();
        }
        reconciler.reconcile_once().unwrap();
        let child = store
            .plan_run_for_parent_step(&execute.subject)
            .unwrap()
            .expect("the used plan run");
        assert_eq!(child.revision, first.revision);
        assert_eq!(child.root_plan_run, run.subject);
        assert_eq!(
            child.parent_step_run.as_deref(),
            Some(execute.subject.as_str())
        );
        assert!(
            child
                .steps
                .iter()
                .all(|step| step.assignee.as_deref() == Some("agent/node.planner"))
        );
        assert_eq!(
            child
                .steps
                .iter()
                .map(|step| step.step.as_str())
                .collect::<Vec<_>>(),
            vec!["finish", "inspect"]
        );

        for step_name in ["inspect", "finish"] {
            for _ in 0..3 {
                reconciler.reconcile_once().unwrap();
            }
            let step = store
                .plan_run(&child.id)
                .unwrap()
                .unwrap()
                .steps
                .into_iter()
                .find(|step| step.step == step_name)
                .unwrap();
            assert_eq!(step.status, "ready");
            for action in ["claim", "complete"] {
                store
                    .work_action(
                        &step.subject,
                        action,
                        &crate::model::WorkRequest {
                            actor: Some("agent/node.planner".into()),
                            incarnation: Some("test".into()),
                            summary: None,
                            reason: None,
                            evidence: Vec::new(),
                            idempotency_key: format!("{action}-{step_name}"),
                        },
                    )
                    .unwrap();
            }
        }
        for _ in 0..8 {
            reconciler.reconcile_once().unwrap();
        }
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().status,
            "completed"
        );
        assert_eq!(
            store.plan_run(&child.id).unwrap().unwrap().status,
            "completed"
        );
        let replica = Store::open_memory("replica").unwrap();
        replica
            .import_replication("node", &store.export_replication(0).unwrap())
            .unwrap();
        let replicated_child = replica
            .plan_run(&child.id)
            .unwrap()
            .expect("the replicated child plan run");
        assert_eq!(replicated_child.root_plan_run, run.subject);
        assert_eq!(replicated_child.parent_step_run, child.parent_step_run);
        assert_eq!(replicated_child.revision, first.revision);
        assert_eq!(
            replicated_child
                .steps
                .iter()
                .map(|step| step.assignee.as_deref())
                .collect::<Vec<_>>(),
            child
                .steps
                .iter()
                .map(|step| step.assignee.as_deref())
                .collect::<Vec<_>>()
        );
        assert!(
            replica
                .plan_output(&compile.subject, 1, &compile.definition_hash)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn an_assignment_waits_until_the_agent_is_in_the_desired_graph() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        apply_source(
            &store,
            r#"
version 2
subgraph {
  plan "assignment" state="ready" {
    goal "Complete plan assignment."
    step "work" { assigned-to "agent/worker" }
  }
}
"#,
            "assignment-plan",
        );
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "assignment".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "assignment-run".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        let blocked = store.plan_run(&run.id).unwrap().unwrap().steps.remove(0);
        assert_eq!(blocked.status, "blocked");
        assert!(
            blocked
                .blocked_reason
                .unwrap()
                .contains("is not present in the desired graph")
        );

        apply_source(
            &store,
            r#"version 2
subgraph { agent "worker" { workspace "/tmp"; command "true"; restart "never" } }"#,
            "assignment-agent",
        );
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "ready"
        );
    }

    #[test]
    fn a_human_gate_accepts_only_the_bound_step_run_review() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        apply_source(
            &store,
            r#"
version 2
subgraph {
  plan "review" state="ready" {
    goal "Complete plan review."
    step "approval" {
      title "The candidate change"
      gate "human-review" type="human" {
        reviewer "person/nathan"
        question "Is the candidate ready?"
        review "resource/plan-run/${ST_PLAN_RUN}/candidate"
      }
    }
  }
}
"#,
            "review-plan",
        );
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "review".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "review-run".into(),
            })
            .unwrap();
        let step = run.steps[0].subject.clone();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();
        let request = store
            .latest_claim(&step, Some("review.requested"))
            .unwrap()
            .expect("the human review was not requested");
        assert_eq!(
            request
                .body
                .pointer("/fields/question")
                .and_then(Value::as_str),
            Some("Is the candidate ready?")
        );
        assert_eq!(
            request
                .body
                .pointer("/fields/review_targets/0")
                .and_then(Value::as_str),
            Some(format!("resource/plan-run/{}/candidate", run.id).as_str())
        );
        store
            .append_claim(&ClaimInput {
                subject: step.clone(),
                kind: "review.decision".into(),
                actor: Some("person/someone-else".into()),
                fields: BTreeMap::from([
                    ("decision".into(), Value::String("approved".into())),
                    ("request".into(), Value::String(request.id.clone())),
                ]),
                evidence: vec![request.id.clone()],
                expected_subject: None,
                idempotency_key: Some("wrong-reviewer".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "ready"
        );
        store
            .append_claim(&ClaimInput {
                subject: step.clone(),
                kind: "review.decision".into(),
                actor: Some("person/nathan".into()),
                fields: BTreeMap::from([("decision".into(), Value::String("approved".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("unbound-review".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "ready"
        );
        store
            .append_claim(&ClaimInput {
                subject: step,
                kind: "review.decision".into(),
                actor: Some("person/nathan".into()),
                fields: BTreeMap::from([
                    ("decision".into(), Value::String("approved".into())),
                    ("request".into(), Value::String(request.id.clone())),
                ]),
                evidence: vec![request.id],
                expected_subject: None,
                idempotency_key: Some("right-reviewer".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "completed"
        );
    }

    #[test]
    fn rejects_an_unstructured_usage_log() {
        assert_eq!(structured_token_usage("the gate finished"), None);
    }

    #[test]
    fn a_started_member_gets_its_graph_identity() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "true"
              }
            }
        "#;
        apply_source(&store, source, "member-identity");
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store,
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();

        let members = runtime.started_members.lock().unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(
            members[0].environment.get("ST_AGENT").map(String::as_str),
            Some("agent/node.worker")
        );
        let executable = std::env::current_exe().unwrap();
        let path = members[0].environment.get("PATH").unwrap();
        assert_eq!(
            std::env::split_paths(std::ffi::OsStr::new(path)).next(),
            executable.parent().map(Path::to_path_buf)
        );
    }

    #[test]
    fn a_native_driver_uses_the_running_st3_executable() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "worker" {
                harness "codex" { prompt "Wait for work." }
              }
            }
        "#;
        apply_source(&store, source, "native-driver-executable");
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store,
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();

        let members = runtime.started_members.lock().unwrap();
        let LaunchSpec::Argv(argv) = &members[0].launch else {
            panic!("the native driver launch is not argv");
        };
        assert_eq!(
            argv.first().map(Path::new),
            Some(std::env::current_exe().unwrap().as_path())
        );
    }

    #[test]
    fn a_plan_step_waits_for_native_driver_readiness_before_starting_a_gate() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              plan "proof" state="ready" {
                goal "Complete plan proof."
                step "native-ready" {
                  title "The native agent is ready"
                  subgraph {
                    agent "worker" {
                      harness "codex" { prompt "Do the work." }
                    }
                    message "kick" {
                      from "requester"
                      to "node.worker"
                      content "Start."
                    }
                  }
                  gate "verify" {
                    exec "true"
                    host "node"
                    workspace "."
                    time-limit "1m"
                  }
                }
              }
            }
        "#;
        apply_source(&store, source, "plan-native-ready");
        store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "proof".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-native-ready".into(),
            })
            .unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        assert_eq!(&*runtime.starts.lock().unwrap(), &["node.worker"]);

        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "running".into(),
            exit_code: None,
            incarnation_id: Some("worker-one".into()),
        });
        reconciler.reconcile_once().unwrap();
        assert_eq!(&*runtime.starts.lock().unwrap(), &["node.worker"]);

        store
            .append_claim(&ClaimInput {
                subject: "agent/node.worker".into(),
                kind: "harness.ready".into(),
                actor: Some("agent/node.worker".into()),
                fields: BTreeMap::from([
                    ("status".into(), Value::String("ready".into())),
                    ("transport".into(), Value::String("app-server".into())),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("worker-ready".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(&*runtime.starts.lock().unwrap(), &["node.worker"]);

        store
            .append_claim(&ClaimInput {
                subject: "message/kick".into(),
                kind: "message.delivered".into(),
                actor: Some("agent/node.worker".into()),
                fields: BTreeMap::from([("status".into(), Value::String("delivered".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("kick-delivered".into()),
            })
            .unwrap();
        reconciler.reconcile_once().unwrap();

        let starts = runtime.starts.lock().unwrap();
        assert_eq!(starts.len(), 2);
        assert!(starts[1].contains(".gate."));
    }

    #[test]
    fn a_simulated_codex_graph_reaches_verdict_and_cleans_its_scope() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              scope "eval/simulated-codex" retention="temporary" {
                plan "eval/simulated-codex" state="ready" {
                  goal "Complete plan eval/simulated-codex."
                step "team" {
                  title "The Codex team is ready"
                  subgraph {
                    agent "sup" {
                      harness "codex" { prompt "Coordinate the work." }
                      restart "never"
                    }
                    agent "worker" {
                      harness "codex" { prompt "Do the work." }
                      restart "never"
                    }
                    message "kickoff" {
                      from "requester"
                      to "node.sup"
                      content "Start."
                    }
                  }
                  gate "condition-1" { exists "agent/node.sup" }
                  gate "condition-2" { exists "agent/node.worker" }
                }
                step "worker-report" {
                  title "The worker report is delivered"
                  depends-on { step "team" completed }
                  subgraph {
                    message "worker-report" {
                      from "node.worker"
                      to "node.sup"
                      content "The work is complete."
                    }
                  }
                }
                step "confirmation" {
                  title "The supervisor confirmation is delivered"
                  depends-on { step "worker-report" completed }
                  subgraph {
                    message "confirmation" {
                      from "node.sup"
                      to "requester"
                      content "The result is verified."
                    }
                  }
                }
                step "mechanical" {
                  title "The mechanical gate passes"
                  depends-on { step "confirmation" completed }
                  gate "mechanical" {
                    exec "true"
                    host "node"
                    workspace "."
                    time-limit "60s"
                  }
                }
                step "semantic" {
                  title "The Codex gate passes"
                  depends-on { step "mechanical" completed }
                  gate "semantic" type="llm" {
                    model "gpt-5.6-sol"
                    host "node"
                    workspace "."
                    tools "shell"
                    token-budget 1000
                    time-limit "60s"
                    prompt "Check the result."
                  }
                }
                step "cleanup" finally=#true {
                  title "The temporary eval scope is empty"
                  subgraph { scope "eval/simulated-codex" { stop } }
                  gate "condition-1" { empty "scope/eval/simulated-codex" }
                }
                }
              }
            }
        "#;
        apply_source(&store, source, "simulated-codex-graph");
        let plan_run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "eval/simulated-codex".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("eval".into()),
                idempotency_key: "run-simulated-codex".into(),
            })
            .unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        let deliver = |subject: &str, recipient: &str| {
            store
                .append_claim(&ClaimInput {
                    subject: subject.into(),
                    kind: "message.delivered".into(),
                    actor: Some(recipient.into()),
                    fields: BTreeMap::from([("status".into(), Value::String("delivered".into()))]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: Some(format!("deliver:{subject}")),
                })
                .unwrap();
        };

        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }
        assert_eq!(runtime.started_members.lock().unwrap().len(), 2);
        runtime.ptys.lock().unwrap().extend([
            RuntimeObservation {
                runtime_id: "node.sup".into(),
                terminal: true,
                status: "running".into(),
                exit_code: None,
                incarnation_id: Some("sup-one".into()),
            },
            RuntimeObservation {
                runtime_id: "node.worker".into(),
                terminal: true,
                status: "running".into(),
                exit_code: None,
                incarnation_id: Some("worker-one".into()),
            },
        ]);
        reconciler.reconcile_once().unwrap();
        for subject in ["agent/node.sup", "agent/node.worker"] {
            store
                .append_claim(&ClaimInput {
                    subject: subject.into(),
                    kind: "harness.ready".into(),
                    actor: Some(subject.into()),
                    fields: BTreeMap::from([
                        ("status".into(), Value::String("ready".into())),
                        ("transport".into(), Value::String("app-server".into())),
                    ]),
                    evidence: Vec::new(),
                    expected_subject: None,
                    idempotency_key: Some(format!("ready:{subject}")),
                })
                .unwrap();
        }
        deliver("message/kickoff", "agent/node.sup");
        for _ in 0..4 {
            reconciler.reconcile_once().unwrap();
        }
        deliver("message/worker-report", "agent/node.sup");
        for _ in 0..4 {
            reconciler.reconcile_once().unwrap();
        }
        deliver("message/confirmation", "requester");
        for _ in 0..4 {
            reconciler.reconcile_once().unwrap();
        }

        let mechanical = runtime
            .started_members
            .lock()
            .unwrap()
            .iter()
            .find(|member| member.driver.as_deref() == Some("mechanical-gate"))
            .unwrap()
            .runtime_id
            .clone();
        runtime.execs.lock().unwrap().insert(
            mechanical.clone(),
            RuntimeObservation {
                runtime_id: mechanical,
                terminal: false,
                status: "exited".into(),
                exit_code: Some(0),
                incarnation_id: Some("mechanical-one".into()),
            },
        );
        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }

        let (llm, gate_subject) = {
            let members = runtime.started_members.lock().unwrap();
            let member = members
                .iter()
                .find(|member| member.driver.as_deref() == Some("llm-gate"))
                .unwrap();
            (
                member.runtime_id.clone(),
                member.environment["ST_GATE_SUBJECT"].clone(),
            )
        };
        store
            .append_claim(&ClaimInput {
                subject: gate_subject,
                kind: "gate.result".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("verdict".into(), Value::String("pass".into())),
                    (
                        "reason".into(),
                        Value::String("the result is correct".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("simulated-llm-result".into()),
            })
            .unwrap();
        runtime.execs.lock().unwrap().insert(
            llm.clone(),
            RuntimeObservation {
                runtime_id: llm.clone(),
                terminal: false,
                status: "exited".into(),
                exit_code: Some(0),
                incarnation_id: Some("llm-one".into()),
            },
        );
        runtime.logs.lock().unwrap().insert(
            llm,
            r#"{"type":"turn.completed","usage":{"total_tokens":120}}"#.into(),
        );
        for _ in 0..8 {
            reconciler.reconcile_once().unwrap();
        }
        assert_eq!(runtime.stops.lock().unwrap().len(), 2);

        runtime.ptys.lock().unwrap().clear();
        for _ in 0..4 {
            reconciler.reconcile_once().unwrap();
        }

        let completed = store.plan_run(&plan_run.id).unwrap().unwrap();
        assert_eq!(completed.status, "completed", "{completed:?}");
        assert_eq!(
            store
                .latest_actual_value("scope/eval/simulated-codex")
                .unwrap()
                .and_then(|actual| actual.get("status").cloned()),
            Some(Value::String("stopped".into()))
        );
    }

    #[test]
    fn a_mechanical_gate_uses_the_async_exec_runtime() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              plan "proof" state="ready" {
                goal "Complete plan proof."
                step "verify" {
                  title "The command passes"
                  gate "verify" {
                    exec "sleep 60"
                    host "node"
                    workspace "."
                    time-limit "1m"
                  }
                }
              }
            }
        "#;
        apply_source(&store, source, "plan-async-gate");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "proof".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-async-gate".into(),
            })
            .unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        for _ in 0..4 {
            reconciler.reconcile_once().unwrap();
        }
        let runtime_id = runtime.starts.lock().unwrap()[0].clone();
        assert_eq!(store.plan_run(&run.id).unwrap().unwrap().status, "running");
        runtime.execs.lock().unwrap().insert(
            runtime_id.clone(),
            RuntimeObservation {
                runtime_id,
                terminal: false,
                status: "exited".into(),
                exit_code: Some(0),
                incarnation_id: Some("gate-one".into()),
            },
        );

        for _ in 0..3 {
            reconciler.reconcile_once().unwrap();
        }

        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().status,
            "completed"
        );
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 1);
    }

    #[test]
    fn an_llm_gate_fails_when_structured_usage_exceeds_its_budget() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              plan "proof" state="ready" {
                goal "Complete plan proof."
                step "review" {
                  title "A held-out gate accepts the result"
                  gate "review" type="llm" {
                    model "claude-sonnet"
                    host "node"
                    workspace "."
                    tools "shell"
                    token-budget 10
                    time-limit "1m"
                    prompt "Inspect the result."
                  }
                }
              }
            }
        "#;
        apply_source(&store, source, "plan-llm-budget");
        store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "proof".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-llm-budget".into(),
            })
            .unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();
        let (runtime_id, result_subject) = {
            let members = runtime.started_members.lock().unwrap();
            let gate = members
                .iter()
                .find(|member| member.driver.as_deref() == Some("llm-gate"))
                .unwrap();
            (
                gate.runtime_id.clone(),
                gate.environment["ST_GATE_SUBJECT"].clone(),
            )
        };
        store
            .append_claim(&ClaimInput {
                subject: result_subject.clone(),
                kind: "gate.result".into(),
                actor: None,
                fields: BTreeMap::from([
                    ("verdict".into(), Value::String("pass".into())),
                    (
                        "reason".into(),
                        Value::String("the result is correct".into()),
                    ),
                ]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("llm-preliminary-result".into()),
            })
            .unwrap();
        runtime.execs.lock().unwrap().insert(
            runtime_id.clone(),
            RuntimeObservation {
                runtime_id: runtime_id.clone(),
                terminal: false,
                status: "exited".into(),
                exit_code: Some(0),
                incarnation_id: Some("gate-run".into()),
            },
        );
        runtime.logs.lock().unwrap().insert(
            runtime_id,
            r#"{"type":"result","usage":{"input_tokens":8,"output_tokens":4}}"#.into(),
        );

        reconciler.reconcile_once().unwrap();

        let result = store
            .latest_claim(&result_subject, Some("gate.result"))
            .unwrap()
            .unwrap();
        assert_eq!(
            result
                .body
                .pointer("/fields/token_usage")
                .and_then(Value::as_u64),
            Some(12)
        );
        assert_eq!(
            result
                .body
                .pointer("/fields/verdict")
                .and_then(Value::as_str),
            Some("fail")
        );
    }

    #[test]
    fn adopts_a_matching_pty_without_a_start() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let intent = parse_intent(
            "version 2\nsubgraph { agent \"worker\" { command \"true\" } }",
            "node",
        )
        .unwrap();
        let plan = store
            .plan(
                &intent,
                crate::model::IntentInput {
                    kdl: "test".into(),
                    source_name: None,
                },
            )
            .unwrap();
        store.apply(&intent, &plan.subject_tokens, "one").unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "running".into(),
            exit_code: None,
            incarnation_id: Some("existing".into()),
        });
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        assert!(runtime.starts.lock().unwrap().is_empty());
        let actual = store
            .latest_actual_value("agent/node.worker")
            .unwrap()
            .unwrap();
        assert_eq!(actual_field(&actual, "adopted"), Some(&Value::Bool(true)));
    }

    #[test]
    fn restart_types_follow_success_and_failure() {
        for (name, restart, exit_code, expected_starts) in [
            ("always-success", "always", 0, 2),
            ("failure-success", "on-failure", 0, 1),
            ("failure-error", "on-failure", 1, 2),
            ("never-error", "never", 1, 1),
        ] {
            let store = Arc::new(Store::open_memory("node").unwrap());
            let source = format!(
                r#"
                    version 2
                    subgraph {{
                      agent "worker" {{
                        command "true"
                        restart "{restart}"
                      }}
                    }}
                "#
            );
            apply_source(&store, &source, name);
            let runtime = Arc::new(FakeRuntime::default());
            let reconciler = Reconciler::new(
                store,
                runtime.clone(),
                "node".into(),
                Arc::new(Notify::new()),
            );

            reconciler.reconcile_once().unwrap();
            runtime.ptys.lock().unwrap().push(RuntimeObservation {
                runtime_id: "node.worker".into(),
                terminal: true,
                status: "exited".into(),
                exit_code: Some(exit_code),
                incarnation_id: Some(format!("{name}-one")),
            });
            reconciler.reconcile_once().unwrap();

            assert_eq!(
                runtime.starts.lock().unwrap().len(),
                expected_starts,
                "{name}"
            );
        }
    }

    #[test]
    fn restart_never_starts_a_new_desired_revision() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        apply_source(
            &store,
            r#"
                version 2
                subgraph {
                  agent "worker" {
                    workspace "/tmp/one"
                    command "true"
                    restart "never"
                  }
                }
            "#,
            "member-one",
        );
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "exited".into(),
            exit_code: Some(0),
            incarnation_id: Some("worker-one".into()),
        });
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 1);

        apply_source(
            &store,
            r#"
                version 2
                subgraph {
                  agent "worker" {
                    workspace "/tmp/two"
                    command "true"
                    restart "never"
                  }
                }
            "#,
            "member-two",
        );
        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();

        assert_eq!(runtime.starts.lock().unwrap().len(), 2);
        assert_eq!(
            runtime.started_members.lock().unwrap()[1].workspace,
            "/tmp/two"
        );
    }

    #[test]
    fn a_reconcile_does_not_redeliver_an_accepted_message() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "sleep 60"
                ding
              }
              message "one" {
                from "requester"
                to "node.worker"
                content "Do the work."
              }
            }
        "#;
        apply_source(&store, source, "one-message");
        let runtime = Arc::new(FakeRuntime::default());
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "running".into(),
            exit_code: None,
            incarnation_id: Some("worker-one".into()),
        });
        let reconciler = Reconciler::new(
            store,
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();

        assert_eq!(runtime.sent_lines.lock().unwrap().len(), 1);
    }

    #[test]
    fn fail_restart_intensity_parks_after_the_launch_budget() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "true"
                restart "always"
                restart {
                  attempts 1
                  interval "60s"
                  mode "fail"
                }
              }
            }
        "#;
        let intent = parse_intent(source, "node").unwrap();
        let plan = store
            .plan(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store.apply(&intent, &plan.subject_tokens, "one").unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 1);
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "exited".into(),
            exit_code: Some(1),
            incarnation_id: Some("first".into()),
        });
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 1);
        let actual = store
            .latest_actual_value("agent/node.worker")
            .unwrap()
            .unwrap();
        assert_eq!(
            actual_field(&actual, "reachability"),
            Some(&Value::String("unreachable".into()))
        );
    }

    #[test]
    fn delay_restart_intensity_waits_for_the_sliding_window() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "true"
                restart "always"
                restart {
                  attempts 1
                  interval "60s"
                  mode "delay"
                }
              }
            }
        "#;
        let intent = parse_intent(source, "node").unwrap();
        let plan = store
            .plan(
                &intent,
                crate::model::IntentInput {
                    kdl: source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store.apply(&intent, &plan.subject_tokens, "one").unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "exited".into(),
            exit_code: Some(1),
            incarnation_id: Some("first".into()),
        });
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 1);
        let decision = store
            .latest_claim("agent/node.worker", Some("supervision.decision"))
            .unwrap()
            .unwrap();
        assert_eq!(
            decision
                .body
                .pointer("/fields/decision")
                .and_then(Value::as_str),
            Some("wait")
        );
    }

    #[test]
    fn restart_delay_starts_at_the_exit_observation() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "true"
                restart "always"
                restart {
                  attempts 3
                  interval "60s"
                  delay "20ms"
                  mode "delay"
                }
              }
            }
        "#;
        apply_source(&store, source, "restart-delay-from-exit");
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store,
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        std::thread::sleep(Duration::from_millis(25));
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "exited".into(),
            exit_code: Some(1),
            incarnation_id: Some("first".into()),
        });
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 1);

        std::thread::sleep(Duration::from_millis(25));
        reconciler.reconcile_once().unwrap();
        assert_eq!(runtime.starts.lock().unwrap().len(), 2);
    }

    #[test]
    fn stop_waits_for_exit_and_then_kills_the_same_incarnation() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let running_source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "sleep 60"
                shutdown-timeout "1ms"
              }
            }
        "#;
        let running = parse_intent(running_source, "node").unwrap();
        let plan = store
            .plan(
                &running,
                crate::model::IntentInput {
                    kdl: running_source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store.apply(&running, &plan.subject_tokens, "run").unwrap();
        let runtime = Arc::new(FakeRuntime::default());
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "running".into(),
            exit_code: None,
            incarnation_id: Some("generation-one".into()),
        });
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();

        let stop_source = r#"version 2
subgraph { stop "agent/node.worker" }"#;
        let stop = parse_intent(stop_source, "node").unwrap();
        let plan = store
            .plan(
                &stop,
                crate::model::IntentInput {
                    kdl: stop_source.into(),
                    source_name: None,
                },
            )
            .unwrap();
        store.apply(&stop, &plan.subject_tokens, "stop").unwrap();
        reconciler.reconcile_once().unwrap();
        assert_eq!(&*runtime.stops.lock().unwrap(), &["node.worker"]);
        assert!(runtime.kills.lock().unwrap().is_empty());
        assert_ne!(
            actual_field(
                &store
                    .latest_actual_value("agent/node.worker")
                    .unwrap()
                    .unwrap(),
                "status"
            ),
            Some(&Value::String("stopped".into()))
        );

        std::thread::sleep(Duration::from_millis(2));
        reconciler.reconcile_once().unwrap();
        assert_eq!(&*runtime.kills.lock().unwrap(), &["node.worker"]);
    }

    #[test]
    fn stop_does_not_kill_a_replacement_incarnation() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let running_source = r#"
            version 2
            subgraph {
              agent "worker" {
                command "sleep 60"
                shutdown-timeout "1ms"
              }
            }
        "#;
        apply_source(&store, running_source, "replacement-run");
        let runtime = Arc::new(FakeRuntime::default());
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "running".into(),
            exit_code: None,
            incarnation_id: Some("generation-one".into()),
        });
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        apply_source(
            &store,
            r#"version 2
subgraph { stop "agent/node.worker" }"#,
            "replacement-stop",
        );
        reconciler.reconcile_once().unwrap();

        runtime.ptys.lock().unwrap()[0].incarnation_id = Some("generation-two".into());
        std::thread::sleep(Duration::from_millis(2));
        reconciler.reconcile_once().unwrap();

        assert_eq!(runtime.stops.lock().unwrap().len(), 2);
        assert!(runtime.kills.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn one_time_schedule_sends_exactly_one_message() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let at = (Utc::now() + chrono::Duration::milliseconds(50))
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let source = format!(
            r#"
                version 2
                subgraph {{
                  schedule "reminder" {{
                    at "{at}"
                    message {{
                      to "worker"
                      content "Run the check."
                    }}
                  }}
                }}
            "#
        );
        apply_source(&store, &source, "schedule-one");
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        reconciler.reconcile_once().unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(
            store
                .claims_for("schedule/reminder", Some("clock.reached"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(store.messages(None, true).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_new_schedule_revision_cancels_the_armed_wake() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let at = (Utc::now() + chrono::Duration::milliseconds(100))
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let source = format!(
            r#"
                version 2
                subgraph {{
                  schedule "reminder" {{
                    at "{at}"
                    message {{
                      to "worker"
                      content "Run the check."
                    }}
                  }}
                }}
            "#
        );
        apply_source(&store, &source, "schedule-arm");
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();

        apply_source(
            &store,
            r#"version 2
subgraph { schedule "reminder" { stop } }"#,
            "schedule-stop",
        );
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            store
                .claims_for("schedule/reminder", Some("clock.reached"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .claims_for("schedule/reminder", Some("clock.wake.cancel.requested"))
                .unwrap()
                .len(),
            1
        );
        assert!(store.messages(None, true).unwrap().is_empty());
    }

    #[test]
    fn a_required_link_holds_only_its_source() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              agent "source" { command "true" }
              agent "target" { command "true" }
              link "dependency" {
                from "agent/node.source"
                to "agent/node.target"
              }
            }
        "#;
        apply_source(&store, source, "link-hold");
        let runtime = Arc::new(FakeRuntime::default());
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();

        assert_eq!(&*runtime.starts.lock().unwrap(), &["node.target"]);
        let decision = store
            .latest_claim("agent/node.source", Some("supervision.decision"))
            .unwrap()
            .unwrap();
        assert_eq!(
            decision
                .body
                .pointer("/fields/decision")
                .and_then(Value::as_str),
            Some("hold")
        );
    }

    #[test]
    fn a_terminal_control_stops_after_its_declared_input_limit() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              supervisor "watch" {
                terminal-control "confirmation" driver="codex" {
                  contains "Press Enter"
                  key "enter"
                  max-inputs 1
                }
              }
              agent "worker" {
                supervisor "watch"
                harness "codex" { prompt "Do the work." }
              }
            }
        "#;
        apply_source(&store, source, "bounded-gate");
        let runtime = Arc::new(FakeRuntime::default());
        runtime.ptys.lock().unwrap().push(RuntimeObservation {
            runtime_id: "node.worker".into(),
            terminal: true,
            status: "running".into(),
            exit_code: None,
            incarnation_id: Some("one".into()),
        });
        *runtime.screen.lock().unwrap() = "Press Enter to continue".into();
        let reconciler = Reconciler::new(
            store.clone(),
            runtime.clone(),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();

        assert_eq!(&*runtime.keys.lock().unwrap(), &["enter"]);
        let decision = store
            .latest_claim("agent/node.worker", Some("supervision.decision"))
            .unwrap()
            .unwrap();
        assert_eq!(
            decision
                .body
                .pointer("/fields/decision")
                .and_then(Value::as_str),
            Some("raise")
        );
    }

    #[tokio::test]
    async fn a_terminal_plan_failure_selects_cleanup() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              scope "eval/demo" retention="temporary" {
                plan "eval/demo" state="ready" {
                  goal "Complete plan eval/demo."
                step "result" timeout="1ms" {
                  title "The result appears"
                  gate "condition-1" { field "status" "resource/result" "is" "ok" }
                }
                step "cleanup" finally=#true {
                  title "The temporary eval scope is empty"
                  subgraph { scope "eval/demo" { stop } }
                  gate "condition-2" { empty "scope/eval/demo" }
                }
                }
              }
              resource "result" { kind "human.review" }
            }
        "#;
        apply_source(&store, source, "plan-cleanup");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "eval/demo".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("eval".into()),
                idempotency_key: "run-cleanup".into(),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );

        reconciler.reconcile_once().unwrap();
        reconciler.reconcile_once().unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        for _ in 0..20 {
            reconciler.reconcile_once().unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let run = store.plan_run(&run.id).unwrap().unwrap();
        assert_eq!(run.status, "failed");
        assert_eq!(
            store
                .latest_actual_value("scope/eval/demo")
                .unwrap()
                .unwrap()
                .get("status")
                .and_then(Value::as_str),
            Some("stopped")
        );
        assert_eq!(
            run.steps
                .iter()
                .find(|step| step.step == "cleanup")
                .unwrap()
                .status,
            "completed"
        );
    }

    #[test]
    fn a_satisfied_dependency_predicate_stays_latched() {
        let store = Arc::new(Store::open_memory("node").unwrap());
        let source = r#"
            version 2
            subgraph {
              resource "approval" { kind "human.review" }
              agent "worker" { workspace "/tmp"; command "true"; restart "never" }
              plan "release" state="ready" {
                goal "Complete plan release."
                step "publish" {
                  assigned-to "agent/worker"
                  depends-on {
                    field "decision" "resource/approval" "is" "approved"
                  }
                }
              }
            }
        "#;
        apply_source(&store, source, "latched-dependency");
        let run = store
            .create_plan_run(&crate::model::PlanRunRequest {
                plan: "release".into(),
                revision: None,
                workspace: "/tmp".into(),
                requester: Some("person/test".into()),
                mode: Some("run".into()),
                idempotency_key: "run-latched-dependency".into(),
            })
            .unwrap();
        store
            .append_claim(&ClaimInput {
                subject: "resource/approval".into(),
                kind: "review.decision".into(),
                actor: Some("person/reviewer".into()),
                fields: BTreeMap::from([("decision".into(), Value::String("approved".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("approve".into()),
            })
            .unwrap();
        let reconciler = Reconciler::new(
            store.clone(),
            Arc::new(FakeRuntime::default()),
            "node".into(),
            Arc::new(Notify::new()),
        );
        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "ready"
        );

        store
            .append_claim(&ClaimInput {
                subject: "resource/approval".into(),
                kind: "review.decision".into(),
                actor: Some("person/reviewer".into()),
                fields: BTreeMap::from([("decision".into(), Value::String("rejected".into()))]),
                evidence: Vec::new(),
                expected_subject: None,
                idempotency_key: Some("reject".into()),
            })
            .unwrap();

        reconciler.reconcile_once().unwrap();
        assert_eq!(
            store.plan_run(&run.id).unwrap().unwrap().steps[0].status,
            "ready"
        );
    }
}
