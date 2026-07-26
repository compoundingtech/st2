//! M1 correctness net: the pure reconcile plan over VRS jobs (services).

use std::collections::BTreeMap;
use std::path::PathBuf;

use st2::spec::{AgentSpec, JobType, Task, TaskKind};
use st2::{Session, reconcile};

fn task(kind: TaskKind, name: &str, id: Option<&str>, command: Option<&str>) -> Task {
    Task {
        kind,
        derived: false,
        name: name.to_string(),
        id: id.map(String::from),
        command: command.map(String::from),
        cwd: None,
        tags: BTreeMap::new(),
        env: BTreeMap::new(),
        keep: false,
    }
}

fn spec(identity: &str, host: Option<&str>, job_type: JobType, retired: bool, tasks: Vec<Task>) -> AgentSpec {
    AgentSpec {
        identity: identity.to_string(),
        host: host.map(String::from),
        role: None,
        job_type,
        workspace: None,
        supervisor: None,
        retired,
        keep: false,
        restart: None,
        tasks,
        path: PathBuf::from(format!("/cat/agents/{}/{identity}/agent.kdl", host.unwrap_or("this"))),
    }
}

fn svc(identity: &str, host: Option<&str>, tasks: Vec<Task>) -> AgentSpec {
    spec(identity, host, JobType::Service, false, tasks)
}

fn live(id: &str) -> Session {
    Session { pty_id: id.to_string(), alive: true, exit_code: None }
}
fn dead(id: &str) -> Session {
    Session { pty_id: id.to_string(), alive: false, exit_code: None }
}

const HOST: &str = "hetz";

#[test]
fn fresh_service_launches_all_tasks_pty_and_exec() {
    let specs = vec![svc(
        "st2-claude",
        Some(HOST),
        vec![
            task(TaskKind::Pty, "agent", Some("hetz.st2-claude"), Some("exec claude 'boot'")),
            task(TaskKind::Exec, "ding", Some("hetz.st2.ding"), Some("st2 ding hetz.st2")),
        ],
    )];
    let plan = reconcile(&specs, &[], HOST);
    assert_eq!(plan.launch.len(), 1);
    assert_eq!(plan.launch[0].tasks.len(), 2);
    let kinds: Vec<TaskKind> = plan.launch[0].tasks.iter().map(|t| t.kind).collect();
    assert!(kinds.contains(&TaskKind::Pty));
    assert!(kinds.contains(&TaskKind::Exec));
}

#[test]
fn all_tasks_live_is_adopted() {
    let specs = vec![svc(
        "a",
        Some(HOST),
        vec![
            task(TaskKind::Pty, "agent", Some("hetz.a"), Some("x")),
            task(TaskKind::Exec, "ding", Some("hetz.a.ding"), Some("y")),
        ],
    )];
    let sessions = vec![live("hetz.a"), live("hetz.a.ding")];
    let plan = reconcile(&specs, &sessions, HOST);
    assert!(plan.launch.is_empty());
    assert_eq!(plan.adopt.len(), 1);
}

#[test]
fn one_dead_task_launches_only_the_missing_one() {
    let specs = vec![svc(
        "a",
        Some(HOST),
        vec![
            task(TaskKind::Pty, "agent", Some("hetz.a"), Some("x")),
            task(TaskKind::Exec, "ding", Some("hetz.a.ding"), Some("y")),
        ],
    )];
    let plan = reconcile(&specs, &[live("hetz.a")], HOST);
    assert_eq!(plan.launch.len(), 1);
    assert_eq!(plan.launch[0].tasks.len(), 1);
    assert_eq!(plan.launch[0].tasks[0].pty_id, "hetz.a.ding");
}

#[test]
fn exited_session_is_reaped_and_relaunched() {
    let specs = vec![svc("a", Some(HOST), vec![task(TaskKind::Pty, "agent", Some("hetz.a"), Some("x"))])];
    let plan = reconcile(&specs, &[dead("hetz.a")], HOST);
    assert_eq!(plan.launch.len(), 1);
    assert_eq!(plan.gc, vec!["hetz.a"]); // reap the corpse, then respawn
}

#[test]
fn dead_keep_task_is_frozen_not_reaped() {
    let mut t = task(TaskKind::Pty, "agent", Some("hetz.a"), Some("x"));
    t.keep = true;
    let specs = vec![svc("a", Some(HOST), vec![t])];
    let plan = reconcile(&specs, &[dead("hetz.a")], HOST);
    assert!(plan.launch.is_empty());
    assert!(plan.gc.is_empty());
    assert_eq!(plan.adopt.len(), 1); // present (frozen)
}

#[test]
fn retired_with_live_sessions_is_torn_down() {
    let specs = vec![spec(
        "old",
        Some(HOST),
        JobType::Service,
        true,
        vec![task(TaskKind::Pty, "agent", Some("hetz.old"), Some("x"))],
    )];
    let plan = reconcile(&specs, &[live("hetz.old")], HOST);
    assert_eq!(plan.teardown.len(), 1);
    assert_eq!(plan.teardown[0].pty_ids, vec!["hetz.old"]);
}

#[test]
fn other_host_specs_are_skipped() {
    let specs = vec![
        svc("here", Some(HOST), vec![task(TaskKind::Pty, "agent", Some("hetz.here"), Some("x"))]),
        svc("there", Some("silber"), vec![task(TaskKind::Pty, "agent", Some("silber.there"), Some("y"))]),
    ];
    let plan = reconcile(&specs, &[], HOST);
    assert_eq!(plan.launch.len(), 1);
    assert_eq!(plan.launch[0].spec.identity, "here");
    assert_eq!(plan.other_host.len(), 1);
    assert_eq!(plan.other_host[0].identity, "there");
}

#[test]
fn host_none_defaults_to_this_host_with_fallback_id() {
    let specs = vec![svc("local", None, vec![task(TaskKind::Pty, "agent", None, Some("x"))])];
    let plan = reconcile(&specs, &[], HOST);
    assert_eq!(plan.launch.len(), 1);
    assert_eq!(plan.launch[0].tasks[0].pty_id, "hetz.local.agent"); // <bus_id>.<name>
}

#[test]
fn unrendered_job_without_commands_is_unrunnable() {
    let specs = vec![svc("nr", Some(HOST), vec![task(TaskKind::Pty, "agent", None, None)])];
    let plan = reconcile(&specs, &[], HOST);
    assert!(plan.launch.is_empty());
    assert_eq!(plan.unrunnable.len(), 1);
}

#[test]
fn generated_ding_only_job_is_unrunnable_and_does_not_launch() {
    let mut ding = task(
        TaskKind::Exec,
        "ding",
        Some("hetz.nr.ding"),
        Some("st2 ding --identity hetz.nr --root $ST_ROOT"),
    );
    ding.derived = true;
    let specs = vec![svc("nr", Some(HOST), vec![ding])];
    let plan = reconcile(&specs, &[], HOST);
    assert!(plan.launch.is_empty());
    assert_eq!(plan.unrunnable.len(), 1);
}

#[test]
fn generated_ding_launches_alongside_authored_work() {
    let agent = task(
        TaskKind::Pty,
        "agent",
        Some("hetz.runnable"),
        Some("codex"),
    );
    let mut ding = task(
        TaskKind::Exec,
        "ding",
        Some("hetz.runnable.ding"),
        Some("st2 ding --identity hetz.runnable --root $ST_ROOT"),
    );
    ding.derived = true;
    let specs = vec![svc("runnable", Some(HOST), vec![agent, ding])];
    let plan = reconcile(&specs, &[], HOST);
    assert_eq!(plan.launch.len(), 1);
    assert_eq!(plan.launch[0].tasks.len(), 2);
}

#[test]
fn agent_level_keep_pins_all_task_targets() {
    let mut s = svc(
        "kept",
        Some(HOST),
        vec![
            task(TaskKind::Pty, "agent", Some("hetz.kept"), Some("x")),
            task(TaskKind::Exec, "ding", Some("hetz.kept.ding"), Some("y")),
        ],
    );
    s.keep = true;
    let plan = reconcile(std::slice::from_ref(&s), &[], HOST);
    assert!(plan.launch[0].tasks.iter().all(|t| t.keep));
}

#[test]
fn workspace_is_carried_into_task_targets_for_cwd_defaulting() {
    let mut s = svc("w", Some(HOST), vec![task(TaskKind::Pty, "agent", Some("hetz.w"), Some("x"))]);
    s.workspace = Some("/repos/w".into());
    let plan = reconcile(std::slice::from_ref(&s), &[], HOST);
    assert_eq!(plan.launch[0].tasks[0].workspace.as_deref(), Some("/repos/w"));
}

#[test]
fn declared_supervisor_is_the_single_source_for_the_spawn_environment() {
    let mut t = task(TaskKind::Pty, "agent", Some("hetz.w"), Some("x"));
    // A stale hand-authored value must not be able to disagree with the normative spec field.
    t.env
        .insert("ST_SUPERVISOR".into(), "stale-env-value".into());
    let mut s = svc("w", Some(HOST), vec![t]);
    s.supervisor = Some("lead".into());

    let plan = reconcile(std::slice::from_ref(&s), &[], HOST);
    assert_eq!(
        plan.launch[0].tasks[0]
            .env
            .get("ST_SUPERVISOR")
            .map(String::as_str),
        Some("lead")
    );

    // Removing the declaration removes the derived variable, even if an old renderer still emits
    // one. Absence must not preserve a phantom supervision relationship.
    s.supervisor = None;
    let plan = reconcile(std::slice::from_ref(&s), &[], HOST);
    assert!(!plan.launch[0].tasks[0].env.contains_key("ST_SUPERVISOR"));
}
