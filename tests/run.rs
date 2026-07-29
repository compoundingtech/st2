//! M1 correctness net: plan execution against a fake Runner (no real processes spawned).

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use st2::message;
use st2::reconcile::{Session, TaskTarget};
use st2::run::Runner;
use st2::run::{CrashLoop, surface_crash_loop, up_once_selected_specs};
use st2::spec::{AgentSpec, JobType, Task, TaskKind};

#[test]
fn selected_one_shot_unknown_refuses_before_runner_list() {
    let runner = FakeRunner::default();
    let error =
        up_once_selected_specs(Path::new("/tmp"), &[], "host.missing.task", "host", &runner)
            .unwrap_err();
    assert!(error.to_string().contains("did not resolve"));
    assert_eq!(runner.list_calls.get(), 0);
    assert!(runner.spawned.borrow().is_empty());
    assert!(runner.killed.borrow().is_empty());
    assert!(runner.reaped.borrow().is_empty());
    assert!(runner.removed.borrow().is_empty());
}

fn task_spec(identity: &str, host: Option<&str>, id: &str) -> AgentSpec {
    AgentSpec {
        identity: identity.into(),
        host: host.map(str::to_owned),
        role: None,
        job_type: JobType::Service,
        workspace: None,
        supervisor: None,
        retired: false,
        keep: false,
        restart: None,
        tasks: vec![Task {
            kind: TaskKind::Exec,
            derived: false,
            name: "work".into(),
            id: Some(id.into()),
            command: Some("true".into()),
            cwd: None,
            tags: BTreeMap::new(),
            env: BTreeMap::new(),
            keep: false,
        }],
        path: "/tmp/spec.kdl".into(),
    }
}

fn assert_refusal(runner: &FakeRunner) {
    assert_eq!(runner.list_calls.get(), 0);
    assert!(runner.spawned.borrow().is_empty());
    assert!(runner.killed.borrow().is_empty());
    assert!(runner.reaped.borrow().is_empty());
    assert!(runner.removed.borrow().is_empty());
}

#[test]
fn selected_one_shot_ambiguous_refuses_before_runner_list() {
    let runner = FakeRunner::default();
    let specs = vec![task_spec("one", None, "dup"), task_spec("two", None, "dup")];
    let error =
        up_once_selected_specs(Path::new("/tmp"), &specs, "dup", "host", &runner).unwrap_err();
    assert!(error.to_string().contains("ambiguous"), "{error}");
    assert_refusal(&runner);
}

#[test]
fn selected_one_shot_wrong_host_refuses_before_runner_list() {
    let runner = FakeRunner::default();
    let specs = vec![task_spec("remote", Some("other"), "other.remote.work")];
    let error = up_once_selected_specs(
        Path::new("/tmp"),
        &specs,
        "other.remote.work",
        "host",
        &runner,
    )
    .unwrap_err();
    assert!(error.to_string().contains("did not resolve"));
    assert_refusal(&runner);
}
use st2::{FlappingCap, UpReport, discover, down, execute, reconcile, up_once};

#[derive(Default)]
struct FakeRunner {
    list_calls: Cell<usize>,
    sessions: Vec<Session>,
    fail_list: bool,
    fail_spawn: Option<String>,
    fail_reap: Option<String>,
    spawned: RefCell<Vec<String>>,
    spawn_dirs: RefCell<Vec<(String, String)>>,
    killed: RefCell<Vec<String>>,
    reaped: RefCell<Vec<String>>,
    removed: RefCell<Vec<String>>,
}

impl Runner for FakeRunner {
    fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
        self.list_calls.set(self.list_calls.get() + 1);
        if self.fail_list {
            anyhow::bail!("simulated list failure");
        }
        Ok(self.sessions.clone())
    }
    fn spawn(&self, target: &TaskTarget, spec_dir: &Path) -> anyhow::Result<()> {
        if self.fail_spawn.as_deref() == Some(target.pty_id.as_str()) {
            anyhow::bail!("simulated spawn failure");
        }
        self.spawned.borrow_mut().push(target.pty_id.clone());
        self.spawn_dirs
            .borrow_mut()
            .push((target.pty_id.clone(), spec_dir.display().to_string()));
        Ok(())
    }
    fn kill(&self, pty_id: &str) -> anyhow::Result<()> {
        self.killed.borrow_mut().push(pty_id.to_string());
        Ok(())
    }
    fn reap_for_restart(&self, pty_id: &str) -> anyhow::Result<()> {
        self.reaped.borrow_mut().push(pty_id.to_string());
        if self.fail_reap.as_deref() == Some(pty_id) {
            anyhow::bail!("reap broke");
        }
        Ok(())
    }
    fn remove(&self, pty_id: &str) -> anyhow::Result<()> {
        self.removed.borrow_mut().push(pty_id.to_string());
        Ok(())
    }
}

fn write(root: &Path, rel: &str, contents: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn live(id: &str) -> Session {
    Session {
        pty_id: id.to_string(),
        alive: true,
        exit_code: None,
    }
}
fn dead(id: &str) -> Session {
    Session {
        pty_id: id.to_string(),
        alive: false,
        exit_code: None,
    }
}

/// A v2 service job: a pty agent + an exec ding.
const AGENT: &str = r#"
identity = "demo"
type = "service"
[pty.agent]
id = "hetz.demo-claude"
command = "exec claude 'boot'"
[exec.ding]
id = "hetz.demo.ding"
command = "st2 ding hetz.demo"
"#;

#[test]
fn up_once_launches_all_tasks_of_a_fresh_agent() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);

    let runner = FakeRunner::default();
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();

    let mut launched = report.launched.clone();
    launched.sort();
    assert_eq!(launched, vec!["hetz.demo-claude", "hetz.demo.ding"]);
    assert!(report.errors.is_empty());
    let dirs = runner.spawn_dirs.borrow();
    assert!(dirs.iter().all(|(_, d)| d.ends_with("agents/hetz/demo")));
}

#[test]
fn up_once_adopts_when_all_tasks_already_live() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);
    let runner = FakeRunner {
        sessions: vec![live("hetz.demo-claude"), live("hetz.demo.ding")],
        ..Default::default()
    };
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    assert!(report.launched.is_empty());
    assert_eq!(report.adopted, vec!["demo"]);
}

#[test]
fn up_once_launches_only_the_missing_task() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);
    let runner = FakeRunner {
        sessions: vec![live("hetz.demo-claude")],
        ..Default::default()
    };
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    assert_eq!(report.launched, vec!["hetz.demo.ding"]);
}

#[test]
fn up_once_tears_down_a_retired_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let retired = r#"
identity = "demo"
retired = true
[pty.agent]
id = "hetz.demo-claude"
command = "exec claude 'boot'"
[exec.ding]
id = "hetz.demo.ding"
command = "st2 ding hetz.demo"
"#;
    write(tmp.path(), "agents/hetz/demo/agent.toml", retired);
    let runner = FakeRunner {
        sessions: vec![live("hetz.demo-claude"), live("hetz.demo.ding")],
        ..Default::default()
    };
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    let mut torn = report.torn_down.clone();
    torn.sort();
    assert_eq!(torn, vec!["hetz.demo-claude", "hetz.demo.ding"]);
    assert!(report.launched.is_empty());
}

#[test]
fn up_once_skips_other_host_specs() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/here/agent.toml",
        "identity=\"here\"\n[pty.agent]\ncommand=\"x\"\n",
    );
    write(
        tmp.path(),
        "agents/silber/there/agent.toml",
        "identity=\"there\"\n[pty.agent]\ncommand=\"y\"\n",
    );
    let runner = FakeRunner::default();
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    assert_eq!(report.launched.len(), 1);
    assert_eq!(report.other_host, vec!["there"]);
}

#[test]
fn up_once_collects_spawn_errors_without_aborting() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);
    let runner = FakeRunner {
        fail_spawn: Some("hetz.demo-claude".into()),
        ..Default::default()
    };
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    assert_eq!(report.launched, vec!["hetz.demo.ding"]);
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].contains("hetz.demo-claude"));
}

#[test]
fn up_once_reaps_dead_nonkeep_then_respawns() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);
    let runner = FakeRunner {
        sessions: vec![dead("hetz.demo-claude"), dead("hetz.demo.ding")],
        ..Default::default()
    };
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    let mut reaped = runner.reaped.borrow().clone();
    reaped.sort();
    assert_eq!(reaped, vec!["hetz.demo-claude", "hetz.demo.ding"]);
    assert!(
        runner.removed.borrow().is_empty(),
        "a crash restart is not final retirement cleanup"
    );
    assert_eq!(report.launched.len(), 2);
}

#[test]
fn up_once_does_not_restart_a_task_when_diagnostic_reap_fails() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);
    let runner = FakeRunner {
        sessions: vec![dead("hetz.demo-claude"), dead("hetz.demo.ding")],
        fail_reap: Some("hetz.demo-claude".into()),
        ..Default::default()
    };

    let report = up_once(tmp.path(), "hetz", &runner).unwrap();

    assert_eq!(report.launched, vec!["hetz.demo.ding"]);
    assert_eq!(runner.spawned.borrow().as_slice(), ["hetz.demo.ding"]);
    assert!(
        report
            .errors
            .iter()
            .any(|error| error == "reap hetz.demo-claude for restart: reap broke")
    );
}

#[test]
fn up_once_finally_removes_dead_retired_tasks_without_restarting_them() {
    let tmp = tempfile::tempdir().unwrap();
    let retired = AGENT.replacen(
        "type = \"service\"",
        "type = \"service\"\nretired = true",
        1,
    );
    write(tmp.path(), "agents/hetz/demo/agent.toml", &retired);
    let runner = FakeRunner {
        sessions: vec![dead("hetz.demo-claude"), dead("hetz.demo.ding")],
        ..Default::default()
    };

    let report = up_once(tmp.path(), "hetz", &runner).unwrap();

    let mut removed = runner.removed.borrow().clone();
    removed.sort();
    assert_eq!(removed, vec!["hetz.demo-claude", "hetz.demo.ding"]);
    assert!(runner.reaped.borrow().is_empty());
    assert!(report.launched.is_empty());
    assert_eq!(report.gc.len(), 2);
}

#[test]
fn flapping_cap_parks_a_fail_mode_task_that_keeps_dying() {
    let tmp = tempfile::tempdir().unwrap();
    // mode=fail → parks after attempts (the default mode=delay would rate-limit instead).
    write(
        tmp.path(),
        "agents/hetz/demo/agent.toml",
        "identity=\"demo\"\nsupervisor=\"cos-claude\"\n[restart]\nattempts=3\ninterval=\"60s\"\nmode=\"fail\"\n[pty.agent]\nid=\"hetz.demo-claude\"\ncommand=\"x\"\n",
    );
    let found = discover(tmp.path());
    let runner = FakeRunner {
        sessions: vec![dead("hetz.demo-claude")],
        ..Default::default()
    };
    let mut cap = FlappingCap::default();

    let mut last = UpReport::default();
    for _ in 0..5 {
        let plan = reconcile(&found.specs, &runner.sessions, "hetz");
        last = UpReport::default();
        execute(&plan, &runner, &mut cap, &mut last);
    }
    assert_eq!(last.launched.len(), 0);
    assert_eq!(last.flapping, vec!["hetz.demo-claude"]);
    assert!(last.gc.is_empty(), "parked flapper keeps its corpse");
    assert_eq!(runner.spawned.borrow().len(), 3); // limit
    assert_eq!(runner.reaped.borrow().len(), 3);
    assert!(
        runner.removed.borrow().is_empty(),
        "crash-loop handling must never take the final-retirement cleanup path"
    );

    // The rich crash-loop record carries what surfacing needs — the parked task, its agent, and the
    // supervisor to notify — recorded once (not per pass).
    assert_eq!(last.crash_loops.len(), 1);
    let cl = &last.crash_loops[0];
    assert_eq!(cl.pty_id, "hetz.demo-claude");
    assert_eq!(cl.identity, "demo");
    assert_eq!(cl.supervisor.as_deref(), Some("cos-claude"));
    assert_eq!(cl.agent_bus_id("hetz"), "hetz.demo");
}

/// A parked crash-loop is surfaced to the agent's supervisor over the native bus: a `crash-loop`-tagged
/// message from the runner lands in the supervisor's inbox. (This is the M2.4 guarantee — a crash-loop
/// isn't only an stderr line an operator has to be watching.)
#[test]
fn surface_crash_loop_notifies_the_supervisor_over_the_bus() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/demo/agent.toml",
        "identity=\"demo\"\nsupervisor=\"cos-claude\"\n[pty.agent]\nid=\"hetz.demo-claude\"\ncommand=\"x\"\n",
    );
    write(
        tmp.path(),
        "agents/hetz/cos-claude/agent.toml",
        "identity=\"cos-claude\"\n[pty.agent]\nid=\"hetz.cos\"\ncommand=\"x\"\n",
    );

    let cl = CrashLoop {
        pty_id: "hetz.demo-claude".to_string(),
        identity: "demo".to_string(),
        host: Some("hetz".to_string()),
        supervisor: Some("cos-claude".to_string()),
    };
    surface_crash_loop(tmp.path(), "hetz", &cl);

    let inbox = message::inbox_dir(&tmp.path().join("agents/hetz/cos-claude"));
    let msgs = message::list_dir(&inbox).unwrap();
    assert_eq!(
        msgs.len(),
        1,
        "supervisor gets exactly one crash-loop message"
    );
    let m = &msgs[0];
    assert_eq!(m.from.as_deref(), Some("st2.hetz")); // the runner is the sender
    assert_eq!(m.subject.as_deref(), Some("crash-loop: hetz.demo parked"));
    assert!(m.tags.contains(&"crash-loop".to_string()));
    assert!(
        m.body.contains("hetz.demo-claude"),
        "body names the parked task"
    );
}

/// `st2 down` kills every LIVE task of THIS host's catalog agents (the explicit teardown), skips
/// other hosts, and is idempotent about already-dead tasks.
#[test]
fn down_tears_down_this_hosts_live_tasks_only() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/demo/agent.toml",
        "identity=\"demo\"\n[pty.agent]\nid=\"hetz.demo-claude\"\ncommand=\"x\"\n",
    );
    write(
        tmp.path(),
        "agents/hetz/dead/agent.toml",
        "identity=\"dead\"\n[pty.agent]\nid=\"hetz.dead\"\ncommand=\"x\"\n",
    );
    write(
        tmp.path(),
        "agents/silber/other/agent.toml",
        "identity=\"other\"\nhost=\"silber\"\n[pty.agent]\nid=\"silber.other\"\ncommand=\"x\"\n",
    );

    // demo is live, dead is dead, other belongs to another host + is live.
    let runner = FakeRunner {
        sessions: vec![
            live("hetz.demo-claude"),
            dead("hetz.dead"),
            live("silber.other"),
        ],
        ..Default::default()
    };
    let report = down(tmp.path(), "hetz", &runner).unwrap();

    // Only hetz's LIVE task is killed. The dead one isn't (idempotent); the other host is skipped.
    assert_eq!(runner.killed.borrow().as_slice(), ["hetz.demo-claude"]);
    assert_eq!(report.torn_down, vec!["hetz.demo-claude"]);
    assert!(report.other_host.contains(&"other".to_string()));
    assert!(report.errors.is_empty());
}

/// No supervisor → nothing is sent (and it doesn't panic); the stderr line is the only surface.
#[test]
fn surface_crash_loop_without_supervisor_sends_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/demo/agent.toml",
        "identity=\"demo\"\n[pty.agent]\nid=\"hetz.demo-claude\"\ncommand=\"x\"\n",
    );
    let cl = CrashLoop {
        pty_id: "hetz.demo-claude".to_string(),
        identity: "demo".to_string(),
        host: Some("hetz".to_string()),
        supervisor: None,
    };
    // Must not panic; there is simply nobody to notify.
    surface_crash_loop(tmp.path(), "hetz", &cl);
}

#[test]
fn up_once_surfaces_discovery_errors_and_unrunnable() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        tmp.path(),
        "agents/hetz/good/agent.toml",
        "identity=\"good\"\n[pty.agent]\ncommand=\"x\"\n",
    );
    write(
        tmp.path(),
        "agents/hetz/bad/agent.toml",
        "identity=\"b\"\nnot valid =",
    );
    write(
        tmp.path(),
        "agents/hetz/nr/agent.toml",
        "identity=\"nr\"\ntype=\"service\"\n",
    );
    let runner = FakeRunner::default();
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    assert_eq!(report.launched, vec!["hetz.good.agent"]);
    assert_eq!(report.unrunnable, vec!["nr"]);
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].contains("bad/agent.toml"));
}

#[test]
fn up_once_marks_a_list_failure_as_a_skipped_pass() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "agents/hetz/demo/agent.toml", AGENT);
    let runner = FakeRunner {
        fail_list: true,
        ..Default::default()
    };
    let report = up_once(tmp.path(), "hetz", &runner).unwrap();
    assert!(report.skipped);
    assert!(report.launched.is_empty());
    assert!(runner.spawned.borrow().is_empty());
    assert_eq!(
        report.errors,
        vec!["list sessions (pass skipped): simulated list failure"]
    );
}
