//! The Nomad-decoupling gate (hard requirement for st2 replacing convoy).
//!
//! st2 is a *decoupled* supervisor: stopping or crashing the runner MUST leave its running tasks
//! alive, and a fresh runner MUST ADOPT them rather than cold-booting. The ONLY thing that kills a
//! task is an explicit teardown (a `retired` spec — st2's `convoy down`). This is the exact guarantee
//! whose absence cost a 45-minute convoy fleet outage, so it is proven here with REAL processes —
//! spawning the real `st2` binary, killing it with SIGTERM *and* SIGKILL, and asserting the task pids
//! survive and get re-adopted — not with a fake runner.
//!
//! Two spawn paths, both covered:
//!   - `exec` — st2 supervises the process directly and detaches it with `setsid`, so it survives
//!     st2's death (this is st2's OWN code providing the property).
//!   - `pty`  — st2 shells out to the `pty` daemon, which owns the session independently of st2.
//!
//! Isolation: every fixture points `$XDG_STATE_HOME` (exec pids) and `$PTY_ROOT` (pty sessions) at its
//! own temp dirs and uses a throwaway host, so these tests can NEVER touch the live fleet's sessions.
//! Tasks are `sleep 120` — long enough for the test, short enough to self-reap if cleanup is ever
//! skipped; the fixture's `Drop` also force-kills anything it spawned, even on a panic.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use st2::host_lock::process_alive;

const HOST: &str = "nomadtest";
// `exec` so `sh -c` replaces itself with sleep (dash otherwise FORKS a bare command, leaving the real
// workload as a grandchild the recorded pid isn't) — so the recorded pid IS the workload and the
// survival/teardown assertions are about the real task, not a shell wrapper.
const TASK_CMD: &str = "exec sleep 120";
const SPAWN_TIMEOUT: Duration = Duration::from_secs(15);
const DEATH_TIMEOUT: Duration = Duration::from_secs(10);

// ── fixture: an env-isolated catalog + a handle to the real st2 binary ───────────────────────────

struct Fixture {
    catalog: PathBuf,
    xdg: PathBuf,
    pty_root: PathBuf,
    /// pty session ids this fixture may have created — force-killed on drop.
    pty_sessions: std::cell::RefCell<Vec<String>>,
    _tmp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (catalog, xdg, pty_root) = (root.join("catalog"), root.join("xdg"), root.join("ptyroot"));
        for d in [&catalog, &xdg, &pty_root] {
            std::fs::create_dir_all(d).unwrap();
        }
        Fixture { catalog, xdg, pty_root, pty_sessions: Default::default(), _tmp: tmp }
    }

    /// An `st2` invocation with this fixture's isolated state env.
    fn st2(&self) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_st2"));
        c.env("XDG_STATE_HOME", &self.xdg).env("PTY_ROOT", &self.pty_root);
        c
    }

    /// Write a one-task service agent (`kind` = "exec" | "pty"), optionally retired.
    fn write_agent(&self, identity: &str, kind: &str, retired: bool) {
        if kind == "pty" {
            self.pty_sessions.borrow_mut().push(format!("{HOST}.{identity}.task"));
        }
        let retired_line = if retired { "retired #true" } else { "" };
        let kdl = format!(
            "agent \"{identity}\" {{\n  identity \"{identity}\"\n  host \"{HOST}\"\n  \
             type \"service\"\n  {retired_line}\n  {kind} \"task\" {{ command \"{TASK_CMD}\" }}\n}}\n"
        );
        let path = self.catalog.join(HOST).join(identity).join("agent.kdl");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, kdl).unwrap();
    }

    /// The pidfile whose liveness == "the task is running": the exec child's pid, or the pty daemon's
    /// pid (which owns the session). Both survive st2's death when the property holds.
    fn task_pidfile(&self, kind: &str, identity: &str) -> PathBuf {
        match kind {
            "exec" => self.xdg.join("st2").join(HOST).join("exec").join(format!("{HOST}.{identity}.task.pid")),
            "pty" => self.pty_root.join(format!("{HOST}.{identity}.task.pid")),
            other => panic!("unknown task kind {other}"),
        }
    }

    /// Spawn the real `st2 up` supervisor loop (long interval — the immediate first pass does the
    /// spawning; the loop just gives us a live runner to kill).
    fn spawn_loop(&self) -> Child {
        self.st2()
            .arg("up")
            .arg(&self.catalog)
            .args(["--host", HOST, "--interval", "60"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    /// One `st2 up --once` pass; returns its stdout (where launched/adopted/torn-down is reported).
    fn up_once(&self) -> String {
        let out = self.st2().arg("up").arg(&self.catalog).args(["--host", HOST, "--once"]).output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Best-effort teardown so nothing outlives the test, even on a panic.
        let quiet = |mut c: Command| {
            let _ = c.stdout(Stdio::null()).stderr(Stdio::null()).status();
        };
        let exec_dir = self.xdg.join("st2").join(HOST).join("exec");
        if let Ok(entries) = std::fs::read_dir(&exec_dir) {
            for e in entries.flatten() {
                if e.path().extension().and_then(|x| x.to_str()) == Some("pid")
                    && let Some(pid) = read_pid(&e.path())
                {
                    // Kill the process group (setsid leader) and the pid, so nothing is left behind.
                    for target in [format!("-{pid}"), pid.to_string()] {
                        let mut c = Command::new("kill");
                        c.arg("-KILL").arg(target);
                        quiet(c);
                    }
                }
            }
        }
        for sess in self.pty_sessions.borrow().iter() {
            for verb in ["kill", "rm"] {
                let mut c = Command::new("pty");
                c.env("PTY_ROOT", &self.pty_root).args([verb, sess]);
                quiet(c);
            }
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────────────────────────

/// Owns the spawned `st2` supervisor child so a panicking assertion can never leave it running (a
/// bare `Child`'s drop neither kills nor reaps). On the happy path the test kills+reaps it explicitly;
/// this just backstops failures.
struct Runner(Child);

impl Drop for Runner {
    fn drop(&mut self) {
        let _ = self.0.kill(); // SIGKILL if still alive (no-op once already exited)
        let _ = self.0.wait(); // reap
    }
}

fn read_pid(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_alive(pidfile: &Path) -> bool {
    read_pid(pidfile).is_some_and(process_alive)
}

fn signal_pid(pid: i32, sig: &str) {
    let ok = Command::new("kill").arg(format!("-{sig}")).arg(pid.to_string()).status().unwrap().success();
    assert!(ok, "failed to send SIG{sig} to pid {pid}");
}

fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    cond()
}

fn pty_available() -> bool {
    Command::new("pty").arg("--help").output().map(|o| o.status.success()).unwrap_or(false)
}

/// These tests drive the real `st2` binary, whose SystemRunner lists `pty` sessions every reconcile
/// pass — so `pty` must be on PATH for ANY of them to prove anything (without it every pass is skipped
/// and nothing spawns). Because this is a gate, a missing `pty` is a HARD FAILURE — never a silent
/// skip that reads as green — unless a dev explicitly opts out with `ST2_ALLOW_PTY_SKIP` on a box that
/// has no pty. Returns true to run, false to return early (only via the opt-out). CI/gating MUST
/// install `pty` so these actually run. (The pty-free exec spawn/kill/group-teardown coverage lives in
/// `tests/exec_backend.rs`, which always runs.)
fn pty_gate(test: &str) -> bool {
    if pty_available() {
        return true;
    }
    assert!(
        std::env::var_os("ST2_ALLOW_PTY_SKIP").is_some(),
        "{test}: `pty` is not on PATH, so st2's nomad-decoupling gate cannot run and is UNPROVEN. \
         Install `pty` (required in CI/gating), or set ST2_ALLOW_PTY_SKIP=1 to skip on a dev box \
         without pty."
    );
    eprintln!("SKIP {test}: `pty` not on PATH (ST2_ALLOW_PTY_SKIP set)");
    false
}

// ── the guarantee: killing the runner leaves the task alive; a fresh runner adopts it ─────────────

fn runner_death_survives_and_readopts(kind: &str, signal: &str) {
    // The whole scenario runs the real `st2` binary, whose SystemRunner lists `pty` sessions every
    // reconcile pass — so pty must be present even for an `exec` task, or every pass is skipped and
    // nothing spawns. Gate accordingly (fail-loud without pty; see `pty_gate`).
    if !pty_gate(&format!("runner_death_survives_and_readopts({kind}/{signal})")) {
        return;
    }
    let fx = Fixture::new();
    fx.write_agent("survivor", kind, false);

    // 1) A real runner spawns the task.
    let mut runner = Runner(fx.spawn_loop());
    let pidfile = fx.task_pidfile(kind, "survivor");
    assert!(
        poll_until(SPAWN_TIMEOUT, || read_alive(&pidfile)),
        "{kind}/{signal}: runner never brought up a live task (pidfile {})",
        pidfile.display()
    );
    let task_pid = read_pid(&pidfile).unwrap();

    // 2) Kill the runner with the given signal and reap it, so it is provably gone.
    let runner_pid = runner.0.id() as i32;
    signal_pid(runner_pid, signal);
    runner.0.wait().unwrap();
    assert!(!process_alive(runner_pid), "{kind}/{signal}: runner did not actually die");

    // 3) THE PROPERTY: the task outlived its supervisor.
    assert!(
        process_alive(task_pid),
        "{kind}/{signal}: task pid {task_pid} died with the runner — st2 is NOT nomad-decoupled"
    );

    // 4) A fresh runner ADOPTS (report says adopted, never launched) and does not disturb the pid.
    let out = fx.up_once();
    assert!(
        out.contains("adopted (1): survivor"),
        "{kind}/{signal}: fresh runner did not adopt the surviving task; output:\n{out}"
    );
    assert!(
        !out.contains("launched"),
        "{kind}/{signal}: fresh runner COLD-BOOTED (launched) instead of adopting; output:\n{out}"
    );
    assert!(
        process_alive(task_pid),
        "{kind}/{signal}: adoption changed/killed the task pid {task_pid} (cold boot, not adopt)"
    );
}

#[test]
fn sigterm_runner_leaves_exec_task_alive_and_readopts() {
    runner_death_survives_and_readopts("exec", "TERM");
}

#[test]
fn sigkill_runner_leaves_exec_task_alive_and_readopts() {
    runner_death_survives_and_readopts("exec", "KILL");
}

#[test]
fn sigterm_runner_leaves_pty_task_alive_and_readopts() {
    runner_death_survives_and_readopts("pty", "TERM");
}

#[test]
fn sigkill_runner_leaves_pty_task_alive_and_readopts() {
    runner_death_survives_and_readopts("pty", "KILL");
}

// ── the inverse: only an explicit teardown kills; a plain stop never does ──────────────────────────

fn explicit_teardown_kills_but_plain_stop_does_not(kind: &str) {
    if !pty_gate(&format!("explicit_teardown_kills_but_plain_stop_does_not({kind})")) {
        return;
    }
    let fx = Fixture::new();
    fx.write_agent("survivor", kind, false);

    let mut runner = Runner(fx.spawn_loop());
    let pidfile = fx.task_pidfile(kind, "survivor");
    assert!(poll_until(SPAWN_TIMEOUT, || read_alive(&pidfile)), "{kind}: runner never brought up a task");
    let task_pid = read_pid(&pidfile).unwrap();

    // A plain stop (SIGTERM the supervisor) must NOT kill the task.
    signal_pid(runner.0.id() as i32, "TERM");
    runner.0.wait().unwrap();
    assert!(process_alive(task_pid), "{kind}: a plain runner stop killed the task — it must not");

    // Retiring the spec is st2's `convoy down`: the ONE action that tears the task down.
    fx.write_agent("survivor", kind, true);
    let out = fx.up_once();
    assert!(out.contains("torn down"), "{kind}: retiring the spec did not tear the task down; output:\n{out}");
    assert!(
        poll_until(DEATH_TIMEOUT, || !process_alive(task_pid)),
        "{kind}: explicit teardown did not actually kill task pid {task_pid}"
    );
}

#[test]
fn explicit_teardown_kills_exec_but_plain_stop_does_not() {
    explicit_teardown_kills_but_plain_stop_does_not("exec");
}

#[test]
fn explicit_teardown_kills_pty_but_plain_stop_does_not() {
    explicit_teardown_kills_but_plain_stop_does_not("pty");
}
