//! `st2 agent attach --id <AGENT-ID>` — hand this terminal to a running agent's canonical PTY.
//!
//! st2's whole contribution is resolve → wait → exec, so that is exactly what these tests pin: the
//! runtime id is *derived from the declaration* (pinned `id`, else `<agent-id>.<task-name>`),
//! selection is exact-ID only (an address must never resolve), the readiness wait needs *positive*
//! liveness evidence and is bounded, and the handoff is a transparent `pty attach --force <id>`
//! carrying the catalog's own bus env.
//!
//! `pty` is injected the way every other st2 integration test injects it: a shim in a temp dir that
//! is the process's *entire* PATH. There is no production binary-override hook and these tests do
//! not add one. Liveness is faked by writing the pidfile `pty` itself would write, which is why the
//! cheap `kill(pid, 0)` probe — not a `pty list --json` subprocess — is the readiness mechanism.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// A catalog whose declared pty-root is a real temp dir, plus a `pty` shim that echoes the bus env
/// and its argv. `agent_kdl` is the whole agent declaration so each test can vary identity, host,
/// and task shape.
struct Fixture {
    _tmp: tempfile::TempDir,
    catalog: PathBuf,
    pty_root: PathBuf,
    bin: PathBuf,
    state: PathBuf,
    marker: PathBuf,
}

impl Fixture {
    fn new(agent_kdl: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let pty_root = tmp.path().join("pty-root");
        let bin = tmp.path().join("bin");
        let state = tmp.path().join("state");
        let marker = tmp.path().join("pty-ran");
        fs::create_dir_all(catalog.join("agents/h/worker")).unwrap();
        fs::create_dir(&pty_root).unwrap();
        fs::create_dir(&bin).unwrap();
        fs::create_dir(&state).unwrap();
        fs::write(
            catalog.join("catalog.kdl"),
            format!("catalog {{ pty-root {:?} }}\n", pty_root.display().to_string()),
        )
        .unwrap();
        fs::write(catalog.join("agents/h/worker/agent.kdl"), agent_kdl).unwrap();
        // The shim records that it ran at all (so a refusal can be proven to have exec'd nothing)
        // and echoes what st2 handed it.
        write_executable(
            &bin.join("pty"),
            "#!/bin/sh\n: > \"$ATTACH_MARKER\"\necho CATALOG=$CATALOG\necho PTY_ROOT=$PTY_ROOT\necho ST_ROOT=$ST_ROOT\necho ARGS=$*\n",
        );
        Self {
            _tmp: tmp,
            catalog,
            pty_root,
            bin,
            state,
            marker,
        }
    }

    /// A pidfile naming a live process — exactly the positive evidence `pty` publishes.
    fn seed_live_session(&self, runtime_id: &str) {
        fs::write(
            self.pty_root.join(format!("{runtime_id}.pid")),
            format!("{}\n", std::process::id()),
        )
        .unwrap();
    }

    fn attach(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_st2"));
        cmd.arg("--catalog")
            .arg(&self.catalog)
            .args(["agent", "attach"])
            .args(args)
            // The shim dir IS the whole PATH: no ambient `pty` can be reached.
            .env("PATH", &self.bin)
            .env("XDG_STATE_HOME", &self.state)
            .env("ATTACH_MARKER", &self.marker)
            .env_remove("CATALOG")
            .env_remove("ST_ROOT")
            .env_remove("PTY_ROOT");
        cmd
    }

    fn run(&self, args: &[&str]) -> Output {
        self.attach(args).output().unwrap()
    }

    /// The scrubbed-env form with an *exported* PTY_ROOT put back — the runner honors that value
    /// over the catalog declaration, so attach must too.
    fn run_with_ambient_pty_root(&self, ambient: &Path, args: &[&str]) -> Output {
        self.attach(args)
            .env("PTY_ROOT", ambient)
            .output()
            .unwrap()
    }

    fn seed_live_session_in(&self, root: &Path, runtime_id: &str) {
        fs::write(
            root.join(format!("{runtime_id}.pid")),
            format!("{}\n", std::process::id()),
        )
        .unwrap();
    }

    fn pty_ran(&self) -> bool {
        self.marker.exists()
    }
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    let mut mode = fs::metadata(path).unwrap().permissions();
    mode.set_mode(0o755);
    fs::set_permissions(path, mode).unwrap();
}

/// An agent whose canonical PTY task pins no id: the runtime id is derived.
const DERIVED: &str = r#"
agent "worker" {
  host "h"
  pty "agent" {
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#;

/// The migrated shape: the canonical task pins the bare agent id.
const PINNED: &str = r#"
agent "worker" {
  host "h"
  pty "agent" {
    id "h.worker"
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#;

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn attach_execs_pty_with_force_and_the_catalogs_own_bus_env() {
    let fx = Fixture::new(DERIVED);
    fx.seed_live_session("h.worker.agent");

    let out = fx.run(&["--id", "h.worker", "--host", "h"]);
    let s = stdout(&out);

    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        s.contains("ARGS=attach --force h.worker.agent"),
        "attach must hand pty a fixed argv with --force (the caller is itself inside a session): {s}"
    );
    assert!(
        s.contains(&format!("CATALOG={}", fx.catalog.display())),
        "{s}"
    );
    assert!(
        s.contains(&format!("ST_ROOT={}", fx.catalog.display())),
        "{s}"
    );
    assert!(
        s.contains(&format!("PTY_ROOT={}", fx.pty_root.display())),
        "the catalog's declared registry must be handed to pty, not the caller's ambient one: {s}"
    );
}

#[test]
fn a_pinned_task_id_is_attached_verbatim() {
    let fx = Fixture::new(PINNED);
    fx.seed_live_session("h.worker");

    let out = fx.run(&["--id", "h.worker", "--host", "h"]);
    let s = stdout(&out);

    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        s.contains("ARGS=attach --force h.worker"),
        "a pinned task id must win over the derived `<agent-id>.<task>` form: {s}"
    );
}

#[test]
fn selection_is_exact_id_only_and_never_falls_back_to_the_address() {
    // An immutable id distinct from the route: `worker` / `h.worker` name this subject only as an
    // address, and attach has no address namespace.
    let fx = Fixture::new(
        r#"
agent "worker" {
  host "h"
  id "AG-1"
  pty "agent" {
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#,
    );
    fx.seed_live_session("AG-1.agent");

    let by_address = fx.run(&["--id", "h.worker", "--host", "h", "--wait", "1"]);
    assert!(!by_address.status.success());
    assert!(
        stderr(&by_address).contains("no agent with id 'h.worker'"),
        "{}",
        stderr(&by_address)
    );
    assert!(
        !fx.pty_ran(),
        "a refused selection must exec nothing at all"
    );

    let by_id = fx.run(&["--id", "AG-1", "--host", "h"]);
    assert!(by_id.status.success(), "{}", stderr(&by_id));
    assert!(
        stdout(&by_id).contains("ARGS=attach --force AG-1.agent"),
        "{}",
        stdout(&by_id)
    );
}

#[test]
fn attach_polls_until_the_session_becomes_live() {
    let fx = Fixture::new(DERIVED);

    // No pidfile yet: a one-shot readiness check would refuse here.
    let child = fx
        .attach(&["--id", "h.worker", "--host", "h", "--wait", "10"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(250));
    fx.seed_live_session("h.worker.agent");

    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).contains("ARGS=attach --force h.worker.agent"),
        "{}",
        stdout(&out)
    );
}

#[test]
fn a_never_live_session_refuses_within_the_bound_and_execs_nothing() {
    let fx = Fixture::new(DERIVED);

    let started = Instant::now();
    let out = fx.run(&["--id", "h.worker", "--host", "h", "--wait", "1"]);
    let elapsed = started.elapsed();
    let err = stderr(&out);

    assert!(!out.status.success());
    assert!(
        elapsed < Duration::from_secs(5),
        "--wait 1 must be a real bound, took {elapsed:?}"
    );
    assert!(
        err.contains("pty session 'h.worker.agent' is not live after 1s"),
        "the diagnostic must name the runtime id actually probed: {err}"
    );
    assert!(
        err.contains("st2 tasks --json") && err.contains("st2 up"),
        "a timeout is a human diagnostic and must carry a remedy: {err}"
    );
    assert!(!fx.pty_ran(), "nothing may be attached after a timeout");
}

#[test]
fn a_dead_pidfile_is_not_readiness() {
    let fx = Fixture::new(DERIVED);
    // A reaped child's pid: the registry entry exists but proves death, not liveness.
    let mut reaped = Command::new("/bin/sh").arg("-c").arg("exit 0").spawn().unwrap();
    let dead_pid = reaped.id();
    reaped.wait().unwrap();
    fs::write(
        fx.pty_root.join("h.worker.agent.pid"),
        format!("{dead_pid}\n"),
    )
    .unwrap();

    let out = fx.run(&["--id", "h.worker", "--host", "h", "--wait", "1"]);

    assert!(!out.status.success(), "{}", stdout(&out));
    assert!(
        stderr(&out).contains("is not live after 1s"),
        "{}",
        stderr(&out)
    );
    assert!(!fx.pty_ran(), "only positive evidence may trigger the exec");
}

#[test]
fn an_agent_without_a_canonical_pty_task_refuses() {
    let missing = Fixture::new(
        r#"
agent "worker" {
  host "h"
  pty "shell" {
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#,
    );
    let out = missing.run(&["--id", "h.worker", "--host", "h", "--wait", "1"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("has no canonical `agent` task"),
        "{}",
        stderr(&out)
    );
    assert!(!missing.pty_ran());

    let not_a_pty = Fixture::new(
        r#"
agent "worker" {
  host "h"
  exec "agent" {
    lifecycle "adopt-only"
    argv "agent-bin"
  }
}
"#,
    );
    let out = not_a_pty.run(&["--id", "h.worker", "--host", "h", "--wait", "1"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("canonical task is not a PTY"),
        "{}",
        stderr(&out)
    );
    assert!(!not_a_pty.pty_ran());
}

#[test]
fn an_agent_homed_elsewhere_refuses_instead_of_probing_a_foreign_registry() {
    let fx = Fixture::new(DERIVED);
    fx.seed_live_session("h.worker.agent");

    let out = fx.run(&["--id", "h.worker", "--host", "other", "--wait", "1"]);
    let err = stderr(&out);

    assert!(!out.status.success(), "{}", stdout(&out));
    assert!(
        err.contains("is homed on host 'h'") && err.contains("from 'other'"),
        "{err}"
    );
    assert!(
        !fx.pty_ran(),
        "a cross-host attach must not exec even when a same-named session looks live locally"
    );
}

#[test]
fn a_missing_pty_binary_surfaces_the_exec_failure() {
    let fx = Fixture::new(DERIVED);
    fx.seed_live_session("h.worker.agent");
    fs::remove_file(fx.bin.join("pty")).unwrap();

    let out = fx.run(&["--id", "h.worker", "--host", "h"]);

    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("failed to exec `pty`"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn an_exported_pty_root_is_both_probed_and_handed_to_pty() {
    // The runner resolves PTY_ROOT ambient-first, so an exported root is where st2-managed sessions
    // actually live. Attach must probe THAT registry and hand `pty` the same one — proving one
    // session alive and then pointing pty at a different directory is the bug this pins.
    let fx = Fixture::new(DERIVED);
    let ambient = tempfile::tempdir().unwrap();

    // A decoy in the catalog-declared root: it must not be mistaken for readiness.
    fx.seed_live_session("h.worker.agent");
    let stale = fx.run_with_ambient_pty_root(ambient.path(), &["--id", "h.worker", "--host", "h", "--wait", "1"]);
    assert!(
        !stale.status.success(),
        "the declared root must not be probed when an ambient root outranks it: {}",
        stdout(&stale)
    );
    assert!(!fx.pty_ran());

    fx.seed_live_session_in(ambient.path(), "h.worker.agent");
    let out = fx.run_with_ambient_pty_root(ambient.path(), &["--id", "h.worker", "--host", "h"]);
    let s = stdout(&out);

    assert!(out.status.success(), "{}", stderr(&out));
    assert!(s.contains("ARGS=attach --force h.worker.agent"), "{s}");
    assert!(
        s.contains(&format!("PTY_ROOT={}", ambient.path().display())),
        "pty must receive exactly the registry st2 proved the session alive in: {s}"
    );
    assert!(
        !s.contains(&format!("PTY_ROOT={}", fx.pty_root.display())),
        "the catalog-rendered root must not shadow the probed one: {s}"
    );
}

#[test]
fn an_absurd_wait_is_refused_instead_of_panicking() {
    let fx = Fixture::new(DERIVED);

    let out = fx.run(&["--id", "h.worker", "--host", "h", "--wait", &u64::MAX.to_string()]);
    let err = stderr(&out);

    assert!(!out.status.success());
    assert!(
        !err.contains("panicked") && !err.contains("overflow when adding duration"),
        "a huge --wait must be a diagnostic, not a runtime panic: {err}"
    );
    assert!(
        err.contains("overflows the monotonic clock"),
        "the refusal must explain the bound it could not represent: {err}"
    );
    assert!(!fx.pty_ran());
}
