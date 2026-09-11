#![cfg(unix)]

use std::fs;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

struct Daemon(Child);

fn st3_command(binary: &Path) -> Command {
    let mut command = Command::new(binary);
    command.env("ST_AGENT", "person/test");
    command
}

impl Daemon {
    fn stop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop();
    }
}

fn start_daemon(binary: &Path, root: &Path, socket: &Path) -> Daemon {
    let stderr = fs::File::create(socket.with_extension("daemon.stderr"))
        .expect("create the daemon diagnostic log");
    Daemon(
        st3_command(binary)
            .arg("up")
            .args(["--node", "survival-node"])
            .arg("--state-dir")
            .arg(root)
            .arg("--pty-root")
            .arg(root.join("pty"))
            .arg("--socket")
            .arg(socket)
            .args(["--peer-listen", "127.0.0.1:0"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("start st3"),
    )
}

#[test]
fn pty_helpers_use_graph_subjects_and_expected_incarnations() {
    let binary = assert_cmd::cargo::cargo_bin!("st3");
    let pty = Command::new("pty")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("the PTY runtime is required");
    assert!(pty.success(), "the PTY runtime is required");
    let temporary = tempfile::Builder::new()
        .prefix("st3-pty-")
        .tempdir_in("/tmp")
        .unwrap();
    let state = temporary.path().join("state");
    let socket = temporary.path().join("st3.sock");
    let intent = temporary.path().join("pty.kdl");
    fs::write(
        &intent,
        r#"version 2

  mission "pty-helpers" state="ready" {
    goal "Keep the operator PTY available."
    step "operator" {

        pty "operator" {
          argv "sh" "-c" "printf ready; read line; printf ' got:%s' \"$line\"; sleep 1"
          restart "never"
        }

    }
  }

"#,
    )
    .unwrap();
    let mut daemon = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            st3_command(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );
    let published = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "publish"])
        .arg(&intent)
        .args(["--as", "person/test"])
        .output()
        .unwrap();
    assert!(
        published.status.success(),
        "{}",
        String::from_utf8_lossy(&published.stderr)
    );
    let started = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "mission",
            "start",
            "pty-helpers",
            "--id",
            "pty-helpers/test",
            "--workspace",
            temporary.path().to_str().unwrap(),
            "--as",
            "person/test",
        ])
        .output()
        .unwrap();
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let subject = "pty/pty-helpers/test/operator".to_owned();
    let waited = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "wait",
            &subject,
            "--for",
            "running",
            "--timeout",
            "5s",
        ])
        .output()
        .unwrap();
    let inspected = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "inspect", &subject])
        .output()
        .unwrap();
    let daemon_stderr = fs::read_to_string(socket.with_extension("daemon.stderr"))
        .unwrap_or_else(|error| format!("cannot read the daemon diagnostic log: {error}"));
    assert!(
        waited.status.success(),
        "wait: {}\ninspect stdout:\n{}\ninspect stderr:\n{}\ndaemon stderr:\n{}",
        String::from_utf8_lossy(&waited.stderr),
        String::from_utf8_lossy(&inspected.stdout),
        String::from_utf8_lossy(&inspected.stderr),
        daemon_stderr,
    );
    let listed = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "pty", "ls"])
        .output()
        .unwrap();
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains(&subject));
    let sent = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "pty",
            "send",
            &subject,
            "hello",
        ])
        .output()
        .unwrap();
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    wait_for(
        || {
            st3_command(binary)
                .args([
                    "--endpoint",
                    socket.to_str().unwrap(),
                    "pty",
                    "peek",
                    &subject,
                ])
                .output()
                .is_ok_and(|output| {
                    output.status.success()
                        && String::from_utf8_lossy(&output.stdout).contains("got:hello")
                })
        },
        "the PTY did not receive the line",
    );
    let signalled = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "pty",
            "signal",
            &subject,
            "interrupt",
        ])
        .output()
        .unwrap();
    assert!(
        signalled.status.success(),
        "{}",
        String::from_utf8_lossy(&signalled.stderr)
    );
    daemon.stop();
}

fn wait_for(mut test: impl FnMut() -> bool, message: &str) {
    for _ in 0..200 {
        if test() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("{message}");
}

fn ready_work_subject(binary: &Path, socket: &Path, actor: &str, step: &str) -> Option<String> {
    let output = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "--json",
            "work",
            "ls",
            "--as",
            actor,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = serde_json::from_slice::<serde_json::Value>(&output.stdout).ok()?;
    value
        .as_array()?
        .iter()
        .find(|item| item["status"] == "ready" && item["step"] == step)
        .and_then(|item| item["subject"].as_str())
        .map(str::to_owned)
}

#[test]
fn an_agent_wait_wakes_when_new_work_becomes_ready() {
    let binary = assert_cmd::cargo::cargo_bin!("st3");
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let socket = temporary.path().join("st3.sock");
    let missions = temporary.path().join("missions.kdl");
    let actor = "agent/wait-owner/worker";
    fs::write(
        &missions,
        format!(
            r#"version 2

resource "wait-release" {{ kind "custom.test.wait-release" }}

mission "wait-owner" state="ready" {{
  goal "Hold one active work lease."
  agent "worker" {{
    workspace "${{ST_WORKSPACE}}"
    command "sleep 30"
    restart "never"
  }}
  step "hold" {{
    assigned-to "agent/${{ST_MISSION_RUN}}/worker"
    goal "Wait for a graph condition while retaining this lease."
  }}
  step "new" {{
    assigned-to "agent/${{ST_MISSION_RUN}}/worker"
    goal "Wake the agent immediately."
    baseline "the release signal is ready" {{
      field "status" "resource/wait-release" "is" "ready"
    }}
  }}
}}
"#
        ),
    )
    .unwrap();
    let mut daemon = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            st3_command(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );
    let published = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "publish"])
        .arg(&missions)
        .args(["--as", "person/test"])
        .output()
        .unwrap();
    assert!(
        published.status.success(),
        "{}",
        String::from_utf8_lossy(&published.stderr)
    );
    let started = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "mission",
            "start",
            "wait-owner",
            "--id",
            "wait-owner",
            "--workspace",
            temporary.path().to_str().unwrap(),
            "--as",
            "person/test",
        ])
        .output()
        .unwrap();
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let mut held = None;
    wait_for(
        || {
            held = ready_work_subject(binary, &socket, actor, "hold");
            held.is_some()
        },
        "the first step did not become ready",
    );
    let claimed = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "work", "claim"])
        .arg(held.unwrap())
        .args(["--as", actor])
        .output()
        .unwrap();
    assert!(
        claimed.status.success(),
        "{}",
        String::from_utf8_lossy(&claimed.stderr)
    );

    let wait_stderr = temporary.path().join("wait.stderr");
    let mut waiting = st3_command(binary);
    let mut waiting = waiting
        .env("ST_AGENT", actor)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "wait",
            "resource/not-ready",
            "--for",
            "ready",
            "--timeout",
            "5s",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::from(fs::File::create(&wait_stderr).unwrap()))
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(waiting.try_wait().unwrap().is_none());

    let ready_at = std::time::Instant::now();
    let released = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "claim",
            "resource/wait-release",
            "resource.observed",
            "--field",
            "status=ready",
            "--actor",
            "person/test",
        ])
        .output()
        .unwrap();
    assert!(
        released.status.success(),
        "{}",
        String::from_utf8_lossy(&released.stderr)
    );
    let mut offered = None;
    wait_for(
        || {
            offered = ready_work_subject(binary, &socket, actor, "new");
            offered.is_some()
        },
        "the second step did not become ready",
    );
    let mut wait_status = None;
    wait_for(
        || {
            wait_status = waiting.try_wait().ok().flatten();
            wait_status.is_some()
        },
        "new ready work did not interrupt the wait",
    );
    assert!(!wait_status.unwrap().success());
    assert!(
        ready_at.elapsed() < Duration::from_secs(1),
        "new ready work took too long to interrupt the wait"
    );
    let diagnostic = fs::read_to_string(wait_stderr).unwrap();
    assert!(diagnostic.contains("has ready work"), "{diagnostic}");
    assert!(diagnostic.contains("Run `st3 work ls`"), "{diagnostic}");
    daemon.stop();
}

fn alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn exec_subject(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .lines()
        .find(|line| line.starts_with("exec/"))
        .expect("the CLI did not print the exec subject")
        .to_owned()
}

fn exec_record(state: &Path, name: &str) -> Option<std::path::PathBuf> {
    fs::read_dir(state.join("exec"))
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.ends_with(&format!(".{name}.json")))
        })
}

fn interruptible_command(binary: &Path) -> Command {
    let mut command = st3_command(binary);
    command.process_group(0);
    unsafe {
        command.pre_exec(|| {
            libc::signal(libc::SIGINT, libc::SIG_DFL);
            let mut mask = std::mem::zeroed();
            libc::sigemptyset(&mut mask);
            if libc::sigprocmask(libc::SIG_SETMASK, &mask, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
}

#[test]
fn exec_returns_the_remote_status_and_retains_the_log() {
    let binary = assert_cmd::cargo::cargo_bin!("st3");
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let socket = temporary.path().join("st3.sock");
    let mut daemon = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            st3_command(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );
    let exec = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "exec",
            "--name",
            "exit-seven",
            "--",
            "sh",
            "-c",
            "printf exact-log; exit 7",
        ])
        .output()
        .expect("run an exec member");
    assert_eq!(exec.status.code(), Some(7));
    assert_eq!(exec.stdout, b"exact-log");
    let subject = exec_subject(&exec.stderr);
    let logs = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "logs", "--all"])
        .arg(&subject)
        .output()
        .expect("read the exec log");
    assert!(
        logs.status.success(),
        "{}",
        String::from_utf8_lossy(&logs.stderr)
    );
    assert_eq!(logs.stdout, b"exact-log");
    let signalled = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "exec",
            "--name",
            "signal-term",
            "--",
            "sh",
            "-c",
            "kill -TERM $$",
        ])
        .output()
        .expect("run a signalled exec member");
    assert_eq!(signalled.status.code(), Some(128 + libc::SIGTERM));

    let detached = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "exec",
            "--name",
            "wait-transition",
            "--detach",
            "--",
            "sh",
            "-c",
            "sleep 0.2",
        ])
        .output()
        .expect("run a detached exec member");
    assert!(detached.status.success());
    let detached_subject = String::from_utf8_lossy(&detached.stdout).trim().to_owned();
    let waited = st3_command(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "wait"])
        .arg(&detached_subject)
        .args(["--for", "exited", "--timeout", "2s"])
        .output()
        .expect("wait for the reconciled exit");
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    daemon.stop();
}

#[test]
fn interrupt_stops_follow_and_optional_cancel_stops_the_member() {
    let binary = assert_cmd::cargo::cargo_bin!("st3");
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let socket = temporary.path().join("st3.sock");
    let mut daemon = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            st3_command(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );

    let followed_started = std::time::Instant::now();
    let mut followed = interruptible_command(binary);
    let followed_stderr = temporary.path().join("followed.stderr");
    let mut followed = followed
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "exec",
            "--name",
            "interrupt-follow",
            "--",
            "sh",
            "-c",
            "sleep 2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::from(fs::File::create(&followed_stderr).unwrap()))
        .spawn()
        .unwrap();
    let mut followed_record = None;
    wait_for(
        || {
            followed_record = exec_record(&state, "interrupt-follow");
            followed_record.is_some()
        },
        "the followed exec did not start",
    );
    wait_for(
        || fs::read_to_string(&followed_stderr).is_ok_and(|text| text.contains("exec/")),
        "the followed exec did not enter the log follower",
    );
    assert!(
        followed_started.elapsed() < Duration::from_secs(1),
        "the CLI did not publish its subject promptly"
    );
    let followed_record = followed_record.unwrap();
    let followed_generation: st_runtime::ExecGeneration =
        serde_json::from_slice(&fs::read(&followed_record).unwrap()).unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        unsafe { libc::kill(-(followed.id() as i32), libc::SIGINT) },
        0
    );
    let mut followed_status = None;
    wait_for(
        || {
            followed_status = followed.try_wait().ok().flatten();
            followed_status.is_some()
        },
        "the interrupted log follower did not exit",
    );
    assert_eq!(followed_status.unwrap().code(), Some(130));
    assert!(
        alive(followed_generation.pid),
        "an ordinary interrupt stopped the remote member"
    );
    wait_for(
        || !alive(followed_generation.pid),
        "the short remote member did not finish",
    );

    let mut cancelled = interruptible_command(binary);
    let cancelled_stderr = temporary.path().join("cancelled.stderr");
    let mut cancelled = cancelled
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "exec",
            "--name",
            "interrupt-cancel",
            "--cancel-on-interrupt",
            "--",
            "sh",
            "-c",
            "sleep 30",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::from(fs::File::create(&cancelled_stderr).unwrap()))
        .spawn()
        .unwrap();
    let mut cancelled_record = None;
    wait_for(
        || {
            cancelled_record = exec_record(&state, "interrupt-cancel");
            cancelled_record.is_some()
        },
        "the cancellable exec did not start",
    );
    wait_for(
        || fs::read_to_string(&cancelled_stderr).is_ok_and(|text| text.contains("exec/")),
        "the cancellable exec did not enter the log follower",
    );
    let cancelled_record = cancelled_record.unwrap();
    let cancelled_generation: st_runtime::ExecGeneration =
        serde_json::from_slice(&fs::read(&cancelled_record).unwrap()).unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        unsafe { libc::kill(-(cancelled.id() as i32), libc::SIGINT) },
        0
    );
    let mut cancelled_status = None;
    wait_for(
        || {
            cancelled_status = cancelled.try_wait().ok().flatten();
            cancelled_status.is_some()
        },
        "the cancelling follower did not exit",
    );
    assert_eq!(cancelled_status.unwrap().code(), Some(130));
    wait_for(
        || !alive(cancelled_generation.pid),
        "cancel-on-interrupt left the remote member running",
    );
    daemon.stop();
}

#[test]
fn an_exec_survives_a_daemon_restart_and_is_adopted() {
    let binary = assert_cmd::cargo::cargo_bin!("st3");
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let socket = temporary.path().join("st3.sock");
    let mut daemon = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            st3_command(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the first daemon did not become ready",
    );

    let detached = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "exec",
            "--name",
            "survival",
            "--detach",
            "--",
            "sh",
            "-c",
            "sleep 2; printf survived",
        ])
        .output()
        .expect("publish the detached exec");
    assert!(
        detached.status.success(),
        "{}",
        String::from_utf8_lossy(&detached.stderr)
    );
    let subject = String::from_utf8_lossy(&detached.stdout).trim().to_owned();
    assert!(subject.starts_with("exec/"));
    let record = state
        .join("exec")
        .join(format!("{}.json", subject.replace('/', ".")));
    wait_for(|| record.exists(), "the exec record did not appear");
    let generation: st_runtime::ExecGeneration =
        serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    assert!(alive(generation.pid));

    daemon.stop();
    assert!(alive(generation.pid), "the daemon kill stopped the exec");

    let mut replacement = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            let output = st3_command(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "inspect", &subject])
                .output();
            output.is_ok_and(|output| {
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains("\"adopted\": true")
            })
        },
        "the replacement daemon did not adopt the exec",
    );
    wait_for(|| !alive(generation.pid), "the exec did not finish");
    let logs = st3_command(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "logs",
            "--all",
            &subject,
        ])
        .output()
        .expect("read the retained log");
    assert!(
        logs.status.success(),
        "{}",
        String::from_utf8_lossy(&logs.stderr)
    );
    assert_eq!(logs.stdout, b"survived");
    replacement.stop();
}
