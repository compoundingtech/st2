#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

struct Daemon(Child);

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
    Daemon(
        Command::new(binary)
            .arg("up")
            .args(["--node", "survival-node"])
            .arg("--state-dir")
            .arg(root)
            .arg("--pty-root")
            .arg(root.join("pty"))
            .arg("--socket")
            .arg(socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
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
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let socket = temporary.path().join("st3.sock");
    let intent = temporary.path().join("pty.kdl");
    fs::write(
        &intent,
        r#"subgraph {
  pty "operator" {
    argv "sh" "-c" "printf ready; read line; printf ' got:%s' \"$line\"; sleep 1"
    restart "never"
  }
}
"#,
    )
    .unwrap();
    let mut daemon = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            Command::new(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );
    let published = Command::new(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "run"])
        .arg(&intent)
        .output()
        .unwrap();
    assert!(
        published.status.success(),
        "{}",
        String::from_utf8_lossy(&published.stderr)
    );
    let waited = Command::new(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "wait",
            "pty/operator",
            "--for",
            "running",
            "--timeout",
            "5s",
        ])
        .output()
        .unwrap();
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    let listed = Command::new(binary)
        .args(["--endpoint", socket.to_str().unwrap(), "pty", "ls"])
        .output()
        .unwrap();
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains("pty/operator"));
    let sent = Command::new(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "pty",
            "send",
            "pty/operator",
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
            Command::new(binary)
                .args([
                    "--endpoint",
                    socket.to_str().unwrap(),
                    "pty",
                    "peek",
                    "pty/operator",
                ])
                .output()
                .is_ok_and(|output| {
                    output.status.success()
                        && String::from_utf8_lossy(&output.stdout).contains("got:hello")
                })
        },
        "the PTY did not receive the line",
    );
    let signalled = Command::new(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "pty",
            "signal",
            "pty/operator",
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

fn alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
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
            Command::new(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );
    let exec = Command::new(binary)
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
    let logs = Command::new(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "logs",
            "--all",
            "exec/exit-seven",
        ])
        .output()
        .expect("read the exec log");
    assert!(
        logs.status.success(),
        "{}",
        String::from_utf8_lossy(&logs.stderr)
    );
    assert_eq!(logs.stdout, b"exact-log");
    let signalled = Command::new(binary)
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

    let detached = Command::new(binary)
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
    let waited = Command::new(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "wait",
            "exec/wait-transition",
            "--for",
            "exited",
            "--timeout",
            "2s",
        ])
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
            Command::new(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the daemon did not become ready",
    );

    let mut followed = Command::new(binary)
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
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let followed_record = state.join("exec/exec.interrupt-follow.json");
    wait_for(
        || followed_record.exists(),
        "the followed exec did not start",
    );
    let followed_generation: st_runtime::ExecGeneration =
        serde_json::from_slice(&fs::read(&followed_record).unwrap()).unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(unsafe { libc::kill(followed.id() as i32, libc::SIGINT) }, 0);
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

    let mut cancelled = Command::new(binary)
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
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let cancelled_record = state.join("exec/exec.interrupt-cancel.json");
    wait_for(
        || cancelled_record.exists(),
        "the cancellable exec did not start",
    );
    let cancelled_generation: st_runtime::ExecGeneration =
        serde_json::from_slice(&fs::read(&cancelled_record).unwrap()).unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        unsafe { libc::kill(cancelled.id() as i32, libc::SIGINT) },
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
            Command::new(binary)
                .args(["--endpoint", socket.to_str().unwrap(), "doctor"])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        "the first daemon did not become ready",
    );

    let detached = Command::new(binary)
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
    let record = state.join("exec/exec.survival.json");
    wait_for(|| record.exists(), "the exec record did not appear");
    let generation: st_runtime::ExecGeneration =
        serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    assert!(alive(generation.pid));

    daemon.stop();
    assert!(alive(generation.pid), "the daemon kill stopped the exec");

    let mut replacement = start_daemon(binary, &state, &socket);
    wait_for(
        || {
            let output = Command::new(binary)
                .args([
                    "--endpoint",
                    socket.to_str().unwrap(),
                    "inspect",
                    "exec/survival",
                ])
                .output();
            output.is_ok_and(|output| {
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains("\"adopted\": true")
            })
        },
        "the replacement daemon did not adopt the exec",
    );
    wait_for(|| !alive(generation.pid), "the exec did not finish");
    let logs = Command::new(binary)
        .args([
            "--endpoint",
            socket.to_str().unwrap(),
            "logs",
            "--all",
            "exec/survival",
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
