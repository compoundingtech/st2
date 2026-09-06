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
        r#"version 2

  plan "pty-helpers" state="ready" {
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
            "plan",
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
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
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
