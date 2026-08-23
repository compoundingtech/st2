//! The stop path's terminal-record ordering, proven against the real wrapper binary.
//!
//! st2's own stop escalates to a SIGKILL of the wrapper's process group after `STOP_GRACE`, which
//! the wrapper itself cannot survive — so the observed-harness-state terminal record must land
//! *before* that escalation. No in-process test can cover this (the escalation kills the test's
//! own group), so the wrapper runs here as a real child in its own process group.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn stop_escalation_writes_the_terminal_record_before_sigkill() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let declaration = catalog.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        &declaration,
        r#"agent "worker" { host "h"; command "true" }"#,
    )
    .unwrap();
    let agent_dir = declaration.parent().unwrap().to_path_buf();
    let bin = tmp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    executable(&bin.join("hostname"), "#!/bin/sh\necho h\n");
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    // The provider ignores SIGTERM, so the wrapper's grace window expires and it must escalate.
    let mut wrapper = Command::new(env!("CARGO_BIN_EXE_st2"));
    wrapper
        .args(["--catalog"])
        .arg(&catalog)
        .args([
            "driver",
            "claude-session",
            "--identity",
            "worker",
            "--runtime-id",
            "worker",
            "--",
            "sh",
            "-c",
            // The ready marker synchronizes the signal: SIGTERM may only fly once the trap is
            // provably installed, or a slow scheduler lets the child die in the grace window and
            // the escalation path goes untested.
            "trap '' TERM; : > \"$READY_MARKER\"; sleep 60",
        ])
        .env("READY_MARKER", tmp.path().join("provider-ready"))
        .env("PATH", &path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        // Its own process group: the wrapper SIGKILLs -pgid on escalation, and that must never
        // reach this test process.
        wrapper.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    let mut wrapper = wrapper.spawn().unwrap();

    // The provider proves its TERM trap is installed before the wrapper is signaled.
    let ready = tmp.path().join("provider-ready");
    let started = Instant::now();
    while !ready.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "provider never installed its trap"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    unsafe {
        libc::kill(wrapper.id() as i32, libc::SIGTERM);
    }
    // STOP_GRACE is 5 s; the wrapper dies with its own SIGKILL shortly after.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if wrapper.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "wrapper survived its own escalation"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let raw = fs::read_to_string(agent_dir.join("harness-state"))
        .expect("terminal record must land before the SIGKILL escalation");
    let record: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(record["state"], "ended", "record: {raw}");
    assert_eq!(record["exit"], "signal 9", "record: {raw}");
}
