//! E2E proof that the real `st2` binary exports OTLP/HTTP JSON spans into an otelite receiver
//! when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (and stays silent without one). The receiver is the
//! `otelite` binary from effect-utils (`packages.x86_64-linux.otelite`); the flake check wires it
//! in via `ST2_OTELITE_BIN`.
//!
//! Needs `ST2_OTELITE_BIN` on a gate run — HARD failure if absent unless `ST2_ALLOW_OTEL_SKIP`
//! is set (a gate must not silently skip).

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Spawn `otelite capture`, wait for its endpoints banner.
/// Returns (child, http endpoint). The child's stdout pipe stays open until otelite exits:
/// a detached reader thread owns it until EOF, so otelite's shutdown writes never hit a
/// broken pipe even though this function returns before the child terminates.
fn spawn_capture(otelite: &Path, out_dir: &Path) -> (std::process::Child, String) {
    let mut child = Command::new(otelite)
        .args(["capture", "--out"])
        .arg(out_dir)
        .arg("--http-port")
        .arg("0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn otelite capture");

    // The endpoints banner is one JSON line: {"grpc":..., "http":..., "out":..., "schema":...}.
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        // A plain blocking `stdout.read()` ignores any deadline — if otelite stalls mid-read
        // the test would hang until the workflow timeout. Ship bytes over a channel instead
        // so the main thread can enforce the timeout with recv_timeout and fail fast.
        let mut stdout = stdout;
        let mut chunk = [0u8; 512];
        loop {
            match stdout.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.send(chunk[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    const BANNER_TIMEOUT: Duration = Duration::from_secs(10);
    let started = Instant::now();
    let mut banner = String::new();
    loop {
        let Some(remaining) = BANNER_TIMEOUT.checked_sub(started.elapsed()) else {
            panic!("otelite capture did not print its endpoints banner within 10s: {banner}");
        };
        match rx.recv_timeout(remaining) {
            Ok(bytes) => banner.push_str(&String::from_utf8_lossy(&bytes)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("otelite capture did not print its endpoints banner within 10s: {banner}");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("otelite capture exited before serving: {banner}");
            }
        }
        if let Some(line) = banner.lines().find(|l| l.contains("otelite.endpoints")) {
            let v: serde_json::Value = serde_json::from_str(line).expect("endpoints banner JSON");
            let http = v["http"].as_str().expect("http endpoint").to_string();
            return (child, http);
        }
    }
}

#[test]
fn st2_exports_spans_to_otelite_when_endpoint_is_set() {
    let Some(otelite) = std::env::var_os("ST2_OTELITE_BIN").map(PathBuf::from) else {
        assert!(
            std::env::var_os("ST2_ALLOW_OTEL_SKIP").is_some(),
            "`ST2_OTELITE_BIN` not set — can't prove OTLP export. Set ST2_ALLOW_OTEL_SKIP=1 to skip."
        );
        eprintln!("SKIP st2_exports_spans_to_otelite: ST2_OTELITE_BIN not set");
        return;
    };

    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cap_dir = tmp.path().join("cap");
    let empty_catalog = tmp.path().join("catalog");
    std::fs::create_dir_all(&empty_catalog).unwrap();

    let (mut capture, endpoint) = spawn_capture(&otelite, &cap_dir);

    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new(bin)
        .args(["up", "--catalog", empty_catalog.to_str().unwrap(), "--once"])
        .env("PATH", path)
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint)
        .output()
        .expect("run st2 up --once");
    assert!(
        out.status.success(),
        "st2 up --once failed.\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // stdin EOF stops the receiver and flushes captured signals to disk. The detached reader
    // thread keeps otelite's stdout pipe open until process exit, so its shutdown writes
    // never hit a broken pipe.
    let _ = capture.stdin.take();
    let _ = capture.wait();

    let traces =
        std::fs::read_to_string(cap_dir.join("traces.ndjson")).expect("traces.ndjson written");
    assert!(
        traces.contains("\"st2.reconcile_pass\""),
        "reconcile pass span missing from capture:\n{traces}"
    );
    assert!(
        traces.contains("st2-cli"),
        "service.name st2-cli missing from capture:\n{traces}"
    );
}

#[test]
fn st2_without_endpoint_does_not_error() {
    let Some(otelite) = std::env::var_os("ST2_OTELITE_BIN").map(PathBuf::from) else {
        assert!(
            std::env::var_os("ST2_ALLOW_OTEL_SKIP").is_some(),
            "`ST2_OTELITE_BIN` not set — can't prove OTLP silence. Set ST2_ALLOW_OTEL_SKIP=1 to skip."
        );
        eprintln!("SKIP st2_without_endpoint_does_not_error: ST2_OTELITE_BIN not set");
        return;
    };

    // No-op guarantee: an unset OTEL_EXPORTER_OTLP_ENDPOINT must keep every command working
    // AND emit no telemetry at all. Run a real `st2 up --once` against a scratch EMPTY
    // catalog with a live otelite receiver attached; if an always-on-export regression ever
    // lands, the receiver flushes non-empty ndjson files here and the test fails — a bare
    // `--version` smoke could never catch that.
    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cap_dir = tmp.path().join("cap");
    let empty_catalog = tmp.path().join("catalog");
    std::fs::create_dir_all(&empty_catalog).unwrap();

    let (mut capture, _endpoint) = spawn_capture(&otelite, &cap_dir);

    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new(bin)
        .args(["up", "--catalog", empty_catalog.to_str().unwrap(), "--once"])
        .env("PATH", path)
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .output()
        .expect("run st2 up --once without endpoint");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "st2 up --once must succeed without an OTLP endpoint\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // stdin EOF stops the receiver and flushes captured signals to disk. The detached reader
    // thread keeps otelite's stdout pipe open until process exit (see spawn_capture).
    let _ = capture.stdin.take();
    let _ = capture.wait();

    for name in ["traces.ndjson", "metrics.ndjson", "logs.ndjson"] {
        let path = cap_dir.join(name);
        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        assert_eq!(
            bytes, 0,
            "{name} must be absent or empty when OTEL_EXPORTER_OTLP_ENDPOINT is unset, got {bytes} bytes"
        );
    }
}
