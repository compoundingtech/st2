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
/// Returns (child, its stdout — keep until after `wait()` so otelite's final
/// writes don't hit a broken pipe, http endpoint).
fn spawn_capture(
    otelite: &Path,
    out_dir: &Path,
) -> (std::process::Child, std::process::ChildStdout, String) {
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
    let mut stdout = child.stdout.take().unwrap();
    let mut banner = String::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Instant::now() < deadline,
            "otelite capture did not print its endpoints banner"
        );
        let mut chunk = [0u8; 512];
        let n = stdout.read(&mut chunk).expect("read otelite stdout");
        if n > 0 {
            banner.push_str(&String::from_utf8_lossy(&chunk[..n]));
        }
        if let Some(line) = banner.lines().find(|l| l.contains("otelite.endpoints")) {
            let v: serde_json::Value = serde_json::from_str(line).expect("endpoints banner JSON");
            let http = v["http"].as_str().expect("http endpoint").to_string();
            return (child, stdout, http);
        }
        if n == 0 {
            panic!("otelite capture exited before serving: {banner}");
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

    let (mut capture, capture_stdout, endpoint) = spawn_capture(&otelite, &cap_dir);

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

    // stdin EOF (or SIGTERM) stops the receiver and flushes captured signals to disk.
    let _ = capture.stdin.take();
    let _ = capture.wait();
    // Only now close otelite's stdout: its shutdown writes must not hit a broken pipe.
    drop(capture_stdout);

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
    // No-op guarantee: an unset OTEL_EXPORTER_OTLP_ENDPOINT must keep every command working
    // (here: a trivially valid CLI invocation) with no telemetry side effects.
    let bin = env!("CARGO_BIN_EXE_st2");
    let out = Command::new(bin)
        .arg("--version")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .output()
        .unwrap();
    assert!(out.status.success());
}
