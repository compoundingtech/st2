//! P3+P4 end-to-end (the benign internal-proof milestone): `st2 eval <folder>` runs a whole eval
//! folder → VERDICT through the real binary. Benign shell "agents" choreograph the real flow on the
//! FLAT eval bus via `st2 message` (exercising the flat-bus bridge): kick → worker→sup report →
//! sup→requester confirmation (post-dating the report), then the judge engine grades the deliverable.
//! No real harness; st2 eval mints its own hermetic catalog + PTY_ROOT (never touches the live fleet).
//!
//! Needs `pty` on PATH — HARD failure if absent unless ST2_ALLOW_PTY_SKIP is set.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn pty_available() -> bool {
    Command::new("pty").arg("--help").output().map(|o| o.status.success()).unwrap_or(false)
}

struct RemoveDirOnDrop(std::path::PathBuf);

impl Drop for RemoveDirOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn st2_eval_runs_a_benign_folder_to_a_pass_verdict() {
    if !pty_available() {
        assert!(
            std::env::var_os("ST2_ALLOW_PTY_SKIP").is_some(),
            "`pty` not on PATH — can't run an eval. Set ST2_ALLOW_PTY_SKIP=1 to skip."
        );
        eprintln!("SKIP st2_eval_runs_a_benign_folder_to_a_pass_verdict: `pty` not on PATH");
        return;
    }

    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cell = tmp.path().join("cell");
    let fixture = cell.join("fixture");
    std::fs::create_dir_all(fixture.join("scripts")).unwrap();
    std::fs::create_dir_all(fixture.join("sup")).unwrap();
    std::fs::create_dir_all(fixture.join("worker")).unwrap();

    // Native-default topology: no authored ST_ROOT. The kickoff, polling agents, built-in bare dings,
    // requester confirmation, and judges must all resolve the same flat catalog bus.
    std::fs::write(
        cell.join("cell.kdl"),
        r#"
env { PTY_ROOT "$CATALOG/pty" }
team "t" {
  agent "sup" {
    workspace "./sup"
    env { ST_AGENT "t.sup" }
    command "sh $CATALOG/scripts/sup.sh"
    ding
  }
  agent "worker" {
    workspace "./worker"
    env { ST_AGENT "t.worker" }
    command "sh $CATALOG/scripts/worker.sh"
    ding
  }
}
eval {
  copy "./fixture"
  message { from "requester"; to "t.sup"; content "relicense the widget" }
  max-timeout "30s"
  judges {
    judge "deliverable exists" { exec "test -f $CATALOG/worker/DONE" }
    judge "deliverable content" { file "worker/DONE" has "resolved" }
    judge "one native bus root" {
      exec "test \"$ST_ROOT\" = \"$CATALOG\" && test \"$(cat $CATALOG/sup/ST_ROOT)\" = \"$CATALOG\" && test \"$(cat $CATALOG/worker/ST_ROOT)\" = \"$CATALOG\""
    }
    judge "no legacy split bus" {
      exec "test ! -e $CATALOG/obsolete-bus/t.sup/inbox && test ! -e $CATALOG/obsolete-bus/t.worker/inbox && test ! -e $CATALOG/obsolete-bus/requester/inbox"
    }
  }
}
"#,
    )
    .unwrap();

    // worker: on boot, do the work (write the deliverable) + report to the sup.
    std::fs::write(
        fixture.join("scripts/worker.sh"),
        r#"#!/bin/sh
sleep 0.4
mkdir -p "$CATALOG/worker"
printf '%s\n' "$ST_ROOT" > "$CATALOG/worker/ST_ROOT"
echo "resolved by t.worker" > "$CATALOG/worker/DONE"
st2 message send t.sup --root "$ST_ROOT" --as t.worker -m "worker done" >/dev/null 2>&1
sleep 60
"#,
    )
    .unwrap();
    // sup: wait for the kick, then the worker's report, then confirm to the requester (post-dating it).
    std::fs::write(
        fixture.join("scripts/sup.sh"),
        r#"#!/bin/sh
printf '%s\n' "$ST_ROOT" > "$CATALOG/sup/ST_ROOT"
for _ in $(seq 1 150); do
  n=$(st2 message ls t.sup --root "$ST_ROOT" --from requester --count 2>/dev/null || echo 0)
  [ "$n" -gt 0 ] && break
  sleep 0.2
done
for _ in $(seq 1 150); do
  n=$(st2 message ls t.sup --root "$ST_ROOT" --from t.worker --count 2>/dev/null || echo 0)
  [ "$n" -gt 0 ] && break
  sleep 0.2
done
st2 message send requester --root "$ST_ROOT" --as t.sup -m "done + verified: worker relicensed, commit clean" >/dev/null 2>&1
sleep 60
"#,
    )
    .unwrap();

    let path = format!("{}:{}", bin_dir.display(), std::env::var("PATH").unwrap_or_default());
    let out = Command::new(bin)
        .args(["eval"])
        .arg(&cell)
        .env("PATH", &path)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains("team signalled done"),
        "the flow didn't reach done (kick → worker report → sup confirm):\n--stdout--\n{stdout}\n--stderr--\n{stderr}"
    );
    assert!(stdout.contains("VERDICT: PASS"), "expected PASS:\n--stdout--\n{stdout}\n--stderr--\n{stderr}");
    assert!(out.status.success(), "exit non-zero:\n{stdout}\n{stderr}");
    // Both judges (bash + declarative) passed.
    assert!(stdout.contains("SCORE: 4 PASS / 0 FAIL"), "expected 4/0:\n{stdout}");

    let json_out = Command::new(bin)
        .args(["eval", "--json"])
        .arg(&cell)
        .env("PATH", &path)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg-json"))
        .output()
        .unwrap();
    assert!(json_out.status.success(), "json eval should preserve exit 0: {}", String::from_utf8_lossy(&json_out.stderr));
    let json: serde_json::Value = serde_json::from_slice(&json_out.stdout).expect("--json emits EvalReport");
    assert_eq!(json["done"], true);
    assert!(json["judges"].as_array().is_some());

    let fail_cell = cell.join("fail.kdl");
    let fail_spec = std::fs::read_to_string(cell.join("cell.kdl")).unwrap()
        .replace("test -f $CATALOG/worker/DONE", "test -f $CATALOG/worker/NOPE");
    std::fs::write(&fail_cell, fail_spec).unwrap();
    let fail_out = Command::new(bin)
        .args(["eval", "--json"])
        .arg(&fail_cell)
        .env("PATH", &path)
        .env_remove("CATALOG").env_remove("ST_ROOT").env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg-fail"))
        .output().unwrap();
    assert!(!fail_out.status.success());
    let fail_json: serde_json::Value = serde_json::from_slice(&fail_out.stdout).expect("failed eval report");
    assert_eq!(fail_json["judges"][0]["passed"], false);
    assert!(!String::from_utf8_lossy(&fail_out.stdout).contains("=="));

    let invalid = Command::new(bin)
        .args(["eval", "--json"])
        .arg(tmp.path().join("missing.kdl"))
        .output()
        .unwrap();
    assert!(!invalid.status.success(), "invalid eval input must retain nonzero exit");
}

/// Under `supervise`, teardown reaps RUNTIME-spawned seats too (the team-standup pattern: a seat spins
/// up an undeclared peer mid-run), not just the declared team. The seat spawns an undeclared `rtpeer`
/// into the eval's hermetic PTY_ROOT; after the eval, no orphan carrying the peer's marker survives.
#[test]
fn supervise_teardown_reaps_a_runtime_spawned_seat() {
    if !pty_available() {
        assert!(std::env::var_os("ST2_ALLOW_PTY_SKIP").is_some(), "`pty` not on PATH; set ST2_ALLOW_PTY_SKIP=1");
        eprintln!("SKIP supervise_teardown_reaps_a_runtime_spawned_seat: `pty` not on PATH");
        return;
    }
    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cell = tmp.path().join("cell");
    std::fs::create_dir_all(&cell).unwrap();
    // The seat spawns an UNDECLARED peer into this eval's hermetic PTY_ROOT and records that peer's
    // daemon pid inside the preserved eval catalog. No done signal → the eval times out → supervised
    // teardown must reap both the declared seat and the runtime peer. The proof below uses only this
    // invocation's root + exact pid; concurrent evals cannot be observed or killed.
    std::fs::write(
        cell.join("cell.kdl"),
        r#"
env { ST_ROOT "$CATALOG/custom-bus"; PTY_ROOT "$CATALOG/pty" }
agent "sup" {
  env { ST_AGENT "sup" }
  command "sh -c 'pty run -d --id rtpeer -- sleep 100000; for _ in $(seq 1 100); do test -s $PTY_ROOT/rtpeer.pid && break; sleep 0.05; done; cat $PTY_ROOT/rtpeer.pid > $CATALOG/runtime-peer.pid; exec sleep 100000'"
}
eval {
  message { from "runner"; to "sup"; content "go" }
  max-timeout "6s"
  supervise
  judges { judge "trivial" { exec "exit 0" } }
}
"#,
    )
    .unwrap();
    let path = format!("{}:{}", bin_dir.display(), std::env::var("PATH").unwrap_or_default());
    let child = Command::new(bin)
        .args(["eval", "--keep"])
        .arg(&cell)
        .env("PATH", &path)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let catalog = std::env::temp_dir().join(format!("st2e-{}", child.id()));
    let _catalog_cleanup = RemoveDirOnDrop(catalog.clone());
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains(&format!(
            "catalog preserved (--keep): {}",
            catalog.display()
        )),
        "eval did not preserve its expected per-process catalog:\n--stdout--\n{stdout}\n--stderr--\n{stderr}"
    );

    let peer_pid: i32 = std::fs::read_to_string(catalog.join("runtime-peer.pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let peer_alive = unsafe { libc::kill(peer_pid, 0) == 0 };
    if peer_alive {
        // Failure cleanup stays inside this invocation's PTY root; never scan or kill by a global
        // command marker. Preserve the pre-cleanup observation for the assertion below.
        let _ = Command::new("pty")
            .args(["--root"])
            .arg(catalog.join("pty"))
            .args(["kill", "rtpeer"])
            .status();
    }
    let sessions = Command::new("pty")
        .args(["--root"])
        .arg(catalog.join("pty"))
        .args(["list", "--json"])
        .output()
        .unwrap();
    assert!(
        sessions.status.success(),
        "could not inspect the eval-scoped PTY registry:\n{}",
        String::from_utf8_lossy(&sessions.stderr)
    );
    let session_json: serde_json::Value = serde_json::from_slice(&sessions.stdout).unwrap();
    assert!(
        !peer_alive
            && !catalog.join("pty/rtpeer.pid").exists()
            && session_json
                .as_array()
                .is_some_and(|sessions| sessions.iter().all(|session| session["name"] != "rtpeer")),
        "runtime-spawned seat leaked after supervise teardown (pid {peer_pid}, registry {session_json}):\n\
         --stdout--\n{stdout}\n--stderr--\n{stderr}"
    );
}

/// A TEAM-LESS eval (no agents, no kickoff) runs its `run` steps to completion, captures each step's
/// stdout/stderr/exit for the judges, and the judges own the verdict. By DEFAULT a step must exit 0 (so
/// `make` gates the verdict); `allow-nonzero` opts a step out (so `probe`'s deliberate exit 3 is fine
/// and a judge asserts it). Flat run-collapse form. No `pty` needed (no seats are booted).
#[test]
fn team_less_run_stage_captures_and_judges() {
    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cell = tmp.path().join("cell");
    std::fs::create_dir_all(&cell).unwrap();
    // `make` succeeds (the default must-exit-0 gate) and writes a file; `probe` deliberately exits 3 with
    // `allow-nonzero`, so that is NOT a failure — the judges assert the file, probe's captured exit (via
    // $RUNS_DIR and $RUN_<id>_EXIT), and probe's captured stdout. Written in the flat run-collapse form.
    std::fs::write(
        cell.join("cell.kdl"),
        r#"
env { ST_ROOT "$CATALOG/custom-bus" }
eval {
  max-timeout "20s"
  run "make"  { command "echo hi > $CATALOG/out.txt" }
  run "probe" { command "echo scanning; exit 3"; allow-nonzero }
  judges {
    judge "file-made"    { exec "test -f $CATALOG/out.txt" }
    judge "probe-exit3"  { exec "test \"$(cat $RUNS_DIR/probe.exit)\" = 3" }
    judge "probe-stdout" { exec "grep -q scanning $RUNS_DIR/probe.out" }
    judge "exit-var"     { exec "test \"$RUN_probe_EXIT\" = 3" }
    judge "probe-log"    { exec "grep -q scanning $LOGS_DIR/probe.log" }
  }
}
"#,
    )
    .unwrap();
    let path = format!("{}:{}", bin_dir.display(), std::env::var("PATH").unwrap_or_default());
    let out = Command::new(bin)
        .args(["eval"])
        .arg(&cell)
        .env("PATH", path)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.contains("team-less eval: 2 run step(s)"), "not the team-less path:\n{stdout}\n{stderr}");
    assert!(stdout.contains("run step probe → exit 3"), "probe's non-zero exit not captured:\n{stdout}");
    assert!(
        stdout.contains("SCORE: 6 PASS / 0 FAIL"),
        "team-less run-stage eval should be 6/0 (make must-exit-0 gate + 5 judges incl. $LOGS_DIR):\n--stdout--\n{stdout}\n--stderr--\n{stderr}"
    );
    assert!(stdout.contains("VERDICT: PASS") && out.status.success(), "expected PASS:\n{stdout}\n{stderr}");
}

/// crash-ding: under `supervise`, a seat that CRASHES (non-zero/killed/vanished) is respawned AND its
/// crash escalates up the supervisor chain — a "worker crash: <id>" bus ding to the direct supervisor
/// AND the root cos (walking the `supervisor` field transitively). A seat that exits CLEANLY (0) stays
/// SILENT (a false ding on a routine finish fails as hard as a missed crash). Keys on the pty session
/// dying → harness-agnostic. Needs `pty` (seats boot).
#[test]
fn supervise_crash_dings_up_the_chain_and_is_silent_on_clean_exit() {
    if !pty_available() {
        assert!(std::env::var_os("ST2_ALLOW_PTY_SKIP").is_some(), "`pty` not on PATH; set ST2_ALLOW_PTY_SKIP=1");
        eprintln!("SKIP supervise_crash_dings_up_the_chain_and_is_silent_on_clean_exit: `pty` not on PATH");
        return;
    }
    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cell = tmp.path().join("cell");
    let fixture = cell.join("fixture");
    std::fs::create_dir_all(fixture.join("scripts")).unwrap();
    // Chain: cd.worker → cd.sup → cd.cos (root). worker CRASHES (exit 1) → ding up to sup AND cos;
    // `clean` exits 0 → stays silent. A clean sentinel exits only after kickoff; its supervisor-driven
    // respawn proves that at least one supervise tick observed worker + clean alive. Only then does the
    // sup release those two seats, so neither can race the boot gate or the `ever_alive` classification
    // under load. Their second launches report that both exits were observed and respawned.
    std::fs::write(
        cell.join("cell.kdl"),
        r#"
env { ST_ROOT "$CATALOG/custom-bus"; PTY_ROOT "$CATALOG/pty" }
team "cd" {
  agent "gate"   { supervisor "cd.cos"; env { ST_AGENT "cd.gate" }; command "sh $CATALOG/scripts/gate.sh" }
  agent "worker" { supervisor "cd.sup"; env { ST_AGENT "cd.worker" }; command "sh $CATALOG/scripts/worker.sh" }
  agent "clean"  { supervisor "cd.cos"; env { ST_AGENT "cd.clean" }; command "sh $CATALOG/scripts/clean.sh" }
  agent "sup"    { supervisor "cd.cos"; env { ST_AGENT "cd.sup" }; command "sh $CATALOG/scripts/sup.sh" }
  agent "cos"    { env { ST_AGENT "cd.cos" }; command "sleep 100000" }
}
eval {
  copy "./fixture"
  message { from "runner"; to "cd.sup"; content "go" }
  max-timeout "20s"
  supervise
  judges { judge "ok" { exec "true" } }
}
"#,
    )
    .unwrap();
    std::fs::write(
        fixture.join("scripts/gate.sh"),
        r#"#!/bin/sh
while [ ! -e "$CATALOG/release-gate-after-kickoff" ]; do sleep 0.05; done
if mkdir "$CATALOG/gate-exited-once" 2>/dev/null; then
  exit 0
fi
st2 message send cd.sup --root "$ST_ROOT" --as cd.gate -m "supervise tick completed" >/dev/null 2>&1
sleep 100000
"#,
    )
    .unwrap();
    std::fs::write(
        fixture.join("scripts/worker.sh"),
        r#"#!/bin/sh
while [ ! -e "$CATALOG/release-after-supervise-tick" ]; do sleep 0.05; done
if mkdir "$CATALOG/worker-exited-once" 2>/dev/null; then
  exit 1
fi
st2 message send cd.sup --root "$ST_ROOT" --as cd.worker -m "worker respawned after crash" >/dev/null 2>&1
sleep 100000
"#,
    )
    .unwrap();
    std::fs::write(
        fixture.join("scripts/clean.sh"),
        r#"#!/bin/sh
while [ ! -e "$CATALOG/release-after-supervise-tick" ]; do sleep 0.05; done
if mkdir "$CATALOG/clean-exited-once" 2>/dev/null; then
  exit 0
fi
st2 message send cd.sup --root "$ST_ROOT" --as cd.clean -m "clean seat respawned" >/dev/null 2>&1
sleep 100000
"#,
    )
    .unwrap();
    std::fs::write(
        fixture.join("scripts/sup.sh"),
        r#"#!/bin/sh
wait_from() {
  from=$1
  for _ in $(seq 1 160); do
    count=$(st2 message ls cd.sup --root "$ST_ROOT" --from "$from" --count 2>/dev/null || echo 0)
    [ "$count" -gt 0 ] 2>/dev/null && return 0
    sleep 0.05
  done
  return 1
}

wait_from runner || exit 2
: > "$CATALOG/release-gate-after-kickoff"
wait_from cd.gate || exit 3
: > "$CATALOG/release-after-supervise-tick"
wait_from st2 || exit 4
wait_from cd.worker || exit 5
wait_from cd.clean || exit 6
st2 message send runner --root "$ST_ROOT" --as cd.sup -m "both supervised exits classified" >/dev/null 2>&1
sleep 100000
"#,
    )
    .unwrap();
    let path = format!("{}:{}", bin_dir.display(), std::env::var("PATH").unwrap_or_default());
    let out = Command::new(bin)
        .args(["eval"])
        .arg(&cell)
        .env("PATH", path)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains("team signalled done"),
        "post-boot exit/respawn handshake didn't complete:\n--stdout--\n{stdout}\n--stderr--\n{stderr}"
    );
    // (b) the crash escalates to BOTH the direct supervisor and the root cos.
    assert!(
        stdout.contains("crash-ding: cd.worker → cd.sup"),
        "crash didn't ding the supervisor:\n--stdout--\n{stdout}\n--stderr--\n{stderr}"
    );
    assert!(
        stdout.contains("crash-ding: cd.worker → cd.cos"),
        "crash didn't reach the root cos:\n--stdout--\n{stdout}\n--stderr--\n{stderr}"
    );
    // (c) the clean-exit seat produces NO ding.
    assert!(
        !stdout.contains("crash-ding: cd.clean"),
        "a clean exit (0) must NOT crash-ding:\n{stdout}"
    );
    assert!(
        !stdout.contains("crash-ding: cd.gate"),
        "the clean gate exit (0) must NOT crash-ding:\n{stdout}"
    );
}

/// A seat whose command exits at boot (127 command-not-found, crash) must fail the eval FAST + loudly,
/// NOT hang until max-timeout waiting for a confirmation that can never come. (CoS robustness finding.)
#[test]
fn st2_eval_fails_fast_when_a_seat_exits_at_boot() {
    if !pty_available() {
        assert!(std::env::var_os("ST2_ALLOW_PTY_SKIP").is_some(), "`pty` not on PATH; set ST2_ALLOW_PTY_SKIP=1");
        eprintln!("SKIP st2_eval_fails_fast_when_a_seat_exits_at_boot: `pty` not on PATH");
        return;
    }
    let bin = env!("CARGO_BIN_EXE_st2");
    let bin_dir = Path::new(bin).parent().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let cell = tmp.path().join("cell");
    std::fs::create_dir_all(cell.join("fixture")).unwrap();
    // A seat command that doesn't exist → the pty session exits ~immediately. max-timeout is huge, so a
    // hang would take 10 min; fail-fast must make this return in seconds.
    std::fs::write(
        cell.join("cell.kdl"),
        r#"
env { ST_ROOT "$CATALOG/custom-bus"; PTY_ROOT "$CATALOG/pty" }
agent "bad" { env { ST_AGENT "bad" }; command "definitely-not-a-real-binary-xyz123" }
eval {
  copy "./fixture"
  message { from "requester"; to "bad"; content "go" }
  max-timeout "600s"
  judges { judge "t" { exec "true" } }
}
"#,
    )
    .unwrap();
    let path = format!("{}:{}", bin_dir.display(), std::env::var("PATH").unwrap_or_default());
    let start = Instant::now();
    let out = Command::new(bin)
        .args(["eval"])
        .arg(&cell)
        .env("PATH", path)
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .env("XDG_STATE_HOME", tmp.path().join("xdg"))
        .output()
        .unwrap();
    let elapsed = start.elapsed();
    let combined = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(!out.status.success(), "a dead-at-boot seat must fail the eval:\n{combined}");
    assert!(combined.contains("exited at boot"), "expected a clear fail-fast error:\n{combined}");
    assert!(elapsed < Duration::from_secs(60), "must fail FAST, not wait max-timeout — took {elapsed:?}");
}
