//! Real-binary proof that changing the effective PTY registry cannot duplicate a surviving task.
//!
//! Every path is under one temporary directory and every session is removed by `Drop`; this test
//! never reads or writes the ambient fleet registry.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const HOST: &str = "roottransition";
const EXACT_IDS: [&str; 2] = ["roottransition.seat", "roottransition.sibling"];

struct Fixture {
    catalog: PathBuf,
    xdg: PathBuf,
    root_a: PathBuf,
    root_b: PathBuf,
    _tmp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let xdg = tmp.path().join("xdg");
        let root_a = tmp.path().join("root-a");
        let root_b = tmp.path().join("root-b");
        for path in [&catalog, &xdg, &root_a, &root_b] {
            fs::create_dir_all(path).unwrap();
        }
        for identity in ["seat", "sibling"] {
            let dir = catalog.join("agents").join(HOST).join(identity);
            fs::create_dir_all(&dir).unwrap();
            fs::write(
                dir.join("agent.kdl"),
                format!("agent \"{identity}\" {{ host \"{HOST}\"; command \"exec sleep 120\" }}\n"),
            )
            .unwrap();
        }
        Self {
            catalog,
            xdg,
            root_a,
            root_b,
            _tmp: tmp,
        }
    }

    fn up(&self, root: &Path) -> Output {
        Command::new(env!("CARGO_BIN_EXE_st2"))
            .args([
                "up",
                "--catalog",
                self.catalog.to_str().unwrap(),
                "--host",
                HOST,
                "--once",
            ])
            .env("XDG_STATE_HOME", &self.xdg)
            .env("PTY_ROOT", root)
            .output()
            .unwrap()
    }

    fn up_spec(&self, root: &Path, spec_dir: &Path) -> Output {
        Command::new(env!("CARGO_BIN_EXE_st2"))
            .arg("up")
            .arg(spec_dir)
            .arg("--once")
            .env("XDG_STATE_HOME", &self.xdg)
            .env("PTY_ROOT", root)
            .output()
            .unwrap()
    }

    fn sessions(&self, root: &Path) -> Vec<serde_json::Value> {
        let out = Command::new("pty")
            .args(["list", "--json"])
            .env("PTY_ROOT", root)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "pty list failed for {}: {}",
            root.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn running_pid(&self, root: &Path, id: &str) -> Option<i64> {
        self.sessions(root)
            .into_iter()
            .find(|session| session["name"] == id && session["status"] == "running")
            .and_then(|session| session["pid"].as_i64())
    }

    fn remove(&self, root: &Path, id: &str) {
        let _ = Command::new("pty")
            .args(["kill", id])
            .env("PTY_ROOT", root)
            .output();
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.running_pid(root, id).is_some() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = Command::new("pty")
            .args(["rm", id])
            .env("PTY_ROOT", root)
            .output();
    }

    fn spawn_prefix_sibling(&self, root: &Path) {
        let out = Command::new("pty")
            .args([
                "run",
                "-d",
                "--id",
                "roottransition.sibling.child",
                "--",
                "sleep",
                "120",
            ])
            .env("PTY_ROOT", root)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "prefix sibling did not start: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for root in [&self.root_a, &self.root_b] {
            for id in EXACT_IDS
                .into_iter()
                .chain(["roottransition.sibling.child"])
            {
                self.remove(root, id);
            }
        }
    }
}

fn prove_transition(source: fn(&Fixture) -> &Path, destination: fn(&Fixture) -> &Path) {
    let fx = Fixture::new();
    let source = source(&fx);
    let destination = destination(&fx);

    let launched = fx.up(source);
    assert!(
        launched.status.success(),
        "initial reconcile failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&launched.stdout),
        String::from_utf8_lossy(&launched.stderr)
    );
    let source_pids = EXACT_IDS.map(|id| {
        fx.running_pid(source, id)
            .unwrap_or_else(|| panic!("{id} was not launched in {}", source.display()))
    });

    let adopted = fx.up(source);
    assert!(
        adopted.status.success(),
        "same-root adoption failed: {}",
        String::from_utf8_lossy(&adopted.stderr)
    );
    assert_eq!(
        EXACT_IDS.map(|id| fx.running_pid(source, id).unwrap()),
        source_pids,
        "same-root reconcile replaced a survivor"
    );
    assert!(
        String::from_utf8_lossy(&adopted.stdout).contains("adopted (2): seat, sibling"),
        "same-root control did not report adoption:\n{}",
        String::from_utf8_lossy(&adopted.stdout)
    );

    let refused = fx.up(destination);
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success(),
        "root transition unexpectedly reconciled"
    );
    for id in EXACT_IDS {
        assert!(stderr.contains(id), "diagnostic omitted {id}:\n{stderr}");
        assert_eq!(
            fx.running_pid(destination, id),
            None,
            "{id} was duplicated into {}",
            destination.display()
        );
    }
    assert!(stderr.contains("previous:"), "{stderr}");
    assert!(stderr.contains("requested:"), "{stderr}");
    assert!(
        stderr.contains("has not killed, adopted, or launched"),
        "{stderr}"
    );
    assert_eq!(
        EXACT_IDS.map(|id| fx.running_pid(source, id).unwrap()),
        source_pids,
        "refusal changed a source survivor"
    );

    for id in EXACT_IDS {
        fx.remove(source, id);
    }
    fx.spawn_prefix_sibling(source);

    let advanced = fx.up(destination);
    assert!(
        advanced.status.success(),
        "clean transition with a prefix sibling did not advance:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&advanced.stdout),
        String::from_utf8_lossy(&advanced.stderr)
    );
    for id in EXACT_IDS {
        assert!(
            fx.running_pid(destination, id).is_some(),
            "{id} was not launched after the exact source survivors were removed"
        );
    }
    assert!(
        fx.running_pid(source, "roottransition.sibling.child")
            .is_some(),
        "the unrelated prefix sibling was mutated"
    );
}

#[test]
fn root_a_to_b_refuses_survivors_then_advances_after_exact_cleanup() {
    prove_transition(|fx| &fx.root_a, |fx| &fx.root_b);
}

#[test]
fn root_b_to_a_refuses_survivors_then_advances_after_exact_cleanup() {
    prove_transition(|fx| &fx.root_b, |fx| &fx.root_a);
}

#[test]
fn single_file_fleet_refuses_to_duplicate_a_survivor_across_roots() {
    let fx = Fixture::new();
    let spec_dir = fx.catalog.join("single-file");
    fs::create_dir_all(&spec_dir).unwrap();
    fs::write(
        spec_dir.join("team.kdl"),
        r#"team "single" {
  agent "seat" { command "exec sleep 120" }
}
"#,
    )
    .unwrap();
    let id = "single.seat";

    let launched = fx.up_spec(&fx.root_a, &spec_dir);
    assert!(
        launched.status.success(),
        "initial single-file reconcile failed: {}",
        String::from_utf8_lossy(&launched.stderr)
    );
    let source_pid = fx.running_pid(&fx.root_a, id).unwrap();

    let refused = fx.up_spec(&fx.root_b, &spec_dir);
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success(), "{stderr}");
    assert!(stderr.contains(id), "{stderr}");
    assert_eq!(fx.running_pid(&fx.root_a, id), Some(source_pid));
    assert_eq!(fx.running_pid(&fx.root_b, id), None);

    fx.remove(&fx.root_a, id);
}
