use std::fs;
use std::path::Path;
use std::process::Command;

use st2::{discover, driver::expand_driver};

fn assert_snapshot(input: &str, expected: &str) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("agents/host/worker/agent.kdl");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, input).unwrap();

    let found = discover(temp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    assert_eq!(found.specs.len(), 1);
    let actual = expand_driver(&found.specs[0], "unused").unwrap().to_string();
    assert_eq!(actual, expected);
    expected.parse::<kdl::KdlDocument>().unwrap();
}

#[test]
fn claude_kdl_expansion_matches_snapshot() {
    assert_snapshot(
        include_str!("fixtures/driver/claude.in.kdl"),
        include_str!("fixtures/driver/claude.out.kdl"),
    );
}

#[test]
fn codex_kdl_expansion_matches_snapshot() {
    assert_snapshot(
        include_str!("fixtures/driver/codex.in.kdl"),
        include_str!("fixtures/driver/codex.out.kdl"),
    );
}

#[test]
fn cli_prints_each_snapshot_without_changing_its_input() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/driver");
    for provider in ["claude", "codex"] {
        let input = fixtures.join(format!("{provider}.in.kdl"));
        let before = fs::read(&input).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_st2"))
            .args(["--catalog"])
            .arg(&fixtures)
            .args(["driver", "expand"])
            .arg(&input)
            .args(
                (provider == "claude")
                    .then_some(["--agent", "Silber.fabric"])
                    .into_iter()
                    .flatten(),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.stdout,
            fs::read(fixtures.join(format!("{provider}.out.kdl"))).unwrap()
        );
        assert_eq!(fs::read(&input).unwrap(), before);
    }
}
