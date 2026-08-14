use std::fs;

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
