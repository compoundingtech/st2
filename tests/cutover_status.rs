//! `st2 cutover status` is the stable, read-only cooperative mutation preflight.

fn st2() -> std::process::Command {
    std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
}

#[test]
fn json_reports_available_with_canonical_catalog_and_requested_host() {
    let catalog = tempfile::tempdir().unwrap();
    let canonical = catalog.path().canonicalize().unwrap();

    let out = st2()
        .arg("--catalog")
        .arg(catalog.path())
        .args(["cutover", "status", "--host", "hetz.test", "--json"])
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["schema"], "st2.mutation-available.v1");
    assert_eq!(value["catalog"], canonical.display().to_string());
    assert_eq!(value["requestedHost"], "hetz.test");
}

#[test]
fn json_reports_typed_busy_and_exits_nonzero_without_rewriting_state() {
    let catalog = tempfile::tempdir().unwrap();
    let cutover = catalog.path().join(".st2/cutover");
    std::fs::create_dir_all(&cutover).unwrap();
    let marker = cutover.join("active.json");
    std::fs::write(&marker, "{}").unwrap();
    let before = std::fs::read(&marker).unwrap();

    let out = st2()
        .arg("--catalog")
        .arg(catalog.path())
        .args(["cutover", "status", "--host", "hetz.test", "--json"])
        .output()
        .unwrap();

    assert!(!out.status.success());
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["schema"], "st2.mutation-busy.v1");
    assert_eq!(value["requestedHost"], "hetz.test");
    assert_eq!(value["reason"], "malformed-active-marker");
    assert_eq!(std::fs::read(&marker).unwrap(), before);
}

#[test]
fn busy_gate_blocks_hook_publication_but_not_hook_verification() {
    let catalog = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let hooks = state.path().join("hooks");
    let cutover = catalog.path().join(".st2/cutover");
    std::fs::create_dir_all(&cutover).unwrap();
    std::fs::write(cutover.join("active.json"), "{}").unwrap();

    let install = st2()
        .arg("--catalog")
        .arg(catalog.path())
        .args(["hooks", "install"])
        .env("ST_HOOKS", &hooks)
        .output()
        .unwrap();
    assert!(!install.status.success());
    assert!(
        !hooks.exists(),
        "admission must run before hook publication"
    );
    assert!(String::from_utf8_lossy(&install.stderr).contains("runtime mutation refused"));

    let verify = st2()
        .arg("--catalog")
        .arg(catalog.path())
        .args(["hooks", "verify"])
        .env("ST_HOOKS", &hooks)
        .output()
        .unwrap();
    assert!(
        !verify.status.success(),
        "missing hooks still fail verification"
    );
    assert!(
        !String::from_utf8_lossy(&verify.stderr).contains("runtime mutation refused"),
        "read-only hook verification must remain outside cutover admission"
    );
}
