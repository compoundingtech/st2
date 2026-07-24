//! `st2 service` (install/status/uninstall) CLI wiring + the `st2 ping` alias for `st2 ding`.
//!
//! We deliberately do NOT exercise a real `service install` here: it would write a real
//! `st2.service` systemd-user unit and enable+start it on the test host. So these tests cover the
//! CLI surface and the pre-systemctl guard (a missing catalog fails before any `systemctl` runs);
//! the unit-rendering logic itself is exhaustively covered by `src/service.rs` unit tests.

fn st2() -> std::process::Command {
    std::process::Command::new(env!("CARGO_BIN_EXE_st2"))
}

#[test]
fn install_rejects_a_missing_catalog_before_touching_systemd() {
    // canonicalize() is the first thing install() does, so this errors out before any systemctl call.
    let out = st2()
        .args(["service", "install", "/definitely/not/a/real/catalog/st2test"])
        .output()
        .unwrap();
    assert!(!out.status.success(), "should fail on a missing catalog");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("does not exist"), "expected a missing-catalog error, got: {err}");
}

#[test]
fn service_exposes_install_status_uninstall() {
    let out = st2().args(["service", "--help"]).output().unwrap();
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("install"), "{help}");
    assert!(help.contains("status"), "{help}");
    assert!(help.contains("uninstall"), "{help}");
}

#[test]
fn ping_is_an_alias_for_ding() {
    // `st2 ping` must dispatch to the ding handler: with no <session> it fails with ding's own
    // "required arguments were not provided: <SESSION>" — NOT an "unrecognized subcommand" error.
    let out = st2().arg("ping").output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("SESSION"), "ping should be the ding command; got: {err}");
    assert!(
        !err.to_lowercase().contains("unrecognized"),
        "ping should be a recognized alias; got: {err}"
    );
}
