//! `<catalog>/catalog.kdl` — the catalog's declaration of its own session registry.
//!
//! Hermetic: every command runs with `CATALOG`/`ST_ROOT`/`PTY_ROOT` removed and `HOME`/
//! `XDG_STATE_HOME` pointed at temp dirs, so nothing here can resolve a real catalog or registry.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn st2(args: &[&str], home: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .args(args)
        .env("HOME", home)
        .env("XDG_STATE_HOME", home.join("state"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT")
        .env_remove("PTY_ROOT")
        .output()
        .unwrap()
}

fn write_agent(catalog: &Path, host: &str, identity: &str) {
    let dir = catalog.join("agents").join(host).join(identity);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("agent.kdl"),
        format!("agent \"{identity}\" {{ host \"{host}\"; command \"true\" }}\n"),
    )
    .unwrap();
}

/// The declared root reaches the bus env every reader resolves from the catalog — no ambient
/// `PTY_ROOT` needed, and no reader left looking under `<catalog>/pty` for sessions that are not
/// there.
#[test]
fn a_declared_pty_root_replaces_the_catalog_default_in_the_bus_env() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = tmp.path().join("shared-registry");
    let declared = tmp.path().join("declared");
    let plain = tmp.path().join("plain");
    write_agent(&declared, "h", "seat");
    write_agent(&plain, "h", "seat");
    fs::write(
        declared.join("catalog.kdl"),
        format!("catalog {{\n  pty-root \"{}\"\n}}\n", shared.display()),
    )
    .unwrap();

    let out = st2(&["env", "--catalog", declared.to_str().unwrap()], tmp.path());
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let env = String::from_utf8_lossy(&out.stdout);
    assert!(
        env.contains(&format!("export PTY_ROOT={}\n", shared.display())),
        "declared root missing from the bus env:\n{env}"
    );
    assert!(
        env.contains(&format!("export ST_ROOT={}\n", declared.canonicalize().unwrap().display())),
        "the flat native ST_ROOT must be unaffected:\n{env}"
    );

    // Control: a catalog that declares nothing keeps the native `<catalog>/pty`.
    let out = st2(&["env", "--catalog", plain.to_str().unwrap()], tmp.path());
    let env = String::from_utf8_lossy(&out.stdout);
    assert!(
        env.contains(&format!(
            "export PTY_ROOT={}/pty\n",
            plain.canonicalize().unwrap().display()
        )),
        "an undeclared catalog must be unchanged:\n{env}"
    );
}

/// `up`/`down` treat a folder holding exactly one parseable top-level spec as a single-file team.
/// `catalog.kdl` is a top-level `*.kdl`, so this pins that a declaring catalog is still a catalog:
/// `catalog` is not a spec node, and `--materialize-only` is refused on the single-file-spec path.
#[test]
fn declaring_a_root_does_not_turn_the_catalog_into_a_single_file_spec() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    write_agent(&catalog, "h", "seat");
    fs::write(
        catalog.join("catalog.kdl"),
        "catalog { pty-root \"/run/agents/pty\" }\n",
    )
    .unwrap();

    let out = st2(
        &[
            "up",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "h",
            "--materialize-only",
        ],
        tmp.path(),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "catalog dispatch broke:\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        !stderr.contains("single-file specs"),
        "the declaration was mistaken for a team spec:\n{stderr}"
    );
}

/// A mistyped field resolves back to `<catalog>/pty`, which is the split registry this declaration
/// exists to prevent — so it fails the gate instead of degrading quietly.
#[test]
fn a_mistyped_declaration_fails_validate() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    write_agent(&catalog, "h", "seat");
    fs::write(
        catalog.join("catalog.kdl"),
        "catalog { pty_root \"/run/agents/pty\" }\n",
    )
    .unwrap();

    let out = st2(
        &["validate", "--catalog", catalog.to_str().unwrap()],
        tmp.path(),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success(), "validate passed a typo:\n{stdout}");
    assert!(
        stdout.contains("unknown catalog field 'pty_root'"),
        "no issue naming the typo:\n{stdout}"
    );

    // The machine-matchable code a renderer's build gate keys on.
    let out = st2(
        &["validate", "--json", "--catalog", catalog.to_str().unwrap()],
        tmp.path(),
    );
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["issues"][0]["code"], "catalog-config");
    assert_eq!(report["issues"][0]["severity"], "error");
}

#[test]
fn matching_host_config_outranks_and_isolates_a_malformed_shared_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let host_config = catalog.join("agents/h/config.kdl");
    fs::create_dir_all(host_config.parent().unwrap()).unwrap();
    fs::write(
        &host_config,
        "host { pty-root \"$CATALOG/host-registry\" }\n",
    )
    .unwrap();
    fs::write(
        catalog.join("catalog.kdl"),
        "catalog { pty_root \"malformed-unused-fallback\" }\n",
    )
    .unwrap();

    let out = st2(
        &["env", "--catalog", catalog.to_str().unwrap(), "--host", "h"],
        tmp.path(),
    );
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains(&format!(
        "export PTY_ROOT={}/host-registry",
        catalog.canonicalize().unwrap().display()
    )));

    // Without a matching host layer, the malformed shared layer is needed and fails closed.
    let out = st2(
        &[
            "env",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "other",
        ],
        tmp.path(),
    );
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unknown catalog field 'pty_root'"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn validate_diagnoses_root_and_every_host_config_without_counting_phantom_agents() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    write_agent(&catalog, "h", "seat");
    fs::write(
        catalog.join("catalog.kdl"),
        "catalog { pty_root \"bad-root\" }\n",
    )
    .unwrap();
    for (host, contents) in [
        ("h", "host { pty_root \"bad-host\" }\n"),
        ("other", "catalog { pty-root \"wrong-node\" }\n"),
    ] {
        let path = catalog.join("agents").join(host).join("config.kdl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    let out = st2(
        &[
            "validate",
            "--json",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "h",
        ],
        tmp.path(),
    );
    assert!(!out.status.success());
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["agents"], 1);
    let issues = report["issues"].as_array().unwrap();
    for expected in [
        "catalog.kdl",
        "agents/h/config.kdl",
        "agents/other/config.kdl",
    ] {
        assert!(
            issues.iter().any(|issue| issue["path"] == expected),
            "missing {expected}: {issues:?}"
        );
    }
}

#[test]
fn up_uses_the_matching_host_root_for_runtime_session_operations() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let config = catalog.join("agents/h/config.kdl");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    fs::write(config, "host { pty-root \"$CATALOG/runtime-registry\" }\n").unwrap();

    let bin = tmp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let marker = tmp.path().join("observed-pty-root");
    let pty = bin.join("pty");
    fs::write(
        &pty,
        format!(
            "#!/bin/sh\nprintf '%s' \"$PTY_ROOT\" > '{}'\nprintf '[]\\n'\n",
            marker.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&pty, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let out = Command::new(env!("CARGO_BIN_EXE_st2"))
        .args([
            "up",
            "--once",
            "--catalog",
            catalog.to_str().unwrap(),
            "--host",
            "h",
        ])
        .env("PATH", path)
        .env("HOME", tmp.path())
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .env_remove("PTY_ROOT")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        fs::read_to_string(marker).unwrap(),
        catalog
            .canonicalize()
            .unwrap()
            .join("runtime-registry")
            .display()
            .to_string()
    );
}
