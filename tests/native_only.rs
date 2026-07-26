use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn clean_path() -> tempfile::TempDir {
    let bin = tempfile::tempdir().unwrap();
    executable(
        &bin.path().join("pty"),
        "#!/bin/sh\nif [ \"$1\" = \"list\" ]; then printf '[]\\n'; fi\nexit 0\n",
    );
    executable(&bin.path().join("codex"), "#!/bin/sh\nexit 0\n");
    executable(&bin.path().join("claude"), "#!/bin/sh\nexit 0\n");
    bin
}

fn native_catalog(root: &Path, workspace: &Path) {
    let declaration = root.join("agents/h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        declaration,
        format!(
            "agent \"worker\" {{\n  host \"h\"\n  role \"worker\"\n  workspace \"{}\"\n  \
             env {{ ST_AGENT \"h.worker\" }}\n  command \"true\"\n  ding\n}}\n",
            workspace.display()
        ),
    )
    .unwrap();
}

#[test]
fn clean_path_supports_help_validate_env_and_doctor() {
    let bin = clean_path();
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path().join("catalog");
    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    native_catalog(&catalog, &workspace);

    for absent in [["c", "onvoy"].concat(), ["s", "t"].concat()] {
        assert!(!bin.path().join(absent).exists());
    }

    let help = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--help")
        .env("PATH", bin.path())
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8_lossy(&help.stdout);
    for command in ["validate", "message", "ding", "hooks", "compile-agent"] {
        assert!(help.contains(command), "missing {command} in help:\n{help}");
    }
    for removed in [
        ["a", "dd"].concat(),
        ["com", "pile"].concat(),
        ["ren", "der"].concat(),
        ["re", "move"].concat(),
        ["build", "-agent"].concat(),
        ["render", "-agent"].concat(),
    ] {
        let listed = help.lines().any(|line| {
            let line = line.trim_start();
            line == removed || line.starts_with(&format!("{removed} "))
        });
        assert!(!listed, "{removed} remained in help:\n{help}");
    }
    assert!(!Path::new("completions").exists());
    assert!(!Path::new("man").exists());

    let validate = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("validate")
        .arg("--catalog")
        .arg(&catalog)
        .env("PATH", bin.path())
        .output()
        .unwrap();
    assert!(
        validate.status.success(),
        "{}",
        String::from_utf8_lossy(&validate.stdout)
    );

    let env = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("env")
        .arg("--catalog")
        .arg(&catalog)
        .env("PATH", bin.path())
        .output()
        .unwrap();
    assert!(env.status.success());
    let env = String::from_utf8_lossy(&env.stdout);
    let canonical = catalog.canonicalize().unwrap();
    assert!(env.contains(&format!("export ST_ROOT={}", canonical.display())));
    assert!(env.contains(&format!("export PTY_ROOT={}/pty", canonical.display())));

    let doctor_catalog = tmp.path().join("doctor-catalog");
    fs::create_dir_all(&doctor_catalog).unwrap();
    let mut owner = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    fs::write(
        doctor_catalog.join(".st2.h.lock"),
        format!("{}\n", owner.id()),
    )
    .unwrap();
    let doctor = Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("doctor")
        .arg("--catalog")
        .arg(&doctor_catalog)
        .args(["--host", "h"])
        .env("PATH", bin.path())
        .env("PTY_ROOT", tmp.path().join("pty"))
        .output()
        .unwrap();
    let _ = owner.kill();
    let _ = owner.wait();
    assert!(
        doctor.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&doctor.stdout),
        String::from_utf8_lossy(&doctor.stderr)
    );
}

#[test]
fn tracked_product_surface_contains_only_native_names() {
    let output = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let paths: Vec<PathBuf> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|raw| !raw.is_empty())
        .map(|raw| PathBuf::from(String::from_utf8(raw.to_vec()).unwrap()))
        .collect();

    let forbidden = [
        ["con", "voy"].concat(),
        ["small", "talk"].concat(),
        [".con", "voy"].concat(),
        ["render", "-agent"].concat(),
        ["build", "-agent"].concat(),
        ["st2 ", "render"].concat(),
        ["st2 ", "add"].concat(),
        ["st2 ", "remove"].concat(),
    ];
    let mut violations = Vec::new();
    for path in paths {
        let Ok(contents) = fs::read_to_string(&path) else {
            continue;
        };
        let lower = contents.to_ascii_lowercase();
        for needle in &forbidden {
            if lower.contains(needle) {
                violations.push(format!("{} contains {needle:?}", path.display()));
            }
        }
        for needle in [
            ["st2 ", "compile "].concat(),
            ["`st2 ", "compile`"].concat(),
        ] {
            if lower.contains(&needle) {
                violations.push(format!("{} contains {needle:?}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "retired product surface returned:\n{}",
        violations.join("\n")
    );
}
