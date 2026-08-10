use std::fs;
use std::path::Path;

use st2::DeliveryTransport;
use st2::reconcile::{TaskCompileContext, compile_generated_tasks};

fn write(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

fn context(root: &Path) -> TaskCompileContext {
    let executable = root.join("bin/st2");
    write(&executable, "test binary");
    TaskCompileContext::new(root.to_path_buf(), executable).unwrap()
}

#[test]
fn app_server_selector_wraps_the_canonical_argv_with_exact_owner_inputs() {
    let tmp = tempfile::tempdir().unwrap();
    let declaration = tmp.path().join("agents/h/worker/agent.kdl");
    write(
        &declaration,
        r#"agent "worker" {
  host "h"
  deliver "app-server"
  argv "codex" "--model" "gpt-test" "boot"
}
"#,
    );
    let mut found = st2::discover(tmp.path());
    assert!(found.errors.is_empty(), "{:?}", found.errors);

    compile_generated_tasks(&mut found.specs, "h", &context(tmp.path())).unwrap();

    let spec = &found.specs[0];
    assert_eq!(spec.delivery, Some(DeliveryTransport::AppServer));
    let task = spec.tasks.iter().find(|task| task.name == "agent").unwrap();
    assert_eq!(task.command, None);
    assert_eq!(
        task.argv.as_deref(),
        Some(
            [
                tmp.path().join("bin/st2").display().to_string(),
                "--catalog".into(),
                tmp.path().display().to_string(),
                "codex-app-server".into(),
                "--identity".into(),
                "h.worker".into(),
                "--runtime-id".into(),
                "h.worker".into(),
                "--".into(),
                "codex".into(),
                "--model".into(),
                "gpt-test".into(),
                "boot".into(),
            ]
            .as_slice()
        )
    );
}

#[test]
fn app_server_selector_rejects_shell_and_pre_remote_launches_without_mutating_them() {
    for (name, launch, expected) in [
        (
            "shell",
            "command \"exec codex\"",
            "must use structured `argv`",
        ),
        (
            "remote",
            "argv \"codex\" \"--remote\" \"unix:///other.sock\"",
            "already declares `--remote`",
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        write(
            &tmp.path().join(format!("agents/h/{name}/agent.kdl")),
            &format!("agent \"{name}\" {{ host \"h\"; deliver \"app-server\"; {launch} }}"),
        );
        let mut found = st2::discover(tmp.path());
        assert!(found.errors.is_empty(), "{name}: {:?}", found.errors);
        let before = found.specs.clone();
        let error =
            compile_generated_tasks(&mut found.specs, "h", &context(tmp.path())).unwrap_err();
        assert!(error.to_string().contains(expected), "{error:#}");
        assert_eq!(found.specs, before, "{name} compile failure mutated source");
    }
}

#[test]
fn mcp_selector_does_not_rewrite_the_authored_launch() {
    let tmp = tempfile::tempdir().unwrap();
    write(
        &tmp.path().join("agents/h/worker/agent.kdl"),
        r#"agent "worker" { host "h"; deliver "mcp"; argv "claude" "boot" }"#,
    );
    let mut found = st2::discover(tmp.path());
    let before = found.specs.clone();
    compile_generated_tasks(&mut found.specs, "h", &context(tmp.path())).unwrap();
    assert_eq!(found.specs, before);
}
