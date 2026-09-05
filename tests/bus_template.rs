//! The shipped bus instructions are an executable contract, not prose: every agent that boots
//! against `templates/bus.st2.md` runs these exact command lines with `$ST_AGENT` in its
//! environment. `$ST_AGENT` carries the immutable agent ID, so a selector that reaches the address
//! parser is wrong twice over — it is the mutable route, and a renamed agent's boot ritual would
//! start failing. These tests read the template itself, so the instruction and the CLI cannot drift.

use std::fs;
use std::path::Path;
use std::process::Command;

const AGENT_ID: &str = "h.worker";

fn template() -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("templates/bus.st2.md"))
        .expect("read the shipped bus template")
}

/// Every backticked snippet in the template that invokes `st2 status` with `$ST_AGENT`.
fn status_snippets(source: &str) -> Vec<String> {
    source
        .split('`')
        .enumerate()
        // Odd spans are inside a backtick pair; even spans are surrounding prose. A snippet may
        // wrap across a hard line break, so collapse the newline the way a reader would.
        .filter(|(index, _)| index % 2 == 1)
        .map(|(_, span)| span.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|span| span.starts_with("st2 status") && span.contains("$ST_AGENT"))
        .collect()
}

#[test]
fn the_shipped_boot_ritual_selects_the_agent_by_immutable_id() {
    let snippets = status_snippets(&template());
    assert!(
        snippets.len() >= 3,
        "the template must keep its boot-ritual and status-discipline commands: {snippets:?}"
    );
    for snippet in &snippets {
        assert!(
            snippet.contains("--id \"$ST_AGENT\""),
            "a shipped status command must pass $ST_AGENT through the exact-ID form, never the \
             positional address slot: {snippet}"
        );
    }
}

/// Run the template's own command lines against a real catalog with the same environment st2
/// injects into a task, and prove each one actually sets the presence it claims.
#[test]
fn the_shipped_status_commands_run_as_written_inside_a_task_environment() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = tmp.path();
    let declaration = catalog.join("h/worker/agent.kdl");
    fs::create_dir_all(declaration.parent().unwrap()).unwrap();
    fs::write(
        &declaration,
        "agent \"worker\" {\n  identity \"worker\"\n  host \"h\"\n  command \"true\"\n}\n",
    )
    .unwrap();

    let bin = Path::new(env!("CARGO_BIN_EXE_st2"));
    let path = format!(
        "{}:{}",
        bin.parent().unwrap().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let snippets = status_snippets(&template());
    assert!(!snippets.is_empty());
    for snippet in &snippets {
        let out = Command::new("sh")
            .arg("-c")
            .arg(snippet)
            .env("PATH", &path)
            .env("CATALOG", catalog)
            .env("ST_AGENT", AGENT_ID)
            .output()
            .expect("run the shipped bus command");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "shipped bus command failed: {snippet}\nstdout: {stdout}\nstderr: {stderr}"
        );
        // `--set <state>` echoes the state it wrote; that is what a booting agent sees.
        let state = snippet
            .split("--set ")
            .nth(1)
            .expect("a shipped status command sets a state")
            .split_whitespace()
            .next()
            .unwrap();
        assert!(
            stdout.contains(state),
            "`{snippet}` must report the state it set ({state}): {stdout}"
        );
    }

    let read = Command::new(bin)
        .args(["status", "--id", AGENT_ID, "--root"])
        .arg(catalog)
        .args(["--host", "h"])
        .output()
        .unwrap();
    assert!(read.status.success(), "{read:?}");
    let observed = String::from_utf8_lossy(&read.stdout);
    let last = snippets
        .last()
        .unwrap()
        .split("--set ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap();
    assert!(
        observed.contains(last),
        "the last shipped write must be the persisted presence ({last}): {observed}"
    );
}
