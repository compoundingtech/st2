//! Isolated task spawn (R21b) — the permanent fleet-fragility fix.
//!
//! st2 already survives its own *process* death (`setsid`, see [`crate::exec_backend`] and
//! `tests/nomad_survival.rs`). But `setsid` changes a process's **session**, not its **cgroup**.
//! systemd tears a unit down by cgroup, so restarting a supervisor unit would kill every task still
//! in that cgroup.
//!
//! The fix: spawn each task into its own OS supervision domain, independent of BOTH the spawner and
//! the transport daemon — one goal, per-OS mechanism.
//!
//! - **Linux**: `systemd-run --user --scope --unit=<unit> --collect --quiet -- <task>`. The task runs
//!   in its own transient scope = its own cgroup, registered with the user manager as a **sibling** of
//!   the transport unit (a scope created inside a service lands at `app.slice/<unit>`, not nested
//!   under the service). A cascade kill of the transport unit's cgroup cannot
//!   reach a sibling. `--scope` (not `--service`) keeps st2 the logical supervisor — systemd provides
//!   only the cgroup; adoption/teardown/restart stay st2's. `--collect` GCs the scope once it empties.
//! - **macOS / non-systemd Linux**: `setsid` + reparent to init/launchd is the whole defense — there
//!   are no cgroups, and launchd does not cascade-kill detached children. Here [`wrap`] is a no-op
//!   pass-through; the caller's existing `setsid` (exec) or the `pty` daemon (pty) provides detachment.
//!   If isolation was *wanted* (a Linux box) but systemd is unreachable, we degrade to that same
//!   pass-through and log a loud WARN — never a silent "isolated" claim.
//!
//! Teardown is unchanged: the scope is for **survival only**. `pty kill` / the exec process-group kill
//! still tear tasks down; the scope just prevents the transport from taking them as collateral.

use std::ffi::OsStr;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// How a task is isolated from its spawner and the transport daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Linux: own transient systemd `--user` scope (own cgroup, sibling of the transport unit).
    Scope,
    /// macOS / non-systemd: `setsid` + reparent to init/launchd (no cgroup needed — the transport
    /// cannot cascade-kill a detached process on these platforms).
    Detached,
    /// Isolation was wanted (a Linux host) but systemd is unreachable — degraded to `Detached` with a
    /// logged WARN. Distinct from `Detached` so callers/tests can tell an intended pass-through from a
    /// degraded one.
    DegradedDetached,
}

static MODE: OnceLock<Isolation> = OnceLock::new();

/// The isolation mode for this host, detected once and cached.
pub fn mode() -> Isolation {
    *MODE.get_or_init(detect)
}

fn detect() -> Isolation {
    if cfg!(target_os = "linux") {
        if systemd_user_available() {
            Isolation::Scope
        } else {
            eprintln!(
                "st2: WARN systemd user scopes unavailable (no `systemd-run --user` / no \
                 $XDG_RUNTIME_DIR) — spawning tasks WITHOUT cgroup isolation. A transport/supervisor \
                 restart may cascade-kill them. Enable a user systemd manager (loginctl enable-linger) \
                 to restore isolation."
            );
            Isolation::DegradedDetached
        }
    } else {
        // macOS et al: setsid + reparent is the whole defense; no cgroups to escape.
        Isolation::Detached
    }
}

/// Whether a `--user` systemd scope can be created here.
fn systemd_user_available() -> bool {
    if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
        return false;
    }
    Command::new("systemd-run")
        .args(["--user", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

static SCOPE_SEQ: AtomicU64 = AtomicU64::new(0);

/// A fresh, systemd-safe scope unit name for a task id. The name is **write-only** — st2 references it
/// only at spawn (`--unit=`); teardown is by process group / `pty kill` and adoption is by pidfile /
/// `pty list`, neither of which needs the scope name. So it must be UNIQUE, not deterministic: a
/// deterministic name collides whenever a scope of that name still lingers — a stale/failed scope on
/// re-spawn, or (in tests) a concurrent spawn of the same declared id — and `systemd-run --unit=<X>`
/// fails hard on an existing `<X>`. A per-process nonce (`<pid>-<seq>`) rules that out. The task id is
/// kept in the name purely for greppability in `systemctl --user list-units`. Non-`[A-Za-z0-9:_.-]`
/// bytes → `_`.
pub fn scope_unit(task_id: &str) -> String {
    let safe: String = task_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '.' | '-') { c } else { '_' })
        .collect();
    let seq = SCOPE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("st2-{safe}-{}-{seq}.scope", std::process::id())
}

/// Build the OUTER launch [`Command`] for the inner `program` + `args`, isolated under `unit`.
///
/// In [`Isolation::Scope`] this is `systemd-run --user --scope --unit=<unit> --collect --quiet --
/// <program> <args>`; otherwise it is `<program> <args>` verbatim. Either way the caller applies
/// env / cwd / stdio / `pre_exec` to the returned Command and they reach the task — for `--scope`,
/// scope mode runs the command in the caller's context, so cwd, environment, and stdio fds all
/// inherit (verified).
pub fn wrap(unit: &str, program: &OsStr, args: &[&OsStr]) -> Command {
    match mode() {
        Isolation::Scope => {
            let mut c = Command::new("systemd-run");
            c.args(["--user", "--scope", "--collect", "--quiet"])
                .arg(format!("--unit={unit}"))
                .arg("--")
                .arg(program)
                .args(args);
            c
        }
        Isolation::Detached | Isolation::DegradedDetached => {
            let mut c = Command::new(program);
            c.args(args);
            c
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_unit_is_unique_and_systemd_safe() {
        // Keeps the (dotted) id for greppability, gains the st2- prefix, a nonce, and the .scope suffix.
        let u = scope_unit("hetz.demo.agent");
        assert!(u.starts_with("st2-hetz.demo.agent-"), "unexpected unit name {u}");
        assert!(u.ends_with(".scope"), "unexpected unit name {u}");
        // Unsafe bytes (space, slash) are replaced so systemd never rejects the unit name.
        assert!(scope_unit("a b/c").starts_with("st2-a_b_c-"));
        // UNIQUE, not deterministic — two spawns of the same id never collide on the scope name.
        assert_ne!(scope_unit("x"), scope_unit("x"));
    }

    #[test]
    fn wrap_scope_prefixes_systemd_run_but_passthrough_does_not() {
        // We can't force `mode()` per-test (it's process-cached), so assert the shape that matches the
        // detected mode: on a systemd Linux CI box it's Scope; otherwise pass-through.
        let cmd = wrap("st2-x.scope", OsStr::new("sh"), &[OsStr::new("-c"), OsStr::new("true")]);
        let args: Vec<String> = cmd.get_args().map(|a| a.to_string_lossy().into_owned()).collect();
        match mode() {
            Isolation::Scope => {
                assert_eq!(cmd.get_program(), OsStr::new("systemd-run"));
                assert!(args.contains(&"--scope".to_string()));
                assert!(args.contains(&"--unit=st2-x.scope".to_string()));
                assert!(args.contains(&"--collect".to_string()));
                // The inner command follows the `--` separator, verbatim.
                let sep = args.iter().position(|a| a == "--").unwrap();
                assert_eq!(&args[sep + 1..], &["sh", "-c", "true"]);
            }
            Isolation::Detached | Isolation::DegradedDetached => {
                assert_eq!(cmd.get_program(), OsStr::new("sh"));
                assert_eq!(args, vec!["-c", "true"]);
            }
        }
    }
}
