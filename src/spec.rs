//! The agent job — st2's view of a rendered VRS `agent.kdl` (spec.md §2).
//!
//! A job reads like a Nomad job: the *agent* is the job, its **tasks** are `pty{}` (interactive —
//! allocates a terminal, an agent harness) and `exec{}` (a plain process — the ding, daemons, a
//! stage's script; must NOT allocate a terminal, R09). st2 reads only the runner-normative subset:
//! `identity`, `host`, `type`, `workspace`, `retired`, `keep`, `supervisor`, `restart{}`, and the
//! tasks. Everything render-only (`harness`, `model`, `role`, `persona`, `permissions`, `transport`,
//! `strategy`, `meta{}`) is baked into the tasks/commands by the render layer and ignored here.
//!
//! Three on-disk formats lower to this model: KDL (canonical, parsed by hand in `kdl_format`), and
//! TOML/JSON (serde). Every spec is a `service` — `type = batch` is retired; evals run through the
//! native `st2 eval` path (`eval_spec`/`eval_run`), which has its own model.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

/// A rendered agent job, lowered to the fields st2 needs to run it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSpec {
    /// Unique id; the bus id is `<host>.<identity>`.
    pub identity: String,
    /// Which machine runs this agent. `None` → resolved to the path's host / this machine.
    pub host: Option<String>,
    /// Optional declared persona role (used by local shepherding; ignored for execution).
    pub role: Option<String>,
    /// `service` (long-running, respawns) — the only job type. Defaults to service.
    pub job_type: JobType,
    /// The repo/worktree; **defaults each task's cwd** (spec.md §2).
    pub workspace: Option<String>,
    /// Identity of this agent's supervisor — crash-dings/escalations route here.
    pub supervisor: Option<String>,
    /// `true` decommissions the agent (an edit, never a file delete) → torn down by reconcile.
    pub retired: bool,
    /// Agent-level GC pin: `true` exempts all of its tasks from garbage collection.
    pub keep: bool,
    /// Crash/restart policy (§4). `None` → the runner's default policy.
    pub restart: Option<Restart>,
    /// How this agent's pane accepts a ding poke, when the author declares it. `None` → inferred
    /// from the command (see `ding::DeliveryMode`). Kept as the raw word so `spec` stays independent
    /// of the ding; `validate` rejects an unknown one.
    pub delivery: Option<String>,
    /// The runnable tasks (`pty` + `exec`), sorted by name for determinism.
    pub tasks: Vec<Task>,
    /// Where this spec was loaded from — the anchor for its resources and for edits.
    pub path: PathBuf,
}

/// The kind of job. Only `service` (long-running) remains — `type = batch` is retired; the native
/// `st2 eval` path (eval_spec/eval_run) replaces the old staged batch executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JobType {
    #[default]
    Service,
}

/// A task: one process st2 keeps running (a `pty` task) or runs once (a terminal-free `exec` task).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    /// `pty` (interactive, allocates a terminal) or `exec` (plain process, terminal-free).
    pub kind: TaskKind,
    /// The task name (`agent`, `ding`, …).
    pub name: String,
    /// Explicit on-disk id. `None` → `<host>.<identity>.<name>` at spawn.
    pub id: Option<String>,
    /// The exec command line, run verbatim under `sh -c`. No command → not a launch target.
    pub command: Option<String>,
    /// Working dir; `None` → the agent's `workspace`, else the spec file's directory.
    pub cwd: Option<String>,
    /// Arbitrary metadata (values are `$`-expanded at spawn).
    pub tags: BTreeMap<String, String>,
    /// Environment (values are `$`-expanded at spawn).
    pub env: BTreeMap<String, String>,
    /// Per-task GC pin.
    pub keep: bool,
}

/// Whether a task allocates a terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    /// Interactive — allocates a pseudo-terminal (an agent harness).
    Pty,
    /// Non-interactive — a plain process, no terminal (R09).
    Exec,
}

/// Restart policy (§4). Applies to long-running `service` tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restart {
    /// Max restarts within `interval`.
    pub attempts: u32,
    /// The window `attempts` is counted over.
    pub interval: Duration,
    /// Wait between restarts.
    pub delay: Duration,
    /// `fail` = stop after `attempts` (surface it) · `delay` = keep restarting, resetting per interval.
    pub mode: RestartMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartMode {
    /// Stop after `attempts` exhaust and surface it.
    Fail,
    /// Keep restarting, resetting the counter each interval.
    Delay,
}

impl Default for Restart {
    /// The runner default when a job omits `restart{}` — mirrors the pre-VRS flapping-cap (3 / 60s),
    /// keep-restarting.
    fn default() -> Self {
        Self {
            attempts: 3,
            interval: Duration::from_secs(60),
            delay: Duration::from_secs(0),
            mode: RestartMode::Delay,
        }
    }
}

impl AgentSpec {
    /// The bus id this spec compiles to — `<host>.<identity>` — using `this_host` when `host` is unset.
    pub fn bus_id(&self, this_host: &str) -> String {
        format!(
            "{}.{}",
            self.host.as_deref().unwrap_or(this_host),
            self.identity
        )
    }

    /// The host that should run this spec, defaulting to `this_host` when unset.
    pub fn resolved_host<'a>(&'a self, this_host: &'a str) -> &'a str {
        self.host.as_deref().unwrap_or(this_host)
    }

    /// True once at least one task carries an explicit `command` (i.e. the job was rendered).
    pub fn is_runnable(&self) -> bool {
        self.tasks.iter().any(|t| t.command.is_some())
    }

    /// The restart policy in effect (declared, else the runner default).
    pub fn restart_policy(&self) -> Restart {
        self.restart.clone().unwrap_or_default()
    }
}

// ---- Duration parsing ("60s", "5s", "20m", "2h", "3d") ---------------------------------------

/// Parse a duration like `60s` / `5m` / `2h` / `3d` (also a bare integer = seconds).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".into());
    }
    let (num, unit) = match s.find(|c: char| c.is_ascii_alphabetic()) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, "s"), // bare number → seconds
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("bad duration number in '{s}'"))?;
    let secs = match unit.trim() {
        "s" | "sec" | "secs" => n,
        "m" | "min" | "mins" => n * 60,
        "h" | "hr" | "hrs" => n * 3600,
        "d" | "day" | "days" => n * 86400,
        "ms" => return Ok(Duration::from_millis(n)),
        other => return Err(format!("unknown duration unit '{other}' in '{s}'")),
    };
    Ok(Duration::from_secs(secs))
}

// ---- Raw deserialization target (shared by TOML + JSON) --------------------------------------

/// The permissive on-disk shape. Unknown keys (`harness`, `model`, `role`, `persona`, `permissions`,
/// `transport`, `strategy`, `meta`, …) are intentionally dropped — that is how st2 stays render-agnostic.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct RawSpec {
    pub identity: Option<String>,
    pub host: Option<String>,
    pub role: Option<String>,
    #[serde(rename = "type")]
    pub job_type: Option<String>,
    pub workspace: Option<String>,
    pub supervisor: Option<String>,
    #[serde(default)]
    pub retired: bool,
    #[serde(default)]
    pub keep: bool,
    pub restart: Option<RawRestart>,
    /// Compact catalog form: the agent itself is one pty carrying this command.
    pub command: Option<String>,
    /// Compact catalog form: environment inherited by the agent pty and every sidecar.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Compact catalog form: include the built-in `st2 ding` sidecar.
    #[serde(default)]
    pub ding: bool,
    /// Declared ding delivery mode, overriding command-shape inference.
    pub delivery: Option<String>,
    /// `pty "<name>" {}` / `[pty.<name>]` — interactive tasks.
    #[serde(default)]
    pub pty: BTreeMap<String, RawTask>,
    /// `exec "<name>" {}` / `[exec.<name>]` — terminal-free tasks.
    #[serde(default)]
    pub exec: BTreeMap<String, RawTask>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RawTask {
    pub id: Option<String>,
    pub command: Option<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub keep: bool,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RawRestart {
    pub attempts: Option<u32>,
    pub interval: Option<String>,
    pub delay: Option<String>,
    pub mode: Option<String>,
}

impl RawRestart {
    pub(crate) fn lower(self) -> Restart {
        let d = Restart::default();
        Restart {
            attempts: self.attempts.unwrap_or(d.attempts),
            interval: self
                .interval
                .and_then(|s| parse_duration(&s).ok())
                .unwrap_or(d.interval),
            delay: self
                .delay
                .and_then(|s| parse_duration(&s).ok())
                .unwrap_or(d.delay),
            mode: match self.mode.as_deref() {
                Some("fail") => RestartMode::Fail,
                Some("delay") => RestartMode::Delay,
                _ => d.mode,
            },
        }
    }
}

impl RawSpec {
    /// A parsed file is a *spec candidate* when it carries an agent-shaped signal — an identity, a
    /// `type`, or task blocks. Random TOML/JSON in the tree has none of these and is skipped.
    pub(crate) fn looks_like_spec(&self) -> bool {
        self.identity.is_some()
            || self.job_type.is_some()
            || self.command.is_some()
            || self.ding
            || !self.pty.is_empty()
            || !self.exec.is_empty()
    }

    /// Lower into an [`AgentSpec`], with `identity`/`host` resolved from the path when content omits them.
    pub(crate) fn into_agent_spec(
        self,
        identity: String,
        host: Option<String>,
        path: PathBuf,
    ) -> AgentSpec {
        let bus_id = format!("{}.{}", host.as_deref().unwrap_or_default(), identity)
            .trim_start_matches('.')
            .to_string();
        let mut tasks: Vec<Task> = Vec::new();
        for (name, t) in self.pty {
            tasks.push(t.lower(TaskKind::Pty, name, &self.env));
        }
        for (name, t) in self.exec {
            tasks.push(t.lower(TaskKind::Exec, name, &self.env));
        }
        if let Some(command) = self.command {
            let mut tags = BTreeMap::new();
            tags.insert("role".to_string(), "agent".to_string());
            tasks.push(Task {
                kind: TaskKind::Pty,
                name: "agent".to_string(),
                // An agent IS its pty: ding defaults its poke target to this same bus id.
                id: Some(bus_id.clone()),
                command: Some(command),
                cwd: None,
                tags,
                env: self.env.clone(),
                keep: false,
            });
        }
        if self.ding {
            tasks.push(Task {
                kind: TaskKind::Exec,
                name: "ding".to_string(),
                id: Some(format!("{bus_id}.ding")),
                command: Some(format!("st2 ding --identity {bus_id} --root $ST_ROOT")),
                cwd: None,
                tags: BTreeMap::new(),
                env: self.env,
                keep: false,
            });
        }
        tasks.sort_by(|a, b| a.name.cmp(&b.name));

        // `service` is the only job type; a stray `type` string is caught by validate (unknown-type).
        let job_type = JobType::Service;

        AgentSpec {
            identity,
            host,
            role: self.role,
            job_type,
            workspace: self.workspace,
            supervisor: self.supervisor,
            retired: self.retired,
            keep: self.keep,
            restart: self.restart.map(RawRestart::lower),
            delivery: self.delivery,
            tasks,
            path,
        }
    }
}

impl RawTask {
    pub(crate) fn lower(
        self,
        kind: TaskKind,
        name: String,
        inherited_env: &BTreeMap<String, String>,
    ) -> Task {
        let mut env = inherited_env.clone();
        env.extend(self.env);
        Task {
            kind,
            name,
            id: self.id,
            command: self.command,
            cwd: self.cwd,
            tags: self.tags,
            env,
            keep: self.keep,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("3d").unwrap(), Duration::from_secs(259200));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30)); // bare = seconds
        assert!(parse_duration("nope").is_err());
        assert!(parse_duration("").is_err());
    }
}
