//! Free, deterministic supervision that runs after a catalog reconcile pass.
//!
//! The pass uses the process snapshot, presence, driver-owned harness state, provider session
//! metadata, and exact known PTY gates. It never reads or runs a plan `Check` command. A model sees
//! only the final residue: a durable nudge for an idle owner of an open plan.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_spec::spec::{AgentSpec, Driver, TaskKind};
use sha2::{Digest as _, Sha256};

use crate::harness_state::{Activity, BlockedOn, SessionLiveness};
use crate::run::{PtyKey, Runner};
use crate::{harness_sessions, harness_state, message, status};

const IDLE_THRESHOLD: Duration = Duration::from_secs(60 * 60);
const FUTURE_ACTIVITY_SKEW: Duration = Duration::from_secs(60);
const MAX_PLAN_HEADER_BYTES: usize = 64 * 1024;
const PLAN_DOCS: [&str; 3] = ["FORMAT.md", "README.md", "TEMPLATE.md"];

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SupervisionReport {
    pub gates_cleared: Vec<String>,
    pub plans_nudged: Vec<String>,
    pub plans_skipped: Vec<String>,
    pub errors: Vec<String>,
}

pub(crate) fn reconcile(
    catalog: &Path,
    host: &str,
    specs: &[AgentSpec],
    sessions: &[crate::Session],
    runner: &dyn Runner,
) -> SupervisionReport {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    reconcile_at(
        catalog,
        host,
        specs,
        sessions,
        runner,
        &crate::run::state_root(),
        home.as_deref(),
        u128::from(message::now_ms()) * 1_000_000,
    )
}

#[allow(clippy::too_many_arguments)]
fn reconcile_at(
    catalog: &Path,
    host: &str,
    specs: &[AgentSpec],
    sessions: &[crate::Session],
    runner: &dyn Runner,
    state_root: &Path,
    home: Option<&Path>,
    now_nanos: u128,
) -> SupervisionReport {
    let live = sessions
        .iter()
        .filter(|session| session.alive)
        .map(|session| session.pty_id.clone())
        .collect::<HashSet<_>>();
    let mut evidence = EvidenceCache::new(catalog, host, specs, live, home);
    let mut report = SupervisionReport::default();
    let cleared = clear_boot_gates(specs, host, runner, &mut evidence, &mut report);
    scan_plans(
        catalog,
        host,
        state_root,
        now_nanos,
        &cleared,
        &mut evidence,
        &mut report,
    );
    report
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KnownGate {
    ClaudeDevelopmentChannel,
    CodexHookTrust,
}

impl KnownGate {
    fn label(self) -> &'static str {
        match self {
            Self::ClaudeDevelopmentChannel => "Claude development-channel confirmation",
            Self::CodexHookTrust => "Codex hook trust",
        }
    }

    fn keys(self) -> &'static [PtyKey] {
        match self {
            Self::ClaudeDevelopmentChannel => &[PtyKey::Return, PtyKey::Return],
            Self::CodexHookTrust => &[PtyKey::Down, PtyKey::Return],
        }
    }
}

fn clear_boot_gates(
    specs: &[AgentSpec],
    host: &str,
    runner: &dyn Runner,
    evidence: &mut EvidenceCache<'_>,
    report: &mut SupervisionReport,
) -> HashSet<String> {
    let mut cleared = HashSet::new();
    for spec in specs {
        if spec.resolved_host(host) != host || !spec.desired_state.is_running() {
            continue;
        }
        let expected = match spec.driver.as_ref() {
            Some(Driver::Claude(driver)) if driver.dev_channels => {
                KnownGate::ClaudeDevelopmentChannel
            }
            Some(Driver::Codex(_)) => KnownGate::CodexHookTrust,
            _ => continue,
        };
        let owner = spec.bus_id(host);
        let observed = match evidence.observe_exact(spec) {
            Ok(observed) => observed,
            Err(reason) => {
                report
                    .errors
                    .push(format!("inspect boot gate for {owner}: {reason}"));
                continue;
            }
        };
        if !observed.process_alive {
            continue;
        }
        let Some(screen) = (match runner.peek_plain(&observed.runtime_id) {
            Ok(screen) => screen,
            Err(error) => {
                report.errors.push(format!(
                    "inspect {} for {owner}: {error:#}",
                    expected.label()
                ));
                continue;
            }
        }) else {
            continue;
        };
        let Some(gate) = match_gate(spec.driver.as_ref(), &screen) else {
            if screen_has_gate(expected, &screen) {
                report.errors.push(format!(
                    "clear {} for {owner}: the default safe choice is not selected; input refused",
                    expected.label()
                ));
            }
            continue;
        };
        if gate != expected {
            continue;
        }
        if let Err(error) = runner.send_keys(&observed.runtime_id, gate.keys()) {
            report
                .errors
                .push(format!("clear {} for {owner}: {error:#}", gate.label()));
            continue;
        }
        match runner.peek_plain(&observed.runtime_id) {
            Ok(Some(after)) if !screen_has_gate(gate, &after) => {
                cleared.insert(owner.clone());
                report
                    .gates_cleared
                    .push(format!("{owner} — {}", gate.label()));
            }
            Ok(Some(_)) => report.errors.push(format!(
                "clear {} for {owner}: the known gate remained visible after input",
                gate.label()
            )),
            Ok(None) => report.errors.push(format!(
                "clear {} for {owner}: the runner could not verify the PTY after input",
                gate.label()
            )),
            Err(error) => report.errors.push(format!(
                "verify {} for {owner} after input: {error:#}",
                gate.label()
            )),
        }
    }
    cleared
}

fn match_gate(driver: Option<&Driver>, screen: &str) -> Option<KnownGate> {
    match driver {
        Some(Driver::Claude(driver))
            if driver.dev_channels
                && screen_has_gate(KnownGate::ClaudeDevelopmentChannel, screen)
                && selected_line(screen, "❯ 1. I am using this for local development") =>
        {
            Some(KnownGate::ClaudeDevelopmentChannel)
        }
        Some(Driver::Codex(_))
            if screen_has_gate(KnownGate::CodexHookTrust, screen)
                && selected_line(screen, "› 1. Review hooks") =>
        {
            Some(KnownGate::CodexHookTrust)
        }
        _ => None,
    }
}

fn screen_has_gate(gate: KnownGate, screen: &str) -> bool {
    let normalized = screen.split_whitespace().collect::<Vec<_>>().join(" ");
    match gate {
        KnownGate::ClaudeDevelopmentChannel => [
            "WARNING: Loading development channels",
            "--dangerously-load-development-channels is for local channel development only.",
            "Do not use this option to run channels you have downloaded off the internet.",
            "Please use --channels to run a list of approved channels.",
            "I am using this for local development",
        ]
        .iter()
        .all(|part| normalized.contains(part)),
        KnownGate::CodexHookTrust => [
            "Hooks need review",
            "Hooks can run outside the sandbox after you trust them.",
            "Review hooks",
            "Trust all and continue",
            "Continue without trusting (hooks won't run)",
        ]
        .iter()
        .all(|part| normalized.contains(part)),
    }
}

fn selected_line(screen: &str, expected: &str) -> bool {
    screen.lines().any(|line| line.trim() == expected)
}

#[derive(Debug, Clone)]
struct AgentEvidence {
    runtime_id: String,
    process_alive: bool,
    presence: status::State,
    harness: Option<harness_state::Observed>,
    sessions: SessionEvidence,
}

#[derive(Debug, Clone)]
enum SessionEvidence {
    Activity {
        modified_at_nanos: u128,
        last_record_type: String,
    },
    Empty,
    Incomplete(String),
    MissingLastRecord,
    Unsupported(String),
}

struct EvidenceCache<'a> {
    catalog: &'a Path,
    host: &'a str,
    specs: &'a [AgentSpec],
    live: HashSet<String>,
    home: Option<&'a Path>,
    cache: HashMap<String, AgentEvidence>,
}

impl<'a> EvidenceCache<'a> {
    fn new(
        catalog: &'a Path,
        host: &'a str,
        specs: &'a [AgentSpec],
        live: HashSet<String>,
        home: Option<&'a Path>,
    ) -> Self {
        Self {
            catalog,
            host,
            specs,
            live,
            home,
            cache: HashMap::new(),
        }
    }

    fn resolve(&self, owner: &str) -> Result<&AgentSpec, String> {
        let exact = self
            .specs
            .iter()
            .filter(|spec| spec.bus_id(self.host) == owner)
            .collect::<Vec<_>>();
        let matches = if exact.is_empty() && !owner.contains('.') {
            self.specs
                .iter()
                .filter(|spec| spec.identity == owner)
                .collect::<Vec<_>>()
        } else {
            exact
        };
        match matches.as_slice() {
            [spec] => Ok(*spec),
            [] => Err(format!("owner `{owner}` is not a declared agent")),
            _ => Err(format!("owner `{owner}` resolves to more than one agent")),
        }
    }

    fn observe_owner(&mut self, owner: &str) -> Result<(String, AgentEvidence), String> {
        let spec = self.resolve(owner)?.clone();
        let canonical = spec.bus_id(self.host);
        let observed = self.observe_exact(&spec)?;
        Ok((canonical, observed))
    }

    fn observe_exact(&mut self, spec: &AgentSpec) -> Result<AgentEvidence, String> {
        let canonical = spec.bus_id(self.host);
        if let Some(observed) = self.cache.get(&canonical) {
            return Ok(observed.clone());
        }
        let runtime_id = primary_runtime_id(spec, self.host)?;
        let agent_dir = spec
            .path
            .parent()
            .ok_or_else(|| format!("Agent Spec `{canonical}` has no parent directory"))?;
        let presence = status::read_state(&status::status_path(agent_dir));
        let harness = harness_state::read(
            &harness_state::harness_state_path(agent_dir),
            Some(&|runtime| {
                if self.live.contains(runtime) {
                    SessionLiveness::Alive
                } else {
                    SessionLiveness::Dead
                }
            }),
        );
        let sessions = match spec.driver.as_ref() {
            Some(Driver::Claude(_)) => match self.home {
                Some(home) => {
                    let inventory =
                        harness_sessions::inspect(self.catalog, self.host, &canonical, home);
                    if !inventory.complete() {
                        SessionEvidence::Incomplete(
                            inventory
                                .errors()
                                .first()
                                .cloned()
                                .unwrap_or_else(|| "inventory is incomplete".to_string()),
                        )
                    } else {
                        match inventory.newest_activity() {
                            None => SessionEvidence::Empty,
                            Some(activity) => match activity
                                .last_record_type
                                .filter(|record_type| !record_type.is_empty())
                            {
                                Some(last_record_type) => SessionEvidence::Activity {
                                    modified_at_nanos: activity.modified_at_nanos,
                                    last_record_type: last_record_type.to_string(),
                                },
                                None => SessionEvidence::MissingLastRecord,
                            },
                        }
                    }
                }
                None => SessionEvidence::Incomplete("HOME is not set".to_string()),
            },
            Some(driver) => SessionEvidence::Unsupported(driver.name().to_string()),
            None => SessionEvidence::Unsupported("no native driver".to_string()),
        };
        let observed = AgentEvidence {
            process_alive: self.live.contains(&runtime_id),
            runtime_id,
            presence,
            harness,
            sessions,
        };
        self.cache.insert(canonical, observed.clone());
        Ok(observed)
    }
}

fn primary_runtime_id(spec: &AgentSpec, host: &str) -> Result<String, String> {
    let tasks = spec
        .tasks
        .iter()
        .filter(|task| !task.derived && task.name == "agent" && task.kind == TaskKind::Pty)
        .collect::<Vec<_>>();
    match tasks.as_slice() {
        [task] => Ok(task
            .id
            .clone()
            .unwrap_or_else(|| format!("{}.agent", spec.bus_id(host)))),
        [] => Err("no canonical PTY agent task".to_string()),
        _ => Err("more than one canonical PTY agent task".to_string()),
    }
}

fn scan_plans(
    catalog: &Path,
    host: &str,
    state_root: &Path,
    now_nanos: u128,
    gates_cleared: &HashSet<String>,
    evidence: &mut EvidenceCache<'_>,
    report: &mut SupervisionReport,
) {
    let plans = catalog.join("plans");
    let entries = match fs::read_dir(&plans) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            report
                .plans_skipped
                .push(format!("plans — cannot read directory: {error}"));
            return;
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                report
                    .plans_skipped
                    .push(format!("plans — cannot inspect directory entry: {error}"));
                continue;
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                report.plans_skipped.push(format!(
                    "{} — cannot inspect entry: {error}",
                    path.display()
                ));
                continue;
            }
        };
        if !file_type.is_dir() {
            paths.push(path);
        }
    }
    paths.retain(|path| path.extension().and_then(|value| value.to_str()) == Some("md"));
    paths.retain(|path| {
        path.file_name()
            .and_then(|value| value.to_str())
            .is_none_or(|name| !PLAN_DOCS.contains(&name))
    });
    paths.sort();
    for path in paths {
        inspect_plan(
            catalog,
            host,
            state_root,
            now_nanos,
            gates_cleared,
            evidence,
            report,
            &path,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn inspect_plan(
    catalog: &Path,
    host: &str,
    state_root: &Path,
    now_nanos: u128,
    gates_cleared: &HashSet<String>,
    evidence: &mut EvidenceCache<'_>,
    report: &mut SupervisionReport,
    path: &Path,
) {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        report.plans_skipped.push(format!(
            "{} — the plan filename is not UTF-8",
            path.display()
        ));
        return;
    };
    let file = match open_plan(path) {
        Ok(file) => file,
        Err(error) => {
            skip(
                report,
                name,
                format!("cannot open a regular plan file: {error}"),
            );
            return;
        }
    };
    let header = match parse_plan_header(file) {
        Ok(header) => header,
        Err(error) => {
            skip(report, name, error);
            return;
        }
    };
    let state = match header.state {
        Some(Ok(state)) => state,
        Some(Err(state)) => {
            skip(report, name, format!("invalid State `{state}`"));
            return;
        }
        None => {
            skip(report, name, "no State".to_string());
            return;
        }
    };
    if state != "OPEN" {
        skip(report, name, format!("State is {state}, not OPEN"));
        return;
    }
    let owner = match header.owner {
        OwnerField::Single(owner) => owner,
        OwnerField::Missing => {
            skip(report, name, "no Owner".to_string());
            return;
        }
        OwnerField::Plural => {
            skip(report, name, "plural Owners".to_string());
            return;
        }
        OwnerField::Descriptive => {
            skip(
                report,
                name,
                "Owner line is descriptive prose, not a trusted owner".to_string(),
            );
            return;
        }
        OwnerField::Ambiguous => {
            skip(report, name, "ambiguous Owner lines".to_string());
            return;
        }
        OwnerField::Malformed(reason) => {
            skip(report, name, format!("malformed Owner: {reason}"));
            return;
        }
    };
    let spec = match evidence.resolve(&owner) {
        Ok(spec) => spec,
        Err(reason) => {
            skip(report, name, reason);
            return;
        }
    };
    if spec.resolved_host(host) != host {
        skip(
            report,
            name,
            format!("owner `{}` belongs to remote host", spec.bus_id(host)),
        );
        return;
    }
    let (owner, observed) = match evidence.observe_owner(&owner) {
        Ok(observed) => observed,
        Err(reason) => {
            skip(report, name, reason);
            return;
        }
    };
    if gates_cleared.contains(&owner) {
        skip(
            report,
            name,
            "the owner's boot gate cleared this pass".to_string(),
        );
        return;
    }
    if !observed.process_alive {
        skip(
            report,
            name,
            "the owner process is not alive; lifecycle reconciliation has priority".to_string(),
        );
        return;
    }
    if observed.presence == status::State::Dnd {
        skip(report, name, "the owner presence is dnd".to_string());
        return;
    }
    if let Some(harness) = &observed.harness {
        if harness.blocked_on == BlockedOn::Human {
            skip(
                report,
                name,
                "the owner harness waits on a human".to_string(),
            );
            return;
        }
        match harness.state {
            Activity::Active | Activity::Child => {
                skip(report, name, "the owner harness is active".to_string());
                return;
            }
            Activity::Ended => {
                skip(
                    report,
                    name,
                    "the owner harness ended; lifecycle recovery has priority".to_string(),
                );
                return;
            }
            Activity::Idle | Activity::Unknown => {}
        }
    }
    let (modified_at_nanos, last_record_type) = match observed.sessions {
        SessionEvidence::Activity {
            modified_at_nanos,
            last_record_type,
        } => (modified_at_nanos, last_record_type),
        SessionEvidence::Empty => {
            skip(
                report,
                name,
                "the owner session inventory is empty".to_string(),
            );
            return;
        }
        SessionEvidence::Incomplete(reason) => {
            skip(
                report,
                name,
                format!("the owner session inventory is incomplete: {reason}"),
            );
            return;
        }
        SessionEvidence::MissingLastRecord => {
            skip(
                report,
                name,
                "the newest owner session has no last record type".to_string(),
            );
            return;
        }
        SessionEvidence::Unsupported(driver) => {
            skip(
                report,
                name,
                format!("the owner session inventory does not support {driver}"),
            );
            return;
        }
    };
    let future_skew = duration_nanos(FUTURE_ACTIVITY_SKEW);
    if modified_at_nanos > now_nanos.saturating_add(future_skew) {
        skip(
            report,
            name,
            "the owner session activity time is in the future".to_string(),
        );
        return;
    }
    let age = now_nanos.saturating_sub(modified_at_nanos);
    if age < duration_nanos(IDLE_THRESHOLD) {
        skip(
            report,
            name,
            format!("the owner has recent harness activity ({last_record_type})"),
        );
        return;
    }
    let idle_minutes = age / duration_nanos(Duration::from_secs(60));
    let subject = format!("OPEN plan: {name}");
    let body = format!("Owner idle: {idle_minutes} minutes.");
    let key = nudge_key(name, &owner, modified_at_nanos);
    let sender_state = match supervision_sender_state(state_root, catalog, host) {
        Ok(state) => state,
        Err(error) => {
            skip(
                report,
                name,
                format!("cannot open the durable nudge scope: {error:#}"),
            );
            return;
        }
    };
    let tags = ["plan-nudge".to_string()];
    match message::send_system_to_resolved_inbox(
        catalog,
        &owner,
        host,
        &sender_state,
        &format!("st2.{host}"),
        &subject,
        &tags,
        &body,
        &key,
    ) {
        Ok(outcome) if outcome.delivered_now => report
            .plans_nudged
            .push(format!("{name} — {owner} ({idle_minutes} minutes idle)")),
        Ok(_) => skip(
            report,
            name,
            "a nudge already exists for the newest owner activity".to_string(),
        ),
        Err(error) => report
            .errors
            .push(format!("nudge owner {owner} for plan {name}: {error:#}")),
    }
}

fn skip(report: &mut SupervisionReport, name: &str, reason: String) {
    report.plans_skipped.push(format!("{name} — {reason}"));
}

fn duration_nanos(duration: Duration) -> u128 {
    duration.as_nanos()
}

fn nudge_key(plan: &str, owner: &str, modified_at_nanos: u128) -> String {
    let mut digest = Sha256::new();
    digest.update(b"st2.plan-nudge.v1\0");
    digest.update(plan.as_bytes());
    digest.update(b"\0");
    digest.update(owner.as_bytes());
    digest.update(b"\0");
    digest.update(modified_at_nanos.to_string().as_bytes());
    format!("plan-nudge-v1:{:x}", digest.finalize())
}

fn supervision_sender_state(
    state_root: &Path,
    catalog: &Path,
    host: &str,
) -> anyhow::Result<PathBuf> {
    let catalog = catalog.canonicalize()?;
    let mut digest = Sha256::new();
    digest.update(b"st2.supervision-scope.v1\0");
    digest.update(catalog.as_os_str().as_bytes());
    digest.update(b"\0");
    digest.update(host.as_bytes());
    Ok(state_root
        .join("st2/supervision")
        .join(format!("{:x}", digest.finalize()))
        .join("sender"))
}

fn open_plan(path: &Path) -> anyhow::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    anyhow::ensure!(file.metadata()?.is_file(), "path is not a regular file");
    Ok(file)
}

#[derive(Debug, PartialEq, Eq)]
struct PlanHeader {
    state: Option<Result<String, String>>,
    owner: OwnerField,
}

#[derive(Debug, PartialEq, Eq)]
enum OwnerField {
    Missing,
    Single(String),
    Plural,
    Descriptive,
    Ambiguous,
    Malformed(String),
}

/// Read only the header. Each one-byte read stops at the `### Check` heading newline, so the
/// arbitrary shell line below that heading never enters this process.
fn parse_plan_header(mut reader: impl Read) -> Result<PlanHeader, String> {
    let mut state = None;
    let mut owners = Vec::new();
    let mut line = Vec::new();
    let mut read = 0_usize;
    loop {
        let mut byte = [0_u8; 1];
        match reader.read(&mut byte) {
            Ok(0) => {
                if !line.is_empty() {
                    process_header_line(&line, &mut state, &mut owners)?;
                }
                break;
            }
            Ok(_) => {
                read += 1;
                if read > MAX_PLAN_HEADER_BYTES {
                    return Err("header exceeds the safe scan limit".to_string());
                }
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    if header_line(&line) == "### Check" {
                        break;
                    }
                    process_header_line(&line, &mut state, &mut owners)?;
                    line.clear();
                }
            }
            Err(error) => return Err(format!("cannot read plan header: {error}")),
        }
    }
    let owner = if owners.is_empty() {
        OwnerField::Missing
    } else if owners.len() == 1 {
        owners.pop().expect("one owner field")
    } else {
        OwnerField::Ambiguous
    };
    Ok(PlanHeader { state, owner })
}

fn header_line(raw: &[u8]) -> &str {
    std::str::from_utf8(raw)
        .unwrap_or("")
        .trim_end_matches(['\r', '\n'])
}

fn process_header_line(
    raw: &[u8],
    state: &mut Option<Result<String, String>>,
    owners: &mut Vec<OwnerField>,
) -> Result<(), String> {
    let line = std::str::from_utf8(raw).map_err(|_| "plan header is not UTF-8".to_string())?;
    if state.is_none()
        && let Some(after) = line.split_once("**State:**").map(|(_, after)| after)
    {
        let word = after
            .trim_start()
            .chars()
            .take_while(char::is_ascii_uppercase)
            .collect::<String>();
        *state = Some(
            if matches!(
                word.as_str(),
                "OPEN" | "BLOCKED" | "FROZEN" | "DONE" | "DEAD"
            ) {
                Ok(word)
            } else {
                Err(word)
            },
        );
    }
    if line.contains("**Owners:**") {
        owners.push(OwnerField::Plural);
    }
    if let Some((_, after)) = line.split_once("**Owner:**") {
        owners.push(parse_owner(after));
    }
    Ok(())
}

fn parse_owner(after: &str) -> OwnerField {
    let Some(open) = after.find('`') else {
        return OwnerField::Malformed("no backticked identity".to_string());
    };
    let after_open = &after[open + 1..];
    let Some(close) = after_open.find('`') else {
        return OwnerField::Malformed("unterminated backticked identity".to_string());
    };
    let identity = &after_open[..close];
    if identity.is_empty() || identity.chars().any(char::is_whitespace) {
        return OwnerField::Malformed("invalid backticked identity".to_string());
    }
    let remainder = &after_open[close + 1..];
    let Some(field_at) = next_field(remainder) else {
        return OwnerField::Descriptive;
    };
    let qualifier = remainder[..field_at].trim();
    if !qualifier.is_empty()
        && !(qualifier.starts_with('(')
            && qualifier.ends_with(')')
            && !qualifier[1..qualifier.len() - 1].contains(['(', ')', '`']))
    {
        return OwnerField::Descriptive;
    }
    OwnerField::Single(identity.to_string())
}

fn next_field(value: &str) -> Option<usize> {
    value.match_indices("**").find_map(|(index, _)| {
        let tail = &value[index + 2..];
        let end = tail.find(":**")?;
        let name = &tail[..end];
        (!name.is_empty()
            && name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == ' '))
        .then_some(index)
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io::Cursor;
    use std::os::unix::ffi::OsStringExt as _;
    use std::time::{SystemTime, UNIX_EPOCH};

    use agent_spec::spec::{ClaudeDriver, CodexDriver};

    use super::*;

    fn claude_driver(dev_channels: bool) -> Driver {
        Driver::Claude(ClaudeDriver {
            model: None,
            effort: None,
            dev_channels,
            prompt: "boot".to_string(),
            args: Vec::new(),
        })
    }

    fn codex_driver() -> Driver {
        Driver::Codex(CodexDriver {
            model: None,
            effort: None,
            prompt: "boot".to_string(),
            args: Vec::new(),
        })
    }

    #[test]
    fn known_gates_require_the_full_screen_and_default_selection() {
        let claude = r#"
  WARNING: Loading development channels

  --dangerously-load-development-channels is for local channel development
  only. Do not use this option to run channels you have downloaded off the
  internet.

  Please use --channels to run a list of approved channels.
  Channels: server:st2
  ❯ 1. I am using this for local development
    2. Exit
"#;
        assert_eq!(
            match_gate(Some(&claude_driver(true)), claude),
            Some(KnownGate::ClaudeDevelopmentChannel)
        );
        assert_eq!(
            KnownGate::ClaudeDevelopmentChannel.keys(),
            [PtyKey::Return, PtyKey::Return]
        );
        assert_eq!(match_gate(Some(&claude_driver(false)), claude), None);
        assert_eq!(
            match_gate(
                Some(&claude_driver(true)),
                &claude
                    .replace("❯ 1.", "  1.")
                    .replace("  2. Exit", "❯ 2. Exit")
            ),
            None
        );

        let codex = r#"
  Hooks need review
  1 hook is new or changed.
  Hooks can run outside the sandbox after you trust them.

› 1. Review hooks
  2. Trust all and continue
  3. Continue without trusting (hooks won't run)
"#;
        assert_eq!(
            match_gate(Some(&codex_driver()), codex),
            Some(KnownGate::CodexHookTrust)
        );
        assert_eq!(
            KnownGate::CodexHookTrust.keys(),
            [PtyKey::Down, PtyKey::Return]
        );
        assert_eq!(
            match_gate(
                Some(&codex_driver()),
                &codex
                    .replace("› 1.", "  1.")
                    .replace("  2. Trust", "› 2. Trust")
            ),
            None
        );
    }

    #[test]
    fn plan_header_stops_before_the_arbitrary_check_command() {
        let raw = b"# Plan\n**Owner:** `host.worker` **Opened:** 2026-08-26 **State:** OPEN\n\
### Check\n    touch /tmp/must-not-be-read\nmore secret shell\n";
        let mut reader = Cursor::new(raw.as_slice());
        let header = parse_plan_header(&mut reader).unwrap();
        assert_eq!(header.state, Some(Ok("OPEN".to_string())));
        assert_eq!(header.owner, OwnerField::Single("host.worker".to_string()));
        let check_heading_end = raw
            .windows(b"### Check\n".len())
            .position(|window| window == b"### Check\n")
            .unwrap()
            + b"### Check\n".len();
        assert_eq!(reader.position() as usize, check_heading_end);
    }

    #[test]
    fn owner_parser_refuses_plural_prose_and_ambiguous_headers() {
        let cases = [
            (
                "**Owner:** `host.worker` **Opened:** today **State:** OPEN\n### Check\n",
                OwnerField::Single("host.worker".to_string()),
            ),
            (
                "**Owner:** `host.worker` (after PR 1) **Opened:** today\n**State:** OPEN\n### Check\n",
                OwnerField::Single("host.worker".to_string()),
            ),
            (
                "**Owner:** `host.cos` sequences it. Each phase has an owner.\n**State:** OPEN\n### Check\n",
                OwnerField::Descriptive,
            ),
            (
                "**Owners:** `host.one` and `host.two`\n**State:** OPEN\n### Check\n",
                OwnerField::Plural,
            ),
            ("**State:** DEAD\n### Check\n", OwnerField::Missing),
            (
                "**Owner:** `host.one` **Opened:** today\n**Owner:** `host.two` **Opened:** today\n**State:** OPEN\n### Check\n",
                OwnerField::Ambiguous,
            ),
        ];
        for (raw, expected) in cases {
            assert_eq!(
                parse_plan_header(raw.as_bytes()).unwrap().owner,
                expected,
                "{raw}"
            );
        }
    }

    #[derive(Default)]
    struct QuietRunner {
        screen: RefCell<Option<String>>,
        sent: RefCell<Vec<Vec<PtyKey>>>,
    }

    impl Runner for QuietRunner {
        fn list_sessions(&self) -> anyhow::Result<Vec<crate::Session>> {
            Ok(Vec::new())
        }

        fn spawn(&self, _target: &crate::TaskTarget, _spec_dir: &Path) -> anyhow::Result<()> {
            Ok(())
        }

        fn peek_plain(&self, _pty_id: &str) -> anyhow::Result<Option<String>> {
            Ok(self.screen.borrow().clone())
        }

        fn send_keys(&self, _pty_id: &str, keys: &[PtyKey]) -> anyhow::Result<()> {
            self.sent.borrow_mut().push(keys.to_vec());
            *self.screen.borrow_mut() = Some("ready".to_string());
            Ok(())
        }

        fn kill(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove(&self, _pty_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn unix_nanos(time: SystemTime) -> u128 {
        time.duration_since(UNIX_EPOCH).unwrap().as_nanos()
    }

    fn claude_project_key(workspace: &Path) -> String {
        workspace
            .to_str()
            .unwrap()
            .chars()
            .map(|character| match character {
                '/' | '.' => '-',
                other => other,
            })
            .collect()
    }

    #[test]
    fn supervision_clears_a_matched_claude_gate_and_verifies_the_screen() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let workspace = tmp.path().join("workspace");
        let agent_dir = catalog.join("agents/host/worker");
        fs::create_dir_all(&agent_dir).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            agent_dir.join("agent.kdl"),
            format!(
                r#"agent "worker" {{
  host "host"
  workspace "{}"
  claude {{ dev-channels #true; prompt "boot" }}
}}
"#,
                workspace.display()
            ),
        )
        .unwrap();
        let mut specs = crate::discover(&catalog).specs;
        let context = crate::reconcile::TaskCompileContext::current(catalog.clone()).unwrap();
        crate::reconcile::compile_generated_tasks(&mut specs, "host", &context).unwrap();
        let runner = QuietRunner {
            screen: RefCell::new(Some(
                r#"WARNING: Loading development channels
--dangerously-load-development-channels is for local channel development only.
Do not use this option to run channels you have downloaded off the internet.
Please use --channels to run a list of approved channels.
❯ 1. I am using this for local development
  2. Exit
"#
                .to_string(),
            )),
            ..Default::default()
        };
        let report = reconcile_at(
            &catalog,
            "host",
            &specs,
            &[crate::Session {
                pty_id: "host.worker".to_string(),
                alive: true,
                exit_code: None,
                presentation: None,
            }],
            &runner,
            &tmp.path().join("state"),
            Some(&tmp.path().join("home")),
            u128::from(message::now_ms()) * 1_000_000,
        );
        assert_eq!(
            runner.sent.borrow().as_slice(),
            &[vec![PtyKey::Return, PtyKey::Return]]
        );
        assert_eq!(report.gates_cleared.len(), 1, "{report:?}");
        assert!(report.errors.is_empty(), "{report:?}");
    }

    #[test]
    fn plan_scan_reports_each_refusal_reason() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let plans = catalog.join("plans");
        let remote = catalog.join("agents/other/worker");
        fs::create_dir_all(&plans).unwrap();
        fs::create_dir_all(&remote).unwrap();
        fs::write(
            remote.join("agent.kdl"),
            "agent \"worker\" { host \"other\"; command \"true\" }\n",
        )
        .unwrap();
        let check = "\n### Check\n    must-not-be-read\n";
        let fixtures = [
            (
                "blocked.md",
                "**Owner:** `other.worker` **Opened:** today **State:** BLOCKED",
            ),
            (
                "plural.md",
                "**Owners:** `other.one` and `other.two` **State:** OPEN",
            ),
            (
                "prose.md",
                "**Owner:** `other.worker` sequences it. **State:** OPEN",
            ),
            ("ownerless.md", "**State:** OPEN"),
            (
                "remote.md",
                "**Owner:** `other.worker` **Opened:** today **State:** OPEN",
            ),
        ];
        for (name, header) in fixtures {
            fs::write(plans.join(name), format!("{header}{check}")).unwrap();
        }
        fs::write(
            plans.join(std::ffi::OsString::from_vec(b"non-utf8-\xff.md".to_vec())),
            format!("**State:** OPEN{check}"),
        )
        .unwrap();
        fs::write(plans.join("README.md"), "not a plan").unwrap();
        let specs = crate::discover(&catalog).specs;
        let report = reconcile_at(
            &catalog,
            "host",
            &specs,
            &[],
            &QuietRunner::default(),
            &tmp.path().join("state"),
            Some(&tmp.path().join("home")),
            u128::from(message::now_ms()) * 1_000_000,
        );
        assert_eq!(report.plans_skipped.len(), 6, "{report:?}");
        for (name, reason) in [
            ("blocked.md", "State is BLOCKED"),
            ("plural.md", "plural Owners"),
            ("prose.md", "descriptive prose"),
            ("ownerless.md", "no Owner"),
            ("remote.md", "remote host"),
        ] {
            assert!(
                report
                    .plans_skipped
                    .iter()
                    .any(|line| line.contains(name) && line.contains(reason)),
                "{name}: {report:?}"
            );
        }
        assert!(
            report
                .plans_skipped
                .iter()
                .any(|line| line.contains("filename is not UTF-8")),
            "{report:?}"
        );
    }

    #[test]
    fn idle_plan_nudge_is_durable_and_new_activity_opens_a_new_period() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("catalog");
        let workspace = tmp.path().join("workspace");
        let home = tmp.path().join("home");
        let state = tmp.path().join("state");
        let agent_dir = catalog.join("agents/host/worker");
        fs::create_dir_all(&agent_dir).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            agent_dir.join("agent.kdl"),
            format!(
                r#"agent "worker" {{
  host "host"
  workspace "{}"
  claude {{ prompt "boot" }}
}}
"#,
                workspace.display()
            ),
        )
        .unwrap();
        fs::create_dir_all(catalog.join("plans")).unwrap();
        let sentinel = tmp.path().join("check-ran");
        fs::write(
            catalog.join("plans/work.md"),
            format!(
                "**Owner:** `host.worker` **Opened:** today **State:** OPEN\n\n### Check\n    touch {}\n",
                sentinel.display()
            ),
        )
        .unwrap();

        let mut specs = crate::discover(&catalog).specs;
        let context = crate::reconcile::TaskCompileContext::current(catalog.clone()).unwrap();
        crate::reconcile::compile_generated_tasks(&mut specs, "host", &context).unwrap();
        let session = crate::Session {
            pty_id: "host.worker".to_string(),
            alive: true,
            exit_code: None,
            presentation: None,
        };

        let session_dir = home
            .join(".claude/projects")
            .join(claude_project_key(&workspace.canonicalize().unwrap()));
        fs::create_dir_all(&session_dir).unwrap();
        let session_file = session_dir.join("session-1.jsonl");
        fs::write(
            &session_file,
            b"{\"type\":\"assistant\",\"sessionId\":\"session-1\",\"timestamp\":\"2026-08-26T00:00:00.000Z\"}\n",
        )
        .unwrap();
        let now = SystemTime::now();
        let first_activity = now - Duration::from_secs(2 * 60 * 60);
        File::options()
            .write(true)
            .open(&session_file)
            .unwrap()
            .set_times(
                fs::FileTimes::new()
                    .set_accessed(first_activity)
                    .set_modified(first_activity),
            )
            .unwrap();

        let runner = QuietRunner::default();
        let first = reconcile_at(
            &catalog,
            "host",
            &specs,
            std::slice::from_ref(&session),
            &runner,
            &state,
            Some(&home),
            unix_nanos(now),
        );
        assert_eq!(first.plans_nudged.len(), 1, "{first:?}");
        assert!(first.errors.is_empty(), "{first:?}");
        assert!(!sentinel.exists(), "the plan Check command must never run");
        let inbox = message::list_inbox(&message::inbox_dir(&agent_dir)).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].from.as_deref(), Some("st2.host"));
        assert_eq!(inbox[0].subject.as_deref(), Some("OPEN plan: work.md"));
        assert_eq!(inbox[0].body, "Owner idle: 120 minutes.\n");

        let second = reconcile_at(
            &catalog,
            "host",
            &specs,
            std::slice::from_ref(&session),
            &runner,
            &state,
            Some(&home),
            unix_nanos(now + Duration::from_secs(5 * 60)),
        );
        assert!(second.plans_nudged.is_empty(), "{second:?}");
        assert!(
            second
                .plans_skipped
                .iter()
                .any(|line| line.contains("nudge already exists")),
            "{second:?}"
        );
        assert_eq!(
            message::list_inbox(&message::inbox_dir(&agent_dir))
                .unwrap()
                .len(),
            1
        );

        let next_activity = first_activity + Duration::from_secs(30 * 60);
        File::options()
            .write(true)
            .open(&session_file)
            .unwrap()
            .set_times(
                fs::FileTimes::new()
                    .set_accessed(next_activity)
                    .set_modified(next_activity),
            )
            .unwrap();
        let third = reconcile_at(
            &catalog,
            "host",
            &specs,
            &[session],
            &runner,
            &state,
            Some(&home),
            unix_nanos(now),
        );
        assert_eq!(third.plans_nudged.len(), 1, "{third:?}");
        assert_eq!(
            message::list_inbox(&message::inbox_dir(&agent_dir))
                .unwrap()
                .len(),
            2
        );
    }
}
