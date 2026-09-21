use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead as _, BufReader, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
#[cfg(not(target_os = "linux"))]
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
#[cfg(not(target_os = "linux"))]
use chrono::NaiveDateTime;
use chrono::{DateTime, Utc};
use kdl::{KdlDocument, KdlEntry, KdlNode};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use walkdir::WalkDir;

const MAX_DISCOVERED_FILES: usize = 10_000;
const MAX_EXPOSED_HISTORY: usize = 2_000;
const MAX_METADATA_LINES: usize = 64;
const MAX_TIMELINE_LINES: usize = 4_096;
const MAX_TIMELINE_BYTES: u64 = 32 * 1024 * 1024;
// A maximum-size page must remain below the client gateway's one-megabyte response ceiling even
// when every native entry contains a large tool payload.
const MAX_TIMELINE_VALUE_BYTES: usize = 8 * 1024;
const DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ExternalDriver {
    Codex,
    Claude,
    Omp,
}

impl ExternalDriver {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Omp => "omp",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ExternalProcess {
    pub(crate) pid: u32,
    pub(crate) parent_pid: u32,
    pub(crate) started_at_unix_ms: u128,
    pub(crate) fingerprint: String,
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) command: String,
    pub(crate) exact_session: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ExternalSession {
    pub(crate) id: String,
    pub(crate) revision: String,
    pub(crate) driver: ExternalDriver,
    pub(crate) native_id: String,
    pub(crate) transcript: PathBuf,
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) title: Option<String>,
    pub(crate) started_at_unix_ms: u128,
    pub(crate) updated_at_unix_ms: u128,
    pub(crate) process: Option<ExternalProcess>,
}

#[derive(Clone, Debug)]
pub(crate) struct UnresolvedProcess {
    pub(crate) id: String,
    pub(crate) revision: String,
    pub(crate) driver: ExternalDriver,
    pub(crate) process: ExternalProcess,
}

#[derive(Clone, Default)]
pub(crate) struct ExternalDiscovery {
    pub(crate) sessions: Vec<ExternalSession>,
    pub(crate) unresolved_processes: Vec<UnresolvedProcess>,
}

#[derive(Clone)]
struct SessionMetadata {
    driver: ExternalDriver,
    native_id: String,
    transcript: PathBuf,
    cwd: Option<PathBuf>,
    title: Option<String>,
    started_at_unix_ms: u128,
    updated_at_unix_ms: u128,
    revision: String,
}

pub(crate) fn discover(home: Option<&Path>, include_history: bool) -> Result<ExternalDiscovery> {
    let Some(home) = home else {
        return Ok(ExternalDiscovery::default());
    };
    static CACHE: OnceLock<Mutex<Option<(PathBuf, Instant, ExternalDiscovery)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(None));
    let mut cache = cache.lock().expect("external session cache mutex poisoned");
    if let Some((cached_home, created, discovery)) = cache.as_ref()
        && cached_home == home
        && created.elapsed() <= DISCOVERY_CACHE_TTL
    {
        return Ok(filter_discovery(discovery.clone(), include_history));
    }
    let discovery = discover_uncached(home)?;
    *cache = Some((home.to_owned(), Instant::now(), discovery.clone()));
    Ok(filter_discovery(discovery, include_history))
}

pub(crate) fn discover_fresh(
    home: Option<&Path>,
    include_history: bool,
) -> Result<ExternalDiscovery> {
    let Some(home) = home else {
        return Ok(ExternalDiscovery::default());
    };
    Ok(filter_discovery(discover_uncached(home)?, include_history))
}

fn discover_uncached(home: &Path) -> Result<ExternalDiscovery> {
    let mut metadata = discover_files(home)?;
    metadata.sort_by(|left, right| {
        right
            .updated_at_unix_ms
            .cmp(&left.updated_at_unix_ms)
            .then_with(|| left.native_id.cmp(&right.native_id))
    });
    metadata.truncate(MAX_EXPOSED_HISTORY);

    let candidates = platform_processes()?;
    let roots = root_processes(&candidates)
        .into_iter()
        .filter(|candidate| !candidate.managed_by_st3)
        .collect::<Vec<_>>();
    let candidate_by_pid = candidates
        .iter()
        .map(|candidate| (candidate.process.pid, candidate))
        .collect::<BTreeMap<_, _>>();
    let mut matched_roots = BTreeSet::new();
    let mut sessions = Vec::new();
    for item in metadata {
        let process = candidates.iter().find_map(|candidate| {
            if candidate.driver != item.driver
                || !(candidate.process.command.contains(&item.native_id)
                    || candidate
                        .process
                        .command
                        .contains(item.transcript.to_string_lossy().as_ref()))
            {
                return None;
            }
            let root = process_root(candidate, &candidate_by_pid);
            if root.managed_by_st3 {
                return None;
            }
            matched_roots.insert(root.process.pid);
            let mut process = root.process.clone();
            process.exact_session = true;
            Some(process)
        });
        let id = external_session_id(item.driver, &item.native_id);
        sessions.push(ExternalSession {
            id,
            revision: item.revision,
            driver: item.driver,
            native_id: item.native_id,
            transcript: item.transcript,
            cwd: item.cwd,
            title: item.title,
            started_at_unix_ms: item.started_at_unix_ms,
            updated_at_unix_ms: item.updated_at_unix_ms,
            process,
        });
    }
    let unresolved_processes = roots
        .into_iter()
        .filter(|process| !matched_roots.contains(&process.process.pid))
        .map(|process| unresolved_process(process.driver, process.process))
        .collect();
    Ok(ExternalDiscovery {
        sessions,
        unresolved_processes,
    })
}

fn filter_discovery(mut discovery: ExternalDiscovery, include_history: bool) -> ExternalDiscovery {
    if !include_history {
        discovery
            .sessions
            .retain(|session| session.process.is_some());
    }
    discovery
}

pub(crate) fn find(home: Option<&Path>, id: &str) -> Result<Option<ExternalSession>> {
    Ok(discover(home, true)?
        .sessions
        .into_iter()
        .find(|item| item.id == id))
}

pub(crate) fn find_fresh(home: Option<&Path>, id: &str) -> Result<Option<ExternalSession>> {
    Ok(discover_fresh(home, true)?
        .sessions
        .into_iter()
        .find(|item| item.id == id))
}

pub(crate) fn timestamp(unix_ms: u128) -> String {
    let millis = i64::try_from(unix_ms).unwrap_or(i64::MAX);
    DateTime::<Utc>::from_timestamp_millis(millis)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(crate) fn normalized_timeline(session: &ExternalSession) -> Result<Vec<Value>> {
    let metadata = fs::metadata(&session.transcript)
        .with_context(|| format!("inspect transcript {}", session.transcript.display()))?;
    let mut file = File::open(&session.transcript)
        .with_context(|| format!("read transcript {}", session.transcript.display()))?;
    let start = metadata.len().saturating_sub(MAX_TIMELINE_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file);
    if start != 0 {
        let mut partial = String::new();
        reader.read_line(&mut partial)?;
    }
    let mut lines = VecDeque::new();
    for line in reader.lines() {
        if lines.len() == MAX_TIMELINE_LINES {
            lines.pop_front();
        }
        lines.push_back(line?);
    }
    let mut items = Vec::new();
    if start != 0 || lines.len() == MAX_TIMELINE_LINES {
        items.push(timeline_item(
            0,
            &timestamp(session.updated_at_unix_ms),
            "system",
            "truncation",
            json!({
                "reason": "the native transcript prefix is outside the bounded read window",
                "omitted_from_sequence": 0,
                "omitted_to_sequence": 0
            }),
        ));
    }
    for (line_index, line) in lines.into_iter().enumerate() {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let sequence = ((line_index as u64).saturating_add(1)).saturating_mul(16);
        match session.driver {
            ExternalDriver::Codex => normalize_codex(&value, sequence, session, &mut items),
            ExternalDriver::Claude => normalize_claude(&value, sequence, session, &mut items),
            ExternalDriver::Omp => normalize_omp(&value, sequence, session, &mut items),
        }
    }
    items.sort_by_key(|item| item["sequence"].as_u64().unwrap_or(u64::MAX));
    Ok(items)
}

pub(crate) struct ImportMission {
    pub(crate) id: String,
    pub(crate) workspace: PathBuf,
    pub(crate) kdl: String,
}

pub(crate) fn import_mission(session: &ExternalSession) -> Result<ImportMission> {
    let workspace = session
        .cwd
        .clone()
        .context("the saved session has no recorded workspace")?;
    anyhow::ensure!(
        workspace.is_dir(),
        "the saved session workspace {} does not exist",
        workspace.display()
    );
    let suffix = &digest(&format!(
        "{}:{}",
        session.driver.as_str(),
        session.native_id
    ))[..24];
    let id = format!("import/{}/{suffix}", session.driver.as_str());
    let mut mission = KdlNode::new("mission");
    mission.entries_mut().push(KdlEntry::new(id.clone()));
    mission
        .entries_mut()
        .push(KdlEntry::new_prop("state", "ready"));
    let mut mission_body = KdlDocument::new();
    mission_body.nodes_mut().push(string_node(
        "goal",
        &format!(
            "Continue the imported {} session {} under durable st3 ownership.",
            session.driver.as_str(),
            session.native_id
        ),
    ));

    let mut agent = KdlNode::new("agent");
    agent.entries_mut().push(KdlEntry::new("session"));
    let mut agent_body = KdlDocument::new();
    agent_body
        .nodes_mut()
        .push(string_node("identity", "session"));
    agent_body.nodes_mut().push(string_node(
        "workspace",
        workspace.to_string_lossy().as_ref(),
    ));
    let mut harness = KdlNode::new("harness");
    harness
        .entries_mut()
        .push(KdlEntry::new(session.driver.as_str()));
    let mut harness_body = KdlDocument::new();
    let mut args = KdlNode::new("args");
    match session.driver {
        ExternalDriver::Codex => {
            args.entries_mut().push(KdlEntry::new("resume"));
            args.entries_mut()
                .push(KdlEntry::new(session.native_id.clone()));
        }
        ExternalDriver::Claude | ExternalDriver::Omp => {
            args.entries_mut().push(KdlEntry::new("--resume"));
            args.entries_mut()
                .push(KdlEntry::new(session.native_id.clone()));
        }
    }
    harness_body.nodes_mut().push(args);
    harness.set_children(harness_body);
    agent_body.nodes_mut().push(harness);
    agent_body
        .nodes_mut()
        .push(string_node("restart", "always"));
    agent.set_children(agent_body);
    mission_body.nodes_mut().push(agent);
    mission.set_children(mission_body);

    let mut document = KdlDocument::new();
    let mut version = KdlNode::new("version");
    version.entries_mut().push(KdlEntry::new(2));
    document.nodes_mut().push(version);
    document.nodes_mut().push(mission);
    document.autoformat();
    Ok(ImportMission {
        id,
        workspace,
        kdl: document.to_string(),
    })
}

pub(crate) fn terminate_exact_process(
    driver: ExternalDriver,
    expected: &ExternalProcess,
) -> Result<()> {
    anyhow::ensure!(
        expected.exact_session,
        "refusing to stop a process without exact native-session evidence"
    );
    let current = platform_processes()?
        .into_iter()
        .find(|candidate| candidate.driver == driver && candidate.process.pid == expected.pid)
        .context("the selected harness process exited before takeover")?;
    anyhow::ensure!(
        current.process.fingerprint == expected.fingerprint,
        "the selected harness process changed before takeover"
    );
    #[cfg(unix)]
    {
        let pid = expected.pid as i32;
        let pgid = unsafe { libc::getpgid(pid) };
        let target = if pgid == pid { -pid } else { pid };
        if unsafe { libc::kill(target, libc::SIGTERM) } != 0 {
            let error = std::io::Error::last_os_error();
            anyhow::ensure!(
                error.raw_os_error() == Some(libc::ESRCH),
                "stop imported harness process {pid}: {error}"
            );
        }
        for _ in 0..100 {
            if !process_is_live(pid) {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if unsafe { libc::kill(target, libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            anyhow::ensure!(
                error.raw_os_error() == Some(libc::ESRCH),
                "kill imported harness process {pid}: {error}"
            );
        }
        Ok(())
    }
    #[cfg(not(unix))]
    anyhow::bail!("session takeover is supported only on Unix hosts")
}

#[cfg(target_os = "linux")]
fn process_is_live(pid: i32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let Some(end_name) = stat.rfind(") ") else {
        return false;
    };
    !matches!(stat[end_name + 2..].chars().next(), Some('Z' | 'X'))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_is_live(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn string_node(name: &str, value: &str) -> KdlNode {
    let mut node = KdlNode::new(name);
    node.entries_mut().push(KdlEntry::new(value));
    node
}

fn discover_files(home: &Path) -> Result<Vec<SessionMetadata>> {
    let roots = [
        (ExternalDriver::Codex, home.join(".codex/sessions")),
        (ExternalDriver::Claude, home.join(".claude/projects")),
        (ExternalDriver::Omp, home.join(".oh-omp/agent/sessions")),
        (ExternalDriver::Omp, home.join(".omp/agent/sessions")),
    ];
    let mut found = Vec::new();
    for (driver, root) in roots {
        if !root.is_dir() {
            continue;
        }
        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == "jsonl")
            })
        {
            if found.len() >= MAX_DISCOVERED_FILES {
                break;
            }
            if let Some(metadata) = read_metadata(driver, entry.path())? {
                found.push(metadata);
            }
        }
    }
    Ok(found)
}

fn read_metadata(driver: ExternalDriver, path: &Path) -> Result<Option<SessionMetadata>> {
    let file_metadata = fs::metadata(path)?;
    let updated_at_unix_ms = system_time_ms(file_metadata.modified().unwrap_or(UNIX_EPOCH));
    let file = File::open(path)?;
    let mut native_id = None;
    let mut cwd = None;
    let mut title = None;
    let mut started_at_unix_ms = None;
    for line in BufReader::new(file).lines().take(MAX_METADATA_LINES) {
        let Ok(value) = serde_json::from_str::<Value>(&line?) else {
            continue;
        };
        match driver {
            ExternalDriver::Codex if value["type"] == "session_meta" => {
                native_id = value
                    .pointer("/payload/id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                cwd = value
                    .pointer("/payload/cwd")
                    .and_then(Value::as_str)
                    .map(PathBuf::from);
                started_at_unix_ms = parse_timestamp(value.get("timestamp"));
                title = value
                    .pointer("/payload/source")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                break;
            }
            ExternalDriver::Claude => {
                native_id = native_id.or_else(|| {
                    value
                        .get("sessionId")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
                cwd = cwd.or_else(|| value.get("cwd").and_then(Value::as_str).map(PathBuf::from));
                started_at_unix_ms =
                    started_at_unix_ms.or_else(|| parse_timestamp(value.get("timestamp")));
                title =
                    title.or_else(|| value.get("slug").and_then(Value::as_str).map(str::to_owned));
            }
            ExternalDriver::Omp if value["type"] == "session" => {
                native_id = value.get("id").and_then(Value::as_str).map(str::to_owned);
                cwd = value.get("cwd").and_then(Value::as_str).map(PathBuf::from);
                started_at_unix_ms = parse_timestamp(value.get("timestamp"));
                title = value
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                break;
            }
            _ => {}
        }
    }
    if driver == ExternalDriver::Claude && native_id.is_none() {
        native_id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .map(str::to_owned);
    }
    let Some(native_id) = native_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let started_at_unix_ms = started_at_unix_ms.unwrap_or(updated_at_unix_ms);
    let revision = digest(&format!(
        "{}:{}:{}:{}",
        driver.as_str(),
        path.display(),
        file_metadata.len(),
        updated_at_unix_ms
    ));
    Ok(Some(SessionMetadata {
        driver,
        native_id,
        transcript: path.to_owned(),
        cwd,
        title,
        started_at_unix_ms,
        updated_at_unix_ms,
        revision,
    }))
}

#[derive(Clone)]
struct ProcessCandidate {
    driver: ExternalDriver,
    process: ExternalProcess,
    managed_by_st3: bool,
}

fn root_processes(candidates: &[ProcessCandidate]) -> Vec<ProcessCandidate> {
    let parents = candidates
        .iter()
        .map(|candidate| (candidate.process.pid, candidate.driver))
        .collect::<BTreeMap<_, _>>();
    candidates
        .iter()
        .filter(|candidate| parents.get(&candidate.process.parent_pid) != Some(&candidate.driver))
        .cloned()
        .collect()
}

fn process_root<'a>(
    candidate: &'a ProcessCandidate,
    candidates: &BTreeMap<u32, &'a ProcessCandidate>,
) -> &'a ProcessCandidate {
    let mut current = candidate;
    while let Some(parent) = candidates.get(&current.process.parent_pid)
        && parent.driver == current.driver
    {
        current = parent;
    }
    current
}

fn unresolved_process(driver: ExternalDriver, process: ExternalProcess) -> UnresolvedProcess {
    let fingerprint = process.fingerprint.clone();
    let id = format!(
        "session/external-process-{}",
        &digest(&format!("{}:{fingerprint}", driver.as_str()))[..24]
    );
    UnresolvedProcess {
        id,
        revision: digest(&fingerprint),
        driver,
        process,
    }
}

fn platform_processes() -> Result<Vec<ProcessCandidate>> {
    #[cfg(target_os = "linux")]
    {
        linux_processes()
    }
    #[cfg(not(target_os = "linux"))]
    {
        ps_processes()
    }
}

#[cfg(target_os = "linux")]
fn linux_processes() -> Result<Vec<ProcessCandidate>> {
    let boot_seconds = fs::read_to_string("/proc/stat")?
        .lines()
        .find_map(|line| line.strip_prefix("btime "))
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or_default();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u128;
    let mut found = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        let bytes = match fs::read(entry.path().join("cmdline")) {
            Ok(bytes) if !bytes.is_empty() => bytes,
            _ => continue,
        };
        let command = String::from_utf8_lossy(&bytes).replace('\0', " ");
        let Some(driver) = driver_for_command(&command) else {
            continue;
        };
        let stat = fs::read_to_string(entry.path().join("stat")).unwrap_or_default();
        let Some(end_name) = stat.rfind(") ") else {
            continue;
        };
        let fields = stat[end_name + 2..].split_whitespace().collect::<Vec<_>>();
        let parent_pid = fields
            .get(1)
            .and_then(|value| value.parse().ok())
            .unwrap_or_default();
        let start_ticks = fields
            .get(19)
            .and_then(|value| value.parse::<u128>().ok())
            .unwrap_or_default();
        let started_at_unix_ms = boot_seconds
            .saturating_mul(1_000)
            .saturating_add(start_ticks.saturating_mul(1_000) / ticks);
        let cwd = fs::read_link(entry.path().join("cwd")).ok();
        let fingerprint = process_fingerprint(pid, started_at_unix_ms, &command);
        found.push(ProcessCandidate {
            driver,
            managed_by_st3: is_st3_driver(&command),
            process: ExternalProcess {
                pid,
                parent_pid,
                started_at_unix_ms,
                fingerprint,
                cwd,
                command,
                exact_session: false,
            },
        });
    }
    Ok(found)
}

#[cfg(not(target_os = "linux"))]
fn ps_processes() -> Result<Vec<ProcessCandidate>> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,lstart=,command="])
        .output()
        .context("list local harness processes")?;
    anyhow::ensure!(
        output.status.success(),
        "ps failed while listing harness processes"
    );
    let mut found = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 8 {
            continue;
        }
        let Ok(pid) = fields[0].parse::<u32>() else {
            continue;
        };
        let parent_pid = fields[1].parse::<u32>().unwrap_or_default();
        let date = fields[2..7].join(" ");
        let started_at_unix_ms = NaiveDateTime::parse_from_str(&date, "%a %b %e %H:%M:%S %Y")
            .map(|value| value.and_utc().timestamp_millis().max(0) as u128)
            .unwrap_or_default();
        let command = fields[7..].join(" ");
        let Some(driver) = driver_for_command(&command) else {
            continue;
        };
        let cwd = process_cwd_from_lsof(pid);
        let fingerprint = process_fingerprint(pid, started_at_unix_ms, &command);
        found.push(ProcessCandidate {
            driver,
            managed_by_st3: is_st3_driver(&command),
            process: ExternalProcess {
                pid,
                parent_pid,
                started_at_unix_ms,
                fingerprint,
                cwd,
                command,
                exact_session: false,
            },
        });
    }
    Ok(found)
}

#[cfg(not(target_os = "linux"))]
fn process_cwd_from_lsof(pid: u32) -> Option<PathBuf> {
    let output = Command::new("lsof")
        .args(["-a", "-p", &pid.to_string(), "-d", "cwd", "-Fn"])
        .output()
        .ok()?;
    output.status.success().then_some(())?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix('n'))
        .map(PathBuf::from)
}

fn driver_for_command(command: &str) -> Option<ExternalDriver> {
    let tokens = command.split_whitespace().collect::<Vec<_>>();
    for driver in [
        ExternalDriver::Codex,
        ExternalDriver::Claude,
        ExternalDriver::Omp,
    ] {
        let name = driver.as_str();
        if tokens.first().and_then(|value| command_basename(value)) == Some(name)
            || matches!(
                tokens.first().and_then(|value| command_basename(value)),
                Some("node" | "bun")
            ) && tokens.get(1).and_then(|value| command_basename(value)) == Some(name)
            || matches!(
                tokens.first().and_then(|value| command_basename(value)),
                Some("st2" | "st3")
            ) && tokens.windows(2).any(|pair| pair == ["driver", name])
        {
            return Some(driver);
        }
    }
    None
}

fn command_basename(value: &str) -> Option<&str> {
    Path::new(value).file_name().and_then(|name| name.to_str())
}

fn is_st3_driver(command: &str) -> bool {
    let tokens = command.split_whitespace().collect::<Vec<_>>();
    tokens.first().and_then(|value| command_basename(value)) == Some("st3")
        && tokens
            .windows(2)
            .any(|pair| pair.first() == Some(&"driver"))
}

fn process_fingerprint(pid: u32, started_at_unix_ms: u128, command: &str) -> String {
    format!("{pid}:{started_at_unix_ms}:{}", digest(command))
}

fn external_session_id(driver: ExternalDriver, native_id: &str) -> String {
    format!(
        "session/external-{}",
        &digest(&format!("{}:{native_id}", driver.as_str()))[..24]
    )
}

fn normalize_codex(
    value: &Value,
    sequence: u64,
    session: &ExternalSession,
    items: &mut Vec<Value>,
) {
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| timestamp(session.updated_at_unix_ms));
    let payload = &value["payload"];
    if value["type"] != "response_item" {
        return;
    }
    match payload["type"].as_str() {
        Some("message") => {
            let role = normalized_role(payload["role"].as_str());
            let message_id = payload["id"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("native/{sequence}"));
            push_message(items, sequence, &timestamp, role, &message_id);
            if let Some(content) = payload["content"].as_array() {
                for (offset, part) in content.iter().enumerate() {
                    if let Some(text) = part
                        .get("text")
                        .or_else(|| part.get("input_text"))
                        .or_else(|| part.get("output_text"))
                        .and_then(Value::as_str)
                    {
                        push_content(items, sequence + 1 + offset as u64, &timestamp, role, text);
                    }
                }
            }
        }
        Some("function_call") => push_tool_call(
            items,
            sequence,
            &timestamp,
            payload["call_id"].as_str().unwrap_or("native-call"),
            payload["name"].as_str().unwrap_or("tool"),
            payload
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({})),
        ),
        Some("function_call_output") => push_tool_result(
            items,
            sequence,
            &timestamp,
            payload["call_id"].as_str().unwrap_or("native-call"),
            payload.get("output").cloned().unwrap_or(Value::Null),
        ),
        _ => {}
    }
}

fn normalize_claude(
    value: &Value,
    sequence: u64,
    session: &ExternalSession,
    items: &mut Vec<Value>,
) {
    let Some(kind @ ("user" | "assistant")) = value["type"].as_str() else {
        return;
    };
    let role = if kind == "user" { "user" } else { "assistant" };
    let timestamp = value["timestamp"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| timestamp(session.updated_at_unix_ms));
    let message_id = value
        .pointer("/message/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("uuid").and_then(Value::as_str))
        .map(str::to_owned)
        .unwrap_or_else(|| format!("native/{sequence}"));
    push_message(items, sequence, &timestamp, role, &message_id);
    match &value["message"]["content"] {
        Value::String(text) => push_content(items, sequence + 1, &timestamp, role, text),
        Value::Array(parts) => {
            for (offset, part) in parts.iter().enumerate() {
                let item_sequence = sequence + 1 + offset as u64;
                match part["type"].as_str() {
                    Some("text") => {
                        if let Some(text) = part["text"].as_str() {
                            push_content(items, item_sequence, &timestamp, role, text);
                        }
                    }
                    Some("tool_use") => push_tool_call(
                        items,
                        item_sequence,
                        &timestamp,
                        part["id"].as_str().unwrap_or("native-call"),
                        part["name"].as_str().unwrap_or("tool"),
                        part.get("input").cloned().unwrap_or_else(|| json!({})),
                    ),
                    Some("tool_result") => push_tool_result(
                        items,
                        item_sequence,
                        &timestamp,
                        part["tool_use_id"].as_str().unwrap_or("native-call"),
                        part.get("content").cloned().unwrap_or(Value::Null),
                    ),
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn normalize_omp(value: &Value, sequence: u64, session: &ExternalSession, items: &mut Vec<Value>) {
    if value["type"] != "message" {
        return;
    }
    let message = &value["message"];
    let role = normalized_role(message["role"].as_str());
    let timestamp = value["timestamp"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| timestamp(session.updated_at_unix_ms));
    let message_id = value["id"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("native/{sequence}"));
    push_message(items, sequence, &timestamp, role, &message_id);
    match &message["content"] {
        Value::String(text) => push_content(items, sequence + 1, &timestamp, role, text),
        Value::Array(parts) => {
            for (offset, part) in parts.iter().enumerate() {
                let item_sequence = sequence + 1 + offset as u64;
                match part["type"].as_str() {
                    Some("text") => {
                        if let Some(text) = part["text"].as_str() {
                            push_content(items, item_sequence, &timestamp, role, text);
                        }
                    }
                    Some("toolCall" | "tool_call") => push_tool_call(
                        items,
                        item_sequence,
                        &timestamp,
                        part.get("id")
                            .or_else(|| part.get("toolCallId"))
                            .and_then(Value::as_str)
                            .unwrap_or("native-call"),
                        part.get("name").and_then(Value::as_str).unwrap_or("tool"),
                        part.get("arguments").cloned().unwrap_or_else(|| json!({})),
                    ),
                    Some("toolResult" | "tool_result") => push_tool_result(
                        items,
                        item_sequence,
                        &timestamp,
                        part.get("toolCallId")
                            .or_else(|| part.get("call_id"))
                            .and_then(Value::as_str)
                            .unwrap_or("native-call"),
                        part.get("content").cloned().unwrap_or(Value::Null),
                    ),
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn normalized_role(role: Option<&str>) -> &'static str {
    match role {
        Some("user") => "user",
        Some("assistant") => "assistant",
        Some("tool") => "tool",
        _ => "system",
    }
}

fn push_message(
    items: &mut Vec<Value>,
    sequence: u64,
    timestamp: &str,
    role: &str,
    message_id: &str,
) {
    items.push(timeline_item(
        sequence,
        timestamp,
        role,
        "message",
        json!({"message_id": message_id}),
    ));
}

fn push_content(items: &mut Vec<Value>, sequence: u64, timestamp: &str, role: &str, text: &str) {
    let text = bounded_text(text);
    items.push(timeline_item(
        sequence,
        timestamp,
        role,
        "content",
        json!({"media_type":"text/plain", "text":text}),
    ));
}

fn push_tool_call(
    items: &mut Vec<Value>,
    sequence: u64,
    timestamp: &str,
    call_id: &str,
    name: &str,
    arguments: Value,
) {
    let arguments = arguments
        .as_str()
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or(arguments);
    let arguments = bounded_value(arguments);
    items.push(timeline_item(
        sequence,
        timestamp,
        "assistant",
        "tool_call",
        json!({"call_id":call_id, "name":name, "arguments":arguments}),
    ));
}

fn push_tool_result(
    items: &mut Vec<Value>,
    sequence: u64,
    timestamp: &str,
    call_id: &str,
    content: Value,
) {
    let content = bounded_value(content);
    items.push(timeline_item(sequence, timestamp, "tool", "tool_result", json!({"call_id":call_id, "status":"success", "media_type":"application/json", "content":content})));
}

fn bounded_text(value: &str) -> String {
    if value.len() <= MAX_TIMELINE_VALUE_BYTES {
        return value.to_owned();
    }
    let mut output = value
        .chars()
        .take(MAX_TIMELINE_VALUE_BYTES)
        .collect::<String>();
    output.push_str("\n[st3 truncated this native timeline value]");
    output
}

fn bounded_value(value: Value) -> Value {
    match serde_json::to_string(&value) {
        Ok(encoded) if encoded.len() > MAX_TIMELINE_VALUE_BYTES => {
            Value::String(bounded_text(&encoded))
        }
        _ => value,
    }
}

fn timeline_item(
    sequence: u64,
    timestamp: &str,
    role: &str,
    entry_type: &str,
    body: Value,
) -> Value {
    json!({
        "id": format!("timeline-entry/native-{sequence}"),
        "sequence": sequence,
        "revision": 1,
        "timestamp": timestamp,
        "role": role,
        "type": entry_type,
        "final": true,
        "body": body
    })
}

fn parse_timestamp(value: Option<&Value>) -> Option<u128> {
    let value = value?.as_str()?;
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.timestamp_millis().max(0) as u128)
}

fn system_time_ms(value: SystemTime) -> u128 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_claude_and_omp_transcripts_normalize_to_one_timeline_shape() {
        let session = |driver| ExternalSession {
            id: "session/external-test".into(),
            revision: "revision".into(),
            driver,
            native_id: "native".into(),
            transcript: PathBuf::new(),
            cwd: None,
            title: None,
            started_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            process: None,
        };
        let mut items = Vec::new();
        normalize_codex(
            &json!({"type":"response_item","timestamp":"2026-01-01T00:00:00Z","payload":{"type":"message","role":"assistant","id":"m1","content":[{"type":"output_text","text":"codex"}]}}),
            0,
            &session(ExternalDriver::Codex),
            &mut items,
        );
        normalize_claude(
            &json!({"type":"assistant","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":[{"type":"tool_use","id":"c1","name":"shell","input":{"command":"true"}}]}}),
            16,
            &session(ExternalDriver::Claude),
            &mut items,
        );
        normalize_omp(
            &json!({"type":"message","id":"m2","timestamp":"2026-01-01T00:00:02Z","message":{"role":"tool","content":[{"type":"toolResult","toolCallId":"c1","content":"ok"}]}}),
            32,
            &session(ExternalDriver::Omp),
            &mut items,
        );
        assert!(
            items
                .iter()
                .any(|item| item["type"] == "content" && item["body"]["text"] == "codex")
        );
        assert!(
            items
                .iter()
                .any(|item| item["type"] == "tool_call" && item["body"]["call_id"] == "c1")
        );
        assert!(
            items
                .iter()
                .any(|item| item["type"] == "tool_result" && item["body"]["call_id"] == "c1")
        );
    }

    #[test]
    fn imported_sessions_render_as_valid_resuming_missions() {
        let workspace = tempfile::tempdir().unwrap();
        for (driver, expected) in [
            (ExternalDriver::Codex, vec!["resume", "native-session"]),
            (ExternalDriver::Claude, vec!["--resume", "native-session"]),
            (ExternalDriver::Omp, vec!["--resume", "native-session"]),
        ] {
            let session = ExternalSession {
                id: "session/external-test".into(),
                revision: "revision".into(),
                driver,
                native_id: "native-session".into(),
                transcript: PathBuf::new(),
                cwd: Some(workspace.path().to_owned()),
                title: None,
                started_at_unix_ms: 0,
                updated_at_unix_ms: 0,
                process: None,
            };
            let import = import_mission(&session).unwrap();
            crate::graph::parse_intent(&import.kdl, "host/test").unwrap();
            for argument in expected {
                assert!(
                    import.kdl.contains(argument),
                    "{} lacks {argument}",
                    import.kdl
                );
            }
        }
    }

    #[test]
    fn process_detection_uses_executables_not_incidental_arguments() {
        assert_eq!(
            driver_for_command("/usr/bin/codex resume abc"),
            Some(ExternalDriver::Codex)
        );
        assert_eq!(
            driver_for_command("node /opt/bin/claude --resume abc"),
            Some(ExternalDriver::Claude)
        );
        assert_eq!(
            driver_for_command("/opt/st3 driver omp --subject agent/x -- omp"),
            Some(ExternalDriver::Omp)
        );
        assert_eq!(driver_for_command("rg codex crates/st3"), None);
        assert_eq!(driver_for_command("bash -c echo claude"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn takeover_stops_only_the_process_matching_the_exact_start_fingerprint() {
        let mut child = std::process::Command::new("/bin/bash")
            .args(["-c", "exec -a codex /bin/sleep 30"])
            .spawn()
            .unwrap();
        let process = (0..100)
            .find_map(|_| {
                let candidate = platform_processes()
                    .unwrap()
                    .into_iter()
                    .find(|candidate| candidate.process.pid == child.id());
                if candidate.is_none() {
                    std::thread::sleep(Duration::from_millis(10));
                }
                candidate
            })
            .expect("the test codex process should be discoverable")
            .process;

        let mut wrong = process.clone();
        wrong.exact_session = true;
        wrong.fingerprint = "different-start-fingerprint".into();
        assert!(terminate_exact_process(ExternalDriver::Codex, &wrong).is_err());
        assert!(child.try_wait().unwrap().is_none());

        let mut exact = process;
        exact.exact_session = true;
        terminate_exact_process(ExternalDriver::Codex, &exact).unwrap();
        assert!(child.wait().unwrap().code().is_none());
    }
}
