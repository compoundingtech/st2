//! Native message bus.
//!
//! st2 owns messaging directly now: a message is a markdown file with YAML frontmatter, named
//! `<unix-ms>-<rand6>.md`, written into the recipient agent's `resources/inbox/` (VRS §5). The
//! **recipient is implied by the path**; `from` lives in the frontmatter; the filename's ms prefix is
//! the send time. Send = write the file. Archive = move inbox→archive. Nobody mutates a file after
//! creation. An archive copy is also a durable receipt: if an eventually-consistent file sync briefly
//! restores the same filename in the inbox, inbox readers suppress the duplicate and a repeated
//! archive removes it without overwriting the receipt. The on-disk grammar is stable.
//!
//! This module is location-agnostic: it operates on an inbox/agent directory a caller resolves (from
//! the catalog for VRS-native, or `$ST_ROOT` for a compat shim).

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;

/// The alphabet st2 *generates* `<rand6>` from — Crockford base32 (`0-9a-z` minus `i l o u`). This is
/// a strict subset of what the reader accepts: the frozen bus grammar is `[0-9a-z]{6}`, so a peer
/// may legally use i/l/o/u and [`is_message_filename`] must not reject those.
const CROCKFORD: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Parsed frontmatter + body of a message. Readers are permissive: missing/malformed frontmatter
/// still yields a readable body with `from = None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// `<unix-ms>-<rand6>.md`.
    pub filename: String,
    /// Send time in unix ms (from the filename prefix).
    pub ts_ms: u64,
    /// `from:` — the claimed sender.
    pub from: Option<String>,
    /// `subject:`.
    pub subject: Option<String>,
    /// `in-reply-to:` — the filename of the message this replies to.
    pub in_reply_to: Option<String>,
    /// `tags:` — comma-separated.
    pub tags: Vec<String>,
    /// `priority:` — `low` | `normal` | `high`, if set.
    pub priority: Option<String>,
    /// `idempotency-key:` — the optional sender key for local retry deduplication.
    pub idempotency_key: Option<String>,
    /// The markdown body.
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdempotentSendSeam {
    BeforePublication,
    AfterPublication,
}

/// Current unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A fresh canonical `<unix-ms>-<rand6>.md` filename (the shared grammar — messages AND context
/// decision entries use it, so both sort chronologically by name).
pub fn new_filename() -> String {
    format!("{}-{}.md", now_ms(), rand6())
}

/// Six Crockford-base32 chars from `/dev/urandom` (falls back to a time-derived value).
fn rand6() -> String {
    let mut buf = [0u8; 6];
    let ok = fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .is_ok();
    if !ok {
        // Degenerate fallback — mix the ns clock so it isn't all-zeros.
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (n >> (i * 8)) as u8;
        }
    }
    buf.iter()
        .map(|b| CROCKFORD[(*b as usize) % 32] as char)
        .collect()
}

/// True if `name` matches the frozen bus grammar `^[0-9]{13}-[0-9a-z]{6}\.md$`.
/// The reader accepts the FULL `[0-9a-z]` rand6 alphabet — st2 generates a Crockford subset, but a
/// peer message may use any lowercase alnum and dropping those would lose real messages.
pub fn is_message_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".md") else {
        return false;
    };
    let Some((ts, rand)) = stem.split_once('-') else {
        return false;
    };
    ts.len() == 13
        && ts.bytes().all(|b| b.is_ascii_digit())
        && rand.len() == 6
        && rand
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

/// Render a message file's contents (frontmatter + body).
pub fn render_message(
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> String {
    render_message_fields(from, subject, in_reply_to, tags, body, None)
}

fn render_message_fields(
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
) -> String {
    let mut s = String::from("---\n");
    s.push_str(&format!("from: {from}\n"));
    if let Some(key) = idempotency_key {
        s.push_str(&format!("idempotency-key: {key}\n"));
    }
    if let Some(subj) = subject {
        s.push_str(&format!("subject: {subj}\n"));
    }
    if let Some(irt) = in_reply_to {
        s.push_str(&format!("in-reply-to: {irt}\n"));
    }
    if !tags.is_empty() {
        s.push_str(&format!("tags: {}\n", tags.join(", ")));
    }
    s.push_str("---\n");
    s.push_str(body);
    if !body.ends_with('\n') {
        s.push('\n');
    }
    s
}

/// Parse a message file's contents into frontmatter fields + body. Permissive.
fn parse_message(filename: &str, contents: &str) -> Message {
    let ts_ms = filename
        .split_once('-')
        .and_then(|(ts, _)| ts.parse::<u64>().ok())
        .unwrap_or(0);

    let mut msg = Message {
        filename: filename.to_string(),
        ts_ms,
        from: None,
        subject: None,
        in_reply_to: None,
        tags: Vec::new(),
        priority: None,
        idempotency_key: None,
        body: String::new(),
    };

    // Frontmatter is an opening `---` line … a closing `---` line.
    let rest = contents
        .strip_prefix("---\n")
        .or_else(|| contents.strip_prefix("---\r\n"));
    if let Some(rest) = rest
        && let Some(end) = rest.find("\n---")
    {
        let front = &rest[..end];
        // body starts after the closing `---` line
        let after = &rest[end + 1..]; // at the `---` line
        let body = after.split_once('\n').map(|x| x.1).unwrap_or("");
        for line in front.lines() {
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            let v = v.trim();
            match k.trim() {
                "from" => msg.from = Some(v.to_string()),
                "subject" => msg.subject = Some(v.to_string()),
                "in-reply-to" => msg.in_reply_to = Some(v.to_string()),
                "tags" => {
                    msg.tags = v
                        .split(',')
                        .map(|t| t.trim().to_string())
                        .filter(|t| !t.is_empty())
                        .collect()
                }
                "priority" => msg.priority = Some(v.to_string()),
                "idempotency-key" => msg.idempotency_key = Some(v.to_string()),
                _ => {}
            }
        }
        msg.body = body.to_string();
    } else {
        // No frontmatter — the whole thing is the body.
        msg.body = contents.to_string();
    }
    msg
}

/// Send: write a new message file into `inbox_dir`, returning its filename. Creates `inbox_dir` if
/// missing; retries on the astronomically-unlikely filename collision.
///
/// The message is materialized atomically (temporary sibling + rename). A direct write under the
/// canonical name exposes an empty or partial file to concurrent readers after create/truncate but
/// before the bytes arrive; that incomplete message has no parseable sender or subject.
pub fn send_to_inbox(
    inbox_dir: &Path,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> anyhow::Result<String> {
    fs::create_dir_all(inbox_dir)?;
    let contents = render_message(from, subject, in_reply_to, tags, body);
    publish_message_with_seam(inbox_dir, &contents, |_| Ok(()))
}

/// Send one normal message with a local idempotency key.
///
/// While the short local lock is held, st2 searches the recipient inbox first and archive second.
/// A retry returns the first matching normal message. If that message is deleted, st2 forgets the
/// key and a later send creates a new message.
#[allow(clippy::too_many_arguments)]
pub fn send_idempotent_to_inbox(
    inbox_dir: &Path,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: &str,
) -> anyhow::Result<String> {
    send_idempotent_to_inbox_with_seam(
        inbox_dir,
        from,
        subject,
        in_reply_to,
        tags,
        body,
        idempotency_key,
        |_| Ok(()),
    )
}

#[allow(clippy::too_many_arguments)]
fn send_idempotent_to_inbox_with_seam(
    inbox_dir: &Path,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: &str,
    seam: impl FnMut(IdempotentSendSeam) -> anyhow::Result<()>,
) -> anyhow::Result<String> {
    validate_idempotency_key(idempotency_key)?;
    fs::create_dir_all(inbox_dir)?;
    let _lock = MessageIdempotencyLock::acquire(inbox_dir)?;

    if let Some(filename) = find_idempotent_message(inbox_dir, idempotency_key)? {
        return Ok(filename);
    }

    let contents = render_message_fields(
        from,
        subject,
        in_reply_to,
        tags,
        body,
        Some(idempotency_key),
    );
    publish_message_with_seam(inbox_dir, &contents, seam)
}

fn validate_idempotency_key(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control),
        "message idempotency key must be non-empty single-line text without surrounding whitespace"
    );
    Ok(())
}

fn find_idempotent_message(inbox_dir: &Path, key: &str) -> anyhow::Result<Option<String>> {
    for message in list_dir(inbox_dir)? {
        if message.idempotency_key.as_deref() == Some(key) {
            return Ok(Some(message.filename));
        }
    }
    for message in list_dir(&sibling_archive_dir(inbox_dir))? {
        if message.idempotency_key.as_deref() == Some(key) {
            return Ok(Some(message.filename));
        }
    }
    Ok(None)
}

const IDEMPOTENCY_LOCK_FILE: &str = ".message-idempotency.lock";

struct MessageIdempotencyLock {
    file: File,
}

impl MessageIdempotencyLock {
    fn acquire(inbox_dir: &Path) -> anyhow::Result<Self> {
        let path = inbox_dir.join(IDEMPOTENCY_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
            .with_context(|| format!("open message idempotency lock {}", path.display()))?;
        anyhow::ensure!(
            file.metadata()?.is_file(),
            "message idempotency lock is not a regular file: {}",
            path.display()
        );
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lock message idempotency file {}", path.display()));
        }
        Ok(Self { file })
    }
}

impl Drop for MessageIdempotencyLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn publish_message_with_seam(
    inbox_dir: &Path,
    contents: &str,
    mut seam: impl FnMut(IdempotentSendSeam) -> anyhow::Result<()>,
) -> anyhow::Result<String> {
    // This deliberately cannot match `is_message_filename`, so a concurrent scan ignores it.
    let tmp = inbox_dir.join(tmp_name());
    if let Err(error) = fs::write(&tmp, &contents) {
        let _ = fs::remove_file(&tmp);
        return Err(error.into());
    }
    seam(IdempotentSendSeam::BeforePublication)?;
    for _ in 0..8 {
        let filename = new_filename();
        let path = inbox_dir.join(&filename);
        if !path.exists() {
            if let Err(error) = fs::rename(&tmp, &path) {
                let _ = fs::remove_file(&tmp);
                return Err(error.into());
            }
            seam(IdempotentSendSeam::AfterPublication)?;
            return Ok(filename);
        }
    }
    let _ = fs::remove_file(&tmp);
    anyhow::bail!(
        "could not allocate a unique message filename in {}",
        inbox_dir.display()
    )
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_name() -> String {
    format!(
        ".message.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Read a canonical entry that was already returned by `read_dir`. Removing a message concurrently
/// (for example, inbox→archive) is normal: that entry vanished, it did not become an empty message.
fn read_message_contents(path: &Path) -> anyhow::Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(anyhow::anyhow!("reading {}: {error}", path.display())),
    }
}

/// List the canonical messages in `dir` (inbox or archive), sorted by send time. Non-message files
/// are skipped. Frontmatter is parsed for metadata.
pub fn list_dir(dir: &Path) -> anyhow::Result<Vec<Message>> {
    let mut msgs = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(msgs), // no dir yet → empty
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_message_filename(&name) {
            continue;
        }
        let Some(contents) = read_message_contents(&entry.path())? else {
            continue;
        };
        msgs.push(parse_message(&name, &contents));
    }
    // Primary order is send time; the `<rand6>` suffix is a deterministic tiebreak so two messages
    // that land in the same millisecond still list in a stable, reproducible order (the wire format
    // can't recover true send-order within a millisecond — this at least isn't `read_dir`-arbitrary).
    msgs.sort_by(|a, b| {
        a.ts_ms
            .cmp(&b.ts_ms)
            .then_with(|| a.filename.cmp(&b.filename))
    });
    Ok(msgs)
}

/// List messages that are logically present in `inbox_dir`.
///
/// The sibling `archive/` is a durable handled-message receipt. Eventually-consistent sync can
/// briefly restore an inbox file after a local inbox→archive move but before the delete tombstone
/// converges. If the same canonical filename already exists in archive, the archived copy wins and
/// the raw inbox duplicate is not unread work. Listing also removes that duplicate so every reader
/// helps the archive receipt converge locally.
pub fn list_inbox(inbox_dir: &Path) -> anyhow::Result<Vec<Message>> {
    let archive_dir = sibling_archive_dir(inbox_dir);
    let mut unread = Vec::new();
    for message in list_dir(inbox_dir)? {
        if archive_dir.join(&message.filename).is_file() {
            remove_inbox_duplicate(&inbox_dir.join(&message.filename), &message.filename)?;
        } else {
            unread.push(message);
        }
    }
    Ok(unread)
}

/// The archive directory paired with an inbox. Both native
/// `resources/{inbox,archive}` and flat `<identity>/{inbox,archive}` layouts use sibling boxes.
fn sibling_archive_dir(inbox_dir: &Path) -> PathBuf {
    inbox_dir.parent().unwrap_or(inbox_dir).join("archive")
}

/// Read one message file from `dir`.
pub fn read_msg(dir: &Path, filename: &str) -> anyhow::Result<Message> {
    let contents = fs::read_to_string(dir.join(filename))
        .map_err(|e| anyhow::anyhow!("reading {filename}: {e}"))?;
    Ok(parse_message(filename, &contents))
}

/// An agent's inbox dir (VRS §5): `<agent_dir>/resources/inbox`.
pub fn inbox_dir(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join("inbox")
}

/// An agent's message-archive dir: `<agent_dir>/resources/archive`.
pub fn archive_dir(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join("archive")
}

/// Eval-owned authority for one external flat requester mailbox. General catalog routing remains
/// declaration-only; possessing this value is the explicit exception at message call sites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalInbox {
    root: PathBuf,
    identity: String,
    inbox: PathBuf,
}

impl ExternalInbox {
    pub fn new(root: &Path, identity: &str) -> anyhow::Result<Self> {
        let mut components = Path::new(identity).components();
        let safe = matches!(components.next(), Some(Component::Normal(component)) if component == identity)
            && components.next().is_none();
        anyhow::ensure!(
            safe,
            "external requester identity must be one non-empty relative path component"
        );
        Ok(Self {
            root: root.to_path_buf(),
            identity: identity.to_owned(),
            inbox: root.join(identity).join("inbox"),
        })
    }

    pub fn provision(root: &Path, identity: &str) -> anyhow::Result<Self> {
        let external = Self::new(root, identity)?;
        fs::create_dir_all(&external.inbox).map_err(|error| {
            anyhow::anyhow!(
                "provisioning external requester {identity:?} inbox {}: {error}",
                external.inbox.display()
            )
        })?;
        Ok(external)
    }
}

/// Resolve an inbox by stable identity. A proven catalog-less root retains the legacy flat bus;
/// inside a catalog an absent identity always fails closed.
pub fn resolve_inbox(root: &Path, id: &str, host: &str) -> anyhow::Result<PathBuf> {
    resolve_list_box(root, id, host, false, false)
}

/// Resolve a normal declared inbox or one exact eval-owned external requester capability.
pub fn resolve_inbox_with_external(
    root: &Path,
    id: &str,
    host: &str,
    external: Option<&ExternalInbox>,
) -> anyhow::Result<PathBuf> {
    match resolve_inbox(root, id, host) {
        Ok(inbox) => Ok(inbox),
        Err(error) => match external {
            Some(external)
                if external.root == root && external.identity == id && external.inbox.is_dir() =>
            {
                Ok(external.inbox.clone())
            }
            _ => Err(error),
        },
    }
}

/// Archive companion to [`resolve_inbox`], with the same stable-ID and catalog-less boundaries.
pub fn resolve_archive(root: &Path, id: &str, host: &str) -> anyhow::Result<PathBuf> {
    resolve_list_box(root, id, host, true, false)
}

/// Resolve one box for `message ls`.
///
/// The permissive flat layout is automatic only when discovery proves that `root` is catalog-less.
/// Once any valid or malformed declaration makes it a catalog, an absent identity is an error.
/// `orphan` is the explicit recovery path for inspecting a raw flat box inside such a root.
pub fn resolve_list_box(
    root: &Path,
    id: &str,
    host: &str,
    archive: bool,
    orphan: bool,
) -> anyhow::Result<PathBuf> {
    let flat = || {
        root.join(id)
            .join(if archive { "archive" } else { "inbox" })
    };
    if orphan {
        return Ok(flat());
    }
    if apply_incomplete(root) {
        return resolve_agent_dir(root, id, host)?
            .map(|agent_dir| {
                if archive {
                    archive_dir(&agent_dir)
                } else {
                    inbox_dir(&agent_dir)
                }
            })
            .with_context(|| {
                format!("agent '{id}' is not addressable while catalog apply is incomplete")
            });
    }
    let discovered = crate::discover(root);
    if let Some(agent_dir) = discovered
        .specs
        .iter()
        .find(|spec| spec.bus_id(host) == id || spec.identity == id)
        .and_then(|spec| spec.path.parent())
    {
        return Ok(if archive {
            archive_dir(agent_dir)
        } else {
            inbox_dir(agent_dir)
        });
    }

    if discovered.specs.is_empty() && discovered.errors.is_empty() {
        return Ok(flat());
    }
    anyhow::bail!("no agent '{id}' found in catalog {}", root.display())
}

/// Resolve a recipient (a bus id `<host>.<id>` or a bare identity) to its agent folder in the
/// catalog, via content discovery. Returns `None` if no agent matches.
pub fn resolve_agent_dir(
    catalog_root: &Path,
    recipient: &str,
    this_host: &str,
) -> anyhow::Result<Option<PathBuf>> {
    Ok(resolve_agent_handle(catalog_root, recipient, this_host)?.map(|agent| agent.path))
}

pub fn with_resolved_agent_dir<T>(
    catalog_root: &Path,
    identity: &str,
    this_host: &str,
    operation: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_resolved_state_dir(catalog_root, identity, this_host, &[], true, operation)
}

pub fn with_resolved_state_dir<T>(
    catalog_root: &Path,
    identity: &str,
    this_host: &str,
    components: &[&str],
    create: bool,
    operation: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    match resolve_agent_handle(catalog_root, identity, this_host)? {
        Some(agent) => {
            test_capability_checkpoint();
            let path = match agent.capability.as_ref() {
                Some(capability) if components.is_empty() => {
                    crate::catalog_transaction::retained_dir_path(capability)?
                }
                Some(capability) => {
                    let directory = open_message_box(capability, components, create)?
                        .context("resolved state directory does not exist")?;
                    let path = crate::catalog_transaction::retained_dir_path(&directory)?;
                    return operation(&path);
                }
                None => components
                    .iter()
                    .fold(agent.path, |path, component| path.join(component)),
            };
            operation(&path)
        }
        None => {
            let discovered = crate::discover(catalog_root);
            anyhow::ensure!(
                crate::catalog_transaction::catalog_transition(catalog_root)?.is_none()
                    && !catalog_root.join(crate::catalog_lock::CONTROL_DIR).exists()
                    && discovered.specs.is_empty()
                    && discovered.errors.is_empty(),
                "no agent '{identity}' found in catalog {}",
                catalog_root.display()
            );
            operation(
                &components
                    .iter()
                    .fold(catalog_root.join(identity), |path, component| {
                        path.join(component)
                    }),
            )
        }
    }
}

fn resolve_agent_handle(
    catalog_root: &Path,
    recipient: &str,
    this_host: &str,
) -> anyhow::Result<Option<AddressableAgent>> {
    for _ in 0..3 {
        let before = address_fence(catalog_root)?;
        let mut candidates = addressable_agent_dirs(catalog_root, this_host, before.1.as_ref())?
            .into_iter()
            .filter(|candidate| candidate.bus_id == recipient || candidate.identity == recipient)
            .collect::<Vec<_>>();
        let after = address_fence(catalog_root)?;
        if before != after {
            continue;
        }
        candidates.sort_by(|left, right| left.path.cmp(&right.path));
        candidates.dedup_by(|left, right| left.path == right.path);
        return Ok((candidates.len() == 1).then(|| candidates.remove(0)));
    }
    anyhow::bail!("catalog address book changed repeatedly while resolving {recipient:?}")
}

fn address_fence(
    catalog_root: &Path,
) -> anyhow::Result<(
    Option<u64>,
    Option<crate::catalog_transaction::CatalogTransition>,
)> {
    let first_generation = crate::catalog_lock::read_generation_token(catalog_root)?;
    let transition = crate::catalog_transaction::catalog_transition(catalog_root)?;
    test_address_fence_checkpoint();
    let second_generation = crate::catalog_lock::read_generation_token(catalog_root)?;
    anyhow::ensure!(
        first_generation == second_generation,
        "catalog address book changed while sampling its transition fence"
    );
    Ok((second_generation, transition))
}

#[cfg(debug_assertions)]
fn test_address_fence_checkpoint() {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_ADDRESS_FENCE_READY"),
        std::env::var("ST2_TEST_ADDRESS_FENCE_RELEASE"),
    ) else {
        return;
    };
    if OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&ready)
        .is_err()
    {
        return;
    }
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
fn test_address_fence_checkpoint() {}

#[derive(Debug)]
struct AddressableAgent {
    bus_id: String,
    identity: String,
    path: PathBuf,
    capability: Option<File>,
}

fn addressable_agent_dirs(
    catalog_root: &Path,
    this_host: &str,
    transition: Option<&crate::catalog_transaction::CatalogTransition>,
) -> anyhow::Result<Vec<AddressableAgent>> {
    let Some(transition) = transition else {
        return crate::discover(catalog_root)
            .specs
            .into_iter()
            .map(|spec| {
                let path = spec
                    .path
                    .parent()
                    .context("Agent Spec has no identity directory")?
                    .to_path_buf();
                let capability = crate::catalog_transaction::open_dir_beneath(catalog_root, &path)?;
                Ok(AddressableAgent {
                    bus_id: spec.bus_id(this_host),
                    identity: spec.identity,
                    path,
                    capability: Some(capability),
                })
            })
            .collect();
    };
    let agents = catalog_root.join("agents");
    let metadata = match fs::symlink_metadata(&agents) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "canonical agents path is not a real directory: {}",
        agents.display()
    );
    let mut result = Vec::new();
    for host in sorted_real_entries(&agents, "host")? {
        let host_name = safe_entry_name(&host, "host")?;
        for identity in sorted_real_entries(&host.path(), "identity")? {
            let identity_name = safe_entry_name(&identity, "identity")?;
            let key = crate::catalog_transaction::AgentKey {
                host: host_name.clone(),
                identity: identity_name.clone(),
            };
            let path = identity.path();
            let capability = crate::catalog_transaction::open_dir_beneath(catalog_root, &path)?;
            let retained = crate::catalog_transaction::retained_dir_path(&capability)?;
            let current_spec = marker_spec_matches(&retained, &key)?;
            let retained_state =
                transition.original_agents.contains(&key) && marker_state_exists(&retained)?;
            if current_spec || retained_state {
                result.push(AddressableAgent {
                    bus_id: format!("{}.{}", key.host, key.identity),
                    identity: key.identity,
                    path,
                    capability: Some(capability),
                });
            }
        }
    }
    Ok(result)
}

fn sorted_real_entries(dir: &Path, label: &str) -> anyhow::Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in &entries {
        let metadata = fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "canonical {label} path is not a real directory: {}",
            entry.path().display()
        );
    }
    Ok(entries)
}

fn safe_entry_name(entry: &fs::DirEntry, label: &str) -> anyhow::Result<String> {
    let value = entry
        .file_name()
        .into_string()
        .map_err(|_| anyhow::anyhow!("canonical {label} is not UTF-8"))?;
    anyhow::ensure!(safe_component(&value), "unsafe canonical {label} {value:?}");
    Ok(value)
}

fn marker_spec_matches(
    agent_dir: &Path,
    key: &crate::catalog_transaction::AgentKey,
) -> anyhow::Result<bool> {
    let path = agent_dir.join("agent.kdl");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "canonical Agent Spec is not a real regular file: {}",
        path.display()
    );
    let declared = crate::discovery::parse_declared(&path)?;
    Ok(declared.len() == 1
        && declared[0].host.as_deref() == Some(&key.host)
        && declared[0].identity.as_deref() == Some(key.identity.as_str()))
}

fn marker_state_exists(agent_dir: &Path) -> anyhow::Result<bool> {
    let mut found = false;
    for name in ["resources", "archive", "inbox"] {
        let path = agent_dir.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => found = true,
            Ok(_) => anyhow::bail!(
                "agent state path is not a real directory: {}",
                path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    match fs::symlink_metadata(agent_dir.join("status")) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => found = true,
        Ok(_) => anyhow::bail!("agent status is not a real regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let resources = agent_dir.join("resources");
    if resources.is_dir() {
        for relative in ["inbox", "archive", "context", "context/decisions", "links"] {
            match fs::symlink_metadata(resources.join(relative)) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => anyhow::bail!("agent resource path is not a real directory"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(found)
}

fn apply_incomplete(root: &Path) -> bool {
    fs::symlink_metadata(crate::catalog_lock::apply_marker_path(root)).is_ok()
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | ".." | ".git" | ".st2")
        && Path::new(value).components().count() == 1
}

/// The default subject for a reply to a message whose subject was `original`: the original prefixed
/// with `re: `, unless it already carries a (case-insensitive) `re:` prefix. `None` stays `None`.
pub fn reply_subject(original: Option<&str>) -> Option<String> {
    original.map(|s| {
        let t = s.trim_start();
        if t.len() >= 3 && t[..3].eq_ignore_ascii_case("re:") {
            s.to_string()
        } else {
            format!("re: {s}")
        }
    })
}

/// One line of a message thread: the message plus its reply depth (0 = the thread root).
#[derive(Debug, Clone)]
pub struct ThreadEntry {
    pub filename: String,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub ts_ms: u64,
    /// Reply depth from the thread root (for `--tree` indentation).
    pub depth: usize,
}

/// Collect the whole thread that `filename` belongs to, across the catalog (a two-party conversation
/// lives in BOTH agents' boxes, so scan every agent's inbox+archive). Returns the thread in reply-tree
/// pre-order with depth; the root's `in-reply-to` chain is walked up first. Empty if `filename` isn't
/// found. Cycles/dangling `in-reply-to` are handled defensively.
pub fn collect_thread(catalog_root: &Path, filename: &str) -> anyhow::Result<Vec<ThreadEntry>> {
    // Gather every message once (dedup by filename — the same file can appear in two boxes).
    let mut all: HashMap<String, Message> = HashMap::new();
    let transition = crate::catalog_transaction::catalog_transition(catalog_root)?;
    for agent in addressable_agent_dirs(catalog_root, "", transition.as_ref())? {
        if let Some(capability) = agent.capability.as_ref() {
            for components in [&["resources", "inbox"][..], &["resources", "archive"][..]] {
                let Some(dir) = open_message_box(capability, components, false)? else {
                    continue;
                };
                for message in list_dir(&crate::catalog_transaction::retained_dir_path(&dir)?)
                    .unwrap_or_default()
                {
                    all.entry(message.filename.clone()).or_insert(message);
                }
            }
        } else {
            for dir in [inbox_dir(&agent.path), archive_dir(&agent.path)] {
                for message in list_dir(&dir).unwrap_or_default() {
                    all.entry(message.filename.clone()).or_insert(message);
                }
            }
        }
    }
    if !all.contains_key(filename) {
        return Ok(Vec::new());
    }

    // Walk up `in-reply-to` to the thread root.
    let mut root = filename.to_string();
    let mut seen = HashSet::new();
    while seen.insert(root.clone()) {
        match all.get(&root).and_then(|m| m.in_reply_to.clone()) {
            Some(parent) if all.contains_key(&parent) => root = parent,
            _ => break,
        }
    }

    // Children by parent, each sorted by send time.
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    for (fname, m) in &all {
        if let Some(p) = &m.in_reply_to
            && all.contains_key(p)
        {
            children.entry(p.clone()).or_default().push(fname.clone());
        }
    }
    for kids in children.values_mut() {
        kids.sort_by_key(|f| all.get(f).map(|m| m.ts_ms).unwrap_or(0));
    }

    // Pre-order DFS from the root, iterative (stack of (filename, depth)); guard against cycles.
    let mut out = Vec::new();
    let mut visited = HashSet::new();
    let mut stack = vec![(root, 0usize)];
    while let Some((fname, depth)) = stack.pop() {
        if !visited.insert(fname.clone()) {
            continue;
        }
        if let Some(m) = all.get(&fname) {
            out.push(ThreadEntry {
                filename: m.filename.clone(),
                from: m.from.clone(),
                subject: m.subject.clone(),
                ts_ms: m.ts_ms,
                depth,
            });
        }
        // Push children in reverse so the earliest is processed first (stack = LIFO).
        if let Some(kids) = children.get(&fname) {
            for k in kids.iter().rev() {
                stack.push((k.clone(), depth + 1));
            }
        }
    }
    Ok(out)
}

pub fn send_to_resolved_inbox(
    catalog_root: &Path,
    recipient: &str,
    this_host: &str,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> anyhow::Result<String> {
    send_to_resolved_inbox_with_key(
        catalog_root,
        recipient,
        this_host,
        from,
        subject,
        in_reply_to,
        tags,
        body,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn send_idempotent_to_resolved_inbox(
    catalog_root: &Path,
    recipient: &str,
    this_host: &str,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: &str,
) -> anyhow::Result<String> {
    send_to_resolved_inbox_with_key(
        catalog_root,
        recipient,
        this_host,
        from,
        subject,
        in_reply_to,
        tags,
        body,
        Some(idempotency_key),
    )
}

#[allow(clippy::too_many_arguments)]
fn send_to_resolved_inbox_with_key(
    catalog_root: &Path,
    recipient: &str,
    this_host: &str,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
) -> anyhow::Result<String> {
    let agent = match resolve_agent_handle(catalog_root, recipient, this_host)? {
        Some(agent) => agent,
        None => {
            let discovered = crate::discover(catalog_root);
            if crate::catalog_transaction::catalog_transition(catalog_root)?.is_none()
                && !catalog_root.join(crate::catalog_lock::CONTROL_DIR).exists()
                && discovered.specs.is_empty()
                && discovered.errors.is_empty()
            {
                return send_to_inbox_with_optional_key(
                    &catalog_root.join(recipient).join("inbox"),
                    from,
                    subject,
                    in_reply_to,
                    tags,
                    body,
                    idempotency_key,
                );
            }
            anyhow::bail!(
                "no agent '{recipient}' found in catalog {}",
                catalog_root.display()
            )
        }
    };
    test_capability_checkpoint();
    if let Some(capability) = agent.capability.as_ref() {
        let inbox = open_message_box(capability, &["resources", "inbox"], true)?
            .context("created inbox capability is missing")?;
        send_to_inbox_with_optional_key(
            &crate::catalog_transaction::retained_dir_path(&inbox)?,
            from,
            subject,
            in_reply_to,
            tags,
            body,
            idempotency_key,
        )
    } else {
        send_to_inbox_with_optional_key(
            &inbox_dir(&agent.path),
            from,
            subject,
            in_reply_to,
            tags,
            body,
            idempotency_key,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn send_to_inbox_with_optional_key(
    inbox: &Path,
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
    idempotency_key: Option<&str>,
) -> anyhow::Result<String> {
    match idempotency_key {
        Some(key) => send_idempotent_to_inbox(inbox, from, subject, in_reply_to, tags, body, key),
        None => send_to_inbox(inbox, from, subject, in_reply_to, tags, body),
    }
}

#[cfg(debug_assertions)]
fn test_capability_checkpoint() {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_MESSAGE_CAPABILITY_READY"),
        std::env::var("ST2_TEST_MESSAGE_CAPABILITY_RELEASE"),
    ) else {
        return;
    };
    let _ = fs::write(ready, b"ready");
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
}

#[cfg(not(debug_assertions))]
fn test_capability_checkpoint() {}

pub fn archive_resolved_message(
    catalog_root: &Path,
    identity: &str,
    this_host: &str,
    filename: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        is_message_filename(filename),
        "invalid message filename {filename:?}"
    );
    let agent = match resolve_agent_handle(catalog_root, identity, this_host)? {
        Some(agent) => agent,
        None => {
            let discovered = crate::discover(catalog_root);
            if crate::catalog_transaction::catalog_transition(catalog_root)?.is_none()
                && !catalog_root.join(crate::catalog_lock::CONTROL_DIR).exists()
                && discovered.specs.is_empty()
                && discovered.errors.is_empty()
            {
                return archive_msg(
                    &catalog_root.join(identity).join("inbox"),
                    &catalog_root.join(identity).join("archive"),
                    filename,
                );
            }
            anyhow::bail!(
                "no agent '{identity}' found in catalog {}",
                catalog_root.display()
            )
        }
    };
    if let Some(capability) = agent.capability.as_ref() {
        let inbox = open_message_box(capability, &["resources", "inbox"], false)?
            .context("message inbox does not exist")?;
        let archive = open_message_box(capability, &["resources", "archive"], true)?
            .context("created archive capability is missing")?;
        archive_msg(
            &crate::catalog_transaction::retained_dir_path(&inbox)?,
            &crate::catalog_transaction::retained_dir_path(&archive)?,
            filename,
        )
    } else {
        archive_msg(&inbox_dir(&agent.path), &archive_dir(&agent.path), filename)
    }
}

fn open_message_box(
    agent: &File,
    components: &[&str],
    create: bool,
) -> anyhow::Result<Option<File>> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd as _;
    let mut current = agent.try_clone()?;
    for component in components {
        match crate::catalog_transaction::openat_dir_nofollow(
            &current,
            std::ffi::OsStr::new(component),
        ) {
            Ok(next) => current = next,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !create => {
                return Ok(None);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = CString::new(*component)?;
                let result = unsafe { libc::mkdirat(current.as_raw_fd(), name.as_ptr(), 0o755) };
                if result != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(error.into());
                    }
                }
                current = crate::catalog_transaction::openat_dir_nofollow(
                    &current,
                    std::ffi::OsStr::new(component),
                )?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Some(current))
}

/// Archive `filename` from inbox to archive.
///
/// Idempotency matters on an eventually-consistent bus: if archive already contains the filename,
/// it is the durable receipt and wins. Remove only a duplicate inbox copy, never overwrite the
/// archived file. A source that disappeared concurrently is also success when the receipt exists.
pub fn archive_msg(inbox_dir: &Path, archive_dir: &Path, filename: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        is_message_filename(filename),
        "invalid message filename {filename:?}"
    );
    fs::create_dir_all(archive_dir)?;
    let source = inbox_dir.join(filename);
    let receipt = archive_dir.join(filename);

    if receipt.is_file() {
        return remove_inbox_duplicate(&source, filename);
    }

    // Link first, then unlink the source: unlike rename on Unix, hard-link creation never replaces a
    // destination that raced us into existence. The two boxes are siblings on one filesystem in
    // both supported layouts, so this gives archive-wins semantics without a clobber window.
    match fs::hard_link(&source, &receipt) {
        Ok(()) => remove_inbox_duplicate(&source, filename),
        // A sync writer may create the receipt between our check and link attempt. If so, preserve
        // that receipt and clean only the duplicate source.
        Err(_) if receipt.is_file() => remove_inbox_duplicate(&source, filename),
        Err(e) => Err(anyhow::anyhow!("archiving {filename}: {e}")),
    }
}

fn remove_inbox_duplicate(source: &Path, filename: &str) -> anyhow::Result<()> {
    match fs::remove_file(source) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow::anyhow!(
            "removing archived inbox duplicate {filename}: {e}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_grammar() {
        assert!(is_message_filename("1784649988123-abc23z.md"));
        assert!(is_message_filename("1784649988123-abciou.md")); // full [0-9a-z] grammar, not just Crockford
        assert!(!is_message_filename("1784649988123-abc23z.txt")); // not .md
        assert!(!is_message_filename("123-abc23z.md")); // ts not 13 digits
        assert!(!is_message_filename("1784649988123-ABC23Z.md")); // uppercase not allowed
        assert!(!is_message_filename("1784649988123-abc2.md")); // rand not 6 chars
        assert!(!is_message_filename("notes.md")); // outside message
    }

    #[test]
    fn rand6_is_six_crockford_chars() {
        let r = rand6();
        assert_eq!(r.len(), 6);
        assert!(r.bytes().all(|b| CROCKFORD.contains(&b)));
    }

    #[test]
    fn render_then_parse_roundtrips() {
        let tags = vec!["urgent".to_string(), "review".to_string()];
        let content = render_message("alice", Some("hi"), Some("123-x.md"), &tags, "hello\nworld");
        let m = parse_message("1784649988123-abc23z.md", &content);
        assert_eq!(m.from.as_deref(), Some("alice"));
        assert_eq!(m.subject.as_deref(), Some("hi"));
        assert_eq!(m.in_reply_to.as_deref(), Some("123-x.md"));
        assert_eq!(m.tags, vec!["urgent", "review"]);
        assert_eq!(m.body.trim_end(), "hello\nworld");
        assert_eq!(m.ts_ms, 1784649988123);
    }

    #[test]
    fn missing_frontmatter_is_permissive() {
        let m = parse_message("1784649988123-abc23z.md", "just a body, no frontmatter");
        assert_eq!(m.from, None);
        assert_eq!(m.body, "just a body, no frontmatter");
    }

    #[test]
    fn a_message_removed_after_enumeration_is_skipped_not_parsed_as_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("1784649988123-abc23z.md");
        fs::write(
            &path,
            render_message("alice", Some("work"), None, &[], "body"),
        )
        .unwrap();
        fs::remove_file(&path).unwrap();
        assert_eq!(read_message_contents(&path).unwrap(), None);
    }

    #[test]
    fn reply_subject_prefixes_once() {
        assert_eq!(reply_subject(Some("hi")).as_deref(), Some("re: hi"));
        assert_eq!(reply_subject(Some("re: hi")).as_deref(), Some("re: hi")); // no double-prefix
        assert_eq!(reply_subject(Some("RE: hi")).as_deref(), Some("RE: hi")); // case-insensitive
        assert_eq!(reply_subject(None), None);
    }

    #[test]
    fn send_list_read_archive_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        let archive = tmp.path().join("archive");

        let f1 = send_to_inbox(&inbox, "alice", Some("first"), None, &[], "hi bob").unwrap();
        // Force f2 into a strictly later millisecond so the send-order assertion below is meaningful —
        // within a single ms the `<ms>-<rand6>` wire format can't recover send order (ties break on the
        // random suffix). See `list_dir`.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let f2 = send_to_inbox(&inbox, "carol", None, Some(&f1), &[], "re: first").unwrap();
        assert!(is_message_filename(&f1) && is_message_filename(&f2));

        let listed = list_dir(&inbox).unwrap();
        assert_eq!(listed.len(), 2);
        // sorted by time — f1 before f2
        assert_eq!(listed[0].filename, f1);
        assert_eq!(listed[0].from.as_deref(), Some("alice"));
        assert_eq!(listed[1].in_reply_to.as_deref(), Some(f1.as_str()));

        let read = read_msg(&inbox, &f1).unwrap();
        assert_eq!(read.body.trim_end(), "hi bob");

        archive_msg(&inbox, &archive, &f1).unwrap();
        assert_eq!(list_dir(&inbox).unwrap().len(), 1);
        assert_eq!(list_dir(&archive).unwrap().len(), 1);
        assert!(!inbox.join(&f1).exists());
    }

    #[test]
    fn archive_receipt_suppresses_and_idempotently_cleans_a_restored_inbox_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("inbox");
        let archive = tmp.path().join("archive");
        let filename =
            send_to_inbox(&inbox, "alice", Some("once"), None, &[], "handle once").unwrap();

        archive_msg(&inbox, &archive, &filename).unwrap();
        let receipt = fs::read(archive.join(&filename)).unwrap();

        // Simulate a sync race restoring the old Present entry after the local archive.
        fs::create_dir_all(&inbox).unwrap();
        fs::write(inbox.join(&filename), b"stale inbox replica must not win").unwrap();
        assert_eq!(
            list_dir(&inbox).unwrap().len(),
            1,
            "the raw replica is present"
        );
        assert!(
            list_inbox(&inbox).unwrap().is_empty(),
            "the archive receipt suppresses it"
        );
        assert!(
            !inbox.join(&filename).exists(),
            "listing must also clean the shadowed inbox replica"
        );

        // Repeating archive is safe after listing already removed the duplicate.
        archive_msg(&inbox, &archive, &filename).unwrap();
        assert!(!inbox.join(&filename).exists());
        assert_eq!(fs::read(archive.join(&filename)).unwrap(), receipt);

        // Source already absent + receipt present is idempotent success too.
        archive_msg(&inbox, &archive, &filename).unwrap();
        assert_eq!(fs::read(archive.join(&filename)).unwrap(), receipt);
    }

    fn send_with_key(inbox: &Path, key: &str, body: &str) -> String {
        send_idempotent_to_inbox(
            inbox,
            "producer",
            Some("retryable message"),
            None,
            &[],
            body,
            key,
        )
        .unwrap()
    }

    #[test]
    fn idempotent_send_follows_inbox_archive_and_deletion_lifetime() {
        let temporary = tempfile::tempdir().unwrap();
        let inbox = temporary.path().join("inbox");
        let archive = temporary.path().join("archive");

        let first = send_with_key(&inbox, "daily-2026-07-31", "first body");
        let first_bytes = fs::read(inbox.join(&first)).unwrap();
        let first_modified = fs::metadata(inbox.join(&first))
            .unwrap()
            .modified()
            .unwrap();
        let parsed = read_msg(&inbox, &first).unwrap();
        assert_eq!(parsed.idempotency_key.as_deref(), Some("daily-2026-07-31"));

        std::thread::sleep(std::time::Duration::from_millis(5));
        let inbox_retry = send_with_key(&inbox, "daily-2026-07-31", "changed body");
        assert_eq!(inbox_retry, first);
        assert_eq!(list_dir(&inbox).unwrap().len(), 1);
        assert_eq!(fs::read(inbox.join(&first)).unwrap(), first_bytes);
        assert_eq!(
            fs::metadata(inbox.join(&first))
                .unwrap()
                .modified()
                .unwrap(),
            first_modified,
            "a retry must not rewrite the DING-triggering inbox file"
        );

        archive_msg(&inbox, &archive, &first).unwrap();
        let archived_retry = send_with_key(&inbox, "daily-2026-07-31", "third body");
        assert_eq!(archived_retry, first);
        assert!(list_dir(&inbox).unwrap().is_empty());
        assert_eq!(list_dir(&archive).unwrap().len(), 1);

        fs::remove_file(archive.join(&first)).unwrap();
        let after_delete = send_with_key(&inbox, "daily-2026-07-31", "new lifetime");
        assert_ne!(after_delete, first);
        assert_eq!(
            read_msg(&inbox, &after_delete).unwrap().body.trim_end(),
            "new lifetime"
        );
        assert!(!temporary.path().join("message-receipts").exists());
    }

    #[test]
    fn concurrent_retries_create_one_normal_message() {
        use std::sync::{Arc, Barrier};

        let temporary = tempfile::tempdir().unwrap();
        let inbox = Arc::new(temporary.path().join("inbox"));
        let workers = 12;
        let barrier = Arc::new(Barrier::new(workers));
        let mut threads = Vec::new();
        for index in 0..workers {
            let inbox = Arc::clone(&inbox);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                send_with_key(&inbox, "delivery-42", &format!("body {index}"))
            }));
        }
        let filenames: Vec<String> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert!(filenames.iter().all(|filename| filename == &filenames[0]));
        assert_eq!(list_dir(&inbox).unwrap().len(), 1);

        let other = send_with_key(&inbox, "delivery-43", "other");
        assert_ne!(other, filenames[0]);
        assert_eq!(list_dir(&inbox).unwrap().len(), 2);
    }

    #[test]
    fn retry_recovers_at_each_message_publication_crash_seam() {
        for crash in [
            IdempotentSendSeam::BeforePublication,
            IdempotentSendSeam::AfterPublication,
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let inbox = temporary.path().join("inbox");
            let failed = send_idempotent_to_inbox_with_seam(
                &inbox,
                "producer",
                Some("crash seam"),
                None,
                &[],
                "first body",
                "alert-1",
                |seam| {
                    if seam == crash {
                        anyhow::bail!("injected crash at {seam:?}");
                    }
                    Ok(())
                },
            );
            assert!(failed.is_err(), "{crash:?}");

            let retry = send_with_key(&inbox, "alert-1", "retry body");
            assert_eq!(list_dir(&inbox).unwrap().len(), 1, "{crash:?}");
            let message = read_msg(&inbox, &retry).unwrap();
            let expected_body = match crash {
                IdempotentSendSeam::BeforePublication => "retry body",
                IdempotentSendSeam::AfterPublication => "first body",
            };
            assert_eq!(message.body.trim_end(), expected_body, "{crash:?}");
        }
    }

    #[test]
    fn inbox_match_wins_before_archive_match() {
        let temporary = tempfile::tempdir().unwrap();
        let inbox = temporary.path().join("inbox");
        let archive = temporary.path().join("archive");
        let archived = send_with_key(&inbox, "same", "archived");
        archive_msg(&inbox, &archive, &archived).unwrap();

        let contents = render_message_fields("producer", None, None, &[], "inbox", Some("same"));
        let inbox_copy = publish_message_with_seam(&inbox, &contents, |_| Ok(())).unwrap();
        assert_ne!(inbox_copy, archived);

        let selected = send_with_key(&inbox, "same", "retry");
        assert_eq!(selected, inbox_copy);
    }

    #[test]
    fn resolve_inbox_falls_back_to_the_flat_bus_when_catalog_less() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // No catalog under root → the flat bus (<root>/<id>/inbox|archive).
        assert_eq!(
            resolve_inbox(root, "mix.sup", "h").unwrap(),
            root.join("mix.sup").join("inbox")
        );
        assert_eq!(
            resolve_archive(root, "mix.sup", "h").unwrap(),
            root.join("mix.sup").join("archive")
        );
        // A discoverable native catalog agent → its resources/inbox.
        let ad = root.join("h").join("mix.sup");
        std::fs::create_dir_all(&ad).unwrap();
        std::fs::write(
            ad.join("agent.kdl"),
            "agent \"mix.sup\" {\n  identity \"mix.sup\"\n  name \"Shared Worker\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" { command \"x\" }\n}\n",
        )
        .unwrap();
        assert_eq!(
            resolve_inbox(root, "mix.sup", "h").unwrap(),
            ad.join("resources").join("inbox")
        );
        assert!(resolve_inbox(root, "Shared Worker", "h").is_err());

        let external = ExternalInbox::new(root, "requester").unwrap();
        assert!(resolve_inbox_with_external(root, "requester", "h", Some(&external)).is_err());

        let requester = root.join("requester").join("inbox");
        std::fs::create_dir_all(&requester).unwrap();
        assert!(resolve_inbox(root, "requester", "h").is_err());
        assert_eq!(
            resolve_inbox_with_external(root, "requester", "h", Some(&external)).unwrap(),
            requester
        );
        assert!(resolve_inbox_with_external(root, "missing", "h", Some(&external)).is_err());
    }

    #[test]
    fn external_inbox_rejects_unsafe_or_nested_identities() {
        let tmp = tempfile::tempdir().unwrap();
        for identity in [
            "",
            ".",
            "..",
            "nested/requester",
            "../requester",
            "/requester",
        ] {
            assert!(
                ExternalInbox::new(tmp.path(), identity).is_err(),
                "accepted unsafe external identity {identity:?}"
            );
        }
    }
}
