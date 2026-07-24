//! Native message bus (M2) — the send/receive core that replaces smalltalk's `st`.
//!
//! st2 owns messaging directly now: a message is a markdown file with YAML frontmatter, named
//! `<unix-ms>-<rand6>.md`, written into the recipient agent's `resources/inbox/` (VRS §5). The
//! **recipient is implied by the path**; `from` lives in the frontmatter; the filename's ms prefix is
//! the send time. Send = write the file. Archive = rename inbox→archive. Nobody mutates a file after
//! creation (append-only + rename-only). The on-disk format is kept wire-compatible with smalltalk so
//! the two interoperate during the migration and tooling/agents port cleanly.
//!
//! This module is location-agnostic: it operates on an inbox/agent directory a caller resolves (from
//! the catalog for VRS-native, or `$ST_ROOT` for a compat shim).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// The alphabet st2 *generates* `<rand6>` from — Crockford base32 (`0-9a-z` minus `i l o u`). This is
/// a strict subset of what the reader accepts: the frozen bus grammar is `[0-9a-z]{6}`, so a peer /
/// smalltalk message may legally use i/l/o/u — [`is_message_filename`] must not reject those.
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
    /// The markdown body.
    pub body: String,
}

/// Current unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// A fresh canonical `<unix-ms>-<rand6>.md` filename (the shared grammar — messages AND context
/// decision entries use it, so both sort chronologically by name).
pub fn new_filename() -> String {
    format!("{}-{}.md", now_ms(), rand6())
}

/// Six Crockford-base32 chars from `/dev/urandom` (falls back to a time-derived value).
fn rand6() -> String {
    let mut buf = [0u8; 6];
    let ok = fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut buf)).is_ok();
    if !ok {
        // Degenerate fallback — mix the ns clock so it isn't all-zeros.
        let n = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (n >> (i * 8)) as u8;
        }
    }
    buf.iter().map(|b| CROCKFORD[(*b as usize) % 32] as char).collect()
}

/// True if `name` matches the frozen bus grammar `^[0-9]{13}-[0-9a-z]{6}\.md$` (smalltalk LAYOUT-004).
/// The reader accepts the FULL `[0-9a-z]` rand6 alphabet — st2 generates a Crockford subset, but a
/// peer/smalltalk message may use any lowercase alnum and dropping those would lose real messages.
pub fn is_message_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".md") else { return false };
    let Some((ts, rand)) = stem.split_once('-') else { return false };
    ts.len() == 13
        && ts.bytes().all(|b| b.is_ascii_digit())
        && rand.len() == 6
        && rand.bytes().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

/// Render a message file's contents (frontmatter + body).
pub fn render_message(
    from: &str,
    subject: Option<&str>,
    in_reply_to: Option<&str>,
    tags: &[String],
    body: &str,
) -> String {
    let mut s = String::from("---\n");
    s.push_str(&format!("from: {from}\n"));
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
        body: String::new(),
    };

    // Frontmatter is an opening `---` line … a closing `---` line.
    let rest = contents.strip_prefix("---\n").or_else(|| contents.strip_prefix("---\r\n"));
    if let Some(rest) = rest
        && let Some(end) = rest.find("\n---")
    {
        let front = &rest[..end];
        // body starts after the closing `---` line
        let after = &rest[end + 1..]; // at the `---` line
        let body = after.split_once('\n').map(|x| x.1).unwrap_or("");
        for line in front.lines() {
            let Some((k, v)) = line.split_once(':') else { continue };
            let v = v.trim();
            match k.trim() {
                "from" => msg.from = Some(v.to_string()),
                "subject" => msg.subject = Some(v.to_string()),
                "in-reply-to" => msg.in_reply_to = Some(v.to_string()),
                "tags" => msg.tags = v.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect(),
                "priority" => msg.priority = Some(v.to_string()),
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
    for _ in 0..8 {
        let filename = new_filename();
        let path = inbox_dir.join(&filename);
        if !path.exists() {
            fs::write(&path, &contents)?;
            return Ok(filename);
        }
    }
    anyhow::bail!("could not allocate a unique message filename in {}", inbox_dir.display())
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
        let contents = fs::read_to_string(entry.path()).unwrap_or_default();
        msgs.push(parse_message(&name, &contents));
    }
    // Primary order is send time; the `<rand6>` suffix is a deterministic tiebreak so two messages
    // that land in the same millisecond still list in a stable, reproducible order (the wire format
    // can't recover true send-order within a millisecond — this at least isn't `read_dir`-arbitrary).
    msgs.sort_by(|a, b| a.ts_ms.cmp(&b.ts_ms).then_with(|| a.filename.cmp(&b.filename)));
    Ok(msgs)
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

/// The inbox dir for `id` under `root`: the NATIVE catalog inbox (`<agent_dir>/resources/inbox`) if a
/// catalog agent is discoverable, else the FLAT smalltalk-style bus inbox (`<root>/<id>/inbox`). The
/// flat fallback lets `st2 ding`/`st2 message` operate on a catalog-LESS bus — e.g. an eval's ST_ROOT,
/// where agents are booted from a single spec (no on-disk `agent.kdl` to discover). `root` is whatever
/// `--root`/ST_ROOT names, so the layout follows the spec, never a hardcoded path.
pub fn resolve_inbox(root: &Path, id: &str, host: &str) -> PathBuf {
    match resolve_agent_dir(root, id, host) {
        Some(dir) => inbox_dir(&dir),
        None => root.join(id).join("inbox"),
    }
}

/// The archive dir for `id` under `root` — native catalog archive if discoverable, else the flat
/// `<root>/<id>/archive` (companion to [`resolve_inbox`]).
pub fn resolve_archive(root: &Path, id: &str, host: &str) -> PathBuf {
    match resolve_agent_dir(root, id, host) {
        Some(dir) => archive_dir(&dir),
        None => root.join(id).join("archive"),
    }
}

/// Resolve a recipient (a bus id `<host>.<id>` or a bare identity) to its agent folder in the
/// catalog, via content discovery. Returns `None` if no agent matches.
pub fn resolve_agent_dir(catalog_root: &Path, recipient: &str, this_host: &str) -> Option<PathBuf> {
    let found = crate::discover(catalog_root);
    found
        .specs
        .into_iter()
        .find(|s| s.bus_id(this_host) == recipient || s.identity == recipient)
        .and_then(|s| s.path.parent().map(Path::to_path_buf))
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
pub fn collect_thread(catalog_root: &Path, filename: &str) -> Vec<ThreadEntry> {
    // Gather every message once (dedup by filename — the same file can appear in two boxes).
    let found = crate::discover(catalog_root);
    let mut all: HashMap<String, Message> = HashMap::new();
    for spec in &found.specs {
        let Some(dir) = spec.path.parent() else { continue };
        for d in [inbox_dir(dir), archive_dir(dir)] {
            for m in list_dir(&d).unwrap_or_default() {
                all.entry(m.filename.clone()).or_insert(m);
            }
        }
    }
    if !all.contains_key(filename) {
        return Vec::new();
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
    out
}

/// Archive: rename `<agent_dir>/inbox/<filename>` → `<agent_dir>/archive/<filename>`.
pub fn archive_msg(inbox_dir: &Path, archive_dir: &Path, filename: &str) -> anyhow::Result<()> {
    fs::create_dir_all(archive_dir)?;
    fs::rename(inbox_dir.join(filename), archive_dir.join(filename))
        .map_err(|e| anyhow::anyhow!("archiving {filename}: {e}"))?;
    Ok(())
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
    fn resolve_inbox_falls_back_to_the_flat_bus_when_catalog_less() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // No catalog under root → the flat smalltalk-style bus (<root>/<id>/inbox|archive).
        assert_eq!(resolve_inbox(root, "mix.sup", "h"), root.join("mix.sup").join("inbox"));
        assert_eq!(resolve_archive(root, "mix.sup", "h"), root.join("mix.sup").join("archive"));
        // A discoverable native catalog agent → its resources/inbox.
        let ad = root.join("h").join("mix.sup");
        std::fs::create_dir_all(&ad).unwrap();
        std::fs::write(
            ad.join("agent.kdl"),
            "agent \"mix.sup\" {\n  identity \"mix.sup\"\n  host \"h\"\n  type \"service\"\n  pty \"agent\" { command \"x\" }\n}\n",
        )
        .unwrap();
        assert_eq!(resolve_inbox(root, "mix.sup", "h"), ad.join("resources").join("inbox"));
    }
}
