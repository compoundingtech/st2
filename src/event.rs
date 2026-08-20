//! Declared event-stream ingress.
//!
//! Events are ordinary inbox files with additional provenance frontmatter. Publication has its
//! own bounded, agent-local receipt ring rather than writing the agent's immutable Sent ledger.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::message;

const EVENT_VERSION: u32 = 1;
pub const RING_CAPACITY: usize = 128;
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamEntry {
    event_id: String,
    filename: String,
    key: Option<String>,
    rendered_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamPending {
    event_id: String,
    filename: String,
    key: Option<String>,
    rendered_sha256: String,
    supersede: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamRecord {
    version: u32,
    stream: String,
    recipient: String,
    pending: Option<StreamPending>,
    recent: Vec<StreamEntry>,
}

impl StreamRecord {
    fn fresh(stream: &str, recipient: &str) -> Self {
        Self {
            version: EVENT_VERSION,
            stream: stream.to_owned(),
            recipient: recipient.to_owned(),
            pending: None,
            recent: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventReceipt {
    pub recipient: String,
    pub stream: String,
    pub event_id: String,
    pub filename: String,
    pub status: EventReceiptStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventReceiptStatus {
    Created,
    Deduplicated,
}

struct ResolvedStream {
    recipient: String,
}

fn resolve_stream(
    root: &Path,
    this_host: &str,
    recipient: &str,
    stream: &str,
) -> anyhow::Result<ResolvedStream> {
    let discovered = crate::discover_strict(root);
    anyhow::ensure!(
        discovered.errors.is_empty(),
        "catalog has errors; refusing event publication: {}",
        discovered
            .errors
            .iter()
            .map(|error| format!("{}: {}", error.path.display(), error.message))
            .collect::<Vec<_>>()
            .join("; ")
    );
    let mut matches = discovered
        .specs
        .into_iter()
        .filter(|spec| {
            spec.bus_id(this_host) == recipient
                || (spec.resolved_host(this_host) == this_host && spec.identity == recipient)
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !matches.is_empty(),
        "no agent '{recipient}' found in catalog {}",
        root.display()
    );
    anyhow::ensure!(
        matches.len() == 1,
        "agent recipient '{recipient}' is ambiguous; matched {} declarations: {}",
        matches.len(),
        matches
            .iter()
            .map(|spec| spec.path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let spec = matches
        .pop()
        .context("exactly one matching agent expected")?;
    anyhow::ensure!(
        spec.streams.iter().any(|declared| declared.name == stream),
        "agent '{}' does not declare stream '{stream}'",
        spec.bus_id(this_host)
    );
    anyhow::ensure!(
        spec.desired_state.is_running(),
        "agent '{}' is {}; refusing event while its eyes are closed",
        spec.bus_id(this_host),
        spec.desired_state.as_str()
    );
    Ok(ResolvedStream {
        recipient: spec.bus_id(this_host),
    })
}

pub fn render_event(
    from: &str,
    subject: Option<&str>,
    stream: &str,
    event_id: &str,
    key: Option<&str>,
    body: &str,
) -> String {
    let mut rendered = String::from("---\n");
    rendered.push_str(&format!("from: {from}\n"));
    if let Some(subject) = subject {
        rendered.push_str(&format!("subject: {subject}\n"));
    }
    rendered.push_str(&format!("stream: {stream}\n"));
    rendered.push_str(&format!("event-id: {event_id}\n"));
    if let Some(key) = key {
        rendered.push_str(&format!("key: {key}\n"));
    }
    rendered.push_str("---\n");
    rendered.push_str(body);
    if !body.ends_with('\n') {
        rendered.push('\n');
    }
    rendered
}

#[allow(clippy::too_many_arguments)]
pub fn emit(
    root: &Path,
    this_host: &str,
    recipient: &str,
    stream: &str,
    event_id: &str,
    key: Option<&str>,
    subject: Option<&str>,
    body: &str,
    supersede: bool,
) -> anyhow::Result<EventReceipt> {
    validate_component("stream", stream)?;
    validate_component("event id", event_id)?;
    if let Some(key) = key {
        validate_component("event key", key)?;
    }
    if let Some(subject) = subject {
        validate_header("event subject", subject, 1_000)?;
    }
    // Serialize the eligibility observation with self-authoring and desired-state changes. Once a
    // suspension edit owns this lock, no later emit can publish from a stale running observation.
    let _catalog_lock = crate::catalog_lock::CatalogLock::exclusive(root)?;
    let resolved = resolve_stream(root, this_host, recipient, stream)?;
    let canonical_recipient = resolved.recipient;
    let from = format!("{canonical_recipient}/{stream}");
    let rendered = render_event(&from, subject, stream, event_id, key, body);
    message::with_resolved_state_dir(
        root,
        &canonical_recipient,
        this_host,
        &["resources", "streams", stream],
        true,
        |state_dir| {
            let _lock = StreamLock::exclusive(state_dir)?;
            let record_path = state_dir.join("state.json");
            let mut record = read_record(&record_path)?
                .unwrap_or_else(|| StreamRecord::fresh(stream, &canonical_recipient));
            anyhow::ensure!(
                record.version == EVENT_VERSION
                    && record.stream == stream
                    && record.recipient == canonical_recipient,
                "stream state for '{}#{stream}' is not readable at version {EVENT_VERSION}",
                canonical_recipient
            );

            if record
                .pending
                .as_ref()
                .is_some_and(|pending| pending.event_id != event_id)
            {
                let pending = record
                    .pending
                    .take()
                    .expect("different pending event was just observed");
                let materialized = message::with_resolved_message_boxes(
                    root,
                    &canonical_recipient,
                    this_host,
                    |inbox, archive| {
                        Ok(message_entry_exists(inbox, &pending.filename)?
                            || message_entry_exists(archive, &pending.filename)?)
                    },
                )?;
                if materialized {
                    record.recent.insert(
                        0,
                        StreamEntry {
                            event_id: pending.event_id,
                            filename: pending.filename,
                            key: pending.key,
                            rendered_sha256: pending.rendered_sha256,
                        },
                    );
                    record.recent.truncate(RING_CAPACITY);
                }
                write_record(&record_path, &record)?;
            }

            let rendered_sha256 = hex_digest(rendered.as_bytes());
            if let Some(entry) = record
                .recent
                .iter()
                .find(|entry| entry.event_id == event_id)
            {
                anyhow::ensure!(
                    entry.rendered_sha256 == rendered_sha256,
                    "event identity `{stream}#{event_id}` reused with different content"
                );
                return Ok(EventReceipt {
                    recipient: canonical_recipient.clone(),
                    stream: stream.to_owned(),
                    event_id: event_id.to_owned(),
                    filename: entry.filename.clone(),
                    status: EventReceiptStatus::Deduplicated,
                    superseded: None,
                });
            }

            let (filename, resumed) = match record.pending.as_ref() {
                Some(pending) if pending.event_id == event_id => {
                    anyhow::ensure!(
                        pending.rendered_sha256 == rendered_sha256
                            && pending.key.as_deref() == key
                            && pending.supersede == supersede,
                        "event identity `{stream}#{event_id}` reused with different content"
                    );
                    (pending.filename.clone(), true)
                }
                Some(pending) => anyhow::bail!(
                    "stream '{stream}' has an interrupted event '{}'; replay it before publishing another",
                    pending.event_id
                ),
                None => (message::new_filename(), false),
            };
            if !resumed {
                record.pending = Some(StreamPending {
                    event_id: event_id.to_owned(),
                    filename: filename.clone(),
                    key: key.map(str::to_owned),
                    rendered_sha256: rendered_sha256.clone(),
                    supersede,
                });
                write_record(&record_path, &record)?;
                test_event_checkpoint(event_id, "pending")?;
            }

            let predecessor_candidates = if supersede {
                record
                    .recent
                    .iter()
                    .filter(|entry| {
                        entry.filename != filename
                            && key.is_none_or(|key| entry.key.as_deref() == Some(key))
                    })
                    .map(|entry| entry.filename.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let mut predecessor = None;
            let created = message::with_resolved_message_boxes(
                root,
                &canonical_recipient,
                this_host,
                |inbox, archive| {
                    // Publish before compacting. If predecessor archival fails or the process
                    // crashes between these operations, both records remain unread; replaying the
                    // durable pending reservation completes compaction without risking a lost wake.
                    let created = if archive.join(&filename).is_file() {
                        false
                    } else {
                        message::materialize_message_once(inbox, &filename, &rendered)?
                    };
                    test_event_checkpoint(event_id, "materialized")?;
                    if let Some(unread) = predecessor_candidates
                        .iter()
                        .find(|candidate| inbox.join(candidate).is_file())
                    {
                        message::archive_msg(inbox, archive, unread)?;
                        predecessor = Some(unread.clone());
                    }
                    Ok(created)
                },
            )?;

            record.pending = None;
            record.recent.insert(
                0,
                StreamEntry {
                    event_id: event_id.to_owned(),
                    filename: filename.clone(),
                    key: key.map(str::to_owned),
                    rendered_sha256,
                },
            );
            record.recent.truncate(RING_CAPACITY);
            write_record(&record_path, &record)?;

            Ok(EventReceipt {
                recipient: canonical_recipient.clone(),
                stream: stream.to_owned(),
                event_id: event_id.to_owned(),
                filename,
                status: if created {
                    EventReceiptStatus::Created
                } else {
                    EventReceiptStatus::Deduplicated
                },
                superseded: predecessor,
            })
        },
    )
}

fn message_entry_exists(directory: &Path, filename: &str) -> anyhow::Result<bool> {
    anyhow::ensure!(
        message::is_message_filename(filename),
        "invalid pending message filename {filename:?}"
    );
    match fs::symlink_metadata(directory.join(filename)) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "pending message entry {filename:?} is not a real regular file"
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(debug_assertions)]
fn test_event_checkpoint(event_id: &str, point: &str) -> anyhow::Result<()> {
    if std::env::var("ST2_TEST_EVENT_FAIL_AT").as_deref()
        == Ok(format!("{event_id}:{point}").as_str())
    {
        anyhow::bail!("injected event failure at {point}");
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn test_event_checkpoint(_event_id: &str, _point: &str) -> anyhow::Result<()> {
    Ok(())
}

fn validate_component(label: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 200
            && value.trim() == value
            && !value.chars().any(char::is_control),
        "{label} must be 1..=200 bytes without surrounding whitespace or controls"
    );
    Ok(())
}

fn validate_header(label: &str, value: &str, max_bytes: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= max_bytes
            && value.trim() == value
            && !value.chars().any(char::is_control),
        "{label} must be 1..={max_bytes} bytes without surrounding whitespace or controls"
    );
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_record(path: &Path) -> anyhow::Result<Option<StreamRecord>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
            format!("stream state {} is malformed", path.display())
        })?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_record(path: &Path, record: &StreamRecord) -> anyhow::Result<()> {
    use std::ffi::CString;

    let parent = path.parent().context("stream state has no parent")?;
    fs::create_dir_all(parent)?;
    let directory = File::open(parent)
        .with_context(|| format!("open stream state directory {}", parent.display()))?;
    let temporary = format!(
        ".state.tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let temporary = CString::new(temporary)?;
    let target = CString::new(
        path.file_name()
            .context("stream state has no filename")?
            .as_encoded_bytes(),
    )?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            temporary.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "create fresh stream state temporary in {}",
                parent.display()
            )
        });
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    let result = (|| -> anyhow::Result<()> {
        file.write_all(&serde_json::to_vec(record)?)?;
        file.sync_all()?;
        let renamed = unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                temporary.as_ptr(),
                directory.as_raw_fd(),
                target.as_ptr(),
            )
        };
        if renamed != 0 {
            return Err(std::io::Error::last_os_error()).context("publish stream state atomically");
        }
        directory.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        unsafe {
            libc::unlinkat(directory.as_raw_fd(), temporary.as_ptr(), 0);
        }
    }
    result?;
    Ok(())
}

struct StreamLock(File);

impl StreamLock {
    fn exclusive(state_dir: &Path) -> anyhow::Result<Self> {
        use std::os::fd::AsRawFd as _;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(state_dir.join(".lock"))?;
        anyhow::ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0,
            "locking stream state failed"
        );
        Ok(Self(file))
    }
}

impl Drop for StreamLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd as _;
        unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}
