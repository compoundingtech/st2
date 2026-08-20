//! Declared event-stream ingress.
//!
//! Events are ordinary inbox files with additional provenance frontmatter. Publication has its
//! own bounded, agent-local receipt ring rather than writing the agent's immutable Sent ledger.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::message;

const EVENT_VERSION: u32 = 1;
const MAX_STATE_BYTES: u64 = 1_048_576;
pub const RING_CAPACITY: usize = 128;
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamEntry {
    event_id: String,
    filename: String,
    key: Option<String>,
    rendered_sha256: String,
    #[serde(default)]
    supersede: bool,
    #[serde(default)]
    predecessor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamPending {
    event_id: String,
    filename: String,
    key: Option<String>,
    rendered_sha256: String,
    supersede: bool,
    #[serde(default)]
    predecessor: Option<StreamEntry>,
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamOwnerBinding {
    schema: String,
    logical_host: String,
    catalog_lock_device: u64,
    catalog_lock_inode: u64,
    supervisor_pid: u32,
    supervisor_start_time_ticks: u64,
}

pub(crate) fn publish_owner_binding(root: &Path, host: &str) -> anyhow::Result<()> {
    let lock = crate::catalog_lock::CatalogLock::shared(root)?;
    let metadata = lock.control().metadata()?;
    let binding = StreamOwnerBinding {
        schema: "st2.stream-owner.v1".to_owned(),
        logical_host: host.to_owned(),
        catalog_lock_device: metadata.dev(),
        catalog_lock_inode: metadata.ino(),
        supervisor_pid: std::process::id(),
        supervisor_start_time_ticks: crate::exec_backend::process_start_time_ticks(
            std::process::id() as i32,
        )?,
    };
    let path = crate::park::SupervisorScope::current(root, host)?.stream_owner_binding_path();
    fs::create_dir_all(path.parent().context("owner binding has no parent")?)?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&temporary, serde_json::to_vec(&binding)?)?;
    File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, &path)?;
    File::open(path.parent().context("owner binding has no parent")?)?.sync_all()?;
    Ok(())
}

pub(crate) fn clear_owner_binding(root: &Path, host: &str) {
    let Ok(path) = crate::park::SupervisorScope::current(root, host)
        .map(|scope| scope.stream_owner_binding_path())
    else {
        return;
    };
    let Ok(bytes) = fs::read(&path) else { return };
    let Ok(binding) = serde_json::from_slice::<StreamOwnerBinding>(&bytes) else {
        return;
    };
    if binding.supervisor_pid == std::process::id()
        && crate::exec_backend::process_start_time_ticks(std::process::id() as i32).ok()
            == Some(binding.supervisor_start_time_ticks)
    {
        let _ = fs::remove_file(path);
    }
}

#[doc(hidden)]
pub fn publish_owner_binding_for_test(root: &Path, host: &str) -> anyhow::Result<()> {
    publish_owner_binding(root, host)
}

fn validate_owner_binding(
    root: &Path,
    host: &str,
    lock: &crate::CatalogLock,
) -> anyhow::Result<()> {
    let path = crate::park::SupervisorScope::current(root, host)?.stream_owner_binding_path();
    let binding: StreamOwnerBinding = serde_json::from_slice(
        &fs::read(&path)
            .with_context(|| format!("no active local stream owner binding for host '{host}'"))?,
    )?;
    let metadata = lock.control().metadata()?;
    anyhow::ensure!(
        binding.schema == "st2.stream-owner.v1"
            && binding.logical_host == host
            && binding.catalog_lock_device == metadata.dev()
            && binding.catalog_lock_inode == metadata.ino(),
        "stream owner binding does not match this catalog lock domain"
    );
    anyhow::ensure!(
        crate::host_lock::process_alive(binding.supervisor_pid as i32)
            && crate::exec_backend::process_start_time_ticks(binding.supervisor_pid as i32).ok()
                == Some(binding.supervisor_start_time_ticks),
        "stream owner binding for host '{host}' is stale"
    );
    Ok(())
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
        spec.resolved_host(this_host) == this_host,
        "agent '{}' is owned by host '{}'; event publication must run on that host",
        spec.bus_id(this_host),
        spec.resolved_host(this_host)
    );
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
    let catalog_lock = crate::catalog_lock::CatalogLock::shared(root)?;
    validate_owner_binding(root, this_host, &catalog_lock)?;
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
                        let inbox_bytes = read_message_entry(inbox, &pending.filename)?;
                        let archive_bytes = read_message_entry(archive, &pending.filename)?;
                        if let Some(bytes) = inbox_bytes.as_deref() {
                            validate_pending_message(stream, &pending, bytes)?;
                        }
                        if let Some(bytes) = archive_bytes.as_deref() {
                            validate_pending_message(stream, &pending, bytes)?;
                        }
                        if inbox_bytes.is_some() || archive_bytes.is_some() {
                            if let Some(predecessor) = pending.predecessor.as_ref() {
                                finish_predecessor(stream, predecessor, inbox, archive)?;
                            }
                            Ok(true)
                        } else {
                            Ok(false)
                        }
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
                            supersede: pending.supersede,
                            predecessor: pending
                                .predecessor
                                .as_ref()
                                .map(|entry| entry.filename.clone()),
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
                anyhow::ensure!(
                    entry.supersede == supersede,
                    "event identity `{stream}#{event_id}` reused with different supersession intent"
                );
                return Ok(EventReceipt {
                    recipient: canonical_recipient.clone(),
                    stream: stream.to_owned(),
                    event_id: event_id.to_owned(),
                    filename: entry.filename.clone(),
                    status: EventReceiptStatus::Deduplicated,
                    superseded: entry.predecessor.clone(),
                });
            }

            let (filename, resumed, predecessor) = match record.pending.as_ref() {
                Some(pending) if pending.event_id == event_id => {
                    anyhow::ensure!(
                        pending.rendered_sha256 == rendered_sha256
                            && pending.key.as_deref() == key
                            && pending.supersede == supersede,
                        "event identity `{stream}#{event_id}` reused with different content"
                    );
                    (pending.filename.clone(), true, pending.predecessor.clone())
                }
                Some(pending) => anyhow::bail!(
                    "stream '{stream}' has an interrupted event '{}'; replay it before publishing another",
                    pending.event_id
                ),
                None => {
                    let predecessor = if supersede {
                        message::with_resolved_message_boxes(
                            root,
                            &canonical_recipient,
                            this_host,
                            |inbox, archive| {
                                for entry in record.recent.iter().filter(|entry| {
                                    key.is_none_or(|key| entry.key.as_deref() == Some(key))
                                }) {
                                    if read_message_entry(inbox, &entry.filename)?.is_some()
                                        && read_message_entry(archive, &entry.filename)?.is_none()
                                    {
                                        return Ok(Some(entry.clone()));
                                    }
                                }
                                Ok(None)
                            },
                        )?
                    } else {
                        None
                    };
                    (message::new_filename(), false, predecessor)
                }
            };
            if !resumed {
                record.pending = Some(StreamPending {
                    event_id: event_id.to_owned(),
                    filename: filename.clone(),
                    key: key.map(str::to_owned),
                    rendered_sha256: rendered_sha256.clone(),
                    supersede,
                    predecessor: predecessor.clone(),
                });
                write_record(&record_path, &record)?;
                test_event_checkpoint(event_id, "pending")?;
            }

            let created = message::with_resolved_message_boxes(
                root,
                &canonical_recipient,
                this_host,
                |inbox, archive| {
                    // Publish before compacting. If predecessor archival fails or the process
                    // crashes between these operations, both records remain unread; replaying the
                    // durable pending reservation completes compaction without risking a lost wake.
                    let (created, published_dir) = match read_message_entry(archive, &filename)? {
                        Some(bytes) => {
                            anyhow::ensure!(
                                bytes == rendered.as_bytes(),
                                "archived pending event '{}#{}' has different bytes",
                                stream,
                                event_id
                            );
                            (false, archive)
                        }
                        None => match read_message_entry(inbox, &filename)? {
                            Some(bytes) => {
                                validate_pending_message(
                                    stream,
                                    record.pending.as_ref().expect("pending reservation exists"),
                                    &bytes,
                                )?;
                                (false, inbox)
                            }
                            None => (
                                message::materialize_message_once(inbox, &filename, &rendered)?,
                                inbox,
                            ),
                        },
                    };
                    sync_message_entry(published_dir, &filename)?;
                    test_event_checkpoint(event_id, "durable")?;
                    test_event_checkpoint(event_id, "materialized")?;
                    if let Some(predecessor) = predecessor.as_ref() {
                        finish_predecessor(stream, predecessor, inbox, archive)?;
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
                    supersede,
                    predecessor: predecessor.as_ref().map(|entry| entry.filename.clone()),
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
                superseded: predecessor.map(|entry| entry.filename),
            })
        },
    )
}

fn read_message_entry(directory: &Path, filename: &str) -> anyhow::Result<Option<Vec<u8>>> {
    Ok(open_message_entry(directory, filename)?.map(|(_, bytes)| bytes))
}

fn open_message_entry(directory: &Path, filename: &str) -> anyhow::Result<Option<(File, Vec<u8>)>> {
    anyhow::ensure!(
        message::is_message_filename(filename),
        "invalid pending message filename {filename:?}"
    );
    let path = directory.join(filename);
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
    {
        Ok(mut file) => {
            let metadata = file.metadata()?;
            anyhow::ensure!(
                metadata.is_file(),
                "pending message entry {filename:?} is not a real regular file"
            );
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            Ok(Some((file, bytes)))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn validate_pending_message(
    stream: &str,
    pending: &StreamPending,
    bytes: &[u8],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        hex_digest(bytes) == pending.rendered_sha256,
        "pending event '{}#{}' reserved file has different bytes",
        stream,
        pending.event_id
    );
    let contents = std::str::from_utf8(bytes).context("pending event record is not UTF-8")?;
    let parsed = message::parse_message(&pending.filename, contents);
    anyhow::ensure!(
        parsed.stream.as_deref() == Some(stream)
            && parsed.event_id.as_deref() == Some(pending.event_id.as_str())
            && parsed.event_key.as_deref() == pending.key.as_deref(),
        "pending event '{}#{}' reserved file has different event identity",
        stream,
        pending.event_id
    );
    Ok(())
}

fn sync_message_entry(directory: &Path, filename: &str) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(directory.join(filename))?;
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "event publication is not a regular file"
    );
    file.sync_all()?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn finish_predecessor(
    stream: &str,
    predecessor: &StreamEntry,
    inbox: &Path,
    archive: &Path,
) -> anyhow::Result<()> {
    let inbox_entry = open_message_entry(inbox, &predecessor.filename)?;
    let archive_bytes = read_message_entry(archive, &predecessor.filename)?;
    if let Some((_, bytes)) = inbox_entry.as_ref() {
        validate_retained_message(stream, predecessor, bytes)?;
    }
    if let Some(bytes) = archive_bytes.as_deref() {
        validate_retained_message(stream, predecessor, bytes)?;
    }
    anyhow::ensure!(
        inbox_entry.is_some() || archive_bytes.is_some(),
        "supersession predecessor '{}#{}' has no inbox file or archive receipt",
        stream,
        predecessor.event_id
    );
    if archive_bytes.is_none()
        && let Some((file, _)) = inbox_entry.as_ref()
    {
        test_predecessor_archive_checkpoint()?;
        archive_validated_file(file, inbox, archive, &predecessor.filename)?;
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn test_predecessor_archive_checkpoint() -> anyhow::Result<()> {
    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_EVENT_ARCHIVE_READY"),
        std::env::var("ST2_TEST_EVENT_ARCHIVE_RELEASE"),
    ) else {
        return Ok(());
    };
    fs::write(&ready, b"validated")?;
    while !Path::new(&release).exists() {
        std::thread::yield_now();
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn test_predecessor_archive_checkpoint() -> anyhow::Result<()> {
    Ok(())
}

fn archive_validated_file(
    file: &File,
    inbox: &Path,
    archive: &Path,
    filename: &str,
) -> anyhow::Result<()> {
    use std::ffi::CString;

    fs::create_dir_all(archive)?;
    let archive_dir = File::open(archive)?;
    #[cfg(target_os = "linux")]
    let capability = format!("/proc/self/fd/{}", file.as_raw_fd());
    #[cfg(not(target_os = "linux"))]
    let capability = format!("/dev/fd/{}", file.as_raw_fd());
    let capability = CString::new(capability)?;
    let filename_c = CString::new(std::ffi::OsStr::new(filename).as_bytes())?;
    let result = unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            capability.as_ptr(),
            archive_dir.as_raw_fd(),
            filename_c.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error).context("archive the validated predecessor capability");
        }
    }
    let archived = read_message_entry(archive, filename)?
        .context("validated predecessor archive receipt disappeared")?;
    let mut source = file.try_clone()?;
    source.rewind()?;
    let mut expected = Vec::new();
    source.read_to_end(&mut expected)?;
    anyhow::ensure!(archived == expected, "archive receipt has different bytes");
    File::open(archive.join(filename))?.sync_all()?;
    archive_dir.sync_all()?;
    let validated = file.metadata()?;
    match fs::symlink_metadata(inbox.join(filename)) {
        Ok(current) if current.dev() == validated.dev() && current.ino() == validated.ino() => {
            fs::remove_file(inbox.join(filename))?;
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect predecessor before conditional unlink"),
    }
    File::open(inbox)?.sync_all()?;
    Ok(())
}

fn validate_retained_message(
    stream: &str,
    entry: &StreamEntry,
    bytes: &[u8],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        hex_digest(bytes) == entry.rendered_sha256,
        "supersession predecessor '{}#{}' has different bytes",
        stream,
        entry.event_id
    );
    let contents = std::str::from_utf8(bytes).context("supersession predecessor is not UTF-8")?;
    let parsed = message::parse_message(&entry.filename, contents);
    anyhow::ensure!(
        parsed.stream.as_deref() == Some(stream)
            && parsed.event_id.as_deref() == Some(entry.event_id.as_str())
            && parsed.event_key.as_deref() == entry.key.as_deref(),
        "supersession predecessor '{}#{}' has different event identity",
        stream,
        entry.event_id
    );
    Ok(())
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
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(mut file) => {
            let metadata = file.metadata()?;
            anyhow::ensure!(
                metadata.is_file(),
                "stream state {} is not a real regular file",
                path.display()
            );
            anyhow::ensure!(
                metadata.len() <= MAX_STATE_BYTES,
                "stream state {} exceeds {MAX_STATE_BYTES} bytes",
                path.display()
            );
            let mut bytes = Vec::with_capacity(metadata.len() as usize);
            file.read_to_end(&mut bytes)?;
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("stream state {} is malformed", path.display())
            })?))
        }
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
