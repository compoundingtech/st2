use std::collections::HashSet;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const HOOK_RECEIPT_VERSION: u8 = 1;
const HOOK_RECEIPT_KIND: &str = "hook-owned";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HookOwnedReceipt {
    v: u8,
    kind: String,
    messages: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PtyOwnedReceipt {
    v: u8,
    kind: String,
    messages: Vec<String>,
    generation: String,
    revision: u64,
    sequence: u64,
}

pub(super) fn control_dir(inbox_dir: &Path) -> PathBuf {
    inbox_dir.parent().unwrap_or(inbox_dir).join("ding-control")
}

/// Record that a provider-native hook already injected exact unread filenames into an
/// already-occurring next context. The caller owns provider translation; this ingress validates
/// only generic durable-inbox facts.
pub fn record_hook_owned(inbox_dir: &Path, messages: &[String]) -> anyhow::Result<PathBuf> {
    if messages.is_empty() {
        anyhow::bail!("hook-owned receipt requires at least one --message");
    }
    let mut unique = HashSet::new();
    for filename in messages {
        if !crate::message::is_message_filename(filename) {
            anyhow::bail!("hook-owned receipt has invalid message filename `{filename}`");
        }
        if !unique.insert(filename.as_str()) {
            anyhow::bail!("hook-owned receipt repeats message filename `{filename}`");
        }
    }
    let unread: HashSet<String> = crate::message::list_inbox(inbox_dir)?
        .into_iter()
        .map(|message| message.filename)
        .collect();
    if let Some(missing) = messages.iter().find(|filename| !unread.contains(*filename)) {
        anyhow::bail!("hook-owned message `{missing}` is not currently unread");
    }

    persist_receipt(
        inbox_dir,
        "hook-owned",
        &HookOwnedReceipt {
            v: HOOK_RECEIPT_VERSION,
            kind: HOOK_RECEIPT_KIND.to_string(),
            messages: messages.to_vec(),
        },
    )
}

pub(super) fn record_pty_owned(
    inbox_dir: &Path,
    messages: &HashSet<String>,
    generation: &str,
    revision: u64,
    sequence: u64,
) -> anyhow::Result<PathBuf> {
    if messages.is_empty() {
        anyhow::bail!("PTY ownership requires at least one unread message");
    }
    let unread = unread_filenames(inbox_dir)?;
    if let Some(missing) = messages.iter().find(|filename| !unread.contains(*filename)) {
        anyhow::bail!("PTY-owned message `{missing}` is not currently unread");
    }
    let mut messages = messages.iter().cloned().collect::<Vec<_>>();
    messages.sort();
    persist_receipt(
        inbox_dir,
        "pty-owned",
        &PtyOwnedReceipt {
            v: HOOK_RECEIPT_VERSION,
            kind: "pty-owned".to_string(),
            messages,
            generation: generation.to_string(),
            revision,
            sequence,
        },
    )
}

fn persist_receipt(
    inbox_dir: &Path,
    name: &str,
    receipt: &impl Serialize,
) -> anyhow::Result<PathBuf> {
    let directory = control_dir(inbox_dir);
    fs::create_dir_all(&directory)?;
    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    serde_json::to_writer(&mut temporary, receipt)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    let target = directory.join(format!(
        "{}.{name}.json",
        crate::message::new_filename()
            .strip_suffix(".md")
            .expect("canonical message filename")
    ));
    temporary
        .persist_noclobber(&target)
        .map_err(|error| anyhow::anyhow!("persisting {name} receipt: {}", error.error))?;
    Ok(target)
}

pub(super) fn clear_pty_owned(path: &Path) -> anyhow::Result<()> {
    fs::remove_file(path)
        .map_err(|error| anyhow::anyhow!("clearing PTY ownership {}: {error}", path.display()))
}

/// Record fields remain generic and exact; this helper validates only the hook-owned envelope.
fn validate_hook_receipt(receipt: &HookOwnedReceipt, path: &Path) -> anyhow::Result<()> {
    if receipt.v != HOOK_RECEIPT_VERSION || receipt.kind != HOOK_RECEIPT_KIND {
        anyhow::bail!("unsupported hook-owned receipt {}", path.display());
    }
    Ok(())
}

/// Record fields remain generic and exact; this helper validates only the PTY-owned envelope.
fn validate_pty_receipt(receipt: &PtyOwnedReceipt, path: &Path) -> anyhow::Result<()> {
    if receipt.v != HOOK_RECEIPT_VERSION
        || receipt.kind != "pty-owned"
        || receipt.generation.is_empty()
        || receipt.sequence == 0
    {
        anyhow::bail!("unsupported PTY-owned receipt {}", path.display());
    }
    Ok(())
}

fn retain_current_messages(
    path: &Path,
    messages: &[String],
    unread: &HashSet<String>,
    owned: &mut HashSet<String>,
) -> anyhow::Result<()> {
    let mut still_unread = false;
    for filename in messages {
        if !crate::message::is_message_filename(filename) {
            anyhow::bail!(
                "DING ownership receipt {} has invalid filename `{filename}`",
                path.display()
            );
        }
        if unread.contains(filename) {
            still_unread = true;
            owned.insert(filename.clone());
        }
    }
    if !still_unread {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn receipt_paths(inbox_dir: &Path, suffix: &str) -> Vec<PathBuf> {
    let directory = control_dir(inbox_dir);
    let Ok(entries) = fs::read_dir(&directory) else {
        return Vec::new();
    };
    let mut paths = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(suffix))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn unread_filenames(inbox_dir: &Path) -> anyhow::Result<HashSet<String>> {
    Ok(crate::message::list_inbox(inbox_dir)?
        .into_iter()
        .map(|message| message.filename)
        .collect())
}

/// Load durable hook ownership, rechecking every filename against the current unread inbox.
/// Receipts remain until all named messages are archived, so sidecar restarts cannot reding work
/// that a hook already injected.
pub(super) fn load_hook_owned(inbox_dir: &Path) -> anyhow::Result<HashSet<String>> {
    let unread = unread_filenames(inbox_dir)?;
    let mut owned = HashSet::new();
    for path in receipt_paths(inbox_dir, ".hook-owned.json") {
        let receipt: HookOwnedReceipt = serde_json::from_slice(&fs::read(&path)?)
            .map_err(|error| anyhow::anyhow!("parsing {}: {error}", path.display()))?;
        validate_hook_receipt(&receipt, &path)?;
        retain_current_messages(&path, &receipt.messages, &unread, &mut owned)?;
    }
    Ok(owned)
}

pub(super) fn load_pty_owned(inbox_dir: &Path) -> anyhow::Result<HashSet<String>> {
    let unread = unread_filenames(inbox_dir)?;
    let mut owned = HashSet::new();
    for path in receipt_paths(inbox_dir, ".pty-owned.json") {
        let receipt: PtyOwnedReceipt = serde_json::from_slice(&fs::read(&path)?)
            .map_err(|error| anyhow::anyhow!("parsing {}: {error}", path.display()))?;
        validate_pty_receipt(&receipt, &path)?;
        retain_current_messages(&path, &receipt.messages, &unread, &mut owned)?;
    }
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{archive_dir, archive_msg, inbox_dir, send_to_inbox};

    #[test]
    fn ingress_accepts_only_exact_currently_unread_filenames_and_survives_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(tmp.path());
        let archive = archive_dir(tmp.path());
        let filename = send_to_inbox(&inbox, "sender", Some("subject"), None, &[], "body").unwrap();
        let receipt = record_hook_owned(&inbox, std::slice::from_ref(&filename)).unwrap();
        assert!(receipt.is_file());
        assert_eq!(
            load_hook_owned(&inbox).unwrap(),
            HashSet::from([filename.clone()])
        );
        assert!(record_hook_owned(&inbox, &["not-a-message".to_string()]).is_err());
        assert!(record_hook_owned(&inbox, &["1785000000000-abc123.md".to_string()]).is_err());

        archive_msg(&inbox, &archive, &filename).unwrap();
        assert!(load_hook_owned(&inbox).unwrap().is_empty());
        assert!(!receipt.exists());
    }

    #[test]
    fn ingress_rejects_duplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(tmp.path());
        let filename = send_to_inbox(&inbox, "sender", None, None, &[], "body").unwrap();
        assert!(
            record_hook_owned(&inbox, &[filename.clone(), filename])
                .unwrap_err()
                .to_string()
                .contains("repeats")
        );
    }

    #[test]
    fn pty_ownership_is_durable_until_conflict_clear_or_archive() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = inbox_dir(tmp.path());
        let archive = archive_dir(tmp.path());
        let filename = send_to_inbox(&inbox, "sender", None, None, &[], "body").unwrap();
        let receipt = record_pty_owned(
            &inbox,
            &HashSet::from([filename.clone()]),
            "generation-a",
            41,
            7,
        )
        .unwrap();
        assert_eq!(
            load_pty_owned(&inbox).unwrap(),
            HashSet::from([filename.clone()])
        );
        clear_pty_owned(&receipt).unwrap();
        assert!(load_pty_owned(&inbox).unwrap().is_empty());

        let receipt = record_pty_owned(
            &inbox,
            &HashSet::from([filename.clone()]),
            "generation-a",
            42,
            8,
        )
        .unwrap();
        archive_msg(&inbox, &archive, &filename).unwrap();
        assert!(load_pty_owned(&inbox).unwrap().is_empty());
        assert!(!receipt.exists());
    }
}
