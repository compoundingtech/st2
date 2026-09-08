//! The two framing helpers every stdio native channel shares.
//!
//! A native channel is a child of the interactive harness, speaking newline-delimited JSON over
//! stdio: the Claude MCP watcher ([`crate::claude_mcp`]) and the pi-family channel
//! ([`crate::pi_channel`]). What a delivered inbox message looks like to the model, and how a
//! frame is terminated on the wire, are st2's decisions rather than each channel's — so they are
//! decided once, here.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{delivery_ledger, message};

/// The envelope a delivered inbox message is handed to the model in. A message with no subject is
/// its body verbatim: an empty `Subject:` line would be noise the model has to read past.
pub(crate) fn channel_content(subject: Option<&str>, body: &str) -> String {
    match subject.filter(|value| !value.is_empty()) {
        Some(subject) => format!("Subject: {subject}\n\n{body}"),
        None => body.to_owned(),
    }
}

/// Stable provider-neutral correlation for a native-channel binding and inbox filename.
///
/// The attempt token, not this value, distinguishes retries. Length-prefixing keeps the
/// derivation unambiguous without depending on runtime paths or provider-session identifiers.
pub fn delivery_correlation(binding: &str, filename: &str) -> String {
    let mut hash = Sha256::new();
    for value in [binding.as_bytes(), filename.as_bytes()] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    format!("{:x}", hash.finalize())
}

type FrameRenderer = fn(message::Message, &str, &delivery_ledger::Permit) -> Result<Value>;

pub(crate) struct DeliveryPump {
    pub(crate) ledger: delivery_ledger::Ledger,
    binding: String,
    agent_dir: PathBuf,
    render: FrameRenderer,
}

impl DeliveryPump {
    pub(crate) fn open(
        agent_dir: &Path,
        identity: &str,
        binding: &str,
        harness: delivery_ledger::Harness,
        render: FrameRenderer,
    ) -> Self {
        Self {
            ledger: delivery_ledger::Ledger::open(
                &agent_dir.join(delivery_ledger::LEDGER_FILE),
                harness.profile(),
                identity,
                binding,
                delivery_correlation,
            ),
            binding: binding.to_owned(),
            agent_dir: agent_dir.to_owned(),
            render,
        }
    }

    /// Reconcile archive settlement, then claim and transport at most the FIFO head.
    pub(crate) fn pump(
        &mut self,
        out: &mut impl Write,
        unread: Vec<message::Message>,
        identity: &str,
    ) -> Result<()> {
        let snapshot = self.ledger.snapshot()?;
        let mut settled = Vec::new();
        for attempt in snapshot.attempts() {
            if message::archive_receipt_exists(&self.agent_dir, &attempt.filename)? {
                settled.push(attempt.fence());
            }
        }
        self.ledger.prune(&settled)?;

        let head = unread
            .into_iter()
            .find_map(|message| {
                match message::archive_receipt_exists(&self.agent_dir, &message.filename) {
                    Ok(false) => Some(Ok(message)),
                    Ok(true) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .transpose()?;
        let Some(head) = head else {
            return Ok(());
        };
        self.ledger.retarget(&self.binding)?;
        let permit = match self.ledger.claim(delivery_ledger::Claimant {
            filename: head.filename.clone(),
            binding: self.binding.clone(),
            correlation: delivery_ledger::Correlation::native(delivery_correlation(
                &self.binding,
                &head.filename,
            )),
            incarnation: None,
        })? {
            delivery_ledger::Claim::Permitted(permit) => permit,
            delivery_ledger::Claim::Held(_) => return Ok(()),
        };
        write_json(out, &(self.render)(head, identity, &permit)?)?;
        Ok(())
    }
}

/// One frame on the wire: compact JSON followed by the newline that terminates it.
pub(crate) fn write_json(out: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_content_reuses_one_subject_and_body_envelope() {
        assert_eq!(
            channel_content(Some("subject"), "body"),
            "Subject: subject\n\nbody"
        );
        assert_eq!(channel_content(None, "body"), "body");
        assert_eq!(channel_content(Some(""), "body"), "body");
    }

    #[test]
    fn delivery_correlation_is_stable_and_length_delimited() {
        assert_eq!(
            delivery_correlation("h.worker", "1787042542238-xex2t4.md"),
            delivery_correlation("h.worker", "1787042542238-xex2t4.md")
        );
        assert_ne!(
            delivery_correlation("ab", "c"),
            delivery_correlation("a", "bc")
        );
    }

    /// The wire is newline-delimited: a reader splitting on newlines must see exactly one frame
    /// per value, and a body carrying its own newline must not be able to forge a second one.
    #[test]
    fn each_frame_is_one_newline_terminated_line() {
        let mut out = Vec::new();
        write_json(&mut out, &serde_json::json!({"a": 1})).unwrap();
        write_json(
            &mut out,
            &serde_json::json!({"content": "line one\nline two"}),
        )
        .unwrap();
        let framed = String::from_utf8(out).unwrap();
        assert_eq!(framed, "{\"a\":1}\n{\"content\":\"line one\\nline two\"}\n");
        assert_eq!(framed.lines().count(), 2);
    }
}
