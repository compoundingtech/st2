//! st2-wire — the JSON shapes st2's CLI emits.
//!
//! st2 prints machine-readable JSON that other programs read. Before this crate each reader
//! hand-typed its own struct to match, which is two transcriptions of one contract: nothing
//! asserted they agreed, either side could drift silently, and the failure surfaced somewhere else
//! entirely — a renderer reporting a parse error for a field the producer had legitimately changed.
//!
//! So the wire shape lives here, st2 serializes *through* it, and readers deserialize *the same
//! type*. Optionality is then whatever st2 says it is, checked by rustc rather than by whoever last
//! read the CLI source.
//!
//! This crate is deliberately small and dependency-light: it holds the shapes and nothing else. The
//! parsing, the filesystem, and the domain types stay in st2 — a reader that wants to render a
//! message list should not thereby depend on the runner's mailbox I/O.
//!
//! - [`message`] — `st2 message ls --json` and `st2 message read --json`.
//!
//! # Readers are additive-tolerant on purpose
//!
//! No type here uses `deny_unknown_fields`. A reader is pinned to a revision of this crate and may
//! legitimately be older than the `st2` binary it shells out to, so an unknown field must be
//! ignored rather than fatal. Rejecting them would turn every future additive field into a broken
//! reader — the exact failure this crate exists to prevent.

pub mod message;
