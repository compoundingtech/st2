//! agent-spec — read a catalog of rendered agent declarations.
//!
//! One agent is one declarative file, Nomad-style: the agent is the job and its `pty`/`exec` blocks
//! are the tasks. This crate owns the two halves any reader of that catalog needs and nothing else:
//!
//! - [`spec`] — the shared model a declaration lowers to ([`AgentSpec`], [`Task`], [`Resource`], …).
//! - [`discovery`] — the catalog walk: parse every `*.{kdl,toml,json}` that looks like a
//!   declaration, and resolve each one's `identity`/`host` with the catalog's precedence rule
//!   (content wins, the path supplies defaults, a mismatch is a warning).
//!
//! KDL is the canonical on-disk format; TOML and JSON lower to the same model. The KDL parser is a
//! private implementation detail — [`discovery`] is the only supported entry point, so every reader
//! resolves identity and host the same way rather than re-deriving it from filenames.
//!
//! st2 consumes this crate, which is what keeps it a reference implementation rather than a copy:
//! a second reader (a TUI, a linter) sees exactly the fields the runner sees, including the ones
//! the runner's roster JSON does not carry (`supervisor`, `role`, `workspace`, `host`, Resource
//! bindings).
//!
//! Render-only fields (`harness`, `model`, `persona`, `permissions`, `transport`, `strategy`,
//! `meta{}`) are read by the render layer and deliberately dropped here — that is what keeps a
//! consumer render-agnostic.

pub mod discovery;
mod kdl_format;
pub mod spec;

pub use discovery::{Declared, Discovered, SpecError, discover, parse_declared, path_defaults};
pub use spec::{
    AgentSpec, JobType, Resource, Restart, RestartMode, Task, TaskKind, parse_duration,
};
