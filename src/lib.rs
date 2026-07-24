//! st2 — a harness-agnostic runner over a unified catalog+inbox folder.
//!
//! st2 is the RUN half of convoy, split out from RENDER. It reads a folder of *already-rendered*
//! agent specs (each carrying explicit pty `command`s) plus each agent's inbox, and it does two
//! dumb jobs: keep every declared agent's ptys running, and deliver messages by moving inbox files.
//! It never needs to know what `claude` or `codex` are — everything harness-specific is baked into
//! the spec's command/env by RENDER before st2 ever sees it.
//!
//! Milestones: **M0** spec model + discovery (this) · M1 reconcile plan · M2 pty execution ·
//! M3 watch loop + flapping-cap + GC · M4 message delivery.

pub mod agents;
pub mod context;
pub mod ding;
pub mod discovery;
pub mod eval_run;
pub mod eval_spec;
pub mod exec_backend;
pub mod expand;
pub mod flapping;
pub mod hooks;
pub mod host_lock;
pub mod isolate;
mod kdl_format;
pub mod materialize;
pub mod message;
pub mod pretrust;
pub mod reconcile;
pub mod render;
pub mod resource;
pub mod run;
pub mod service;
pub mod spec;
pub mod status;
pub mod validate;

pub use discovery::{Discovered, SpecError, discover};
pub use exec_backend::ExecBackend;
pub use expand::{expand_env, expand_vars};
pub use flapping::FlappingCap;
pub use host_lock::HostLock;
pub use reconcile::{Launch, ReconcilePlan, Session, TaskTarget, Teardown, reconcile};
pub use run::{
    PtyCli, Runner, SystemRunner, UpReport, detect_host, down, down_specs, exec_state_dir, execute,
    up_loop, up_loop_specs, up_once, up_once_specs,
};
pub use spec::{AgentSpec, JobType, Restart, RestartMode, Task, TaskKind, parse_duration};
