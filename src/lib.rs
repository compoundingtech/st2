//! st2 — a harness-agnostic runner over a unified catalog+inbox folder.
//!
//! st2 reads a catalog of hand-authored agent declarations plus each agent's inbox. It keeps every
//! declared task running and delivers native messages. Harness-specific behavior stays explicit in
//! each declaration's command, environment, hooks, and workspace materialization block.

pub mod agents;
pub mod compile_agent;
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
pub mod resource;
pub mod run;
pub mod service;
pub(crate) mod shepherd;
pub mod spec;
pub mod status;
pub mod validate;
pub mod version;

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
