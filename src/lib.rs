//! st2 — a harness-agnostic runner over a unified catalog+inbox folder.
//!
//! st2 reads a catalog of hand-authored agent declarations plus each agent's inbox. It keeps every
//! declared task running and delivers native messages. Harness-specific behavior stays explicit in
//! each declaration's command, environment, hooks, and workspace materialization block.

pub mod agent_author;
pub mod agents;
pub mod catalog;
pub mod compile_agent;
pub mod context;
pub mod ding;
pub mod eval_run;
pub mod eval_spec;
pub mod exec_backend;
pub mod expand;
pub mod flapping;
pub mod hooks;
pub mod host_lock;
pub mod isolate;
pub mod materialize;
pub mod message;
pub mod pretrust;
pub mod reconcile;
pub mod resource;
pub mod run;
pub mod service;
pub mod status;
pub mod task_inventory;
pub mod validate;
pub mod version;
mod watch;

// The declaration model and the catalog walk live in the `agent-spec` crate, so st2 and any other
// reader of the same catalog share one implementation. Re-exported under their original paths:
// `st2::spec::…` / `st2::discovery::…` keep working for the binary and the test suite.
pub use agent_spec::{discovery, spec};

pub use agent_spec::discovery::{Discovered, SpecError, discover};
pub use agent_spec::spec::{
    AgentSpec, JobType, Resource, Restart, RestartMode, Task, TaskKind, TaskLifecycle,
    parse_duration,
};
pub use exec_backend::ExecBackend;
pub use expand::{expand_env, expand_vars};
pub use flapping::FlappingCap;
pub use host_lock::HostLock;
pub use reconcile::{Launch, ReconcilePlan, Session, TaskLaunch, TaskTarget, Teardown, reconcile};
pub use run::{
    PtyCli, Runner, SystemRunner, UpReport, detect_host, down, down_specs, exec_state_dir, execute,
    up_loop, up_loop_specs, up_once, up_once_selected, up_once_selected_specs, up_once_specs,
};
