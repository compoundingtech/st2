//! Native process and PTY operations shared by st2 and st3.
//!
//! This crate owns process identity and the small `pty` CLI boundary. It has no
//! catalog, graph, claim, or provider policy.

mod process;
mod pty;

pub use process::{ExecGeneration, ExecObservation, ExecRuntime, process_start_token};
pub use pty::{Launch, PtyObservation, PtyRuntime};
