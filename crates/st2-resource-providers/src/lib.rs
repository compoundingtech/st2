//! Closed host capabilities for production resource-observer components.

mod github_issue;
mod github_pr;
mod pty_stats;

pub use github_issue::{GitHubIssueConfig, GitHubIssueModule};
pub use github_pr::{GitHubPrConfig, GitHubPrModule};
pub use pty_stats::{PtyStatsConfig, PtyStatsModule, PtyStatsScope};
