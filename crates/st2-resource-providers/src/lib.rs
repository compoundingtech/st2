//! Closed host capabilities for production resource-observer components.

mod github_issue;
mod pty_stats;

pub use github_issue::{GitHubIssueCancellation, GitHubIssueConfig, GitHubIssueModule};
pub use pty_stats::{PtyStatsCancellation, PtyStatsConfig, PtyStatsModule, PtyStatsScope};
