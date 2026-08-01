//! The restart cap (M1b) — st2's crash-loop guard, driven by the job's declared `restart{}` policy
//! (spec.md §4 / R16), so every conformant runner behaves identically on a flapping task.
//!
//! st2 owns liveness (it respawns a dead task on the next reconcile), so it also owns the restart
//! decision. Per task it tracks launch times in a sliding `interval` window and enforces `delay`
//! spacing. When `attempts` within the window are exhausted, `mode` decides: `fail` **parks** the task
//! (give up + surface it) while `delay` just **rate-limits** (keep restarting once the window clears).
//! The clock is injected (`now: Instant`) so every branch is unit-testable without sleeping.

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Instant;

use agent_spec::spec::{Restart, RestartMode};

/// What to do with a would-be (re)launch this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartDecision {
    /// Launch it now.
    Allow,
    /// Too soon after the last launch (`delay` spacing) — skip, retry next pass.
    Delaying,
    /// `attempts` used up within `interval` under `mode = delay` — skip until the window clears.
    RateLimited,
    /// `attempts` exhausted under `mode = fail` — parked; give up and surface it.
    GaveUp,
}

/// Per-task launch history + the parked set. One instance lives in the supervisor loop.
#[derive(Debug, Default)]
pub struct FlappingCap {
    launches: HashMap<String, Vec<Instant>>,
    last_launch: HashMap<String, Instant>,
    parked: HashSet<String>,
    presentation_batch_cursor: usize,
}

impl FlappingCap {
    pub fn new() -> Self {
        Self::default()
    }

    /// True once a task has been parked (gave up).
    pub fn is_parked(&self, id: &str) -> bool {
        self.parked.contains(id)
    }

    /// The parked task ids — for reporting "gave up" once each.
    pub fn parked_ids(&self) -> impl Iterator<Item = &String> {
        self.parked.iter()
    }

    pub(crate) fn presentation_batch_start(&mut self, total: usize, batch: usize) -> usize {
        if total == 0 {
            return 0;
        }
        let start = self.presentation_batch_cursor % total;
        self.presentation_batch_cursor = (start + batch.min(total)) % total;
        start
    }

    /// Decide whether `id` may be (re)launched at `now` under `policy`. On `Allow` the caller should
    /// spawn and then call [`record`](Self::record).
    pub fn decide(&mut self, id: &str, now: Instant, policy: &Restart) -> RestartDecision {
        if self.parked.contains(id) {
            return RestartDecision::GaveUp;
        }
        // `delay` spacing between restarts.
        if let Some(&last) = self.last_launch.get(id)
            && now.duration_since(last) < policy.delay
        {
            return RestartDecision::Delaying;
        }
        // `attempts` within the `interval` window.
        let recent = self.recent_count(id, now, policy);
        if recent >= policy.attempts as usize {
            match policy.mode {
                RestartMode::Fail => {
                    self.parked.insert(id.to_string());
                    return RestartDecision::GaveUp;
                }
                RestartMode::Delay => return RestartDecision::RateLimited,
            }
        }
        RestartDecision::Allow
    }

    /// Record a launch of `id` at `now` (call only after a successful spawn).
    pub fn record(&mut self, id: &str, now: Instant) {
        self.launches.entry(id.to_string()).or_default().push(now);
        self.last_launch.insert(id.to_string(), now);
    }

    /// Count (and prune) launches of `id` within `policy.interval` ending at `now`.
    fn recent_count(&mut self, id: &str, now: Instant, policy: &Restart) -> usize {
        let Some(times) = self.launches.get_mut(id) else {
            return 0;
        };
        let window = policy.interval;
        times.retain(|t| now.duration_since(*t) < window);
        times.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn policy(attempts: u32, interval_s: u64, delay_s: u64, mode: RestartMode) -> Restart {
        Restart {
            attempts,
            interval: Duration::from_secs(interval_s),
            delay: Duration::from_secs(delay_s),
            mode,
        }
    }

    #[test]
    fn fail_mode_parks_after_attempts() {
        let mut cap = FlappingCap::new();
        let p = policy(3, 60, 0, RestartMode::Fail);
        let t0 = Instant::now();
        for i in 0..3 {
            let now = t0 + Duration::from_secs(i);
            assert_eq!(cap.decide("p", now, &p), RestartDecision::Allow);
            cap.record("p", now);
        }
        assert_eq!(cap.decide("p", t0 + Duration::from_secs(4), &p), RestartDecision::GaveUp);
        assert!(cap.is_parked("p"));
        // stays parked even after the window empties
        assert_eq!(cap.decide("p", t0 + Duration::from_secs(600), &p), RestartDecision::GaveUp);
    }

    #[test]
    fn delay_mode_rate_limits_but_never_parks() {
        let mut cap = FlappingCap::new();
        let p = policy(3, 60, 0, RestartMode::Delay);
        let t0 = Instant::now();
        for i in 0..3 {
            let now = t0 + Duration::from_secs(i);
            cap.decide("p", now, &p);
            cap.record("p", now);
        }
        // 4th within the window → rate-limited, NOT parked.
        assert_eq!(cap.decide("p", t0 + Duration::from_secs(4), &p), RestartDecision::RateLimited);
        assert!(!cap.is_parked("p"));
        // Once the window clears, it's allowed again.
        assert_eq!(cap.decide("p", t0 + Duration::from_secs(120), &p), RestartDecision::Allow);
    }

    #[test]
    fn delay_spacing_is_enforced() {
        let mut cap = FlappingCap::new();
        let p = policy(5, 60, 10, RestartMode::Delay);
        let t0 = Instant::now();
        cap.record("p", t0);
        // 5s later — still within the 10s delay.
        assert_eq!(cap.decide("p", t0 + Duration::from_secs(5), &p), RestartDecision::Delaying);
        // 11s later — delay satisfied.
        assert_eq!(cap.decide("p", t0 + Duration::from_secs(11), &p), RestartDecision::Allow);
    }

    #[test]
    fn parking_is_per_task() {
        let mut cap = FlappingCap::new();
        let p = policy(1, 60, 0, RestartMode::Fail);
        let t0 = Instant::now();
        cap.record("a", t0);
        assert_eq!(cap.decide("a", t0 + Duration::from_secs(1), &p), RestartDecision::GaveUp);
        assert_eq!(cap.decide("b", t0 + Duration::from_secs(1), &p), RestartDecision::Allow);
    }
}
