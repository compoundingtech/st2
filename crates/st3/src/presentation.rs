use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::IsTerminal as _;

use st3::model::{
    MissionInputKind, MissionRunView, RevisionCutover, RevisionProposalView, RunGenerationView,
    StepRunView,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OutputStyle {
    color: bool,
}

impl OutputStyle {
    pub(crate) fn stdout() -> Self {
        Self::from_terminal(
            std::io::stdout().is_terminal(),
            std::env::var_os("NO_COLOR").is_some(),
        )
    }

    fn from_terminal(terminal: bool, no_color: bool) -> Self {
        Self {
            color: terminal && !no_color,
        }
    }

    #[cfg(test)]
    fn plain() -> Self {
        Self { color: false }
    }

    #[cfg(test)]
    fn colored() -> Self {
        Self { color: true }
    }

    fn paint(self, code: &str, value: impl std::fmt::Display) -> String {
        if self.color {
            format!("\x1b[{code}m{value}\x1b[0m")
        } else {
            value.to_string()
        }
    }

    fn heading(self, value: impl std::fmt::Display) -> String {
        self.paint("1;36", value)
    }

    fn muted(self, value: impl std::fmt::Display) -> String {
        self.paint("2", value)
    }

    fn status(self, value: &str) -> String {
        let code = match value {
            "completed" | "standing" | "available" => "1;32",
            "ready" | "claimed" => "1;36",
            "working" | "verifying" | "running" | "busy" => "1;33",
            "blocked" | "failed" | "unreachable" => "1;31",
            "cancelled" | "pending" => "2",
            _ => "1",
        };
        self.paint(code, value)
    }

    fn status_value(self, status: &str, value: impl std::fmt::Display) -> String {
        let code = match status {
            "completed" | "standing" | "available" => "1;32",
            "ready" | "claimed" => "1;36",
            "working" | "verifying" | "running" | "busy" => "1;33",
            "blocked" | "failed" | "unreachable" => "1;31",
            "cancelled" | "pending" => "2",
            _ => "1",
        };
        self.paint(code, value)
    }
}

pub(crate) fn render_mission_run(
    selected: &MissionRunView,
    runs: &[MissionRunView],
    style: OutputStyle,
    now_unix_ms: u128,
) -> String {
    let mut output = String::new();
    let children = mission_children(runs);
    let visible_runs = mission_subtree(selected, &children);
    let steps = visible_runs
        .iter()
        .flat_map(|run| run.steps.iter())
        .collect::<Vec<_>>();
    let completed = steps
        .iter()
        .filter(|step| step.status == "completed")
        .count();
    let active = steps
        .iter()
        .filter(|step| is_moving_state(&step.status))
        .count();
    let blocked = steps.iter().filter(|step| step.status == "blocked").count();

    let _ = writeln!(
        output,
        "{}  {}",
        style.heading("MISSION"),
        short_subject(&selected.mission, "mission/")
    );
    let _ = writeln!(output, "RUN       {}", selected.subject);
    let _ = writeln!(
        output,
        "STATE     {} · {}",
        style.status(&selected.status),
        selected.phase
    );
    let _ = writeln!(output, "REVISION  {}", selected.revision);
    let _ = writeln!(output, "GENERATION {}", selected.generation);
    let _ = writeln!(output, "WORKSPACE {}", selected.workspace);
    let _ = writeln!(output, "REQUESTER {}", selected.requester);
    let _ = writeln!(
        output,
        "UPDATED   {}",
        relative_time(selected.updated_at_unix_ms, now_unix_ms)
    );
    let _ = writeln!(
        output,
        "PROGRESS  {completed}/{} completed · {active} active · {blocked} blocked",
        steps.len()
    );
    if let Some(deadline) = selected.deadline_at_unix_ms {
        let _ = writeln!(output, "DEADLINE  {}", relative_time(deadline, now_unix_ms));
    }
    if !selected.inputs.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "{}", style.heading("INPUTS"));
        for (name, input) in &selected.inputs {
            let kind = match input.kind {
                MissionInputKind::Text => "text",
                MissionInputKind::Resource => "resource",
            };
            let _ = writeln!(output, "  {name}  {kind}  {}", input.value);
        }
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "{}", style.heading("WORK"));
    if selected.steps.is_empty() {
        let _ = writeln!(output, "  {}", style.muted("No steps."));
    } else {
        render_run_steps(&mut output, selected, &children, "  ", style);
    }
    output
}

pub(crate) fn render_work_list(
    actor: Option<&str>,
    work: &[StepRunView],
    show_all: bool,
    style: OutputStyle,
) -> String {
    let mut output = String::new();
    let title = actor.map_or_else(|| "WORK".to_owned(), |actor| format!("WORK FOR {actor}"));
    let active = work
        .iter()
        .filter(|step| matches!(step.status.as_str(), "claimed" | "working" | "verifying"))
        .count();
    let ready = work.iter().filter(|step| step.status == "ready").count();
    let blocked = work.iter().filter(|step| step.status == "blocked").count();
    let pending = work.iter().filter(|step| step.status == "pending").count();
    let terminal = work
        .iter()
        .filter(|step| is_terminal_state(&step.status))
        .count();
    let _ = writeln!(output, "{}", style.heading(title));
    let _ = writeln!(
        output,
        "{active} active · {ready} ready · {blocked} blocked · {pending} waiting · {terminal} terminal"
    );

    let states = if show_all {
        vec![
            "working",
            "verifying",
            "claimed",
            "ready",
            "blocked",
            "pending",
            "failed",
            "completed",
            "cancelled",
        ]
    } else {
        vec!["working", "verifying", "claimed", "ready", "blocked"]
    };
    let mut shown = BTreeSet::new();
    for state in states {
        let matching = work
            .iter()
            .filter(|step| step.status == state)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            continue;
        }
        let _ = writeln!(output);
        let _ = writeln!(
            output,
            "{}",
            style.status_value(state, state.to_uppercase())
        );
        for step in matching {
            shown.insert(step.subject.as_str());
            render_work_list_item(&mut output, step, style);
        }
    }
    let other = work
        .iter()
        .filter(|step| !shown.contains(step.subject.as_str()))
        .filter(|step| step.status != "pending" && !is_terminal_state(&step.status))
        .collect::<Vec<_>>();
    if !other.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "{}", style.heading("OTHER"));
        for step in &other {
            render_work_list_item(&mut output, step, style);
        }
    }
    if !show_all && pending + terminal > 0 {
        let _ = writeln!(output);
        let _ = writeln!(
            output,
            "{}",
            style.muted(format!(
                "{pending} waiting items and {terminal} terminal items hidden. Use --all to show them."
            ))
        );
    }
    if shown.is_empty() && other.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "{}", style.muted("No ready or active work."));
    }
    output
}

pub(crate) fn render_step_run(step: &StepRunView, style: OutputStyle, now_unix_ms: u128) -> String {
    let mut output = String::new();
    let title = step.title.as_deref().unwrap_or(&step.step);
    let _ = writeln!(output, "{}  {title}", style.heading("STEP"));
    let _ = writeln!(output, "SUBJECT    {}", step.subject);
    let _ = writeln!(
        output,
        "STATE      {} · attempt {}",
        style.status(&step.status),
        step.attempt
    );
    let _ = writeln!(output, "MISSION RUN {}", step.run);
    let _ = writeln!(output, "GENERATION {}", step.generation);
    let _ = writeln!(output, "DEFINITION {}", step.definition_hash);
    let _ = writeln!(
        output,
        "UPDATED    {}",
        relative_time(step.updated_at_unix_ms, now_unix_ms)
    );
    if let Some((queue, position)) = step.queue.as_deref().zip(step.queue_position) {
        let _ = writeln!(output, "QUEUE      {queue} #{position}");
    }
    if let Some(not_before) = step.not_before_unix_ms {
        let _ = writeln!(
            output,
            "NOT BEFORE {}",
            relative_time(not_before, now_unix_ms)
        );
    }
    if let Some(reason) = &step.blocked_reason {
        let _ = writeln!(output, "REASON     {reason}");
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "{}", style.heading("WORKER"));
    if step.agentless {
        let _ = writeln!(output, "  Agentless");
    }
    if let Some(agent) = &step.assigned_to {
        let _ = writeln!(output, "  Assigned to  {agent}");
    }
    for agent in &step.available_to {
        let _ = writeln!(output, "  Available to {agent}");
    }
    if let Some(agent) = &step.claimant {
        let _ = writeln!(output, "  Claimant     {agent}");
    }
    if let Some(incarnation) = &step.claim_incarnation {
        let _ = writeln!(output, "  Incarnation  {incarnation}");
    }
    if let Some(expires) = step.claim_expires_at_unix_ms {
        let _ = writeln!(
            output,
            "  Lease        {}",
            relative_time(expires, now_unix_ms)
        );
    }
    for grouping in &step.under {
        if let Some(reason) = &grouping.reason {
            let _ = writeln!(output, "  Under        {} ({reason})", grouping.agent);
        } else {
            let _ = writeln!(output, "  Under        {}", grouping.agent);
        }
    }
    if !step.goals.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "{}", style.heading("GOALS"));
        for goal in &step.goals {
            let _ = writeln!(output, "  • {goal}");
        }
    }
    if !step.constraints.is_empty() {
        let _ = writeln!(output);
        let _ = writeln!(output, "{}", style.heading("CONSTRAINTS"));
        for constraint in &step.constraints {
            let _ = writeln!(output, "  • {constraint}");
        }
    }
    output
}

pub(crate) fn render_revision_proposal(
    proposal: &RevisionProposalView,
    style: OutputStyle,
    now_unix_ms: u128,
) -> String {
    let mut output = String::new();
    let cutover = match proposal.cutover {
        RevisionCutover::RestartActive => "restart-active",
        RevisionCutover::WhenIdle => "when-idle",
    };
    let _ = writeln!(
        output,
        "{}  {}",
        style.heading("REVISION PROPOSAL"),
        proposal.subject
    );
    let _ = writeln!(output, "STATE      {}", style.status(&proposal.status));
    let _ = writeln!(output, "RUN        {}", proposal.run);
    let _ = writeln!(output, "SOURCE     {}", proposal.source_generation);
    let _ = writeln!(output, "CANDIDATE  {}", proposal.candidate_revision);
    let _ = writeln!(output, "ACTOR      {}", proposal.actor);
    let _ = writeln!(output, "CUTOVER    {cutover}");
    let _ = writeln!(
        output,
        "UPDATED    {}",
        relative_time(proposal.updated_at_unix_ms, now_unix_ms)
    );
    let _ = writeln!(output, "REASON     {}", proposal.reason);
    if let Some(hash) = &proposal.preview_hash {
        let _ = writeln!(output, "APPROVAL HASH {hash}");
    }
    append_named_list(
        &mut output,
        "COMPATIBLE STEPS",
        &proposal.compatible_steps,
        style,
    );
    append_named_list(&mut output, "REVIEWERS", &proposal.reviewers, style);
    append_named_list(&mut output, "APPROVALS", &proposal.approvals, style);
    if let Some(successor) = &proposal.successor_generation {
        let _ = writeln!(output);
        let _ = writeln!(output, "SUCCESSOR  {successor}");
    }
    output
}

pub(crate) fn render_generations(
    run: &MissionRunView,
    generations: &[RunGenerationView],
    style: OutputStyle,
) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "{}  {}", style.heading("GENERATIONS"), run.subject);
    let _ = writeln!(output, "CURRENT  {}", run.generation);
    let mut ordered = generations.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|generation| generation.created_at_unix_ms);
    for generation in ordered {
        let mark = if generation.subject == run.generation {
            "●"
        } else {
            "○"
        };
        let _ = writeln!(
            output,
            "{mark} {}  {}",
            style.status(&generation.status),
            generation.subject
        );
        let _ = writeln!(output, "  revision {}", generation.revision);
        let _ = writeln!(
            output,
            "  actor {} · {}",
            generation.actor, generation.reason
        );
        if let Some(predecessor) = &generation.predecessor {
            let _ = writeln!(output, "  after {predecessor}");
        }
    }
    output
}

pub(crate) fn render_generation(generation: &RunGenerationView, style: OutputStyle) -> String {
    let mut output = String::new();
    let completed = generation
        .steps
        .iter()
        .filter(|step| step.status == "completed")
        .count();
    let active = generation
        .steps
        .iter()
        .filter(|step| is_moving_state(&step.status))
        .count();
    let _ = writeln!(
        output,
        "{}  {}",
        style.heading("GENERATION"),
        generation.subject
    );
    let _ = writeln!(output, "STATE     {}", style.status(&generation.status));
    let _ = writeln!(output, "RUN       {}", generation.run);
    let _ = writeln!(output, "REVISION  {}", generation.revision);
    let _ = writeln!(output, "ACTOR     {}", generation.actor);
    let _ = writeln!(output, "REASON    {}", generation.reason);
    if let Some(predecessor) = &generation.predecessor {
        let _ = writeln!(output, "PREVIOUS  {predecessor}");
    }
    let _ = writeln!(
        output,
        "PROGRESS  {completed}/{} completed · {active} active",
        generation.steps.len()
    );
    let _ = writeln!(output);
    let _ = writeln!(output, "{}", style.heading("WORK"));
    if generation.steps.is_empty() {
        let _ = writeln!(output, "  {}", style.muted("No steps."));
    } else {
        render_flat_step_tree(&mut output, &generation.steps, "  ", style);
    }
    output
}

pub(crate) fn mission_run_signature(runs: &[MissionRunView]) -> anyhow::Result<String> {
    let state = runs
        .iter()
        .map(|run| {
            serde_json::json!({
                "subject": run.subject,
                "revision": run.revision,
                "generation": run.generation,
                "status": run.status,
                "phase": run.phase,
                "steps": run.steps.iter().map(|step| serde_json::json!({
                    "subject": step.subject,
                    "status": step.status,
                    "attempt": step.attempt,
                    "claimant": step.claimant,
                    "blocked_reason": step.blocked_reason,
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&state).map_err(Into::into)
}

pub(crate) fn follow_snapshot(frame: &str, interactive: bool, has_prior: bool) -> String {
    if interactive {
        format!("\x1b[2J\x1b[H{frame}")
    } else if has_prior {
        format!("\n{frame}")
    } else {
        frame.to_owned()
    }
}

fn mission_children(runs: &[MissionRunView]) -> BTreeMap<&str, Vec<&MissionRunView>> {
    let mut children = BTreeMap::<_, Vec<_>>::new();
    for run in runs {
        if let Some(parent) = run.parent_step_run.as_deref() {
            children.entry(parent).or_default().push(run);
        }
    }
    children
}

fn mission_subtree<'a>(
    selected: &'a MissionRunView,
    children: &BTreeMap<&str, Vec<&'a MissionRunView>>,
) -> Vec<&'a MissionRunView> {
    fn collect<'a>(
        run: &'a MissionRunView,
        children: &BTreeMap<&str, Vec<&'a MissionRunView>>,
        output: &mut Vec<&'a MissionRunView>,
    ) {
        output.push(run);
        for step in &run.steps {
            for child in children.get(step.subject.as_str()).into_iter().flatten() {
                collect(child, children, output);
            }
        }
    }
    let mut output = Vec::new();
    collect(selected, children, &mut output);
    output
}

fn render_run_steps(
    output: &mut String,
    run: &MissionRunView,
    children: &BTreeMap<&str, Vec<&MissionRunView>>,
    indent: &str,
    style: OutputStyle,
) {
    let base_depth = run
        .steps
        .iter()
        .map(|step| step_depth(&step.step))
        .min()
        .unwrap_or_default();
    let mut roots = run
        .steps
        .iter()
        .filter(|step| step_depth(&step.step) == base_depth)
        .collect::<Vec<_>>();
    roots.sort_by(
        |left, right| match (left.queue_position, right.queue_position) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => std::cmp::Ordering::Equal,
        },
    );
    for step in roots {
        render_graph_step(output, step, indent, style);
        let prefix = format!("{}/", step.step);
        let nested = run
            .steps
            .iter()
            .filter(|candidate| candidate.step.starts_with(&prefix))
            .collect::<Vec<_>>();
        if !nested.is_empty() {
            let completed = nested
                .iter()
                .filter(|candidate| candidate.status == "completed")
                .count();
            if is_active_state(&step.status) || matches!(step.status.as_str(), "failed" | "blocked")
            {
                for nested_step in nested {
                    let relative = step_depth(&nested_step.step).saturating_sub(base_depth);
                    render_graph_step(
                        output,
                        nested_step,
                        &format!("{indent}{}", "  ".repeat(relative)),
                        style,
                    );
                }
            } else {
                let _ = writeln!(
                    output,
                    "{indent}  ↳ nested work · {completed}/{} completed",
                    nested.len()
                );
            }
        }
        for child in children.get(step.subject.as_str()).into_iter().flatten() {
            let child_completed = child
                .steps
                .iter()
                .filter(|nested| nested.status == "completed")
                .count();
            let _ = writeln!(
                output,
                "{indent}  ↳ {} · {} · {child_completed}/{} completed",
                short_subject(&child.mission, "mission/"),
                style.status(&child.status),
                child.steps.len()
            );
            if !matches!(child.status.as_str(), "completed" | "cancelled") {
                render_run_steps(output, child, children, &format!("{indent}    "), style);
            }
        }
    }
}

fn render_flat_step_tree(
    output: &mut String,
    steps: &[StepRunView],
    indent: &str,
    style: OutputStyle,
) {
    let base_depth = steps
        .iter()
        .map(|step| step_depth(&step.step))
        .min()
        .unwrap_or_default();
    for step in steps {
        let relative = step_depth(&step.step).saturating_sub(base_depth);
        render_graph_step(
            output,
            step,
            &format!("{indent}{}", "  ".repeat(relative)),
            style,
        );
    }
}

fn render_graph_step(output: &mut String, step: &StepRunView, indent: &str, style: OutputStyle) {
    let title = step.title.as_deref().unwrap_or(&step.step);
    let actor = work_actor(step)
        .map(|actor| format!(" · {}", short_subject(actor, "agent/")))
        .unwrap_or_default();
    let attempt = if step.attempt > 1 {
        format!(" · attempt {}", step.attempt)
    } else {
        String::new()
    };
    let queue = step
        .queue
        .as_deref()
        .zip(step.queue_position)
        .map(|(queue, position)| format!(" · queue {queue} #{position}"))
        .unwrap_or_default();
    let _ = writeln!(
        output,
        "{indent}{} {} {} — {title}{actor}{attempt}{queue}",
        style.status_value(&step.status, state_mark(&step.status)),
        style.status_value(&step.status, format!("{:<10}", step.status)),
        step.step.rsplit('/').next().unwrap_or(&step.step),
    );
    if let Some(reason) = &step.blocked_reason {
        let _ = writeln!(output, "{indent}  reason: {reason}");
    }
}

fn render_work_list_item(output: &mut String, step: &StepRunView, style: OutputStyle) {
    let title = step.title.as_deref().unwrap_or(&step.step);
    let _ = writeln!(
        output,
        "  {} {title}",
        style.status_value(&step.status, state_mark(&step.status))
    );
    let _ = write!(output, "    {}", step.subject);
    if let Some((queue, position)) = step.queue.as_deref().zip(step.queue_position) {
        let _ = write!(output, " · queue {queue} #{position}");
    }
    if let Some(actor) = work_actor(step) {
        let _ = write!(output, " · {}", short_subject(actor, "agent/"));
    }
    let _ = writeln!(output);
    if let Some(reason) = &step.blocked_reason {
        let _ = writeln!(output, "    reason: {reason}");
    }
}

fn append_named_list(output: &mut String, name: &str, values: &[String], style: OutputStyle) {
    if values.is_empty() {
        return;
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "{}", style.heading(name));
    for value in values {
        let _ = writeln!(output, "  • {value}");
    }
}

fn short_subject<'a>(subject: &'a str, prefix: &str) -> &'a str {
    subject.strip_prefix(prefix).unwrap_or(subject)
}

fn step_depth(step: &str) -> usize {
    step.matches('/').count()
}

fn is_active_state(status: &str) -> bool {
    matches!(
        status,
        "ready" | "claimed" | "working" | "verifying" | "blocked"
    )
}

fn is_moving_state(status: &str) -> bool {
    matches!(status, "ready" | "claimed" | "working" | "verifying")
}

fn is_terminal_state(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

fn state_mark(status: &str) -> &'static str {
    match status {
        "completed" => "✓",
        "working" | "verifying" => "▶",
        "claimed" => "◉",
        "ready" => "●",
        "blocked" => "!",
        "failed" => "✗",
        "cancelled" => "×",
        _ => "·",
    }
}

fn work_actor(step: &StepRunView) -> Option<&str> {
    step.claimant
        .as_deref()
        .or(step.assigned_to.as_deref())
        .or_else(|| (step.available_to.len() == 1).then(|| step.available_to[0].as_str()))
}

fn relative_time(value: u128, now: u128) -> String {
    let (future, delta) = if value >= now {
        (true, value - now)
    } else {
        (false, now - value)
    };
    let seconds = delta / 1_000;
    if seconds == 0 {
        return "now".into();
    }
    let amount = if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else {
        format!("{}d", seconds / 86_400)
    };
    if future {
        format!("in {amount}")
    } else {
        format!("{amount} ago")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use st3::model::{MissionRunInput, UnderSpec};

    fn step(subject: &str, path: &str, status: &str) -> StepRunView {
        StepRunView {
            subject: subject.into(),
            run: "mission-run/demo/run".into(),
            generation: "run-generation/demo-generation".into(),
            step: path.into(),
            queue: None,
            queue_position: None,
            definition_hash: "definition-hash".into(),
            status: status.into(),
            attempt: 1,
            assigned_to: Some("agent/demo/worker".into()),
            available_to: Vec::new(),
            agentless: false,
            title: Some(path.replace('/', " ")),
            goals: vec!["Complete the work.".into()],
            constraints: vec!["Keep the proof.".into()],
            under: Vec::<UnderSpec>::new(),
            worker_reported: false,
            claimant: None,
            claim_incarnation: None,
            claim_expires_at_unix_ms: None,
            readiness_epoch: 1,
            blocked_reason: None,
            not_before_unix_ms: None,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
        }
    }

    fn run(subject: &str, parent: Option<&str>, steps: Vec<StepRunView>) -> MissionRunView {
        MissionRunView {
            subject: subject.into(),
            id: subject
                .strip_prefix("mission-run/")
                .unwrap_or(subject)
                .into(),
            mission: "mission/demo".into(),
            generation: "run-generation/demo-generation".into(),
            initial_revision: "initial-revision".into(),
            revision: "current-revision".into(),
            root_revision: "root-revision".into(),
            root_mission_run: "mission-run/demo/run".into(),
            parent_step_run: parent.map(str::to_owned),
            workspace: "/workspace".into(),
            requester: "person/tester".into(),
            inputs: BTreeMap::from([(
                "branch".into(),
                MissionRunInput {
                    kind: MissionInputKind::Text,
                    value: "main".into(),
                    subject: None,
                    claim_id: None,
                },
            )]),
            mode: "run".into(),
            timeout_ms: None,
            deadline_at_unix_ms: None,
            status: "running".into(),
            phase: "normal".into(),
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
            steps,
        }
    }

    #[test]
    fn mission_view_expands_active_work_and_summarizes_completed_children() {
        let mut queue = step(
            "step-run/demo-generation/investigate",
            "investigate",
            "working",
        );
        queue.queue = Some("issues".into());
        queue.queue_position = Some(1);
        let nested = step(
            "step-run/demo-generation/investigate/research",
            "investigate/research",
            "completed",
        );
        let release = step("step-run/demo-generation/release", "release", "pending");
        let root = run("mission-run/demo/run", None, vec![queue, nested, release]);
        let mut child = run(
            "mission-run/demo/child",
            Some("step-run/demo-generation/release"),
            vec![step(
                "step-run/child-generation/publish",
                "publish",
                "completed",
            )],
        );
        child.status = "completed".into();

        let rendered =
            render_mission_run(&root, &[root.clone(), child], OutputStyle::plain(), 3_000);

        assert!(rendered.contains("MISSION  demo"));
        assert!(rendered.contains("PROGRESS  2/4 completed · 1 active"));
        assert!(rendered.contains("research — investigate research"));
        assert!(rendered.contains("queue issues #1"));
        assert!(rendered.contains("↳ demo · completed · 1/1 completed"));
        assert!(!rendered.contains("publish — publish"));
    }

    #[test]
    fn work_list_hides_waiting_and_terminal_items_by_default() {
        let ready = step("step-run/demo-generation/ready", "ready", "ready");
        let pending = step("step-run/demo-generation/later", "later", "pending");
        let completed = step("step-run/demo-generation/done", "done", "completed");

        let rendered = render_work_list(
            Some("agent/demo/worker"),
            &[ready, pending, completed],
            false,
            OutputStyle::plain(),
        );

        assert!(rendered.contains("READY"));
        assert!(rendered.contains("step-run/demo-generation/ready"));
        assert!(!rendered.contains("step-run/demo-generation/later"));
        assert!(rendered.contains("1 waiting items and 1 terminal items hidden"));
    }

    #[test]
    fn work_list_all_includes_waiting_and_terminal_items() {
        let pending = step("step-run/demo-generation/later", "later", "pending");
        let completed = step("step-run/demo-generation/done", "done", "completed");

        let rendered = render_work_list(None, &[pending, completed], true, OutputStyle::plain());

        assert!(rendered.contains("PENDING"));
        assert!(rendered.contains("COMPLETED"));
        assert!(rendered.contains("step-run/demo-generation/later"));
        assert!(rendered.contains("step-run/demo-generation/done"));
    }

    #[test]
    fn step_view_keeps_exact_work_identifiers() {
        let mut claimed = step("step-run/demo-generation/build", "build", "claimed");
        claimed.claimant = Some("agent/demo/worker".into());
        claimed.claim_incarnation = Some("incarnation/exact".into());
        claimed.claim_expires_at_unix_ms = Some(62_000);

        let rendered = render_step_run(&claimed, OutputStyle::plain(), 2_000);

        assert!(rendered.contains("SUBJECT    step-run/demo-generation/build"));
        assert!(rendered.contains("GENERATION run-generation/demo-generation"));
        assert!(rendered.contains("Lease        in 1m"));
        assert!(rendered.contains("• Complete the work."));
    }

    #[test]
    fn colored_style_adds_ansi_and_plain_style_does_not() {
        let colored = OutputStyle::colored().status("working");
        let plain = OutputStyle::plain().status("working");

        assert!(colored.contains("\x1b["));
        assert!(!plain.contains("\x1b["));
    }

    #[test]
    fn terminal_and_no_color_select_the_output_style() {
        assert!(OutputStyle::from_terminal(true, false).color);
        assert!(!OutputStyle::from_terminal(false, false).color);
        assert!(!OutputStyle::from_terminal(true, true).color);
    }

    #[test]
    fn follow_snapshots_redraw_terminals_and_append_to_pipes() {
        assert_eq!(follow_snapshot("frame", true, false), "\x1b[2J\x1b[Hframe");
        assert_eq!(follow_snapshot("frame", false, false), "frame");
        assert_eq!(follow_snapshot("frame", false, true), "\nframe");
    }

    #[test]
    fn mission_signature_ignores_lease_renewal_times() {
        let first = run(
            "mission-run/demo/run",
            None,
            vec![step("step-run/demo-generation/work", "work", "working")],
        );
        let mut renewed = first.clone();
        renewed.updated_at_unix_ms += 60_000;
        renewed.steps[0].updated_at_unix_ms += 60_000;
        renewed.steps[0].claim_expires_at_unix_ms = Some(120_000);

        assert_eq!(
            mission_run_signature(&[first]).unwrap(),
            mission_run_signature(&[renewed]).unwrap()
        );
    }

    #[test]
    fn generation_lineage_marks_the_current_generation() {
        let root = run("mission-run/demo/run", None, Vec::new());
        let generations = vec![RunGenerationView {
            subject: root.generation.clone(),
            id: "demo-generation".into(),
            run: root.subject.clone(),
            revision: root.revision.clone(),
            predecessor: None,
            status: "current".into(),
            actor: "agent/demo/worker".into(),
            reason: "Initial run".into(),
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
            steps: Vec::new(),
        }];

        let rendered = render_generations(&root, &generations, OutputStyle::plain());

        assert!(rendered.contains("● current  run-generation/demo-generation"));
        assert!(rendered.contains("revision current-revision"));
    }

    #[test]
    fn revision_proposal_keeps_the_exact_approval_token() {
        let proposal = RevisionProposalView {
            subject: "revision-proposal/exact".into(),
            id: "exact".into(),
            run: "mission-run/demo/run".into(),
            source_generation: "run-generation/old".into(),
            candidate_revision: "candidate-revision".into(),
            actor: "agent/demo/worker".into(),
            reason: "Add a verification step.".into(),
            status: "pending".into(),
            cutover: RevisionCutover::WhenIdle,
            compatible_steps: vec!["build".into()],
            reviewers: vec!["person/reviewer".into()],
            approvals: vec!["person/first-reviewer".into()],
            preview_hash: Some("full-preview-hash".into()),
            successor_generation: None,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
        };

        let rendered = render_revision_proposal(&proposal, OutputStyle::plain(), 3_000);

        assert!(rendered.contains("REVISION PROPOSAL  revision-proposal/exact"));
        assert!(rendered.contains("CUTOVER    when-idle"));
        assert!(rendered.contains("APPROVAL HASH full-preview-hash"));
        assert!(rendered.contains("• person/reviewer"));
    }

    #[test]
    fn generation_view_shows_blocked_retries_and_empty_work() {
        let mut blocked = step("step-run/new/build", "build", "blocked");
        blocked.attempt = 2;
        blocked.blocked_reason = Some("The review is pending.".into());
        let generation = RunGenerationView {
            subject: "run-generation/new".into(),
            id: "new".into(),
            run: "mission-run/demo/run".into(),
            revision: "revision".into(),
            predecessor: Some("run-generation/old".into()),
            status: "current".into(),
            actor: "agent/demo/worker".into(),
            reason: "Revise the work.".into(),
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
            steps: vec![blocked],
        };

        let rendered = render_generation(&generation, OutputStyle::plain());
        assert!(rendered.contains("blocked"));
        assert!(rendered.contains("attempt 2"));
        assert!(rendered.contains("reason: The review is pending."));

        let mut empty = generation;
        empty.steps.clear();
        assert!(render_generation(&empty, OutputStyle::plain()).contains("No steps."));
    }
}
