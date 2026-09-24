mod cache;
mod model;

use anyhow::{Context, Result};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use model::{Model, timeline_line};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs, Wrap},
};
use st3_client::{
    Client, ClientError, ErrorCode, Fence, LaunchCreateParameters, LaunchTarget,
    MessageSendParameters, TargetParameters, TerminalInputMode, TerminalInputParameters,
    TerminalScreen,
};
use std::{
    collections::{BTreeMap, HashSet},
    io::{self, IsTerminal, Stdout},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

const TABS: [&str; 4] = ["Now", "Chat", "Control", "Fleet"];
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}
impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        match Terminal::new(CrosstermBackend::new(io::stdout())) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(error) => {
                let _ = execute!(io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
                let _ = disable_raw_mode();
                Err(error.into())
            }
        }
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
        let _ = disable_raw_mode();
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Normal,
    Chat,
    Title,
    Request,
    Mission,
    Workspace,
}
struct Attached {
    terminal_id: String,
    attachment_id: String,
    screen: TerminalScreen,
}
enum Update {
    Partial(Box<Model>),
    Model(Box<Model>),
    Timeline(String, Vec<st3_client::TimelineEntry>, bool),
    TimelineInvalidated(String),
    TimelineCursorGap,
    Error(String),
}
struct App {
    model: Model,
    tab: usize,
    selected: [usize; 4],
    scroll: [u16; 4],
    sidebar: bool,
    show_system_missions: bool,
    mode: Mode,
    input: String,
    launch: [String; 4],
    attached: Option<Attached>,
    return_focused: bool,
    live_ready: bool,
    dirty: bool,
    timeline_requested: Option<String>,
    timeline_cache: BTreeMap<String, (Vec<st3_client::TimelineEntry>, bool)>,
    chat_scroll_cache: BTreeMap<String, u16>,
    chat_draft_cache: BTreeMap<String, String>,
    last_timeline: Instant,
    last_terminal: Instant,
}
impl App {
    fn new(model: Model) -> Self {
        Self {
            model,
            tab: 0,
            selected: [0; 4],
            scroll: [0; 4],
            sidebar: true,
            show_system_missions: false,
            mode: Mode::Normal,
            input: String::new(),
            launch: Default::default(),
            attached: None,
            return_focused: false,
            live_ready: false,
            dirty: true,
            timeline_requested: None,
            timeline_cache: BTreeMap::new(),
            chat_scroll_cache: BTreeMap::new(),
            chat_draft_cache: BTreeMap::new(),
            last_timeline: Instant::now(),
            last_terminal: Instant::now(),
        }
    }
    fn remember_chat_scroll(&mut self) {
        if let Some(id) = self.selected_session_id() {
            self.chat_scroll_cache.insert(id, self.scroll[1]);
            while self.chat_scroll_cache.len() > 32 {
                if let Some(oldest) = self.chat_scroll_cache.keys().next().cloned() {
                    self.chat_scroll_cache.remove(&oldest);
                }
            }
        }
    }
    fn invalidate_timelines(&mut self) {
        self.timeline_cache.clear();
        self.model.timeline.clear();
        self.model.timeline_truncated = false;
        self.timeline_requested = None;
        self.last_timeline = Instant::now() - Duration::from_secs(10);
    }
    fn restore_chat_scroll(&mut self) {
        self.scroll[1] = self
            .selected_session_id()
            .and_then(|id| self.chat_scroll_cache.get(&id).copied())
            .unwrap_or_default();
    }
    fn chat_draft_key(&self) -> Option<String> {
        self.selected_session_id()
            .or_else(|| self.peer().map(|peer| peer.header.id.clone()))
    }
    fn start_chat_composer(&mut self) {
        self.input = self
            .chat_draft_key()
            .and_then(|key| self.chat_draft_cache.get(&key).cloned())
            .unwrap_or_default();
        self.mode = Mode::Chat;
    }
    fn remember_chat_draft(&mut self) {
        if let Some(key) = self.chat_draft_key() {
            if self.input.is_empty() {
                self.chat_draft_cache.remove(&key);
            } else {
                self.chat_draft_cache.insert(key, self.input.clone());
                while self.chat_draft_cache.len() > 32 {
                    if let Some(oldest) = self.chat_draft_cache.keys().next().cloned() {
                        self.chat_draft_cache.remove(&oldest);
                    }
                }
            }
        }
    }
    fn peer(&self) -> Option<&st3_client::Agent> {
        self.agent_tree()
            .get(self.selected[1])
            .map(|(agent, _)| *agent)
    }
    fn agent_tree(&self) -> Vec<(&st3_client::Agent, usize)> {
        let agents = self.model.agents().collect::<Vec<_>>();
        let ids = agents
            .iter()
            .map(|a| a.header.id.as_str())
            .collect::<HashSet<_>>();
        let mut result = Vec::new();
        let mut seen = HashSet::new();
        fn add<'a>(
            agent: &'a st3_client::Agent,
            depth: usize,
            agents: &[&'a st3_client::Agent],
            seen: &mut HashSet<String>,
            result: &mut Vec<(&'a st3_client::Agent, usize)>,
        ) {
            if !seen.insert(agent.header.id.clone()) {
                return;
            }
            result.push((agent, depth));
            for child in agents.iter().filter(|child| {
                child
                    .under
                    .iter()
                    .any(|parent| parent.agent_id == agent.header.id)
            }) {
                add(child, depth + 1, agents, seen, result);
            }
        }
        for agent in &agents {
            if !agent
                .under
                .iter()
                .any(|parent| ids.contains(parent.agent_id.as_str()))
            {
                add(agent, 0, &agents, &mut seen, &mut result);
            }
        }
        for agent in &agents {
            add(agent, 0, &agents, &mut seen, &mut result);
        }
        result
    }
    fn undeclared_session(&self) -> Option<&st3_client::Session> {
        let index = self.selected[1].checked_sub(self.model.agents().count())?;
        self.model.undeclared_sessions().nth(index)
    }
    fn selected_session_id(&self) -> Option<String> {
        self.peer()
            .and_then(|peer| {
                peer.current_session_id.clone().or_else(|| {
                    self.model
                        .sessions
                        .items
                        .iter()
                        .find_map(|item| match item {
                            st3_client::Resource::Session(session)
                                if session.owner_id == peer.header.id
                                    && session.state == "running" =>
                            {
                                Some(session.header.id.clone())
                            }
                            _ => None,
                        })
                })
            })
            .or_else(|| {
                self.undeclared_session()
                    .map(|session| session.header.id.clone())
            })
    }
    fn runtime(&self) -> Option<&st3_client::Runtime> {
        let peer = self.peer()?;
        self.model
            .runtimes()
            .find(|runtime| runtime.owner_id == peer.header.id && runtime.terminal_id.is_some())
    }
    fn count(&self) -> usize {
        self.count_for(self.tab)
    }
    fn count_for(&self, tab: usize) -> usize {
        match tab {
            0 => self.model.attention().count(),
            1 => self.model.agents().count() + self.model.undeclared_sessions().count(),
            2 => self.control_missions().len(),
            _ => self.model.machines().count(),
        }
    }
    fn mission_group(&self, mission: &st3_client::Mission) -> &'static str {
        let steps = self
            .model
            .work()
            .filter(|work| mission.runs.contains(&work.mission_run_id));
        let states = steps.map(|work| work.state.as_str()).collect::<Vec<_>>();
        if states.contains(&"blocked") {
            "Blocked"
        } else if states.contains(&"waiting") {
            "Waiting"
        } else if matches!(mission.state.as_str(), "running" | "standing") {
            "Running"
        } else if matches!(mission.state.as_str(), "ready" | "draft") {
            "Drafts"
        } else {
            "Archive"
        }
    }
    fn control_missions(&self) -> Vec<&st3_client::Mission> {
        let mut missions = self
            .model
            .missions()
            .filter(|mission| {
                self.show_system_missions || !mission.header.id.starts_with("mission/__st3/")
            })
            .collect::<Vec<_>>();
        missions.sort_by_key(|mission| match self.mission_group(mission) {
            "Blocked" => 0,
            "Waiting" => 1,
            "Running" => 2,
            "Drafts" => 3,
            _ => 4,
        });
        missions
    }
    fn render(&self, frame: &mut ratatui::Frame<'_>) {
        let area = frame.area();
        let chunks = Layout::vertical([
            Constraint::Length(2),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(area);
        let title = if self.attached.is_some() {
            " Smalltalk  ·  [Return to Smalltalk] "
        } else {
            " Smalltalk  ·  st3 "
        };
        frame.render_widget(
            Paragraph::new(title)
                .style(Style::default().fg(if self.return_focused {
                    Color::Yellow
                } else {
                    Color::Cyan
                }))
                .block(Block::default().borders(Borders::BOTTOM)),
            chunks[0],
        );
        if let Some(attached) = &self.attached {
            let columns = Layout::horizontal([
                Constraint::Length(if self.sidebar && area.width >= 66 {
                    17
                } else {
                    0
                }),
                Constraint::Min(1),
            ])
            .split(chunks[1]);
            if columns[0].width > 0 {
                frame.render_widget(
                    Paragraph::new("Now\nChat\nControl\nFleet")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(Block::default().borders(Borders::ALL)),
                    columns[0],
                );
            }
            let lines: Vec<Line> = attached
                .screen
                .lines
                .iter()
                .map(|line| {
                    Line::from(if line.redacted {
                        "[redacted]"
                    } else {
                        &line.text
                    })
                })
                .collect();
            frame.render_widget(
                Paragraph::new(lines).block(
                    Block::default()
                        .title(attached.screen.title.as_str())
                        .borders(Borders::ALL),
                ),
                columns[1],
            );
            frame.render_widget(
                Paragraph::new("Ctrl+\\ detach  ·  Click Return control then Enter"),
                chunks[2],
            );
            return;
        }
        let rows = Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).split(chunks[1]);
        frame.render_widget(
            Tabs::new(TABS.map(Line::from))
                .select(self.tab)
                .highlight_style(Style::default().fg(Color::Cyan))
                .divider("  ·  "),
            rows[0],
        );
        let columns = Layout::horizontal([
            Constraint::Length(if self.sidebar && area.width >= 66 {
                area.width / 3
            } else {
                0
            }),
            Constraint::Min(1),
        ])
        .split(rows[1]);
        if columns[0].width > 0 {
            let list: Vec<String> = match self.tab {
                0 => self
                    .model
                    .attention()
                    .map(|v| format!("{} {}", v.priority, v.title))
                    .collect(),
                1 => self
                    .agent_tree()
                    .iter()
                    .map(|(v, depth)| {
                        format!("{}{}  ·  {}", "  ".repeat(*depth), agent_label(v), v.state)
                    })
                    .chain(self.model.undeclared_sessions().map(|v| {
                        let driver = v
                            .extra
                            .get("driver")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("harness");
                        format!("[undeclared] {driver} {}", v.header.id)
                    }))
                    .collect(),
                2 => self
                    .control_missions()
                    .iter()
                    .map(|v| format!("{}  ·  {}", mission_label(v), self.mission_group(v)))
                    .collect(),
                _ => self
                    .model
                    .machines()
                    .map(|v| format!("{} {}", v.name, v.state))
                    .collect(),
            };
            let entries = list
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    ListItem::new(format!("{v}\n")).style(Style::default().fg(
                        if i == self.selected[self.tab] {
                            Color::White
                        } else {
                            Color::Gray
                        },
                    ))
                })
                .collect::<Vec<_>>();
            let mut state = ListState::default().with_selected(Some(self.selected[self.tab]));
            frame.render_stateful_widget(
                List::new(if entries.is_empty() {
                    vec![ListItem::new("No items")]
                } else {
                    entries
                })
                .block(Block::default().title(" Browse ").borders(Borders::ALL))
                .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan))
                .highlight_symbol("› "),
                columns[0],
                &mut state,
            );
        }
        let mut lines = Vec::new();
        match self.tab {
            0 => {
                lines.push("Actionable person attention".into());
                if let Some(v) = self.model.attention().nth(self.selected[0]) {
                    lines.extend([
                        v.title.clone(),
                        v.detail.clone(),
                        format!("Source: {}", v.source_id),
                        format!("Actions: {}", v.actions.join(", ")),
                    ]);
                } else {
                    lines.push("Nothing needs your attention.".into());
                }
                if self.model.now.truncated {
                    lines.push("[More attention beyond bounded view]".into());
                }
            }
            1 => {
                if let Some(peer) = self.peer() {
                    lines.push(format!("AGENT  /  {}", peer.name));
                    lines.push(String::new());
                    lines.push(format!("{}  ·  {}", peer.state, peer.reachability));
                    if let Some(driver) = &peer.driver {
                        lines.push(format!("Harness: {driver}"));
                    }
                    lines.push(format!(
                        "Current session: {}",
                        self.selected_session_id().as_deref().unwrap_or("none")
                    ));
                    if !peer.current_work_ids.is_empty() || peer.next_work_id.is_some() {
                        lines.push(String::new());
                        lines.push("MISSION WORK".into());
                        for current in &peer.current_work_ids {
                            lines.push(format!("  Current: {current}"));
                        }
                        if let Some(next) = &peer.next_work_id {
                            lines.push(format!("  Next: {next}"));
                            lines.push(format!("  {} ready across runs", peer.queued_work_count));
                        }
                    }
                    lines.push(String::new());
                    lines.push("RECENT MESSAGES".into());
                    for message in self
                        .model
                        .messages(self.selected_session_id().as_deref(), &peer.header.id)
                        .take(4)
                    {
                        lines.push(format!(
                            "  {}: {}",
                            message.from,
                            message.content.replace('\n', " ⏎ ")
                        ));
                        lines.push(String::new());
                    }
                    lines.push("CONVERSATION".into());
                    for entry in self
                        .model
                        .timeline
                        .iter()
                        .rev()
                        .filter(|entry| matches!(entry.body, st3_client::TimelineBody::Content(_)))
                        .take(12)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                    {
                        lines.push(format!("  {}", timeline_line(entry)));
                        lines.push(String::new());
                    }
                    if self.model.timeline_truncated || self.model.messages.truncated {
                        lines.push("[More history beyond bounded view]".into());
                    }
                } else if let Some(session) = self.undeclared_session() {
                    let driver = session
                        .extra
                        .get("driver")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("harness");
                    let exact = session
                        .extra
                        .get("native_session_id")
                        .and_then(serde_json::Value::as_str)
                        .is_some();
                    lines.push(format!("Undeclared {driver} · {}", session.state));
                    lines.push(format!("Session: {}", session.header.id));
                    lines.push(
                        if exact {
                            "Exact native session · read-only"
                        } else {
                            "Unresolved running process · read-only"
                        }
                        .into(),
                    );
                    if let Some(workspace) = session
                        .extra
                        .get("workspace")
                        .and_then(serde_json::Value::as_str)
                    {
                        lines.push(format!("Workspace: {workspace}"));
                    }
                    lines.push("── Normalized history ──".into());
                    lines.extend(self.model.timeline.iter().map(timeline_line));
                    if self.model.timeline_truncated || self.model.sessions.truncated {
                        lines.push("[More sessions or history beyond bounded view]".into());
                    }
                    lines.push("This session is not managed by st3; message and terminal controls are unavailable.".into());
                } else {
                    lines.push("No agents or undeclared sessions available.".into());
                }
            }
            2 => {
                lines.push("CONTROL  /  MISSIONS".into());
                lines.push(String::new());
                if let Some(mission) = self.control_missions().get(self.selected[2]).copied() {
                    lines.push(mission_label(mission));
                    lines.push(format!(
                        "{}  ·  {} runs",
                        self.mission_group(mission),
                        mission.runs.len()
                    ));
                    lines.push(mission.header.id.clone());
                    lines.push(String::new());
                    lines.push("CURRENT WORK".into());
                    let steps = self
                        .model
                        .work()
                        .filter(|work| mission.runs.contains(&work.mission_run_id));
                    let mut count = 0;
                    for step in steps {
                        if matches!(step.state.as_str(), "completed" | "cancelled") {
                            continue;
                        }
                        lines.push(format!("  {}  ·  {}", step.path, step.state));
                        if let Some(reason) = &step.blocked_reason {
                            lines.push(format!("    Blocked: {reason}"));
                        }
                        if let Some(goal) = step.goals.first() {
                            lines.push(format!("    Goal: {goal}"));
                        }
                        if let Some(claimant) = &step.claimant {
                            lines.push(format!("    Agent: {claimant}"));
                        }
                        count += 1;
                    }
                    if count == 0 {
                        lines.push("  No active steps.".into());
                    }
                } else {
                    lines.push("No current missions.".into());
                }
                lines.push(String::new());
                if self.model.launches().next().is_some() {
                    lines.push("PLANNER LAUNCHES".into());
                    for launch in self.model.launches().take(4) {
                        lines.push(format!("  {}  ·  {}", launch.title, launch.phase));
                    }
                }
                lines.push(String::new());
                lines.push(format!(
                    "c create mission  ·  x {} system missions",
                    if self.show_system_missions {
                        "hide"
                    } else {
                        "show"
                    }
                ));
                if self.model.work.truncated {
                    lines.push("[More progress beyond bounded view]".into());
                }
            }
            _ => {
                lines.push("FLEET  /  MACHINES".into());
                lines.push(String::new());
                if let Some(machine) = self.model.machines().nth(self.selected[3]) {
                    lines.push(format!("{}  ·  {}", machine.name, machine.state));
                    lines.push(format!("Capacity: {}", machine.capacity.state));
                    lines.push(format!(
                        "Running runtimes: {}",
                        machine.occupancy.running_runtimes
                    ));
                    lines.push(String::new());
                    lines.push("CONNECTIVITY".into());
                    for transport in &machine.transports {
                        lines.push(format!("  {:<18} {}", transport.protocol, transport.status));
                    }
                } else {
                    lines.push("No machines in the current snapshot.".into());
                }
                lines.push(String::new());
                lines.push("AGENT WORK".into());
                for agent in self
                    .model
                    .agents()
                    .filter(|agent| agent.active_work_count > 0 || agent.queued_work_count > 0)
                {
                    lines.push(format!(
                        "  {}  ·  {} active · {} queued",
                        agent_label(agent),
                        agent.active_work_count,
                        agent.queued_work_count
                    ));
                    if let Some(next) = &agent.next_work_id {
                        lines.push(format!("    Next: {next}"));
                    }
                }
                lines.push(String::new());
                lines.push("YOU & DEVICES".into());
                for v in self.model.devices() {
                    lines.push(format!("  {}  ·  {}", v.person_id, v.state));
                }
                let gateway = self
                    .model
                    .sessions
                    .snapshot
                    .as_ref()
                    .map(|s| s.host_id.as_str())
                    .unwrap_or("connected machine");
                lines.push(String::new());
                lines.push(format!("UNDECLARED ON {gateway}"));
                for session in self.model.undeclared_sessions() {
                    let driver = session
                        .extra
                        .get("driver")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("harness");
                    let exact = session
                        .extra
                        .get("native_session_id")
                        .and_then(serde_json::Value::as_str)
                        .is_some();
                    lines.push(format!(
                        "  {driver}  ·  {}  ·  {}",
                        if exact {
                            "native session"
                        } else {
                            "unresolved process"
                        },
                        session.header.id
                    ));
                }
                lines.push(String::new());
                lines.push("Discovery is local to the connected gateway.".into());
                if self.model.machines.truncated || self.model.devices.truncated {
                    lines.push("[More fleet items beyond bounded view]".into());
                }
            }
        }
        frame.render_widget(
            Paragraph::new(lines.join("\n"))
                .wrap(Wrap { trim: false })
                .scroll((self.scroll[self.tab], 0))
                .block(Block::default().title(TABS[self.tab]).borders(Borders::ALL)),
            columns[1],
        );
        let footer = match self.mode {
            Mode::Normal => format!(
                "{}  ·  1–4 views  ·  ↑↓ select  ·  s sidebar  ·  {}  ·  q quit",
                self.model.status,
                match self.tab {
                    1 => "Enter terminal / c message",
                    2 => "c new mission",
                    _ => "",
                }
            ),
            Mode::Chat => format!("Message: {}█ · Enter send · Esc cancel", self.input),
            Mode::Title => format!("Launch title: {}█", self.input),
            Mode::Request => format!("Request: {}█", self.input),
            Mode::Mission => format!("Mission ID: {}█", self.input),
            Mode::Workspace => format!("Workspace: {}█ · Enter create", self.input),
        };
        frame.render_widget(Paragraph::new(footer), chunks[2]);
    }
}

fn action_pair() -> (String, String) {
    let id = format!("action/{}", uuid::Uuid::now_v7());
    (id.clone(), id)
}
fn mission_label(mission: &st3_client::Mission) -> String {
    let slug = mission.title.rsplit('/').next().unwrap_or(&mission.title);
    slug.split('-')
        .map(|word| match word.to_ascii_lowercase().as_str() {
            "tui" => "TUI".into(),
            "ios" => "iOS".into(),
            "st3" => "ST3".into(),
            "api" => "API".into(),
            "pty" => "PTY".into(),
            _ => {
                let mut chars = word.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                    .unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
fn agent_label(agent: &st3_client::Agent) -> String {
    let slug = agent.name.rsplit('/').next().unwrap_or(&agent.name);
    slug.split('-')
        .map(|word| match word.to_ascii_lowercase().as_str() {
            "st3" => "ST3".to_string(),
            "cos" => "COS".to_string(),
            "ios" => "iOS".to_string(),
            "tui" => "TUI".to_string(),
            "pty" => "PTY".to_string(),
            _ => {
                let mut chars = word.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                    .unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
fn key_input(key: KeyEvent) -> Option<String> {
    match key.code {
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => Some(format!("C-{c}")),
        KeyCode::Char(c) => Some(c.to_string()),
        KeyCode::Enter => Some("return".into()),
        KeyCode::Tab => Some("tab".into()),
        KeyCode::Backspace => Some("backspace".into()),
        KeyCode::Esc => Some("escape".into()),
        KeyCode::Up => Some("Up".into()),
        KeyCode::Down => Some("Down".into()),
        KeyCode::Left => Some("Left".into()),
        KeyCode::Right => Some("Right".into()),
        KeyCode::Delete => Some("Delete".into()),
        KeyCode::Home => Some("Home".into()),
        KeyCode::End => Some("End".into()),
        _ => None,
    }
}

async fn attach(app: &mut App, client: &Client) -> Result<()> {
    let Some(runtime) = app.runtime() else {
        app.model.status = "No controllable terminal for selected agent".into();
        app.dirty = true;
        return Ok(());
    };
    let Some(terminal_id) = runtime.terminal_id.clone() else {
        app.model.status = "No terminal on selected runtime".into();
        return Ok(());
    };
    let runtime_id = runtime.header.id.clone();
    let fence = app
        .model
        .runtimes
        .fence(&runtime_id)
        .context("runtime fence unavailable")?;
    let (id, key) = action_pair();
    let attachment = client
        .terminal_attach(
            id,
            key,
            fence,
            TargetParameters {
                target_id: terminal_id.clone(),
                ..Default::default()
            },
        )
        .await?
        .value
        .terminal_attachment
        .context("attach returned no viewer")?;
    let screen = if let Some(capability) = attachment.stream_capability.as_deref() {
        client
            .terminal_frames(
                &terminal_id,
                None,
                Some(&attachment.runtime_incarnation),
                capability,
                Some(0),
            )
            .await?
            .screen
            .value
    } else {
        client.terminal_screen(&terminal_id).await?.value
    };
    app.attached = Some(Attached {
        terminal_id,
        attachment_id: attachment.attachment_id,
        screen,
    });
    app.dirty = true;
    Ok(())
}
async fn detach(app: &mut App, client: &Client) -> Result<()> {
    let Some(attached) = app.attached.as_ref() else {
        return Ok(());
    };
    let terminal_id = attached.terminal_id.clone();
    let attachment_id = attached.attachment_id.clone();
    let incarnation = attached.screen.runtime_incarnation.clone();
    for attempt in 0..3 {
        let fence = terminal_fence(client, &terminal_id, &incarnation).await?;
        let (id, key) = action_pair();
        match client
            .terminal_detach(
                id,
                key,
                fence,
                TargetParameters {
                    target_id: attachment_id.clone(),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => break,
            Err(ClientError::Api(ErrorCode::StaleFence, _, _)) if attempt < 2 => continue,
            Err(error) => return Err(error.into()),
        }
    }
    app.attached = None;
    app.return_focused = false;
    app.dirty = true;
    Ok(())
}
async fn terminal_fence(client: &Client, terminal_id: &str, incarnation: &str) -> Result<Fence> {
    let screen = client.terminal_screen(terminal_id).await?;
    terminal_screen_fence(&screen, incarnation)
}
fn terminal_screen_fence(
    screen: &st3_client::Envelope<st3_client::TerminalScreen>,
    incarnation: &str,
) -> Result<Fence> {
    anyhow::ensure!(
        screen.value.runtime_incarnation == incarnation,
        "terminal incarnation changed; reattach before sending input"
    );
    Ok(Fence {
        snapshot_id: screen.snapshot.id.clone(),
        runtime_incarnation: Some(incarnation.to_owned()),
        terminal_sequence: Some(screen.value.next_sequence),
        ..Fence::default()
    })
}
async fn handle_key(app: &mut App, client: &Client, key: KeyEvent) -> Result<bool> {
    if key.kind != KeyEventKind::Press {
        return Ok(false);
    }
    if let Some(attached) = &app.attached {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('\\') {
            detach(app, client).await?;
            return Ok(false);
        }
        if app.return_focused && key.code == KeyCode::Enter {
            detach(app, client).await?;
            return Ok(false);
        }
        if let Some(value) = key_input(key) {
            for attempt in 0..3 {
                let fence = terminal_fence(
                    client,
                    &attached.terminal_id,
                    &attached.screen.runtime_incarnation,
                )
                .await?;
                let (id, idem) = action_pair();
                match client
                    .terminal_input(
                        id,
                        idem,
                        fence,
                        TerminalInputParameters {
                            terminal_id: attached.terminal_id.clone(),
                            mode: TerminalInputMode::Key,
                            value: value.clone(),
                        },
                    )
                    .await
                {
                    Ok(_) => break,
                    Err(ClientError::Api(ErrorCode::StaleFence, _, _)) if attempt < 2 => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        return Ok(false);
    }
    if app.mode != Mode::Normal {
        match key.code {
            KeyCode::Esc => {
                if app.mode == Mode::Chat {
                    app.remember_chat_draft();
                }
                app.mode = Mode::Normal;
                app.input.clear();
            }
            KeyCode::Backspace => {
                app.input.pop();
            }
            KeyCode::Enter => {
                let value = std::mem::take(&mut app.input);
                match app.mode {
                    Mode::Chat => {
                        if !value.trim().is_empty() {
                            if !app.live_ready {
                                app.input = value;
                                app.model.status = "Reconnect before sending".into();
                                app.dirty = true;
                                return Ok(false);
                            }
                            let peer = app.peer().context("no selected agent")?;
                            let fence = Fence {
                                snapshot_id: app
                                    .model
                                    .messages
                                    .snapshot
                                    .as_ref()
                                    .or(app.model.agents.snapshot.as_ref())
                                    .context("no snapshot")?
                                    .id
                                    .clone(),
                                ..Default::default()
                            };
                            let (id, idem) = action_pair();
                            if let Err(error) = client
                                .message_send(
                                    id,
                                    idem,
                                    fence,
                                    MessageSendParameters {
                                        to: peer.header.id.clone(),
                                        content: value.clone(),
                                        title: None,
                                        in_reply_to: None,
                                        session_id: peer.current_session_id.clone(),
                                        tags: vec![],
                                    },
                                )
                                .await
                            {
                                app.input = value;
                                app.remember_chat_draft();
                                app.model.status = format!("Send failed: {error}");
                                app.dirty = true;
                                return Ok(false);
                            }
                            if let Some(key) = app.chat_draft_key() {
                                app.chat_draft_cache.remove(&key);
                            }
                            if let Err(error) = app.model.reload(client).await {
                                app.model.status = format!("Sent; refresh failed: {error}");
                            }
                        }
                        app.mode = Mode::Normal;
                    }
                    Mode::Title => {
                        app.launch[0] = value;
                        app.mode = Mode::Request;
                    }
                    Mode::Request => {
                        app.launch[1] = value;
                        app.mode = Mode::Mission;
                        app.input = format!("mission/{}", uuid::Uuid::now_v7());
                    }
                    Mode::Mission => {
                        app.launch[2] = value;
                        app.mode = Mode::Workspace;
                    }
                    Mode::Workspace => {
                        if !app.live_ready {
                            app.input = value;
                            app.model.status = "Reconnect before creating a launch".into();
                            app.dirty = true;
                            return Ok(false);
                        }
                        app.launch[3] = value;
                        let fence = Fence {
                            snapshot_id: app
                                .model
                                .launches
                                .snapshot
                                .as_ref()
                                .or(app.model.missions.snapshot.as_ref())
                                .context("no snapshot")?
                                .id
                                .clone(),
                            ..Default::default()
                        };
                        let (id, idem) = action_pair();
                        client
                            .launch_create(
                                id,
                                idem,
                                fence,
                                LaunchCreateParameters {
                                    title: app.launch[0].clone(),
                                    request: app.launch[1].clone(),
                                    target: LaunchTarget::NewMission {
                                        mission_id: app.launch[2].clone(),
                                        workspace: app.launch[3].clone(),
                                    },
                                    provider: None,
                                    model: None,
                                    effort: None,
                                },
                            )
                            .await?;
                        app.model.reload(client).await?;
                        app.mode = Mode::Normal;
                    }
                    Mode::Normal => {}
                }
            }
            KeyCode::Char(c)
                if !key.modifiers.contains(KeyModifiers::CONTROL) && app.input.len() < 4096 =>
            {
                app.input.push(c);
            }
            _ => {}
        }
        app.dirty = true;
        return Ok(false);
    }
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
        KeyCode::Char(d @ '1'..='4') => {
            app.tab = d as usize - '1' as usize;
            app.selected[app.tab] = app.selected[app.tab].min(app.count().saturating_sub(1));
        }
        KeyCode::Char('s') => app.sidebar = !app.sidebar,
        KeyCode::Char('x') if app.tab == 2 => {
            app.show_system_missions = !app.show_system_missions;
            app.selected[2] = app.selected[2].min(app.count_for(2).saturating_sub(1));
        }
        KeyCode::Up => {
            if app.tab == 1 {
                app.remember_chat_scroll();
            }
            app.selected[app.tab] = app.selected[app.tab].saturating_sub(1);
            if app.tab == 1 {
                app.restore_chat_scroll();
            } else {
                app.scroll[app.tab] = 0;
            }
        }
        KeyCode::Down => {
            if app.tab == 1 {
                app.remember_chat_scroll();
            }
            app.selected[app.tab] = (app.selected[app.tab] + 1).min(app.count().saturating_sub(1));
            if app.tab == 1 {
                app.restore_chat_scroll();
            } else {
                app.scroll[app.tab] = 0;
            }
        }
        KeyCode::PageUp => {
            app.scroll[app.tab] = app.scroll[app.tab].saturating_sub(10);
            if app.tab == 1 {
                app.remember_chat_scroll();
            }
        }
        KeyCode::PageDown => {
            app.scroll[app.tab] = app.scroll[app.tab].saturating_add(10);
            if app.tab == 1 {
                app.remember_chat_scroll();
            }
        }
        KeyCode::Char('c') if app.tab == 1 => {
            if !app.live_ready {
                app.model.status = "Reconnect before composing".into();
            } else if app.peer().is_some() {
                app.start_chat_composer();
            } else {
                app.model.status = "Undeclared sessions are read-only".into();
            }
        }
        KeyCode::Char('c') if app.tab == 2 => {
            if app.live_ready {
                app.mode = Mode::Title;
                app.input.clear();
            } else {
                app.model.status = "Reconnect before creating a launch".into();
            }
        }
        KeyCode::Enter if app.tab == 1 => {
            if app.live_ready {
                attach(app, client).await?;
            } else {
                app.model.status = "Reconnect before attaching a terminal".into();
            }
        }
        _ => {}
    }
    app.dirty = true;
    Ok(false)
}

fn main() -> Result<()> {
    if !io::stdout().is_terminal() {
        anyhow::bail!("stui needs an interactive terminal");
    }
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ] {
        signal_hook::flag::register(signal, stopping.clone())?;
    }
    let path =
        st3_client::discover_unix_endpoint(std::env::var_os("ST3_ENDPOINT").map(PathBuf::from))?;
    let person = std::env::var("ST3_PERSON").ok();
    let cache_path = person
        .as_deref()
        .and_then(|actor| cache::path(&path, actor));
    let client = match person.as_deref() {
        Some(person) => Client::unix_as(&path, person),
        None => Client::unix(&path),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (updates, incoming) = mpsc::channel::<Update>();
    let background_client = client.clone();
    let background_updates = updates.clone();
    let background_cache_path = cache_path.clone();
    let background_actor = person.clone();
    runtime.spawn(async move {
        let mut model = loop {
            match Model::bootstrap(&background_client).await {
                Ok(model) => break model,
                Err(error) => {
                    if background_updates
                        .send(Update::Error(format!("Initial load: {error}")))
                        .is_err()
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        };
        let _ = background_updates.send(Update::Partial(Box::new(model.clone())));
        loop {
            match model.reload(&background_client).await {
                Ok(()) => break,
                Err(error) => {
                    if background_updates
                        .send(Update::Error(format!("Initial details: {error}")))
                        .is_err()
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        if let (Some(path), Some(actor)) = (&background_cache_path, &background_actor) {
            let _ = cache::save(path, actor, &model);
        }
        let _ = background_updates.send(Update::Model(Box::new(model.clone())));
        let mut last_external_scan = Instant::now();
        let mut last_cache_save = Instant::now();
        let mut was_offline = false;
        loop {
            let mut changed = match model.sync(&background_client).await {
                Ok((changed, invalidated_sessions, cursor_gap)) => {
                    if cursor_gap && background_updates.send(Update::TimelineCursorGap).is_err() {
                        return;
                    }
                    for id in invalidated_sessions {
                        if background_updates
                            .send(Update::TimelineInvalidated(id))
                            .is_err()
                        {
                            return;
                        }
                    }
                    let recovered = was_offline;
                    was_offline = false;
                    if recovered {
                        model.status = "Connected".into();
                    }
                    changed || recovered
                }
                Err(error) => {
                    was_offline = true;
                    if background_updates
                        .send(Update::Error(format!("Sync: {error}")))
                        .is_err()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    false
                }
            };
            if last_external_scan.elapsed() >= Duration::from_secs(15) {
                match model.refresh_sessions(&background_client).await {
                    Ok(sessions_changed) => changed |= sessions_changed,
                    Err(error) => {
                        if background_updates
                            .send(Update::Error(format!("Session discovery: {error}")))
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                last_external_scan = Instant::now();
            }
            if changed && last_cache_save.elapsed() >= Duration::from_secs(60) {
                if let (Some(path), Some(actor)) = (&background_cache_path, &background_actor) {
                    let _ = cache::save(path, actor, &model);
                }
                last_cache_save = Instant::now();
            }
            if changed
                && background_updates
                    .send(Update::Model(Box::new(model.clone())))
                    .is_err()
            {
                break;
            }
        }
    });
    let mut app = App::new(Model::default());
    app.model.status = "Loading…".into();
    let mut guard = TerminalGuard::enter()?;
    #[cfg(debug_assertions)]
    if std::env::var_os("STUI_TEST_PANIC_AFTER_ENTER").is_some() {
        panic!("terminal restoration probe");
    }
    guard.terminal.draw(|frame| app.render(frame))?;
    app.dirty = false;
    if let (Some(path), Some(actor)) = (&cache_path, &person)
        && let Some(cached) = cache::load(path, actor)
    {
        app.model = cached;
        app.dirty = true;
    }
    while !stopping.load(Ordering::Relaxed) {
        while let Ok(update) = incoming.try_recv() {
            match update {
                Update::Partial(model) => {
                    if !app.live_ready {
                        if app.model.status.starts_with("Cached") && app.model.actor == model.actor
                        {
                            app.model.now = model.now;
                            app.model.agents = model.agents;
                            app.model.sessions = model.sessions;
                            app.model.messages = model.messages;
                            app.model.status = "Cached · loading details…".into();
                        } else {
                            app.model = *model;
                        }
                        app.dirty = true;
                    }
                }
                Update::Model(mut model) => {
                    if app.model.actor == model.actor {
                        model.timeline = std::mem::take(&mut app.model.timeline);
                        model.timeline_truncated = app.model.timeline_truncated;
                    } else {
                        app.timeline_cache.clear();
                        app.chat_scroll_cache.clear();
                        app.chat_draft_cache.clear();
                        app.timeline_requested = None;
                    }
                    app.model = *model;
                    app.live_ready = true;
                    for tab in 0..4 {
                        app.selected[tab] =
                            app.selected[tab].min(app.count_for(tab).saturating_sub(1));
                    }
                    app.dirty = true;
                }
                Update::Timeline(id, timeline, truncated) => {
                    app.timeline_cache
                        .insert(id.clone(), (timeline.clone(), truncated));
                    while app.timeline_cache.len() > 32 {
                        if let Some(oldest) = app.timeline_cache.keys().next().cloned() {
                            app.timeline_cache.remove(&oldest);
                        }
                    }
                    if app.selected_session_id().as_deref() == Some(&id) {
                        app.model.timeline = timeline;
                        app.model.timeline_truncated = truncated;
                        app.dirty = true;
                    }
                }
                Update::TimelineInvalidated(id) => {
                    if app.selected_session_id().as_deref() == Some(&id) {
                        app.last_timeline = Instant::now() - Duration::from_secs(10);
                    }
                }
                Update::TimelineCursorGap => {
                    app.invalidate_timelines();
                    app.dirty = true;
                }
                Update::Error(error) => {
                    if error.starts_with("Sync:") || error.starts_with("Initial load:") {
                        app.live_ready = false;
                    }
                    if app.model.status != error {
                        app.model.status = error;
                        app.dirty = true;
                    }
                }
            }
        }
        if app.tab == 1 {
            let selected_id = app.selected_session_id();
            if selected_id != app.timeline_requested
                || app.last_timeline.elapsed() >= Duration::from_secs(10)
            {
                if selected_id != app.timeline_requested {
                    let cached = selected_id
                        .as_ref()
                        .and_then(|id| app.timeline_cache.get(id))
                        .cloned();
                    (app.model.timeline, app.model.timeline_truncated) = cached.unwrap_or_default();
                    app.dirty = true;
                }
                app.timeline_requested = selected_id.clone();
                app.last_timeline = Instant::now();
                if let Some(id) = selected_id {
                    let timeline_client = client.clone();
                    let timeline_updates = updates.clone();
                    runtime.spawn(async move {
                        let mut model = Model::default();
                        match model.load_timeline(&timeline_client, &id).await {
                            Ok(()) => {
                                let _ = timeline_updates.send(Update::Timeline(
                                    id,
                                    model.timeline,
                                    model.timeline_truncated,
                                ));
                            }
                            Err(error) => {
                                let _ = timeline_updates
                                    .send(Update::Error(format!("Conversation: {error}")));
                            }
                        }
                    });
                } else {
                    app.model.timeline.clear();
                    app.dirty = true;
                }
            }
        }
        if app.dirty {
            guard.terminal.draw(|frame| app.render(frame))?;
            app.dirty = false;
        }
        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                Event::Key(key) => match runtime.block_on(handle_key(&mut app, &client, key)) {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => {
                        app.model.status = error.to_string();
                        app.dirty = true;
                    }
                },
                Event::Mouse(mouse)
                    if matches!(mouse.kind, MouseEventKind::Down(_))
                        && app.attached.is_some()
                        && mouse.row < 2 =>
                {
                    app.return_focused = true;
                    app.dirty = true;
                }
                Event::Resize(_, _) => app.dirty = true,
                _ => {}
            }
        }
        if let Some(attached) = app.attached.as_mut()
            && app.last_terminal.elapsed() >= Duration::from_millis(400)
        {
            if let Ok(screen) = runtime.block_on(client.terminal_screen(&attached.terminal_id))
                && attached.screen != screen.value
            {
                attached.screen = screen.value;
                app.dirty = true;
            }
            app.last_terminal = Instant::now();
        }
    }
    if app.attached.is_some() {
        let _ = runtime.block_on(detach(&mut app, &client));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    #[test]
    fn all_views_render_at_normal_and_narrow_width() {
        let mut app = App::new(Model::default());
        for width in [80, 40] {
            let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
            for (tab, label) in TABS.iter().enumerate() {
                app.tab = tab;
                terminal.draw(|frame| app.render(frame)).unwrap();
                let content = terminal
                    .backend()
                    .buffer()
                    .content()
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(content.contains(label));
            }
        }
    }
    #[test]
    fn terminal_key_encoding() {
        assert_eq!(
            key_input(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::CONTROL)).as_deref(),
            Some("C-x")
        );
    }

    #[test]
    fn terminal_fence_uses_owner_sequence_for_relayed_screen() {
        let mut screen: st3_client::Envelope<st3_client::TerminalScreen> = serde_json::from_str(
            include_str!("../../../docs/st3/client-v0/fixtures/terminal-screen.json"),
        )
        .unwrap();
        screen.snapshot.store_index = 10;
        screen.value.next_sequence = 42;
        let incarnation = screen.value.runtime_incarnation.clone();
        let fence = terminal_screen_fence(&screen, &incarnation).unwrap();
        assert_eq!(fence.terminal_sequence, Some(42));
        assert_eq!(fence.snapshot_id, screen.snapshot.id);
    }

    #[test]
    fn agent_relationships_render_as_a_tree_and_selection_tracks_it() {
        let parent: st3_client::Resource = serde_json::from_str(r#"{"kind":"agent","id":"agent/root","revision":"one","updated_at":"2026-09-24T09:00:00Z","name":"Root","state":"running","reachability":"local","runtime_ids":[],"under":[]}"#).unwrap();
        let child: st3_client::Resource = serde_json::from_str(r#"{"kind":"agent","id":"agent/child","revision":"one","updated_at":"2026-09-24T09:00:00Z","name":"Child","state":"running","reachability":"local","runtime_ids":[],"under":[{"agent_id":"agent/root","reason":"delegated"}]}"#).unwrap();
        let mut model = Model::default();
        model.agents.items.extend([child, parent]);
        let mut app = App::new(model);
        assert_eq!(
            app.agent_tree()
                .iter()
                .map(|(agent, depth)| (agent.name.as_str(), *depth))
                .collect::<Vec<_>>(),
            vec![("Root", 0), ("Child", 1)]
        );
        app.selected[1] = 1;
        assert_eq!(app.peer().unwrap().name, "Child");
        assert_eq!(agent_label(app.peer().unwrap()), "Child");
    }

    #[test]
    fn chat_and_fleet_show_agent_work_queue() {
        let agent: st3_client::Resource = serde_json::from_value(serde_json::json!({
            "kind": "agent", "id": "agent/worker", "revision": "one",
            "updated_at": "2026-09-24T09:00:00Z", "name": "Worker",
            "state": "running", "reachability": "local", "runtime_ids": [],
            "current_work_ids": ["step-run/old/work"], "active_work_count": 1,
            "next_work_id": "step-run/new/review", "queued_work_count": 2
        }))
        .unwrap();
        let mut model = Model::default();
        model.agents.items.push(agent);
        let mut app = App::new(model);
        let mut terminal = Terminal::new(TestBackend::new(100, 35)).unwrap();
        for (tab, expected) in [(1, "Next: step-run/new/review"), (3, "2 queued")] {
            app.tab = tab;
            terminal.draw(|frame| app.render(frame)).unwrap();
            let rows = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(rows.contains(expected), "tab {tab} did not show {expected}");
        }
    }

    #[test]
    fn chat_and_fleet_label_undeclared_sessions() {
        let session: st3_client::Resource = serde_json::from_str(
            r#"{"kind":"session","id":"session/external","revision":"one","updated_at":"2026-09-24T09:00:00Z","owner_id":"external-session/codex/one","state":"running","started_at":"2026-09-24T08:00:00Z","ended_at":null,"timeline_cursor":"cursor/one","managed":false,"driver":"codex","native_session_id":"one"}"#,
        )
        .unwrap();
        let mut model = Model::default();
        model.sessions.items.push(session);
        let mut app = App::new(model);
        let mut terminal = Terminal::new(TestBackend::new(100, 25)).unwrap();
        for tab in [1, 3] {
            app.tab = tab;
            terminal.draw(|frame| app.render(frame)).unwrap();
            let content = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(content.to_lowercase().contains("undeclared"));
        }
    }

    #[test]
    fn chat_finds_running_session_when_agent_field_is_absent() {
        let agent: st3_client::Resource = serde_json::from_str(r#"{"kind":"agent","id":"agent/cos","revision":"one","updated_at":"2026-09-24T09:00:00Z","name":"cos","state":"running","reachability":"reachable"}"#).unwrap();
        let session: st3_client::Resource = serde_json::from_str(r#"{"kind":"session","id":"session/cos-current","revision":"one","updated_at":"2026-09-24T09:00:00Z","owner_id":"agent/cos","state":"running","started_at":"2026-09-24T08:00:00Z","ended_at":null,"timeline_cursor":"cursor/one"}"#).unwrap();
        let mut model = Model::default();
        model.agents.items.push(agent);
        model.sessions.items.push(session);
        let app = App::new(model);
        assert_eq!(
            app.selected_session_id().as_deref(),
            Some("session/cos-current")
        );
    }

    #[test]
    fn chat_restores_each_sessions_scroll_position() {
        let mut model = Model::default();
        for (name, session) in [("alpha", "session/alpha"), ("beta", "session/beta")] {
            let agent: st3_client::Resource = serde_json::from_value(serde_json::json!({
                "kind":"agent", "id":format!("agent/{name}"), "revision":"one",
                "updated_at":"2026-09-24T09:00:00Z", "name":name, "state":"running",
                "reachability":"reachable", "current_session_id":session
            }))
            .unwrap();
            model.agents.items.push(agent);
        }
        let mut app = App::new(model);
        app.tab = 1;
        app.scroll[1] = 18;
        app.remember_chat_scroll();
        app.selected[1] = 1;
        app.restore_chat_scroll();
        assert_eq!(app.scroll[1], 0);
        app.scroll[1] = 4;
        app.remember_chat_scroll();
        app.selected[1] = 0;
        app.restore_chat_scroll();
        assert_eq!(app.scroll[1], 18);
        app.input = "unfinished alpha".into();
        app.remember_chat_draft();
        app.input.clear();
        app.selected[1] = 1;
        app.start_chat_composer();
        assert!(app.input.is_empty());
        app.input = "unfinished beta".into();
        app.remember_chat_draft();
        app.selected[1] = 0;
        app.start_chat_composer();
        assert_eq!(app.input, "unfinished alpha");
        assert_eq!(app.chat_draft_cache.len(), 2);
        app.timeline_cache
            .insert("session/alpha".into(), (Vec::new(), true));
        app.model.timeline_truncated = true;
        app.timeline_requested = Some("session/alpha".into());
        app.invalidate_timelines();
        assert!(app.timeline_cache.is_empty());
        assert!(!app.model.timeline_truncated);
        assert_eq!(app.timeline_requested, None);
        assert_eq!(app.chat_draft_cache.len(), 2);
        assert_eq!(app.chat_scroll_cache.get("session/alpha"), Some(&18));
    }
}
