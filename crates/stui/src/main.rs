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
    widgets::{Block, Borders, Paragraph, Tabs, Wrap},
};
use st3_client::{
    Client, Fence, LaunchCreateParameters, LaunchTarget, MessageSendParameters, TargetParameters,
    TerminalInputMode, TerminalInputParameters, TerminalScreen,
};
use std::{
    io::{self, IsTerminal, Stdout},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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
    runtime_id: String,
    terminal_id: String,
    attachment_id: String,
    screen: TerminalScreen,
}
struct App {
    model: Model,
    tab: usize,
    selected: [usize; 4],
    scroll: [u16; 4],
    sidebar: bool,
    mode: Mode,
    input: String,
    launch: [String; 4],
    attached: Option<Attached>,
    return_focused: bool,
    dirty: bool,
    last_sync: Instant,
    last_external_scan: Instant,
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
            mode: Mode::Normal,
            input: String::new(),
            launch: Default::default(),
            attached: None,
            return_focused: false,
            dirty: true,
            last_sync: Instant::now(),
            last_external_scan: Instant::now(),
            last_terminal: Instant::now(),
        }
    }
    fn peer(&self) -> Option<&st3_client::Agent> {
        self.model.agents().nth(self.selected[1])
    }
    fn undeclared_session(&self) -> Option<&st3_client::Session> {
        let index = self.selected[1].checked_sub(self.model.agents().count())?;
        self.model.undeclared_sessions().nth(index)
    }
    fn selected_session_id(&self) -> Option<String> {
        self.peer()
            .and_then(|peer| peer.current_session_id.clone())
            .or_else(|| {
                self.undeclared_session()
                    .map(|session| session.header.id.clone())
            })
    }
    fn runtime(&self) -> Option<&st3_client::Runtime> {
        self.model.runtimes().nth(self.selected[2])
    }
    fn count(&self) -> usize {
        match self.tab {
            0 => self.model.attention().count(),
            1 => self.model.agents().count() + self.model.undeclared_sessions().count(),
            2 => self.model.runtimes().count(),
            _ => self.model.machines().count(),
        }
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
                    .model
                    .agents()
                    .map(|v| format!("{} {}", v.name, v.state))
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
                    .model
                    .runtimes()
                    .map(|v| format!("{} {}", v.owner_id, v.state))
                    .collect(),
                _ => self
                    .model
                    .machines()
                    .map(|v| format!("{} {}", v.name, v.state))
                    .collect(),
            };
            let lines = list
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    format!(
                        "{} {v}",
                        if i == self.selected[self.tab] {
                            '›'
                        } else {
                            ' '
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            frame.render_widget(
                Paragraph::new(if lines.is_empty() { "No items" } else { &lines })
                    .block(Block::default().title("Browse").borders(Borders::ALL)),
                columns[0],
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
                    lines.push(format!("{} · {}", peer.name, peer.state));
                    lines.push(format!(
                        "Current session: {}",
                        peer.current_session_id.as_deref().unwrap_or("none")
                    ));
                    lines.push("── Conversation ──".into());
                    for message in self
                        .model
                        .messages(peer.current_session_id.as_deref(), &peer.header.id)
                    {
                        lines.push(format!(
                            "{}: {}",
                            message.from,
                            message.content.replace('\n', " ⏎ ")
                        ));
                    }
                    lines.push("── Session history ──".into());
                    lines.extend(self.model.timeline.iter().map(timeline_line));
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
                lines.push("Mission progress".into());
                for v in self.model.missions() {
                    lines.push(format!("{} · {}", v.title, v.state));
                }
                for v in self.model.work() {
                    lines.push(format!(
                        "  {} · {} · attempt {}",
                        v.path, v.state, v.attempt
                    ));
                }
                lines.push("── Terminals and launches ──".into());
                if let Some(v) = self.runtime() {
                    lines.push(format!(
                        "{} · {} · terminal {}",
                        v.owner_id,
                        v.state,
                        v.terminal_id.as_deref().unwrap_or("none")
                    ));
                }
                for v in self.model.launches() {
                    lines.push(format!("{} · {}", v.title, v.phase));
                }
                lines.push("Enter attaches to selected terminal; c creates a launch.".into());
                if self.model.work.truncated {
                    lines.push("[More progress beyond bounded view]".into());
                }
            }
            _ => {
                lines.push("Machines".into());
                for v in self.model.machines() {
                    lines.push(format!(
                        "{} · {} · capacity {} · {} runtimes",
                        v.name, v.state, v.capacity.state, v.occupancy.running_runtimes
                    ));
                    for t in &v.transports {
                        lines.push(format!("  {} {}", t.protocol, t.status));
                    }
                }
                lines.push("── Paired devices ──".into());
                for v in self.model.devices() {
                    lines.push(format!("{} · {} · {}", v.header.id, v.person_id, v.state));
                }
                let gateway = self
                    .model
                    .sessions
                    .snapshot
                    .as_ref()
                    .map(|s| s.host_id.as_str())
                    .unwrap_or("connected machine");
                lines.push(format!("── Undeclared sessions on {gateway} ──"));
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
                        "{driver} · {} · {}",
                        if exact {
                            "native session"
                        } else {
                            "unresolved process"
                        },
                        session.header.id
                    ));
                }
                lines.push("Discovery is local to this connected machine.".into());
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
                "{} · 1–4 views · ↑↓ select · s sidebar · Enter open · c compose/create · q quit",
                self.model.status
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
        runtime_id,
        terminal_id,
        attachment_id: attachment.attachment_id,
        screen,
    });
    app.dirty = true;
    Ok(())
}
async fn detach(app: &mut App, client: &Client) -> Result<()> {
    let Some(attached) = app.attached.take() else {
        return Ok(());
    };
    let fence = app
        .model
        .runtimes
        .fence(&attached.runtime_id)
        .context("runtime fence unavailable")?;
    let (id, key) = action_pair();
    client
        .terminal_detach(
            id,
            key,
            fence,
            TargetParameters {
                target_id: attached.attachment_id,
                ..Default::default()
            },
        )
        .await?;
    app.return_focused = false;
    app.dirty = true;
    Ok(())
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
            let fence = app
                .model
                .runtimes
                .fence(&attached.runtime_id)
                .context("runtime fence unavailable")?;
            let (id, idem) = action_pair();
            client
                .terminal_input(
                    id,
                    idem,
                    fence,
                    TerminalInputParameters {
                        terminal_id: attached.terminal_id.clone(),
                        mode: TerminalInputMode::Key,
                        value,
                    },
                )
                .await?;
        }
        return Ok(false);
    }
    if app.mode != Mode::Normal {
        match key.code {
            KeyCode::Esc => {
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
                            client
                                .message_send(
                                    id,
                                    idem,
                                    fence,
                                    MessageSendParameters {
                                        to: peer.header.id.clone(),
                                        content: value,
                                        title: None,
                                        in_reply_to: None,
                                        session_id: peer.current_session_id.clone(),
                                        tags: vec![],
                                    },
                                )
                                .await?;
                            app.model.reload(client).await?;
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
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if app.input.len() < 4096 {
                    app.input.push(c);
                }
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
        KeyCode::Up => {
            app.selected[app.tab] = app.selected[app.tab].saturating_sub(1);
            app.scroll[app.tab] = 0;
        }
        KeyCode::Down => {
            app.selected[app.tab] = (app.selected[app.tab] + 1).min(app.count().saturating_sub(1));
            app.scroll[app.tab] = 0;
        }
        KeyCode::PageUp => app.scroll[app.tab] = app.scroll[app.tab].saturating_sub(10),
        KeyCode::PageDown => app.scroll[app.tab] = app.scroll[app.tab].saturating_add(10),
        KeyCode::Char('c') if app.tab == 1 => {
            if app.peer().is_some() {
                app.mode = Mode::Chat;
                app.input.clear();
            } else {
                app.model.status = "Undeclared sessions are read-only".into();
            }
        }
        KeyCode::Char('c') if app.tab == 2 => {
            app.mode = Mode::Title;
            app.input.clear();
        }
        KeyCode::Enter if app.tab == 2 => attach(app, client).await?,
        KeyCode::Enter if app.tab == 1 => {
            if let Some(id) = app.selected_session_id() {
                app.model.load_timeline(client, &id).await?;
            }
        }
        _ => {}
    }
    if app.tab == 1 {
        if let Some(id) = app.selected_session_id() {
            app.model.load_timeline(client, &id).await?;
        } else {
            app.model.timeline.clear();
        }
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
    let client = match std::env::var("ST3_PERSON") {
        Ok(person) => Client::unix_as(&path, person),
        Err(_) => Client::unix(&path),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut app = App::new(runtime.block_on(Model::load(&client))?);
    let mut guard = TerminalGuard::enter()?;
    #[cfg(debug_assertions)]
    if std::env::var_os("STUI_TEST_PANIC_AFTER_ENTER").is_some() {
        panic!("terminal restoration probe");
    }
    while !stopping.load(Ordering::Relaxed) {
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
        if app.last_sync.elapsed() >= Duration::from_secs(2) {
            match runtime.block_on(app.model.sync(&client)) {
                Ok(changed) => {
                    if changed && app.tab == 1 {
                        if let Some(id) = app.selected_session_id() {
                            let _ = runtime.block_on(app.model.load_timeline(&client, &id));
                        }
                    }
                    app.dirty |= changed;
                }
                Err(error) => {
                    let status = format!("Sync: {error}");
                    if app.model.status != status {
                        app.model.status = status;
                        app.dirty = true;
                    }
                }
            }
            app.last_sync = Instant::now();
        }
        if app.last_external_scan.elapsed() >= Duration::from_secs(15) {
            match runtime.block_on(app.model.refresh_sessions(&client)) {
                Ok(changed) => {
                    let count =
                        app.model.agents().count() + app.model.undeclared_sessions().count();
                    app.selected[1] = app.selected[1].min(count.saturating_sub(1));
                    app.dirty |= changed;
                }
                Err(error) => {
                    let status = format!("Session discovery: {error}");
                    if app.model.status != status {
                        app.model.status = status;
                        app.dirty = true;
                    }
                }
            }
            app.last_external_scan = Instant::now();
        }
        if app.attached.is_some() && app.last_terminal.elapsed() >= Duration::from_millis(400) {
            let attached = app.attached.as_mut().unwrap();
            if let Ok(screen) = runtime.block_on(client.terminal_screen(&attached.terminal_id)) {
                if attached.screen != screen.value {
                    attached.screen = screen.value;
                    app.dirty = true;
                }
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
            for tab in 0..4 {
                app.tab = tab;
                terminal.draw(|frame| app.render(frame)).unwrap();
                let content = terminal
                    .backend()
                    .buffer()
                    .content()
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(content.contains(TABS[tab]));
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
            assert!(content.contains("Undeclared") || content.contains("undeclared"));
        }
    }
}
