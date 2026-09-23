use std::io::{self, Stdout};

use anyhow::{Context, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Layout},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph, Tabs},
};

const TABS: [&str; 4] = ["Now", "Chat", "Control", "Fleet"];

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enable terminal raw mode")?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error).context("enter alternate screen");
        }
        match Terminal::new(CrosstermBackend::new(io::stdout())) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(error) => {
                let _ = execute!(io::stdout(), LeaveAlternateScreen);
                let _ = disable_raw_mode();
                Err(error).context("initialize terminal")
            }
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
        let _ = self.terminal.show_cursor();
    }
}

fn render(frame: &mut ratatui::Frame<'_>, selected: usize) {
    let [header, tabs, body, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(2),
    ])
    .areas(frame.area());

    frame.render_widget(
        Paragraph::new("Smalltalk  ·  starter shell  ·  no live data")
            .block(Block::default().borders(Borders::ALL)),
        header,
    );
    frame.render_widget(
        Tabs::new(TABS.map(Line::from))
            .select(selected)
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .divider("  ·  ")
            .block(Block::default().borders(Borders::BOTTOM)),
        tabs,
    );
    frame.render_widget(
        Paragraph::new(format!(
            "Hello, Smalltalk.\n\n{} is ready for the product build.",
            TABS[selected]
        ))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .title(format!(" {} ", TABS[selected]))
                .borders(Borders::ALL),
        ),
        body,
    );
    frame.render_widget(
        Paragraph::new("1–4 switch screens  ·  q quits  ·  No connection or actions yet"),
        footer,
    );
}

fn main() -> Result<()> {
    if !io::IsTerminal::is_terminal(&io::stdout()) {
        anyhow::bail!("stui needs an interactive terminal");
    }
    let mut guard = TerminalGuard::enter()?;
    let mut selected = 0;
    loop {
        guard.terminal.draw(|frame| render(frame, selected))?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char(digit @ '1'..='4') => selected = digit as usize - '1' as usize,
                _ => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    #[test]
    fn every_primary_screen_renders_in_a_terminal() {
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        for selected in 0..4 {
            terminal
                .draw(|frame| super::render(frame, selected))
                .unwrap();
            let rendered = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>();
            assert!(rendered.contains(super::TABS[selected]));
            assert!(rendered.contains("Hello, Smalltalk"));
        }
    }
}
