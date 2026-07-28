//! Rendered-screen fixtures shared by the DING tests.
//!
//! Hand-written synthetic screens, never captured pane text. They live here rather than beside one
//! harness because the router tests, the transport tests, and both harness adapters assert against
//! the same shapes; a builder duplicated per consumer would let two copies drift and quietly
//! weaken whichever test kept the stale one.

/// Status lines for an ACTIVE turn — Return must never be sent.
pub(crate) const ACTIVE_TURN_STATUS: [&str; 4] = [
    "✻ Frolicking… (3m 35s · ↓ 6.9k tokens)",
    "✽ Schlepping…",
    "· Metamorphosing…",
    "✶ Schlepping… (9s · ↓ 296 tokens · thinking with high effort)",
];

/// Status lines for a FINISHED turn — these sit above every genuinely idle composer, so
/// treating them as blocked would stop delivery entirely.
pub(crate) const FINISHED_TURN_STATUS: [&str; 4] = [
    "✻ Brewed for 5s",
    "✻ Crunched for 7s",
    "✻ Cogitated for 11s · 1 shell still running",
    "✻ Baked for 3s · 1 shell still running",
];

pub(crate) fn claude_rule() -> String {
    "─".repeat(80)
}

pub(crate) fn idle_claude_screen() -> String {
    let rule = claude_rule();
    format!(
        "Claude Code v2.1.220\r\n{rule}\r\n❯\u{00a0}Try \"write a test for validate.rs\"\r\n\
         {rule}\r\n  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents"
    )
}

pub(crate) fn idle_claude_screen_without_hint() -> String {
    idle_claude_screen().replace(" (shift+tab to cycle)", "")
}

pub(crate) fn staged_claude_screen(text: &str) -> String {
    assert!(text.is_ascii());
    let rule = claude_rule();
    let composer = if text.len() <= 77 {
        format!("❯\u{00a0}{text}")
    } else {
        let (first, continuation) = text.split_at(77);
        format!("❯\u{00a0}{first}\r\n  {continuation}")
    };
    format!(
        "Claude Code v2.1.220\r\n{rule}\r\n{composer}\r\n{rule}\r\n\
         ⏵⏵ bypass permissions on (shift+tab to cycle)"
    )
}

/// A pane that has been used: the startup banner has scrolled out of the peeked viewport, the
/// composer is empty rather than showing a rotating placeholder, and the footer omits the
/// conditional `(shift+tab to cycle)` hint while keeping the permission-mode indicator.
pub(crate) fn mature_claude_screen(composer: &str) -> String {
    let rule = claude_rule();
    let footer = "  ⏵⏵ bypass permissions on · PR #42 · 2 shells · ← 1 agent";
    format!("  an earlier turn\r\n{rule}\r\n{composer}\r\n{rule}\r\n{footer}")
}

pub(crate) fn mature_idle_claude_screen() -> String {
    mature_claude_screen("❯\u{00a0}")
}

pub(crate) fn mature_idle_claude_screen_with_hint() -> String {
    mature_idle_claude_screen().replace(
        "⏵⏵ bypass permissions on",
        "⏵⏵ bypass permissions on (shift+tab to cycle)",
    )
}

/// The same used pane in accept-edits mode. `permissions on` is specific to the bypass footer,
/// so no accept-edits or auto pane is positively idle to this classifier.
pub(crate) fn mature_idle_accept_edits_claude_screen() -> String {
    mature_idle_claude_screen().replace("⏵⏵ bypass permissions on", "⏵⏵ accept edits on")
}

pub(crate) fn mature_staged_claude_screen(text: &str) -> String {
    mature_claude_screen(&format!("❯\u{00a0}{text}"))
}

/// The same used pane with an in-flight turn: a spinner status line above the composer. Every
/// frame below was observed on a real 2.1.220 pane; the glyph animates and the elapsed timer is
/// not always rendered, so both variations appear here.
pub(crate) fn mid_turn_claude_screen(status: &str, composer: &str) -> String {
    let rule = claude_rule();
    let footer = "  ⏵⏵ bypass permissions on · PR #42 · 2 shells · ← 1 agent";
    format!("  an earlier turn\r\n{status}\r\n{rule}\r\n{composer}\r\n{rule}\r\n{footer}")
}

pub(crate) fn idle_codex_screen() -> String {
    "\x1b[1m›\x1b[1C\x1b[22;2mFind and fix a bug in @filename\r\n\r\n\
     \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
        .to_string()
}

pub(crate) fn staged_codex_screen(text: &str) -> String {
    let rendered = text.replace(' ', "\x1b[1C");
    format!(
        "\x1b[1m›\x1b[1C\x1b[0m{rendered}\r\n\r\n\
         \x1b[2C\x1b[0mgpt-5.6-sol xhigh · /workspace"
    )
}

pub(crate) fn human_codex_screen() -> String {
    staged_codex_screen("please keep my half-written draft")
}

/// A Codex pane whose scrollback holds a captured Claude screen — two ruled lines around a `❯`
/// row plus a Claude idle footer — above the live, ANSI-detected Codex composer. Capturing and
/// pasting pane text is routine, so this shape is not exotic.
pub(crate) fn codex_screen_below_claude_transcript(transcript_row: &str, codex: &str) -> String {
    let rule = claude_rule();
    format!(
        "  scrollback: a pasted Claude pane\r\n{rule}\r\n❯\u{00a0}{transcript_row}\r\n{rule}\r\n\
         \u{0020} ⏵⏵ bypass permissions on (shift+tab to cycle)\r\n\r\n{codex}"
    )
}

/// A live Claude composer with a stale Codex composer above it in scrollback, preceded by
/// escape-heavy output. The escapes inflate the Codex byte offset far past the Claude
/// composer's row, which is what makes the two locators' units observably disagree.
pub(crate) fn live_claude_below_escape_heavy_codex_transcript() -> String {
    let padding = "\x1b[1;32m\x1b[38;5;204mpadding with lots of escapes\x1b[0m\x1b[0m\r\n".repeat(10);
    let codex = staged_codex_screen("a stale pasted codex draft");
    let filler = "\x1b[1;32mmore padding\x1b[0m\r\n".repeat(6);
    format!("{padding}{codex}\r\n{filler}{}", mature_idle_claude_screen())
}
