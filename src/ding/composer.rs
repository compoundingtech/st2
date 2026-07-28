//! What a peeked screen proves about one exact notice, and the routing that decides which
//! composer on the screen is the live one.

use super::harness::{self, Screen};

/// What the current bottom composer proves about one exact normalized notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ComposerState {
    /// A maintained harness is positively idle and contains only its known placeholder.
    EmptySafe,
    /// The exact notice is the complete composer and the harness is positively idle.
    ExactSafe,
    /// The exact notice is present, but a modal, active turn, or non-idle footer blocks Return.
    ExactBlocked,
    /// A maintained composer contains different text (including a human draft).
    Changed,
    /// No maintained, unambiguous composer state was proven.
    Ambiguous,
}

/// Enumerate the two logical strings possible at each renderer-shaped soft-wrap row: the TUI either
/// discarded one inter-word space or split a token. Current 80-column Codex/Claude composers wrap
/// long DING rows at 70+ content cells and indent continuations by exactly two cells. Short or
/// unfamiliar multiline input remains literal and cannot equal a normalized single-line DING.
pub(super) fn logical_soft_wrap_candidates(input: &str, minimum_first_content_chars: usize) -> Vec<String> {
    let rows: Vec<&str> = input.lines().collect();
    let Some(first) = rows.first() else {
        return vec![String::new()];
    };
    if rows.len() == 1 {
        return vec![(*first).to_string()];
    }
    let mut candidates = vec![(*first).to_string()];
    let mut previous = *first;
    for (index, row) in rows[1..].iter().enumerate() {
        let required_previous_width = minimum_first_content_chars + usize::from(index > 0) * 2;
        if previous.chars().count() < required_previous_width
            || !row.starts_with("  ")
            || row.trim().is_empty()
        {
            return vec![input.to_string()];
        }
        let continuation = row.strip_prefix("  ").expect("prefix checked").trim_end();
        let mut next = Vec::with_capacity(candidates.len().saturating_mul(2).min(32));
        for candidate in candidates {
            if next.len() >= 32 {
                return vec![input.to_string()];
            }
            next.push(format!("{candidate}{continuation}"));
            next.push(format!("{candidate} {continuation}"));
        }
        candidates = next;
        previous = row;
    }
    candidates
}

pub(super) fn looks_like_choice_menu(plain: &str) -> bool {
    let mut first = false;
    let mut later = false;
    for line in plain.lines().map(str::trim_start) {
        first |= line.starts_with("› 1.") || line.starts_with("> 1.");
        later |= line.starts_with("2.") || line.starts_with("3.");
    }
    first && later
}

/// Strip the CSI/OSC sequences emitted by `pty peek` while preserving rendered text. Bounded
/// cursor-forward sequences represent visible spaces in current Codex and Claude panes.
pub(super) fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != 0x1b {
            let ch = input[index..].chars().next().expect("valid UTF-8 boundary");
            out.push(ch);
            index += ch.len_utf8();
            continue;
        }
        index += 1;
        if index >= bytes.len() {
            break;
        }
        match bytes[index] {
            b'[' => {
                index += 1;
                let params_start = index;
                let mut final_byte = None;
                while index < bytes.len() {
                    let byte = bytes[index];
                    index += 1;
                    if (0x40..=0x7e).contains(&byte) {
                        final_byte = Some(byte);
                        break;
                    }
                }
                if final_byte == Some(b'C') {
                    let params = &bytes[params_start..index.saturating_sub(1)];
                    let width = if params.is_empty() {
                        Some(1)
                    } else if params.iter().all(u8::is_ascii_digit) {
                        std::str::from_utf8(params)
                            .ok()
                            .and_then(|value| value.parse::<usize>().ok())
                            .map(|value| value.max(1))
                    } else {
                        None
                    };
                    if let Some(width) = width.filter(|width| *width <= 512) {
                        for _ in 0..width {
                            out.push(' ');
                        }
                    }
                }
            }
            b']' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == 0x07 {
                        index += 1;
                        break;
                    }
                    if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    out
}

/// Locate every maintained composer and classify the LOWEST one on screen.
///
/// A pane has exactly one live composer and it sits at the bottom of the viewport, so anything
/// composer-shaped above it is scrollback — typically a pasted or logged screen from the other
/// harness. Preferring one harness unconditionally lets that transcript decide, which is a wrong
/// *positive*: the paste and the Return go to the pane's real composer whichever text was read, so
/// it can type into, or submit, a human's live draft. Scrollback is above the live composer by
/// construction, so picking the lowest needs no per-pair special case.
pub(super) fn classify_composer(screen: &str, expected: &str) -> ComposerState {
    let plain = strip_ansi(screen);
    let screen = Screen { raw: screen, plain: &plain };
    harness::all()
        .into_iter()
        .filter_map(|harness| harness.locate(&screen).map(|located| (located.row, harness)))
        .max_by_key(|(row, _)| *row)
        .map(|(_, harness)| harness.classify(&screen, expected))
        // No maintained composer is locatable, so nothing is proven either way.
        .unwrap_or(ComposerState::Ambiguous)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ding::fixtures::*;

    #[test]
    fn maintained_composer_classifiers_require_exact_idle_state() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";
        assert_eq!(
            classify_composer(&idle_codex_screen(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&staged_codex_screen(expected), expected),
            ComposerState::ExactSafe
        );
        assert_eq!(
            classify_composer(&human_codex_screen(), expected),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(
                &format!("Create a plan?\r\n{}", staged_codex_screen(expected)),
                expected
            ),
            ComposerState::ExactBlocked
        );

        assert_eq!(
            classify_composer(&idle_claude_screen(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&idle_claude_screen_without_hint(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&staged_claude_screen(expected), expected),
            ComposerState::ExactSafe
        );
        assert_eq!(
            classify_composer(&staged_claude_screen("a changed human composer"), expected),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(
                &format!("Esc to interrupt\r\n{}", staged_claude_screen(expected)),
                expected
            ),
            ComposerState::ExactBlocked
        );
        // A used pane must classify exactly like a fresh one: neither the scrolled-away banner, the
        // empty composer, nor the missing cycle hint is evidence that Return is unsafe.
        assert_eq!(
            classify_composer(&mature_idle_claude_screen(), expected),
            ComposerState::EmptySafe
        );
        assert_eq!(
            classify_composer(&mature_idle_claude_screen_with_hint(), expected),
            ComposerState::EmptySafe
        );
        // Only the bypass footer carries `permissions on`, so an otherwise identical accept-edits
        // pane is never positively idle. It stays unsubmitted rather than being proven safe.
        assert_eq!(
            classify_composer(&mature_idle_accept_edits_claude_screen(), expected),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(&mature_staged_claude_screen(expected), expected),
            ComposerState::ExactSafe
        );
        // The same pane shape must still fail closed on a human draft and on an active turn.
        assert_eq!(
            classify_composer(
                &mature_staged_claude_screen("a changed human composer"),
                expected
            ),
            ComposerState::Changed
        );
        assert_eq!(
            classify_composer(
                &format!(
                    "Esc to interrupt\r\n{}",
                    mature_staged_claude_screen(expected)
                ),
                expected
            ),
            ComposerState::ExactBlocked
        );

        assert_eq!(
            classify_composer("unknown terminal pixels", expected),
            ComposerState::Ambiguous
        );
    }

    /// Scrollback that merely looks like a composer must never outrank the live one. The paste and
    /// the Return always go to the pane's real bottom composer, so misreading transcript text as
    /// "idle" or "already staged" is a wrong positive: it can type into, or submit, a human draft.
    #[test]
    fn transcript_composers_never_outrank_the_live_bottom_composer() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";

        // The live Codex composer holds a human draft in both cases, so both must stay `Changed`.
        // An empty transcript row would otherwise read as positively-empty and allow the paste.
        assert_eq!(
            classify_composer(
                &codex_screen_below_claude_transcript("", &human_codex_screen()),
                expected
            ),
            ComposerState::Changed
        );
        // A transcript row holding the exact notice is the worse case: it would otherwise satisfy
        // the two adjacent exact observations and send a bare Return to the draft.
        assert_eq!(
            classify_composer(
                &codex_screen_below_claude_transcript(expected, &human_codex_screen()),
                expected
            ),
            ComposerState::Changed
        );

        // The rule is positional, not a Codex preference: a genuine Claude pane whose scrollback
        // shows a captured Codex composer still classifies from its own live Claude composer.
        assert_eq!(
            classify_composer(
                &format!("{}\r\n{}", staged_codex_screen("a stale pasted codex draft"), mature_idle_claude_screen()),
                expected
            ),
            ComposerState::EmptySafe
        );
    }

    /// The two locators do not natively work in the same units. Codex is matched with `rfind` over
    /// the raw screen, so it reports a **byte offset** inflated by every escape sequence above it;
    /// Claude is matched over stripped lines, so it reports a **row**. Comparing those directly
    /// picks Codex almost always, since an offset dwarfs a row — including when the live composer
    /// is Claude's and the Codex match is stale scrollback. Both must be normalized to a row.
    ///
    /// On the screen below, measured: the Codex composer sits at byte offset 560 but row 10, while
    /// the live Claude composer is row 20. Comparing row against offset picks Codex, so this would
    /// classify from a pasted draft instead of the real composer.
    #[test]
    fn composer_positions_are_compared_as_rows_not_raw_byte_offsets() {
        let expected =
            "[DING] new st2 message: [id:abc123] exact observation (from cos); check your inbox";
        assert_eq!(
            classify_composer(&live_claude_below_escape_heavy_codex_transcript(), expected),
            ComposerState::EmptySafe
        );
    }

    /// Normalizing the Codex offset means counting newlines in the *stripped* prefix, which is only
    /// faithful if stripping preserves them. It does for well-formed input. It does not for an
    /// unterminated sequence: the CSI scanner runs until a byte in `0x40..=0x7e` and `\n` is `0x0a`,
    /// so it eats newlines, and an unterminated OSC consumes to the end of input. Both are recorded
    /// here so a future change to `strip_ansi` cannot silently shift every row.
    #[test]
    fn stripping_preserves_newlines_for_well_formed_sequences_only() {
        let nl = |text: &str| strip_ansi(text).matches('\n').count();

        assert_eq!(
            nl("\x1b[1;32mone\x1b[0m\r\n\x1b[2Ctwo\x1b[0m\r\n\x1b[1mthree\x1b[0m\r\n"),
            3
        );
        // Unterminated CSI: the newline is consumed while hunting for a final byte.
        assert_eq!(nl("before\r\n\x1b[999999\r\nafter\r\n"), 2);
        // Unterminated OSC: everything to the end of input is consumed.
        assert_eq!(nl("before\r\n\x1b]0;no terminator\r\nafter\r\n"), 1);
    }
}
