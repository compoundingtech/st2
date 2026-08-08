//! A self-contained reproduction of the staged-unsent Claude shape, built from a synthesized pane
//! so it carries no real message content, host, or agent identity.
//!
//! The notice is rendered the way Claude Code renders it — greedy **word** wrap with a two-cell
//! continuation indent — and then classified by the production path. The shared candidate builder
//! only accepts a continuation whose preceding row is at least `70`/`72` cells wide, which models a
//! *character-level* wrap. A word wrap breaks before the limit whenever the next word does not fit,
//! so the preceding row is short and the shape is refused: `Unsupported` → `Unproven` → the sidecar
//! retains staged ownership and never submits.

#[cfg(test)]
mod repro {
    use crate::ding::composer::{classify_composer, classify_receipt};
    use crate::ding::harness::ReceiptState;

    /// Content cells available to the composer inside an 80-column pane, measured from captured
    /// panes: the widest content row observed at a rule width of 80 is 76.
    const CONTENT_WIDTH: usize = 76;

    /// Greedy word wrap: emit the current row as soon as the next word would not fit. This is what
    /// leaves a short row before a long unbreakable token, and it is the only renderer behaviour the
    /// reproduction depends on.
    fn word_wrap(text: &str, width: usize) -> Vec<String> {
        let mut rows: Vec<String> = Vec::new();
        let mut row = String::new();
        for word in text.split(' ') {
            let projected = if row.is_empty() {
                word.chars().count()
            } else {
                row.chars().count() + 1 + word.chars().count()
            };
            if !row.is_empty() && projected > width {
                rows.push(std::mem::take(&mut row));
            }
            if !row.is_empty() {
                row.push(' ');
            }
            row.push_str(word);
        }
        rows.push(row);
        rows
    }

    /// A Claude pane holding `notice` in its composer: two full-width rules around a `❯ ` first row
    /// and two-space continuations, with the idle permission footer below.
    fn claude_pane(notice: &str) -> String {
        let rows = word_wrap(notice, CONTENT_WIDTH);
        let rule = "─".repeat(80);
        let mut pane = String::new();
        pane.push_str("✻ Brewed for 5s\n");
        pane.push_str(&rule);
        pane.push('\n');
        for (index, row) in rows.iter().enumerate() {
            if index == 0 {
                pane.push_str("❯\u{00a0}");
            } else {
                pane.push_str("  ");
            }
            pane.push_str(row);
            pane.push('\n');
        }
        pane.push_str(&rule);
        pane.push('\n');
        pane.push_str("  agent[work] | Model | ◐ 27%\n");
        pane.push_str("  ⏵⏵ bypass permissions on (shift+tab to cycle)\n");
        pane
    }

    /// A notice whose `(from <sender>)` suffix carries a long dotted identity, which is what
    /// `poke_text` appends to every notice. The token cannot be split across rows, so the renderer
    /// breaks early and leaves a short row before it.
    fn notice_with_dotted_sender() -> String {
        "[DING] new st2 message: [id:a1b2c3] Reviewed the queue drain and the retry budget looks \
         wrong under backpressure, details inside (from svc.example.pipeline.orchestration); \
         check your inbox"
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn word_wrapped_notice_renders_a_short_row_before_the_sender_token() {
        let notice = notice_with_dotted_sender();
        let rows = word_wrap(&notice, CONTENT_WIDTH);
        assert!(rows.len() > 2, "notice must wrap to be a repro: {rows:?}");
        // The row before the dotted sender token is well under the builder's 70/72 threshold.
        let short = rows
            .iter()
            .take(rows.len() - 1)
            .any(|row| row.chars().count() < 70);
        assert!(short, "expected a short row before the token: {rows:?}");
    }

    /// The bug. Flip these two assertions to `RetainedSafe` / `ExactSafe` when the guard is fixed.
    #[test]
    fn staged_notice_is_never_submittable() {
        let notice = notice_with_dotted_sender();
        let pane = claude_pane(&notice);
        assert_eq!(
            classify_receipt(&pane, &notice),
            ReceiptState::Unproven,
            "a word-wrapped notice is refused by the character-wrap guard, so the sidecar retains \
             staged ownership forever"
        );
        assert!(matches!(
            classify_composer(&pane, &notice),
            crate::ding::composer::ComposerState::Ambiguous
        ));
    }

    /// The control that must keep passing: the same notice in a pane wide enough that it never
    /// wraps takes the single-row early return and is submitted.
    #[test]
    fn unwrapped_notice_is_submittable() {
        let notice = notice_with_dotted_sender();
        let rows = word_wrap(&notice, 400);
        assert_eq!(rows.len(), 1, "control must not wrap");
        let rule = "─".repeat(400);
        let pane = format!(
            "✻ Brewed for 5s\n{rule}\n❯\u{00a0}{notice}\n{rule}\n  agent[work] | Model | ◐ 27%\n  \
             ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
        );
        assert_eq!(classify_receipt(&pane, &notice), ReceiptState::RetainedSafe);
    }
}
