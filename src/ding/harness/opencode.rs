//! The OpenCode full-screen composer: bordered input rows immediately above its mode footer.

use super::{Harness, Located, ReceiptState, Screen};
use crate::ding::composer::ComposerState;

pub(super) struct OpenCode;

impl Harness for OpenCode {
    fn locate(&self, screen: &Screen<'_>) -> Option<Located> {
        locate_composer(screen.plain).map(|composer| Located {
            row: composer.start,
        })
    }

    fn classify(&self, screen: &Screen<'_>, expected: &str) -> ComposerState {
        let Some(composer) = locate_composer(screen.plain) else {
            return ComposerState::Ambiguous;
        };
        let exact = equivalent(&composer.input, expected);
        if exact {
            if composer.idle {
                ComposerState::ExactSafe
            } else {
                ComposerState::ExactBlocked
            }
        } else if composer.input.is_empty() && composer.idle {
            ComposerState::EmptySafe
        } else if composer.input.is_empty() {
            ComposerState::Ambiguous
        } else {
            ComposerState::Changed
        }
    }

    fn receipt(&self, screen: &Screen<'_>, expected: &str) -> ReceiptState {
        let Some(composer) = locate_composer(screen.plain) else {
            return ReceiptState::Unproven;
        };
        if equivalent(&composer.input, expected) {
            return if composer.idle {
                ReceiptState::RetainedSafe
            } else {
                ReceiptState::RetainedBlocked
            };
        }
        if !composer.input.is_empty() {
            return ReceiptState::NotRetained;
        }
        if composer.idle && preceding_submitted_box(&screen.plain[..composer.start_byte], expected)
        {
            ReceiptState::Accepted
        } else if composer.idle {
            ReceiptState::NotRetained
        } else {
            ReceiptState::Unproven
        }
    }
}

struct Composer {
    start: usize,
    start_byte: usize,
    input: String,
    idle: bool,
}

fn locate_composer(plain: &str) -> Option<Composer> {
    let lines = plain.split_inclusive('\n').collect::<Vec<_>>();
    let closure = lines.iter().rposition(|line| {
        let line = line.trim();
        line.strip_prefix('╹').is_some_and(|rule| {
            rule.chars().filter(|ch| *ch == '▀').count() >= 40
                && rule.chars().all(|ch| ch == '▀' || ch.is_whitespace())
        })
    })?;
    let footer = (0..closure).rev().find(|index| {
        let line = lines[*index].trim();
        line.starts_with('┃') && line.contains(" · ") && line.contains("OpenCode")
    })?;
    let start = (0..=footer)
        .rev()
        .take_while(|index| lines[*index].trim_start().starts_with('┃'))
        .last()?;
    let input = lines[start..footer]
        .iter()
        .map(|line| line.trim_start().strip_prefix('┃').unwrap_or(line).trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let start_byte = lines[..start].iter().map(|line| line.len()).sum();
    let tail = lines[footer..].concat();
    Some(Composer {
        start,
        start_byte,
        input,
        idle: !tail.contains("esc interrupt")
            && !tail.contains("esc to interrupt")
            && !tail.contains("esc again to interrupt"),
    })
}

fn equivalent(observed: &str, expected: &str) -> bool {
    let compact = |value: &str| {
        value
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>()
    };
    compact(observed).contains(&compact(expected))
}

fn preceding_submitted_box(before_composer: &str, expected: &str) -> bool {
    let mut groups = Vec::<String>::new();
    let mut current = Vec::<&str>::new();
    for line in before_composer.lines() {
        if let Some(content) = line.trim_start().strip_prefix('┃') {
            current.push(content.trim());
        } else if !current.is_empty() {
            groups.push(current.join(" "));
            current.clear();
        }
    }
    if !current.is_empty() {
        groups.push(current.join(" "));
    }
    groups
        .last()
        .is_some_and(|group| equivalent(group, expected))
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED: &str = "[DING] id=abc from=dev3.cos subject=liveness";

    fn screen(composer: &str, history: &str, status: &str) -> String {
        format!(
            "{history}\n  ┃ {composer}\n  ┃\n  ┃\n  ┃ Build auto · MiMo V2.5 Free OpenCode Zen\n  ╹{}\n   /work  39K {status}\n",
            "▀".repeat(60)
        )
    }

    #[test]
    fn recognizes_idle_empty_and_exact_composers() {
        let empty = screen("", "answer", "ctrl+p commands");
        let exact = screen(EXPECTED, "answer", "ctrl+p commands");
        assert_eq!(
            OpenCode.classify(
                &Screen {
                    raw: &empty,
                    plain: &empty
                },
                EXPECTED
            ),
            ComposerState::EmptySafe
        );
        assert_eq!(
            OpenCode.classify(
                &Screen {
                    raw: &exact,
                    plain: &exact
                },
                EXPECTED
            ),
            ComposerState::ExactSafe
        );
    }

    #[test]
    fn active_turn_blocks_submission() {
        let active = screen(EXPECTED, "answer", "esc interrupt");
        assert_eq!(
            OpenCode.classify(
                &Screen {
                    raw: &active,
                    plain: &active
                },
                EXPECTED
            ),
            ComposerState::ExactBlocked
        );
    }

    #[test]
    fn accepted_receipt_requires_history_and_an_empty_idle_composer() {
        let accepted = screen("", &format!("  ┃ {EXPECTED}\n\n"), "ctrl+p commands");
        assert_eq!(
            OpenCode.receipt(
                &Screen {
                    raw: &accepted,
                    plain: &accepted
                },
                EXPECTED
            ),
            ReceiptState::Accepted
        );
    }

    #[test]
    fn ordinary_transcript_text_is_not_an_acceptance_receipt() {
        let transcript = screen(
            "",
            &format!("assistant repeated {EXPECTED}\n\n"),
            "ctrl+p commands",
        );
        assert_eq!(
            OpenCode.receipt(
                &Screen {
                    raw: &transcript,
                    plain: &transcript
                },
                EXPECTED
            ),
            ReceiptState::NotRetained
        );
    }
}
