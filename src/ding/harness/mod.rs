//! Per-harness composer adapters behind one positional rule.
//!
//! A pane has exactly one live composer and it is the lowest one on screen; anything
//! composer-shaped above it is scrollback, typically a captured screen from the other harness.
//! Asking every harness where its composer is and then classifying the lowest makes that a single
//! comparison rather than a special case per harness pair, and it means adding a harness cannot
//! reintroduce a preference ordering.
//!
//! Active-turn and modal detection is per-harness on purpose: the shapes are harness-specific TUI
//! chrome, not a shared contract. Only genuinely cross-harness shapes stay in `composer`.

pub(super) mod claude;
pub(super) mod codex;

use super::composer::ComposerState;

/// One peeked screen in both forms a harness may need: the raw bytes, in which the Codex composer
/// markers are written as ANSI sequences, and the stripped text, in which the Claude rules are
/// found. Both describe the same screen, so a row index is comparable across harnesses.
pub(super) struct Screen<'a> {
    pub(super) raw: &'a str,
    pub(super) plain: &'a str,
}

/// Where a harness found its live composer.
pub(super) struct Located {
    /// The composer's first row in the stripped screen. The router classifies the lowest.
    pub(super) row: usize,
}

pub(super) trait Harness {
    /// Locate this harness's composer, if this screen has one.
    fn locate(&self, screen: &Screen<'_>) -> Option<Located>;

    /// Classify the composer `locate` found. Only called when `locate` returned `Some`, so each
    /// implementation re-derives it rather than the registry threading a harness-specific payload
    /// through; a screen is one viewport, so the extra scan is a few string searches.
    fn classify(&self, screen: &Screen<'_>, expected: &str) -> ComposerState;
}

/// Every registered harness. Claude is last so that an exact row tie resolves to Claude, which is
/// how the positional comparison behaved before the harnesses were split apart.
pub(super) fn all() -> [&'static dyn Harness; 2] {
    [&codex::Codex, &claude::Claude]
}
