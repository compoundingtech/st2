//! Shared release-version parsing for the harness admission gates.
//!
//! Each maintained driver gates its provider on a version it was actually measured against. The
//! *policy* — which releases are admitted, and what evidence admitting one costs — stays with the
//! harness that owns it, because those policies genuinely differ: omp gates the launch, opencode
//! degrades to no native delivery, and codex refuses semantic-version reasoning outright.
//!
//! What is shared is only the *parsing*, because getting it wrong is subtle in exactly the same
//! way for every caller: `18.10` is not `18.1`, a bare `18` is not a release, and `18.0.9-rc1` is
//! not `18.0.9`. One tested parser is better than the same three edge cases reimplemented per
//! harness and drifting apart.

/// A parsed `MAJOR.MINOR.PATCH` release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Release {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Release {
    /// The `(major, minor)` series a minor-keyed gate admits.
    pub fn series(&self) -> (u32, u32) {
        (self.major, self.minor)
    }
}

/// Parse exactly `MAJOR.MINOR.PATCH`, all ASCII digits. Anything else is `None` so callers fail
/// closed.
///
/// Strictness is the whole point — it is what stands between a minor-keyed gate and "accept
/// anything that starts with 18":
/// - components are compared as NUMBERS by the caller, so `18.10.0` is a different series from
///   `18.1.0` rather than a string-prefix match of it;
/// - exactly three components are required, so a stray `18` or `7` elsewhere in a version banner
///   cannot be mistaken for a release;
/// - a pre-release or build-metadata suffix (`18.0.9-rc1`, `18.0.9+meta`) does NOT parse, and so
///   is never admitted as its base release. That is deliberate: a pre-release is not the build any
///   capture measured, so it has to be measured and admitted on its own.
pub fn parse_release(token: &str) -> Option<Release> {
    let mut parts = token.split('.');
    let major = parts.next()?;
    let minor = parts.next()?;
    let patch = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let numeric = |part: &str| -> Option<u32> {
        if part.is_empty() || !part.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        part.parse().ok()
    };
    Some(Release {
        major: numeric(major)?,
        minor: numeric(minor)?,
        patch: numeric(patch)?,
    })
}

/// Find the first `MAJOR.MINOR.PATCH` release in a `--version` banner, tolerating the prefixes the
/// providers actually print (`omp/18.0.9`, `v1.18.25`). Returns the exact token alongside the
/// parsed release so a refusal can quote what the binary said.
pub fn find_release(printed: &str) -> Option<(&str, Release)> {
    printed
        .split_whitespace()
        .map(|token| {
            token
                .trim_start_matches("omp/")
                .trim_start_matches("opencode/")
                .trim_start_matches('v')
        })
        .find_map(|token| parse_release(token).map(|release| (token, release)))
}

/// Render admitted series as `18.0.x` for a refusal message.
pub fn series_display(series: &[(u32, u32)]) -> String {
    series
        .iter()
        .map(|(major, minor)| format!("{major}.{minor}.x"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_release_parses_into_its_series() {
        assert_eq!(
            parse_release("18.0.9"),
            Some(Release {
                major: 18,
                minor: 0,
                patch: 9
            })
        );
        assert_eq!(parse_release("18.0.9").unwrap().series(), (18, 0));
    }

    /// The specific way a minor gate turns into "accept anything that starts with 18": series are
    /// numbers, so `18.10` and `18.1` are different, and neither is a prefix of the other.
    #[test]
    fn series_are_numeric_not_string_prefixes() {
        assert_eq!(parse_release("18.1.0").unwrap().series(), (18, 1));
        assert_eq!(parse_release("18.10.0").unwrap().series(), (18, 10));
        assert_ne!(
            parse_release("18.1.0").unwrap().series(),
            parse_release("18.10.0").unwrap().series()
        );
        // Leading zeros must not smuggle a different series past a numeric comparison.
        assert_eq!(parse_release("18.01.0").unwrap().series(), (18, 1));
    }

    #[test]
    fn a_prerelease_or_build_metadata_suffix_does_not_parse() {
        for token in ["18.0.9-rc1", "18.0.9+meta", "18.0.9rc1", "18.0.9_1"] {
            assert_eq!(parse_release(token), None, "{token} must not parse");
        }
    }

    #[test]
    fn only_exactly_three_numeric_components_parse() {
        for token in ["18", "18.0", "18.0.9.1", "18..9", "18.0.", ".0.9", "", "x.y.z"] {
            assert_eq!(parse_release(token), None, "{token} must not parse");
        }
    }

    #[test]
    fn a_banner_yields_its_release_and_ignores_stray_numbers() {
        let (token, release) = find_release("build 7\nomp/18.0.9\n").unwrap();
        assert_eq!(token, "18.0.9");
        assert_eq!(release.series(), (18, 0));
        assert_eq!(find_release("opencode/1.18.25").unwrap().1.series(), (1, 18));
        assert_eq!(find_release("v1.18.25").unwrap().1.series(), (1, 18));
    }

    #[test]
    fn a_banner_with_no_release_is_none() {
        assert!(find_release("not-a-version").is_none());
        assert!(find_release("18").is_none());
        assert!(find_release("").is_none());
    }

    #[test]
    fn series_display_reads_as_a_patch_series() {
        assert_eq!(series_display(&[(18, 0)]), "18.0.x");
        assert_eq!(series_display(&[(1, 18), (1, 19)]), "1.18.x, 1.19.x");
    }
}
