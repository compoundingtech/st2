pub const BOOT_PROMPT: &str =
    "Read @.st3/boot.md completely. Then list, claim, do, and finish your current st3 work.";

pub const BOOT_DOCUMENT: &str = r#"# st3 boot

The st3 graph is the authority for current work. This file contains stable runtime guidance only.

`ST3_BIN` is the exact st3 executable that started your harness. Use `"$ST3_BIN"` for every st3 command.

Run `"$ST3_BIN" --help`, `"$ST3_BIN" work --help`, and `"$ST3_BIN" message --help` before you need an unfamiliar command.

Read each Small Talk message before you act on it. Archive the message after you complete its related action.

A notification does not create work. Repeated delivery does not authorize repeated work.

Run `"$ST3_BIN" work ls` to list work that is available to you. Claim one eligible step.

If no step is ready, finish this turn. Do not wait for work that is not ready.

Do its work and finish the step in the same turn when possible.

The claim output contains the step goals and all effective constraints. A parent claim can expose nested mission steps.

Use `"$ST3_BIN" work progress` only for a material update. Finish with `"$ST3_BIN" work complete`, `"$ST3_BIN" work fail`, or `"$ST3_BIN" work release`.

Use `"$ST3_BIN" wait` only when claimed work needs a graph condition. Do not use an agent turn to poll.

The wait command exits early when a new message or a new eligible step needs your attention.

Publish a `harness.diagnostic` claim when the harness itself fails.

When a person must act, run `"$ST3_BIN" attention request --help` and publish one explicit request for the responsible person.
"#;

pub fn compose_prompt(authored: Option<&str>) -> String {
    let normalized = authored.unwrap_or_default().replace(BOOT_PROMPT, "");
    let authored = normalized.trim();
    if authored.is_empty() {
        return BOOT_PROMPT.into();
    }
    format!("{authored}\n\n{BOOT_PROMPT}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_boot_prompt_is_present_exactly_once() {
        assert_eq!(compose_prompt(None), BOOT_PROMPT);
        assert_eq!(
            compose_prompt(Some("Do the task.")),
            format!("Do the task.\n\n{BOOT_PROMPT}")
        );
        assert_eq!(
            compose_prompt(Some(&format!("Do the task.\n\n{BOOT_PROMPT}"))),
            format!("Do the task.\n\n{BOOT_PROMPT}")
        );
        assert_eq!(
            compose_prompt(Some(&format!(
                "{BOOT_PROMPT}\n\nDo the task.\n\n{BOOT_PROMPT}"
            ))),
            format!("Do the task.\n\n{BOOT_PROMPT}")
        );
        assert!(BOOT_PROMPT.contains("claim, do, and finish"));
    }

    #[test]
    fn the_boot_document_continues_after_a_claim() {
        assert!(
            BOOT_DOCUMENT
                .contains("Do its work and finish the step in the same turn when possible.")
        );
        assert!(BOOT_DOCUMENT.contains("ST3_BIN"));
        assert!(BOOT_DOCUMENT.contains("If no step is ready, finish this turn."));
        assert!(BOOT_DOCUMENT.contains("wait` only when claimed work needs"));
        assert!(BOOT_DOCUMENT.contains("attention request --help"));
    }
}
