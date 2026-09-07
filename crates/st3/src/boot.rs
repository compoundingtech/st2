pub const BOOT_PROMPT: &str =
    "Read @.st3/boot.md completely. Then list and claim your current st3 work.";

pub const BOOT_DOCUMENT: &str = r#"# st3 boot

The st3 graph is the authority for current work. This file contains stable runtime guidance only.

Run `st3 --help`, `st3 work --help`, and `st3 message --help` before you need an unfamiliar command.

Read each Small Talk message before you act on it. Archive the message after you complete its related action.

A notification does not create work. Repeated delivery does not authorize repeated work.

Run `st3 work ls` to list work that is available to you. Claim a step before you do its work.

The claim output contains the step goals and all effective constraints. A parent claim can expose nested mission steps.

Use `st3 work progress` only for a material update. Finish with `st3 work complete`, `st3 work fail`, or `st3 work release`.

Use `st3 wait` for a graph condition. Do not use an agent turn to poll.

If an unexpected harness fault needs human attention, publish a `harness.diagnostic` claim on your own agent subject.
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
    }
}
