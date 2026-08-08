//! TEMPORARY experiment harness (not for merge): replay captured production panes through the real
//! composer classifier, using the message that actually produced each notice as the oracle, so the
//! staged-unsent shape can be named from a source location instead of forensically.
//!
//! Delete once the finding is carried into synthesized, publishable regression fixtures.

#[cfg(test)]
mod probe {
    use crate::ding::composer::{classify_composer, classify_receipt, strip_ansi};
    use crate::ding::poke_text;
    use crate::message::read_msg;
    use std::path::{Path, PathBuf};

    /// The `[id:<rand6>]` a staged notice carries. That id is the message filename's `<rand6>`, so
    /// it recovers the exact `Message` the sidecar staged and therefore the exact expected text.
    ///
    /// Take the LAST match, not the first. A pane may hold an older submitted notice in scrollback
    /// above the live composer; the live composer is the lowest by construction, which is the same
    /// positional rule the classifier itself uses. Reading the first match would recover the wrong
    /// `expected` and manufacture a spurious `Unproven`.
    fn staged_poke_id(plain: &str) -> Option<String> {
        let (_, rest) = plain.rsplit_once("[DING] new st2 message: [id:")?;
        let (id, _) = rest.split_once(']')?;
        (!id.is_empty() && id.chars().all(char::is_alphanumeric)).then(|| id.to_string())
    }

    /// Find `<unix-ms>-<rand6>.md` in the seat's inbox, then its archive: a seat may have archived
    /// the message after the capture, and an archived message is still the right oracle.
    fn locate_message(catalog: &Path, seat: &str, id: &str) -> Option<(PathBuf, String)> {
        let (host, identity) = seat.split_once('.')?;
        let base = catalog.join("agents").join(host).join(identity);
        for box_name in ["inbox", "archive"] {
            let dir = base.join("resources").join(box_name);
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.filter_map(Result::ok) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(&format!("-{id}.md")) {
                    return Some((dir, name));
                }
            }
        }
        None
    }

    /// Width of the composer box: the two full-width `─` rules that bound it.
    fn composer_rule_width(plain: &str) -> Option<usize> {
        let widths: Vec<usize> = plain
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                (trimmed.chars().count() >= 40 && trimmed.chars().all(|ch| ch == '─'))
                    .then(|| trimmed.chars().count())
            })
            .collect();
        widths.last().copied()
    }

    /// Does the candidate fix's predicate accept this boundary? A renderer word-wrap is proven when
    /// the previous row plus one space plus the continuation's first word could not have fit in the
    /// content width. A break that WOULD have fit is a human newline and must stay unsupported.
    fn wrap_forced(previous_content: usize, continuation: &str, content_width: usize) -> bool {
        let first_word = continuation.split_whitespace().next().unwrap_or("");
        previous_content + 1 + first_word.chars().count() > content_width
    }

    /// Measure the candidate fix against every captured claude composer.
    #[test]
    fn measure_wrap_predicate() {
        let Ok(frames) = std::env::var("DING_FRAMES") else {
            eprintln!("DING_FRAMES unset; skipping");
            return;
        };
        let mut entries: Vec<_> = std::fs::read_dir(&frames)
            .expect("frames dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();
        entries.sort();

        println!("seat\trule\tmaxrow\trows\tcur\tfix\tdetail");
        for path in entries {
            let seat = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("?")
                .trim_end_matches(".ansi.txt")
                .trim_end_matches(".txt")
                .to_string();
            let Ok(raw) = std::fs::read_to_string(&path) else {
                continue;
            };
            let plain = strip_ansi(&raw);
            let Some((_row, candidates, _footer, input)) =
                crate::ding::harness::claude::located_bottom_claude_composer_probe(&plain)
            else {
                continue;
            };
            let rows: Vec<&str> = input.lines().collect();
            let rule = composer_rule_width(&plain).unwrap_or(0);
            if rows.len() < 2 {
                // Still report the width pair: single-row composers calibrate nothing, but a wrapped
                // one at a non-80 rule width is the second data point the chrome offset needs.
                println!(
                    "{seat}\t{rule}\t{}\t1\t-\t-\t-",
                    rows.first().map_or(0, |row| row.chars().count())
                );
                continue;
            }
            // Content width the renderer actually had: the widest row it produced, in content cells
            // (continuations carry a two-cell indent that row 0 does not).
            let content_width = rows
                .iter()
                .enumerate()
                .map(|(index, row)| row.chars().count() - usize::from(index > 0) * 2)
                .max()
                .unwrap_or(0);
            let current = if candidates.proven().is_some() {
                "proven"
            } else {
                "UNSUP"
            };
            let mut detail = Vec::new();
            let fix_ok = rows.iter().enumerate().skip(1).all(|(index, row)| {
                let previous_content =
                    rows[index - 1].chars().count() - usize::from(index - 1 > 0) * 2;
                let continuation = row.strip_prefix("  ").unwrap_or(row);
                let forced = wrap_forced(previous_content, continuation, content_width);
                detail.push(format!(
                    "r{}={}+{}{}",
                    index - 1,
                    previous_content,
                    continuation.split_whitespace().next().unwrap_or("").len(),
                    if forced { "" } else { "!FIT" }
                ));
                forced
            });
            println!(
                "{seat}\t{rule}\t{content_width}\t{}\t{current}\t{}\t{}",
                rows.len(),
                if fix_ok { "proven" } else { "UNSUP" },
                detail.join(" ")
            );
        }
    }

    /// `DING_FRAMES=<dir> DING_CATALOG=<catalog> cargo test --lib ding::frame_probe -- --nocapture`
    #[test]
    fn replay_captured_panes() {
        let (Ok(frames), Ok(catalog)) = (
            std::env::var("DING_FRAMES"),
            std::env::var("DING_CATALOG"),
        ) else {
            eprintln!("DING_FRAMES / DING_CATALOG unset; skipping");
            return;
        };
        let catalog = PathBuf::from(catalog);

        let mut entries: Vec<_> = std::fs::read_dir(&frames)
            .expect("frames dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();
        entries.sort();

        println!("seat\tid\toracle\tcomposer\treceipt");
        for path in entries {
            let seat = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("?")
                .trim_end_matches(".ansi.txt")
                .trim_end_matches(".txt")
                .to_string();
            let Ok(raw) = std::fs::read_to_string(&path) else {
                continue;
            };
            let plain = strip_ansi(&raw);
            let Some(id) = staged_poke_id(&plain) else {
                continue;
            };
            let Some((dir, filename)) = locate_message(&catalog, &seat, &id) else {
                println!("{seat}\t{id}\tNO-MESSAGE\t-\t-");
                continue;
            };
            let Ok(msg) = read_msg(&dir, &filename) else {
                println!("{seat}\t{id}\tUNREADABLE\t-\t-");
                continue;
            };
            let expected = poke_text(&msg);
            println!(
                "{seat}\t{id}\tok\t{:?}\t{:?}",
                classify_composer(&raw, &expected),
                classify_receipt(&raw, &expected)
            );
        }
    }
}
