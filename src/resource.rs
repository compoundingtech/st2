//! Resources (M2.6) — st2's native version of smalltalk's `resources/` (brief-009): an agent links
//! high-value output (a PR, a doc, a dashboard) as a durable, listable record so peers/supervisors can
//! find it without digging through the inbox.
//!
//! Each resource is a markdown file with YAML frontmatter (`url`, optional `title`/`tags`/`relation`)
//! and an optional body, named `<unix-ms>-<rand6>.md` (the shared grammar → chronological by name),
//! under `<agent_dir>/resources/links/`. (st2 keeps the inbox/archive/context under `resources/`, so
//! link records get their own `links/` subdir rather than colliding.) Append-only + rename/delete,
//! atomic writes.

use std::fs;
use std::path::{Path, PathBuf};

use crate::message;

/// A resource record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource {
    pub filename: String,
    pub url: String,
    pub title: Option<String>,
    pub tags: Vec<String>,
    /// e.g. `output`, `reference`, `blocked-by` — free-form.
    pub relation: Option<String>,
    pub body: String,
}

/// `<agent_dir>/resources/links` — where an agent's resource records live.
pub fn links_dir(agent_dir: &Path) -> PathBuf {
    agent_dir.join("resources").join("links")
}

/// Basic URL shape: a `scheme:` prefix (http, https, file, pty, anything an agent invents). No host
/// validation — schemes are open-ended by design.
fn valid_url(url: &str) -> bool {
    match url.split_once(':') {
        Some((scheme, _)) => {
            !scheme.is_empty()
                && scheme.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'+' || b == b'-' || b == b'.')
        }
        None => false,
    }
}

/// Render a resource record's file contents (frontmatter + body).
pub fn render(url: &str, title: Option<&str>, tags: &[String], relation: Option<&str>, body: &str) -> String {
    let mut s = String::from("---\n");
    s.push_str(&format!("url: {url}\n"));
    if let Some(t) = title {
        s.push_str(&format!("title: {t}\n"));
    }
    if !tags.is_empty() {
        s.push_str(&format!("tags: {}\n", tags.join(", ")));
    }
    if let Some(r) = relation {
        s.push_str(&format!("relation: {r}\n"));
    }
    s.push_str("---\n");
    s.push_str(body);
    if !body.is_empty() && !body.ends_with('\n') {
        s.push('\n');
    }
    s
}

fn parse(filename: &str, contents: &str) -> Resource {
    let mut r = Resource {
        filename: filename.to_string(),
        url: String::new(),
        title: None,
        tags: Vec::new(),
        relation: None,
        body: String::new(),
    };
    let rest = contents.strip_prefix("---\n").or_else(|| contents.strip_prefix("---\r\n"));
    if let Some(rest) = rest
        && let Some(end) = rest.find("\n---")
    {
        let front = &rest[..end];
        let after = &rest[end + 1..];
        let body = after.split_once('\n').map(|x| x.1).unwrap_or("");
        for line in front.lines() {
            let Some((k, v)) = line.split_once(':') else { continue };
            let v = v.trim();
            match k.trim() {
                "url" => r.url = v.to_string(),
                "title" => r.title = Some(v.to_string()),
                "tags" => r.tags = v.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect(),
                "relation" => r.relation = Some(v.to_string()),
                _ => {}
            }
        }
        r.body = body.to_string();
    } else {
        r.body = contents.to_string();
    }
    r
}

/// Add a resource record for `url`, returning its filename. Creates `links/` if missing.
pub fn add(
    links_dir: &Path,
    url: &str,
    title: Option<&str>,
    tags: &[String],
    relation: Option<&str>,
    body: &str,
) -> anyhow::Result<String> {
    if !valid_url(url) {
        anyhow::bail!("resource add: '{url}' is not a URL (needs a `scheme:` prefix)");
    }
    fs::create_dir_all(links_dir)?;
    let contents = render(url, title, tags, relation, body);
    for _ in 0..8 {
        let filename = message::new_filename();
        let path = links_dir.join(&filename);
        if !path.exists() {
            fs::write(&path, &contents)?;
            return Ok(filename);
        }
    }
    anyhow::bail!("could not allocate a unique resource filename in {}", links_dir.display())
}

/// List an agent's resources, sorted by add time (filename).
pub fn list(links_dir: &Path) -> Vec<Resource> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(links_dir) else { return out };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !message::is_message_filename(&name) {
            continue;
        }
        out.push(parse(&name, &fs::read_to_string(e.path()).unwrap_or_default()));
    }
    out.sort_by(|a, b| a.filename.cmp(&b.filename));
    out
}

/// Read one resource record.
pub fn read(links_dir: &Path, filename: &str) -> anyhow::Result<Resource> {
    let contents = fs::read_to_string(links_dir.join(filename))
        .map_err(|e| anyhow::anyhow!("reading resource {filename}: {e}"))?;
    Ok(parse(filename, &contents))
}

/// Remove one resource record.
pub fn remove(links_dir: &Path, filename: &str) -> anyhow::Result<()> {
    fs::remove_file(links_dir.join(filename)).map_err(|e| anyhow::anyhow!("removing {filename}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_list_read_remove_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = links_dir(tmp.path());
        let tags = vec!["pr".to_string(), "st2".to_string()];
        let f = add(&dir, "https://github.com/x/y/pull/1", Some("M2.6 PR"), &tags, Some("output"), "the resource PR").unwrap();

        let all = list(&dir);
        assert_eq!(all.len(), 1);
        let r = &all[0];
        assert_eq!(r.url, "https://github.com/x/y/pull/1");
        assert_eq!(r.title.as_deref(), Some("M2.6 PR"));
        assert_eq!(r.tags, ["pr", "st2"]);
        assert_eq!(r.relation.as_deref(), Some("output"));

        assert_eq!(read(&dir, &f).unwrap().body.trim_end(), "the resource PR");
        remove(&dir, &f).unwrap();
        assert!(list(&dir).is_empty());
    }

    #[test]
    fn rejects_non_urls() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = links_dir(tmp.path());
        assert!(add(&dir, "not-a-url", None, &[], None, "").is_err());
        assert!(add(&dir, "file:///x", None, &[], None, "").is_ok());
        assert!(add(&dir, "pty://session/abc", None, &[], None, "").is_ok());
    }

    #[test]
    fn missing_links_dir_lists_empty() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(list(&links_dir(tmp.path())).is_empty());
    }
}
