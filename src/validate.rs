//! `st2 validate <catalog>` — check that a catalog conforms to st2's runner contract before running.
//! Read-only; changes nothing.
//!
//! Two severities, graded by the CoS principle:
//! - **ERROR** — the agent will fail to run, or run and silently do the wrong thing (parse failure,
//!   no identity, unknown `type`, a task silently dropped, an unrendered service, a duplicate id, a
//!   relative path, or a missing **catalog-rooted** path — the renderer's own output). Exits non-zero.
//! - **WARN** — advisory; the run still works (identity/host path↔content mismatch — the spec says a
//!   mismatch is a warning; a dangling supervisor — crash-dings just route nowhere; a missing
//!   **external** path for an agent assigned to the selected validation host; an overlay `@import`
//!   that does not resolve — a *render* concern, not st2 law, since a valid spec may carry no
//!   persona). `--strict` promotes every WARN to a failure so a renderer's CI can demand spotless.
//!
//! st2 stays render-agnostic: render-only fields (`harness`, `model`, `persona`,
//! `permissions`, …) are never required — their absence is never an issue.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use agent_spec::discovery::{discover, parse_declared, path_defaults};
use agent_spec::spec::{AgentSpec, JobType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warn,
}

impl Severity {
    /// Fixed-width label for aligned human output.
    pub fn label(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Warn => "WARN ",
        }
    }
    /// Lowercase tag for `--json`.
    pub fn tag(self) -> &'static str {
        match self {
            Severity::Error => "error",
            Severity::Warn => "warn",
        }
    }
}

/// One conformance issue. `code` is stable and machine-matchable; `message` is human prose.
#[derive(Debug, Clone)]
pub struct Issue {
    pub severity: Severity,
    pub code: &'static str,
    /// Catalog-relative path of the offending file.
    pub path: String,
    /// The agent's identity, when known.
    pub agent: Option<String>,
    pub message: String,
}

impl Issue {
    fn error(code: &'static str, path: String, agent: Option<String>, message: String) -> Self {
        Issue {
            severity: Severity::Error,
            code,
            path,
            agent,
            message,
        }
    }
    fn warn(code: &'static str, path: String, agent: Option<String>, message: String) -> Self {
        Issue {
            severity: Severity::Warn,
            code,
            path,
            agent,
            message,
        }
    }
}

/// The outcome of a validation pass.
#[derive(Debug, Default)]
pub struct Report {
    pub issues: Vec<Issue>,
    /// How many agents resolved in the catalog (context for the summary line).
    pub agents: usize,
}

impl Report {
    pub fn errors(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .count()
    }
    pub fn warnings(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Warn)
            .count()
    }
}

/// Validate a catalog. Returns every issue found, in a stable order (files sorted by discovery).
pub fn validate(root: &Path) -> Report {
    validate_scoped(root, None)
}

/// Validate a whole catalog while checking host-local filesystem facts only for `this_host`.
///
/// Structural checks remain fleet-wide. This scope only prevents a synced multi-host catalog from
/// warning that another machine's external workspace or task cwd is absent locally.
pub fn validate_for_host(root: &Path, this_host: &str) -> Report {
    validate_scoped(root, Some(this_host))
}

fn validate_scoped(root: &Path, this_host: Option<&str>) -> Report {
    // Canonicalize so `$CATALOG`-rooted paths expand to absolute paths (a relative root would make
    // every `$CATALOG/...` look relative). Falls back to the given root if it does not exist yet.
    let root = &root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let d = discover(root);
    let mut issues = Vec::new();

    // 1. Files that looked like specs but did not parse/resolve — discovery already caught these.
    for e in &d.errors {
        let (code, message) = if e.message.contains("no identity") {
            (
                "no-identity",
                "spec has no identity in content or path".to_string(),
            )
        } else {
            ("parse-error", e.message.clone())
        };
        issues.push(Issue::error(code, rel(root, &e.path), None, message));
    }

    // 2. The catalog's own declaration. Its field set is closed (like `type`), so a typo is checkable
    //    here without touching render-agnosticism — and it must be, because a mistyped `pty-root`
    //    silently resolves back to `<catalog>/pty` and reads as an agent whose task is dead.
    if let Err(e) = crate::catalog::load(root) {
        issues.push(Issue::error(
            "catalog-config",
            crate::catalog::CONFIG_FILE.to_string(),
            None,
            e.to_string(),
        ));
    }

    // 3. Raw pass (once per file): a typo'd `type` is normalized to `service` by the parser, so it can
    //    only be seen before lowering. A KDL `pty`/`exec` block with no name is silently dropped — the
    //    task just vanishes, the classic "silently does the wrong thing".
    let mut files: Vec<&PathBuf> = d.specs.iter().map(|s| &s.path).collect();
    files.sort();
    files.dedup();
    for f in files {
        if let Ok(raws) = parse_declared(f) {
            for raw in raws {
                if let Some(t) = &raw.job_type
                    && t != "service"
                {
                    // FOLLOW-UP (CoS-noted, deferred): a broader "unknown field / typo" WARN lint
                    // (e.g. a `harnes` typo) is worth having, but it must not error on render-only
                    // nodes st2 ignores by design — kept out of scope for now to preserve
                    // render-agnosticism. `type` is safe to check because its value set is closed.
                    issues.push(Issue::error(
                        "unknown-type",
                        rel(root, f),
                        raw.identity.clone(),
                        format!("unknown type '{t}' (expected service; `type = batch` is retired — use `st2 eval`)"),
                    ));
                }
            }
        }
        issues.extend(kdl_shape_check(root, f));
    }

    // 4. Resolved pass: cross-spec + field checks over each agent.
    let identities: HashSet<&str> = d.specs.iter().map(|s| s.identity.as_str()).collect();
    let mut seen: HashMap<String, PathBuf> = HashMap::new();
    // Placeholder host for bus-id collision: catalogs carry explicit host, and an empty host still
    // makes two unset-host same-identity specs collide (which is the real bug).
    let collision_host = "";
    let addresses: HashSet<String> = d
        .specs
        .iter()
        .flat_map(|s| {
            let mut values = vec![s.identity.clone()];
            if s.host.is_some() {
                values.push(s.bus_id(collision_host));
            }
            values
        })
        .collect();

    for s in &d.specs {
        let rp = rel(root, &s.path);
        let ag = Some(s.identity.clone());
        let runs_on_selected_host = match this_host {
            Some(host) => s.resolved_host(host) == host,
            None => true,
        };

        // Duplicate bus id — the runner cannot run two agents under one <host>.<identity>.
        let bid = s.bus_id(collision_host);
        if let Some(prev) = seen.insert(bid.clone(), s.path.clone()) {
            issues.push(Issue::error(
                "dup-id",
                rp.clone(),
                ag.clone(),
                format!(
                    "duplicate agent id '{}' (also declared in {})",
                    bid,
                    rel(root, &prev)
                ),
            ));
        }

        // identity / host path↔content mismatch (content wins; advisory) — re-derived structurally.
        let (path_id, path_host) = path_defaults(root, &s.path);
        if let Some(pid) = &path_id
            && pid != &s.identity
        {
            issues.push(Issue::warn(
                "id-path-mismatch",
                rp.clone(),
                ag.clone(),
                format!(
                    "identity '{}' differs from folder '{pid}' (content wins)",
                    s.identity
                ),
            ));
        }
        if let (Some(h), Some(ph)) = (&s.host, &path_host)
            && h != ph
        {
            issues.push(Issue::warn(
                "host-path-mismatch",
                rp.clone(),
                ag.clone(),
                format!("host '{h}' differs from folder '{ph}' (content wins)"),
            ));
        }

        // A rendered service agent must be runnable. Batch jobs legitimately carry no pty/exec tasks
        // (their work is in stages/run) — never flag them here.
        if s.job_type == JobType::Service && !s.is_runnable() {
            issues.push(Issue::error(
                "not-runnable",
                rp.clone(),
                ag.clone(),
                "service agent has no task with a command (unrendered, or the renderer emitted none)"
                    .to_string(),
            ));
        }

        // Path fields must be absolute or $CATALOG-rooted, and must exist.
        for (field, raw) in path_fields(s) {
            if let Some(issue) = check_path(root, &rp, &ag, &field, &raw, runs_on_selected_host) {
                issues.push(issue);
            }
        }

        // Runtime routing accepts either a bare identity or a fully-qualified <host>.<identity>.
        // Validation must index the same address set or it rejects declarations the bus can route.
        if let Some(sup) = &s.supervisor
            && !identities.contains(sup.as_str())
            && !addresses.contains(sup)
        {
            issues.push(Issue::warn(
                "dangling-supervisor",
                rp.clone(),
                ag.clone(),
                format!("supervisor '{sup}' is not an agent in this catalog"),
            ));
        }

        // Overlay lint: render's persona overlay `@import`s must resolve (WARN — render concern).
        if runs_on_selected_host {
            issues.extend(overlay_lint(&rp, &ag, s));
        }

        // Declarative render is a pre-boot gate: malformed directives, unsafe destinations, or a
        // missing catalog-owned copy source would prevent this agent from booting.
        if let Err(error) =
            crate::materialize::validate_agent(root, s, s.host.as_deref().unwrap_or(""))
        {
            issues.push(Issue::error("render-error", rp, ag, format!("{error:#}")));
        }
    }

    Report {
        issues,
        agents: d.specs.len(),
    }
}

/// Catalog-relative display path, falling back to the full path.
fn rel(root: &Path, p: &Path) -> String {
    p.strip_prefix(root).unwrap_or(p).display().to_string()
}

/// The literal path fields st2 owns: the agent `workspace` and each task's `cwd`. The task `command`
/// is an opaque `sh -c` line, not a path, so it is not checked here.
fn path_fields(s: &AgentSpec) -> Vec<(String, String)> {
    let mut v = Vec::new();
    if let Some(w) = &s.workspace {
        v.push(("workspace".to_string(), w.clone()));
    }
    for t in &s.tasks {
        if let Some(cwd) = &t.cwd {
            v.push((format!("task '{}' cwd", t.name), cwd.clone()));
        }
    }
    v
}

/// Check one path field: `$CATALOG` expands to the catalog root; a path bearing any *other* `$VAR` is
/// skipped (an unset var is a literal token — do not guess). What remains must be absolute (R11:
/// final-spec paths are absolute or $CATALOG-rooted, never relative). Catalog-owned paths must
/// always exist; external paths are checked only for an agent assigned to the selected host.
fn check_path(
    root: &Path,
    rp: &str,
    ag: &Option<String>,
    field: &str,
    raw: &str,
    check_external_presence: bool,
) -> Option<Issue> {
    let root_s = root.to_string_lossy();
    let expanded = raw
        .replace("${CATALOG}", &root_s)
        .replace("$CATALOG", &root_s);
    if expanded.contains('$') {
        return None; // another variable — cannot resolve without runtime env; do not guess
    }
    let p = Path::new(&expanded);
    if !p.is_absolute() {
        return Some(Issue::error(
            "bad-path",
            rp.to_string(),
            ag.clone(),
            format!(
                "{field} '{raw}' is relative (final-spec paths must be absolute or $CATALOG-rooted)"
            ),
        ));
    }
    if !p.exists() {
        // A **catalog-rooted** path is the renderer's own output — its absence is a real render bug
        // (ERROR). An **external** absolute path is checked only for the selected run host; its
        // absence there is advisory (WARN), not a structural catalog failure.
        return if p.starts_with(root) {
            Some(Issue::error(
                "bad-path",
                rp.to_string(),
                ag.clone(),
                format!("{field} '{raw}' does not exist"),
            ))
        } else if check_external_presence {
            Some(Issue::warn(
                "bad-path",
                rp.to_string(),
                ag.clone(),
                format!("{field} '{raw}' does not exist (absent on this host? — not the run host)"),
            ))
        } else {
            None
        };
    }
    None
}

/// Catch runner-significant KDL shapes that the permissive lowerer cannot accept silently. TOML/JSON
/// tasks are keyed maps and cannot be nameless; `schedule` is a reserved future KDL surface.
fn kdl_shape_check(root: &Path, path: &Path) -> Vec<Issue> {
    if path.extension().and_then(|e| e.to_str()) != Some("kdl") {
        return Vec::new();
    }
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(doc) = kdl::KdlDocument::parse(&text) else {
        return Vec::new(); // a parse failure is already reported via discovery
    };
    let mut out = Vec::new();
    for node in doc.nodes() {
        if node.name().value() != "agent" {
            continue;
        }
        let agent = node.get(0).and_then(|v| v.as_string()).map(String::from);
        for child in node.children().into_iter().flat_map(|c| c.nodes()) {
            let kind = child.name().value();
            if (kind == "pty" || kind == "exec")
                && child.get(0).and_then(|v| v.as_string()).is_none()
            {
                out.push(Issue::error(
                    "unknown-task-kind",
                    rel(root, path),
                    agent.clone(),
                    format!("{kind} task has no name — it is silently dropped and never runs"),
                ));
            }
            if kind == "schedule" {
                out.push(Issue::error(
                    "unsupported-schedule",
                    rel(root, path),
                    agent.clone(),
                    "scheduled work is reserved for a future contract and is not implemented"
                        .to_string(),
                ));
            }
        }
    }
    out
}

/// Best-effort persona-overlay lint: if render's overlay is present in the agent's workspace
/// (`<workspace>/.claude/rules/*.md`), any `@<relative-path>` import it declares must resolve. Absent
/// overlay → nothing to lint (a valid spec may carry no persona). WARN only — render, not st2 law.
fn overlay_lint(rp: &str, ag: &Option<String>, s: &AgentSpec) -> Vec<Issue> {
    let Some(ws) = &s.workspace else {
        return Vec::new();
    };
    if ws.contains('$') {
        return Vec::new(); // unresolved workspace path — cannot locate the overlay
    }
    let rules_dir = Path::new(ws).join(".claude").join("rules");
    let Ok(entries) = fs::read_dir(&rules_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let rule = entry.path();
        if rule.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        let Ok(body) = fs::read_to_string(&rule) else {
            continue;
        };
        for line in body.lines() {
            let line = line.trim();
            if let Some(target) = line.strip_prefix('@')
                && !target.is_empty()
                && !target.contains(char::is_whitespace)
            {
                let resolved = rule.parent().unwrap_or(&rules_dir).join(target);
                if !resolved.exists() {
                    out.push(Issue::warn(
                        "dangling-import",
                        rp.to_string(),
                        ag.clone(),
                        format!(
                            "overlay {} imports '{target}', which does not exist",
                            rule.display()
                        ),
                    ));
                }
            }
        }
    }
    out
}
