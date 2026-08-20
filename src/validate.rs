//! `st2 validate <catalog>` — check that a catalog conforms to st2's runner contract before running.
//! Read-only; changes nothing.
//!
//! Two severities, graded by the CoS principle:
//! - **ERROR** — the agent will fail to run, or run and silently do the wrong thing (parse failure,
//!   no identity, unknown `type`, a task silently dropped, an unrendered service, a duplicate id, a
//!   relative path, or a missing **catalog-rooted** path — the renderer's own output). Exits non-zero.
//! - **WARN** — advisory; the run still works (a partially explicit identity/host placement that
//!   mismatches its path-derived default; a dangling supervisor — crash-dings just route nowhere; a
//!   missing **external** path for an agent assigned to the selected validation host; an overlay
//!   `@import` that does not resolve — a *render* concern, not st2 law, since a valid spec may carry
//!   no persona). `--strict` promotes every WARN to a failure so a renderer's CI can demand
//!   spotless.
//!
//! st2 stays render-agnostic: render-only fields (`harness`, `model`, `persona`,
//! `permissions`, …) are never required — their absence is never an issue.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use agent_spec::discovery::{discover, path_defaults};
use agent_spec::spec::{AgentSpec, JobType};
use agent_spec::{DeclaredDiagnosticCode, DeclaredParse, DeclaredSeverity, DeclaredValue};

pub const VALIDATE_RECEIPT_SCHEMA: &str = "st2.validate.v2";
pub const CORE_CATALOG_POLICY_PROFILE: &str = "st2.core+catalog.v1";

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
    let task_context = match crate::reconcile::TaskCompileContext::current(root.to_path_buf()) {
        Ok(context) => Some(context),
        Err(error) => {
            issues.push(Issue::error(
                "launch-compile-error",
                ".".to_string(),
                None,
                format!("cannot prepare generated task compilation: {error:#}"),
            ));
            None
        }
    };

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
    let mut explicit_placements: HashSet<(PathBuf, String, String)> = HashSet::new();
    for file in &d.declarations {
        for raw in &file.agents {
            if let (Some(identity), Some(host)) = (&raw.identity, &raw.host) {
                explicit_placements.insert((file.path.clone(), identity.clone(), host.clone()));
            }
            if let Some(t) = &raw.job_type
                && t != "service"
            {
                // FOLLOW-UP (CoS-noted, deferred): a broader "unknown field / typo" WARN lint
                // (e.g. a `harnes` typo) is worth having, but it must not error on render-only
                // nodes st2 ignores by design — kept out of scope for now to preserve
                // render-agnosticism. `type` is safe to check because its value set is closed.
                issues.push(Issue::error(
                    "unknown-type",
                    rel(root, &file.path),
                    raw.identity.clone(),
                    format!("unknown type '{t}' (expected service; `type = batch` is retired — use `st2 eval`)"),
                ));
            }
        }
        if let Some(parse) = &file.parse {
            issues.extend(kdl_shape_check(root, &file.path, parse));
        }
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
        let compiled = task_context.as_ref().map(|context| {
            let mut compiled = s.clone();
            let compile_host = this_host.or(s.host.as_deref()).unwrap_or("");
            crate::reconcile::compile_generated_tasks(
                std::slice::from_mut(&mut compiled),
                compile_host,
                context,
            )
            .map(|()| compiled)
            .map_err(|error| format!("{error:#}"))
        });

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

        if let Some(Err(error)) = &compiled {
            let code = if s.driver.is_some() && s.delivery.is_some() {
                "driver-deliver-conflict"
            } else {
                "launch-compile-error"
            };
            issues.push(Issue::error(code, rp.clone(), ag.clone(), error.clone()));
        }

        // An explicit identity+host pair is authoritative regardless of folder names. When either
        // field is omitted, path defaults remain part of placement and mismatches stay advisory.
        let explicit_placement = s.host.as_ref().is_some_and(|host| {
            explicit_placements.contains(&(s.path.clone(), s.identity.clone(), host.clone()))
        });
        let (path_id, path_host) = path_defaults(root, &s.path);
        if let Some(pid) = &path_id
            && pid != &s.identity
            && !explicit_placement
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
            && !explicit_placement
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
        if let Some(Ok(compiled)) = &compiled
            && s.job_type == JobType::Service
            && !compiled.is_runnable()
        {
            issues.push(Issue::error(
                "not-runnable",
                rp.clone(),
                ag.clone(),
                "service agent has no task with `command` or `argv` (unrendered, or the renderer emitted none)"
                    .to_string(),
            ));
        }

        // Path fields must be absolute, $CATALOG-rooted, or the one canonical relative workspace.
        for (field, raw) in path_fields(s) {
            if let Some(issue) = check_path(
                root,
                s.path.parent().unwrap_or(root),
                &rp,
                &ag,
                &field,
                &raw,
                runs_on_selected_host,
            ) {
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

    if let Some(host) = this_host {
        for conflict in crate::materialize::render_ownership_conflicts(root, &d.specs, host) {
            issues.push(Issue::error(
                "render-owner-conflict",
                ".".to_string(),
                None,
                format!(
                    "conflicting render ownership for '{}': active agents {} declare incompatible content for one shared workspace target",
                    conflict.destination.display(),
                    conflict.owners.iter().cloned().collect::<Vec<_>>().join(", ")
                ),
            ));
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

/// Check one path field through the shared launch-equivalent resolver. Absolute paths remain valid;
/// a relative path must normalize to the declaring bundle's `.workspace`, and an unresolved
/// variable fails closed. Catalog-owned paths must always exist; external paths are checked only
/// for an agent assigned to the selected host.
fn check_path(
    root: &Path,
    spec_dir: &Path,
    rp: &str,
    ag: &Option<String>,
    field: &str,
    raw: &str,
    check_external_presence: bool,
) -> Option<Issue> {
    let resolved = match crate::expand::resolve_spec_path(raw, root, spec_dir) {
        Ok(path) => path,
        Err(error) => {
            return Some(Issue::error(
                "bad-path",
                rp.to_string(),
                ag.clone(),
                format!("{field} '{raw}' is invalid: {error}"),
            ));
        }
    };
    let p = &resolved;
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
fn kdl_shape_check(root: &Path, path: &Path, parsed: &DeclaredParse) -> Vec<Issue> {
    let document = parsed.document.as_ref();
    parsed
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.severity == DeclaredSeverity::Error)
        // Discovery already reports syntax errors through its established parse-error contract.
        .filter(|diagnostic| diagnostic.code != DeclaredDiagnosticCode::KdlSyntax)
        .map(|diagnostic| {
            let agent = document.and_then(|document| {
                document
                    .agents
                    .iter()
                    .find(|agent| {
                        let end = agent.span.offset + agent.span.length;
                        diagnostic.span.offset >= agent.span.offset && diagnostic.span.offset < end
                    })
                    .and_then(|agent| agent.identity())
                    .and_then(DeclaredValue::as_str)
                    .map(str::to_owned)
            });
            Issue::error(
                if diagnostic.code == DeclaredDiagnosticCode::TaskNameMissing {
                    "unknown-task-kind"
                } else {
                    match diagnostic.code {
                        DeclaredDiagnosticCode::UnsupportedSchedule => "unsupported-schedule",
                        DeclaredDiagnosticCode::UnsupportedStreamInterval => {
                            "unsupported-stream-interval"
                        }
                        DeclaredDiagnosticCode::UnexpectedTopLevelNode => {
                            "unexpected-top-level-node"
                        }
                        _ => "declaration-shape",
                    }
                },
                rel(root, path),
                agent,
                diagnostic.message.clone(),
            )
        })
        .collect()
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
