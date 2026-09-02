use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use agent_spec::spec::Driver;
use anyhow::{Context as _, Result};
use clap::{Args, Parser, Subcommand};
use kdl::{KdlDocument, KdlEntry, KdlNode};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use st2::eval_spec::{Check, JsonScalar, JudgeKind, Spec as EvalSpec};
use walkdir::WalkDir;

const WAIT_TEAM_DONE: &[u8] = include_bytes!("../assets/wait-team-done.sh");

#[derive(Parser)]
#[command(
    name = "st3-migrate",
    version,
    about = "Offline st2 to st3 KDL transformer"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    File(FileArgs),
    Catalog(TreeArgs),
    Evals(TreeArgs),
}

#[derive(Args)]
struct FileArgs {
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    report: PathBuf,
}

#[derive(Args)]
struct TreeArgs {
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    report: PathBuf,
    #[arg(long, default_value = "local")]
    host: String,
}

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    mode: String,
    input: String,
    output: String,
    files: Vec<FileReport>,
    documents: Vec<DocumentReport>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FileReport {
    input: String,
    output: String,
    subjects: Vec<String>,
    source_hash: String,
}

#[derive(Debug, Serialize)]
struct DocumentReport {
    name: String,
    hash: String,
    source: String,
    staged: String,
    put_command: String,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::File(args) => migrate_file(args),
        Command::Catalog(args) => migrate_catalog(args),
        Command::Evals(args) => migrate_evals(args),
    }
}

fn migrate_file(args: FileArgs) -> Result<()> {
    anyhow::ensure!(
        args.input != args.output,
        "the output must differ from the input"
    );
    let source = fs::read_to_string(&args.input)?;
    let transformed = transform_declaration(&source, None)?;
    let host = "local";
    let normalized = st3::parse_intent(&transformed, host)?;
    write_file(&args.output, transformed.as_bytes())?;
    let report = Report {
        schema: "st3-migrate-report.v1",
        mode: "file".into(),
        input: args.input.display().to_string(),
        output: args.output.display().to_string(),
        files: vec![FileReport {
            input: args.input.display().to_string(),
            output: args.output.display().to_string(),
            subjects: normalized.subjects.keys().cloned().collect(),
            source_hash: normalized.source_hash,
        }],
        documents: Vec::new(),
        warnings: Vec::new(),
    };
    write_report(&args.report, &report)
}

fn migrate_catalog(args: TreeArgs) -> Result<()> {
    validate_tree_args(&args)?;
    clear_generated_assets(&args.output)?;
    let discovery = agent_spec::discovery::discover_strict(&args.input);
    anyhow::ensure!(
        discovery.errors.is_empty(),
        "catalog discovery failed: {}",
        discovery
            .errors
            .iter()
            .map(|error| format!("{}: {}", error.path.display(), error.message))
            .collect::<Vec<_>>()
            .join("; ")
    );
    let states = discovery
        .specs
        .iter()
        .map(|spec| (spec.path.clone(), spec.desired_state.is_running()))
        .collect::<BTreeMap<_, _>>();
    let mut report = new_report("catalog", &args);
    let mut files = discovery
        .declarations
        .iter()
        .filter(|declaration| {
            declaration
                .path
                .extension()
                .and_then(|value| value.to_str())
                == Some("kdl")
        })
        .map(|declaration| declaration.path.clone())
        .collect::<Vec<_>>();
    files.sort();
    for input in files {
        let relative = input.strip_prefix(&args.input)?;
        let output = args.output.join(relative);
        let source = fs::read_to_string(&input)?;
        let running = states.get(&input).copied();
        let mut transformed = transform_declaration(&source, running)?;
        let documents =
            rewrite_render_documents(&mut transformed, &args.input, &args.output, relative)?;
        report.documents.extend(documents);
        let normalized = st3::parse_intent(&transformed, &args.host)
            .with_context(|| format!("validate transformed {}", input.display()))?;
        write_file(&output, transformed.as_bytes())?;
        report.files.push(FileReport {
            input: input.display().to_string(),
            output: output.display().to_string(),
            subjects: normalized.subjects.keys().cloned().collect(),
            source_hash: normalized.source_hash,
        });
    }
    report.warnings.extend(discovery.warnings);
    write_report(&args.report, &report)
}

fn migrate_evals(args: TreeArgs) -> Result<()> {
    validate_tree_args(&args)?;
    let mut report = new_report("evals", &args);
    for input in eval_definition_files(&args.input)? {
        let source = fs::read_to_string(&input)?;
        let spec = match st2::eval_spec::parse_spec(&source) {
            Ok(spec) if spec.eval.is_some() => spec,
            Ok(_) => continue,
            Err(error) => {
                report
                    .warnings
                    .push(format!("{} is not an eval: {error}", input.display()));
                continue;
            }
        };
        let cell = input.parent().context("eval KDL has no parent")?;
        let relative_cell = cell.strip_prefix(&args.input)?;
        let output_cell = args.output.join(relative_cell);
        fs::create_dir_all(&output_cell)?;
        clear_generated_assets(&output_cell)?;
        copy_eval_assets(
            cell,
            &output_cell,
            &input,
            spec.eval.as_ref().and_then(|eval| eval.copy.as_deref()),
        )?;
        let (transformed, documents) = transform_eval(&spec, cell, &output_cell, &args.host)?;
        report.documents.extend(documents);
        let normalized = st3::parse_intent(&transformed, &args.host)
            .with_context(|| format!("validate transformed {}", input.display()))?;
        let output = output_cell.join("eval.kdl");
        write_file(&output, transformed.as_bytes())?;
        report.files.push(FileReport {
            input: input.display().to_string(),
            output: output.display().to_string(),
            subjects: normalized.subjects.keys().cloned().collect(),
            source_hash: normalized.source_hash,
        });
    }
    write_report(&args.report, &report)
}

fn transform_declaration(source: &str, running: Option<bool>) -> Result<String> {
    let document: KdlDocument = source
        .parse()
        .with_context(|| format!("parse legacy checkpoint KDL after harness rewrite:\n{source}"))?;
    let version = st2::kdl_version::document_version(&document)?;
    let roots = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() != "version")
        .collect::<Vec<_>>();
    if version == 2 && roots.len() == 1 && roots[0].name().value() == "subgraph" {
        st3::parse_intent(source, "local")?;
        return Ok(source.to_owned());
    }
    anyhow::ensure!(
        st2::kdl_version::ST2_KDL_VERSIONS.contains(&version),
        "the source uses unsupported KDL version {version}"
    );
    anyhow::ensure!(!document.nodes().is_empty(), "the declaration is empty");
    let mut children = KdlDocument::new();
    for node in roots {
        anyhow::ensure!(
            node.name().value() == "agent",
            "old declaration root `{}` is not an agent",
            node.name().value()
        );
        let name = node
            .get(0)
            .and_then(|value| value.as_string())
            .context("old agent has no name")?;
        if running == Some(false) {
            let host = node
                .children()
                .and_then(|body| body.get("host"))
                .and_then(|host| host.get(0))
                .and_then(|value| value.as_string());
            let identity = node
                .children()
                .and_then(|body| body.get("identity"))
                .and_then(|identity| identity.get(0))
                .and_then(|value| value.as_string())
                .unwrap_or(name);
            let bus = if identity.contains('.') {
                identity.to_owned()
            } else if let Some(host) = host {
                format!("{host}.{identity}")
            } else {
                identity.to_owned()
            };
            let mut stop = KdlNode::new("stop");
            stop.entries_mut()
                .push(KdlEntry::new(format!("agent/{bus}")));
            children.nodes_mut().push(stop);
            continue;
        }
        let mut agent = node.clone();
        let body = agent.children_mut().get_or_insert_with(KdlDocument::new);
        body.nodes_mut().retain(|child| {
            !matches!(
                child.name().value(),
                "retired" | "desired-state" | "suspended"
            ) && !(child.name().value() == "harness" && child.children().is_none())
        });
        rewrite_harness_nodes(body);
        remove_legacy_context_hooks(body);
        let has_restart_type = body.nodes().iter().any(|child| {
            child.name().value() == "restart"
                && child.children().is_none()
                && child.get(0).and_then(|value| value.as_string()).is_some()
        });
        if !has_restart_type {
            let mut restart = KdlNode::new("restart");
            restart.entries_mut().push(KdlEntry::new("always"));
            body.nodes_mut().push(restart);
        }
        children.nodes_mut().push(agent);
    }
    let mut root = KdlNode::new("subgraph");
    root.set_children(children);
    let mut output = KdlDocument::new();
    let mut version = KdlNode::new("version");
    version.entries_mut().push(KdlEntry::new(2));
    output.nodes_mut().push(version);
    output.nodes_mut().push(root);
    output.autoformat();
    Ok(rewrite_catalog_text(&output.to_string()))
}

fn rewrite_harness_nodes(document: &mut KdlDocument) {
    for node in document.nodes_mut() {
        let provider = match node.name().value() {
            "claude" | "codex" | "pi" | "opencode" if node.children().is_some() => {
                Some(node.name().value().to_owned())
            }
            _ => None,
        };
        if let Some(provider) = provider {
            node.set_name("harness");
            node.entries_mut().insert(0, KdlEntry::new(provider));
        }
    }
}

fn remove_legacy_context_hooks(document: &mut KdlDocument) {
    document.nodes_mut().retain(|node| {
        if node.name().value() != "json-upsert" {
            return true;
        }
        let content = node
            .get(1)
            .and_then(|value| value.as_string())
            .unwrap_or("");
        !content.contains("codex-pre-compact.sh")
            && !content.contains("codex-session-start.sh")
            && !content.contains("codex-stop.sh")
    });
    for node in document.nodes_mut() {
        if node.name().value() == "json-upsert"
            && let Some(content) = node.get(1).and_then(|value| value.as_string())
            && content.contains("$ST_HOOKS/claude-")
            && let Some(cleaned) = clean_claude_hook_json(content)
        {
            node.entries_mut()[1] = KdlEntry::new(cleaned);
        }
        if let Some(children) = node.children_mut() {
            remove_legacy_context_hooks(children);
        }
    }
}

fn clean_claude_hook_json(content: &str) -> Option<String> {
    let mut value: serde_json::Value = serde_json::from_str(content).ok()?;
    let hooks = value.get_mut("hooks")?.as_object_mut()?;
    hooks.retain(|_, groups| {
        let Some(groups) = groups.as_array_mut() else {
            return true;
        };
        groups.retain_mut(|group| {
            let Some(entries) = group
                .get_mut("hooks")
                .and_then(|value| value.as_array_mut())
            else {
                return true;
            };
            entries.retain(|entry| {
                !entry
                    .get("command")
                    .and_then(|value| value.as_str())
                    .is_some_and(|command| command.contains("$ST_HOOKS/claude-"))
            });
            !entries.is_empty()
        });
        !groups.is_empty()
    });
    serde_json::to_string_pretty(&value).ok()
}

fn rewrite_render_documents(
    source: &mut String,
    catalog_root: &Path,
    output_root: &Path,
    relative: &Path,
) -> Result<Vec<DocumentReport>> {
    let mut document: KdlDocument = source.parse()?;
    let mut documents = Vec::new();
    for root in document.nodes_mut() {
        rewrite_copy_nodes(root, catalog_root, output_root, relative, &mut documents)?;
    }
    document.autoformat();
    *source = document.to_string();
    Ok(documents)
}

fn rewrite_copy_nodes(
    node: &mut KdlNode,
    catalog_root: &Path,
    output_root: &Path,
    relative: &Path,
    documents: &mut Vec<DocumentReport>,
) -> Result<()> {
    if node.name().value() == "copy"
        && let Some(source) = node.get(0).and_then(|value| value.as_string())
    {
        let path = catalog_root.join(source);
        if path.is_file() {
            let bytes = fs::read(&path)?;
            let bytes = match std::str::from_utf8(&bytes) {
                Ok(text) => rewrite_catalog_text(text).into_bytes(),
                Err(_) => bytes,
            };
            let hash = hex::encode(Sha256::digest(&bytes));
            let clean = source.trim_start_matches("./").replace(['\\', ' '], "-");
            let name = format!("doc/catalog/{clean}");
            let reference = format!("{name}@{hash}");
            node.entries_mut()[0] = KdlEntry::new(reference);
            documents.push(stage_document(output_root, &name, &hash, &path, &bytes)?);
        }
    }
    if let Some(children) = node.children_mut() {
        for child in children.nodes_mut() {
            rewrite_copy_nodes(child, catalog_root, output_root, relative, documents)?;
        }
    }
    let _ = relative;
    Ok(())
}

fn rewrite_catalog_text(source: &str) -> String {
    source
        .split_inclusive('\n')
        .map(|line| {
            if line.contains("Catalog selection on every catalog-aware command:") {
                return "st3 selects its daemon through --endpoint or ST3_ENDPOINT.\n".into();
            }
            if line.contains("Bus ops retain --root as a legacy") {
                return String::new();
            }
            if line.trim_start().starts_with("${XDG_STATE_HOME:")
                || (line.contains("Bus ops retain") && line.contains("--root"))
            {
                return String::new();
            }
            line.replace("`st2` CLI", "`st3` CLI")
                .replace("The host (`st2 up`)", "The host (`st3 up`)")
                .replace("st2 message", "st3 message")
                .replace("st2 status", "st3 status")
                .replace("st2 agents", "st3 agents")
                .replace("st2 context", "st3 context")
                .replace("st2 resource", "st3 resource")
                .replace("st2 bus", "st3 graph message API")
                .replace("st2 boot ritual", "st3 boot ritual")
        })
        .collect()
}

fn transform_eval(
    spec: &EvalSpec,
    cell: &Path,
    output_cell: &Path,
    host: &str,
) -> Result<(String, Vec<DocumentReport>)> {
    let (mut legacy, documents) = transform_eval_checkpoint(spec, cell, output_cell, host)?;
    let eval = spec.eval.as_ref().context("eval is missing")?;
    let mut identities = spec
        .agents
        .iter()
        .chain(eval.agents.iter())
        .map(|agent| eval_agent_identity(&agent.id, host))
        .filter(|identity| identity != "requester")
        .collect::<Vec<_>>();
    identities.sort_by_key(|identity| std::cmp::Reverse(identity.len()));
    identities.dedup();
    for identity in identities {
        let run_identity = format!("{identity}.${{PLAN_RUN}}");
        legacy = legacy.replace(&identity, &run_identity);
        for field in ["agent", "identity", "from", "to"] {
            legacy = legacy.replace(
                &format!("{field} {run_identity}"),
                &format!("{field} {run_identity:?}"),
            );
        }
    }
    legacy = legacy.replace("${EVAL_ROOT}", "${WORKSPACE}");
    let name = cell
        .file_name()
        .and_then(|value| value.to_str())
        .context("eval name is not UTF-8")?;
    Ok((checkpoint_intent_to_plan(&legacy, name)?, documents))
}

fn transform_eval_checkpoint(
    spec: &EvalSpec,
    cell: &Path,
    output_cell: &Path,
    host: &str,
) -> Result<(String, Vec<DocumentReport>)> {
    let eval = spec.eval.as_ref().context("eval is missing")?;
    let name = cell
        .file_name()
        .and_then(|value| value.to_str())
        .context("eval name is not UTF-8")?;
    let scope = format!("scope/eval/{name}");
    let sequence = format!("eval/{name}");
    let restart = if eval.supervise { "always" } else { "never" };
    let mut documents = Vec::new();
    let mut output = String::new();
    output.push_str("version 2\nsubgraph {\n");
    output.push_str(&format!("  checkpoints {sequence:?} scope={scope:?} {{\n"));
    let mut team_checkpoint = String::new();
    team_checkpoint.push_str("    checkpoint \"The eval team is running\" {\n      subgraph {\n");
    let all_agents = spec
        .agents
        .iter()
        .chain(eval.agents.iter())
        .collect::<Vec<_>>();
    let has_claude = all_agents
        .iter()
        .any(|agent| matches!(agent.driver.as_ref(), Some(Driver::Claude(_))));
    let has_development_channel = all_agents.iter().any(|agent| {
        matches!(
            agent.driver.as_ref(),
            Some(Driver::Claude(driver)) if driver.dev_channels
        )
    });
    let eval_supervisor = has_claude.then(|| format!("eval-{name}"));
    if let Some(supervisor) = &eval_supervisor {
        write_eval_supervisor(&mut team_checkpoint, supervisor, has_development_channel);
    }
    for agent in spec.agents.iter().chain(eval.agents.iter()) {
        write_eval_agent(
            &mut team_checkpoint,
            agent,
            restart,
            host,
            eval_supervisor.as_deref(),
        );
    }
    if let Some(kick) = &eval.message {
        let content = if cell.join(&kick.content).is_file() {
            let path = cell.join(&kick.content);
            let bytes = fs::read(&path)?;
            let hash = hex::encode(Sha256::digest(&bytes));
            let doc_name = format!("doc/evals/{name}/task");
            documents.push(stage_document(
                output_cell,
                &doc_name,
                &hash,
                &path,
                &bytes,
            )?);
            format!("{doc_name}@{hash}")
        } else {
            kick.content.clone()
        };
        team_checkpoint.push_str(&format!(
            "        message \"kickoff/${{PLAN_RUN}}\" {{\n          from {:?}\n          to {:?}\n          content {:?}\n        }}\n",
            eval_agent_identity(&kick.from, host),
            eval_agent_identity(&kick.to, host),
            content
        ));
    }
    team_checkpoint.push_str("      }\n      judges {\n");
    let agents = spec
        .agents
        .iter()
        .chain(eval.agents.iter())
        .collect::<Vec<_>>();
    if agents.is_empty() {
        team_checkpoint.push_str(&format!("        exists {scope:?}\n"));
    } else {
        for agent in agents {
            let subject = format!("agent/{}", eval_agent_identity(&agent.id, host));
            if agent.driver.is_some() {
                // The checkpoint reconciler requires a native driver to reach ready, working, or
                // idle before it evaluates this explicit existence predicate.
                team_checkpoint.push_str(&format!("        exists {subject:?}\n"));
            } else {
                team_checkpoint.push_str(&format!(
                    "        field \"status\" {subject:?} \"is\" \"running\"\n"
                ));
            }
        }
    }
    team_checkpoint.push_str(&format!(
        "        deadline {:?}\n",
        format!("{}ms", eval.max_timeout.as_millis())
    ));
    team_checkpoint.push_str("      }\n    }\n");

    for (ordinal, step) in eval.run_steps.iter().enumerate() {
        let subject = format!("eval/{name}/run/{ordinal}-{}", step.id);
        output.push_str(&format!(
            "    checkpoint {:?} {{\n",
            format!("Run step {} finishes", step.id)
        ));
        output.push_str("      subgraph {\n");
        output.push_str(&format!("        exec {subject:?} {{\n"));
        output.push_str(&format!("          host {host:?}\n"));
        output.push_str(&format!(
            "          workspace {:?}\n          cwd {:?}\n          command {:?}\n          restart \"never\"\n",
            "${EVAL_ROOT}",
            step.workspace.as_deref().unwrap_or("${EVAL_ROOT}"),
            rewrite_bus_command(&step.command)
        ));
        output.push_str("          env {\n");
        for (key, value) in &step.env {
            if key != "ST_ROOT" && key != "CATALOG" && key != "ST3_MESSAGE_ROOT" {
                output.push_str(&format!("            {key} {value:?}\n"));
            }
        }
        output.push_str("            CATALOG \"${EVAL_ROOT}\"\n");
        output.push_str("            ST_ROOT \"${EVAL_ROOT}/.st3-messages\"\n");
        output.push_str("            ST3_MESSAGE_ROOT \"${EVAL_ROOT}/.st3-messages\"\n");
        output.push_str("          }\n");
        if !step.unset.is_empty() {
            output.push_str("          unset");
            for name in &step.unset {
                output.push_str(&format!(" {name:?}"));
            }
            output.push('\n');
        }
        output.push_str("        }\n      }\n      judges {\n");
        output.push_str(&format!(
            "        field \"status\" {:?} \"is\" \"exited\"\n",
            format!("exec/{subject}")
        ));
        if !step.allow_nonzero {
            output.push_str(&format!(
                "        field \"exit_code\" {:?} \"is\" 0\n",
                format!("exec/{subject}")
            ));
        }
        output.push_str(&format!(
            "        deadline {:?}\n",
            format!("{}ms", eval.max_timeout.as_millis())
        ));
        output.push_str("      }\n    }\n");
    }

    output.push_str(&team_checkpoint);
    if let Some(kick) = &eval.message {
        let generated = output_cell.join(".st3-migration/wait-team-done.sh");
        write_file(&generated, WAIT_TEAM_DONE)?;
        let supervisor = eval_agent_identity(&kick.to, host);
        let workers = spec
            .agents
            .iter()
            .map(|agent| eval_agent_identity(&agent.id, host))
            .filter(|identity| identity != &supervisor)
            .collect::<Vec<_>>();
        write_team_completion_checkpoint(
            &mut output,
            &eval_agent_identity(&kick.from, host),
            &supervisor,
            &workers,
            host,
            eval.max_timeout.as_millis() as u64,
        );
    }

    output.push_str("    checkpoint \"All held-out judges pass\" {\n");
    let signal_judges = eval
        .judges
        .iter()
        .enumerate()
        .filter(|(_, judge)| judge.signal)
        .collect::<Vec<_>>();
    if !signal_judges.is_empty() {
        output.push_str("      subgraph {\n");
        for (ordinal, judge) in signal_judges {
            let command = match &judge.kind {
                JudgeKind::Bash(command) => rewrite_bus_command(command),
                JudgeKind::Declarative(checks) => declarative_command(checks),
                JudgeKind::Ask { .. } => {
                    anyhow::bail!(
                        "signal ask judge {:?} needs a manual translation",
                        judge.name
                    )
                }
            };
            output.push_str(&format!(
                "        exec {:?} {{ host {host:?}; workspace \"${{EVAL_ROOT}}\"; command {command:?}; restart \"never\"; env {{ CATALOG \"${{EVAL_ROOT}}\"; ST_ROOT \"${{EVAL_ROOT}}/.st3-messages\"; ST3_MESSAGE_ROOT \"${{EVAL_ROOT}}/.st3-messages\" }} }}\n",
                format!("eval/{name}/signal/{ordinal}")
            ));
        }
        output.push_str("      }\n");
    }
    output.push_str("      judges {\n");
    let mut gating_judges = 0usize;
    for judge in eval.judges.iter().filter(|judge| !judge.signal) {
        gating_judges += 1;
        match &judge.kind {
            JudgeKind::Bash(command) => write_mechanical_judge(
                &mut output,
                &judge.name,
                &rewrite_bus_command(command),
                host,
                judge.timeout.unwrap_or(eval.max_timeout).as_millis() as u64,
            ),
            JudgeKind::Declarative(checks) => {
                let command = declarative_command(checks);
                write_mechanical_judge(
                    &mut output,
                    &judge.name,
                    &command,
                    host,
                    judge.timeout.unwrap_or(eval.max_timeout).as_millis() as u64,
                );
            }
            JudgeKind::Ask { agent, prompt } => {
                let model = eval
                    .agents
                    .iter()
                    .find(|candidate| &candidate.id == agent)
                    .and_then(infer_agent_model)
                    .unwrap_or_else(|| "gpt-5.6-sol".into());
                output.push_str(&format!("        judge {:?} type=\"llm\" {{\n", judge.name));
                output.push_str(&format!(
                    "          model {model:?}\n          host {host:?}\n          workspace \"${{EVAL_ROOT}}\"\n          tools \"shell\" \"git\"\n          env {{ CATALOG \"${{EVAL_ROOT}}\"; ST_ROOT \"${{EVAL_ROOT}}/.st3-messages\"; ST3_MESSAGE_ROOT \"${{EVAL_ROOT}}/.st3-messages\" }}\n          token-budget 8192\n          time-limit {:?}\n          prompt {:?}\n",
                    format!("{}ms", judge.timeout.unwrap_or(eval.max_timeout).as_millis()),
                    prompt
                ));
                output.push_str("        }\n");
            }
        }
    }
    if gating_judges == 0 {
        write_mechanical_judge(
            &mut output,
            "The non-gating signals were recorded",
            "true",
            host,
            1_000,
        );
    }
    output.push_str(&format!(
        "        deadline {:?}\n",
        format!("{}ms", eval.max_timeout.as_millis())
    ));
    output.push_str("      }\n    }\n");
    output.push_str("    checkpoint \"The temporary eval scope is empty\" {\n      subgraph {\n");
    output.push_str(&format!(
        "        scope {:?} {{ stop }}\n",
        format!("eval/{name}")
    ));
    output.push_str(&format!(
        "      }}\n      judges {{ empty {scope:?} }}\n    }}\n"
    ));
    output.push_str("  }\n}\n");
    let mut formatted: KdlDocument = output
        .parse()
        .with_context(|| format!("parse migrated plan KDL:\n{output}"))?;
    formatted.autoformat();
    Ok((formatted.to_string(), documents))
}

fn checkpoint_intent_to_plan(source: &str, name: &str) -> Result<String> {
    let document: KdlDocument = source
        .parse()
        .with_context(|| format!("parse legacy checkpoint KDL after harness rewrite:\n{source}"))?;
    let root = document
        .nodes()
        .iter()
        .find(|node| node.name().value() == "subgraph")
        .context("translated eval has no subgraph root")?;
    let checkpoints = root
        .children()
        .and_then(|children| {
            children
                .nodes()
                .iter()
                .find(|node| node.name().value() == "checkpoints")
        })
        .context("translated eval has no checkpoint sequence")?;
    let stages = checkpoints
        .children()
        .context("translated checkpoint sequence is empty")?;
    let mut output = format!(
        "version 2\nsubgraph {{\n  scope {:?} retention=\"temporary\" change-policy=\"agent\" {{\n    plan {:?} state=\"ready\" {{\n",
        format!("eval/{name}/${{PLAN_RUN}}"),
        format!("eval/{name}")
    );
    let mut prior = None::<String>;
    for (ordinal, checkpoint) in stages.nodes().iter().enumerate() {
        let title = checkpoint
            .entries()
            .iter()
            .find(|entry| entry.name().is_none())
            .and_then(|entry| entry.value().as_string())
            .context("translated checkpoint has no title")?;
        let cleanup = title == "The temporary eval scope is empty";
        let id = if cleanup {
            "cleanup".into()
        } else {
            format!("{:02}-{}", ordinal, slug(title))
        };
        let mut timeout = None::<String>;
        let mut body_nodes = Vec::new();
        if let Some(body) = checkpoint.children() {
            for child in body.nodes() {
                let mut child = child.clone();
                if child.name().value() == "judges"
                    && let Some(judges) = child.children_mut()
                {
                    for deadline in judges
                        .nodes()
                        .iter()
                        .filter(|node| node.name().value() == "deadline")
                    {
                        timeout = deadline
                            .entries()
                            .iter()
                            .find(|entry| entry.name().is_none())
                            .and_then(|entry| entry.value().as_string())
                            .map(str::to_owned);
                    }
                    judges
                        .nodes_mut()
                        .retain(|node| node.name().value() != "deadline");
                }
                body_nodes.push(child);
            }
        }
        output.push_str(&format!("      step {id:?}"));
        if let Some(timeout) = timeout {
            output.push_str(&format!(" timeout={timeout:?}"));
        }
        if cleanup {
            output.push_str(" finally=#true");
        }
        output.push_str(" {\n");
        output.push_str(&format!("        title {title:?}\n"));
        if !cleanup && let Some(prior) = &prior {
            output.push_str(&format!(
                "        depends-on {{ step {prior:?} completed }}\n"
            ));
        }
        for child in body_nodes {
            if child.name().value() == "judges"
                && child
                    .children()
                    .is_some_and(|children| children.nodes().is_empty())
            {
                continue;
            }
            output.push_str(&child.to_string());
            output.push('\n');
        }
        output.push_str("      }\n");
        if !cleanup {
            prior = Some(id);
        }
    }
    output.push_str("    }\n  }\n}\n");
    let mut formatted: KdlDocument = output
        .parse()
        .with_context(|| format!("parse checkpoint conversion KDL:\n{output}"))?;
    formatted.autoformat();
    Ok(formatted.to_string())
}

fn slug(value: &str) -> String {
    let mut output = String::new();
    let mut dash = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            if dash && !output.is_empty() {
                output.push('-');
            }
            output.push(character.to_ascii_lowercase());
            dash = false;
        } else {
            dash = true;
        }
    }
    output.trim_matches('-').chars().take(80).collect()
}

fn write_eval_agent(
    output: &mut String,
    agent: &st2::eval_spec::SpecAgent,
    restart: &str,
    host: &str,
    supervisor: Option<&str>,
) {
    output.push_str(&format!("    agent {:?} {{\n", agent.id));
    if !agent.id.contains('.') {
        output.push_str(&format!(
            "      identity {:?}\n",
            eval_agent_identity(&agent.id, host)
        ));
    }
    if let Some(workspace) = &agent.workspace {
        output.push_str(&format!(
            "      workspace {:?}\n",
            format!("${{EVAL_ROOT}}/{workspace}")
        ));
    } else {
        output.push_str("      workspace \"${EVAL_ROOT}\"\n");
    }
    if let Some(supervisor) = supervisor {
        output.push_str(&format!("      supervisor {supervisor:?}\n"));
    }
    if let Some(command) = &agent.command {
        output.push_str(&format!(
            "      command {:?}\n",
            rewrite_bus_command(command)
        ));
    }
    if let Some(driver) = &agent.driver {
        write_eval_driver(output, driver);
    }
    output.push_str(&format!("      restart {restart:?}\n"));
    output.push_str("      env {\n");
    for (key, value) in &agent.env {
        if key != "ST_ROOT" && key != "CATALOG" && key != "ST3_MESSAGE_ROOT" {
            output.push_str(&format!("        {key} {:?}\n", rewrite_bus_command(value)));
        }
    }
    output.push_str("        CATALOG \"${EVAL_ROOT}\"\n");
    output.push_str("        ST_ROOT \"${EVAL_ROOT}/.st3-messages\"\n");
    output.push_str("        ST3_MESSAGE_ROOT \"${EVAL_ROOT}/.st3-messages\"\n");
    output.push_str("      }\n");
    for exec in &agent.execs {
        if exec.derived {
            continue;
        }
        let leaf = exec.id.rsplit('.').next().unwrap_or(&exec.id);
        output.push_str(&format!(
            "      exec {leaf:?} {{ command {:?} }}\n",
            rewrite_bus_command(&exec.command)
        ));
    }
    output.push_str("    }\n");
}

fn write_eval_supervisor(output: &mut String, supervisor: &str, development_channel: bool) {
    output.push_str(&format!("        supervisor {supervisor:?} {{\n"));
    output.push_str(
        "          gate \"claude-workspace-trust\" driver=\"claude\" {\n\
                   contains \"Quick safety check: Is this a project you created or one you trust?\"\n\
                   key \"enter\"\n\
                   max-inputs 1\n\
                   }\n",
    );
    if development_channel {
        output.push_str(
            "          gate \"claude-development-channel\" driver=\"claude\" {\n\
                       contains \"WARNING: Loading development channels\"\n\
                       contains \"Channels: server:st3\"\n\
                       key \"enter\"\n\
                       max-inputs 1\n\
                       }\n",
        );
    }
    output.push_str("        }\n");
}

fn write_eval_driver(output: &mut String, driver: &Driver) {
    let (name, model, effort, prompt, args, dev_channels) = match driver {
        Driver::Claude(driver) => (
            "claude",
            driver.model.as_deref(),
            driver.effort.as_deref(),
            driver.prompt.as_str(),
            driver.args.as_slice(),
            Some(driver.dev_channels),
        ),
        Driver::Codex(driver) => (
            "codex",
            driver.model.as_deref(),
            driver.effort.as_deref(),
            driver.prompt.as_str(),
            driver.args.as_slice(),
            None,
        ),
        Driver::Pi(_) | Driver::OpenCode(_) | Driver::Omp(_) => {
            unreachable!("the compact eval grammar accepts only Claude and Codex drivers")
        }
    };
    output.push_str(&format!("      harness {name:?} {{\n"));
    if let Some(model) = model {
        output.push_str(&format!("        model {model:?}\n"));
    }
    if let Some(effort) = effort {
        output.push_str(&format!("        effort {effort:?}\n"));
    }
    if dev_channels == Some(true) {
        output.push_str("        dev-channels #true\n");
    }
    output.push_str(&format!(
        "        prompt {:?}\n",
        rewrite_eval_prompt(prompt)
    ));
    if !args.is_empty() {
        output.push_str("        args");
        for arg in args {
            output.push_str(&format!(" {arg:?}"));
        }
        output.push('\n');
    }
    output.push_str("      }\n");
}

fn eval_agent_identity(identity: &str, host: &str) -> String {
    if identity.contains('.') || identity.contains('/') || identity == "requester" {
        identity.into()
    } else {
        format!("{host}.{identity}")
    }
}

fn write_team_completion_checkpoint(
    output: &mut String,
    requester: &str,
    supervisor: &str,
    workers: &[String],
    host: &str,
    timeout_ms: u64,
) {
    let mut command = format!(
        "TIMEOUT_SECONDS={} bash ./.st3-migration/wait-team-done.sh {} {} kickoff/${{PLAN_RUN}}",
        timeout_ms.div_ceil(1_000),
        shell(requester),
        shell(supervisor)
    );
    for worker in workers {
        command.push(' ');
        command.push_str(&shell(worker));
    }
    output.push_str("    checkpoint \"The team reported completion\" {\n      judges {\n");
    let judge_name = if workers.is_empty() {
        "The supervisor confirmed after kickoff"
    } else {
        "Every worker reported before the supervisor confirmed"
    };
    write_raw_mechanical_judge(
        output,
        judge_name,
        &command,
        host,
        timeout_ms.saturating_add(5_000),
    );
    output.push_str("      }\n    }\n");
}

fn write_mechanical_judge(
    output: &mut String,
    name: &str,
    command: &str,
    host: &str,
    timeout_ms: u64,
) {
    let command =
        format!("st3 message export \"${{EVAL_ROOT}}/.st3-messages\" >/dev/null && {command}");
    write_raw_mechanical_judge(output, name, &command, host, timeout_ms);
}

fn write_raw_mechanical_judge(
    output: &mut String,
    name: &str,
    command: &str,
    host: &str,
    timeout_ms: u64,
) {
    output.push_str(&format!("        judge {name:?} {{\n"));
    output.push_str(&format!(
        "          exec {command:?}\n          host {host:?}\n          workspace \"${{EVAL_ROOT}}\"\n          env {{ CATALOG \"${{EVAL_ROOT}}\"; ST_ROOT \"${{EVAL_ROOT}}/.st3-messages\"; ST3_MESSAGE_ROOT \"${{EVAL_ROOT}}/.st3-messages\" }}\n          time-limit {:?}\n",
        format!("{timeout_ms}ms")
    ));
    output.push_str("        }\n");
}

fn declarative_command(checks: &[Check]) -> String {
    let mut commands = vec!["set -eu".to_owned()];
    for check in checks {
        match check {
            Check::FileHas { path, text } => {
                commands.push(format!(
                    "grep -F -- {} {} >/dev/null",
                    shell(text),
                    shell(path)
                ));
            }
            Check::FileLacks { path, text } => {
                commands.push(format!(
                    "! grep -F -- {} {} >/dev/null",
                    shell(text),
                    shell(path)
                ));
            }
            Check::JsonField { path, field, value } => {
                let value = match value {
                    JsonScalar::String(value) => serde_json::to_string(value).unwrap(),
                    JsonScalar::Bool(value) => value.to_string(),
                    JsonScalar::Integer(value) => value.to_string(),
                };
                commands.push(format!(
                    "test \"$(jq -c {} {})\" = {}",
                    shell(&format!(".{field}")),
                    shell(path),
                    shell(&value)
                ));
            }
            Check::Committed { path } => commands.push(format!(
                "git -C {} diff --quiet --exit-code && git -C {} diff --cached --quiet --exit-code",
                shell(path),
                shell(path)
            )),
        }
    }
    commands.join("; ")
}

fn infer_model(command: &str) -> Option<String> {
    command
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .find(|pair| pair[0] == "--model")
        .map(|pair| pair[1].trim_matches(['\'', '"']).to_owned())
}

fn infer_agent_model(agent: &st2::eval_spec::SpecAgent) -> Option<String> {
    match agent.driver.as_ref() {
        Some(Driver::Claude(driver)) => driver.model.clone(),
        Some(Driver::Codex(driver)) => driver.model.clone(),
        Some(Driver::Pi(_)) | Some(Driver::OpenCode(_)) | Some(Driver::Omp(_)) => None,
        None => agent.command.as_deref().and_then(infer_model),
    }
}

fn rewrite_bus_command(value: &str) -> String {
    value
        .replace("st2 message", "st3 message")
        .replace("st2 channel message", "st3 graph message notification")
        .replace("st2 bus", "st3 graph message API")
        .replace("hermetic st2 eval", "hermetic st3 eval")
}

fn rewrite_eval_prompt(value: &str) -> String {
    let mut output = rewrite_bus_command(value).replace(
        "wait for an st3 graph message notification",
        "end the turn and stay idle",
    );
    if !output.ends_with(char::is_whitespace) {
        output.push(' ');
    }
    output.push_str(
        "After an empty inbox drain, end the turn and stay idle. Do not run a blocking wait, trace, poll, or sleep command. The native driver will start a new turn when a message arrives.",
    );
    output
}

fn clear_generated_assets(output_root: &Path) -> Result<()> {
    for name in [".st3-documents", ".st3-migration"] {
        let generated = output_root.join(name);
        match fs::remove_dir_all(&generated) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("remove stale output {}", generated.display()));
            }
        }
    }
    Ok(())
}

fn stage_document(
    output_root: &Path,
    name: &str,
    hash: &str,
    source: &Path,
    bytes: &[u8],
) -> Result<DocumentReport> {
    std::str::from_utf8(bytes).context("st3 documents must contain UTF-8 text")?;
    let staged = output_root.join(".st3-documents").join(hash);
    write_file(&staged, bytes)?;
    Ok(DocumentReport {
        name: name.into(),
        hash: hash.into(),
        source: source.display().to_string(),
        staged: staged.display().to_string(),
        put_command: format!(
            "st3 doc put {} --as {}",
            shell(&staged.display().to_string()),
            shell(name)
        ),
    })
}

fn copy_eval_assets(
    cell: &Path,
    output: &Path,
    input_kdl: &Path,
    fixture: Option<&str>,
) -> Result<()> {
    let fixture = fixture
        .map(|fixture| {
            let mut relative = PathBuf::new();
            for component in Path::new(fixture).components() {
                match component {
                    std::path::Component::CurDir => {}
                    std::path::Component::Normal(component) => relative.push(component),
                    _ => anyhow::bail!("eval copy path must stay inside its eval directory"),
                }
            }
            anyhow::ensure!(!relative.as_os_str().is_empty(), "eval copy path is empty");
            let source = cell.join(relative);
            anyhow::ensure!(
                source.is_dir(),
                "eval copy source {} is not a directory",
                source.display()
            );
            Ok(source)
        })
        .transpose()?;
    for entry in WalkDir::new(cell).follow_links(false) {
        let entry = entry?;
        if entry.path() == cell
            || entry.path() == input_kdl
            || fixture
                .as_ref()
                .is_some_and(|fixture| entry.path().starts_with(fixture))
        {
            continue;
        }
        let relative = entry.path().strip_prefix(cell)?;
        copy_eval_asset(entry.path(), &output.join(relative))?;
    }
    if let Some(fixture) = fixture {
        for entry in WalkDir::new(&fixture).min_depth(1).follow_links(false) {
            let entry = entry?;
            let relative = entry.path().strip_prefix(&fixture)?;
            copy_eval_asset(entry.path(), &output.join(relative))?;
        }
    }
    Ok(())
}

fn copy_eval_asset(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    anyhow::ensure!(
        !metadata.file_type().is_symlink(),
        "eval contains a symlink"
    );
    if metadata.is_dir() {
        fs::create_dir_all(target)?;
    } else if metadata.is_file() {
        let bytes = fs::read(source)?;
        let bytes = match std::str::from_utf8(&bytes) {
            Ok(text) => rewrite_eval_asset(text).into_bytes(),
            Err(_) => bytes,
        };
        write_file(target, &bytes)?;
    } else {
        anyhow::bail!("eval contains special file {}", source.display());
    }
    Ok(())
}

fn rewrite_eval_asset(source: &str) -> String {
    source
        .split_inclusive('\n')
        .map(|line| {
            if line.contains("--catalog") {
                line.to_owned()
            } else {
                line.replace("st2 message", "st3 message")
                    .replace("st2 status", "st3 status")
                    .replace("st2 agents", "st3 agents")
                    .replace("st2 bus", "st3 graph message API")
            }
        })
        .collect()
}

fn validate_tree_args(args: &TreeArgs) -> Result<()> {
    anyhow::ensure!(
        args.input.is_dir(),
        "input {} is not a directory",
        args.input.display()
    );
    anyhow::ensure!(
        args.input != args.output,
        "the output must differ from the input"
    );
    anyhow::ensure!(
        !args.output.starts_with(&args.input),
        "the output cannot be inside the input tree"
    );
    Ok(())
}

fn new_report(mode: &str, args: &TreeArgs) -> Report {
    Report {
        schema: "st3-migrate-report.v1",
        mode: mode.into(),
        input: args.input.display().to_string(),
        output: args.output.display().to_string(),
        files: Vec::new(),
        documents: Vec::new(),
        warnings: Vec::new(),
    }
}

fn eval_definition_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut cells = fs::read_dir(root)?.collect::<Result<Vec<_>, _>>()?;
    cells.sort_by_key(|entry| entry.file_name());
    for cell in cells {
        if !cell.file_type()?.is_dir() {
            continue;
        }
        let mut candidates = fs::read_dir(cell.path())?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|entry| {
                entry.path().extension().and_then(|value| value.to_str()) == Some("kdl")
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        candidates.sort();
        anyhow::ensure!(
            candidates.len() == 1,
            "eval {} must contain exactly one top-level KDL file",
            cell.path().display()
        );
        files.push(candidates.remove(0));
    }
    Ok(files)
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

fn write_report(path: &Path, report: &Report) -> Result<()> {
    write_file(path, &serde_json::to_vec_pretty(report)?)
}

fn shell(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_completion_wait(
        supervisor_messages: &str,
        requester_messages: &str,
        workers: &[&str],
    ) -> std::process::ExitStatus {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let script = temporary.path().join("wait-team-done.sh");
        fs::write(&script, WAIT_TEAM_DONE).unwrap();
        let bin = temporary.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let st3 = bin.join("st3");
        fs::write(
            &st3,
            r#"#!/bin/sh
case "$3" in
  supervisor) printf '%s\n' "$SUPERVISOR_MESSAGES" ;;
  requester) printf '%s\n' "$REQUESTER_MESSAGES" ;;
  *) printf '%s\n' '[]' ;;
esac
"#,
        )
        .unwrap();
        fs::set_permissions(&st3, fs::Permissions::from_mode(0o755)).unwrap();
        let path = std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        )))
        .unwrap();
        std::process::Command::new("bash")
            .arg(script)
            .args(["requester", "supervisor", "kickoff"])
            .args(workers)
            .env("TIMEOUT_SECONDS", "0")
            .env("SUPERVISOR_MESSAGES", supervisor_messages)
            .env("REQUESTER_MESSAGES", requester_messages)
            .env("PATH", path)
            .status()
            .unwrap()
    }

    #[test]
    fn catalog_translation_wraps_old_agents_and_adds_restart_policy() {
        let translated = transform_declaration(
            r#"version 1
agent "worker" { host "host-a"; workspace "/work"; command "true" }"#,
            Some(true),
        )
        .unwrap();
        assert!(translated.starts_with("version 2\n"));
        let intent = st3::parse_intent(&translated, "local").unwrap();
        let worker = &intent.subjects["agent/host-a.worker"];
        assert_eq!(
            worker.member.as_ref().unwrap().restart,
            st3::model::RestartType::Always
        );
    }

    #[test]
    fn catalog_translation_rewrites_typed_drivers_to_harness_blocks() {
        let translated = transform_declaration(
            r#"
agent "worker" {
  host "host-a"
  harness "claude"
  codex {
    model "gpt-5.6-sol"
    effort "medium"
    prompt "Do the work."
  }
}
"#,
            Some(true),
        )
        .unwrap();
        let intent = st3::parse_intent(&translated, "local").unwrap();
        let member = intent.subjects["agent/host-a.worker"]
            .member
            .as_ref()
            .unwrap();
        assert_eq!(member.driver.as_deref(), Some("codex"));
        assert!(translated.contains("harness codex"));
        assert!(!translated.contains("harness claude"));
    }

    #[test]
    fn retired_catalog_agents_become_explicit_stops() {
        let translated = transform_declaration(
            r#"agent "worker" { host "host-a"; workspace "/work"; command "true" }"#,
            Some(false),
        )
        .unwrap();
        let intent = st3::parse_intent(&translated, "local").unwrap();
        assert_eq!(intent.subjects["agent/host-a.worker"].kind, "stop");
    }

    #[test]
    fn claude_hook_cleanup_preserves_unrelated_hooks() {
        let source = serde_json::json!({
            "hooks": {
                "Stop": [{"hooks": [
                    {"type": "command", "command": "\"$ST_HOOKS/claude-observe.sh\" Stop"},
                    {"type": "command", "command": "echo keep"}
                ]}]
            }
        })
        .to_string();
        let cleaned = clean_claude_hook_json(&source).unwrap();
        assert!(!cleaned.contains("$ST_HOOKS/claude-"));
        assert!(cleaned.contains("echo keep"));
    }

    #[test]
    fn eval_translation_qualifies_short_agent_identities() {
        assert_eq!(eval_agent_identity("worker", "node-a"), "node-a.worker");
        assert_eq!(eval_agent_identity("team.worker", "node-a"), "team.worker");
        assert_eq!(eval_agent_identity("requester", "node-a"), "requester");
    }

    #[test]
    fn team_completion_requires_every_worker_report() {
        let status = run_completion_wait(
            r#"[{"from":"agent/worker.one","created_index":10}]"#,
            r#"[{"from":"agent/supervisor","created_index":20}]"#,
            &["worker.one", "worker.two"],
        );
        assert!(!status.success());
    }

    #[test]
    fn team_completion_requires_confirmation_after_the_latest_report() {
        let reports = r#"[
            {"from":"agent/worker.one","created_index":10},
            {"from":"agent/worker.two","created_index":15}
        ]"#;
        let early = run_completion_wait(
            reports,
            r#"[{"from":"agent/supervisor","created_index":14}]"#,
            &["worker.one", "worker.two"],
        );
        assert!(!early.success());

        let final_confirmation = run_completion_wait(
            reports,
            r#"[{"from":"agent/supervisor","created_index":16}]"#,
            &["worker.one", "worker.two"],
        );
        assert!(final_confirmation.success());
    }

    #[test]
    fn eval_translation_preserves_native_driver_declarations() {
        let source = r#"
            team "mix" {
              agent "sup" {
                workspace "./sup"
                claude {
                  model "claude-sonnet-5"
                  effort "medium"
                  prompt "Coordinate the task."
                  args "--permission-mode" "bypassPermissions"
                }
              }
            }
            eval {
              message { from "requester"; to "mix.sup"; content "Do the work." }
              max-timeout "60s"
              agent "judge" {
                codex {
                  model "gpt-5.6-sol"
                  effort "medium"
                  prompt "Judge the result."
                }
              }
              judges {
                judge "review" { ask "judge" "Check the result." }
              }
            }
        "#;
        let spec = st2::eval_spec::parse_spec(source).unwrap();
        let cell = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let (translated, documents) =
            transform_eval(&spec, cell.path(), output.path(), "local").unwrap();
        let intent = st3::parse_intent(&translated, "local").unwrap();

        assert!(documents.is_empty());
        assert!(translated.starts_with("version 2\n"));
        assert!(translated.contains("harness claude {"));
        assert!(translated.contains("harness codex {"));
        assert!(!translated.contains("command \"exec claude"));
        let plan = intent.plans.values().next().unwrap();
        let team = &plan.steps["00-the-eval-team-is-running"];
        let team_graph = team.subgraph_kdl.as_deref().unwrap();
        assert!(team_graph.contains("agent \"mix.sup.${PLAN_RUN}\""));
        assert!(team_graph.contains("identity \"local.judge.${PLAN_RUN}\""));
        assert!(team_graph.contains("message \"kickoff/${PLAN_RUN}\""));
        assert!(translated.contains("model gpt-5.6-sol"));
        assert!(team.judges.iter().any(|judge| {
            matches!(
                judge,
                st3::model::JudgeSpec::Exists { subject } if subject == "agent/mix.sup.${PLAN_RUN}"
            )
        }));
        assert!(translated.contains("title \"The team reported completion\""));
        assert!(translated.contains(".st3-migration/wait-team-done.sh"));
        assert!(translated.contains("kickoff/${PLAN_RUN}"));
        assert!(translated.contains("supervisor eval-"));
        assert!(translated.contains("gate claude-workspace-trust"));
        assert!(
            !translated.contains("wait-team-done.sh 'requester' 'mix.sup' kickoff 'local.judge'")
        );
        assert!(
            output
                .path()
                .join(".st3-migration/wait-team-done.sh")
                .is_file()
        );
    }

    #[test]
    fn eval_run_steps_finish_before_the_team_starts_and_judges_wait_for_completion() {
        let source = r#"
            agent "sup" { command "sleep 60" }
            eval {
              run "setup" { command "true" }
              message { from "requester"; to "sup"; content "Do the work." }
              max-timeout "60s"
              judges { judge "result" { exec "true" } }
            }
        "#;
        let spec = st2::eval_spec::parse_spec(source).unwrap();
        let cell = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let (translated, _) = transform_eval(&spec, cell.path(), output.path(), "local").unwrap();
        st3::parse_intent(&translated, "local").unwrap();

        let run = translated
            .find("title \"Run step setup finishes\"")
            .unwrap();
        let team = translated
            .find("title \"The eval team is running\"")
            .unwrap();
        let completion = translated
            .find("title \"The team reported completion\"")
            .unwrap();
        let judges = translated
            .find("title \"All held-out judges pass\"")
            .unwrap();
        assert!(run < team && team < completion && completion < judges);
    }

    #[test]
    fn eval_copy_contents_become_the_runtime_root() {
        let cell = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        fs::create_dir_all(cell.path().join("fixture/sup")).unwrap();
        fs::create_dir_all(cell.path().join("judges")).unwrap();
        fs::write(
            cell.path().join("fixture/sup/CLAUDE.md"),
            "Use st2 message.\n",
        )
        .unwrap();
        fs::write(cell.path().join("judges/grade.sh"), "st2 status\n").unwrap();
        let input = cell.path().join("cell.kdl");
        fs::write(&input, "eval { copy \"./fixture\" }").unwrap();

        copy_eval_assets(cell.path(), output.path(), &input, Some("./fixture")).unwrap();

        assert_eq!(
            fs::read_to_string(output.path().join("sup/CLAUDE.md")).unwrap(),
            "Use st3 message.\n"
        );
        assert_eq!(
            fs::read_to_string(output.path().join("judges/grade.sh")).unwrap(),
            "st3 status\n"
        );
        assert!(!output.path().join("fixture").exists());
    }

    #[test]
    fn eval_prompt_rewrite_removes_st2_channel_language() {
        let translated = rewrite_eval_prompt(
            "In a hermetic st2 eval, use st2 message and wait for an st2 channel message.",
        );
        assert!(translated.starts_with(
            "In a hermetic st3 eval, use st3 message and end the turn and stay idle."
        ));
        assert!(translated.contains("Do not run a blocking wait, trace, poll, or sleep command."));
        assert!(!translated.contains("st2"));
    }

    #[test]
    fn migration_refresh_removes_stale_staged_documents() {
        let output = tempfile::tempdir().unwrap();
        let stage = output.path().join(".st3-documents");
        fs::create_dir_all(&stage).unwrap();
        fs::write(stage.join("old-hash"), "old").unwrap();

        clear_generated_assets(output.path()).unwrap();
        clear_generated_assets(output.path()).unwrap();

        assert!(!stage.exists());
    }
}
