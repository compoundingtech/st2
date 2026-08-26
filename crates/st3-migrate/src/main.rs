use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use clap::{Args, Parser, Subcommand};
use kdl::{KdlDocument, KdlEntry, KdlNode};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use st2::eval_spec::{Check, JsonScalar, JudgeKind, Spec as EvalSpec};
use walkdir::WalkDir;

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
        copy_eval_assets(cell, &output_cell, &input)?;
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
    let document: KdlDocument = source.parse()?;
    if document.nodes().len() == 1 && document.nodes()[0].name().value() == "subgraph" {
        st3::parse_intent(source, "local")?;
        return Ok(source.to_owned());
    }
    anyhow::ensure!(!document.nodes().is_empty(), "the declaration is empty");
    let mut children = KdlDocument::new();
    for node in document.nodes() {
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
            )
        });
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
    output.nodes_mut().push(root);
    output.autoformat();
    Ok(rewrite_catalog_text(&output.to_string()))
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
    let eval = spec.eval.as_ref().context("eval is missing")?;
    let name = cell
        .file_name()
        .and_then(|value| value.to_str())
        .context("eval cell name is not UTF-8")?;
    let scope = format!("scope/eval/{name}");
    let sequence = format!("eval/{name}");
    let restart = if eval.supervise { "always" } else { "never" };
    let mut documents = Vec::new();
    let mut output = String::new();
    output.push_str("subgraph {\n");
    output.push_str(&format!("  checkpoints {sequence:?} scope={scope:?} {{\n"));
    output.push_str("    checkpoint \"The eval team is running\" {\n      subgraph {\n");
    for agent in spec.agents.iter().chain(eval.agents.iter()) {
        write_eval_agent(&mut output, agent, restart, host);
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
        output.push_str(&format!(
            "        message \"kickoff\" {{\n          from {:?}\n          to {:?}\n          content {:?}\n        }}\n",
            eval_agent_identity(&kick.from, host),
            eval_agent_identity(&kick.to, host),
            content
        ));
    }
    output.push_str("      }\n      judges {\n");
    let agents = spec
        .agents
        .iter()
        .chain(eval.agents.iter())
        .collect::<Vec<_>>();
    if agents.is_empty() {
        output.push_str(&format!("        exists {scope:?}\n"));
    } else {
        for agent in agents {
            output.push_str(&format!(
                "        field \"status\" {:?} \"is\" \"running\"\n",
                format!("agent/{}", eval_agent_identity(&agent.id, host))
            ));
        }
    }
    output.push_str(&format!(
        "        deadline {:?}\n",
        format!("{}ms", eval.max_timeout.as_millis())
    ));
    output.push_str("      }\n    }\n");

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
                    .and_then(|candidate| infer_model(&candidate.command))
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
    Ok((output, documents))
}

fn write_eval_agent(
    output: &mut String,
    agent: &st2::eval_spec::SpecAgent,
    restart: &str,
    host: &str,
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
    output.push_str(&format!(
        "      command {:?}\n      restart {restart:?}\n",
        rewrite_bus_command(&agent.command)
    ));
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

fn eval_agent_identity(identity: &str, host: &str) -> String {
    if identity.contains('.') || identity.contains('/') || identity == "requester" {
        identity.into()
    } else {
        format!("{host}.{identity}")
    }
}

fn write_mechanical_judge(
    output: &mut String,
    name: &str,
    command: &str,
    host: &str,
    timeout_ms: u64,
) {
    let command =
        format!("st3 message export \"${{EVAL_ROOT}}/.st3-messages\" >/dev/null; {command}");
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

fn rewrite_bus_command(value: &str) -> String {
    value
        .replace("st2 message", "st3 message")
        .replace("st2 bus", "st3 graph message API")
        .replace("hermetic st2 eval", "hermetic st3 eval")
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
        put_command: format!("st3 doc put {} --as {}", staged.display(), name),
    })
}

fn copy_eval_assets(cell: &Path, output: &Path, input_kdl: &Path) -> Result<()> {
    for entry in WalkDir::new(cell).follow_links(false) {
        let entry = entry?;
        if entry.path() == cell || entry.path() == input_kdl {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "eval contains a symlink"
        );
        let relative = entry.path().strip_prefix(cell)?;
        let target = output.join(relative);
        if metadata.is_dir() {
            fs::create_dir_all(&target)?;
        } else if metadata.is_file() {
            let bytes = fs::read(entry.path())?;
            let bytes = match std::str::from_utf8(&bytes) {
                Ok(text) => rewrite_eval_asset(text).into_bytes(),
                Err(_) => bytes,
            };
            write_file(&target, &bytes)?;
        } else {
            anyhow::bail!("eval contains special file {}", entry.path().display());
        }
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
            "eval cell {} must contain exactly one top-level KDL file",
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

    #[test]
    fn catalog_translation_wraps_old_agents_and_adds_restart_policy() {
        let translated = transform_declaration(
            r#"agent "worker" { host "host-a"; workspace "/work"; command "true" }"#,
            Some(true),
        )
        .unwrap();
        let intent = st3::parse_intent(&translated, "local").unwrap();
        let worker = &intent.subjects["agent/host-a.worker"];
        assert_eq!(
            worker.member.as_ref().unwrap().restart,
            st3::model::RestartType::Always
        );
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
}
