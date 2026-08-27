use std::collections::{BTreeMap, BTreeSet, HashSet};

use kdl::{KdlDocument, KdlNode, KdlValue};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};

use crate::model::{
    CheckpointActivation, CheckpointSpec, DesiredSubject, GateSpec, JudgeSpec, LaunchSpec,
    LinkSpec, MemberKind, MemberLifecycle, MemberSpec, MessageTemplate, NormalizedIntent,
    RestartIntensity, RestartType, ScheduleSpec, St3Error,
};

const ROOT_NODES: &[&str] = &[
    "agent",
    "exec",
    "pty",
    "scope",
    "host",
    "resource",
    "person",
    "account",
    "supervisor",
    "link",
    "plan",
    "message",
    "schedule",
    "stop",
];

struct ParseContext {
    default_host: String,
    subjects: BTreeMap<String, DesiredSubject>,
    checkpoints: Vec<CheckpointSpec>,
    document_refs: BTreeSet<String>,
    checkpoint: Option<CheckpointActivation>,
    scopes: BTreeSet<String>,
}

pub fn parse_intent(source: &str, default_host: &str) -> Result<NormalizedIntent, St3Error> {
    if source.len() > 16 * 1024 * 1024 {
        return Err(St3Error::new(
            "intent-too-large",
            "an intent cannot exceed 16 MiB",
        ));
    }
    let document: KdlDocument = source
        .parse::<KdlDocument>()
        .map_err(|error| St3Error::new("invalid-kdl", error.to_string()))?;
    let [root] = document.nodes() else {
        return Err(St3Error::new(
            "invalid-root",
            "an st3 intent must contain exactly one root node",
        ));
    };
    if root.name().value() != "subgraph" || root.ty().is_some() || !root.entries().is_empty() {
        return Err(St3Error::new(
            "invalid-root",
            "an st3 intent must use one untyped `subgraph` root with no values",
        ));
    }
    let children = root.children().ok_or_else(|| {
        St3Error::new(
            "empty-subgraph",
            "the root subgraph must contain desired state",
        )
    })?;
    if children.nodes().is_empty() {
        return Err(St3Error::new(
            "empty-subgraph",
            "the root subgraph must contain desired state",
        ));
    }

    let plans = crate::plan::parse_plans(root, default_host)?;
    let mut context = ParseContext {
        default_host: default_host.to_owned(),
        subjects: BTreeMap::new(),
        checkpoints: Vec::new(),
        document_refs: BTreeSet::new(),
        checkpoint: None,
        scopes: BTreeSet::new(),
    };
    for node in children.nodes() {
        parse_desired_node(node, None, &mut context)?;
    }
    collect_document_refs(root, &mut context.document_refs)?;
    validate_links(&context.subjects)?;

    let normalized_nodes = children
        .nodes()
        .iter()
        .map(canonical_node)
        .collect::<Result<Vec<_>, _>>()?;
    let normalized = json!({ "subgraph": normalized_nodes });
    let source_hash = hash_json(&normalized);
    Ok(NormalizedIntent {
        schema: "st3.v1".into(),
        source_hash,
        subjects: context.subjects,
        checkpoints: context.checkpoints,
        plans,
        document_refs: context.document_refs,
        normalized,
    })
}

pub fn resolve_document_references(
    source: &str,
    bindings: &BTreeMap<String, String>,
) -> Result<String, St3Error> {
    let mut document = source
        .parse::<KdlDocument>()
        .map_err(|error| St3Error::new("invalid-kdl", error.to_string()))?;
    for node in document.nodes_mut() {
        resolve_node_documents(node, bindings);
    }
    document.autoformat();
    Ok(document.to_string())
}

fn resolve_node_documents(node: &mut KdlNode, bindings: &BTreeMap<String, String>) {
    for entry in node.entries_mut() {
        let replacement = match entry.value() {
            KdlValue::String(value) if value.starts_with("doc/") && !value.contains('@') => {
                bindings.get(value).map(|hash| format!("{value}@{hash}"))
            }
            _ => None,
        };
        if let Some(replacement) = replacement {
            entry.set_value(replacement);
        }
    }
    if let Some(children) = node.children_mut() {
        for child in children.nodes_mut() {
            resolve_node_documents(child, bindings);
        }
    }
}

fn parse_desired_node(
    node: &KdlNode,
    enclosing_host: Option<&str>,
    context: &mut ParseContext,
) -> Result<(), St3Error> {
    reject_type(node)?;
    let kind = node.name().value();
    if !ROOT_NODES.contains(&kind) {
        return Err(St3Error::new(
            "unknown-node",
            format!("unknown desired-state node `{kind}`"),
        ));
    }
    match kind {
        "host" => parse_host(node, context),
        "scope" => parse_scope(node, enclosing_host, context),
        "agent" => parse_agent(node, enclosing_host, context),
        "exec" | "pty" => parse_standalone_member(node, kind, enclosing_host, context),
        "plan" => Ok(()),
        "stop" => parse_stop(node, context),
        _ => parse_structure(node, kind, context),
    }
}

fn parse_host(node: &KdlNode, context: &mut ParseContext) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    let name = one_string_with_children(node)?;
    validate_name(&name, false)?;
    let subject = format!("host/{name}");
    insert_subject(
        context,
        DesiredSubject {
            subject,
            kind: "host".into(),
            desired: canonical_node(node)?,
            member: None,
            activation: context.checkpoint.clone(),
            scopes: context.scopes.clone(),
        },
    )?;
    if let Some(children) = node.children() {
        for child in children.nodes() {
            if !matches!(child.name().value(), "agent" | "exec" | "pty") {
                return Err(St3Error::new(
                    "invalid-host-child",
                    format!("host `{name}` cannot contain `{}`", child.name().value()),
                ));
            }
            parse_desired_node(child, Some(&name), context)?;
        }
    }
    Ok(())
}

fn parse_scope(
    node: &KdlNode,
    enclosing_host: Option<&str>,
    context: &mut ParseContext,
) -> Result<(), St3Error> {
    let name = one_string_with_children(node)?;
    let plan_only = node.children().is_some_and(|children| {
        !children.nodes().is_empty()
            && children
                .nodes()
                .iter()
                .all(|child| child.name().value() == "plan")
    });
    if plan_only {
        return Ok(());
    }
    if node.children().is_some_and(|children| {
        children
            .nodes()
            .iter()
            .any(|child| child.name().value() == "plan")
    }) {
        return Err(St3Error::new(
            "mixed-plan-scope",
            "a plan scope cannot mix plan definitions with immediate desired state",
        ));
    }
    validate_name(&name, false)?;
    ensure_only_properties(node, &["retention"])?;
    let retention = property_string(node, "retention")?.unwrap_or_else(|| "persistent".into());
    if !matches!(retention.as_str(), "persistent" | "temporary") {
        return Err(St3Error::new(
            "invalid-scope-retention",
            format!("scope `{name}` has invalid retention `{retention}`"),
        ));
    }
    let subject = namespaced("scope", &name);
    let is_stop = node.children().is_some_and(|children| {
        children.nodes().len() == 1 && children.nodes()[0].name().value() == "stop"
    });
    insert_subject(
        context,
        DesiredSubject {
            subject: subject.clone(),
            kind: if is_stop { "scope-stop" } else { "scope" }.into(),
            desired: canonical_node(node)?,
            member: None,
            activation: context.checkpoint.clone(),
            scopes: context.scopes.clone(),
        },
    )?;
    let Some(children) = node.children() else {
        return Err(St3Error::new(
            "empty-scope",
            format!("scope `{name}` must contain members or `stop`"),
        ));
    };
    if is_stop {
        ensure_bare(&children.nodes()[0])?;
        return Ok(());
    }
    if children
        .nodes()
        .iter()
        .any(|child| child.name().value() == "stop")
    {
        return Err(St3Error::new(
            "mixed-scope-stop",
            format!("scope `{name}` cannot mix `stop` with members"),
        ));
    }
    let desired_children = children
        .nodes()
        .iter()
        .filter(|child| child.name().value() != "plan")
        .collect::<Vec<_>>();
    if desired_children.is_empty() {
        return Ok(());
    }
    let prior_scopes = context.scopes.clone();
    context.scopes.insert(subject);
    for child in desired_children {
        if !matches!(
            child.name().value(),
            "agent" | "exec" | "pty" | "resource" | "message" | "stop"
        ) {
            return Err(St3Error::new(
                "invalid-scope-child",
                format!("scope `{name}` cannot contain `{}`", child.name().value()),
            ));
        }
        parse_desired_node(child, enclosing_host, context)?;
    }
    context.scopes = prior_scopes;
    Ok(())
}

fn parse_agent(
    node: &KdlNode,
    enclosing_host: Option<&str>,
    context: &mut ParseContext,
) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    let node_name = one_string_with_children(node)?;
    let children = node.children().ok_or_else(|| {
        St3Error::new(
            "missing-agent-body",
            format!("agent `{node_name}` has no body"),
        )
    })?;
    validate_agent_body(children, &node_name)?;
    let identity = child_string(children, "identity")?.unwrap_or(node_name);
    let host = child_string(children, "host")?
        .or_else(|| enclosing_host.map(str::to_owned))
        .unwrap_or_else(|| context.default_host.clone());
    let host = placement_host(host, &context.default_host);
    let bus_id = if identity.contains('.') {
        identity.clone()
    } else {
        format!("{host}.{identity}")
    };
    validate_name(&bus_id, false)?;
    let subject = format!("agent/{bus_id}");
    let workspace = child_string(children, "workspace")?.unwrap_or_else(|| ".".into());
    let environment = parse_map_child(children, "env")?;
    let display_name = child_string(children, "name")?;
    let supervisor = child_string(children, "supervisor")?
        .map(|name| namespaced("supervisor", &name))
        .unwrap_or_else(|| "supervisor/root".into());
    let lifecycle = parse_lifecycle(child_string(children, "lifecycle")?)?;
    let restart = parse_restart_type(restart_type_value(children)?)?;
    let restart_intensity = parse_restart_intensity(children)?;
    let shutdown_timeout_ms = child_string(children, "shutdown-timeout")?
        .map(|value| parse_duration(&value, true))
        .transpose()?
        .unwrap_or(5_000);

    let driver_nodes = children
        .nodes()
        .iter()
        .filter(|child| child.name().value() == "harness")
        .collect::<Vec<_>>();
    let command = child_string(children, "command")?;
    let argv = child_strings(children, "argv")?;
    if driver_nodes.len() > 1 || command.is_some() as usize + argv.is_some() as usize > 1 {
        return Err(St3Error::new(
            "multiple-agent-launches",
            format!("agent `{bus_id}` has multiple compact launches or drivers"),
        ));
    }
    if !driver_nodes.is_empty() && (command.is_some() || argv.is_some()) {
        return Err(St3Error::new(
            "multiple-agent-launches",
            format!("agent `{bus_id}` mixes a driver and a compact launch"),
        ));
    }

    let mut primary = None;
    if let Some(driver) = driver_nodes.first() {
        primary = Some(driver_member(
            driver,
            &subject,
            &bus_id,
            &host,
            &workspace,
            &environment,
            display_name.clone(),
            lifecycle.clone(),
            restart.clone(),
            restart_intensity.clone(),
            shutdown_timeout_ms,
            &supervisor,
        )?);
    } else if let Some(launch) = compact_launch(command, argv)? {
        primary = Some(MemberSpec {
            kind: MemberKind::Agent,
            host: host.clone(),
            runtime_id: bus_id.clone(),
            workspace: workspace.clone(),
            cwd: workspace.clone(),
            terminal: true,
            launch,
            environment: environment.clone(),
            tags: BTreeMap::new(),
            display_name: display_name.clone(),
            lifecycle: lifecycle.clone(),
            restart: restart.clone(),
            restart_intensity: restart_intensity.clone(),
            shutdown_timeout_ms,
            driver: None,
            supervisor: supervisor.clone(),
        });
    }

    insert_subject(
        context,
        DesiredSubject {
            subject: subject.clone(),
            kind: "agent".into(),
            desired: canonical_node(node)?,
            member: primary,
            activation: context.checkpoint.clone(),
            scopes: context.scopes.clone(),
        },
    )?;

    let mut task_names = HashSet::new();
    for child in children.nodes() {
        if !matches!(child.name().value(), "pty" | "exec") {
            continue;
        }
        let task_name = first_string(child)?;
        if !task_names.insert(task_name.clone()) {
            return Err(St3Error::new(
                "duplicate-task",
                format!("agent `{bus_id}` repeats task `{task_name}`"),
            ));
        }
        let task_subject = format!("{}/{bus_id}/{task_name}", child.name().value());
        let member = task_member(
            child,
            &task_subject,
            child.name().value() == "pty",
            &host,
            &workspace,
            &environment,
            lifecycle.clone(),
            restart.clone(),
            restart_intensity.clone(),
            shutdown_timeout_ms,
            false,
            &supervisor,
        )?;
        insert_subject(
            context,
            DesiredSubject {
                subject: task_subject,
                kind: child.name().value().into(),
                desired: canonical_node(child)?,
                member: Some(member),
                activation: context.checkpoint.clone(),
                scopes: context.scopes.clone(),
            },
        )?;
    }
    if context
        .subjects
        .get(&subject)
        .is_some_and(|subject| subject.member.is_none())
        && task_names.is_empty()
    {
        return Err(St3Error::new(
            "missing-agent-launch",
            format!("agent `{bus_id}` has no launch"),
        ));
    }
    Ok(())
}

fn parse_standalone_member(
    node: &KdlNode,
    kind: &str,
    enclosing_host: Option<&str>,
    context: &mut ParseContext,
) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    let name = one_string_with_children(node)?;
    validate_name(&name, false)?;
    let subject = namespaced(kind, &name);
    let children = node.children().ok_or_else(|| {
        St3Error::new(
            "missing-member-body",
            format!("{kind} `{name}` has no body"),
        )
    })?;
    let host = child_string(children, "host")?
        .or_else(|| enclosing_host.map(str::to_owned))
        .unwrap_or_else(|| context.default_host.clone());
    let host = placement_host(host, &context.default_host);
    let workspace = child_string(children, "workspace")?.unwrap_or_else(|| ".".into());
    let environment = parse_map_child(children, "env")?;
    let lifecycle = parse_lifecycle(child_string(children, "lifecycle")?)?;
    let restart = parse_restart_type(restart_type_value(children)?)?;
    let restart_intensity = parse_restart_intensity(children)?;
    let supervisor = child_string(children, "supervisor")?
        .map(|name| namespaced("supervisor", &name))
        .unwrap_or_else(|| "supervisor/root".into());
    let shutdown_timeout_ms = child_string(children, "shutdown-timeout")?
        .map(|value| parse_duration(&value, true))
        .transpose()?
        .unwrap_or(5_000);
    let member = task_member(
        node,
        &subject,
        kind == "pty",
        &host,
        &workspace,
        &environment,
        lifecycle,
        restart,
        restart_intensity,
        shutdown_timeout_ms,
        true,
        &supervisor,
    )?;
    insert_subject(
        context,
        DesiredSubject {
            subject,
            kind: kind.into(),
            desired: canonical_node(node)?,
            member: Some(member),
            activation: context.checkpoint.clone(),
            scopes: context.scopes.clone(),
        },
    )
}

fn parse_structure(node: &KdlNode, kind: &str, context: &mut ParseContext) -> Result<(), St3Error> {
    let name = first_string(node)?;
    validate_name(&name, false)?;
    match kind {
        "resource" => validate_resource(node)?,
        "person" => {
            ensure_no_properties(node)?;
            one_string(node)?;
        }
        "account" => validate_account(node)?,
        "supervisor" => validate_supervisor(node)?,
        "link" => validate_link(node)?,
        "message" => validate_message(node)?,
        "schedule" => validate_schedule(node)?,
        _ => unreachable!("the desired-state registry controls structure kinds"),
    }
    let subject = match kind {
        "resource" if name.starts_with("doc/") || name.starts_with("file/") => name,
        _ => namespaced(kind, &name),
    };
    insert_subject(
        context,
        DesiredSubject {
            subject,
            kind: kind.into(),
            desired: canonical_node(node)?,
            member: None,
            activation: context.checkpoint.clone(),
            scopes: context.scopes.clone(),
        },
    )
}

fn parse_stop(node: &KdlNode, context: &mut ParseContext) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    ensure_no_children(node)?;
    let subject = one_string(node)?;
    if !matches!(subject.split('/').next(), Some("agent" | "exec" | "pty")) {
        return Err(St3Error::new(
            "invalid-stop-subject",
            format!("stop requires a full agent, exec, or PTY subject; got `{subject}`"),
        ));
    }
    insert_subject(
        context,
        DesiredSubject {
            subject: subject.clone(),
            kind: "stop".into(),
            desired: json!({ "stop": subject }),
            member: None,
            activation: context.checkpoint.clone(),
            scopes: context.scopes.clone(),
        },
    )
}

#[allow(dead_code)]
fn parse_checkpoints(node: &KdlNode, context: &mut ParseContext) -> Result<(), St3Error> {
    if context.checkpoint.is_some() {
        return Err(St3Error::new(
            "nested-checkpoints",
            "a checkpoint subgraph cannot contain checkpoints",
        ));
    }
    ensure_only_properties(node, &["scope"])?;
    let name = one_string_with_children(node)?;
    let sequence = namespaced("checkpoint", &name);
    let sequence_subject = sequence.clone();
    let sequence_scope = property_string(node, "scope")?;
    if let Some(scope) = &sequence_scope {
        if !scope.starts_with("scope/") {
            return Err(St3Error::new(
                "invalid-checkpoint-scope",
                "a checkpoint scope must be a full scope subject",
            ));
        }
        validate_name(scope, true)?;
    }
    insert_subject(
        context,
        DesiredSubject {
            subject: sequence_subject,
            kind: "checkpoints".into(),
            desired: canonical_node(node)?,
            member: None,
            activation: None,
            scopes: sequence_scope.clone().into_iter().collect(),
        },
    )?;
    let children = node.children().ok_or_else(|| {
        St3Error::new(
            "empty-checkpoints",
            format!("checkpoint sequence `{name}` is empty"),
        )
    })?;
    let mut names = HashSet::new();
    for (ordinal, checkpoint) in children.nodes().iter().enumerate() {
        if checkpoint.name().value() != "checkpoint" {
            return Err(St3Error::new(
                "invalid-checkpoint-child",
                format!(
                    "checkpoint sequence `{name}` contains `{}`",
                    checkpoint.name().value()
                ),
            ));
        }
        ensure_no_properties(checkpoint)?;
        let checkpoint_name = one_string_with_children(checkpoint)?;
        if checkpoint_name.is_empty() || checkpoint_name.len() > 160 {
            return Err(St3Error::new(
                "invalid-checkpoint-name",
                "a checkpoint name must contain 1 through 160 bytes",
            ));
        }
        if !names.insert(checkpoint_name.clone()) {
            return Err(St3Error::new(
                "duplicate-checkpoint",
                format!("checkpoint sequence `{name}` repeats `{checkpoint_name}`"),
            ));
        }
        let body = checkpoint.children().ok_or_else(|| {
            St3Error::new(
                "missing-checkpoint-body",
                format!("checkpoint `{checkpoint_name}` has no body"),
            )
        })?;
        let subgraphs = body
            .nodes()
            .iter()
            .filter(|child| child.name().value() == "subgraph")
            .collect::<Vec<_>>();
        let judges_nodes = body
            .nodes()
            .iter()
            .filter(|child| child.name().value() == "judges")
            .collect::<Vec<_>>();
        if subgraphs.len() > 1 || judges_nodes.len() != 1 || body.nodes().len() > 2 {
            return Err(St3Error::new(
                "invalid-checkpoint-shape",
                format!(
                    "checkpoint `{checkpoint_name}` needs one judges block and at most one subgraph"
                ),
            ));
        }
        let judges = parse_judges(judges_nodes[0], &context.default_host)?;
        if judges.is_empty() {
            return Err(St3Error::new(
                "empty-judges",
                format!("checkpoint `{checkpoint_name}` has no judges"),
            ));
        }
        let activation = CheckpointActivation {
            sequence: sequence.clone(),
            ordinal: ordinal as u32,
        };
        if let Some(subgraph) = subgraphs.first() {
            ensure_bare(subgraph)?;
            let body = subgraph.children().ok_or_else(|| {
                St3Error::new(
                    "empty-checkpoint-subgraph",
                    format!("checkpoint `{checkpoint_name}` has an empty subgraph"),
                )
            })?;
            let prior_activation = context.checkpoint.replace(activation.clone());
            let prior_scopes = context.scopes.clone();
            if let Some(scope) = &sequence_scope {
                context.scopes.insert(scope.clone());
            }
            for child in body.nodes() {
                if child.name().value() == "checkpoints" {
                    return Err(St3Error::new(
                        "nested-checkpoints",
                        "a checkpoint subgraph cannot contain checkpoints",
                    ));
                }
                parse_desired_node(child, None, context)?;
            }
            context.scopes = prior_scopes;
            context.checkpoint = prior_activation;
        }
        let checkpoint_spec = CheckpointSpec {
            subject: format!("checkpoint/{name}/{ordinal}"),
            sequence: sequence.clone(),
            name: checkpoint_name,
            ordinal: ordinal as u32,
            judges,
        };
        insert_subject(
            context,
            DesiredSubject {
                subject: checkpoint_spec.subject.clone(),
                kind: "checkpoint-stage".into(),
                desired: serde_json::to_value(&checkpoint_spec).map_err(|error| {
                    St3Error::new("internal", format!("normalize checkpoint: {error}"))
                })?,
                member: None,
                activation: None,
                scopes: sequence_scope.clone().into_iter().collect(),
            },
        )?;
        context.checkpoints.push(checkpoint_spec);
    }
    Ok(())
}

pub(crate) fn parse_judges(node: &KdlNode, default_host: &str) -> Result<Vec<JudgeSpec>, St3Error> {
    ensure_bare(node)?;
    let children = node.children().ok_or_else(|| {
        St3Error::new(
            "empty-judges",
            "a judges block must contain at least one judge",
        )
    })?;
    let mut output = Vec::new();
    let mut running_names = HashSet::new();
    let mut has_deadline = false;
    for child in children.nodes() {
        reject_type(child)?;
        match child.name().value() {
            "exists" => {
                ensure_no_properties(child)?;
                let subject = one_string(child)?;
                validate_full_subject(&subject)?;
                output.push(JudgeSpec::Exists { subject });
            }
            "empty" => {
                ensure_no_properties(child)?;
                let subject = one_string(child)?;
                if !subject.starts_with("scope/") {
                    return Err(St3Error::new(
                        "invalid-empty-subject",
                        "empty requires a full scope subject",
                    ));
                }
                validate_full_subject(&subject)?;
                output.push(JudgeSpec::Empty { subject });
            }
            "has" | "lacks" => {
                let values = positional_strings(child)?;
                if values.len() != 2 {
                    return Err(St3Error::new(
                        "invalid-text-predicate",
                        "has and lacks require a subject and text",
                    ));
                }
                validate_full_subject(&values[0])?;
                if !matches!(
                    values[0].split('/').next(),
                    Some("file" | "doc" | "message")
                ) {
                    return Err(St3Error::new(
                        "unsupported-predicate-subject",
                        "has and lacks require a file, document, or message subject",
                    ));
                }
                let judge = if child.name().value() == "has" {
                    JudgeSpec::Has {
                        subject: values[0].clone(),
                        text: values[1].clone(),
                    }
                } else {
                    JudgeSpec::Lacks {
                        subject: values[0].clone(),
                        text: values[1].clone(),
                    }
                };
                output.push(judge);
            }
            "field" => {
                ensure_no_properties(child)?;
                let entries = child
                    .entries()
                    .iter()
                    .filter(|entry| entry.name().is_none())
                    .map(|entry| entry.value())
                    .collect::<Vec<_>>();
                if entries.len() != 4 {
                    return Err(St3Error::new(
                        "invalid-field-predicate",
                        "field requires path, subject, operator, and value",
                    ));
                }
                let path = value_string(entries[0])?;
                if !valid_field_path(&path) {
                    return Err(St3Error::new(
                        "invalid-field-path",
                        format!("invalid field path `{path}`"),
                    ));
                }
                let operator = value_string(entries[2])?;
                if !matches!(operator.as_str(), "is" | "starts-with" | "contains") {
                    return Err(St3Error::new(
                        "invalid-field-operator",
                        format!("invalid field operator `{operator}`"),
                    ));
                }
                let subject = value_string(entries[1])?;
                validate_full_subject(&subject)?;
                output.push(JudgeSpec::Field {
                    path,
                    subject,
                    operator,
                    value: json_value(entries[3])?,
                });
            }
            "deadline" => {
                if has_deadline {
                    return Err(St3Error::new(
                        "duplicate-deadline",
                        "a judges block can contain one deadline",
                    ));
                }
                has_deadline = true;
                let duration = one_duration(child)?;
                output.push(JudgeSpec::Deadline {
                    duration_ms: duration,
                });
            }
            "judge" => {
                let name = first_string(child)?;
                if !running_names.insert(name.clone()) {
                    return Err(St3Error::new(
                        "duplicate-judge",
                        format!("running judge `{name}` repeats"),
                    ));
                }
                output.push(parse_running_judge(child, name, default_host)?);
            }
            "human" => {
                ensure_no_properties(child)?;
                let reviewer = one_string(child)?;
                if !reviewer.starts_with("person/") {
                    return Err(St3Error::new(
                        "invalid-human-reviewer",
                        "a human judge needs a full person subject",
                    ));
                }
                output.push(JudgeSpec::Human { reviewer });
            }
            other => {
                return Err(St3Error::new(
                    "unknown-judge",
                    format!("unknown judge `{other}`"),
                ));
            }
        }
    }
    Ok(output)
}

fn parse_running_judge(
    node: &KdlNode,
    name: String,
    default_host: &str,
) -> Result<JudgeSpec, St3Error> {
    ensure_only_properties(node, &["type"])?;
    let body = node.children().ok_or_else(|| {
        St3Error::new("missing-judge-body", format!("judge `{name}` has no body"))
    })?;
    let judge_type = property_string(node, "type")?;
    let allowed: &[&str] = match judge_type.as_deref() {
        None => &["exec", "host", "workspace", "env", "time-limit"],
        Some("llm") => &[
            "model",
            "host",
            "workspace",
            "tools",
            "env",
            "token-budget",
            "time-limit",
            "prompt",
        ],
        Some(other) => {
            return Err(St3Error::new(
                "invalid-judge-type",
                format!("judge `{name}` has invalid type `{other}`"),
            ));
        }
    };
    reject_unknown_children(body, allowed, "judge", &name)?;
    for child in allowed {
        unique_child(body, child)?;
    }
    let host = placement_host(required_child_string(body, "host", &name)?, default_host);
    let workspace = required_child_string(body, "workspace", &name)?;
    let environment = parse_map_child(body, "env")?;
    if let Some(env) = unique_child(body, "env")? {
        validate_string_map(env, true)?;
    }
    match judge_type.as_deref() {
        None => {
            let time_limit_ms = child_string(body, "time-limit")?
                .map(|value| parse_duration(&value, true))
                .transpose()?
                .unwrap_or(120_000);
            Ok(JudgeSpec::Mechanical {
                name: name.clone(),
                command: required_child_string(body, "exec", &name)?,
                host,
                workspace,
                environment,
                time_limit_ms,
            })
        }
        Some("llm") => {
            let tools = child_strings(body, "tools")?.ok_or_else(|| {
                St3Error::new("missing-judge-field", format!("judge `{name}` needs tools"))
            })?;
            let token_budget = child_integer(body, "token-budget")?.ok_or_else(|| {
                St3Error::new(
                    "missing-judge-field",
                    format!("judge `{name}` needs token-budget"),
                )
            })?;
            if token_budget <= 0 {
                return Err(St3Error::new(
                    "invalid-token-budget",
                    "an LLM token budget must be positive",
                ));
            }
            for tool in &tools {
                if !matches!(tool.as_str(), "shell" | "git" | "gh" | "network") {
                    return Err(St3Error::new(
                        "unsupported-capability",
                        format!("judge tool `{tool}` is not registered"),
                    ));
                }
            }
            let time_limit_ms = child_string(body, "time-limit")?
                .ok_or_else(|| {
                    St3Error::new(
                        "missing-judge-field",
                        format!("judge `{name}` needs time-limit"),
                    )
                })
                .and_then(|value| parse_duration(&value, true))?;
            Ok(JudgeSpec::Llm {
                name: name.clone(),
                model: required_child_string(body, "model", &name)?,
                host,
                workspace,
                tools,
                environment,
                token_budget: token_budget as u64,
                time_limit_ms,
                prompt: required_child_string(body, "prompt", &name)?,
            })
        }
        Some(_) => unreachable!("judge type was validated"),
    }
}

#[allow(clippy::too_many_arguments)]
fn driver_member(
    driver: &KdlNode,
    subject: &str,
    runtime_id: &str,
    host: &str,
    workspace: &str,
    environment: &BTreeMap<String, String>,
    display_name: Option<String>,
    lifecycle: MemberLifecycle,
    restart: RestartType,
    restart_intensity: RestartIntensity,
    shutdown_timeout_ms: u64,
    supervisor: &str,
) -> Result<MemberSpec, St3Error> {
    let name = one_string_with_children(driver)?;
    let children = driver.children().ok_or_else(|| {
        St3Error::new(
            "missing-driver-body",
            format!("harness `{name}` has no body"),
        )
    })?;
    let prompt = required_child_string(children, "prompt", &name)?;
    let model = child_string(children, "model")?;
    let effort = child_string(children, "effort")?;
    let extra = child_strings(children, "args")?.unwrap_or_default();
    let mut provider = vec![name.clone()];
    if name == "claude" {
        let mcp = serde_json::json!({
            "mcpServers": {
                "st3": {
                    "type": "stdio",
                    "command": "st3",
                    "args": ["driver", "claude-mcp", "--subject", subject]
                }
            }
        });
        provider.extend([
            "--mcp-config".into(),
            mcp.to_string(),
            "--strict-mcp-config".into(),
        ]);
        let dev_channels = unique_child(children, "dev-channels")?
            .map(one_bool)
            .transpose()?
            .unwrap_or(false);
        if dev_channels {
            provider.push("--dangerously-load-development-channels=server:st3".into());
        }
    }
    if let Some(model) = model {
        match name.as_str() {
            "codex" => provider.extend(["--model".into(), model]),
            _ => provider.extend(["--model".into(), model]),
        }
    }
    if let Some(effort) = effort {
        match name.as_str() {
            "codex" => provider.extend(["-c".into(), format!("model_reasoning_effort={effort}")]),
            "pi" => provider.extend(["--thinking".into(), effort]),
            "opencode" => {
                return Err(St3Error::new(
                    "invalid-driver-child",
                    "opencode does not accept effort",
                ));
            }
            _ => provider.extend(["--effort".into(), effort]),
        }
    }
    provider.extend(extra);
    match name.as_str() {
        "opencode" => provider.extend(["--prompt".into(), prompt]),
        _ => provider.push(prompt),
    }
    let mut wrapper = vec![
        "st3".into(),
        "driver".into(),
        name.clone(),
        "--subject".into(),
        subject.into(),
        "--".into(),
    ];
    wrapper.extend(provider);
    Ok(MemberSpec {
        kind: MemberKind::Agent,
        host: host.into(),
        runtime_id: runtime_id.into(),
        workspace: workspace.into(),
        cwd: workspace.into(),
        terminal: true,
        launch: LaunchSpec::Argv(wrapper),
        environment: environment.clone(),
        tags: BTreeMap::from([("st3.subject".into(), subject.into())]),
        display_name,
        lifecycle,
        restart,
        restart_intensity,
        shutdown_timeout_ms,
        driver: Some(name),
        supervisor: supervisor.into(),
    })
}

#[allow(clippy::too_many_arguments)]
fn task_member(
    node: &KdlNode,
    subject: &str,
    terminal: bool,
    default_host: &str,
    default_workspace: &str,
    default_environment: &BTreeMap<String, String>,
    default_lifecycle: MemberLifecycle,
    default_restart: RestartType,
    default_intensity: RestartIntensity,
    default_shutdown_timeout_ms: u64,
    standalone: bool,
    supervisor: &str,
) -> Result<MemberSpec, St3Error> {
    let body = node.children().ok_or_else(|| {
        St3Error::new(
            "missing-task-body",
            format!("member `{subject}` has no body"),
        )
    })?;
    validate_task_body(body, subject, standalone)?;
    let host = placement_host(
        child_string(body, "host")?.unwrap_or_else(|| default_host.into()),
        default_host,
    );
    let workspace = child_string(body, "workspace")?.unwrap_or_else(|| default_workspace.into());
    let cwd = child_string(body, "cwd")?.unwrap_or_else(|| workspace.clone());
    let runtime_id = child_string(body, "id")?.unwrap_or_else(|| runtime_id(subject));
    let mut environment = default_environment.clone();
    environment.extend(parse_map_child(body, "env")?);
    if let Some(unset) = child_strings(body, "unset")? {
        for name in unset {
            environment.remove(&name);
        }
    }
    let command = child_string(body, "command")?;
    let argv = child_strings(body, "argv")?;
    let launch = compact_launch(command, argv)?.ok_or_else(|| {
        St3Error::new(
            "missing-task-launch",
            format!("member `{subject}` needs command or argv"),
        )
    })?;
    let lifecycle = child_string(body, "lifecycle")?
        .map(|value| parse_lifecycle(Some(value)))
        .transpose()?
        .unwrap_or(default_lifecycle);
    let restart = restart_type_value(body)?
        .map(|value| parse_restart_type(Some(value)))
        .transpose()?
        .unwrap_or(default_restart);
    let restart_intensity = if has_block_child(body, "restart") {
        parse_restart_intensity(body)?
    } else {
        default_intensity
    };
    let shutdown_timeout_ms = child_string(body, "shutdown-timeout")?
        .map(|value| parse_duration(&value, true))
        .transpose()?
        .unwrap_or(default_shutdown_timeout_ms);
    Ok(MemberSpec {
        kind: if terminal {
            MemberKind::Pty
        } else {
            MemberKind::Exec
        },
        host,
        runtime_id,
        workspace,
        cwd,
        terminal,
        launch,
        environment,
        tags: parse_tags(body)?,
        display_name: None,
        lifecycle,
        restart,
        restart_intensity,
        shutdown_timeout_ms,
        driver: None,
        supervisor: supervisor.into(),
    })
}

fn compact_launch(
    command: Option<String>,
    argv: Option<Vec<String>>,
) -> Result<Option<LaunchSpec>, St3Error> {
    match (command, argv) {
        (Some(command), None) => Ok(Some(LaunchSpec::Shell(command))),
        (None, Some(argv)) if !argv.is_empty() => Ok(Some(LaunchSpec::Argv(argv))),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(St3Error::new(
            "multiple-launches",
            "a member cannot contain both command and argv",
        )),
        (None, Some(_)) => Err(St3Error::new("empty-argv", "argv needs a program")),
    }
}

fn parse_lifecycle(value: Option<String>) -> Result<MemberLifecycle, St3Error> {
    match value.as_deref() {
        None | Some("service") => Ok(MemberLifecycle::Service),
        Some("adopt-only") => Ok(MemberLifecycle::AdoptOnly),
        Some(value) => Err(St3Error::new(
            "invalid-lifecycle",
            format!("invalid lifecycle `{value}`"),
        )),
    }
}

fn parse_restart_type(value: Option<String>) -> Result<RestartType, St3Error> {
    match value.as_deref() {
        None | Some("always") => Ok(RestartType::Always),
        Some("on-failure") => Ok(RestartType::OnFailure),
        Some("never") => Ok(RestartType::Never),
        Some(value) => Err(St3Error::new(
            "invalid-restart",
            format!("invalid restart type `{value}`"),
        )),
    }
}

fn placement_host(host: String, default_host: &str) -> String {
    if host == "local" {
        default_host.into()
    } else {
        host
    }
}

fn parse_restart_intensity(document: &KdlDocument) -> Result<RestartIntensity, St3Error> {
    let nodes = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "restart" && node.children().is_some())
        .collect::<Vec<_>>();
    let Some(node) = nodes.first().copied() else {
        return Ok(RestartIntensity::default());
    };
    if nodes.len() > 1 {
        return Err(St3Error::new(
            "duplicate-child",
            "a restart intensity block repeats",
        ));
    }
    let body = node.children().expect("checked");
    let attempts = child_integer(body, "attempts")?.unwrap_or(3);
    if attempts <= 0 || attempts > u32::MAX as i128 {
        return Err(St3Error::new(
            "invalid-restart-attempts",
            "restart attempts must be a positive u32",
        ));
    }
    let interval_ms = child_string(body, "interval")?
        .map(|value| parse_duration(&value, true))
        .transpose()?
        .unwrap_or(60_000);
    let delay_ms = child_string(body, "delay")?
        .map(|value| parse_duration(&value, false))
        .transpose()?
        .unwrap_or(0);
    let mode = child_string(body, "mode")?.unwrap_or_else(|| "delay".into());
    if !matches!(mode.as_str(), "delay" | "fail") {
        return Err(St3Error::new(
            "invalid-restart-mode",
            format!("invalid restart mode `{mode}`"),
        ));
    }
    Ok(RestartIntensity {
        attempts: attempts as u32,
        interval_ms,
        delay_ms,
        mode,
    })
}

fn restart_type_value(document: &KdlDocument) -> Result<Option<String>, St3Error> {
    let nodes = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "restart" && node.children().is_none())
        .collect::<Vec<_>>();
    match nodes.as_slice() {
        [] => Ok(None),
        [node] => one_string(node).map(Some),
        _ => Err(St3Error::new("duplicate-child", "a restart type repeats")),
    }
}

fn validate_agent_body(document: &KdlDocument, owner: &str) -> Result<(), St3Error> {
    const ALLOWED: &[&str] = &[
        "identity",
        "name",
        "description",
        "role",
        "type",
        "host",
        "workspace",
        "supervisor",
        "keep",
        "lifecycle",
        "restart",
        "shutdown-timeout",
        "deliver",
        "command",
        "argv",
        "ding",
        "env",
        "meta",
        "render",
        "harness",
        "pty",
        "exec",
        "resource",
        "stream",
    ];
    reject_unknown_children(document, ALLOWED, "agent", owner)?;
    for child in [
        "identity",
        "name",
        "description",
        "role",
        "type",
        "host",
        "workspace",
        "supervisor",
        "keep",
        "lifecycle",
        "shutdown-timeout",
        "deliver",
        "command",
        "argv",
        "ding",
        "env",
        "meta",
        "render",
        "harness",
    ] {
        unique_child(document, child)?;
    }
    validate_restart_forms(document)?;
    if let Some(value) = child_string(document, "type")?
        && value != "service"
    {
        return Err(St3Error::new(
            "invalid-agent-type",
            "an agent type must be `service`",
        ));
    }
    if let Some(node) = unique_child(document, "keep")? {
        one_bool(node)?;
    }
    if let Some(name) = child_string(document, "name")?
        && name.len() > 160
    {
        return Err(St3Error::new(
            "display-name-too-long",
            "an agent display name cannot exceed 160 bytes",
        ));
    }
    if let Some(description) = child_string(document, "description")?
        && description.len() > 1_000
    {
        return Err(St3Error::new(
            "description-too-long",
            "an agent description cannot exceed 1,000 bytes",
        ));
    }
    if let Some(deliver) = child_string(document, "deliver")? {
        if !matches!(deliver.as_str(), "mcp" | "app-server" | "pi-channel") {
            return Err(St3Error::new(
                "invalid-delivery",
                format!("invalid delivery transport `{deliver}`"),
            ));
        }
        if document
            .nodes()
            .iter()
            .any(|node| node.name().value() == "harness")
        {
            return Err(St3Error::new(
                "multiple-agent-launches",
                "a typed driver cannot occur with `deliver`",
            ));
        }
    }
    if let Some(ding) = unique_child(document, "ding")? {
        ensure_bare(ding)?;
        ensure_no_children(ding)?;
        if child_string(document, "deliver")?.is_some() {
            return Err(St3Error::new(
                "invalid-ding",
                "`ding` cannot occur with `deliver`",
            ));
        }
    }
    if let Some(env) = unique_child(document, "env")? {
        validate_string_map(env, true)?;
    }
    if let Some(meta) = unique_child(document, "meta")? {
        validate_scalar_map(meta)?;
    }
    if let Some(render) = unique_child(document, "render")? {
        validate_render(render)?;
    }
    for driver in document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "harness")
    {
        validate_driver(driver)?;
    }
    for resource in document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "resource")
    {
        validate_agent_resource(resource)?;
    }
    for stream in document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "stream")
    {
        validate_stream(stream)?;
    }
    Ok(())
}

fn validate_task_body(
    document: &KdlDocument,
    owner: &str,
    standalone: bool,
) -> Result<(), St3Error> {
    let mut allowed = vec![
        "id",
        "command",
        "argv",
        "cwd",
        "keep",
        "lifecycle",
        "tags",
        "env",
        "unset",
    ];
    if standalone {
        allowed.extend([
            "host",
            "workspace",
            "supervisor",
            "restart",
            "shutdown-timeout",
            "render",
        ]);
    }
    reject_unknown_children(document, &allowed, "member", owner)?;
    for child in [
        "id",
        "command",
        "argv",
        "cwd",
        "keep",
        "lifecycle",
        "tags",
        "env",
        "unset",
    ] {
        unique_child(document, child)?;
    }
    if standalone {
        for child in [
            "host",
            "workspace",
            "supervisor",
            "shutdown-timeout",
            "render",
        ] {
            unique_child(document, child)?;
        }
        validate_restart_forms(document)?;
    }
    if let Some(node) = unique_child(document, "keep")? {
        one_bool(node)?;
    }
    if let Some(tags) = unique_child(document, "tags")? {
        validate_tags(tags)?;
    }
    if let Some(env) = unique_child(document, "env")? {
        validate_string_map(env, true)?;
    }
    let unset = child_strings(document, "unset")?.unwrap_or_default();
    for name in &unset {
        validate_environment_name(name)?;
    }
    let environment = parse_map_child(document, "env")?;
    if let Some(name) = unset.iter().find(|name| environment.contains_key(*name)) {
        return Err(St3Error::new(
            "environment-set-and-unset",
            format!("member `{owner}` both sets and unsets `{name}`"),
        ));
    }
    if let Some(render) = unique_child(document, "render")? {
        validate_render(render)?;
    }
    Ok(())
}

fn validate_restart_forms(document: &KdlDocument) -> Result<(), St3Error> {
    let nodes = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "restart")
        .collect::<Vec<_>>();
    let scalar = nodes
        .iter()
        .filter(|node| node.children().is_none())
        .count();
    let block = nodes
        .iter()
        .filter(|node| node.children().is_some())
        .count();
    if scalar > 1 || block > 1 || nodes.len() > 2 {
        return Err(St3Error::new(
            "duplicate-child",
            "a restart type or intensity block repeats",
        ));
    }
    if let Some(node) = nodes.iter().find(|node| node.children().is_some()) {
        ensure_no_properties(node)?;
        if !positional_values(node).is_empty() {
            return Err(St3Error::new(
                "unexpected-value",
                "a restart intensity block cannot have values",
            ));
        }
        let body = node.children().expect("selected a block");
        reject_unknown_children(
            body,
            &["attempts", "interval", "delay", "mode"],
            "restart",
            "restart",
        )?;
        for child in ["attempts", "interval", "delay", "mode"] {
            unique_child(body, child)?;
        }
    }
    Ok(())
}

fn validate_driver(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    let provider = one_string_with_children(node)?;
    let body = node.children().ok_or_else(|| {
        St3Error::new(
            "missing-driver-body",
            format!("harness `{provider}` has no body"),
        )
    })?;
    let allowed: &[&str] = match provider.as_str() {
        "claude" => &["model", "effort", "dev-channels", "prompt", "args"],
        "codex" | "pi" => &["model", "effort", "prompt", "args"],
        "opencode" => &["model", "prompt", "args"],
        _ => return Err(St3Error::new("unknown-driver", "unknown typed driver")),
    };
    reject_unknown_children(body, allowed, "harness", &provider)?;
    for child in allowed {
        unique_child(body, child)?;
    }
    required_child_string(body, "prompt", &provider)?;
    if let Some(node) = unique_child(body, "dev-channels")? {
        one_bool(node)?;
    }
    Ok(())
}

fn validate_agent_resource(node: &KdlNode) -> Result<(), St3Error> {
    ensure_only_properties(node, &["uri", "reason", "inactive-reason"])?;
    one_string(node)?;
    let uri = property_string(node, "uri")?
        .ok_or_else(|| St3Error::new("missing-resource-uri", "an agent resource needs `uri`"))?;
    if !uri.contains(':') {
        return Err(St3Error::new(
            "invalid-resource-uri",
            "an agent resource URI needs an absolute scheme",
        ));
    }
    let reason = property_string(node, "reason")?.ok_or_else(|| {
        St3Error::new(
            "missing-resource-reason",
            "an agent resource needs `reason`",
        )
    })?;
    validate_reason(&reason)?;
    if let Some(reason) = property_string(node, "inactive-reason")? {
        validate_reason(&reason)?;
    }
    Ok(())
}

fn validate_stream(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let Some(body) = node.children() else {
        return Ok(());
    };
    reject_unknown_children(body, &["command", "argv"], "stream", "stream")?;
    let command = child_string(body, "command")?;
    let argv = child_strings(body, "argv")?;
    compact_launch(command, argv)?;
    Ok(())
}

fn validate_render(node: &KdlNode) -> Result<(), St3Error> {
    ensure_bare(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("empty-render", "a render block cannot be empty"))?;
    for operation in body.nodes() {
        reject_type(operation)?;
        let name = operation.name().value();
        match name {
            "copy" => {
                ensure_only_properties(operation, &["executable"])?;
                require_string_count(operation, 2)?;
                ensure_no_children(operation)?;
            }
            "file" | "json-upsert" => {
                let allowed = if name == "json-upsert" {
                    &["arrays", "executable"][..]
                } else {
                    &["executable"][..]
                };
                ensure_only_properties(operation, allowed)?;
                let args = positional_strings_without_children(operation)?;
                let child_content = operation.children().is_some();
                if !matches!((args.len(), child_content), (2, false) | (1, true)) {
                    return Err(St3Error::new(
                        "invalid-render-operation",
                        format!("render `{name}` needs a destination and exactly one content form"),
                    ));
                }
                if let Some(children) = operation.children() {
                    reject_unknown_children(children, &["content"], "render operation", name)?;
                    required_child_string(children, "content", name)?;
                }
                if name == "json-upsert" {
                    if let Some(arrays) = property_string(operation, "arrays")?
                        && !matches!(arrays.as_str(), "replace" | "union")
                    {
                        return Err(St3Error::new(
                            "invalid-json-array-mode",
                            format!("invalid JSON array mode `{arrays}`"),
                        ));
                    }
                    let source = if args.len() == 2 {
                        args[1].clone()
                    } else {
                        required_child_string(
                            operation.children().expect("child content"),
                            "content",
                            name,
                        )?
                    };
                    let value: Value = serde_json::from_str(&source)
                        .map_err(|error| St3Error::new("invalid-render-json", error.to_string()))?;
                    if !value.is_object() {
                        return Err(St3Error::new(
                            "invalid-render-json",
                            "json-upsert content must be an object",
                        ));
                    }
                }
            }
            "ensure-line" => {
                ensure_only_properties(operation, &["executable"])?;
                require_string_count(operation, 2)?;
                ensure_no_children(operation)?;
            }
            "git-exclude" => {
                ensure_no_properties(operation)?;
                let values = positional_strings(operation)?;
                if values.is_empty() {
                    return Err(St3Error::new(
                        "invalid-git-exclude",
                        "git-exclude needs at least one path",
                    ));
                }
            }
            other => {
                return Err(St3Error::new(
                    "unknown-render-operation",
                    format!("unknown render operation `{other}`"),
                ));
            }
        }
        if let Some(value) = property_bool(operation, "executable")? {
            let _ = value;
        }
    }
    Ok(())
}

fn validate_resource(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("missing-resource-body", "a resource needs a kind"))?;
    reject_unknown_children(body, &["kind", "binding"], "resource", "resource")?;
    let kind = required_child_string(body, "kind", "resource")?;
    if !matches!(
        kind.as_str(),
        "vcs.pull-request"
            | "ci.run"
            | "repository"
            | "file"
            | "document"
            | "harness.session-file"
            | "human.review"
    ) {
        return Err(St3Error::new(
            "unsupported-capability",
            format!("resource kind `{kind}` is not registered"),
        ));
    }
    if let Some(binding) = child_string(body, "binding")?
        && binding != "late"
    {
        return Err(St3Error::new(
            "invalid-resource-binding",
            "a resource binding must be `late`",
        ));
    }
    Ok(())
}

fn validate_account(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("missing-account-body", "an account needs a body"))?;
    reject_unknown_children(
        body,
        &["provider", "external-account", "auth-type"],
        "account",
        "account",
    )?;
    required_child_string(body, "provider", "account")?;
    required_child_string(body, "external-account", "account")?;
    let auth = required_child_string(body, "auth-type", "account")?;
    if !matches!(auth.as_str(), "subscription" | "api-key") {
        return Err(St3Error::new(
            "invalid-auth-type",
            format!("invalid account auth type `{auth}`"),
        ));
    }
    Ok(())
}

fn validate_supervisor(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let Some(body) = node.children() else {
        return Ok(());
    };
    reject_unknown_children(body, &["gate"], "supervisor", "supervisor")?;
    let mut names = HashSet::new();
    for gate in body.nodes() {
        let name = first_string(gate)?;
        if !names.insert(name.clone()) {
            return Err(St3Error::new(
                "duplicate-gate",
                format!("gate `{name}` repeats"),
            ));
        }
        ensure_only_properties(gate, &["driver"])?;
        let driver = property_string(gate, "driver")?.ok_or_else(|| {
            St3Error::new(
                "missing-gate-driver",
                format!("gate `{name}` needs a driver"),
            )
        })?;
        if !matches!(driver.as_str(), "claude" | "codex" | "pi" | "opencode") {
            return Err(St3Error::new(
                "invalid-gate-driver",
                format!("invalid gate driver `{driver}`"),
            ));
        }
        let children = gate
            .children()
            .ok_or_else(|| St3Error::new("empty-gate", format!("gate `{name}` is empty")))?;
        reject_unknown_children(
            children,
            &["contains", "selected", "key", "max-inputs"],
            "gate",
            &name,
        )?;
        unique_child(children, "selected")?;
        unique_child(children, "max-inputs")?;
        let matchers = children
            .nodes()
            .iter()
            .filter(|child| matches!(child.name().value(), "contains" | "selected"))
            .count();
        let keys = children
            .nodes()
            .iter()
            .filter(|child| child.name().value() == "key")
            .collect::<Vec<_>>();
        if matchers == 0 || keys.is_empty() {
            return Err(St3Error::new(
                "invalid-gate",
                format!("gate `{name}` needs a matcher and a key"),
            ));
        }
        for key in &keys {
            let key = one_string(key)?;
            if !matches!(
                key.as_str(),
                "enter" | "escape" | "tab" | "space" | "up" | "down" | "left" | "right"
            ) {
                return Err(St3Error::new(
                    "invalid-gate-key",
                    format!("invalid gate key `{key}`"),
                ));
            }
        }
        let max_inputs = child_integer(children, "max-inputs")?.unwrap_or(keys.len() as i128);
        if max_inputs < keys.len() as i128 || max_inputs > u32::MAX as i128 {
            return Err(St3Error::new(
                "invalid-gate-limit",
                format!("gate `{name}` has an invalid max-inputs value"),
            ));
        }
    }
    Ok(())
}

fn validate_link(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("missing-link-body", "a link needs a body"))?;
    reject_unknown_children(
        body,
        &["from", "to", "required", "on-unreachable"],
        "link",
        "link",
    )?;
    let from = required_child_string(body, "from", "link")?;
    let to = required_child_string(body, "to", "link")?;
    validate_full_subject(&from)?;
    validate_full_subject(&to)?;
    if from == to {
        return Err(St3Error::new("link-cycle", "a link cannot target itself"));
    }
    let required = unique_child(body, "required")?
        .map(one_bool)
        .transpose()?
        .unwrap_or(true);
    let policy = child_string(body, "on-unreachable")?.unwrap_or_else(|| "hold".into());
    if !matches!(policy.as_str(), "hold" | "void") || (!required && policy != "hold") {
        return Err(St3Error::new(
            "invalid-link-policy",
            format!("invalid link policy `{policy}`"),
        ));
    }
    Ok(())
}

fn validate_message(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("missing-message-body", "a message needs a body"))?;
    reject_unknown_children(body, &["from", "to", "content"], "message", "message")?;
    child_string(body, "from")?;
    required_child_string(body, "to", "message")?;
    let content = required_child_string(body, "content", "message")?;
    if content.trim().is_empty() {
        return Err(St3Error::new(
            "empty-message",
            "a message needs nonempty content",
        ));
    }
    if content.len() > 4_096 && !content.starts_with("doc/") {
        return Err(St3Error::new(
            "message-too-large",
            "an inline message cannot exceed 4 KiB",
        ));
    }
    Ok(())
}

fn validate_schedule(node: &KdlNode) -> Result<(), St3Error> {
    ensure_no_properties(node)?;
    one_string_with_children(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("missing-schedule-body", "a schedule needs a body"))?;
    if body.nodes().len() == 1 && body.nodes()[0].name().value() == "stop" {
        ensure_bare(&body.nodes()[0])?;
        ensure_no_children(&body.nodes()[0])?;
        return Ok(());
    }
    reject_unknown_children(
        body,
        &[
            "host",
            "at",
            "every",
            "anchor",
            "catch-up",
            "max-catch-up",
            "message",
        ],
        "schedule",
        "schedule",
    )?;
    for child in [
        "host",
        "at",
        "every",
        "anchor",
        "catch-up",
        "max-catch-up",
        "message",
    ] {
        unique_child(body, child)?;
    }
    let at = child_string(body, "at")?;
    let every = child_string(body, "every")?;
    if at.is_some() == every.is_some() {
        return Err(St3Error::new(
            "invalid-schedule-time",
            "a schedule needs exactly one of `at` and `every`",
        ));
    }
    let anchor = child_string(body, "anchor")?;
    if at.is_some() && anchor.is_some() || every.is_some() && anchor.is_none() {
        return Err(St3Error::new(
            "invalid-schedule-anchor",
            "an interval schedule needs an anchor and a one-time schedule cannot have one",
        ));
    }
    if let Some(at) = at.as_deref().or(anchor.as_deref()) {
        parse_utc_time(at)?;
    }
    if let Some(every) = every {
        parse_duration(&every, true)?;
    }
    let catch_up = child_string(body, "catch-up")?;
    let max = child_integer(body, "max-catch-up")?;
    if at.is_some() && (catch_up.is_some() || max.is_some()) {
        return Err(St3Error::new(
            "invalid-schedule-catch-up",
            "a one-time schedule cannot have catch-up controls",
        ));
    }
    if let Some(policy) = catch_up.as_deref() {
        if !matches!(policy, "all" | "latest" | "skip") {
            return Err(St3Error::new(
                "invalid-schedule-catch-up",
                format!("invalid catch-up policy `{policy}`"),
            ));
        }
        if (policy == "all") != max.is_some() {
            return Err(St3Error::new(
                "invalid-schedule-catch-up",
                "catch-up `all` requires max-catch-up, and other policies forbid it",
            ));
        }
    } else if max.is_some() {
        return Err(St3Error::new(
            "invalid-schedule-catch-up",
            "max-catch-up requires catch-up `all`",
        ));
    }
    if let Some(max) = max
        && (max <= 0 || max > u32::MAX as i128)
    {
        return Err(St3Error::new(
            "invalid-schedule-catch-up",
            "max-catch-up must be a positive u32",
        ));
    }
    let message = unique_child(body, "message")?.ok_or_else(|| {
        St3Error::new(
            "missing-schedule-message",
            "a schedule needs a message template",
        )
    })?;
    ensure_bare(message)?;
    let message_body = message
        .children()
        .ok_or_else(|| St3Error::new("missing-schedule-message", "a schedule message is empty"))?;
    reject_unknown_children(
        message_body,
        &["from", "to", "content"],
        "schedule message",
        "message",
    )?;
    child_string(message_body, "from")?;
    required_child_string(message_body, "to", "schedule message")?;
    let content = required_child_string(message_body, "content", "schedule message")?;
    if content.trim().is_empty() {
        return Err(St3Error::new(
            "empty-message",
            "a schedule message needs nonempty content",
        ));
    }
    Ok(())
}

fn reject_unknown_children(
    document: &KdlDocument,
    allowed: &[&str],
    kind: &str,
    owner: &str,
) -> Result<(), St3Error> {
    for child in document.nodes() {
        if !allowed.contains(&child.name().value()) {
            return Err(St3Error::new(
                "unknown-child",
                format!(
                    "{kind} `{owner}` does not accept child `{}`",
                    child.name().value()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_string_map(node: &KdlNode, environment: bool) -> Result<(), St3Error> {
    ensure_bare(node)?;
    let Some(body) = node.children() else {
        return Ok(());
    };
    let mut names = HashSet::new();
    for child in body.nodes() {
        let name = child.name().value();
        if !names.insert(name) {
            return Err(St3Error::new(
                "duplicate-map-key",
                format!("map key `{name}` repeats"),
            ));
        }
        if environment {
            validate_environment_name(name)?;
        }
        one_string(child)?;
    }
    Ok(())
}

fn validate_scalar_map(node: &KdlNode) -> Result<(), St3Error> {
    ensure_bare(node)?;
    let Some(body) = node.children() else {
        return Ok(());
    };
    let mut names = HashSet::new();
    for child in body.nodes() {
        ensure_no_properties(child)?;
        ensure_no_children(child)?;
        if !names.insert(child.name().value()) || positional_values(child).len() != 1 {
            return Err(St3Error::new(
                "invalid-meta",
                "meta keys must be unique and have one scalar",
            ));
        }
        json_value(positional_values(child)[0])?;
    }
    Ok(())
}

fn validate_tags(node: &KdlNode) -> Result<(), St3Error> {
    reject_type(node)?;
    ensure_no_children(node)?;
    if node.entries().iter().any(|entry| entry.name().is_none()) {
        return Err(St3Error::new(
            "invalid-tags",
            "a tags entry needs a property name",
        ));
    }
    let mut names = HashSet::new();
    for entry in node.entries() {
        let name = entry.name().expect("checked").value();
        if !names.insert(name) {
            return Err(St3Error::new(
                "duplicate-tag",
                format!("tag `{name}` repeats"),
            ));
        }
        value_string(entry.value())?;
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> Result<(), St3Error> {
    let valid = name
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid {
        return Err(St3Error::new(
            "invalid-environment-name",
            format!("invalid environment name `{name}`"),
        ));
    }
    Ok(())
}

fn validate_reason(reason: &str) -> Result<(), St3Error> {
    if reason.is_empty() || reason.len() > 160 || reason.chars().any(char::is_control) {
        return Err(St3Error::new(
            "invalid-resource-reason",
            "a resource reason must contain 1 through 160 printable bytes",
        ));
    }
    Ok(())
}

fn parse_utc_time(value: &str) -> Result<i64, St3Error> {
    if !value.ends_with('Z') {
        return Err(St3Error::new(
            "invalid-utc-time",
            "an absolute time must use the UTC `Z` offset",
        ));
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|time| time.timestamp_millis())
        .map_err(|error| St3Error::new("invalid-utc-time", error.to_string()))
}

pub fn link_spec(value: &Value) -> Option<LinkSpec> {
    Some(LinkSpec {
        from: canonical_child_value(value, "from")?.as_str()?.to_owned(),
        to: canonical_child_value(value, "to")?.as_str()?.to_owned(),
        required: canonical_child_value(value, "required")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        on_unreachable: canonical_child_value(value, "on-unreachable")
            .and_then(Value::as_str)
            .unwrap_or("hold")
            .to_owned(),
    })
}

pub fn schedule_spec(value: &Value, default_host: &str) -> Option<ScheduleSpec> {
    let children = value.get("children")?.as_array()?;
    if children.len() == 1 && children[0].get("name").and_then(Value::as_str) == Some("stop") {
        return Some(ScheduleSpec {
            stopped: true,
            host: default_host.into(),
            at_unix_ms: None,
            every_ms: None,
            anchor_unix_ms: None,
            catch_up: "latest".into(),
            max_catch_up: None,
            message: None,
        });
    }
    let host = canonical_child_value(value, "host")
        .and_then(Value::as_str)
        .unwrap_or(default_host)
        .to_owned();
    let host = placement_host(host, default_host);
    let at_unix_ms = canonical_child_value(value, "at")
        .and_then(Value::as_str)
        .and_then(|value| parse_utc_time(value).ok());
    let every_ms = canonical_child_value(value, "every")
        .and_then(Value::as_str)
        .and_then(|value| parse_duration(value, true).ok());
    let anchor_unix_ms = canonical_child_value(value, "anchor")
        .and_then(Value::as_str)
        .and_then(|value| parse_utc_time(value).ok());
    let catch_up = canonical_child_value(value, "catch-up")
        .and_then(Value::as_str)
        .unwrap_or("latest")
        .to_owned();
    let max_catch_up = canonical_child_value(value, "max-catch-up")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    let message_node = children
        .iter()
        .find(|child| child.get("name").and_then(Value::as_str) == Some("message"))?;
    let message = MessageTemplate {
        from: canonical_child_value(message_node, "from")
            .and_then(Value::as_str)
            .unwrap_or("requester")
            .to_owned(),
        to: canonical_child_value(message_node, "to")?
            .as_str()?
            .to_owned(),
        content: canonical_child_value(message_node, "content")?
            .as_str()?
            .to_owned(),
    };
    Some(ScheduleSpec {
        stopped: false,
        host,
        at_unix_ms,
        every_ms,
        anchor_unix_ms,
        catch_up,
        max_catch_up,
        message: Some(message),
    })
}

pub fn supervisor_gates(value: &Value) -> Vec<GateSpec> {
    let Some(children) = value.get("children").and_then(Value::as_array) else {
        return Vec::new();
    };
    children
        .iter()
        .filter(|child| child.get("name").and_then(Value::as_str) == Some("gate"))
        .filter_map(|gate| {
            let name = gate
                .get("arguments")?
                .as_array()?
                .first()?
                .as_str()?
                .to_owned();
            let driver = gate.pointer("/properties/driver")?.as_str()?.to_owned();
            let body = gate.get("children")?.as_array()?;
            let values = |kind: &str| {
                body.iter()
                    .filter(|child| child.get("name").and_then(Value::as_str) == Some(kind))
                    .filter_map(|child| {
                        child
                            .get("arguments")?
                            .as_array()?
                            .first()?
                            .as_str()
                            .map(str::to_owned)
                    })
                    .collect::<Vec<_>>()
            };
            let contains = values("contains");
            let selected = values("selected").into_iter().next();
            let keys = values("key");
            let max_inputs = body
                .iter()
                .find(|child| child.get("name").and_then(Value::as_str) == Some("max-inputs"))
                .and_then(|child| child.get("arguments"))
                .and_then(Value::as_array)
                .and_then(|values| values.first())
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(keys.len() as u32);
            Some(GateSpec {
                name,
                driver,
                contains,
                selected,
                keys,
                max_inputs,
            })
        })
        .collect()
}

fn canonical_child_value<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value
        .get("children")?
        .as_array()?
        .iter()
        .find(|child| child.get("name").and_then(Value::as_str) == Some(name))?
        .get("arguments")?
        .as_array()?
        .first()
}

fn one_bool(node: &KdlNode) -> Result<bool, St3Error> {
    ensure_no_properties(node)?;
    ensure_no_children(node)?;
    let [entry] = node.entries() else {
        return Err(St3Error::new(
            "wrong-argument-count",
            format!("node `{}` needs one Boolean", node.name().value()),
        ));
    };
    match entry.value() {
        KdlValue::Bool(value) => Ok(*value),
        _ => Err(St3Error::new(
            "expected-boolean",
            format!("node `{}` needs one Boolean", node.name().value()),
        )),
    }
}

fn property_bool(node: &KdlNode, property: &str) -> Result<Option<bool>, St3Error> {
    node.entries()
        .iter()
        .find(|entry| entry.name().is_some_and(|name| name.value() == property))
        .map(|entry| match entry.value() {
            KdlValue::Bool(value) => Ok(*value),
            _ => Err(St3Error::new(
                "expected-boolean",
                format!("property `{property}` needs a Boolean"),
            )),
        })
        .transpose()
}

fn require_string_count(node: &KdlNode, count: usize) -> Result<Vec<String>, St3Error> {
    let values = positional_strings_without_children(node)?;
    if values.len() != count {
        return Err(St3Error::new(
            "wrong-argument-count",
            format!("node `{}` needs {count} string values", node.name().value()),
        ));
    }
    Ok(values)
}

fn positional_strings_without_children(node: &KdlNode) -> Result<Vec<String>, St3Error> {
    positional_values(node)
        .into_iter()
        .map(value_string)
        .collect()
}

fn parse_tags(document: &KdlDocument) -> Result<BTreeMap<String, String>, St3Error> {
    let Some(node) = unique_child(document, "tags")? else {
        return Ok(BTreeMap::new());
    };
    let mut tags = BTreeMap::new();
    for entry in node.entries() {
        let name = entry.name().ok_or_else(|| {
            St3Error::new("invalid-tags", "a tags entry must have a property name")
        })?;
        let value = value_string(entry.value())?;
        if tags.insert(name.value().into(), value).is_some() {
            return Err(St3Error::new(
                "duplicate-tag",
                format!("tag `{}` repeats", name.value()),
            ));
        }
    }
    Ok(tags)
}

fn collect_document_refs(node: &KdlNode, output: &mut BTreeSet<String>) -> Result<(), St3Error> {
    for entry in node.entries() {
        if let KdlValue::String(value) = entry.value()
            && value.starts_with("doc/")
        {
            validate_document_ref(value)?;
            output.insert(value.clone());
        }
    }
    if let Some(children) = node.children() {
        for child in children.nodes() {
            collect_document_refs(child, output)?;
        }
    }
    Ok(())
}

fn validate_document_ref(value: &str) -> Result<(), St3Error> {
    let (name, hash) = value.rsplit_once('@').unwrap_or((value, ""));
    if name.len() <= 4
        || name.contains("..")
        || name.ends_with('/')
        || (!hash.is_empty()
            && (hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())))
    {
        return Err(St3Error::new(
            "invalid-document-reference",
            format!("invalid document reference `{value}`"),
        ));
    }
    Ok(())
}

fn validate_links(subjects: &BTreeMap<String, DesiredSubject>) -> Result<(), St3Error> {
    let mut edges = BTreeMap::<String, String>::new();
    for desired in subjects.values().filter(|subject| subject.kind == "link") {
        let Some(spec) = link_spec(&desired.desired) else {
            continue;
        };
        if spec.on_unreachable == "void" {
            let temporary = desired.scopes.iter().any(|scope| {
                subjects.get(scope).is_some_and(|scope| {
                    scope
                        .desired
                        .get("properties")
                        .and_then(|properties| properties.get("retention"))
                        .and_then(Value::as_str)
                        == Some("temporary")
                })
            });
            if !temporary {
                return Err(St3Error::new(
                    "invalid-link-policy",
                    format!(
                        "link `{}` uses void outside a temporary scope",
                        desired.subject
                    ),
                ));
            }
        }
        if spec.required {
            edges.insert(spec.from, spec.to);
        }
    }
    for start in edges.keys() {
        let mut seen = BTreeSet::new();
        let mut cursor = start;
        while let Some(next) = edges.get(cursor) {
            if !seen.insert(cursor.clone()) || next == start {
                return Err(St3Error::new(
                    "link-cycle",
                    format!("a required link cycle includes `{start}`"),
                ));
            }
            cursor = next;
        }
    }
    Ok(())
}

fn insert_subject(context: &mut ParseContext, subject: DesiredSubject) -> Result<(), St3Error> {
    let name = subject.subject.clone();
    if context.subjects.insert(name.clone(), subject).is_some() {
        return Err(St3Error::new(
            "duplicate-subject",
            format!("one publish declares `{name}` more than once"),
        ));
    }
    Ok(())
}

fn canonical_node(node: &KdlNode) -> Result<Value, St3Error> {
    let mut properties = BTreeMap::<String, Value>::new();
    let mut arguments = Vec::new();
    for entry in node.entries() {
        let value = json_value(entry.value())?;
        if let Some(name) = entry.name() {
            if properties.insert(name.value().into(), value).is_some() {
                return Err(St3Error::new(
                    "duplicate-property",
                    format!(
                        "node `{}` repeats property `{}`",
                        node.name().value(),
                        name.value()
                    ),
                ));
            }
        } else {
            arguments.push(value);
        }
    }
    let children = node
        .children()
        .map(|document| {
            document
                .nodes()
                .iter()
                .map(canonical_node)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let mut output = Map::new();
    output.insert("name".into(), Value::String(node.name().value().into()));
    if !arguments.is_empty() {
        output.insert("arguments".into(), Value::Array(arguments));
    }
    if !properties.is_empty() {
        output.insert(
            "properties".into(),
            serde_json::to_value(properties).expect("BTreeMap serializes"),
        );
    }
    if !children.is_empty() {
        output.insert("children".into(), Value::Array(children));
    }
    Ok(Value::Object(output))
}

fn hash_json(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).expect("normalized JSON serializes");
    hex::encode(Sha256::digest(bytes))
}

fn json_value(value: &KdlValue) -> Result<Value, St3Error> {
    Ok(match value {
        KdlValue::String(value) => Value::String(value.clone()),
        KdlValue::Integer(value) => {
            let integer = i64::try_from(*value).map_err(|_| {
                St3Error::new(
                    "integer-range",
                    "a KDL integer must fit a signed 64-bit value",
                )
            })?;
            Value::Number(integer.into())
        }
        KdlValue::Float(_) => {
            return Err(St3Error::new(
                "unsupported-float",
                "st3.v1 does not accept floating-point values",
            ));
        }
        KdlValue::Bool(value) => Value::Bool(*value),
        KdlValue::Null => Value::Null,
    })
}

fn reject_type(node: &KdlNode) -> Result<(), St3Error> {
    if node.ty().is_some() {
        Err(St3Error::new(
            "unexpected-type",
            format!(
                "node `{}` cannot have a type annotation",
                node.name().value()
            ),
        ))
    } else {
        Ok(())
    }
}

fn ensure_bare(node: &KdlNode) -> Result<(), St3Error> {
    reject_type(node)?;
    if !node.entries().is_empty() {
        return Err(St3Error::new(
            "unexpected-value",
            format!("node `{}` cannot have values", node.name().value()),
        ));
    }
    Ok(())
}

fn ensure_no_children(node: &KdlNode) -> Result<(), St3Error> {
    if node.children().is_some() {
        Err(St3Error::new(
            "unexpected-children",
            format!("node `{}` cannot have children", node.name().value()),
        ))
    } else {
        Ok(())
    }
}

fn ensure_no_properties(node: &KdlNode) -> Result<(), St3Error> {
    ensure_only_properties(node, &[])
}

fn ensure_only_properties(node: &KdlNode, allowed: &[&str]) -> Result<(), St3Error> {
    reject_type(node)?;
    let mut seen = HashSet::new();
    for entry in node.entries().iter().filter(|entry| entry.name().is_some()) {
        let name = entry.name().expect("filtered").value();
        if !allowed.contains(&name) {
            return Err(St3Error::new(
                "unknown-property",
                format!(
                    "node `{}` does not accept property `{name}`",
                    node.name().value()
                ),
            ));
        }
        if !seen.insert(name) {
            return Err(St3Error::new(
                "duplicate-property",
                format!("node `{}` repeats property `{name}`", node.name().value()),
            ));
        }
    }
    Ok(())
}

fn one_string(node: &KdlNode) -> Result<String, St3Error> {
    ensure_no_children(node)?;
    one_string_with_children(node)
}

fn one_string_with_children(node: &KdlNode) -> Result<String, St3Error> {
    first_string(node).and_then(|first| {
        if positional_values(node).len() == 1 {
            Ok(first)
        } else {
            Err(St3Error::new(
                "wrong-argument-count",
                format!(
                    "node `{}` needs one string; received `{}`",
                    node.name().value(),
                    node
                ),
            ))
        }
    })
}

fn first_string(node: &KdlNode) -> Result<String, St3Error> {
    positional_values(node)
        .first()
        .ok_or_else(|| {
            St3Error::new(
                "missing-argument",
                format!("node `{}` needs a name", node.name().value()),
            )
        })
        .and_then(|value| value_string(value))
}

fn positional_values(node: &KdlNode) -> Vec<&KdlValue> {
    node.entries()
        .iter()
        .filter(|entry| entry.name().is_none())
        .map(|entry| entry.value())
        .collect()
}

fn positional_strings(node: &KdlNode) -> Result<Vec<String>, St3Error> {
    ensure_no_properties(node)?;
    ensure_no_children(node)?;
    positional_values(node)
        .into_iter()
        .map(value_string)
        .collect()
}

fn value_string(value: &KdlValue) -> Result<String, St3Error> {
    match value {
        KdlValue::String(value) if !value.is_empty() => Ok(value.clone()),
        _ => Err(St3Error::new(
            "expected-string",
            "this value must be a non-empty string",
        )),
    }
}

fn property_string(node: &KdlNode, property: &str) -> Result<Option<String>, St3Error> {
    node.entries()
        .iter()
        .find(|entry| entry.name().is_some_and(|name| name.value() == property))
        .map(|entry| value_string(entry.value()))
        .transpose()
}

fn unique_child<'a>(
    document: &'a KdlDocument,
    name: &str,
) -> Result<Option<&'a KdlNode>, St3Error> {
    let matches = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == name)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [node] => Ok(Some(*node)),
        _ => Err(St3Error::new(
            "duplicate-child",
            format!("child `{name}` repeats"),
        )),
    }
}

fn child_string(document: &KdlDocument, name: &str) -> Result<Option<String>, St3Error> {
    unique_child(document, name)?.map(one_string).transpose()
}

fn required_child_string(
    document: &KdlDocument,
    child: &str,
    owner: &str,
) -> Result<String, St3Error> {
    child_string(document, child)?
        .ok_or_else(|| St3Error::new("missing-child", format!("`{owner}` needs child `{child}`")))
}

fn child_strings(document: &KdlDocument, name: &str) -> Result<Option<Vec<String>>, St3Error> {
    unique_child(document, name)?
        .map(positional_strings)
        .transpose()
}

fn child_integer(document: &KdlDocument, name: &str) -> Result<Option<i128>, St3Error> {
    let Some(node) = unique_child(document, name)? else {
        return Ok(None);
    };
    ensure_no_properties(node)?;
    ensure_no_children(node)?;
    let [entry] = node.entries() else {
        return Err(St3Error::new(
            "wrong-argument-count",
            format!("child `{name}` needs one integer"),
        ));
    };
    match entry.value() {
        KdlValue::Integer(value) => Ok(Some(*value)),
        _ => Err(St3Error::new(
            "expected-integer",
            format!("child `{name}` needs one integer"),
        )),
    }
}

fn parse_map_child(
    document: &KdlDocument,
    name: &str,
) -> Result<BTreeMap<String, String>, St3Error> {
    let Some(node) = unique_child(document, name)? else {
        return Ok(BTreeMap::new());
    };
    ensure_bare(node)?;
    let Some(children) = node.children() else {
        return Ok(BTreeMap::new());
    };
    let mut output = BTreeMap::new();
    for child in children.nodes() {
        let value = one_string(child)?;
        let key = child.name().value().to_owned();
        if output.insert(key.clone(), value).is_some() {
            return Err(St3Error::new(
                "duplicate-map-key",
                format!("map key `{key}` repeats"),
            ));
        }
    }
    Ok(output)
}

fn has_block_child(document: &KdlDocument, name: &str) -> bool {
    document
        .nodes()
        .iter()
        .any(|node| node.name().value() == name && node.children().is_some())
}

fn one_duration(node: &KdlNode) -> Result<u64, St3Error> {
    ensure_no_properties(node)?;
    ensure_no_children(node)?;
    let [entry] = node.entries() else {
        return Err(St3Error::new(
            "wrong-argument-count",
            format!("node `{}` needs one duration", node.name().value()),
        ));
    };
    match entry.value() {
        KdlValue::String(value) => parse_duration(value, true),
        KdlValue::Integer(value) if *value > 0 => u64::try_from(*value)
            .map(|value| value * 1_000)
            .map_err(|_| St3Error::new("duration-range", "duration is too large")),
        _ => Err(St3Error::new(
            "invalid-duration",
            "a required duration must be positive",
        )),
    }
}

pub fn parse_duration(value: &str, positive: bool) -> Result<u64, St3Error> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 1)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60_000)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 3_600_000)
    } else if let Some(value) = value.strip_suffix('d') {
        (value, 86_400_000)
    } else {
        (value, 1_000)
    };
    let number = number
        .parse::<u64>()
        .map_err(|_| St3Error::new("invalid-duration", format!("invalid duration `{value}`")))?;
    let duration = number
        .checked_mul(multiplier)
        .ok_or_else(|| St3Error::new("duration-range", "duration is too large"))?;
    if positive && duration == 0 {
        return Err(St3Error::new(
            "invalid-duration",
            "a required duration must be positive",
        ));
    }
    Ok(duration)
}

fn validate_name(value: &str, full: bool) -> Result<(), St3Error> {
    if value.is_empty() || value.len() > 512 || !value.is_ascii() {
        return Err(St3Error::new(
            "invalid-subject-name",
            format!("invalid subject name `{value}`"),
        ));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"._-@/".contains(&byte))
        || !value.as_bytes()[0].is_ascii_alphanumeric()
        || value.ends_with('/')
        || value.split('/').any(|part| part.is_empty() || part == "..")
        || (full && !value.contains('/'))
    {
        return Err(St3Error::new(
            "invalid-subject-name",
            format!("invalid subject name `{value}`"),
        ));
    }
    Ok(())
}

fn validate_full_subject(value: &str) -> Result<(), St3Error> {
    if value.contains("${") {
        let mut concrete = String::with_capacity(value.len());
        let mut rest = value;
        while let Some(start) = rest.find("${") {
            concrete.push_str(&rest[..start]);
            let tail = &rest[start + 2..];
            let Some(end) = tail.find('}') else {
                return Err(St3Error::new(
                    "invalid-variable",
                    "a variable reference has no closing brace",
                ));
            };
            concrete.push('x');
            rest = &tail[end + 1..];
        }
        concrete.push_str(rest);
        return validate_full_subject(&concrete);
    }
    if let Some(rest) = value.strip_prefix("file/") {
        let Some((host, path)) = rest.split_once(':') else {
            return Err(St3Error::new(
                "invalid-file-subject",
                "a file subject needs `file/HOST:/ABSOLUTE-PATH`",
            ));
        };
        validate_name(host, false)?;
        if !path.starts_with('/') || path.contains("/../") || path.ends_with("/..") {
            return Err(St3Error::new(
                "invalid-file-subject",
                "a file subject needs a safe absolute path",
            ));
        }
        return Ok(());
    }
    validate_name(value, true)
}

fn namespaced(namespace: &str, value: &str) -> String {
    if value.starts_with(&format!("{namespace}/")) {
        value.into()
    } else {
        format!("{namespace}/{value}")
    }
}

fn runtime_id(subject: &str) -> String {
    subject.replace('/', ".")
}

fn valid_field_path(path: &str) -> bool {
    path.split('.').all(|segment| {
        !segment.is_empty()
            && segment
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic())
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_old_agent_root() {
        let error = parse_intent("agent \"worker\" { command \"true\" }", "host")
            .expect_err("old KDL must fail");
        assert_eq!(error.code, "invalid-root");
    }

    #[test]
    fn parses_plain_agent_and_document_version() {
        let intent = parse_intent(
            r#"
subgraph {
  message "task" {
    to "worker"
    content "doc/tasks/work@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  agent "worker" {
    workspace "/work"
    restart "never"
    harness "claude" {
      prompt "Work on the task."
    }
  }
}
"#,
            "node",
        )
        .expect("new KDL parses");
        assert!(intent.subjects.contains_key("agent/node.worker"));
        assert!(intent.subjects.contains_key("message/task"));
        assert_eq!(intent.document_refs.len(), 1);
    }

    #[test]
    fn parses_plan_order_and_dependencies() {
        let intent = parse_intent(
            r#"
subgraph {
  plan "build" state="ready" {
    step "build" {
      title "The first step passes"
      subgraph { exec "one" { command "true"; restart "never" } }
      judges { field "status" "exec/one" is "exited" }
    }

    step "review" {
      title "The work is reviewed"
      depends-on { step "build" completed }
      judges { field "decision" "resource/review" is "approved" }
    }
  }
}
"#,
            "node",
        )
        .expect("plan KDL parses");
        let plan = &intent.plans["build"];
        assert_eq!(plan.display_order, ["build", "review"]);
        assert!(matches!(
            &plan.steps["review"].dependencies[0],
            crate::model::DependencySpec::Step { step, state }
                if step == "build" && state == "completed"
        ));
    }

    #[test]
    fn local_placement_resolves_to_the_receiving_node() {
        let source = r#"
            subgraph {
              exec "setup" {
                host "local"
                command "true"
              }
              plan "proof" state="ready" {
                step "verify" {
                  title "The local judge passes"
                  judges {
                    judge "verify" {
                      exec "true"
                      host "local"
                      workspace "."
                    }
                  }
                }
              }
            }
        "#;

        let intent = parse_intent(source, "node-a").unwrap();

        assert_eq!(
            intent.subjects["exec/setup"].member.as_ref().unwrap().host,
            "node-a"
        );
        let JudgeSpec::Mechanical { host, .. } = &intent.plans["proof"].steps["verify"].judges[0]
        else {
            panic!("the test judge is not mechanical");
        };
        assert_eq!(host, "node-a");
    }

    #[test]
    fn nested_tasks_inherit_lifecycle_and_restart_controls() {
        let intent = parse_intent(
            r#"
subgraph {
  agent "worker" {
    workspace "/work"
    lifecycle "adopt-only"
    restart "never"
    shutdown-timeout "9s"
    restart {
      attempts 7
      interval "2m"
      delay "3s"
      mode "fail"
    }
    exec "build" { command "true" }
  }
}
"#,
            "node",
        )
        .unwrap();
        let member = intent.subjects["exec/node.worker/build"]
            .member
            .as_ref()
            .unwrap();
        assert_eq!(member.lifecycle, MemberLifecycle::AdoptOnly);
        assert_eq!(member.restart, RestartType::Never);
        assert_eq!(member.shutdown_timeout_ms, 9_000);
        assert_eq!(member.restart_intensity.attempts, 7);
        assert_eq!(member.restart_intensity.interval_ms, 120_000);
        assert_eq!(member.restart_intensity.delay_ms, 3_000);
        assert_eq!(member.restart_intensity.mode, "fail");
    }

    #[test]
    fn strict_grammar_rejects_unknown_children_and_properties() {
        let child = parse_intent(
            r#"subgraph { agent "worker" { command "true"; retired #true } }"#,
            "node",
        )
        .expect_err("retired is old syntax");
        assert_eq!(child.code, "unknown-child");

        let property = parse_intent(
            r#"subgraph { exec "work" mystery="value" { command "true" } }"#,
            "node",
        )
        .expect_err("unknown property");
        assert_eq!(property.code, "unknown-property");
    }

    #[test]
    fn authored_messages_reject_empty_content() {
        let error = parse_intent(
            "subgraph { message \"empty\" { to \"worker\"; content \"  \" } }",
            "node",
        )
        .expect_err("an empty message must not enter the delivery FIFO");
        assert_eq!(error.code, "empty-message");
    }

    #[test]
    fn a_plan_can_pin_bare_document_references() {
        let source = r#"
subgraph {
  message "task" {
    to "worker"
    content "doc/tasks/work"
  }
}
"#;
        let resolved = resolve_document_references(
            source,
            &BTreeMap::from([("doc/tasks/work".into(), "a".repeat(64))]),
        )
        .unwrap();
        let intent = parse_intent(&resolved, "node").unwrap();
        assert_eq!(
            intent.document_refs.into_iter().next().unwrap(),
            format!("doc/tasks/work@{}", "a".repeat(64))
        );
    }
}
