use std::collections::{BTreeMap, BTreeSet};

use kdl::{KdlDocument, KdlNode, KdlValue};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::model::{
    ChangePolicy, DependencySpec, JudgeSpec, PlanSpec, PlanState, ProductSpec, RetrySpec, St3Error,
    StepSpec,
};

const VARIABLES: &[&str] = &[
    "PLAN",
    "PLAN_REVISION",
    "PLAN_RUN",
    "RUN_SCOPE",
    "WORKSPACE",
    "STEP",
    "STEP_RUN",
    "ATTEMPT",
    "ASSIGNEE",
    "REQUESTER",
    "PARENT_STEP_RUN",
    "ROOT_PLAN_RUN",
];

pub fn parse_plans(
    root: &KdlNode,
    default_host: &str,
) -> Result<BTreeMap<String, PlanSpec>, St3Error> {
    let mut plans = BTreeMap::new();
    let Some(children) = root.children() else {
        return Ok(plans);
    };
    for node in children.nodes() {
        match node.name().value() {
            "plan" => insert_plan(
                &mut plans,
                parse_plan(node, None, None, None, default_host, true)?,
            )?,
            "scope" => parse_scope_plans(node, default_host, &mut plans)?,
            _ => {}
        }
    }
    Ok(plans)
}

fn parse_scope_plans(
    node: &KdlNode,
    default_host: &str,
    plans: &mut BTreeMap<String, PlanSpec>,
) -> Result<(), St3Error> {
    let scope_name = first_string(node)?;
    let scope = if scope_name.starts_with("scope/") {
        scope_name
    } else {
        format!("scope/{scope_name}")
    };
    let policy = property_string(node, "change-policy")?
        .map(|value| parse_change_policy(&value))
        .transpose()?
        .unwrap_or(ChangePolicy::Agent);
    let authority = property_string(node, "change-authority")?;
    if !matches!(policy, ChangePolicy::Agent) && authority.is_none() {
        return Err(St3Error::new(
            "missing-change-authority",
            format!("scope `{scope}` needs change-authority for its selected change policy"),
        ));
    }
    if let Some(children) = node.children() {
        for child in children
            .nodes()
            .iter()
            .filter(|child| child.name().value() == "plan")
        {
            insert_plan(
                plans,
                parse_plan(
                    child,
                    Some(&scope),
                    Some(policy.clone()),
                    authority.clone(),
                    default_host,
                    true,
                )?,
            )?;
        }
    }
    Ok(())
}

fn insert_plan(plans: &mut BTreeMap<String, PlanSpec>, plan: PlanSpec) -> Result<(), St3Error> {
    if plans.insert(plan.id.clone(), plan.clone()).is_some() {
        return Err(St3Error::new(
            "duplicate-plan",
            format!("plan `{}` repeats", plan.id),
        ));
    }
    Ok(())
}

fn parse_plan(
    node: &KdlNode,
    scope: Option<&str>,
    inherited_policy: Option<ChangePolicy>,
    inherited_authority: Option<String>,
    default_host: &str,
    require_state: bool,
) -> Result<PlanSpec, St3Error> {
    reject_type(node)?;
    ensure_only_properties(node, &["state"])?;
    let id = first_string(node)?;
    validate_id(&id, "plan")?;
    let authored_state = property_string(node, "state")?;
    if require_state && authored_state.is_none() {
        return Err(St3Error::new(
            "missing-plan-state",
            format!("plan `{id}` needs an explicit state"),
        ));
    }
    let state = authored_state
        .map(|value| parse_plan_state(&value))
        .transpose()?
        .unwrap_or(PlanState::Ready);
    let children = node
        .children()
        .ok_or_else(|| St3Error::new("empty-plan", format!("plan `{id}` has no steps")))?;
    let mut steps = BTreeMap::new();
    let mut display_order = Vec::new();
    for child in children.nodes() {
        if child.name().value() != "step" {
            return Err(St3Error::new(
                "invalid-plan-child",
                format!("plan `{id}` cannot contain `{}`", child.name().value()),
            ));
        }
        let step = parse_step(child, "", default_host)?;
        if steps.insert(step.id.clone(), step.clone()).is_some() {
            return Err(St3Error::new(
                "duplicate-step",
                format!("plan `{id}` repeats step `{}`", step.id),
            ));
        }
        display_order.push(step.id);
    }
    if steps.is_empty() {
        return Err(St3Error::new(
            "empty-plan",
            format!("plan `{id}` has no steps"),
        ));
    }
    validate_dependencies(&id, &steps)?;
    let mut plan = PlanSpec {
        subject: format!("plan/{id}"),
        id,
        state,
        revision: String::new(),
        scope_template: scope.map(str::to_owned),
        change_policy: inherited_policy.unwrap_or(ChangePolicy::Agent),
        change_authority: inherited_authority,
        steps,
        display_order,
    };
    plan.revision = hash(&plan)?;
    Ok(plan)
}

fn parse_step(node: &KdlNode, parent_path: &str, default_host: &str) -> Result<StepSpec, St3Error> {
    reject_type(node)?;
    ensure_only_properties(node, &["timeout", "finally"])?;
    let id = first_string(node)?;
    validate_id(&id, "step")?;
    let path = if parent_path.is_empty() {
        id.clone()
    } else {
        format!("{parent_path}/{id}")
    };
    let timeout_ms = property_string(node, "timeout")?
        .map(|value| parse_duration(&value))
        .transpose()?;
    let finally = property_bool(node, "finally")?.unwrap_or(false);
    let mut title = None;
    let mut goal = None;
    let mut assigned_to = None;
    let mut dependencies = Vec::new();
    let mut subgraph_kdl = None;
    let mut products = Vec::new();
    let mut judges = Vec::new();
    let mut nested_plan = None;
    let mut retry = RetrySpec::default();
    if let Some(children) = node.children() {
        let mut names = BTreeSet::new();
        for child in children.nodes() {
            let name = child.name().value();
            if !matches!(name, "depends-on") && !names.insert(name.to_owned()) {
                return Err(St3Error::new(
                    "duplicate-step-field",
                    format!("step `{path}` repeats `{name}`"),
                ));
            }
            match name {
                "title" => title = Some(first_string(child)?),
                "goal" => goal = Some(first_string(child)?),
                "assigned-to" => {
                    assigned_to = Some(normalize_assignee(&first_string(child)?, default_host))
                }
                "depends-on" => dependencies.extend(parse_dependencies(child, default_host)?),
                "subgraph" => {
                    ensure_bare(child)?;
                    if child.children().is_none_or(|body| body.nodes().is_empty()) {
                        return Err(St3Error::new(
                            "empty-step-subgraph",
                            format!("step `{path}` has an empty subgraph"),
                        ));
                    }
                    subgraph_kdl = Some(format!("version 2\n{child}\n"));
                }
                "produces" => products = parse_products(child)?,
                "judges" => {
                    judges = crate::graph::parse_judges(child, default_host)?;
                    if judges
                        .iter()
                        .any(|judge| matches!(judge, JudgeSpec::Deadline { .. }))
                    {
                        return Err(St3Error::new(
                            "invalid-plan-deadline",
                            format!("step `{path}` must use its timeout property"),
                        ));
                    }
                }
                "plan" => {
                    let mut plan = parse_plan(child, None, None, None, default_host, false)?;
                    rewrite_nested_paths(&mut plan, &path)?;
                    nested_plan = Some(Box::new(plan));
                }
                "retry" => retry = parse_retry(child)?,
                other => {
                    return Err(St3Error::new(
                        "unknown-step-field",
                        format!("step `{path}` cannot contain `{other}`"),
                    ));
                }
            }
        }
    }
    let mut step = StepSpec {
        id,
        path,
        title,
        goal,
        timeout_ms,
        retry,
        finally,
        assigned_to,
        dependencies,
        subgraph_kdl,
        products,
        judges,
        nested_plan,
        definition_hash: String::new(),
    };
    validate_variables(&serde_json::to_value(&step).map_err(internal)?)?;
    step.definition_hash = hash(&step)?;
    Ok(step)
}

fn rewrite_nested_paths(plan: &mut PlanSpec, parent: &str) -> Result<(), St3Error> {
    let old = std::mem::take(&mut plan.steps);
    let mut rewritten = BTreeMap::new();
    for (id, mut step) in old {
        step.path = format!("{parent}/{}/{}", plan.id, step.id);
        if let Some(nested) = step.nested_plan.as_mut() {
            rewrite_nested_paths(nested, &step.path)?;
        }
        step.definition_hash = hash(&step)?;
        rewritten.insert(id, step);
    }
    plan.steps = rewritten;
    plan.revision = hash(plan)?;
    Ok(())
}

fn parse_dependencies(node: &KdlNode, default_host: &str) -> Result<Vec<DependencySpec>, St3Error> {
    reject_type(node)?;
    let mut output = positional_strings(node)?
        .into_iter()
        .map(|step| DependencySpec::Step {
            step,
            state: "completed".into(),
        })
        .collect::<Vec<_>>();
    if let Some(children) = node.children() {
        for child in children.nodes() {
            if child.name().value() == "step" {
                let values = positional_strings(child)?;
                if values.is_empty() || values.len() > 2 {
                    return Err(St3Error::new(
                        "invalid-step-dependency",
                        "a step dependency needs an ID and an optional state",
                    ));
                }
                let state = values.get(1).cloned().unwrap_or_else(|| "completed".into());
                if !matches!(state.as_str(), "completed" | "failed" | "terminal") {
                    return Err(St3Error::new(
                        "invalid-step-dependency-state",
                        format!("step dependency state `{state}` is not registered"),
                    ));
                }
                output.push(DependencySpec::Step {
                    step: values[0].clone(),
                    state,
                });
            } else {
                let mut judges = KdlNode::new("judges");
                let mut body = KdlDocument::new();
                body.nodes_mut().push(child.clone());
                judges.set_children(body);
                let parsed = crate::graph::parse_judges(&judges, default_host)?;
                if parsed.len() != 1
                    || matches!(
                        parsed[0],
                        JudgeSpec::Deadline { .. }
                            | JudgeSpec::Mechanical { .. }
                            | JudgeSpec::Llm { .. }
                            | JudgeSpec::Human { .. }
                    )
                {
                    return Err(St3Error::new(
                        "invalid-dependency-predicate",
                        "depends-on accepts only graph predicates and step states",
                    ));
                }
                output.push(DependencySpec::Predicate {
                    judge: parsed[0].clone(),
                });
            }
        }
    }
    if output.is_empty() {
        return Err(St3Error::new(
            "empty-depends-on",
            "depends-on cannot be empty",
        ));
    }
    Ok(output)
}

fn parse_products(node: &KdlNode) -> Result<Vec<ProductSpec>, St3Error> {
    ensure_bare(node)?;
    let children = node.children().ok_or_else(|| {
        St3Error::new(
            "empty-produces",
            "produces must contain at least one graph shape",
        )
    })?;
    let mut output = Vec::new();
    let mut subjects = BTreeSet::new();
    for product in children.nodes() {
        reject_type(product)?;
        let kind = product.name().value();
        if !matches!(
            kind,
            "resource" | "message" | "agent" | "exec" | "pty" | "scope"
        ) {
            return Err(St3Error::new(
                "invalid-product-kind",
                format!("produces cannot match `{kind}`"),
            ));
        }
        let subject = normalize_subject(&first_string(product)?, kind);
        if !subjects.insert(subject.clone()) {
            return Err(St3Error::new(
                "duplicate-product",
                format!("produces repeats `{subject}`"),
            ));
        }
        let mut fields = BTreeMap::new();
        if let Some(body) = product.children() {
            for field in body.nodes() {
                reject_type(field)?;
                if field.children().is_some()
                    || field.entries().len() != 1
                    || field.entries()[0].name().is_some()
                {
                    return Err(St3Error::new(
                        "invalid-product-field",
                        format!(
                            "product field `{}` must contain one scalar value",
                            field.name().value()
                        ),
                    ));
                }
                fields.insert(
                    field.name().value().to_owned(),
                    json_value(field.entries()[0].value())?,
                );
            }
        }
        output.push(ProductSpec { subject, fields });
    }
    Ok(output)
}

fn parse_retry(node: &KdlNode) -> Result<RetrySpec, St3Error> {
    ensure_bare(node)?;
    let body = node
        .children()
        .ok_or_else(|| St3Error::new("empty-retry", "retry is empty"))?;
    let attempts = child_integer(body, "attempts")?.unwrap_or(1);
    if !(1..=100).contains(&attempts) {
        return Err(St3Error::new(
            "invalid-retry-attempts",
            "retry attempts must be between 1 and 100",
        ));
    }
    let backoff_ms = child_string(body, "backoff")?
        .map(|value| parse_duration(&value))
        .transpose()?
        .unwrap_or(0);
    Ok(RetrySpec {
        attempts: attempts as u32,
        backoff_ms,
    })
}

fn validate_dependencies(plan: &str, steps: &BTreeMap<String, StepSpec>) -> Result<(), St3Error> {
    for step in steps.values() {
        for dependency in &step.dependencies {
            if let DependencySpec::Step { step: target, .. } = dependency
                && !steps.contains_key(target)
            {
                return Err(St3Error::new(
                    "unknown-step-dependency",
                    format!(
                        "step `{}` in plan `{plan}` depends on unknown step `{target}`",
                        step.id
                    ),
                ));
            }
        }
    }
    fn visit(
        id: &str,
        steps: &BTreeMap<String, StepSpec>,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> Result<(), St3Error> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.to_owned()) {
            return Err(St3Error::new(
                "dependency-cycle",
                format!("the plan has a dependency cycle through step `{id}`"),
            ));
        }
        for dependency in &steps[id].dependencies {
            if let DependencySpec::Step { step, .. } = dependency {
                visit(step, steps, visiting, visited)?;
            }
        }
        visiting.remove(id);
        visited.insert(id.to_owned());
        Ok(())
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for id in steps.keys() {
        visit(id, steps, &mut visiting, &mut visited)?;
    }
    Ok(())
}

pub fn interpolate(source: &str, variables: &BTreeMap<String, String>) -> Result<String, St3Error> {
    let mut output = String::with_capacity(source.len());
    let mut rest = source;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let Some(end) = tail.find('}') else {
            return Err(St3Error::new(
                "invalid-variable",
                "a variable reference has no closing brace",
            ));
        };
        let name = &tail[..end];
        if !VARIABLES.contains(&name) {
            return Err(St3Error::new(
                "unknown-variable",
                format!("variable `{name}` is not registered"),
            ));
        }
        let value = variables.get(name).ok_or_else(|| {
            St3Error::new(
                "unavailable-variable",
                format!("variable `{name}` is not available in this phase"),
            )
        })?;
        output.push_str(value);
        rest = &tail[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}

fn validate_variables(value: &Value) -> Result<(), St3Error> {
    match value {
        Value::String(value) => {
            let mut rest = value.as_str();
            while let Some(start) = rest.find("${") {
                let tail = &rest[start + 2..];
                let Some(end) = tail.find('}') else {
                    return Err(St3Error::new(
                        "invalid-variable",
                        "a variable reference has no closing brace",
                    ));
                };
                let name = &tail[..end];
                if !VARIABLES.contains(&name) {
                    return Err(St3Error::new(
                        "unknown-variable",
                        format!("variable `{name}` is not registered"),
                    ));
                }
                rest = &tail[end + 1..];
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_variables(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_variables(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_plan_state(value: &str) -> Result<PlanState, St3Error> {
    match value {
        "draft" => Ok(PlanState::Draft),
        "ready" => Ok(PlanState::Ready),
        "retired" => Ok(PlanState::Retired),
        _ => Err(St3Error::new(
            "invalid-plan-state",
            format!("plan state `{value}` is not registered"),
        )),
    }
}

fn parse_change_policy(value: &str) -> Result<ChangePolicy, St3Error> {
    match value {
        "agent" => Ok(ChangePolicy::Agent),
        "supervisor" => Ok(ChangePolicy::Supervisor),
        "human-review" => Ok(ChangePolicy::HumanReview),
        _ => Err(St3Error::new(
            "invalid-change-policy",
            format!("change policy `{value}` is not registered"),
        )),
    }
}

fn normalize_subject(value: &str, kind: &str) -> String {
    if value.starts_with(&format!("{kind}/")) {
        value.to_owned()
    } else {
        format!("{kind}/{value}")
    }
}

fn normalize_assignee(value: &str, default_host: &str) -> String {
    let identity = value.strip_prefix("agent/").unwrap_or(value);
    if identity.contains('.') || identity.contains("${") {
        format!("agent/{identity}")
    } else {
        format!("agent/{default_host}.{identity}")
    }
}

fn validate_id(value: &str, kind: &str) -> Result<(), St3Error> {
    let invalid_path = kind != "plan" && value.contains('/');
    if value.is_empty()
        || value.len() > 160
        || invalid_path
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
        || value.chars().any(char::is_whitespace)
    {
        return Err(St3Error::new(
            "invalid-plan-id",
            format!("{kind} ID `{value}` is invalid"),
        ));
    }
    Ok(())
}

fn parse_duration(value: &str) -> Result<u64, St3Error> {
    let units = [
        ("ms", 1_u64),
        ("s", 1_000),
        ("m", 60_000),
        ("h", 3_600_000),
        ("d", 86_400_000),
    ];
    for (suffix, multiplier) in units {
        if let Some(number) = value.strip_suffix(suffix) {
            let number = number.parse::<u64>().map_err(|_| {
                St3Error::new("invalid-duration", format!("duration `{value}` is invalid"))
            })?;
            return number
                .checked_mul(multiplier)
                .ok_or_else(|| St3Error::new("invalid-duration", "the duration is too large"));
        }
    }
    Err(St3Error::new(
        "invalid-duration",
        format!("duration `{value}` needs ms, s, m, h, or d"),
    ))
}

fn hash(value: &impl Serialize) -> Result<String, St3Error> {
    let bytes = serde_json::to_vec(value).map_err(internal)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn internal(error: impl std::fmt::Display) -> St3Error {
    St3Error::new("internal", error.to_string())
}

fn reject_type(node: &KdlNode) -> Result<(), St3Error> {
    if node.ty().is_some() {
        Err(St3Error::new(
            "typed-node",
            "typed KDL nodes are not supported",
        ))
    } else {
        Ok(())
    }
}

fn ensure_bare(node: &KdlNode) -> Result<(), St3Error> {
    if node.ty().is_none() && node.entries().is_empty() {
        Ok(())
    } else {
        Err(St3Error::new(
            "invalid-node",
            format!(
                "`{}` must not have values or properties",
                node.name().value()
            ),
        ))
    }
}

fn ensure_only_properties(node: &KdlNode, allowed: &[&str]) -> Result<(), St3Error> {
    for entry in node.entries() {
        if let Some(name) = entry.name()
            && !allowed.contains(&name.value())
        {
            return Err(St3Error::new(
                "unknown-property",
                format!(
                    "`{}` has unknown property `{}`",
                    node.name().value(),
                    name.value()
                ),
            ));
        }
    }
    Ok(())
}

fn first_string(node: &KdlNode) -> Result<String, St3Error> {
    let values = positional_strings(node)?;
    if values.len() != 1 {
        return Err(St3Error::new(
            "invalid-value-count",
            format!("`{}` needs one string", node.name().value()),
        ));
    }
    Ok(values[0].clone())
}

fn positional_strings(node: &KdlNode) -> Result<Vec<String>, St3Error> {
    node.entries()
        .iter()
        .filter(|entry| entry.name().is_none())
        .map(|entry| match entry.value() {
            KdlValue::String(value) => Ok(value.clone()),
            _ => Err(St3Error::new(
                "invalid-value",
                format!("`{}` needs string values", node.name().value()),
            )),
        })
        .collect()
}

fn property_string(node: &KdlNode, name: &str) -> Result<Option<String>, St3Error> {
    match node.get(name) {
        None => Ok(None),
        Some(KdlValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(St3Error::new(
            "invalid-property",
            format!("property `{name}` must be a string"),
        )),
    }
}

fn property_bool(node: &KdlNode, name: &str) -> Result<Option<bool>, St3Error> {
    match node.get(name) {
        None => Ok(None),
        Some(KdlValue::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(St3Error::new(
            "invalid-property",
            format!("property `{name}` must be a boolean"),
        )),
    }
}

fn child_string(document: &KdlDocument, name: &str) -> Result<Option<String>, St3Error> {
    let nodes = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == name)
        .collect::<Vec<_>>();
    if nodes.len() > 1 {
        return Err(St3Error::new(
            "duplicate-field",
            format!("`{name}` repeats"),
        ));
    }
    nodes.first().map(|node| first_string(node)).transpose()
}

fn child_integer(document: &KdlDocument, name: &str) -> Result<Option<i64>, St3Error> {
    let nodes = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == name)
        .collect::<Vec<_>>();
    if nodes.len() > 1 {
        return Err(St3Error::new(
            "duplicate-field",
            format!("`{name}` repeats"),
        ));
    }
    let Some(node) = nodes.first() else {
        return Ok(None);
    };
    if node.entries().len() != 1 {
        return Err(St3Error::new(
            "invalid-field",
            format!("`{name}` needs one integer"),
        ));
    }
    match node.entries()[0].value() {
        KdlValue::Integer(value) => i64::try_from(*value)
            .map(Some)
            .map_err(|_| St3Error::new("invalid-field", format!("`{name}` is too large"))),
        _ => Err(St3Error::new(
            "invalid-field",
            format!("`{name}` needs one integer"),
        )),
    }
}

fn json_value(value: &KdlValue) -> Result<Value, St3Error> {
    match value {
        KdlValue::String(value) => Ok(Value::String(value.clone())),
        KdlValue::Bool(value) => Ok(Value::Bool(*value)),
        KdlValue::Integer(value) => i64::try_from(*value)
            .map(Value::from)
            .map_err(|_| St3Error::new("invalid-number", "an integer is too large")),
        KdlValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| St3Error::new("invalid-number", "a float must be finite")),
        KdlValue::Null => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_parallel_steps_nested_work_and_products() {
        let source = r#"
version 2
subgraph {
  scope "eval/demo/${PLAN_RUN}" retention="temporary" change-policy="agent" {
    plan "demo" state="ready" {
      step "start" {
        subgraph {
          agent "worker.${PLAN_RUN}" {
            workspace "."
            harness "codex" { prompt "Run durable work." }
          }
        }
      }
      step "one" {
        assigned-to "agent/worker.${PLAN_RUN}"
        depends-on { step "start" completed }
        plan "work" { step "inspect" { } }
        produces {
          resource "plan-run/${PLAN_RUN}/change" { kind "vcs.revision"; state "published" }
        }
      }
      step "two" { depends-on { step "start" completed } }
      step "join" { depends-on { step "one" completed; step "two" completed } }
      step "cleanup" finally=#true { subgraph { scope "eval/demo/${PLAN_RUN}" { stop } } }
    }
  }
}
"#;
        let intent = crate::graph::parse_intent(source, "node").unwrap();
        let plan = &intent.plans["demo"];
        assert_eq!(plan.steps.len(), 5);
        assert_eq!(
            plan.scope_template.as_deref(),
            Some("scope/eval/demo/${PLAN_RUN}")
        );
        assert!(plan.steps["one"].nested_plan.is_some());
        assert_eq!(
            plan.steps["one"].products[0].subject,
            "resource/plan-run/${PLAN_RUN}/change"
        );
    }

    #[test]
    fn rejects_checkpoint_and_dependency_cycles() {
        let old =
            crate::graph::parse_intent("version 2\nsubgraph { checkpoints \"old\" { } }", "node")
                .unwrap_err();
        assert_eq!(old.code, "unknown-node");
        let cycle = crate::graph::parse_intent(
            "version 2\nsubgraph {\n  plan \"cycle\" state=\"ready\" {\n    step \"a\" { depends-on \"b\" }\n    step \"b\" { depends-on \"a\" }\n  }\n}\n",
            "node",
        )
        .unwrap_err();
        assert_eq!(cycle.code, "dependency-cycle");
    }

    #[test]
    fn parses_a_human_review_contract() {
        let intent = crate::graph::parse_intent(
            r#"
version 2
subgraph {
  plan "review" state="ready" {
    step "approval" {
      judges {
        human "person/nathan" {
          question "Is this change ready to merge?"
          review "resource/plan-run/${PLAN_RUN}/pull-request"
          review "doc/reports/run@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
      }
    }
  }
}
"#,
            "node",
        )
        .unwrap();
        let judge = &intent.plans["review"].steps["approval"].judges[0];
        let crate::model::JudgeSpec::Human {
            reviewer,
            question,
            review_targets,
        } = judge
        else {
            panic!("the judge is not human");
        };
        assert_eq!(reviewer, "person/nathan");
        assert_eq!(question.as_deref(), Some("Is this change ready to merge?"));
        assert_eq!(
            review_targets,
            &[
                "resource/plan-run/${PLAN_RUN}/pull-request",
                "doc/reports/run@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ]
        );
    }
}
