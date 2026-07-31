//! Experimental, read-only plan discovery and inspection.
//!
//! Plans remain ordinary KDL and referenced files. This module parses intent, validates declared
//! version links, and derives the frontier; it never executes, schedules, reconciles, or writes.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use kdl::{KdlDocument, KdlEntry, KdlNode};
use serde::Serialize;

/// A normalized set of plans selected from one catalog folder or KDL file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanCatalog {
    pub plans: Vec<Plan>,
}

/// One explicit plan, independent of its directory name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub identity: String,
    pub owner: String,
    pub versions: Vec<PlanVersion>,
    pub frontier: Vec<String>,
    pub source: PathBuf,
    pub source_kind: PlanSourceKind,
    pub referenced_by: Vec<String>,
}

/// Whether a plan is a top-level declaration or nested in an agent declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlanSourceKind {
    External,
    Inline,
}

/// One declared intent revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanVersion {
    pub identity: String,
    pub parents: Vec<String>,
    pub why: Option<String>,
    pub resource: String,
    pub resolved_resource: PathBuf,
}

/// A classified plan parse or validation error.
#[derive(Debug)]
pub struct PlanError {
    code: &'static str,
    path: PathBuf,
    message: String,
}

impl PlanError {
    fn new(code: &'static str, path: &Path, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.to_path_buf(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.path.display(), self.message)
    }
}

impl std::error::Error for PlanError {}

#[derive(Debug)]
struct PlanReference {
    owner: String,
    target: PathBuf,
}

/// Parse, normalize, and validate plans without mutating the selected files.
pub fn load(selected: &Path) -> Result<PlanCatalog, PlanError> {
    let selected = selected.canonicalize().map_err(|error| {
        PlanError::new(
            "selection-read-failed",
            selected,
            format!("reading selected plan input: {error}"),
        )
    })?;
    let boundary = if selected.is_dir() {
        selected.clone()
    } else {
        selected
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| PlanError::new("invalid-selection", &selected, "input has no parent"))?
    };
    let files = collect_kdl_files(&selected, &boundary)?;
    let mut plans = Vec::new();
    let mut external_by_source: BTreeMap<PathBuf, Vec<usize>> = BTreeMap::new();
    let mut references = Vec::new();

    for path in files {
        parse_seed(
            &path,
            &boundary,
            &mut plans,
            &mut external_by_source,
            &mut references,
        )?;
    }

    for reference in references {
        if !external_by_source.contains_key(&reference.target) {
            parse_external_file(
                &reference.target,
                &boundary,
                &mut plans,
                &mut external_by_source,
            )?;
        }
        let matches = &external_by_source[&reference.target];
        if matches.len() != 1 {
            return Err(PlanError::new(
                "ambiguous-plan-reference",
                &reference.target,
                "a plan-ref target must contain exactly one top-level plan",
            ));
        }
        plans[matches[0]].referenced_by.push(reference.owner);
    }

    let mut identities = BTreeMap::new();
    for (index, plan) in plans.iter().enumerate() {
        if let Some(previous) = identities.insert(plan.identity.clone(), index) {
            return Err(PlanError::new(
                "duplicate-plan-identity",
                &plan.source,
                format!(
                    "plan '{}' is also declared in {}",
                    plan.identity,
                    plans[previous].source.display()
                ),
            ));
        }
    }
    for plan in &mut plans {
        plan.referenced_by.sort();
        plan.referenced_by.dedup();
    }
    plans.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(PlanCatalog { plans })
}

fn collect_kdl_files(selected: &Path, boundary: &Path) -> Result<Vec<PathBuf>, PlanError> {
    if selected.is_file() {
        if selected.extension().and_then(|value| value.to_str()) != Some("kdl") {
            return Err(PlanError::new(
                "unsupported-plan-input",
                selected,
                "plan input must be a catalog folder or KDL file",
            ));
        }
        return Ok(vec![selected.to_path_buf()]);
    }
    if !selected.is_dir() {
        return Err(PlanError::new(
            "invalid-selection",
            selected,
            "plan input is neither a folder nor a file",
        ));
    }

    fn walk(root: &Path, directory: &Path, out: &mut Vec<PathBuf>) -> Result<(), PlanError> {
        let entries = fs::read_dir(directory).map_err(|error| {
            PlanError::new(
                "selection-read-failed",
                directory,
                format!("reading plan input folder: {error}"),
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                PlanError::new(
                    "selection-read-failed",
                    directory,
                    format!("reading plan input entry: {error}"),
                )
            })?;
            let path = entry.path();
            if !crate::discovery::is_catalog_path(root, &path) {
                continue;
            }
            let kind = entry.file_type().map_err(|error| {
                PlanError::new(
                    "selection-read-failed",
                    &path,
                    format!("reading plan input type: {error}"),
                )
            })?;
            if kind.is_dir() {
                walk(root, &path, out)?;
            } else if kind.is_file()
                && path.extension().and_then(|value| value.to_str()) == Some("kdl")
            {
                out.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    walk(boundary, selected, &mut files)?;
    files.sort();
    Ok(files)
}

fn parse_seed(
    path: &Path,
    boundary: &Path,
    plans: &mut Vec<Plan>,
    external_by_source: &mut BTreeMap<PathBuf, Vec<usize>>,
    references: &mut Vec<PlanReference>,
) -> Result<(), PlanError> {
    let document = read_document(path)?;
    for node in document.nodes() {
        match node.name().value() {
            "plan" => {
                let plan = parse_plan(node, path, boundary, PlanSourceKind::External, None)?;
                let index = plans.len();
                plans.push(plan);
                external_by_source
                    .entry(path.to_path_buf())
                    .or_default()
                    .push(index);
            }
            "agent" => parse_agent_plans(node, path, boundary, plans, references)?,
            _ => {}
        }
    }
    Ok(())
}

fn parse_external_file(
    path: &Path,
    boundary: &Path,
    plans: &mut Vec<Plan>,
    external_by_source: &mut BTreeMap<PathBuf, Vec<usize>>,
) -> Result<(), PlanError> {
    let document = read_document(path)?;
    for node in document.nodes() {
        if node.name().value() == "plan" {
            let plan = parse_plan(node, path, boundary, PlanSourceKind::External, None)?;
            let index = plans.len();
            plans.push(plan);
            external_by_source
                .entry(path.to_path_buf())
                .or_default()
                .push(index);
        }
    }
    if !external_by_source.contains_key(path) {
        return Err(PlanError::new(
            "missing-plan",
            path,
            "plan-ref target contains no top-level plan",
        ));
    }
    Ok(())
}

fn read_document(path: &Path) -> Result<KdlDocument, PlanError> {
    let text = fs::read_to_string(path).map_err(|error| {
        PlanError::new("plan-read-failed", path, format!("reading KDL: {error}"))
    })?;
    KdlDocument::parse(&text).map_err(|error| {
        PlanError::new("malformed-plan-kdl", path, format!("parsing KDL: {error}"))
    })
}

fn parse_agent_plans(
    agent: &KdlNode,
    source: &Path,
    boundary: &Path,
    plans: &mut Vec<Plan>,
    references: &mut Vec<PlanReference>,
) -> Result<(), PlanError> {
    let Some(children) = agent.children() else {
        return Ok(());
    };
    let owner = explicit_agent_identity(agent);
    for child in children.nodes() {
        match child.name().value() {
            "plan" => plans.push(parse_plan(
                child,
                source,
                boundary,
                PlanSourceKind::Inline,
                Some(required_inline_owner(owner.as_deref(), source)?),
            )?),
            "plan-ref" => {
                let target = exact_string_node(child, source, "plan-ref")?;
                let target = resolve_file_reference(source, boundary, &target, "plan-ref")?;
                if target.extension().and_then(|value| value.to_str()) != Some("kdl") {
                    return Err(PlanError::new(
                        "invalid-plan-reference",
                        source,
                        "plan-ref must resolve to a KDL file",
                    ));
                }
                references.push(PlanReference {
                    owner: required_inline_owner(owner.as_deref(), source)?,
                    target,
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn required_inline_owner(owner: Option<&str>, source: &Path) -> Result<String, PlanError> {
    owner.map(str::to_string).ok_or_else(|| {
        PlanError::new(
            "inline-owner-required",
            source,
            "an agent with inline plans or plan-ref must declare an explicit identity",
        )
    })
}

fn explicit_agent_identity(agent: &KdlNode) -> Option<String> {
    let mut identity = positional_string(agent);
    if let Some(children) = agent.children() {
        for child in children.nodes() {
            if child.name().value() == "identity" {
                identity = positional_string(child).or(identity);
            }
        }
    }
    identity.filter(|value| valid_text(value))
}

fn parse_plan(
    node: &KdlNode,
    source: &Path,
    boundary: &Path,
    source_kind: PlanSourceKind,
    inline_owner: Option<String>,
) -> Result<Plan, PlanError> {
    let identity = exact_positional_with_properties(node, source, "plan", &[])?
        .0
        .ok_or_else(|| {
            PlanError::new("plan-identity-required", source, "plan needs an identity")
        })?;
    if !valid_text(&identity) {
        return Err(PlanError::new(
            "invalid-plan-identity",
            source,
            "plan identity must be non-empty and contain no control characters",
        ));
    }
    let children = node.children().ok_or_else(|| {
        PlanError::new(
            "plan-body-required",
            source,
            format!("plan '{identity}' needs a body"),
        )
    })?;
    let mut owner = inline_owner;
    let mut versions = Vec::new();
    for child in children.nodes() {
        match child.name().value() {
            "owner" if source_kind == PlanSourceKind::External => {
                if owner.is_some() {
                    return Err(PlanError::new(
                        "duplicate-plan-owner",
                        source,
                        format!("plan '{identity}' declares owner more than once"),
                    ));
                }
                owner = Some(exact_string_node(child, source, "owner")?);
            }
            "owner" => {
                return Err(PlanError::new(
                    "inline-owner-is-derived",
                    source,
                    format!("inline plan '{identity}' derives owner from its containing agent"),
                ));
            }
            "version" => versions.push(parse_version(child, source, boundary, &identity)?),
            other => {
                return Err(PlanError::new(
                    "unsupported-plan-field",
                    source,
                    format!(
                        "plan '{identity}' contains unsupported '{other}'; execution, current pointers, steps, retries, claims, and schedules are outside this experiment"
                    ),
                ));
            }
        }
    }
    let owner = owner.ok_or_else(|| {
        PlanError::new(
            "plan-owner-required",
            source,
            format!("external plan '{identity}' needs one owner"),
        )
    })?;
    if !valid_text(&owner) {
        return Err(PlanError::new(
            "invalid-plan-owner",
            source,
            format!("plan '{identity}' owner must be non-empty"),
        ));
    }
    let frontier = validate_versions(source, &identity, &mut versions)?;
    Ok(Plan {
        identity,
        owner,
        versions,
        frontier,
        source: source.to_path_buf(),
        source_kind,
        referenced_by: Vec::new(),
    })
}

fn parse_version(
    node: &KdlNode,
    source: &Path,
    boundary: &Path,
    plan: &str,
) -> Result<PlanVersion, PlanError> {
    let (identity, properties) =
        exact_positional_with_properties(node, source, "version", &["resource"])?;
    let identity = identity.ok_or_else(|| {
        PlanError::new(
            "version-identity-required",
            source,
            format!("plan '{plan}' has a version without an identity"),
        )
    })?;
    if !valid_text(&identity) {
        return Err(PlanError::new(
            "invalid-version-identity",
            source,
            format!("plan '{plan}' has an invalid version identity"),
        ));
    }
    let resource = properties.get("resource").cloned().ok_or_else(|| {
        PlanError::new(
            "version-resource-required",
            source,
            format!("plan '{plan}' version '{identity}' needs resource=\"file:...\""),
        )
    })?;
    let resolved_resource =
        resolve_file_reference(source, boundary, &resource, "version resource")?;
    let mut parents = Vec::new();
    let mut why = None;
    if let Some(children) = node.children() {
        for child in children.nodes() {
            match child.name().value() {
                "parent" => parents.push(exact_string_node(child, source, "parent")?),
                "why" => {
                    if why.is_some() {
                        return Err(PlanError::new(
                            "duplicate-version-reason",
                            source,
                            format!(
                                "plan '{plan}' version '{identity}' declares why more than once"
                            ),
                        ));
                    }
                    why = Some(exact_string_node(child, source, "why")?);
                }
                other => {
                    return Err(PlanError::new(
                        "unsupported-version-field",
                        source,
                        format!(
                            "plan '{plan}' version '{identity}' contains unsupported '{other}'"
                        ),
                    ));
                }
            }
        }
    }
    parents.sort();
    if parents.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(PlanError::new(
            "duplicate-version-parent",
            source,
            format!("plan '{plan}' version '{identity}' repeats a parent"),
        ));
    }
    if !parents.is_empty() && why.as_ref().is_none_or(|value| !valid_text(value)) {
        return Err(PlanError::new(
            "revision-reason-required",
            source,
            format!("plan '{plan}' revision '{identity}' needs a non-empty why"),
        ));
    }
    Ok(PlanVersion {
        identity,
        parents,
        why,
        resource,
        resolved_resource,
    })
}

fn validate_versions(
    source: &Path,
    plan: &str,
    versions: &mut [PlanVersion],
) -> Result<Vec<String>, PlanError> {
    if versions.is_empty() {
        return Err(PlanError::new(
            "plan-version-required",
            source,
            format!("plan '{plan}' needs at least one version"),
        ));
    }
    let mut indices = BTreeMap::new();
    for (index, version) in versions.iter().enumerate() {
        if indices.insert(version.identity.clone(), index).is_some() {
            return Err(PlanError::new(
                "duplicate-version-identity",
                source,
                format!(
                    "plan '{plan}' declares version '{}' more than once",
                    version.identity
                ),
            ));
        }
    }
    let mut referenced = BTreeSet::new();
    for version in versions.iter() {
        for parent in &version.parents {
            if !indices.contains_key(parent) {
                return Err(PlanError::new(
                    "unknown-version-parent",
                    source,
                    format!(
                        "plan '{plan}' version '{}' has unknown parent '{parent}'",
                        version.identity
                    ),
                ));
            }
            referenced.insert(parent.clone());
        }
    }

    fn visit(
        index: usize,
        versions: &[PlanVersion],
        indices: &BTreeMap<String, usize>,
        marks: &mut [u8],
    ) -> bool {
        if marks[index] == 1 {
            return false;
        }
        if marks[index] == 2 {
            return true;
        }
        marks[index] = 1;
        for parent in &versions[index].parents {
            if !visit(indices[parent], versions, indices, marks) {
                return false;
            }
        }
        marks[index] = 2;
        true
    }
    let mut marks = vec![0; versions.len()];
    for index in 0..versions.len() {
        if !visit(index, versions, &indices, &mut marks) {
            return Err(PlanError::new(
                "version-cycle",
                source,
                format!("plan '{plan}' version parents contain a cycle"),
            ));
        }
    }

    let frontier = indices
        .keys()
        .filter(|identity| !referenced.contains(*identity))
        .cloned()
        .collect();
    versions.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(frontier)
}

fn exact_string_node(node: &KdlNode, source: &Path, field: &str) -> Result<String, PlanError> {
    let (value, properties) = exact_positional_with_properties(node, source, field, &[])?;
    if !properties.is_empty() || node.children().is_some() {
        return Err(PlanError::new(
            "invalid-plan-field",
            source,
            format!("{field} must be one string without properties or children"),
        ));
    }
    value.filter(|value| valid_text(value)).ok_or_else(|| {
        PlanError::new(
            "invalid-plan-field",
            source,
            format!("{field} must be one non-empty string"),
        )
    })
}

fn exact_positional_with_properties(
    node: &KdlNode,
    source: &Path,
    field: &str,
    allowed_properties: &[&str],
) -> Result<(Option<String>, BTreeMap<String, String>), PlanError> {
    let mut positional = None;
    let mut properties = BTreeMap::new();
    for entry in node.entries() {
        match entry.name() {
            None if positional.is_none() => {
                positional = Some(entry_string(entry).ok_or_else(|| {
                    PlanError::new(
                        "invalid-plan-field",
                        source,
                        format!("{field} positional value must be a string"),
                    )
                })?)
            }
            None => {
                return Err(PlanError::new(
                    "invalid-plan-field",
                    source,
                    format!("{field} accepts exactly one positional string"),
                ));
            }
            Some(name) if allowed_properties.contains(&name.value()) => {
                let name = name.value().to_string();
                let value = entry_string(entry).ok_or_else(|| {
                    PlanError::new(
                        "invalid-plan-field",
                        source,
                        format!("{field} property '{name}' must be a string"),
                    )
                })?;
                if properties.insert(name.clone(), value).is_some() {
                    return Err(PlanError::new(
                        "invalid-plan-field",
                        source,
                        format!("{field} property '{name}' is duplicated"),
                    ));
                }
            }
            Some(name) => {
                return Err(PlanError::new(
                    "unsupported-plan-property",
                    source,
                    format!("{field} contains unsupported property '{}'", name.value()),
                ));
            }
        }
    }
    Ok((positional, properties))
}

fn entry_string(entry: &KdlEntry) -> Option<String> {
    entry.value().as_string().map(str::to_string)
}

fn positional_string(node: &KdlNode) -> Option<String> {
    node.entries()
        .iter()
        .find(|entry| entry.name().is_none())
        .and_then(entry_string)
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
}

fn resolve_file_reference(
    source: &Path,
    boundary: &Path,
    reference: &str,
    field: &str,
) -> Result<PathBuf, PlanError> {
    let relative = reference.strip_prefix("file:").ok_or_else(|| {
        PlanError::new(
            "unsupported-plan-reference",
            source,
            format!("{field} must use a relative file: reference"),
        )
    })?;
    if relative.is_empty() || relative.starts_with("//") || Path::new(relative).is_absolute() {
        return Err(PlanError::new(
            "nonrelative-plan-reference",
            source,
            format!("{field} must be relative to its declaring KDL file"),
        ));
    }
    let candidate = source
        .parent()
        .unwrap_or(boundary)
        .join(relative)
        .canonicalize()
        .map_err(|error| {
            PlanError::new(
                "missing-plan-resource",
                source,
                format!("resolving {field} '{reference}': {error}"),
            )
        })?;
    if !candidate.starts_with(boundary) {
        return Err(PlanError::new(
            "plan-reference-escape",
            source,
            format!("{field} '{reference}' resolves outside the selected catalog"),
        ));
    }
    if !candidate.is_file() {
        return Err(PlanError::new(
            "invalid-plan-resource",
            source,
            format!("{field} '{reference}' does not resolve to a regular file"),
        ));
    }
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, relative: &str, contents: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn external_and_inline_forms_normalize_and_preserve_concurrent_frontier_heads() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "agent.kdl",
            r#"
agent "worker" {
  plan-ref "file:plans/shared/plan.kdl"
  plan "local" {
    version "v0" resource="file:plans/local-v0.md"
  }
}
"#,
        );
        write(root, "plans/local-v0.md", "# Local\n");
        write(root, "plans/shared/v0.md", "# v0\n");
        write(root, "plans/shared/v1.md", "# v1\n");
        write(root, "plans/shared/v2.md", "# v2\n");
        write(
            root,
            "plans/shared/plan.kdl",
            r#"
plan "shared" {
  owner "cos"
  version "v2" resource="file:v2.md" {
    parent "v0"
    why "Second concurrent direction."
  }
  version "v0" resource="file:v0.md"
  version "v1" resource="file:v1.md" {
    parent "v0"
    why "First concurrent direction."
  }
}
"#,
        );

        let loaded = load(root).unwrap();
        assert_eq!(loaded.plans.len(), 2);
        let local = &loaded.plans[0];
        assert_eq!(local.identity, "local");
        assert_eq!(local.owner, "worker");
        assert_eq!(local.frontier, ["v0"]);
        assert_eq!(local.source_kind, PlanSourceKind::Inline);

        let shared = &loaded.plans[1];
        assert_eq!(shared.owner, "cos");
        assert_eq!(shared.frontier, ["v1", "v2"]);
        assert_eq!(shared.referenced_by, ["worker"]);
        assert_eq!(
            shared
                .versions
                .iter()
                .map(|version| version.identity.as_str())
                .collect::<Vec<_>>(),
            ["v0", "v1", "v2"]
        );
    }

    #[test]
    fn validation_rejects_mutable_current_execution_fields_and_bad_revision_graphs() {
        let temporary = tempfile::tempdir().unwrap();
        write(temporary.path(), "v0.md", "# v0\n");
        write(
            temporary.path(),
            "plan.kdl",
            r#"
plan "bad" {
  owner "cos"
  current "v0"
  version "v0" resource="file:v0.md"
}
"#,
        );
        assert_eq!(
            load(temporary.path()).unwrap_err().code(),
            "unsupported-plan-field"
        );

        write(
            temporary.path(),
            "plan.kdl",
            r#"
plan "bad" {
  owner "cos"
  version "v1" resource="file:v0.md" {
    parent "missing"
  }
}
"#,
        );
        assert_eq!(
            load(temporary.path()).unwrap_err().code(),
            "revision-reason-required"
        );

        write(
            temporary.path(),
            "plan.kdl",
            r#"
plan "bad" {
  owner "cos"
  version "v1" resource="file:v0.md" {
    parent "missing"
    why "Revision."
  }
}
"#,
        );
        assert_eq!(
            load(temporary.path()).unwrap_err().code(),
            "unknown-version-parent"
        );

        write(
            temporary.path(),
            "plan.kdl",
            r#"
plan "bad" {
  owner "cos"
  version "v0" resource="file:v0.md" {
    parent "v1"
    why "First half."
  }
  version "v1" resource="file:v0.md" {
    parent "v0"
    why "Second half."
  }
}
"#,
        );
        assert_eq!(load(temporary.path()).unwrap_err().code(), "version-cycle");
    }

    #[test]
    fn references_are_source_relative_bounded_and_read_only() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        write(
            root,
            "nested/agent.kdl",
            r#"
agent "worker" {
  plan "local" {
    version "v0" resource="file:body.md"
  }
}
"#,
        );
        write(root, "nested/body.md", "unchanged\n");
        let before = fs::read(root.join("nested/agent.kdl")).unwrap();
        let loaded = load(root).unwrap();
        assert_eq!(
            loaded.plans[0].versions[0].resolved_resource,
            root.join("nested/body.md").canonicalize().unwrap()
        );
        assert_eq!(fs::read(root.join("nested/agent.kdl")).unwrap(), before);
        assert_eq!(
            fs::read_to_string(root.join("nested/body.md")).unwrap(),
            "unchanged\n"
        );

        let bounded = root.join("bounded");
        fs::create_dir(&bounded).unwrap();
        fs::write(root.join("outside.md"), "outside\n").unwrap();
        write(
            &bounded,
            "plan.kdl",
            r#"
plan "escape" {
  owner "worker"
  version "v0" resource="file:../outside.md"
}
"#,
        );
        assert_eq!(load(&bounded).unwrap_err().code(), "plan-reference-escape");
    }
}
