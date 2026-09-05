//! `st2 catalog migrate-ids` — freeze every legacy subject's immutable agent ID in one transaction.
//!
//! Decision 0015 separates the immutable catalog-global agent ID from the mutable address that the
//! positional `identity` currently serves as. Migration is the step that makes an existing catalog
//! expressible in that model without re-keying any durable state: a live subject freezes its
//! existing `<host>.<identity>` bus identity as its explicit `id`, so every runtime identifier,
//! task ID, socket path, and declaration-anchored state path keeps its exact bytes. A structurally
//! archived subject freezes the same bytes when they remain unique across the combined
//! live-and-archived subject set; an archived collision receives a generated UUIDv7 in both its
//! declaration and its tombstone, and the reassignment is recorded durably so a reader of a
//! version-1 durable record never retypes colliding bytes into the wrong subject.
//!
//! Supervisor references resolve against the combined *pre-migration* index and are rewritten to
//! the parent's migrated ID inside the same transaction. A missing or ambiguous reference refuses
//! before any write: the operator unarchives and repairs that declaration through the ordinary
//! pre-activation authoring path, then retries.
//!
//! The transaction is the same shape `catalog_archive` uses — one exclusive authoring lock, one
//! strict discovery, a pure plan, then every write inside one generation commit. A durable marker
//! makes an interrupted run resumable rather than indeterminate.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kdl::{KdlDocument, KdlNode};
use serde::{Deserialize, Serialize};

use crate::agent_author::{
    agent_identity_parts, exact_agent_node, insert_node, is_nix_managed, quoted,
};
use crate::catalog_archive::{self, TOMBSTONE_SCHEMA, Tombstone};
use crate::catalog_lock::{CONTROL_DIR, CatalogLock};
use crate::catalog_transaction;

pub const MIGRATE_SCHEMA: &str = "st2.catalog-migrate-ids.v1";
/// The durable record of every legacy bus identity migration reassigned.
pub const MIGRATION_RECORD_SCHEMA: &str = "st2.agent-id-migration.v1";
pub const MARKER_SCHEMA: &str = "st2.catalog-migrate-ids-incomplete.v1";

const MARKER_FILE: &str = "migrate-ids-incomplete";
const MIGRATION_RECORD_FILE: &str = "agent-id-migration.json";
const TOMBSTONE_SUFFIX: &str = ".tombstone.json";
const DECLARATION_STEMS: [&str; 1] = ["agent"];
const KDL_EXTENSION: &str = "kdl";
const FOREIGN_EXTENSIONS: [&str; 2] = ["toml", "json"];

#[derive(Debug, Clone)]
pub struct MigrateRequest {
    pub catalog: PathBuf,
    /// Host used to freeze a `<host>.<identity>` bus identity for a declaration that omits `host`,
    /// and to resolve a bare-identity supervisor reference.
    pub host: String,
    pub dry_run: bool,
    pub resume: bool,
}

/// Which plane a subject's declaration lives in. Both are migrated in the same transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Plane {
    Live,
    Archived,
}

/// Where a subject's frozen ID came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdSource {
    /// The subject's existing `<host>.<identity>` bus identity, frozen verbatim.
    FrozenBusIdentity,
    /// A generated UUIDv7, because the bus identity was already claimed in the combined set.
    GeneratedUuidV7,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlannedId {
    pub agent_id: String,
    pub host: String,
    pub identity: String,
    pub plane: Plane,
    pub source: IdSource,
    /// Catalog-relative declaration path, so a moved catalog stays readable.
    pub declaration: String,
}

/// One legacy bus identity that migration could not give to both claimants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reassignment {
    pub legacy_bus_identity: String,
    /// The subject that kept the colliding bytes as its own immutable ID.
    pub kept_by_agent_id: String,
    pub kept_by_plane: Plane,
    /// The immutable ID the other claimant received instead.
    pub reassigned_agent_id: String,
    pub reassigned_host: String,
    pub reassigned_identity: String,
    pub reassigned_plane: Plane,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRewrite {
    pub declaration: String,
    pub host: String,
    pub identity: String,
    /// The legacy reference as written.
    pub from: String,
    /// The parent's migrated immutable ID.
    pub to_agent_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrateStatus {
    Migrated,
    Unchanged,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateResult {
    pub schema: &'static str,
    pub host: String,
    pub dry_run: bool,
    pub resumed: bool,
    pub status: MigrateStatus,
    /// The catalog generation after the transaction, or the observed one for a dry run.
    pub generation: Option<u64>,
    pub assigned: Vec<PlannedId>,
    pub reassigned: Vec<Reassignment>,
    pub supervisor_rewrites: Vec<SupervisorRewrite>,
    /// Subjects that already carry an explicit `id`; their bytes are left untouched.
    pub already_migrated: Vec<PlannedId>,
    /// Migrated declarations that carry `meta { managed-by "nix" }`. Their upstream generator must
    /// emit `id` before the next activation re-projects the file without one.
    pub nix_owned: Vec<String>,
}

/// The durable reassignment record consulted by readers of version-1 durable records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationRecord {
    pub schema: String,
    pub migrated_at_ms: u64,
    pub reassigned: Vec<Reassignment>,
}

/// Read the durable reassignment record, if this catalog has one.
///
/// An absent record means no legacy bus identity was reassigned, which is the ordinary case: it is
/// written only when an archived subject's bytes were already claimed. A record whose schema this
/// version does not own is an error rather than an empty answer — silently reading it as "nothing
/// was reassigned" would let a reader retype colliding legacy bytes into the wrong subject, which
/// is exactly what the record exists to prevent.
pub fn read_migration_record(catalog: &Path) -> Result<Option<MigrationRecord>> {
    let path = migration_record_path(catalog);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect agent-id migration record {}", path.display()));
        }
    };
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "agent-id migration record is not a real regular file"
    );
    let bytes = fs::read(&path)
        .with_context(|| format!("read agent-id migration record {}", path.display()))?;
    let record: MigrationRecord =
        serde_json::from_slice(&bytes).context("parse agent-id migration record")?;
    anyhow::ensure!(
        record.schema == MIGRATION_RECORD_SCHEMA,
        "unknown agent-id migration record schema '{}'",
        record.schema
    );
    Ok(Some(record))
}

/// The immutable ID a version-1 durable record's legacy endpoint denotes for `state_owner`.
///
/// Migration froze most legacy bus identities as the ID of the subject that already held them, so
/// those bytes need no translation. A reassigned identity denotes two subjects, and the record's
/// own state owner is the only endpoint whose subject is provable: the sender for a sender-owned
/// row, the recipient for an inbox row. Any other colliding endpoint is unattributed
/// (`MESSAGE-R04`), and this returns `None` for it so the caller renders the legacy bytes as a
/// historical address rather than addressing the live replacement.
pub fn attribute_legacy_endpoint(
    record: Option<&MigrationRecord>,
    endpoint: &str,
    state_owner: Option<&str>,
) -> Option<String> {
    let reassigned = record.and_then(|record| {
        record
            .reassigned
            .iter()
            .find(|entry| entry.legacy_bus_identity == endpoint)
    });
    let Some(reassigned) = reassigned else {
        // Not reassigned: the bytes are the keeping subject's frozen ID.
        return Some(endpoint.to_owned());
    };
    match state_owner {
        Some(owner) if owner == endpoint => Some(reassigned.kept_by_agent_id.clone()),
        _ => None,
    }
}

pub fn migration_record_path(catalog: &Path) -> PathBuf {
    catalog.join(CONTROL_DIR).join(MIGRATION_RECORD_FILE)
}

pub fn marker_path(catalog: &Path) -> PathBuf {
    catalog.join(CONTROL_DIR).join(MARKER_FILE)
}

/// One subject in the combined pre-migration index.
#[derive(Debug, Clone)]
struct Subject {
    plane: Plane,
    host: String,
    identity: String,
    declaration: PathBuf,
    declared_id: Option<String>,
    supervisor: Option<String>,
    nix_owned: bool,
    /// The tombstone beside an archived subject's directory.
    tombstone: Option<PathBuf>,
}

impl Subject {
    fn bus_identity(&self) -> String {
        format!("{}.{}", self.host, self.identity)
    }
}

/// One text edit to apply to one agent node.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Edit {
    /// Insert `id "<agent-id>"` into an agent that has none.
    InsertId(String),
    /// Rewrite `supervisor "<legacy>"` to the parent's migrated ID.
    RewriteSupervisor { from: String, to: String },
}

/// Every edit one declaration file needs, keyed by the agent node it applies to.
#[derive(Debug, Clone, Default)]
struct FileEdits {
    /// `(host, identity, edit)` — the node selector plus what to do to it.
    edits: Vec<(String, String, Edit)>,
}

#[derive(Debug, Clone, Default)]
struct Plan {
    assigned: Vec<PlannedId>,
    already_migrated: Vec<PlannedId>,
    reassigned: Vec<Reassignment>,
    supervisor_rewrites: Vec<SupervisorRewrite>,
    nix_owned: Vec<String>,
    files: BTreeMap<PathBuf, FileEdits>,
    /// `(tombstone path, agent id)` for every archived subject whose tombstone must record its ID.
    tombstones: Vec<(PathBuf, String)>,
}

impl Plan {
    fn is_empty(&self) -> bool {
        self.files.is_empty() && self.tombstones.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Marker {
    schema: String,
    catalog: String,
    host: String,
    started_at_ms: u64,
    /// Every ID this transaction intends to freeze, so a resume cannot silently plan another one.
    assigned: Vec<PlannedId>,
    reassigned: Vec<Reassignment>,
    supervisor_rewrites: Vec<SupervisorRewrite>,
}

/// Migrate every live and structurally archived legacy subject in one transaction.
pub fn migrate_ids(request: MigrateRequest) -> Result<MigrateResult> {
    anyhow::ensure!(
        !(request.dry_run && request.resume),
        "--dry-run and --resume are mutually exclusive"
    );
    anyhow::ensure!(
        !request.host.is_empty() && !request.host.contains('/') && !request.host.contains('.'),
        "host '{}' must be one non-empty path component without `.`",
        request.host
    );
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    let lock = CatalogLock::exclusive(&catalog)?;

    let existing_marker = read_marker(&catalog)?;
    if let Some(marker) = existing_marker.as_ref() {
        anyhow::ensure!(
            request.resume || request.dry_run,
            "a previous `st2 catalog migrate-ids` did not complete; rerun with --resume (marker: {})",
            marker_path(&catalog).display()
        );
        anyhow::ensure!(
            marker.catalog == catalog.display().to_string(),
            "migration marker names catalog '{}', not {}",
            marker.catalog,
            catalog.display()
        );
    } else {
        anyhow::ensure!(
            !request.resume,
            "nothing to resume: no migration marker at {}",
            marker_path(&catalog).display()
        );
    }

    let subjects = collect_subjects(&catalog, &request.host)?;
    let plan = plan(&catalog, &request.host, &subjects)?;

    if let Some(marker) = existing_marker.as_ref() {
        verify_resumable(marker, &plan)?;
    }

    if request.dry_run || plan.is_empty() {
        return Ok(MigrateResult {
            schema: MIGRATE_SCHEMA,
            host: request.host,
            dry_run: request.dry_run,
            resumed: request.resume,
            status: if plan.is_empty() {
                MigrateStatus::Unchanged
            } else {
                MigrateStatus::Migrated
            },
            generation: lock.generation()?,
            assigned: plan.assigned,
            reassigned: plan.reassigned,
            supervisor_rewrites: plan.supervisor_rewrites,
            already_migrated: plan.already_migrated,
            nix_owned: plan.nix_owned,
        });
    }

    // Prove the catalog admits BEFORE rewriting a byte. Migration re-admits the whole live plane
    // after its writes, and a plane that already fails admission would fail that re-admission for
    // a reason migration did not cause — leaving every declaration rewritten, the generation
    // unmoved, and a marker whose resume can only fail the same way. Refusing up front keeps the
    // pre-existing fault the operator's to repair, exactly as `catalog apply` does.
    catalog_transaction::validate_full_catalog(&catalog).context(
        "refusing to migrate: the catalog does not currently admit, so a rewritten plane could not be re-admitted either; repair the declarations named above and retry",
    )?;

    if existing_marker.is_none() {
        write_marker(
            &lock,
            &catalog,
            &Marker {
                schema: MARKER_SCHEMA.to_owned(),
                catalog: catalog.display().to_string(),
                host: request.host.clone(),
                started_at_ms: crate::message::now_ms(),
                assigned: plan.assigned.clone(),
                reassigned: plan.reassigned.clone(),
                supervisor_rewrites: plan.supervisor_rewrites.clone(),
            },
        )?;
    }

    let generation = lock.begin_generation_commit()?;
    // Declarations before tombstones: a crash between them leaves a migrated declaration whose
    // tombstone has not caught up, which `--resume` finishes. The opposite order would leave a
    // tombstone advertising an ID its declaration does not carry.
    for (path, edits) in &plan.files {
        apply_file_edits(&lock, &catalog, path, edits)?;
        test_checkpoint("migrate-ids-declaration-written");
    }
    for (path, agent_id) in &plan.tombstones {
        apply_tombstone_id(&lock, &catalog, path, agent_id)?;
    }
    if !plan.reassigned.is_empty() {
        write_migration_record(
            &lock,
            &catalog,
            &MigrationRecord {
                schema: MIGRATION_RECORD_SCHEMA.to_owned(),
                migrated_at_ms: crate::message::now_ms(),
                reassigned: plan.reassigned.clone(),
            },
        )?;
    }
    // Re-admit the whole live plane before the generation moves: a migration that produced an
    // inadmissible catalog must fail with its marker intact rather than publish a broken plane.
    catalog_transaction::validate_full_catalog(&catalog)
        .context("migrated catalog fails full validation")?;
    reparse_archived(&plan)?;
    generation.commit()?;
    clear_marker(&catalog)?;

    Ok(MigrateResult {
        schema: MIGRATE_SCHEMA,
        host: request.host,
        dry_run: false,
        resumed: request.resume,
        status: MigrateStatus::Migrated,
        generation: lock.generation()?,
        assigned: plan.assigned,
        reassigned: plan.reassigned,
        supervisor_rewrites: plan.supervisor_rewrites,
        already_migrated: plan.already_migrated,
        nix_owned: plan.nix_owned,
    })
}

/// Build the combined pre-migration live-and-archived subject index.
///
/// Both halves must be complete. A declaration that failed to parse could be the one naming a
/// subject as its `supervisor`, and an unexplained archive entry could be a subject whose bytes a
/// live freeze would then steal — so partial input refuses rather than migrating a guess.
fn collect_subjects(catalog: &Path, host: &str) -> Result<Vec<Subject>> {
    let found = crate::discover_strict(catalog);
    anyhow::ensure!(
        found.errors.is_empty(),
        "refusing to migrate: catalog discovery is incomplete, so a subject or supervisor reference could be hidden:\n{}",
        found
            .errors
            .iter()
            .map(|error| format!("  {}: {}", error.path.display(), error.message))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let mut subjects = Vec::new();
    for spec in &found.specs {
        let declaration = spec.path.clone();
        subjects.push(Subject {
            plane: Plane::Live,
            host: spec.resolved_host(host).to_owned(),
            identity: spec.identity.clone(),
            declaration: declaration.clone(),
            declared_id: spec.id.clone(),
            supervisor: spec.supervisor.clone(),
            nix_owned: declaration_is_nix_owned(&declaration, &spec.identity)?,
            tombstone: None,
        });
    }

    let observation = catalog_archive::observe(catalog)?;
    anyhow::ensure!(
        observation.issues.is_empty(),
        "refusing to migrate: the structural archive has unexplained state, so a frozen ID could collide with a subject this run cannot see:\n{}",
        observation
            .issues
            .iter()
            .map(|issue| format!("  {}: {}", issue.path, issue.message))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let archive_root = catalog_archive::archive_root(catalog);
    for tombstone in &observation.archived {
        let directory = archive_root.join(&tombstone.host).join(&tombstone.identity);
        let declaration = archived_declaration(&directory).with_context(|| {
            format!(
                "locate the archived declaration of {}.{}",
                tombstone.host, tombstone.identity
            )
        })?;
        let (declared_id, supervisor, nix_owned) = archived_declaration_facts(
            &declaration,
            &tombstone.host,
            &tombstone.identity,
        )?;
        subjects.push(Subject {
            plane: Plane::Archived,
            host: tombstone.host.clone(),
            identity: tombstone.identity.clone(),
            declaration,
            declared_id,
            supervisor,
            nix_owned,
            tombstone: Some(
                archive_root
                    .join(&tombstone.host)
                    .join(format!("{}{TOMBSTONE_SUFFIX}", tombstone.identity)),
            ),
        });
    }
    Ok(subjects)
}

/// The canonical declaration inside an archived identity directory.
fn archived_declaration(directory: &Path) -> Result<PathBuf> {
    for stem in DECLARATION_STEMS {
        let kdl = directory.join(format!("{stem}.{KDL_EXTENSION}"));
        if kdl.is_file() {
            return Ok(kdl);
        }
        for extension in FOREIGN_EXTENSIONS {
            let foreign = directory.join(format!("{stem}.{extension}"));
            if foreign.is_file() {
                return Ok(foreign);
            }
        }
    }
    anyhow::bail!(
        "archived identity {} holds no `agent.{{kdl,toml,json}}` declaration",
        directory.display()
    )
}

/// `(declared id, supervisor, nix-owned)` of one archived declaration.
///
/// Archived declarations are structurally undiscoverable — `.st2` is excluded at every depth — so
/// they are read directly rather than through catalog discovery.
fn archived_declaration_facts(
    declaration: &Path,
    host: &str,
    identity: &str,
) -> Result<(Option<String>, Option<String>, bool)> {
    if declaration.extension().and_then(|ext| ext.to_str()) != Some(KDL_EXTENSION) {
        // A foreign-format archived declaration is readable but not editable here; `plan` turns
        // that into a refusal only if the subject actually needs an edit.
        return Ok((None, None, false));
    }
    let text = fs::read_to_string(declaration)
        .with_context(|| format!("read archived declaration {}", declaration.display()))?;
    let document = KdlDocument::parse(&text)
        .map_err(|error| anyhow::anyhow!("parse {}: {error}", declaration.display()))?;
    let node = exact_agent_node(&document, identity, host, identity)
        .map_err(|error| anyhow::anyhow!("{}: {error}", declaration.display()))?;
    Ok((
        child_string(node, "id"),
        child_string(node, "supervisor"),
        is_nix_managed(node),
    ))
}

fn child_string(node: &KdlNode, name: &str) -> Option<String> {
    node.children()
        .into_iter()
        .flat_map(|children| children.nodes())
        .find(|child| child.name().value() == name)
        .and_then(|child| child.get(0))
        .and_then(|value| value.as_string())
        .map(str::to_owned)
}

fn declaration_is_nix_owned(declaration: &Path, identity: &str) -> Result<bool> {
    if declaration.extension().and_then(|ext| ext.to_str()) != Some(KDL_EXTENSION) {
        return Ok(false);
    }
    let text = fs::read_to_string(declaration)
        .with_context(|| format!("read declaration {}", declaration.display()))?;
    let document = KdlDocument::parse(&text)
        .map_err(|error| anyhow::anyhow!("parse {}: {error}", declaration.display()))?;
    Ok(document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "agent")
        .filter(|node| {
            let (_, declared) = agent_identity_parts(node);
            declared.as_deref() == Some(identity) || declared.is_none()
        })
        .any(is_nix_managed))
}

/// Decide every ID and every rewrite without touching a byte.
fn plan(catalog: &Path, host: &str, subjects: &[Subject]) -> Result<Plan> {
    // Live first: a live subject always keeps its own bus identity, so its claim is decided before
    // any archived subject can compete for the same bytes.
    let mut ordered: Vec<&Subject> = subjects.iter().collect();
    ordered.sort_by(|left, right| {
        left.plane
            .cmp(&right.plane)
            .then_with(|| left.host.cmp(&right.host))
            .then_with(|| left.identity.cmp(&right.identity))
    });

    let mut claimed: BTreeMap<String, (String, Plane)> = BTreeMap::new();
    // Explicit IDs already in the catalog are claimed before anything is frozen: a legacy freeze
    // may never take an ID a migrated subject already owns.
    for subject in &ordered {
        if let Some(id) = subject.declared_id.as_deref() {
            if let Some((_, plane)) = claimed.insert(id.to_owned(), (id.to_owned(), subject.plane)) {
                anyhow::bail!(
                    "refusing to migrate: agent id '{id}' is declared by more than one subject (second: {} {}.{}, first plane {plane:?})",
                    plane_label(subject.plane),
                    subject.host,
                    subject.identity
                );
            }
        }
    }

    let mut plan = Plan::default();
    let mut assigned_ids: BTreeMap<String, String> = BTreeMap::new();
    for subject in &ordered {
        let bus_identity = subject.bus_identity();
        let planned = PlannedId {
            agent_id: String::new(),
            host: subject.host.clone(),
            identity: subject.identity.clone(),
            plane: subject.plane,
            source: IdSource::FrozenBusIdentity,
            declaration: relative(catalog, &subject.declaration),
        };
        if let Some(id) = subject.declared_id.as_deref() {
            plan.already_migrated.push(PlannedId {
                agent_id: id.to_owned(),
                ..planned
            });
            assigned_ids.insert(bus_identity, id.to_owned());
            continue;
        }
        let (agent_id, source) = match claimed.get(&bus_identity) {
            None => (bus_identity.clone(), IdSource::FrozenBusIdentity),
            Some((kept_by, kept_plane)) => {
                anyhow::ensure!(
                    subject.plane == Plane::Archived,
                    "refusing to migrate: live subject {bus_identity} cannot freeze its bus identity because '{kept_by}' is already claimed; resolve the duplicate declaration first"
                );
                let generated = crate::uuid_v7::uuid_v7()?;
                plan.reassigned.push(Reassignment {
                    legacy_bus_identity: bus_identity.clone(),
                    kept_by_agent_id: kept_by.clone(),
                    kept_by_plane: *kept_plane,
                    reassigned_agent_id: generated.clone(),
                    reassigned_host: subject.host.clone(),
                    reassigned_identity: subject.identity.clone(),
                    reassigned_plane: subject.plane,
                });
                (generated, IdSource::GeneratedUuidV7)
            }
        };
        agent_spec::validate_agent_id(&agent_id).with_context(|| {
            format!(
                "refusing to migrate {}: the frozen id is not a usable agent id",
                subject.declaration.display()
            )
        })?;
        anyhow::ensure!(
            claimed
                .insert(agent_id.clone(), (agent_id.clone(), subject.plane))
                .is_none(),
            "refusing to migrate: generated agent id '{agent_id}' collides with an existing one"
        );
        assigned_ids.insert(bus_identity, agent_id.clone());

        refuse_foreign_format(&subject.declaration)?;
        plan.files
            .entry(subject.declaration.clone())
            .or_default()
            .edits
            .push((
                subject.host.clone(),
                subject.identity.clone(),
                Edit::InsertId(agent_id.clone()),
            ));
        if subject.nix_owned {
            plan.nix_owned.push(relative(catalog, &subject.declaration));
        }
        if let Some(tombstone) = subject.tombstone.as_ref() {
            plan.tombstones
                .push((tombstone.clone(), agent_id.clone()));
        }
        plan.assigned.push(PlannedId {
            agent_id,
            source,
            ..planned
        });
    }

    // Supervisor references resolve against the combined pre-migration index — bus identities as
    // written — and are rewritten to the parent's migrated ID.
    let index: BTreeMap<String, &Subject> = subjects
        .iter()
        .map(|subject| (subject.bus_identity(), subject))
        .collect();
    for subject in &ordered {
        let Some(reference) = subject.supervisor.as_deref() else {
            continue;
        };
        let parent = resolve_supervisor(&index, reference, &subject.host, host).with_context(
            || {
                format!(
                    "legacy-supervisor-unresolved: {} declares supervisor '{reference}'",
                    relative(catalog, &subject.declaration)
                )
            },
        )?;
        let parent_id = assigned_ids
            .get(&parent.bus_identity())
            .cloned()
            .with_context(|| {
                format!(
                    "legacy-supervisor-unresolved: {} declares supervisor '{reference}', whose subject has no migrated id",
                    relative(catalog, &subject.declaration)
                )
            })?;
        if reference == parent_id {
            continue;
        }
        refuse_foreign_format(&subject.declaration)?;
        plan.files
            .entry(subject.declaration.clone())
            .or_default()
            .edits
            .push((
                subject.host.clone(),
                subject.identity.clone(),
                Edit::RewriteSupervisor {
                    from: reference.to_owned(),
                    to: parent_id.clone(),
                },
            ));
        plan.supervisor_rewrites.push(SupervisorRewrite {
            declaration: relative(catalog, &subject.declaration),
            host: subject.host.clone(),
            identity: subject.identity.clone(),
            from: reference.to_owned(),
            to_agent_id: parent_id,
        });
    }

    plan.nix_owned.sort();
    plan.nix_owned.dedup();
    Ok(plan)
}

/// A reference matches a full `<host>.<identity>` bus identity, or a bare identity on the
/// referring declaration's own resolved host. Absence and ambiguity both refuse.
fn resolve_supervisor<'a>(
    index: &BTreeMap<String, &'a Subject>,
    reference: &str,
    referring_host: &str,
    default_host: &str,
) -> Result<&'a Subject> {
    let mut candidates: Vec<&&Subject> = Vec::new();
    if let Some(subject) = index.get(reference) {
        candidates.push(subject);
    }
    for host in [referring_host, default_host] {
        if let Some(subject) = index.get(&format!("{host}.{reference}")) {
            candidates.push(subject);
        }
    }
    candidates.dedup_by(|left, right| {
        left.bus_identity() == right.bus_identity() && left.plane == right.plane
    });
    match candidates.as_slice() {
        [subject] => Ok(subject),
        [] => anyhow::bail!(
            "no live or archived subject matches it; unarchive and repair the declaration, then retry"
        ),
        _ => anyhow::bail!(
            "it matches {} subjects; unarchive and repair the declaration, then retry",
            candidates.len()
        ),
    }
}

fn refuse_foreign_format(declaration: &Path) -> Result<()> {
    let extension = declaration.extension().and_then(|ext| ext.to_str());
    anyhow::ensure!(
        extension == Some(KDL_EXTENSION),
        "unsupported-declaration-format: {} is not canonical KDL, so migration cannot author its `id`; convert it to KDL first",
        declaration.display()
    );
    Ok(())
}

fn plane_label(plane: Plane) -> &'static str {
    match plane {
        Plane::Live => "live",
        Plane::Archived => "archived",
    }
}

/// A resume may only finish work the original transaction planned.
fn verify_resumable(marker: &Marker, plan: &Plan) -> Result<()> {
    let recorded: BTreeSet<(String, String, String)> = marker
        .assigned
        .iter()
        .map(|planned| {
            (
                planned.host.clone(),
                planned.identity.clone(),
                planned.agent_id.clone(),
            )
        })
        .collect();
    for planned in &plan.assigned {
        anyhow::ensure!(
            recorded.contains(&(
                planned.host.clone(),
                planned.identity.clone(),
                planned.agent_id.clone()
            )),
            "refusing to resume: {}.{} would now receive id '{}', which the interrupted transaction did not plan",
            planned.host,
            planned.identity,
            planned.agent_id
        );
    }
    // Every already-migrated subject the marker planned must carry exactly the planned ID: an
    // outside writer that gave it a different one makes the resume indeterminate.
    let applied: BTreeMap<(String, String), String> = plan
        .already_migrated
        .iter()
        .map(|planned| {
            (
                (planned.host.clone(), planned.identity.clone()),
                planned.agent_id.clone(),
            )
        })
        .collect();
    for planned in &marker.assigned {
        if let Some(observed) = applied.get(&(planned.host.clone(), planned.identity.clone())) {
            anyhow::ensure!(
                observed == &planned.agent_id,
                "refusing to resume: {}.{} carries id '{observed}', not the planned '{}'",
                planned.host,
                planned.identity,
                planned.agent_id
            );
        }
    }
    Ok(())
}

/// Apply every planned edit to one declaration file and publish it atomically.
///
/// Edits are applied one at a time against a freshly parsed document, because each insertion moves
/// every later source span. The file is re-parsed and re-checked before it is published.
fn apply_file_edits(
    lock: &CatalogLock,
    catalog: &Path,
    path: &Path,
    edits: &FileEdits,
) -> Result<()> {
    let original = fs::read_to_string(path)
        .with_context(|| format!("read declaration {}", path.display()))?;
    let mode = fs::symlink_metadata(path)
        .with_context(|| format!("stat declaration {}", path.display()))?;
    anyhow::ensure!(
        mode.is_file() && !mode.file_type().is_symlink(),
        "declaration is not a real regular file: {}",
        path.display()
    );

    let mut text = original.clone();
    for (host, identity, edit) in &edits.edits {
        let document = KdlDocument::parse(&text)
            .map_err(|error| anyhow::anyhow!("parse {}: {error}", path.display()))?;
        let node = exact_agent_node(&document, identity, host, identity)
            .map_err(|error| anyhow::anyhow!("{}: {error}", path.display()))?;
        text = match edit {
            Edit::InsertId(agent_id) => {
                let authored = format!(
                    "id {}",
                    quoted(agent_id).map_err(|error| anyhow::anyhow!("{error}"))?
                );
                insert_node(&text, node, &authored).map_err(|error| anyhow::anyhow!("{error}"))?
            }
            Edit::RewriteSupervisor { from, to } => {
                rewrite_supervisor(&text, node, from, to).with_context(|| {
                    format!("rewrite supervisor in {}", path.display())
                })?
            }
        };
    }

    verify_file(&text, path, edits)?;

    let directory = path
        .parent()
        .with_context(|| format!("declaration {} has no parent", path.display()))?;
    let control = catalog_transaction::retained_dir_path(lock.control())?;
    let mut temporary = tempfile::Builder::new()
        .prefix("catalog-migrate-ids-")
        .tempfile_in(&control)
        .with_context(|| format!("stage migrated declaration {}", path.display()))?;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;
    temporary
        .as_file_mut()
        .set_permissions(fs::Permissions::from_mode(mode.permissions().mode() & 0o7777))?;
    temporary.write_all(text.as_bytes())?;
    temporary.as_file().sync_all()?;
    // The exclusive authoring lock excludes cooperating writers, not a direct same-UID write, so
    // the exact preimage is rechecked immediately before publication.
    let observed = fs::read_to_string(path)
        .with_context(|| format!("re-read declaration {}", path.display()))?;
    anyhow::ensure!(
        observed == original,
        "declaration {} changed while the migration was authored",
        path.display()
    );
    catalog_transaction::persist_tempfile_from_control(
        lock.control(),
        catalog,
        temporary,
        path,
    )
    .with_context(|| format!("publish migrated declaration {}", path.display()))?;
    catalog_transaction::open_dir_beneath(catalog, directory)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("sync declaration directory {}", directory.display()))?;
    Ok(())
}

/// Replace the `supervisor` value in place, preserving every other byte.
fn rewrite_supervisor(text: &str, node: &KdlNode, from: &str, to: &str) -> Result<String> {
    let children = node
        .children()
        .context("agent declares no child block, so it cannot declare a supervisor")?;
    let matches = children
        .nodes()
        .iter()
        .filter(|child| child.name().value() == "supervisor")
        .collect::<Vec<_>>();
    let [supervisor] = matches.as_slice() else {
        anyhow::bail!(
            "agent declares `supervisor` {} times; exactly one is required to rewrite it",
            matches.len()
        );
    };
    anyhow::ensure!(
        supervisor.children().is_none()
            && supervisor.entries().len() == 1
            && supervisor.entries()[0].name().is_none(),
        "`supervisor` must contain exactly one positional string"
    );
    anyhow::ensure!(
        supervisor.get(0).and_then(|value| value.as_string()) == Some(from),
        "`supervisor` no longer reads '{from}'"
    );
    let span = supervisor.entries()[0].span();
    let range = span.offset()..span.offset() + span.len();
    anyhow::ensure!(
        text.get(range.clone()).is_some(),
        "supervisor value span falls outside the declaration"
    );
    let mut replacement = text.to_owned();
    replacement.replace_range(
        range,
        &quoted(to).map_err(|error| anyhow::anyhow!("{error}"))?,
    );
    Ok(replacement)
}

/// Prove the edited text parses and says exactly what the plan intended.
fn verify_file(text: &str, path: &Path, edits: &FileEdits) -> Result<()> {
    let document = KdlDocument::parse(text)
        .map_err(|error| anyhow::anyhow!("migration produced invalid KDL in {}: {error}", path.display()))?;
    for (host, identity, edit) in &edits.edits {
        let node = exact_agent_node(&document, identity, host, identity)
            .map_err(|error| anyhow::anyhow!("{}: {error}", path.display()))?;
        match edit {
            Edit::InsertId(agent_id) => {
                let observed = node
                    .children()
                    .into_iter()
                    .flat_map(|children| children.nodes())
                    .filter(|child| child.name().value() == "id")
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    observed.len() == 1,
                    "migration produced {} `id` fields in {}",
                    observed.len(),
                    path.display()
                );
                anyhow::ensure!(
                    observed[0].get(0).and_then(|value| value.as_string()) == Some(agent_id.as_str()),
                    "migration did not write id '{agent_id}' in {}",
                    path.display()
                );
            }
            Edit::RewriteSupervisor { to, .. } => {
                anyhow::ensure!(
                    child_string(node, "supervisor").as_deref() == Some(to.as_str()),
                    "migration did not rewrite supervisor to '{to}' in {}",
                    path.display()
                );
            }
        }
    }
    Ok(())
}

/// Record an archived subject's migrated ID in its tombstone.
fn apply_tombstone_id(
    lock: &CatalogLock,
    catalog: &Path,
    path: &Path,
    agent_id: &str,
) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("read tombstone {}", path.display()))?;
    let mut tombstone: Tombstone =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    anyhow::ensure!(
        tombstone.schema == TOMBSTONE_SCHEMA,
        "unknown archive tombstone schema '{}'",
        tombstone.schema
    );
    if tombstone.agent_id.as_deref() == Some(agent_id) {
        return Ok(());
    }
    tombstone.agent_id = Some(agent_id.to_owned());
    let mut serialized = serde_json::to_vec_pretty(&tombstone)?;
    serialized.push(b'\n');
    write_control_file(lock, catalog, path, &serialized)
}

fn write_migration_record(
    lock: &CatalogLock,
    catalog: &Path,
    record: &MigrationRecord,
) -> Result<()> {
    let mut serialized = serde_json::to_vec_pretty(record)?;
    serialized.push(b'\n');
    write_control_file(lock, catalog, &migration_record_path(catalog), &serialized)
}

/// Atomically replace one file beneath the catalog through the retained control capability.
fn write_control_file(
    lock: &CatalogLock,
    catalog: &Path,
    path: &Path,
    bytes: &[u8],
) -> Result<()> {
    let directory = path
        .parent()
        .with_context(|| format!("{} has no parent", path.display()))?;
    let control = catalog_transaction::retained_dir_path(lock.control())?;
    let mut temporary = tempfile::Builder::new()
        .prefix("catalog-migrate-ids-")
        .tempfile_in(&control)
        .with_context(|| format!("stage {}", path.display()))?;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;
    temporary
        .as_file_mut()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    catalog_transaction::persist_tempfile_from_control(lock.control(), catalog, temporary, path)
        .with_context(|| format!("publish {}", path.display()))?;
    catalog_transaction::open_dir_beneath(catalog, directory)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("sync {}", directory.display()))?;
    Ok(())
}

/// Prove every rewritten archived declaration still parses as exactly one agent.
///
/// Archived declarations are outside `validate_full_catalog`'s projection by construction, so they
/// get their own re-admission pass rather than none.
fn reparse_archived(plan: &Plan) -> Result<()> {
    for (tombstone, agent_id) in &plan.tombstones {
        let directory = tombstone
            .parent()
            .and_then(|host_root| {
                tombstone
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_suffix(TOMBSTONE_SUFFIX))
                    .map(|identity| host_root.join(identity))
            })
            .with_context(|| format!("derive archived directory from {}", tombstone.display()))?;
        let declaration = archived_declaration(&directory)?;
        let text = fs::read_to_string(&declaration)
            .with_context(|| format!("re-read archived declaration {}", declaration.display()))?;
        let document = KdlDocument::parse(&text).map_err(|error| {
            anyhow::anyhow!(
                "migrated archived declaration {} is invalid KDL: {error}",
                declaration.display()
            )
        })?;
        let agents = document
            .nodes()
            .iter()
            .filter(|node| node.name().value() == "agent")
            .collect::<Vec<_>>();
        anyhow::ensure!(
            agents.len() == 1,
            "archived declaration {} holds {} agents; exactly one is required",
            declaration.display(),
            agents.len()
        );
        anyhow::ensure!(
            child_string(agents[0], "id").as_deref() == Some(agent_id.as_str()),
            "migrated archived declaration {} does not carry id '{agent_id}'",
            declaration.display()
        );
    }
    Ok(())
}

fn read_marker(catalog: &Path) -> Result<Option<Marker>> {
    let path = marker_path(catalog);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {}", path.display()));
        }
    };
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "migration marker is not a real regular file: {}",
        path.display()
    );
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let marker: Marker =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    anyhow::ensure!(
        marker.schema == MARKER_SCHEMA,
        "unknown migration marker schema '{}'",
        marker.schema
    );
    Ok(Some(marker))
}

fn write_marker(lock: &CatalogLock, catalog: &Path, marker: &Marker) -> Result<()> {
    let mut serialized = serde_json::to_vec_pretty(marker)?;
    serialized.push(b'\n');
    write_control_file(lock, catalog, &marker_path(catalog), &serialized)?;
    test_checkpoint("migrate-ids-marker-written");
    Ok(())
}

fn clear_marker(catalog: &Path) -> Result<()> {
    let path = marker_path(catalog);
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("clear {}", path.display())),
    }
    catalog_transaction::sync_dir(&catalog.join(CONTROL_DIR))
}

fn relative(catalog: &Path, path: &Path) -> String {
    path.strip_prefix(catalog)
        .map(|relative| relative.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

#[cfg(debug_assertions)]
fn test_checkpoint(point: &str) {
    if std::env::var("ST2_TEST_MIGRATE_IDS_ABORT_AT").ok().as_deref() == Some(point) {
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
fn test_checkpoint(_point: &str) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(entries: &[(&str, &str, &str)]) -> MigrationRecord {
        MigrationRecord {
            schema: MIGRATION_RECORD_SCHEMA.to_owned(),
            migrated_at_ms: 1,
            reassigned: entries
                .iter()
                .map(|(legacy, kept, reassigned)| Reassignment {
                    legacy_bus_identity: (*legacy).to_owned(),
                    kept_by_agent_id: (*kept).to_owned(),
                    kept_by_plane: Plane::Live,
                    reassigned_agent_id: (*reassigned).to_owned(),
                    reassigned_host: "h".to_owned(),
                    reassigned_identity: "gone".to_owned(),
                    reassigned_plane: Plane::Archived,
                })
                .collect(),
        }
    }

    /// Migration froze most legacy bus identities as the ID of the subject that already held them,
    /// so an untouched endpoint needs no translation and no record lookup.
    #[test]
    fn an_untouched_legacy_endpoint_is_its_own_migrated_id() {
        let record = record(&[("h.gone", "h.gone", "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1")]);
        assert_eq!(
            attribute_legacy_endpoint(Some(&record), "h.other", Some("h.other")).as_deref(),
            Some("h.other")
        );
        // No record at all means nothing was ever reassigned.
        assert_eq!(
            attribute_legacy_endpoint(None, "h.gone", Some("h.gone")).as_deref(),
            Some("h.gone")
        );
    }

    /// A reassigned identity denotes two subjects. Only the row's own state owner is provable, and
    /// every other colliding endpoint stays unattributed rather than addressing the live
    /// replacement (`MESSAGE-R04`).
    #[test]
    fn a_reassigned_legacy_endpoint_resolves_only_for_the_rows_state_owner() {
        let record = record(&[("h.gone", "h.gone", "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1")]);
        assert_eq!(
            attribute_legacy_endpoint(Some(&record), "h.gone", Some("h.gone")).as_deref(),
            Some("h.gone"),
            "the state owner's own endpoint resolves to the keeping subject"
        );
        assert_eq!(
            attribute_legacy_endpoint(Some(&record), "h.gone", Some("h.reader")),
            None,
            "the same bytes at the other endpoint are unattributed"
        );
        assert_eq!(
            attribute_legacy_endpoint(Some(&record), "h.gone", None),
            None,
            "an ownerless row cannot attribute colliding bytes either"
        );
    }

    /// A reference matches a full bus identity or a bare identity on the referring host; both
    /// readings existing at once is undecidable rather than first-wins.
    #[test]
    fn supervisor_resolution_is_fail_closed_on_absence_and_ambiguity() {
        let subject = |host: &str, identity: &str| Subject {
            plane: Plane::Live,
            host: host.to_owned(),
            identity: identity.to_owned(),
            declaration: PathBuf::from(format!("agents/{host}/{identity}/agent.kdl")),
            declared_id: None,
            supervisor: None,
            nix_owned: false,
            tombstone: None,
        };
        let subjects = vec![
            subject("h", "root"),
            subject("h", "a.b"),
            subject("a", "b"),
            subject("h", "only-here"),
        ];
        let index: BTreeMap<String, &Subject> = subjects
            .iter()
            .map(|subject| (subject.bus_identity(), subject))
            .collect();

        // A bare identity on the referring host.
        assert_eq!(
            resolve_supervisor(&index, "only-here", "h", "h")
                .unwrap()
                .identity,
            "only-here"
        );
        // A fully qualified bus identity.
        assert_eq!(
            resolve_supervisor(&index, "h.only-here", "h", "h")
                .unwrap()
                .identity,
            "only-here"
        );
        // Absent.
        assert!(resolve_supervisor(&index, "ghost", "h", "h").is_err());
        // Both readings exist: host `a`'s `b`, and host `h`'s dotted identity `a.b`.
        let error = resolve_supervisor(&index, "a.b", "h", "h").unwrap_err();
        assert!(
            format!("{error}").contains("matches 2 subjects"),
            "{error}"
        );
    }

    /// A resume may only finish work the interrupted transaction planned.
    #[test]
    fn a_resume_refuses_an_id_the_interrupted_transaction_did_not_plan() {
        let planned = |identity: &str, agent_id: &str| PlannedId {
            agent_id: agent_id.to_owned(),
            host: "h".to_owned(),
            identity: identity.to_owned(),
            plane: Plane::Live,
            source: IdSource::FrozenBusIdentity,
            declaration: format!("agents/h/{identity}/agent.kdl"),
        };
        let marker = Marker {
            schema: MARKER_SCHEMA.to_owned(),
            catalog: "/catalog".to_owned(),
            host: "h".to_owned(),
            started_at_ms: 1,
            assigned: vec![planned("worker", "h.worker")],
            reassigned: Vec::new(),
            supervisor_rewrites: Vec::new(),
        };

        let mut plan = Plan {
            assigned: vec![planned("worker", "h.worker")],
            ..Plan::default()
        };
        verify_resumable(&marker, &plan).expect("the planned assignment resumes");

        plan.assigned = vec![planned("worker", "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1")];
        let error = verify_resumable(&marker, &plan).unwrap_err();
        assert!(format!("{error}").contains("did not plan"), "{error}");

        // An outside writer that gave a planned subject a different id makes the resume
        // indeterminate rather than silently correct.
        plan.assigned = Vec::new();
        plan.already_migrated = vec![planned("worker", "someone-elses-id")];
        let error = verify_resumable(&marker, &plan).unwrap_err();
        assert!(format!("{error}").contains("not the planned"), "{error}");
    }
}
