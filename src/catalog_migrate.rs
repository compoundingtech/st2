//! Additive legacy explicit-ID migration: one catalog transaction that freezes every legacy
//! subject's existing bus identity as its explicit immutable agent ID.
//!
//! The migration is deliberately *additive*. A live legacy subject receives exactly the bytes its
//! runtime, task IDs, sockets, and declaration-anchored state already use
//! (`<resolved-host>.<identity>`), so nothing moves: `AgentSpec::agent_id` returns the same value
//! before and after the field appears. A structurally archived subject freezes the same bytes when
//! they are still unused across the combined live-and-archived subject set; an archived collision
//! receives a generated UUIDv7 in **both** its declaration and its tombstone, because two subjects
//! cannot share one catalog-global ID.
//!
//! An archived collision means some version-1 durable record carries bytes that now belong to a
//! different subject than the one that wrote them. Migration therefore records every reassignment
//! durably (see [`LegacyIdCollisions`]) so a tolerant reader can refuse to retype those bytes into
//! the wrong subject instead of guessing.
//!
//! Supervisor references resolve against the combined **pre-migration** live-and-archived subject
//! index and are rewritten to the parent's migrated ID in the same transaction. A missing or
//! ambiguous reference refuses before any write with [`LEGACY_SUPERVISOR_UNRESOLVED`]; the
//! operator unarchives and repairs that declaration through the pre-activation legacy authoring
//! path, then retries.
//!
//! Transactional shape reuses the R27 machinery already in this crate: the exclusive authoring
//! lock, complete prospective validation before the first byte is written, the durable
//! incomplete-generation intent that fences readers across the commit and is recovered by the next
//! exclusive writer, source-preserving span-bounded KDL edits, and fsync + atomic rename out of
//! the control plane. Recovery needs no separate replay stage: every individual declaration write
//! is independently valid and the collision metadata is durable *before* the declarations it
//! explains, so re-running `migrate` completes an interrupted pass — and a completed pass is a
//! proven no-op.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use kdl::{KdlDocument, KdlNode};
use serde::{Deserialize, Serialize};

use crate::catalog_lock::{CONTROL_DIR, CatalogLock};
use crate::catalog_transaction::sync_dir;
use crate::{AgentId, legacy_bus_identity};

pub const MIGRATE_SCHEMA: &str = "st2.catalog-id-migration.v1";

/// Versioned schema identifier of the durable collision record.
pub const LEGACY_ID_COLLISIONS_SCHEMA: &str = "st2.catalog-legacy-id-collisions.v1";

/// The exact refusal code for an unresolvable pre-migration supervisor reference.
pub const LEGACY_SUPERVISOR_UNRESOLVED: &str = "legacy-supervisor-unresolved";

/// `<catalog>/.st2/legacy-id-collisions.json` — reserved control plane, never a declaration leaf.
const LEGACY_ID_COLLISIONS_FILE: &str = "legacy-id-collisions.json";

#[derive(Debug, Clone)]
pub struct MigrateRequest {
    pub catalog: PathBuf,
    /// The host used to resolve a declaration that omits `host`. Canonical declarations always
    /// carry one; this only closes the legacy path.
    pub host: String,
    /// Prove the plan without writing anything.
    pub dry_run: bool,
}

/// A classified migration refusal. `code` is stable for machine consumers.
#[derive(Debug)]
pub struct MigrateRefusal {
    pub code: &'static str,
    pub message: String,
}

impl MigrateRefusal {
    fn new(code: &'static str, message: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(Self {
            code,
            message: message.into(),
        })
    }
}

impl fmt::Display for MigrateRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for MigrateRefusal {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrateStatus {
    /// At least one declaration or tombstone changed.
    Migrated,
    /// Every live and archived declaration already carries an explicit ID and every supervisor
    /// reference is already ID-keyed. Nothing was written.
    Unchanged,
}

impl MigrateStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Migrated => "migrated",
            Self::Unchanged => "unchanged",
        }
    }
}

impl fmt::Display for MigrateStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which plane a migrated declaration lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Plane {
    Live,
    Archived,
}

impl Plane {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Archived => "archived",
        }
    }
}

impl fmt::Display for Plane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One subject that received an explicit ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigratedSubject {
    pub plane: Plane,
    pub host: String,
    pub identity: String,
    pub id: AgentId,
    /// Catalog-relative declaration path.
    pub declaration: String,
    /// `true` when the frozen legacy bytes were already taken and a UUIDv7 was generated instead.
    pub generated: bool,
}

/// One supervisor reference rewritten to its parent's migrated ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRewrite {
    pub plane: Plane,
    pub host: String,
    pub identity: String,
    pub from: String,
    pub to: AgentId,
}

/// Stable machine-readable receipt from one migration pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateResult {
    pub schema: &'static str,
    pub status: MigrateStatus,
    pub host: String,
    pub dry_run: bool,
    pub migrated: Vec<MigratedSubject>,
    pub supervisors: Vec<SupervisorRewrite>,
    pub collisions: Vec<LegacyIdCollision>,
    /// Archived tombstones rewritten to agree with their declaration. Non-zero without any
    /// `migrated` entry means this pass converged an interrupted predecessor.
    pub tombstones_repaired: usize,
}

// ---- Durable collision record -----------------------------------------------------------------

/// One reassigned legacy bus identity.
///
/// `keeper` is the subject that kept those bytes as its immutable ID; `reassigned` are the
/// archived subjects that lost them and received generated IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyIdCollision {
    /// The `<host>.<identity>` bytes a version-1 durable record may carry.
    pub legacy_bus_identity: String,
    pub keeper: AgentId,
    pub reassigned: Vec<AgentId>,
}

/// Every legacy bus identity migration reassigned, keyed for tolerant readers of version-1
/// records.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyIdCollisions {
    pub schema: String,
    pub entries: Vec<LegacyIdCollision>,
}

/// What a legacy `<host>.<identity>` endpoint means after migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyEndpoint {
    /// No collision was recorded for these bytes, so they *are* the frozen agent ID of the subject
    /// that wrote them. A reader may retype them directly.
    Frozen,
    /// These bytes were contested at migration time. `keeper` owns them as its immutable ID;
    /// `reassigned` subjects wrote records with these bytes and no longer own them, so a reader
    /// must not retype the endpoint into any subject it cannot otherwise prove.
    Collision {
        keeper: AgentId,
        reassigned: Vec<AgentId>,
    },
}

/// `<catalog>/.st2/legacy-id-collisions.json`.
pub fn legacy_id_collisions_path(catalog: &Path) -> PathBuf {
    catalog
        .join(CONTROL_DIR)
        .join(LEGACY_ID_COLLISIONS_FILE)
}

/// Read the durable collision record.
///
/// Only a genuinely **absent** directory entry reads as empty — that is exactly the meaning a
/// pre-migration deployment has, and it makes every legacy endpoint [`LegacyEndpoint::Frozen`].
/// Every other outcome (an I/O or permission failure, an entry that is not a regular file, a
/// corrupt body, or a schema string this binary does not understand) is propagated, because
/// "empty" is the fail-OPEN answer: it would retype contested legacy bytes into whichever subject
/// kept them and hand an interrupted send, a reply, or a request attribution to the wrong keeper.
///
/// Existence is decided with `symlink_metadata`, never with the outcome of the read: a dangling
/// symlink makes `fs::read` report `NotFound` for an entry that demonstrably exists, so following
/// the read would let an alias forge "no collisions were ever recorded".
pub fn load_legacy_id_collisions(catalog: &Path) -> Result<LegacyIdCollisions> {
    let path = legacy_id_collisions_path(catalog);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => anyhow::ensure!(
            metadata.file_type().is_file(),
            "{}: the legacy-id-collision record is not a regular file ({:?}); refusing to \
             attribute legacy endpoints from a collision set it cannot read",
            path.display(),
            metadata.file_type()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LegacyIdCollisions {
                schema: LEGACY_ID_COLLISIONS_SCHEMA.to_owned(),
                entries: Vec::new(),
            });
        }
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!(
                "stat the legacy-id-collision record {}: refusing to attribute legacy endpoints \
                 from a collision set it cannot read",
                path.display()
            )));
        }
    }
    let bytes = fs::read(&path).with_context(|| {
        format!(
            "read the legacy-id-collision record {}: refusing to attribute legacy endpoints from \
             an unreadable collision set",
            path.display()
        )
    })?;
    let record: LegacyIdCollisions = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "parse the legacy-id-collision record {}: refusing to attribute legacy endpoints from \
             a corrupt collision set",
            path.display()
        )
    })?;
    anyhow::ensure!(
        record.schema == LEGACY_ID_COLLISIONS_SCHEMA,
        "{}: unsupported legacy-id-collision schema '{}' (this binary reads '{}'); refusing to \
         attribute legacy endpoints from a collision set it cannot read",
        path.display(),
        record.schema,
        LEGACY_ID_COLLISIONS_SCHEMA
    );
    Ok(record)
}

impl LegacyIdCollisions {
    /// How a tolerant reader must attribute one legacy `<host>.<identity>` endpoint.
    ///
    /// This is the collision-aware attribution boundary: a reader of a version-1 record calls it
    /// with the record's endpoint bytes and only attributes the record to a migrated ID when that
    /// ID independently owns the row. Anything else is a historical address with no reply or
    /// automation authority.
    pub fn attribution(&self, legacy_bus_identity: &str) -> LegacyEndpoint {
        match self
            .entries
            .iter()
            .find(|entry| entry.legacy_bus_identity == legacy_bus_identity)
        {
            Some(entry) => LegacyEndpoint::Collision {
                keeper: entry.keeper.clone(),
                reassigned: entry.reassigned.clone(),
            },
            None => LegacyEndpoint::Frozen,
        }
    }
}

// ---- Migration ---------------------------------------------------------------------------------

/// Whether this declaration set has already been migrated.
///
/// A migrated catalog has at least one declaration and every one of them carries an explicit `id`.
/// An empty catalog is not "migrated": there is nothing whose ID could have been frozen.
pub fn is_migrated(specs: &[agent_spec::spec::AgentSpec]) -> bool {
    !specs.is_empty() && specs.iter().all(|spec| spec.id.is_some())
}

/// One pre-migration subject in the combined live-and-archived index.
#[derive(Debug, Clone)]
struct Indexed {
    plane: Plane,
    path: PathBuf,
    host: String,
    identity: String,
    /// Effective address before migration: explicit `address`, else the positional identity.
    effective_address: String,
    /// The ID this subject had before migration: explicit `id`, else its legacy bus identity.
    prior_id: String,
    /// The ID it has after this transaction.
    migrated_id: AgentId,
    /// `true` when this pass assigns `migrated_id` for the first time.
    assigned: bool,
    /// `true` when `migrated_id` was generated because the legacy bytes were taken.
    generated: bool,
    supervisor: Option<String>,
    retired: bool,
    spec: agent_spec::spec::AgentSpec,
    /// The archived tombstone that also has to carry a generated ID.
    tombstone: Option<(PathBuf, crate::catalog_archive::Tombstone)>,
}

impl Indexed {
    fn legacy(&self) -> String {
        legacy_bus_identity(&self.host, &self.identity)
    }

    fn bus_address(&self) -> String {
        legacy_bus_identity(&self.host, &self.effective_address)
    }
}

/// Freeze every legacy subject's explicit ID and rewrite every supervisor reference, in one
/// transaction.
pub fn migrate(request: MigrateRequest) -> Result<MigrateResult> {
    let catalog = request
        .catalog
        .canonicalize()
        .with_context(|| format!("canonicalize catalog {}", request.catalog.display()))?;
    let lock = CatalogLock::exclusive(&catalog)?;

    let found = crate::discover_strict(&catalog);
    if !found.errors.is_empty() {
        return Err(MigrateRefusal::new(
            "catalog-incomplete",
            format!(
                "refusing to migrate: catalog discovery is incomplete, so a legacy subject or supervisor reference could be hidden:\n{}",
                found
                    .errors
                    .iter()
                    .map(|error| format!("  {}: {}", error.path.display(), error.message))
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
        ));
    }
    let archived = crate::catalog_archive::archived_declarations(&catalog)?;

    let mut index = plan(&request.host, &found.specs, &archived)?;
    let mut collisions = assign_ids(&mut index)?;
    let rewrites = resolve_supervisors(&mut index)?;

    validate_prospective(&index)?;

    let migrated = index
        .iter()
        .filter(|entry| entry.assigned)
        .map(|entry| MigratedSubject {
            plane: entry.plane,
            host: entry.host.clone(),
            identity: entry.identity.clone(),
            id: entry.migrated_id.clone(),
            declaration: relative(&catalog, &entry.path),
            generated: entry.generated,
        })
        .collect::<Vec<_>>();

    let edits = declaration_edits(&index, &rewrites)?;
    // Repairs are scheduled from what the archive actually says, not from "does this declaration
    // already carry an id". A pass interrupted between an archived declaration write and its
    // tombstone write leaves the two disagreeing while the declaration looks migrated; only the
    // observed disagreement can converge that state on a rerun.
    let tombstones = tombstone_repairs(&index);
    merge_collisions(&mut collisions, observed_collisions(&index));
    // A record this binary cannot read is a refusal, not an empty collision set: rewriting it
    // from "no collisions" would drop the reassignments a tolerant reader depends on.
    let collisions = pending_collisions(collisions, &load_legacy_id_collisions(&catalog)?);

    if edits.is_empty() && tombstones.is_empty() && collisions.is_empty() {
        return Ok(MigrateResult {
            schema: MIGRATE_SCHEMA,
            status: MigrateStatus::Unchanged,
            host: request.host,
            dry_run: request.dry_run,
            migrated,
            supervisors: rewrites,
            collisions,
            tombstones_repaired: 0,
        });
    }
    if request.dry_run {
        return Ok(MigrateResult {
            schema: MIGRATE_SCHEMA,
            status: MigrateStatus::Migrated,
            host: request.host,
            dry_run: true,
            migrated,
            supervisors: rewrites,
            collisions,
            tombstones_repaired: tombstones.len(),
        });
    }

    let control = crate::catalog_transaction::retained_dir_path(lock.control())?;
    let generation = lock.begin_generation_commit()?;
    // The record explains declarations, so it lands before them: an interrupted pass leaves a
    // catalog whose already-written IDs are all explained, and re-running completes the rest.
    if !collisions.is_empty() {
        write_collisions(&lock, &catalog, &control, &collisions)?;
    }
    for edit in &edits {
        write_declaration(&lock, &catalog, &control, &edit.path, &edit.bytes)?;
    }
    for (path, tombstone) in &tombstones {
        write_tombstone(&lock, &catalog, &control, path, tombstone)?;
    }
    verify(&catalog, &request.host, &index)?;
    generation.commit()?;

    Ok(MigrateResult {
        schema: MIGRATE_SCHEMA,
        status: MigrateStatus::Migrated,
        host: request.host,
        dry_run: false,
        migrated,
        supervisors: rewrites,
        collisions,
        tombstones_repaired: tombstones.len(),
    })
}

/// Build the combined pre-migration live-and-archived subject index.
fn plan(
    this_host: &str,
    specs: &[agent_spec::spec::AgentSpec],
    archived: &[crate::catalog_archive::ArchivedDeclaration],
) -> Result<Vec<Indexed>> {
    let mut index = Vec::with_capacity(specs.len() + archived.len());
    for spec in specs {
        index.push(entry(Plane::Live, spec, this_host, None)?);
    }
    for declaration in archived {
        index.push(entry(
            Plane::Archived,
            &declaration.spec,
            &declaration.host,
            declaration
                .tombstone
                .clone()
                .map(|tombstone| (declaration.tombstone_path.clone(), tombstone)),
        )?);
    }
    Ok(index)
}

fn entry(
    plane: Plane,
    spec: &agent_spec::spec::AgentSpec,
    this_host: &str,
    tombstone: Option<(PathBuf, crate::catalog_archive::Tombstone)>,
) -> Result<Indexed> {
    let prior_id = spec.agent_id(this_host);
    // An already migrated subject keeps exactly its declared ID; only an unmigrated one is
    // assigned below.
    let migrated_id = match &spec.id {
        Some(id) => id.clone(),
        None => AgentId::parse(&prior_id).map_err(|error| {
            MigrateRefusal::new(
                "unmigratable-legacy-identity",
                format!("{prior_id} cannot become an explicit agent ID: {error}"),
            )
        })?,
    };
    Ok(Indexed {
        plane,
        path: spec.path.clone(),
        host: spec.resolved_host(this_host).to_owned(),
        identity: spec.identity.clone(),
        effective_address: spec.effective_address().to_owned(),
        prior_id,
        migrated_id,
        assigned: false,
        generated: false,
        supervisor: spec.supervisor.clone(),
        retired: spec.desired_state.is_retired(),
        spec: spec.clone(),
        tombstone,
    })
}

/// Freeze live legacy bytes, then place archived subjects, generating an ID for a collision.
fn assign_ids(index: &mut [Indexed]) -> Result<Vec<LegacyIdCollision>> {
    let mut taken: BTreeSet<String> = index
        .iter()
        .filter(|entry| entry.spec.id.is_some())
        .map(|entry| entry.migrated_id.as_str().to_owned())
        .collect();

    // Live first, unconditionally: a live subject's runtime, task IDs, and declaration-anchored
    // state already use these bytes, so they are not negotiable.
    let mut order: Vec<usize> = (0..index.len()).collect();
    order.sort_by_key(|&position| {
        (
            matches!(index[position].plane, Plane::Archived),
            index[position].host.clone(),
            index[position].identity.clone(),
            index[position].path.clone(),
        )
    });

    let mut contested: BTreeMap<String, Vec<AgentId>> = BTreeMap::new();
    for position in order {
        if index[position].spec.id.is_some() {
            continue;
        }
        let legacy = index[position].legacy();
        if taken.insert(legacy.clone()) {
            index[position].assigned = true;
            continue;
        }
        if matches!(index[position].plane, Plane::Live) {
            // Only an archived subject may be moved off its legacy bytes. A live subject's bytes
            // are load-bearing runtime identity, so a live collision is a catalog to repair.
            return Err(MigrateRefusal::new(
                "identity-not-unique",
                format!(
                    "live subject {legacy} cannot freeze its legacy bus identity: another subject already claims those bytes as its agent ID"
                ),
            ));
        }
        let generated = AgentId::generate().map_err(|error| {
            MigrateRefusal::new(
                "id-generation-failed",
                format!("generate a replacement agent ID for archived {legacy}: {error}"),
            )
        })?;
        anyhow::ensure!(
            taken.insert(generated.as_str().to_owned()),
            "generated agent ID {generated} is already claimed"
        );
        index[position].migrated_id = generated.clone();
        index[position].assigned = true;
        index[position].generated = true;
        contested.entry(legacy).or_default().push(generated);
    }

    let mut collisions = Vec::new();
    for (legacy, reassigned) in contested {
        let keeper = index
            .iter()
            .find(|entry| entry.migrated_id.as_str() == legacy)
            .map(|entry| entry.migrated_id.clone())
            .with_context(|| {
                format!("legacy identity {legacy} was contested but no subject kept it")
            })?;
        collisions.push(LegacyIdCollision {
            legacy_bus_identity: legacy,
            keeper,
            reassigned,
        });
    }
    Ok(collisions)
}

/// Every archived tombstone whose recorded ID disagrees with its own declaration.
///
/// Two states produce a disagreement, and both need the same repair: a fresh archived collision
/// whose tombstone still carries the legacy bytes, and a pass interrupted after that subject's
/// declaration was written but before its tombstone was. Deriving the work from the observed
/// disagreement makes the second state converge instead of being skipped as "already migrated".
fn tombstone_repairs(index: &[Indexed]) -> Vec<(PathBuf, crate::catalog_archive::Tombstone)> {
    let mut repairs = Vec::new();
    for entry in index {
        let Some((path, tombstone)) = &entry.tombstone else {
            continue;
        };
        if tombstone.id == entry.migrated_id.as_str() {
            continue;
        }
        let mut next = tombstone.clone();
        next.id = entry.migrated_id.as_str().to_owned();
        repairs.push((path.clone(), next));
    }
    repairs
}

/// Collision rows reconstructible from an observed declaration-vs-tombstone disagreement.
///
/// The stale tombstone still names the bytes this subject lost, and the index names the subject
/// that kept them, so an interrupted pass can rebuild its own metadata rather than depending on
/// having reached the record write.
fn observed_collisions(index: &[Indexed]) -> Vec<LegacyIdCollision> {
    let mut rows = Vec::new();
    for entry in index {
        let Some((_, tombstone)) = &entry.tombstone else {
            continue;
        };
        if tombstone.id == entry.migrated_id.as_str() {
            continue;
        }
        let Some(keeper) = index
            .iter()
            .find(|other| other.migrated_id.as_str() == tombstone.id)
            .map(|other| other.migrated_id.clone())
        else {
            continue;
        };
        rows.push(LegacyIdCollision {
            legacy_bus_identity: tombstone.id.clone(),
            keeper,
            reassigned: vec![entry.migrated_id.clone()],
        });
    }
    rows
}

/// Fold `additional` into `rows`, one row per contested legacy bus identity.
fn merge_collisions(rows: &mut Vec<LegacyIdCollision>, additional: Vec<LegacyIdCollision>) {
    for row in additional {
        match rows
            .iter_mut()
            .find(|existing| existing.legacy_bus_identity == row.legacy_bus_identity)
        {
            Some(existing) => {
                for id in row.reassigned {
                    if !existing.reassigned.contains(&id) {
                        existing.reassigned.push(id);
                    }
                }
            }
            None => rows.push(row),
        }
    }
    rows.sort_by(|left, right| left.legacy_bus_identity.cmp(&right.legacy_bus_identity));
}

/// Drop the rows the durable record already explains, so a rerun reports and rewrites nothing.
fn pending_collisions(
    rows: Vec<LegacyIdCollision>,
    record: &LegacyIdCollisions,
) -> Vec<LegacyIdCollision> {
    rows.into_iter()
        .filter(|row| {
            !record.entries.iter().any(|durable| {
                durable.legacy_bus_identity == row.legacy_bus_identity
                    && durable.keeper == row.keeper
                    && row
                        .reassigned
                        .iter()
                        .all(|id| durable.reassigned.contains(id))
            })
        })
        .collect()
}

/// Resolve every supervisor reference against the combined pre-migration index.
fn resolve_supervisors(index: &mut [Indexed]) -> Result<Vec<SupervisorRewrite>> {
    let snapshot = index.to_vec();
    let mut rewrites = Vec::new();
    for position in 0..index.len() {
        let Some(reference) = index[position].supervisor.clone() else {
            continue;
        };
        let host = index[position].host.clone();
        let target = resolve_reference(&snapshot, &reference, &host)?;
        if target.as_str() == reference {
            continue;
        }
        rewrites.push(SupervisorRewrite {
            plane: index[position].plane,
            host,
            identity: index[position].identity.clone(),
            from: reference,
            to: target,
        });
    }
    rewrites.sort_by(|left, right| {
        (&left.host, &left.identity).cmp(&(&right.host, &right.identity))
    });
    Ok(rewrites)
}

/// One pre-migration supervisor reference to exactly one parent's migrated ID.
///
/// A reference names a pre-migration subject either by its exact prior ID or by the ordinary
/// pre-migration route (a bare address on the referring host, or a host-qualified bus address).
/// Zero or several matches refuse: migration may not guess which subject an ID-keyed durable edge
/// should point at.
fn resolve_reference(index: &[Indexed], reference: &str, host: &str) -> Result<AgentId> {
    let mut by_id = index
        .iter()
        .filter(|entry| entry.prior_id == reference)
        .collect::<Vec<_>>();
    if by_id.is_empty() {
        by_id = index
            .iter()
            .filter(|entry| {
                (entry.host == host && entry.effective_address == reference)
                    || entry.bus_address() == reference
                    || (entry.host == host && entry.identity == reference)
                    || entry.legacy() == reference
            })
            .collect::<Vec<_>>();
    }
    let mut distinct = BTreeSet::new();
    by_id.retain(|entry| distinct.insert(entry.migrated_id.clone()));
    match by_id.as_slice() {
        [only] => Ok(only.migrated_id.clone()),
        [] => Err(MigrateRefusal::new(
            LEGACY_SUPERVISOR_UNRESOLVED,
            format!(
                "supervisor '{reference}' on host '{host}' names no subject in the combined pre-migration live-and-archived index; unarchive and repair that declaration through the pre-activation legacy authoring path, then retry"
            ),
        )),
        many => Err(MigrateRefusal::new(
            LEGACY_SUPERVISOR_UNRESOLVED,
            format!(
                "supervisor '{reference}' on host '{host}' names {} subjects in the combined pre-migration live-and-archived index: {}",
                many.len(),
                many.iter()
                    .map(|entry| entry.migrated_id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
    }
}

/// Prove the complete prospective catalog before the first byte is written.
fn validate_prospective(index: &[Indexed]) -> Result<()> {
    let subjects = index
        .iter()
        .map(|entry| agent_spec::Subject {
            id: entry.migrated_id.clone(),
            host: entry.host.clone(),
            effective_address: entry.effective_address.clone(),
            // An archived subject released its address; a retired live one is non-routable too.
            routable: matches!(entry.plane, Plane::Live) && !entry.retired,
        })
        .collect::<Vec<_>>();
    let conflicts = agent_spec::AddressBook::new(subjects).conflicts();
    if conflicts.is_empty() {
        return Ok(());
    }
    Err(MigrateRefusal::new(
        "identity-not-unique",
        format!(
            "the migrated catalog would not be unique:\n{}",
            conflicts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        ),
    ))
}

// ---- Source-preserving declaration edits -------------------------------------------------------

#[derive(Debug)]
struct DeclarationEdit {
    path: PathBuf,
    bytes: Vec<u8>,
}

/// Author every declaration change, verifying each candidate before anything is written.
fn declaration_edits(
    index: &[Indexed],
    rewrites: &[SupervisorRewrite],
) -> Result<Vec<DeclarationEdit>> {
    let mut supervisor_of: BTreeMap<(&str, &str), &AgentId> = BTreeMap::new();
    for rewrite in rewrites {
        supervisor_of.insert((&rewrite.host, &rewrite.identity), &rewrite.to);
    }

    let mut by_path: BTreeMap<&Path, Vec<&Indexed>> = BTreeMap::new();
    for entry in index {
        let supervisor = supervisor_of
            .get(&(entry.host.as_str(), entry.identity.as_str()))
            .copied();
        if entry.assigned || supervisor.is_some() {
            by_path.entry(entry.path.as_path()).or_default().push(entry);
        }
    }

    let mut edits = Vec::new();
    for (path, entries) in by_path {
        if path.extension().and_then(|value| value.to_str()) != Some("kdl") {
            return Err(MigrateRefusal::new(
                "unsupported-declaration-format",
                format!(
                    "{} is not a canonical KDL declaration; migrate it to KDL through the legacy authoring path first",
                    path.display()
                ),
            ));
        }
        let original = fs::read_to_string(path)
            .with_context(|| format!("read declaration {}", path.display()))?;
        let mut text = original.clone();
        for entry in entries {
            let supervisor = supervisor_of
                .get(&(entry.host.as_str(), entry.identity.as_str()))
                .copied();
            // Each step is confined against its immediate predecessor, so a file declaring
            // several agents accumulates one bounded edit per subject.
            let previous = text.clone();
            text = edit_agent_node(
                &text,
                path,
                &entry.host,
                &entry.identity,
                entry.assigned.then(|| entry.migrated_id.clone()).as_ref(),
                supervisor,
            )?;
            verify_candidate(&previous, &text, path, entry, supervisor)?;
        }
        if text != original {
            edits.push(DeclarationEdit {
                path: path.to_path_buf(),
                bytes: text.into_bytes(),
            });
        }
    }
    Ok(edits)
}

/// Insert `id` and rewrite `supervisor` inside exactly one `agent` node, preserving every other
/// byte of the file.
fn edit_agent_node(
    text: &str,
    path: &Path,
    host: &str,
    identity: &str,
    id: Option<&AgentId>,
    supervisor: Option<&AgentId>,
) -> Result<String> {
    let mut text = text.to_owned();
    if let Some(id) = id {
        let document = parse(&text, path)?;
        let target = agent_node(&document, path, host, identity)?;
        refuse_nix(target, path, host, identity)?;
        match child(target, "id") {
            Some(node) => {
                let declared = positional(node, "id", path)?;
                anyhow::ensure!(
                    declared == id.as_str(),
                    "{} declares id {declared:?} for {}, expected {}",
                    path.display(),
                    legacy_bus_identity(host, identity),
                    id
                );
            }
            None => text = insert_child(&text, target, &format!("id {}", quoted(id.as_str())?))?,
        }
    }
    if let Some(supervisor) = supervisor {
        let document = parse(&text, path)?;
        let target = agent_node(&document, path, host, identity)?;
        refuse_nix(target, path, host, identity)?;
        let node = child(target, "supervisor").with_context(|| {
            format!(
                "{} no longer declares a supervisor for {}",
                path.display(),
                legacy_bus_identity(host, identity)
            )
        })?;
        positional(node, "supervisor", path)?;
        text = replace_positional(&text, node, supervisor.as_str(), path)?;
    }
    Ok(text)
}

fn parse(text: &str, path: &Path) -> Result<KdlDocument> {
    text.parse::<KdlDocument>()
        .with_context(|| format!("parse canonical KDL declaration {}", path.display()))
}

fn agent_node<'a>(
    document: &'a KdlDocument,
    path: &Path,
    host: &str,
    identity: &str,
) -> Result<&'a KdlNode> {
    let matches = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == "agent")
        .filter(|node| {
            let declared_identity = node
                .get(0)
                .and_then(|value| value.as_string())
                .or_else(|| child(node, "identity").and_then(|node| node.get(0)?.as_string()));
            let declared_host = child(node, "host").and_then(|node| node.get(0)?.as_string());
            declared_identity.is_none_or(|value| value == identity)
                && declared_host.is_none_or(|value| value == host)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [only] => Ok(*only),
        [] => anyhow::bail!(
            "{} no longer declares {}",
            path.display(),
            legacy_bus_identity(host, identity)
        ),
        many => anyhow::bail!(
            "{} declares {} {} times; migration edits exactly one node",
            path.display(),
            legacy_bus_identity(host, identity),
            many.len()
        ),
    }
}

fn refuse_nix(node: &KdlNode, path: &Path, host: &str, identity: &str) -> Result<()> {
    let nix_owned = node.children().is_some_and(|children| {
        children
            .nodes()
            .iter()
            .filter(|child| child.name().value() == "meta")
            .filter_map(KdlNode::children)
            .flat_map(|meta| meta.nodes())
            .filter(|child| child.name().value() == "managed-by")
            .any(|child| child.get(0).and_then(|value| value.as_string()) == Some("nix"))
    });
    if nix_owned {
        return Err(MigrateRefusal::new(
            "nix-owned-declaration",
            format!(
                "{} is Nix-owned ({}); migrate it at its source instead",
                path.display(),
                legacy_bus_identity(host, identity)
            ),
        ));
    }
    Ok(())
}

fn child<'a>(node: &'a KdlNode, name: &str) -> Option<&'a KdlNode> {
    node.children()?
        .nodes()
        .iter()
        .find(|child| child.name().value() == name)
}

fn positional<'a>(node: &'a KdlNode, field: &str, path: &Path) -> Result<&'a str> {
    anyhow::ensure!(
        node.children().is_none() && node.entries().len() == 1 && node.entries()[0].name().is_none(),
        "{}: `{field}` must contain exactly one positional string",
        path.display()
    );
    node.get(0)
        .and_then(|value| value.as_string())
        .with_context(|| format!("{}: `{field}` must contain a string", path.display()))
}

fn quoted(value: &str) -> Result<String> {
    serde_json::to_string(value).context("encode an identity value as canonical KDL")
}

/// Replace exactly one node's single positional entry. The entry span carries neither leading
/// trivia nor the line terminator, so the surrounding line survives untouched.
fn replace_positional(text: &str, node: &KdlNode, value: &str, path: &Path) -> Result<String> {
    let span = node.entries()[0].span();
    let range = span.offset()..span.offset() + span.len();
    anyhow::ensure!(
        text.get(range.clone()).is_some(),
        "{}: declaration value span falls outside the source",
        path.display()
    );
    let mut replacement = text.to_owned();
    replacement.replace_range(range, &quoted(value)?);
    Ok(replacement)
}

/// Insert one child node into `target`, preserving its existing block shape and indentation.
fn insert_child(text: &str, target: &KdlNode, authored: &str) -> Result<String> {
    let span = target.span();
    let start = span.offset();
    let end = start + span.len();
    let source = text
        .get(start..end)
        .context("agent span falls outside the declaration")?;
    let mut replacement = text.to_owned();
    if target.children().is_none() {
        replacement.insert_str(end, &format!(" {{ {authored} }}"));
        return Ok(replacement);
    }
    anyhow::ensure!(
        source.ends_with('}'),
        "agent child block does not end at a source-preserving insertion point"
    );
    let close = source.len() - 1;
    if let Some(newline) = source[..close].rfind('\n') {
        let closing_indent = &source[newline + 1..close];
        anyhow::ensure!(
            closing_indent
                .chars()
                .all(|value| matches!(value, ' ' | '\t')),
            "cannot preserve a non-whitespace closing-brace prefix"
        );
        let child_indent = target
            .children()
            .and_then(|children| children.nodes().first())
            .and_then(|child| line_indent(text, child.span().offset()))
            .unwrap_or_else(|| format!("{closing_indent}  "));
        replacement.insert_str(start + newline + 1, &format!("{child_indent}{authored}\n"));
        return Ok(replacement);
    }
    let before_close = &source[..close];
    let trimmed = before_close.trim_end();
    let insertion = if trimmed.ends_with('{') {
        format!(" {authored}")
    } else if trimmed.ends_with(';') {
        format!(" {authored};")
    } else {
        format!("; {authored}")
    };
    replacement.insert_str(start + trimmed.len(), &insertion);
    Ok(replacement)
}

fn line_indent(text: &str, offset: usize) -> Option<String> {
    let head = text.get(..offset)?;
    let start = head.rfind('\n').map_or(0, |index| index + 1);
    let indent = &head[start..];
    indent
        .chars()
        .all(|value| matches!(value, ' ' | '\t'))
        .then(|| indent.to_owned())
}

/// Prove the candidate changed exactly the identity fields and nothing else.
///
/// Two independent checks, because each catches what the other cannot. The line-level containment
/// proves the *source* was preserved: the only lines that may appear or disappear are the inserted
/// `id` and the rewritten `supervisor`, so every unknown field, comment, and blank line survives.
/// Re-lowering proves the *meaning*: the new bytes parse and resolve to the intended ID, address,
/// and supervisor for exactly this subject.
fn verify_candidate(
    original: &str,
    candidate: &str,
    path: &Path,
    entry: &Indexed,
    supervisor: Option<&AgentId>,
) -> Result<()> {
    let mut permitted_additions = vec![format!("id {}", quoted(entry.migrated_id.as_str())?)];
    if let Some(supervisor) = supervisor {
        permitted_additions.push(format!("supervisor {}", quoted(supervisor.as_str())?));
    }
    let before = original.lines().collect::<Vec<_>>();
    let after = candidate.lines().collect::<Vec<_>>();
    for line in after.iter().filter(|line| !before.contains(line)) {
        anyhow::ensure!(
            permitted_additions
                .iter()
                .any(|authored| line.trim() == authored),
            "{}: the candidate added an unexpected line: {line}",
            path.display()
        );
    }
    for line in before.iter().filter(|line| !after.contains(line)) {
        anyhow::ensure!(
            supervisor.is_some() && line.trim_start().starts_with("supervisor "),
            "{}: the candidate removed a line it must preserve: {line}",
            path.display()
        );
    }

    let staging = tempfile::tempdir().context("create declaration verification staging root")?;
    let directory = staging.path().join(&entry.host).join(&entry.identity);
    fs::create_dir_all(&directory).context("create declaration verification directory")?;
    let staged = directory.join("agent.kdl");
    fs::write(&staged, candidate).context("stage the candidate declaration")?;
    let (specs, _) = agent_spec::discover_file(staging.path(), &staged)
        .with_context(|| format!("re-parse the candidate for {}", path.display()))?;
    let parsed = specs
        .into_iter()
        .find(|spec| {
            spec.resolved_host(&entry.host) == entry.host && spec.identity == entry.identity
        })
        .with_context(|| {
            format!(
                "the candidate for {} no longer declares {}",
                path.display(),
                legacy_bus_identity(&entry.host, &entry.identity)
            )
        })?;
    anyhow::ensure!(
        parsed.id.as_ref() == Some(&entry.migrated_id),
        "{}: the candidate does not lower to id {}",
        path.display(),
        entry.migrated_id
    );
    anyhow::ensure!(
        parsed.address == entry.spec.address && parsed.effective_address() == entry.effective_address,
        "{}: the candidate changed the subject's address",
        path.display()
    );
    let expected_supervisor = supervisor
        .map(|id| id.as_str().to_owned())
        .or_else(|| entry.supervisor.clone());
    anyhow::ensure!(
        parsed.supervisor == expected_supervisor,
        "{}: the candidate does not lower to supervisor {expected_supervisor:?}",
        path.display()
    );
    anyhow::ensure!(
        parsed.desired_state == entry.spec.desired_state,
        "{}: the candidate changed the subject's desired state",
        path.display()
    );
    Ok(())
}

// ---- Durable writes ----------------------------------------------------------------------------

fn write_collisions(
    lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    collisions: &[LegacyIdCollision],
) -> Result<()> {
    let mut record = load_legacy_id_collisions(catalog)?;
    for collision in collisions {
        match record
            .entries
            .iter_mut()
            .find(|entry| entry.legacy_bus_identity == collision.legacy_bus_identity)
        {
            Some(entry) => {
                for id in &collision.reassigned {
                    if !entry.reassigned.contains(id) {
                        entry.reassigned.push(id.clone());
                    }
                }
                entry.keeper = collision.keeper.clone();
            }
            None => record.entries.push(collision.clone()),
        }
    }
    record.schema = LEGACY_ID_COLLISIONS_SCHEMA.to_owned();
    record
        .entries
        .sort_by(|left, right| left.legacy_bus_identity.cmp(&right.legacy_bus_identity));
    let mut body = serde_json::to_vec_pretty(&record)?;
    body.push(b'\n');
    write_control_file(lock, catalog, control, &legacy_id_collisions_path(catalog), &body)
}

fn write_declaration(
    lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    bytes: &[u8],
) -> Result<()> {
    write_control_file(lock, catalog, control, path, bytes)
}

fn write_tombstone(
    lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    path: &Path,
    tombstone: &crate::catalog_archive::Tombstone,
) -> Result<()> {
    let mut body = serde_json::to_vec_pretty(tombstone)?;
    body.push(b'\n');
    write_control_file(lock, catalog, control, path, &body)
}

/// Stage in the control plane, fsync, then atomically rename into place and fsync the parent.
fn write_control_file(
    lock: &CatalogLock,
    catalog: &Path,
    control: &Path,
    target: &Path,
    bytes: &[u8],
) -> Result<()> {
    let parent = target
        .parent()
        .with_context(|| format!("{} has no parent directory", target.display()))?;
    let mode = fs::symlink_metadata(target)
        .ok()
        .filter(|metadata| metadata.file_type().is_file())
        .map_or(0o600, |metadata| {
            std::os::unix::fs::PermissionsExt::mode(&metadata.permissions()) & 0o777
        });
    let mut temporary = tempfile::Builder::new()
        .prefix("catalog-migrate-")
        .tempfile_in(control)
        .with_context(|| format!("stage {}", target.display()))?;
    temporary
        .as_file_mut()
        .set_permissions(std::os::unix::fs::PermissionsExt::from_mode(mode))
        .and_then(|()| temporary.write_all(bytes))
        .and_then(|()| temporary.as_file().sync_all())
        .with_context(|| format!("stage {}", target.display()))?;
    crate::catalog_transaction::persist_tempfile_from_control(
        lock.control(),
        catalog,
        temporary,
        target,
    )
    .with_context(|| format!("atomically publish {}", target.display()))?;
    sync_dir(parent)
}

/// Re-read the committed catalog and prove the transaction's whole intent landed.
fn verify(catalog: &Path, this_host: &str, index: &[Indexed]) -> Result<()> {
    let found = crate::discover_strict(catalog);
    anyhow::ensure!(
        found.errors.is_empty(),
        "the migrated catalog no longer discovers completely"
    );
    let archived = crate::catalog_archive::archived_declarations(catalog)?;
    // Keyed by plane: a re-created live seat and the archived subject it displaced legitimately
    // share one `<host>.<identity>` key and must not shadow each other here.
    let mut committed: BTreeMap<(&'static str, String, String), String> = BTreeMap::new();
    for spec in &found.specs {
        committed.insert(
            (
                Plane::Live.as_str(),
                spec.resolved_host(this_host).to_owned(),
                spec.identity.clone(),
            ),
            spec.agent_id(this_host),
        );
    }
    for declaration in &archived {
        committed.insert(
            (
                Plane::Archived.as_str(),
                declaration.host.clone(),
                declaration.identity.clone(),
            ),
            declaration.spec.agent_id(&declaration.host),
        );
        if let Some(tombstone) =
            crate::catalog_archive::read_tombstone(&declaration.tombstone_path)?
        {
            anyhow::ensure!(
                tombstone.id == declaration.spec.agent_id(&declaration.host),
                "archived tombstone {} records id {}, but its declaration carries {}",
                declaration.tombstone_path.display(),
                tombstone.id,
                declaration.spec.agent_id(&declaration.host)
            );
        }
    }
    for entry in index {
        let key = (
            entry.plane.as_str(),
            entry.host.clone(),
            entry.identity.clone(),
        );
        let observed = committed.get(&key).with_context(|| {
            format!(
                "{} disappeared from the migrated {} plane",
                legacy_bus_identity(&entry.host, &entry.identity),
                entry.plane
            )
        })?;
        anyhow::ensure!(
            observed == entry.migrated_id.as_str(),
            "{} committed id {observed}, expected {}",
            legacy_bus_identity(&entry.host, &entry.identity),
            entry.migrated_id
        );
    }
    let subjects = archived
        .iter()
        .map(|declaration| {
            let mut subject = declaration.spec.subject(&declaration.host)?;
            subject.routable = false;
            Ok(subject)
        })
        .collect::<Result<Vec<_>>>()?;
    crate::catalog_transaction::validate_identity_uniqueness(&found.specs, &subjects)
        .context("the migrated catalog is not unique")
}

fn relative(catalog: &Path, path: &Path) -> String {
    path.strip_prefix(catalog)
        .map(|value| value.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}
