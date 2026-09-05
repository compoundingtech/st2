#![cfg(unix)]
//! Additive legacy explicit-ID migration: freeze every live and archived legacy subject's existing
//! bus identity as its immutable agent ID, without moving one byte of runtime or declaration state.

use std::fs;
use std::path::{Path, PathBuf};

use st2::catalog_archive::{self, UnarchiveRequest};
use st2::catalog_migrate::{
    self, LegacyEndpoint, MigrateRefusal, MigrateRequest, MigrateStatus, Plane,
};

const HOST: &str = "h";

fn write(root: &Path, relative: &str, body: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

fn catalog(temporary: &tempfile::TempDir) -> PathBuf {
    let catalog = temporary.path().join("catalog");
    fs::create_dir_all(&catalog).unwrap();
    catalog
}

/// A legacy declaration: no `id`, so its implicit ID is exactly `<host>.<identity>`.
fn legacy_agent(root: &Path, identity: &str, extra: &str) {
    write(
        root,
        &format!("agents/{HOST}/{identity}/agent.kdl"),
        &format!(
            "agent \"{identity}\" {{\n  host \"{HOST}\"\n  name \"Worker {identity}\"\n{extra}  command \"true\"\n}}\n"
        ),
    );
}

fn declaration(root: &Path, identity: &str) -> String {
    fs::read_to_string(root.join(format!("agents/{HOST}/{identity}/agent.kdl"))).unwrap()
}

fn archived_declaration(root: &Path, identity: &str) -> String {
    fs::read_to_string(
        root.join(format!(".st2/archive/{HOST}/{identity}/agent.kdl")),
    )
    .unwrap()
}

fn tombstone(root: &Path, identity: &str) -> serde_json::Value {
    serde_json::from_slice(
        &fs::read(root.join(format!(".st2/archive/{HOST}/{identity}.tombstone.json"))).unwrap(),
    )
    .unwrap()
}

fn migrate(root: &Path) -> anyhow::Result<catalog_migrate::MigrateResult> {
    catalog_migrate::migrate(MigrateRequest {
        catalog: root.to_path_buf(),
        host: HOST.to_owned(),
        dry_run: false,
    })
}

fn refusal_code(error: &anyhow::Error) -> &'static str {
    error
        .downcast_ref::<MigrateRefusal>()
        .unwrap_or_else(|| panic!("not a classified migration refusal: {error:#}"))
        .code
}

/// Structurally archive one identity by performing exactly the two steps the archive transaction's
/// `move_out` performs: rename the identity directory under the archive root, then write its
/// tombstone beside it.
///
/// The transaction around those steps — eligibility, the exclusive lock, the generation commit —
/// is proven end to end in `tests/catalog_archive.rs`. Reproducing the on-disk result here keeps
/// the migration suite independent of the CLI binary and of a `pty list` shim, so it exercises
/// exactly the archived state migration has to read.
fn archive(root: &Path, identity: &str) {
    write(
        root,
        &format!("agents/{HOST}/{identity}/agent.kdl"),
        &format!(
            "agent \"{identity}\" {{\n  host \"{HOST}\"\n  desired-state \"retired\" reason=\"done\"\n  command \"true\"\n}}\n"
        ),
    );
    let host_root = root.join(format!(".st2/archive/{HOST}"));
    fs::create_dir_all(&host_root).unwrap();
    fs::rename(
        root.join(format!("agents/{HOST}/{identity}")),
        host_root.join(identity),
    )
    .unwrap();
    let tombstone = serde_json::json!({
        "schema": "st2.catalog-archive-tombstone.v1",
        "id": format!("{HOST}.{identity}"),
        "host": HOST,
        "identity": identity,
        "archivedAt": 1_750_000_000_000_u64,
        "reason": "done",
        "archiveRoot": format!(".st2/archive/{HOST}/{identity}"),
    });
    fs::write(
        host_root.join(format!("{identity}.tombstone.json")),
        format!("{}\n", serde_json::to_string_pretty(&tombstone).unwrap()),
    )
    .unwrap();
    assert!(host_root.join(identity).join("agent.kdl").exists());
}

#[test]
fn live_legacy_subjects_freeze_their_bus_identity_without_moving_state() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "alpha", "");
    legacy_agent(&root, "beta", "");
    write(
        &root,
        &format!("agents/{HOST}/alpha/resources/goal.md"),
        "# goal\n",
    );
    let before = declaration(&root, "alpha");
    let state = fs::read(root.join(format!("agents/{HOST}/alpha/resources/goal.md"))).unwrap();

    let result = migrate(&root).unwrap();

    assert_eq!(result.status, MigrateStatus::Migrated);
    assert_eq!(result.migrated.len(), 2, "{result:#?}");
    let alpha = result
        .migrated
        .iter()
        .find(|entry| entry.identity == "alpha")
        .unwrap();
    assert_eq!(alpha.id.as_str(), "h.alpha");
    assert_eq!(alpha.plane, Plane::Live);
    assert!(!alpha.generated);

    let after = declaration(&root, "alpha");
    assert!(after.contains("id \"h.alpha\""), "{after}");
    // Every pre-existing byte survives: the migration adds one line and touches nothing else.
    let added: Vec<&str> = after
        .lines()
        .filter(|line| !before.lines().any(|original| original == *line))
        .collect();
    assert_eq!(added, vec!["  id \"h.alpha\""], "{after}");
    // Declaration-anchored state does not move.
    assert_eq!(
        fs::read(root.join(format!("agents/{HOST}/alpha/resources/goal.md"))).unwrap(),
        state
    );

    // The ownership selector is byte-identical before and after, which is why no runtime,
    // task ID, or socket path moves.
    let found = st2::discover_strict(&root);
    let spec = found
        .specs
        .iter()
        .find(|spec| spec.identity == "alpha")
        .unwrap();
    assert_eq!(spec.agent_id(HOST), "h.alpha");
    assert_eq!(spec.id.as_ref().unwrap().as_str(), "h.alpha");
}

#[test]
fn an_archived_subject_freezes_the_same_bytes_when_they_are_unique() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "alpha", "");
    archive(&root, "gone");

    let result = migrate(&root).unwrap();

    let archived = result
        .migrated
        .iter()
        .find(|entry| entry.identity == "gone")
        .unwrap();
    assert_eq!(archived.plane, Plane::Archived);
    assert_eq!(archived.id.as_str(), "h.gone");
    assert!(!archived.generated);
    assert!(result.collisions.is_empty());
    let text = archived_declaration(&root, "gone");
    assert!(text.contains("id \"h.gone\""), "{text}");
    // A non-colliding archived subject keeps its tombstone untouched.
    assert_eq!(tombstone(&root, "gone")["id"], "h.gone");
}

#[test]
fn an_archived_collision_receives_a_generated_id_in_its_declaration_and_tombstone() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "worker");
    // The seat came back under the same name after archival: both subjects want `h.worker`.
    legacy_agent(&root, "worker", "");

    let result = migrate(&root).unwrap();

    let live = result
        .migrated
        .iter()
        .find(|entry| entry.plane == Plane::Live)
        .unwrap();
    assert_eq!(live.id.as_str(), "h.worker", "the live subject keeps its bytes");
    assert!(!live.generated);
    let archived = result
        .migrated
        .iter()
        .find(|entry| entry.plane == Plane::Archived)
        .unwrap();
    assert!(archived.generated);
    let generated = archived.id.as_str().to_owned();
    // UUIDv7: canonical hyphenated form with version nibble 7.
    assert_eq!(generated.len(), 36, "{generated}");
    assert_eq!(generated.as_bytes()[14], b'7', "{generated}");
    assert_ne!(generated, "h.worker");

    let text = archived_declaration(&root, "worker");
    assert!(text.contains(&format!("id \"{generated}\"")), "{text}");
    assert_eq!(tombstone(&root, "worker")["id"], generated);
    assert!(declaration(&root, "worker").contains("id \"h.worker\""));
}

#[test]
fn collision_metadata_is_recorded_durably_and_readable() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "worker");
    legacy_agent(&root, "worker", "");

    let result = migrate(&root).unwrap();

    assert_eq!(result.collisions.len(), 1, "{result:#?}");
    let collision = &result.collisions[0];
    assert_eq!(collision.legacy_bus_identity, "h.worker");
    assert_eq!(collision.keeper.as_str(), "h.worker");
    let generated = collision.reassigned[0].as_str().to_owned();

    let record = catalog_migrate::load_legacy_id_collisions(&root).unwrap();
    assert_eq!(record.schema, "st2.catalog-legacy-id-collisions.v1");
    match record.attribution("h.worker") {
        LegacyEndpoint::Collision { keeper, reassigned } => {
            assert_eq!(keeper.as_str(), "h.worker");
            assert_eq!(
                reassigned.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                vec![generated.as_str()]
            );
        }
        other => panic!("expected a recorded collision, got {other:?}"),
    }
    // Bytes nobody contested need no record: they are the frozen ID by construction.
    assert_eq!(record.attribution("h.other"), LegacyEndpoint::Frozen);
    // A pre-migration deployment reads every endpoint as frozen.
    let empty = catalog_migrate::load_legacy_id_collisions(temporary.path()).unwrap();
    assert_eq!(empty.attribution("h.worker"), LegacyEndpoint::Frozen);
}

/// Absent means "pre-migration", and only absent. A record that exists but cannot be read proves
/// nothing about which legacy bytes were contested, and reading it as empty would retype every
/// contested endpoint into whichever subject kept the bytes.
#[test]
fn an_unreadable_collision_record_refuses_instead_of_reading_as_empty() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    let path = catalog_migrate::legacy_id_collisions_path(&root);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    // A dangling symlink: the entry demonstrably exists, but `fs::read` reports NotFound for it,
    // so deciding absence from the read would let an alias forge "no collisions were recorded".
    std::os::unix::fs::symlink(root.join("nowhere.json"), &path).unwrap();
    let refusal = format!(
        "{:#}",
        catalog_migrate::load_legacy_id_collisions(&root)
            .expect_err("a dangling collision-record symlink must refuse")
    );
    assert!(
        refusal.contains("not a regular file"),
        "the refusal must name the aliased entry: {refusal}"
    );
    // A symlink to a valid record is refused for the same reason: the bytes are not the ones the
    // control plane wrote and can be swapped out from under a reader.
    std::fs::write(
        root.join("nowhere.json"),
        br#"{"schema":"st2.catalog-legacy-id-collisions.v1","entries":[]}"#,
    )
    .unwrap();
    assert!(
        catalog_migrate::load_legacy_id_collisions(&root).is_err(),
        "an aliased collision record must refuse even when its target parses"
    );
    std::fs::remove_file(&path).unwrap();

    // Corrupt body.
    std::fs::write(&path, b"{ this is not json").unwrap();
    let refusal = format!(
        "{:#}",
        catalog_migrate::load_legacy_id_collisions(&root)
            .expect_err("a corrupt collision record must refuse")
    );
    assert!(
        refusal.contains("legacy-id-collision record"),
        "the refusal must name the record: {refusal}"
    );

    // A schema string this binary does not understand.
    std::fs::write(
        &path,
        br#"{"schema":"st2.catalog-legacy-id-collisions.v2","entries":[]}"#,
    )
    .unwrap();
    let refusal = format!(
        "{:#}",
        catalog_migrate::load_legacy_id_collisions(&root)
            .expect_err("a foreign-schema collision record must refuse")
    );
    assert!(
        refusal.contains("unsupported legacy-id-collision schema"),
        "the refusal must name the unsupported schema: {refusal}"
    );

    // Migration itself refuses rather than rewriting the record it could not read.
    legacy_agent(&root, "worker", "");
    let migrate_refusal = format!("{:#}", migrate(&root).expect_err("migrate must refuse"));
    assert!(
        migrate_refusal.contains("legacy-id-collision"),
        "migrate must surface the unreadable record: {migrate_refusal}"
    );
}

#[test]
fn supervisor_references_are_rewritten_to_the_parents_migrated_id() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    // A parent that only exists in the structural archive: resolution uses the *combined*
    // pre-migration live-and-archived index, not just what discovery can see.
    archive(&root, "ghostboss");
    legacy_agent(&root, "boss", "");
    // A bare address, resolved on the child's own host.
    legacy_agent(&root, "child", "  supervisor \"boss\"\n");
    // Already the parent's pre-migration ID bytes: nothing to rewrite.
    legacy_agent(&root, "cousin", &format!("  supervisor \"{HOST}.boss\"\n"));
    legacy_agent(&root, "orphaned", "  supervisor \"ghostboss\"\n");

    let result = migrate(&root).unwrap();

    let mut rewritten = result
        .supervisors
        .iter()
        .map(|rewrite| (rewrite.identity.as_str(), rewrite.to.as_str()))
        .collect::<Vec<_>>();
    rewritten.sort();
    assert_eq!(
        rewritten,
        vec![("child", "h.boss"), ("orphaned", "h.ghostboss")],
        "{result:#?}"
    );
    assert!(declaration(&root, "child").contains("supervisor \"h.boss\""));
    assert!(declaration(&root, "orphaned").contains("supervisor \"h.ghostboss\""));
    // An already ID-keyed reference is left byte-identical rather than rewritten to itself.
    assert!(declaration(&root, "cousin").contains("supervisor \"h.boss\""));

    // The edge now points at the parent's immutable ID.
    let found = st2::discover_strict(&root);
    let child = found
        .specs
        .iter()
        .find(|spec| spec.identity == "child")
        .unwrap();
    assert_eq!(child.supervisor.as_deref(), Some("h.boss"));
}

#[test]
fn a_missing_supervisor_reference_refuses_before_any_write() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "orphan", "  supervisor \"ghost\"\n");
    let before = declaration(&root, "orphan");

    let error = migrate(&root).unwrap_err();

    assert_eq!(refusal_code(&error), "legacy-supervisor-unresolved");
    assert_eq!(declaration(&root, "orphan"), before, "nothing may be written");
    assert!(!catalog_migrate::legacy_id_collisions_path(&root).exists());
}

#[test]
fn an_ambiguous_supervisor_reference_refuses_before_any_write() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    // Archived `h.boss` and live `h.boss` are two distinct pre-migration subjects, so a reference
    // to `boss` cannot be attributed to one ID-keyed parent.
    archive(&root, "boss");
    legacy_agent(&root, "boss", "");
    legacy_agent(&root, "child", "  supervisor \"boss\"\n");


    let error = migrate(&root).unwrap_err();

    assert_eq!(refusal_code(&error), "legacy-supervisor-unresolved");
    assert!(declaration(&root, "child").contains("supervisor \"boss\""));
    assert!(!declaration(&root, "child").contains("id \""));
}

#[test]
fn agent_ids_are_unique_across_the_live_catalog_and_the_structural_archive() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "twin");
    migrate(&root).unwrap();
    // A new live declaration that explicitly claims the archived subject's frozen ID.
    write(
        &root,
        "agents/h/fresh/agent.kdl",
        "agent \"fresh\" {\n  host \"h\"\n  id \"h.twin\"\n  command \"true\"\n}\n",
    );

    let error = migrate(&root).unwrap_err();

    assert_eq!(refusal_code(&error), "identity-not-unique");
    assert!(
        format!("{error:#}").contains("h.twin"),
        "the conflict names the duplicated ID: {error:#}"
    );
}

#[test]
fn host_local_address_uniqueness_catches_an_identity_fallback_collision() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "worker", "");
    // An explicit address that collides with another declaration's identity fallback.
    write(
        &root,
        "agents/h/renamed/agent.kdl",
        "agent \"renamed\" {\n  host \"h\"\n  address \"worker\"\n  command \"true\"\n}\n",
    );

    let error = migrate(&root).unwrap_err();

    assert_eq!(refusal_code(&error), "identity-not-unique");
    assert!(
        format!("{error:#}").contains("worker"),
        "the conflict names the contested address: {error:#}"
    );
    assert!(!declaration(&root, "worker").contains("id \""));
}

#[test]
fn a_second_migration_pass_is_a_proven_no_op() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "worker");
    legacy_agent(&root, "worker", "");
    legacy_agent(&root, "boss", "");
    legacy_agent(&root, "child", "  supervisor \"boss\"\n");
    let first = migrate(&root).unwrap();
    assert_eq!(first.status, MigrateStatus::Migrated);
    let live = declaration(&root, "worker");
    let archived = archived_declaration(&root, "worker");
    let child = declaration(&root, "child");
    let record = fs::read(catalog_migrate::legacy_id_collisions_path(&root)).unwrap();

    let second = migrate(&root).unwrap();

    assert_eq!(second.status, MigrateStatus::Unchanged);
    assert!(second.migrated.is_empty(), "{second:#?}");
    assert!(second.supervisors.is_empty(), "{second:#?}");
    assert!(second.collisions.is_empty(), "{second:#?}");
    assert_eq!(declaration(&root, "worker"), live);
    assert_eq!(archived_declaration(&root, "worker"), archived);
    assert_eq!(declaration(&root, "child"), child);
    assert_eq!(
        fs::read(catalog_migrate::legacy_id_collisions_path(&root)).unwrap(),
        record
    );
}

#[test]
fn an_archived_subject_releases_its_address_but_keeps_its_id() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "seat");
    migrate(&root).unwrap();

    let subjects = catalog_archive::archived_subjects(&root).unwrap();
    assert_eq!(subjects.len(), 1);
    assert_eq!(subjects[0].id.as_str(), "h.seat");
    assert!(!subjects[0].routable, "a retired subject is non-routable");
    assert_eq!(subjects[0].bus_address(), None, "it released its address");

    // Because the address is released, a new live subject may take those exact route bytes;
    // the archived subject's ID stays reserved.
    write(
        &root,
        "agents/h/successor/agent.kdl",
        "agent \"successor\" {\n  host \"h\"\n  address \"seat\"\n  command \"true\"\n}\n",
    );
    let result = migrate(&root).unwrap();
    assert_eq!(result.status, MigrateStatus::Migrated);
    assert_eq!(result.migrated.len(), 1);
    assert_eq!(result.migrated[0].id.as_str(), "h.successor");
    assert_eq!(
        catalog_archive::archived_subjects(&root).unwrap()[0]
            .id
            .as_str(),
        "h.seat"
    );
}

#[test]
fn unarchive_preserves_and_validates_the_migrated_id() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "alpha", "");
    archive(&root, "back");
    migrate(&root).unwrap();

    let receipt = catalog_archive::unarchive(UnarchiveRequest {
        catalog: root.clone(),
        host: HOST.to_owned(),
        identity: "back".to_owned(),
    })
    .unwrap();

    assert_eq!(receipt.id, "h.back", "the subject keeps its frozen ID");
    assert!(declaration(&root, "back").contains("id \"h.back\""));
    assert!(!root.join(".st2/archive/h/back").exists());
}

#[test]
fn unarchive_refuses_an_id_the_live_catalog_already_claims() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "twin");
    migrate(&root).unwrap();
    // The archived declaration's frozen ID, claimed by a live subject under another name.
    write(
        &root,
        "agents/h/other/agent.kdl",
        "agent \"other\" {\n  host \"h\"\n  id \"h.twin\"\n  command \"true\"\n}\n",
    );

    let error = catalog_archive::unarchive(UnarchiveRequest {
        catalog: root.clone(),
        host: HOST.to_owned(),
        identity: "twin".to_owned(),
    })
    .unwrap_err();

    let rendered = format!("{error:#}");
    assert!(rendered.contains("h.twin"), "{rendered}");
    assert!(
        root.join(".st2/archive/h/twin/agent.kdl").exists(),
        "the refusal happens before the move"
    );
}

#[test]
fn unarchive_refuses_an_unmigrated_archived_declaration_in_a_migrated_catalog() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "stale");
    legacy_agent(&root, "alpha", "");
    // Migrate only the live plane, leaving the archived declaration unmigrated, exactly the state
    // an operator reaches by archiving after activation without repairing the tombstoned spec.
    migrate(&root).unwrap();
    let archived_path = root.join(".st2/archive/h/stale/agent.kdl");
    let text = fs::read_to_string(&archived_path).unwrap();
    fs::write(&archived_path, text.replace("  id \"h.stale\"\n", "")).unwrap();

    let error = catalog_archive::unarchive(UnarchiveRequest {
        catalog: root.clone(),
        host: HOST.to_owned(),
        identity: "stale".to_owned(),
    })
    .unwrap_err();

    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("no explicit `id`"),
        "the refusal explains the missing migration: {rendered}"
    );
    assert!(archived_path.exists(), "the refusal happens before the move");
}

#[test]
fn a_nix_owned_declaration_refuses_instead_of_being_rewritten() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    write(
        &root,
        "agents/h/managed/agent.kdl",
        "agent \"managed\" {\n  host \"h\"\n  meta {\n    managed-by \"nix\"\n  }\n  command \"true\"\n}\n",
    );
    let before = declaration(&root, "managed");

    let error = migrate(&root).unwrap_err();

    assert_eq!(refusal_code(&error), "nix-owned-declaration");
    assert_eq!(declaration(&root, "managed"), before);
}

#[test]
fn a_dry_run_proves_the_plan_without_writing() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "alpha", "");
    let before = declaration(&root, "alpha");

    let result = catalog_migrate::migrate(MigrateRequest {
        catalog: root.clone(),
        host: HOST.to_owned(),
        dry_run: true,
    })
    .unwrap();

    assert_eq!(result.status, MigrateStatus::Migrated);
    assert!(result.dry_run);
    assert_eq!(result.migrated[0].id.as_str(), "h.alpha");
    assert_eq!(declaration(&root, "alpha"), before);
}

/// The declaration projection models `id` and `address`, so a whole-catalog comparison sees the
/// migration as a semantic field change rather than as opaque byte drift.
#[test]
fn the_declaration_projection_models_id_and_address_as_comparable_fields() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    legacy_agent(&root, "alpha", "");
    let prepared = temporary.path().join("prepared");
    fs::create_dir_all(prepared.join(format!("agents/{HOST}/alpha"))).unwrap();
    fs::write(
        prepared.join(format!("agents/{HOST}/alpha/agent.kdl")),
        declaration(&root, "alpha")
            .replace("  host \"h\"\n", "  host \"h\"\n  id \"h.alpha\"\n  address \"build.owner\"\n"),
    )
    .unwrap();

    let snapshot = st2::catalog_transaction::snapshot(st2::catalog_transaction::SnapshotRequest {
        catalog: root.clone(),
        output: temporary.path().join("snapshot.json"),
        raw_preimage: false,
    })
    .unwrap();
    let diff = st2::catalog_transaction::diff(st2::catalog_transaction::DiffRequest {
        catalog: root.clone(),
        prepared,
        expect_sha256: snapshot.root_sha256,
    })
    .unwrap();

    let agent = diff
        .agents
        .iter()
        .find(|delta| delta.identity == "alpha")
        .unwrap_or_else(|| panic!("no semantic delta for alpha: {diff:#?}"));
    let changed = agent
        .fields
        .iter()
        .map(|field| field.address.as_str())
        .collect::<Vec<_>>();
    assert!(
        changed.contains(&"/agents/h/alpha/id"),
        "the added explicit ID is a modelled field: {changed:?}"
    );
    assert!(
        changed.contains(&"/agents/h/alpha/address"),
        "the address cutover is a modelled field: {changed:?}"
    );
}

/// A pass interrupted between an archived declaration write and its tombstone write converges.
///
/// The declaration already carries its generated ID, so "does this declaration have an id" reports
/// nothing to do while the tombstone still names the bytes the subject lost. Repairs are scheduled
/// from the observed disagreement, so the rerun closes it and rebuilds the collision metadata.
#[test]
fn an_interrupted_pass_repairs_a_tombstone_that_disagrees_with_its_declaration() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "worker");
    legacy_agent(&root, "worker", "");
    let first = migrate(&root).unwrap();
    let generated = first.collisions[0].reassigned[0].as_str().to_owned();
    let archived_bytes = archived_declaration(&root, "worker");

    // Rewind to the exact intermediate state: declaration written, tombstone and record not.
    let tombstone_path = root.join(format!(".st2/archive/{HOST}/worker.tombstone.json"));
    let mut stale: serde_json::Value =
        serde_json::from_slice(&fs::read(&tombstone_path).unwrap()).unwrap();
    stale["id"] = serde_json::Value::String("h.worker".to_owned());
    fs::write(
        &tombstone_path,
        format!("{}\n", serde_json::to_string_pretty(&stale).unwrap()),
    )
    .unwrap();
    fs::remove_file(catalog_migrate::legacy_id_collisions_path(&root)).unwrap();

    let repair = migrate(&root).unwrap();

    assert_eq!(repair.status, MigrateStatus::Migrated, "{repair:#?}");
    assert_eq!(repair.tombstones_repaired, 1, "{repair:#?}");
    assert!(
        repair.migrated.is_empty(),
        "no declaration needed a new ID: {repair:#?}"
    );
    assert_eq!(tombstone(&root, "worker")["id"], generated);
    assert_eq!(
        archived_declaration(&root, "worker"),
        archived_bytes,
        "the declaration was already correct and must not be rewritten"
    );
    // The metadata that keeps a tolerant reader from retyping `h.worker` is durable again.
    let record = catalog_migrate::load_legacy_id_collisions(&root).unwrap();
    match record.attribution("h.worker") {
        LegacyEndpoint::Collision { keeper, reassigned } => {
            assert_eq!(keeper.as_str(), "h.worker");
            assert_eq!(reassigned[0].as_str(), generated);
        }
        other => panic!("the rebuilt record must explain the collision, got {other:?}"),
    }

    // And it stays a proven no-op from there.
    let settled = migrate(&root).unwrap();
    assert_eq!(settled.status, MigrateStatus::Unchanged, "{settled:#?}");
    assert_eq!(settled.tombstones_repaired, 0);
}

/// An aliased archive entry is uncertainty, not absence.
///
/// Every consumer of the archived reader feeds a catalog-global uniqueness proof, so an entry that
/// could be hiding an occupied agent ID must refuse rather than be skipped.
#[test]
fn the_archived_reader_refuses_an_aliased_entry() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "seat");
    legacy_agent(&root, "alpha", "");
    std::os::unix::fs::symlink(
        root.join(format!(".st2/archive/{HOST}/seat")),
        root.join(format!(".st2/archive/{HOST}/alias")),
    )
    .unwrap();

    let error = catalog_archive::archived_subjects(&root).unwrap_err();
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("alias") && rendered.contains("refusing to prove archived identity"),
        "{rendered}"
    );
    // The transaction that would claim an ID inherits the refusal instead of a short archive.
    let error = migrate(&root).unwrap_err();
    assert!(
        format!("{error:#}").contains("refusing to prove archived identity"),
        "{error:#}"
    );
    assert!(!declaration(&root, "alpha").contains("id \""));
}

/// Two archived subjects claiming one agent ID are both reported, never silently collapsed.
#[test]
fn the_archive_observation_reports_duplicate_archived_ids() {
    let temporary = tempfile::tempdir().unwrap();
    let root = catalog(&temporary);
    archive(&root, "one");
    archive(&root, "two");
    // Force the pathological state the ID-keyed map used to hide: one ID, two placements.
    for identity in ["one", "two"] {
        let path = root.join(format!(".st2/archive/{HOST}/{identity}.tombstone.json"));
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        value["id"] = serde_json::Value::String("h.shared".to_owned());
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&value).unwrap()),
        )
        .unwrap();
    }

    let observation = catalog_archive::observe(&root).unwrap();

    assert_eq!(
        observation.archived.len(),
        2,
        "both archived identities stay visible: {observation:#?}"
    );
    assert!(
        observation.issues.iter().any(|issue| {
            issue.message.contains("h.shared") && issue.message.contains("2 archived identities")
        }),
        "the duplicate is its own diagnostic: {observation:#?}"
    );
}
