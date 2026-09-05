#![cfg(unix)]
//! `st2 catalog migrate-ids` freezes every legacy subject's immutable agent ID in one transaction.
//!
//! The headline fixture mirrors the shape a real catalog presents at migration time: one counted
//! root per host, a supervisor chain, hundreds of structurally archived subjects, and one archived
//! subject whose `<host>.<identity>` bytes a live re-projection has already reclaimed — the
//! oscillation an archiving supervisor and a re-projecting generator produce together.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn write(root: &Path, relative: &str, body: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

fn fixture(temporary: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    let catalog = temporary.path().join("catalog");
    fs::create_dir_all(&catalog).unwrap();
    (catalog, temporary.path().join("bin"))
}

fn st2(root: &Path, bin: &Path, args: &[&str]) -> Output {
    st2_with(root, bin, args, &[])
}

fn st2_with(root: &Path, bin: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    let home = root.parent().unwrap().join("home");
    fs::create_dir_all(&home).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_st2"));
    command
        .args(["--catalog", root.to_str().unwrap()])
        .args(args)
        .env("PATH", bin)
        .env("HOME", &home)
        .env("XDG_STATE_HOME", home.join("state"))
        .env("PTY_ROOT", home.join("pty"))
        .env_remove("CATALOG")
        .env_remove("ST_ROOT");
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().unwrap()
}

fn json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON ({error}):\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn ok(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn generation(root: &Path) -> String {
    fs::read_to_string(root.join(".st2/catalog-generation")).unwrap_or_default()
}

/// The explicit `id` a declaration file carries, if any.
fn declared_id(path: &Path) -> Option<String> {
    let text = fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    text.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("id \""))
        .and_then(|rest| rest.strip_suffix('"'))
        .map(str::to_owned)
}

/// The `supervisor` value a declaration file carries, if any.
fn declared_supervisor(path: &Path) -> Option<String> {
    let text = fs::read_to_string(path).unwrap();
    text.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("supervisor \""))
        .and_then(|rest| rest.strip_suffix('"'))
        .map(str::to_owned)
}

fn live(root: &Path, host: &str, identity: &str, body: &str) {
    write(
        root,
        &format!("agents/{host}/{identity}/agent.kdl"),
        &format!("agent \"{identity}\" {{\n  host \"{host}\"\n{body}  argv \"true\"\n}}\n"),
    );
}

/// One structurally archived subject: its moved directory plus the tombstone that explains it.
fn archived(root: &Path, host: &str, identity: &str, supervisor: Option<&str>) {
    let supervisor_line = supervisor
        .map(|value| format!("  supervisor \"{value}\"\n"))
        .unwrap_or_default();
    write(
        root,
        &format!(".st2/archive/{host}/{identity}/agent.kdl"),
        &format!(
            "agent \"{identity}\" {{\n  host \"{host}\"\n{supervisor_line}  desired-state \"retired\" reason=\"Work finished\"\n  argv \"true\"\n}}\n"
        ),
    );
    write(
        root,
        &format!(".st2/archive/{host}/{identity}.tombstone.json"),
        &format!(
            "{{\n  \"schema\": \"st2.catalog-archive-tombstone.v1\",\n  \"id\": \"{host}.{identity}\",\n  \"host\": \"{host}\",\n  \"identity\": \"{identity}\",\n  \"archivedAt\": 1750000000000,\n  \"reason\": \"Work finished\",\n  \"archiveRoot\": \".st2/archive/{host}/{identity}\"\n}}\n"
        ),
    );
}

const ARCHIVED_COUNT: usize = 600;
const LIVE_COUNT: usize = 40;
/// The identity a re-projecting generator recreated live after the supervisor archived it, so both
/// planes claim one `<host>.<identity>`.
const COLLIDING: &str = "reprojected";

/// A catalog shaped like a real one at migration time.
fn dev3_shaped(root: &Path) {
    live(root, "h", "root", "");
    // A five-deep supervisor chain, so the rewrite is proved on more than a flat fan-out.
    live(root, "h", "chain-1", "  supervisor \"root\"\n");
    for depth in 2..=5 {
        live(
            root,
            "h",
            &format!("chain-{depth}"),
            &format!("  supervisor \"chain-{}\"\n", depth - 1),
        );
    }
    // Identities that themselves contain dots, which is what a real catalog's semantic routes look
    // like — the reference `dotfiles.worker` is still a BARE identity, not a host-qualified one.
    live(root, "h", "dotfiles.worker", "  supervisor \"root\"\n");
    live(
        root,
        "h",
        "dotfiles.worker.child",
        "  supervisor \"dotfiles.worker\"\n",
    );
    for index in 0..LIVE_COUNT {
        live(
            root,
            "h",
            &format!("live-{index:03}"),
            "  supervisor \"root\"\n",
        );
    }
    // The live half of the collision.
    live(root, "h", COLLIDING, "  supervisor \"root\"\n");

    for index in 0..ARCHIVED_COUNT {
        archived(root, "h", &format!("arch-{index:03}"), Some("root"));
    }
    // One archived subject supervised by another archived subject: supervisor resolution must see
    // the archived half of the combined index, not only the live catalog.
    archived(root, "h", "arch-child", Some("arch-000"));
    // The archived half of the collision.
    archived(root, "h", COLLIDING, Some("root"));
}

#[test]
fn a_dev3_shaped_catalog_freezes_live_bus_identities_and_reassigns_only_an_archived_collision() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    dev3_shaped(root);

    let before = generation(root);
    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    ok(&output);
    let receipt = json(&output);
    assert_eq!(receipt["schema"], "st2.catalog-migrate-ids.v1");
    assert_eq!(receipt["status"], "migrated");
    assert_eq!(receipt["dryRun"], false);

    let assigned = receipt["assigned"].as_array().unwrap();
    // every live subject + every archived subject
    let live_total = LIVE_COUNT + 1 /* root */ + 5 /* chain */ + 2 /* dotted */ + 1 /* colliding */;
    let archived_total = ARCHIVED_COUNT + 1 /* arch-child */ + 1 /* colliding */;
    assert_eq!(assigned.len(), live_total + archived_total, "{receipt:#}");

    // A live subject freezes its own bus identity, so nothing runtime-visible moves.
    for index in 0..LIVE_COUNT {
        let identity = format!("live-{index:03}");
        let path = root.join(format!("agents/h/{identity}/agent.kdl"));
        assert_eq!(
            declared_id(&path).as_deref(),
            Some(format!("h.{identity}").as_str()),
            "{identity} must freeze its bus identity"
        );
    }
    assert_eq!(
        declared_id(&root.join("agents/h/root/agent.kdl")).as_deref(),
        Some("h.root")
    );
    assert_eq!(
        declared_id(&root.join("agents/h/dotfiles.worker/agent.kdl")).as_deref(),
        Some("h.dotfiles.worker")
    );

    // A non-colliding archived subject freezes the same bytes.
    assert_eq!(
        declared_id(&root.join(".st2/archive/h/arch-000/agent.kdl")).as_deref(),
        Some("h.arch-000")
    );

    // The live claimant of the colliding bytes keeps them; the archived one receives UUIDv7.
    assert_eq!(
        declared_id(&root.join(format!("agents/h/{COLLIDING}/agent.kdl"))).as_deref(),
        Some(format!("h.{COLLIDING}").as_str())
    );
    let reassigned_id =
        declared_id(&root.join(format!(".st2/archive/h/{COLLIDING}/agent.kdl"))).unwrap();
    assert_eq!(reassigned_id.len(), 36, "{reassigned_id} must be a UUID");
    assert_eq!(
        reassigned_id.as_bytes()[14], b'7',
        "{reassigned_id} must be version 7"
    );
    assert!(
        matches!(reassigned_id.as_bytes()[19], b'8' | b'9' | b'a' | b'b'),
        "{reassigned_id} must carry the RFC 9562 variant"
    );

    let reassigned = receipt["reassigned"].as_array().unwrap();
    assert_eq!(reassigned.len(), 1, "only the collision is reassigned");
    assert_eq!(reassigned[0]["legacyBusIdentity"], format!("h.{COLLIDING}"));
    assert_eq!(reassigned[0]["keptByAgentId"], format!("h.{COLLIDING}"));
    assert_eq!(reassigned[0]["keptByPlane"], "live");
    assert_eq!(reassigned[0]["reassignedAgentId"], reassigned_id);
    assert_eq!(reassigned[0]["reassignedPlane"], "archived");

    // The tombstone records the same immutable ID, so the archived subject stays identifiable.
    let tombstone: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join(format!(".st2/archive/h/{COLLIDING}.tombstone.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(tombstone["agentId"], reassigned_id);
    assert_eq!(tombstone["id"], format!("h.{COLLIDING}"));

    // The durable reassignment record is what keeps a version-1 durable record readable.
    let record: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join(".st2/agent-id-migration.json")).unwrap())
            .unwrap();
    assert_eq!(record["schema"], "st2.agent-id-migration.v1");
    assert_eq!(record["reassigned"].as_array().unwrap().len(), 1);
    assert_eq!(record["reassigned"][0]["keptByAgentId"], format!("h.{COLLIDING}"));
    assert_eq!(record["reassigned"][0]["reassignedAgentId"], reassigned_id);

    // Every supervisor reference now names the parent's migrated ID.
    assert_eq!(
        declared_supervisor(&root.join("agents/h/chain-5/agent.kdl")).as_deref(),
        Some("h.chain-4")
    );
    assert_eq!(
        declared_supervisor(&root.join("agents/h/dotfiles.worker.child/agent.kdl")).as_deref(),
        Some("h.dotfiles.worker")
    );
    assert_eq!(
        declared_supervisor(&root.join(".st2/archive/h/arch-child/agent.kdl")).as_deref(),
        Some("h.arch-000"),
        "an archived parent resolves through the combined index"
    );
    let rewrites = receipt["supervisorRewrites"].as_array().unwrap();
    assert_eq!(
        rewrites.len(),
        live_total - 1 + archived_total,
        "every subject except the root declares a supervisor"
    );

    assert_ne!(generation(root), before, "the transaction commits one generation");
    assert!(
        !root.join(".st2/migrate-ids-incomplete").exists(),
        "a completed transaction leaves no marker"
    );

    // Re-running is a no-op: nothing is written and the generation does not move.
    let after_first = generation(root);
    let again = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    ok(&again);
    let second = json(&again);
    assert_eq!(second["status"], "unchanged");
    assert_eq!(second["assigned"].as_array().unwrap().len(), 0);
    assert_eq!(
        second["alreadyMigrated"].as_array().unwrap().len(),
        live_total + archived_total
    );
    assert_eq!(generation(root), after_first, "a no-op advances no generation");
}

#[test]
fn a_dry_run_reports_the_whole_plan_and_writes_nothing() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    live(root, "h", "worker", "  supervisor \"root\"\n");
    archived(root, "h", "gone", Some("root"));

    let before = generation(root);
    let output = st2(
        root,
        &bin,
        &[
            "catalog",
            "migrate-ids",
            "--host",
            "h",
            "--dry-run",
            "--json",
        ],
    );
    ok(&output);
    let receipt = json(&output);
    assert_eq!(receipt["dryRun"], true);
    assert_eq!(receipt["status"], "migrated");
    assert_eq!(receipt["assigned"].as_array().unwrap().len(), 3);
    assert_eq!(receipt["supervisorRewrites"].as_array().unwrap().len(), 2);

    assert_eq!(declared_id(&root.join("agents/h/worker/agent.kdl")), None);
    assert_eq!(
        declared_id(&root.join(".st2/archive/h/gone/agent.kdl")),
        None
    );
    assert_eq!(
        declared_supervisor(&root.join("agents/h/worker/agent.kdl")).as_deref(),
        Some("root")
    );
    assert_eq!(generation(root), before);
    assert!(!root.join(".st2/migrate-ids-incomplete").exists());
    assert!(!root.join(".st2/agent-id-migration.json").exists());
}

#[test]
fn an_unresolved_supervisor_reference_refuses_before_any_write() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    live(root, "h", "orphan", "  supervisor \"ghost\"\n");

    let before = generation(root);
    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("legacy-supervisor-unresolved"),
        "expected the named refusal:\n{message}"
    );
    assert!(
        message.contains("supervisor 'ghost'"),
        "the refusal must name the reference:\n{message}"
    );
    assert!(
        message.contains("agents/h/orphan/agent.kdl"),
        "the refusal must name the declaration:\n{message}"
    );

    assert_eq!(declared_id(&root.join("agents/h/root/agent.kdl")), None);
    assert_eq!(declared_id(&root.join("agents/h/orphan/agent.kdl")), None);
    assert_eq!(generation(root), before);
    assert!(!root.join(".st2/migrate-ids-incomplete").exists());
}

#[test]
fn an_ambiguous_supervisor_reference_refuses_before_any_write() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    // `a.b` is readable two ways against the combined index: host `a`'s subject `b`, and host
    // `h`'s dotted identity `a.b`. Both exist, so the reference is undecidable.
    live(root, "h", "root", "");
    live(root, "a", "b", "");
    live(root, "h", "a.b", "  supervisor \"root\"\n");
    live(root, "h", "child", "  supervisor \"a.b\"\n");

    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("legacy-supervisor-unresolved") && message.contains("matches 2 subjects"),
        "expected an ambiguity refusal:\n{message}"
    );
    assert_eq!(declared_id(&root.join("agents/h/child/agent.kdl")), None);
}

#[test]
fn a_non_kdl_declaration_refuses_rather_than_being_rewritten() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    write(
        root,
        "agents/h/legacy/agent.toml",
        "identity = \"legacy\"\nhost = \"h\"\nsupervisor = \"root\"\nargv = [\"true\"]\n",
    );

    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("unsupported-declaration-format")
            && message.contains("agents/h/legacy/agent.toml"),
        "expected a format refusal naming the declaration:\n{message}"
    );
    assert_eq!(declared_id(&root.join("agents/h/root/agent.kdl")), None);
}

#[test]
fn a_nix_owned_declaration_is_migrated_and_reported_for_its_generator() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    write(
        root,
        "agents/h/projected/agent.kdl",
        "agent \"projected\" {\n  host \"h\"\n  supervisor \"root\"\n  argv \"true\"\n  meta { managed-by \"nix\" }\n}\n",
    );

    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    ok(&output);
    let receipt = json(&output);
    // Migration is not interactive authoring: the Nix marker guards `st2 rename`, not the one
    // transaction that has to reach the whole plane. The receipt names it so the generator can be
    // taught to emit `id` before the next activation re-projects the file without one.
    assert_eq!(
        receipt["nixOwned"].as_array().unwrap(),
        &vec![serde_json::json!("agents/h/projected/agent.kdl")]
    );
    assert_eq!(
        declared_id(&root.join("agents/h/projected/agent.kdl")).as_deref(),
        Some("h.projected")
    );
    let text = fs::read_to_string(root.join("agents/h/projected/agent.kdl")).unwrap();
    assert!(
        text.contains("meta { managed-by \"nix\" }"),
        "unrelated source bytes are preserved:\n{text}"
    );
}

#[test]
fn a_catalog_that_does_not_currently_admit_refuses_before_any_write() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    // Two counted roots on one host: the catalog already fails admission, so a rewritten plane
    // could not be re-admitted either.
    live(root, "h", "root", "");
    live(root, "h", "second-root", "");

    let before = generation(root);
    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("does not currently admit") && message.contains("root-count"),
        "expected the pre-migration admission refusal:\n{message}"
    );
    assert_eq!(declared_id(&root.join("agents/h/root/agent.kdl")), None);
    assert_eq!(generation(root), before);
    assert!(!root.join(".st2/migrate-ids-incomplete").exists());
}

#[test]
fn an_explicit_id_is_left_alone_and_blocks_a_freeze_that_would_claim_it() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    // A migrated subject already owns the bytes `h.worker`, which is exactly what the legacy
    // subject `worker` would freeze. The transaction may not hand one ID to two subjects.
    live(
        root,
        "h",
        "impostor",
        "  id \"h.worker\"\n  supervisor \"root\"\n",
    );
    live(root, "h", "worker", "  supervisor \"root\"\n");

    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("cannot freeze its bus identity"),
        "expected a freeze refusal:\n{message}"
    );
    assert_eq!(
        declared_id(&root.join("agents/h/impostor/agent.kdl")).as_deref(),
        Some("h.worker"),
        "the already-migrated declaration is untouched"
    );
    assert_eq!(declared_id(&root.join("agents/h/worker/agent.kdl")), None);
}

#[test]
fn an_interrupted_transaction_refuses_a_plain_rerun_and_resumes_exactly_the_remaining_work() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    for index in 0..4 {
        live(
            root,
            "h",
            &format!("worker-{index}"),
            "  supervisor \"root\"\n",
        );
    }

    let before = generation(root);
    // Abort after the first declaration is published: the marker exists, one file is migrated, and
    // the generation has not moved.
    let aborted = st2_with(
        root,
        &bin,
        &["catalog", "migrate-ids", "--host", "h", "--json"],
        &[(
            "ST2_TEST_MIGRATE_IDS_ABORT_AT",
            "migrate-ids-declaration-written",
        )],
    );
    assert!(!aborted.status.success(), "the run must not complete");
    assert!(
        root.join(".st2/migrate-ids-incomplete").is_file(),
        "an interrupted transaction leaves its marker"
    );
    let migrated_before_resume = ["root", "worker-0", "worker-1", "worker-2", "worker-3"]
        .into_iter()
        .filter(|identity| {
            declared_id(&root.join(format!("agents/h/{identity}/agent.kdl"))).is_some()
        })
        .count();
    assert_eq!(
        migrated_before_resume, 1,
        "exactly one declaration was published before the abort"
    );
    assert_eq!(generation(root), before, "an aborted run commits no generation");

    // A plain rerun refuses rather than planning a second transaction over a half-migrated plane.
    let plain = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!plain.status.success());
    assert!(
        stderr(&plain).contains("--resume"),
        "the refusal must name the recovery path:\n{}",
        stderr(&plain)
    );

    let resumed = st2(
        root,
        &bin,
        &[
            "catalog",
            "migrate-ids",
            "--host",
            "h",
            "--resume",
            "--json",
        ],
    );
    ok(&resumed);
    let receipt = json(&resumed);
    assert_eq!(receipt["resumed"], true);
    assert_eq!(receipt["status"], "migrated");
    assert_eq!(
        receipt["assigned"].as_array().unwrap().len(),
        4,
        "only the remaining declarations are assigned"
    );
    assert_eq!(
        receipt["alreadyMigrated"].as_array().unwrap().len(),
        1,
        "the declaration published before the abort is already migrated"
    );
    for identity in ["root", "worker-0", "worker-1", "worker-2", "worker-3"] {
        assert_eq!(
            declared_id(&root.join(format!("agents/h/{identity}/agent.kdl"))).as_deref(),
            Some(format!("h.{identity}").as_str()),
            "{identity} must be migrated after the resume"
        );
    }
    assert!(
        !root.join(".st2/migrate-ids-incomplete").exists(),
        "a completed resume clears the marker"
    );
    assert_ne!(generation(root), before);
}

#[test]
fn resume_without_a_marker_refuses() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");

    let output = st2(
        root,
        &bin,
        &[
            "catalog",
            "migrate-ids",
            "--host",
            "h",
            "--resume",
            "--json",
        ],
    );
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("nothing to resume"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn an_unexplained_archive_entry_refuses_because_a_hidden_subject_could_lose_its_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let (catalog, bin) = fixture(&temporary);
    let root = catalog.as_path();
    fs::create_dir_all(&bin).unwrap();
    live(root, "h", "root", "");
    // An archived directory with no tombstone: `observe` reports it, and migration must not freeze
    // bytes that this unreadable subject may already own.
    write(
        root,
        ".st2/archive/h/mystery/agent.kdl",
        "agent \"mystery\" {\n  host \"h\"\n  argv \"true\"\n}\n",
    );

    let output = st2(root, &bin, &["catalog", "migrate-ids", "--host", "h", "--json"]);
    assert!(!output.status.success());
    let message = stderr(&output);
    assert!(
        message.contains("unexplained state") && message.contains("no readable tombstone"),
        "expected the archive refusal:\n{message}"
    );
    assert_eq!(declared_id(&root.join("agents/h/root/agent.kdl")), None);
}
