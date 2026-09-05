//! Agent selection: immutable IDs, mutable addresses, and the activation gate between them.
//!
//! Decision 0015 splits one overloaded string into two typed namespaces. The immutable **agent ID**
//! is catalog-global and never routes for humans; the mutable **agent address** is unique per
//! logical host and is the only thing an ordinary human reference resolves against. Equal bytes in
//! the two namespaces do not collide, so an exact-ID selector performs only ID lookup and never
//! falls through to address lookup — that is what keeps an existing semantic ID from silently
//! staying alive as a route after a rename.
//!
//! Ordinary references are decided by a fail-closed candidate set rather than a precedence rule,
//! because a dotted semantic address and a host-qualified bus address are indistinguishable by
//! shape: `dotfiles.fractal.chat` is a legal bare address and a legal `<host>.<address>` split.
//! Collecting both readings and requiring exactly one surviving subject makes the question
//! decidable without guessing which dot is the separator.
//!
//! [`activation`] is the gate DELTA-003 step 5 turns on. Until every live and structurally archived
//! subject carries an explicit ID, target writers stay on legacy behavior: a partially migrated
//! catalog has no coherent ID namespace to key ownership, provenance, or task identity on.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result};

/// How a caller named one agent.
///
/// The two forms are mutually exclusive by construction. Every agent-selecting command exposes
/// both, and a command that defaults from `ST_AGENT` consumes it through [`Self::Id`] — an ambient
/// actor is an exact subject, never a route to re-resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentSelector {
    /// Catalog-global exact ID lookup. Never falls through to address lookup.
    Id(String),
    /// An ordinary human reference, resolved by [`resolve_address`].
    Address(String),
}

/// One routable subject in the address book.
///
/// Retired subjects are absent: retirement releases the address and makes the subject
/// non-routable, so it neither resolves nor occupies the namespace. Suspended subjects are present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressBookEntry {
    /// The immutable catalog-global agent ID.
    pub id: String,
    /// The resolved logical host.
    pub host: String,
    /// The effective address: explicit `address`, else the positional `identity` fallback.
    pub address: String,
}

impl AddressBookEntry {
    /// The human-routable bus address `<host>.<address>`.
    pub fn bus_address(&self) -> String {
        format!("{}.{}", self.host, self.address)
    }
}

/// Why an ordinary reference did not name exactly one subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No routable subject carries this address, in any admitted reading.
    Unknown { reference: String },
    /// More than one distinct subject survives, so the reference is undecidable.
    Ambiguous {
        reference: String,
        /// The surviving subjects' IDs, sorted, so a diagnostic can name them.
        ids: Vec<String>,
    },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown { reference } => write!(
                formatter,
                "no routable agent has the address '{reference}'; a retired subject releases its address and does not resolve"
            ),
            Self::Ambiguous { reference, ids } => write!(
                formatter,
                "the address '{reference}' is ambiguous: it names {} subjects ({}); qualify it with a host or select the subject by its exact id",
                ids.len(),
                ids.join(", ")
            ),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Resolve one exact agent ID. Catalog-global, and never an address lookup.
pub fn resolve_id<'a>(
    entries: &'a [AddressBookEntry],
    id: &str,
) -> std::result::Result<&'a AddressBookEntry, ResolveError> {
    entries
        .iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| ResolveError::Unknown {
            reference: id.to_owned(),
        })
}

/// Resolve one ordinary human reference through the fail-closed candidate set (R24).
///
/// 1. When the caller pins a host, treat the complete input as an address in that host, and also
///    try the qualified split whose prefix equals the pinned host.
/// 2. Otherwise treat the complete input as a bare address across the selected catalog, and also
///    try every dotted split whose prefix is an admitted logical host and whose suffix is an
///    effective address in that host.
/// 3. Deduplicate by agent ID and succeed only when exactly one subject remains.
pub fn resolve_address<'a>(
    entries: &'a [AddressBookEntry],
    reference: &str,
    pinned_host: Option<&str>,
) -> std::result::Result<&'a AddressBookEntry, ResolveError> {
    let hosts: BTreeSet<&str> = entries.iter().map(|entry| entry.host.as_str()).collect();
    let mut candidates: BTreeMap<&str, &AddressBookEntry> = BTreeMap::new();

    let mut consider = |host: &str, address: &str| {
        for entry in entries {
            if entry.host == host && entry.address == address {
                candidates.insert(entry.id.as_str(), entry);
            }
        }
    };

    match pinned_host {
        Some(pinned) => {
            consider(pinned, reference);
            // Only the pinned host's own prefix is an admitted split: a caller that pinned a host
            // cannot reach another one by spelling it into the reference.
            if let Some(suffix) = reference
                .strip_prefix(pinned)
                .and_then(|rest| rest.strip_prefix('.'))
            {
                consider(pinned, suffix);
            }
        }
        None => {
            for host in &hosts {
                consider(host, reference);
            }
            for (index, _) in reference.match_indices('.') {
                let (prefix, suffix) = (&reference[..index], &reference[index + 1..]);
                if hosts.contains(prefix) {
                    consider(prefix, suffix);
                }
            }
        }
    }

    match candidates.len() {
        1 => Ok(candidates.into_values().next().expect("one candidate")),
        0 => Err(ResolveError::Unknown {
            reference: reference.to_owned(),
        }),
        _ => Err(ResolveError::Ambiguous {
            reference: reference.to_owned(),
            ids: candidates.into_keys().map(str::to_owned).collect(),
        }),
    }
}

/// Resolve either selector form against one coherent address book.
pub fn resolve<'a>(
    entries: &'a [AddressBookEntry],
    selector: &AgentSelector,
    pinned_host: Option<&str>,
) -> std::result::Result<&'a AddressBookEntry, ResolveError> {
    match selector {
        AgentSelector::Id(id) => resolve_id(entries, id),
        AgentSelector::Address(reference) => resolve_address(entries, reference, pinned_host),
    }
}

/// The routable address book of a discovered catalog.
///
/// A subject with no explicit `id` contributes its effective ID — the legacy bus identity migration
/// freezes — so resolution works identically before and after migration.
pub fn address_book(specs: &[agent_spec::AgentSpec], this_host: &str) -> Vec<AddressBookEntry> {
    specs
        .iter()
        .filter(|spec| !spec.desired_state.is_retired())
        .map(|spec| AddressBookEntry {
            id: spec.effective_id(this_host),
            host: spec.resolved_host(this_host).to_owned(),
            address: spec.effective_address().to_owned(),
        })
        .collect()
}

/// Whether the target identity model is active for this catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityActivation {
    /// Every live and structurally archived subject carries an explicit immutable ID.
    Activated,
    /// The catalog is not fully migrated, so legacy identity behavior remains normative.
    Legacy(LegacyReason),
}

/// Why the target model is not active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyReason {
    /// Live or archived subjects still carry no explicit `id`.
    CatalogNotMigrated {
        unmigrated: usize,
        /// One example, so a diagnostic can name something actionable.
        first: String,
    },
    /// A `st2 catalog migrate-ids` transaction did not complete.
    MigrationIncomplete,
}

impl std::fmt::Display for LegacyReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CatalogNotMigrated { unmigrated, first } => write!(
                formatter,
                "{unmigrated} subject(s) carry no explicit agent id (for example {first}); run `st2 catalog migrate-ids`"
            ),
            Self::MigrationIncomplete => formatter.write_str(
                "a `st2 catalog migrate-ids` transaction did not complete; rerun it with --resume",
            ),
        }
    }
}

impl IdentityActivation {
    pub fn is_activated(&self) -> bool {
        matches!(self, Self::Activated)
    }
}

/// Decide activation from an already-discovered live catalog plus the structural archive.
///
/// Both planes must be migrated. An unmigrated archived subject cannot re-enter an ID-keyed
/// catalog, and an unmigrated live subject has no ID to key ownership, provenance, or task
/// identity on — so a partially migrated catalog keeps every current invariant normative rather
/// than mixing two identity models in one pass.
pub fn activation_from(
    specs: &[agent_spec::AgentSpec],
    archived: &[crate::catalog_archive::Tombstone],
    migration_incomplete: bool,
) -> IdentityActivation {
    if migration_incomplete {
        return IdentityActivation::Legacy(LegacyReason::MigrationIncomplete);
    }
    let mut unmigrated = Vec::new();
    for spec in specs {
        if spec.id.is_none() {
            unmigrated.push(spec.path.display().to_string());
        }
    }
    for tombstone in archived {
        if tombstone.agent_id.is_none() {
            unmigrated.push(format!(
                ".st2/archive/{}/{}",
                tombstone.host, tombstone.identity
            ));
        }
    }
    match unmigrated.first() {
        None => IdentityActivation::Activated,
        Some(first) => IdentityActivation::Legacy(LegacyReason::CatalogNotMigrated {
            unmigrated: unmigrated.len(),
            first: first.clone(),
        }),
    }
}

/// Decide activation by reading the catalog. The caller must already hold a catalog read fence.
pub fn activation(catalog: &Path) -> Result<IdentityActivation> {
    let found = crate::discover_strict(catalog);
    anyhow::ensure!(
        found.errors.is_empty(),
        "cannot decide identity activation: catalog discovery is incomplete:\n{}",
        found
            .errors
            .iter()
            .map(|error| format!("  {}: {}", error.path.display(), error.message))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let observation = crate::catalog_archive::observe(catalog)
        .context("observe the structural archive for identity activation")?;
    anyhow::ensure!(
        observation.issues.is_empty(),
        "cannot decide identity activation: the structural archive has unexplained state"
    );
    let migration_incomplete = crate::catalog_migrate_ids::marker_path(catalog).exists();
    Ok(activation_from(
        &found.specs,
        &observation.archived,
        migration_incomplete,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, host: &str, address: &str) -> AddressBookEntry {
        AddressBookEntry {
            id: id.to_owned(),
            host: host.to_owned(),
            address: address.to_owned(),
        }
    }

    #[test]
    fn a_bare_address_resolves_within_the_selected_catalog() {
        let entries = vec![entry("id-1", "dev3", "chat"), entry("id-2", "dev4", "notes")];
        assert_eq!(resolve_address(&entries, "chat", None).unwrap().id, "id-1");
        assert_eq!(resolve_address(&entries, "notes", None).unwrap().id, "id-2");
    }

    /// A host-qualified bus address and a dotted semantic address are the same shape, so both
    /// readings are collected and the answer is the surviving subject.
    #[test]
    fn a_dotted_reference_is_decided_by_the_candidate_set_not_by_precedence() {
        let entries = vec![
            entry("id-1", "dev3", "fractal.chat"),
            entry("id-2", "dev4", "notes"),
        ];
        // Only the bare reading exists.
        assert_eq!(
            resolve_address(&entries, "fractal.chat", None).unwrap().id,
            "id-1"
        );
        // Only the qualified reading exists.
        assert_eq!(
            resolve_address(&entries, "dev4.notes", None).unwrap().id,
            "id-2"
        );
        // The qualified reading of a dotted address also resolves, because `dev3` is an admitted
        // host and `fractal.chat` is an address in it.
        assert_eq!(
            resolve_address(&entries, "dev3.fractal.chat", None)
                .unwrap()
                .id,
            "id-1"
        );
    }

    /// Both readings naming different subjects is undecidable, not first-wins.
    #[test]
    fn two_readings_of_one_reference_fail_closed() {
        let entries = vec![
            // The bare reading: host `dev3` has the dotted address `dev4.notes`.
            entry("id-1", "dev3", "dev4.notes"),
            // The qualified reading: host `dev4` has the address `notes`.
            entry("id-2", "dev4", "notes"),
        ];
        let error = resolve_address(&entries, "dev4.notes", None).unwrap_err();
        match error {
            ResolveError::Ambiguous { ids, .. } => assert_eq!(ids, vec!["id-1", "id-2"]),
            other => panic!("expected ambiguity, got {other:?}"),
        }
    }

    /// The same address on two hosts is legal, so an unqualified reference to it is ambiguous and a
    /// qualified one is exact.
    #[test]
    fn one_address_on_two_hosts_needs_a_host_to_be_decidable() {
        let entries = vec![entry("id-1", "dev3", "chat"), entry("id-2", "dev4", "chat")];
        assert!(matches!(
            resolve_address(&entries, "chat", None),
            Err(ResolveError::Ambiguous { .. })
        ));
        assert_eq!(
            resolve_address(&entries, "dev3.chat", None).unwrap().id,
            "id-1"
        );
        assert_eq!(
            resolve_address(&entries, "chat", Some("dev4")).unwrap().id,
            "id-2"
        );
    }

    /// A pinned host is a boundary: a reference cannot reach another host by spelling it.
    #[test]
    fn a_pinned_host_admits_only_its_own_qualified_split() {
        let entries = vec![entry("id-1", "dev3", "chat"), entry("id-2", "dev4", "chat")];
        assert_eq!(
            resolve_address(&entries, "dev3.chat", Some("dev3"))
                .unwrap()
                .id,
            "id-1"
        );
        assert!(matches!(
            resolve_address(&entries, "dev4.chat", Some("dev3")),
            Err(ResolveError::Unknown { .. })
        ));
    }

    /// Deduplication is by agent ID, so one subject reachable through both readings is not
    /// ambiguous with itself.
    #[test]
    fn one_subject_reached_twice_is_not_ambiguous() {
        // `dev3` has an address that literally reads `dev3.chat`, so the bare reading and the
        // qualified reading are the same subject only if the host also has `chat`.
        let entries = vec![entry("id-1", "dev3", "chat")];
        assert_eq!(
            resolve_address(&entries, "dev3.chat", None).unwrap().id,
            "id-1"
        );
        assert_eq!(
            resolve_address(&entries, "dev3.chat", Some("dev3"))
                .unwrap()
                .id,
            "id-1"
        );
    }

    #[test]
    fn an_unknown_address_fails_with_an_address_specific_diagnostic() {
        let entries = vec![entry("id-1", "dev3", "chat")];
        let error = resolve_address(&entries, "ghost", None).unwrap_err();
        assert!(
            format!("{error}").contains("no routable agent has the address 'ghost'"),
            "{error}"
        );
    }

    /// Exact ID selection never falls through to address lookup, and equal bytes across the two
    /// namespaces do not collide.
    #[test]
    fn exact_id_selection_never_falls_through_to_address_lookup() {
        let entries = vec![
            entry("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1", "dev3", "chat"),
            entry("id-2", "dev4", "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1"),
        ];
        // The ID namespace answers with the subject that owns the ID.
        assert_eq!(
            resolve_id(&entries, "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1")
                .unwrap()
                .host,
            "dev3"
        );
        // The address namespace answers with the subject that owns the address.
        assert_eq!(
            resolve_address(&entries, "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1", None)
                .unwrap()
                .host,
            "dev4"
        );
        // An ID that is not an ID does not become an address.
        assert!(matches!(
            resolve_id(&entries, "chat"),
            Err(ResolveError::Unknown { .. })
        ));
    }

    #[test]
    fn the_selector_forms_route_to_their_own_namespace() {
        let entries = vec![entry("id-1", "dev3", "chat")];
        assert_eq!(
            resolve(&entries, &AgentSelector::Id("id-1".to_owned()), None)
                .unwrap()
                .address,
            "chat"
        );
        assert_eq!(
            resolve(&entries, &AgentSelector::Address("chat".to_owned()), None)
                .unwrap()
                .id,
            "id-1"
        );
        assert!(resolve(&entries, &AgentSelector::Id("chat".to_owned()), None).is_err());
    }

    #[test]
    fn activation_requires_both_planes_and_a_complete_transaction() {
        use crate::catalog_archive::{TOMBSTONE_SCHEMA, Tombstone};

        let tombstone = |agent_id: Option<&str>| Tombstone {
            schema: TOMBSTONE_SCHEMA.to_owned(),
            id: "h.gone".to_owned(),
            host: "h".to_owned(),
            identity: "gone".to_owned(),
            archived_at: 1,
            reason: None,
            archive_root: ".st2/archive/h/gone".to_owned(),
            agent_id: agent_id.map(str::to_owned),
        };

        assert_eq!(
            activation_from(&[], &[tombstone(Some("h.gone"))], false),
            IdentityActivation::Activated
        );
        // An unmigrated archived subject blocks activation: it cannot re-enter an ID-keyed catalog.
        assert!(matches!(
            activation_from(&[], &[tombstone(None)], false),
            IdentityActivation::Legacy(LegacyReason::CatalogNotMigrated { unmigrated: 1, .. })
        ));
        // An interrupted transaction blocks activation regardless of what is already migrated.
        assert_eq!(
            activation_from(&[], &[tombstone(Some("h.gone"))], true),
            IdentityActivation::Legacy(LegacyReason::MigrationIncomplete)
        );
    }
}
