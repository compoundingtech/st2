//! One walk of the declared `supervisor` edge, shared by every consumer that needs it.
//!
//! DING walks it to render a relationship marker; resync walks it to reach the carriers an agent's
//! ancestors declare. Both need the same cycle guard and the same depth bound, so the traversal
//! lives here once rather than being reimplemented per consumer.
//!
//! The walk is keyed by immutable agent ID throughout. A declared `supervisor` value is resolved
//! once, at the edge, and only its resolved ID travels: nothing downstream ever re-parses the
//! free-form reference, so an address cutover cannot re-key a chain or a cycle guard.

use std::collections::HashSet;

use crate::AddressBook;
use crate::AgentSelector;
use crate::AgentSpec;
use crate::spec::address_book;

/// A declaration graph deeper than this is a declaration fault, not a chain worth walking. The
/// bound is what keeps a malformed catalog from turning a walk into a hang; validation rejecting
/// cycles is not relied on here.
pub const SUPERVISOR_CHAIN_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorChainError {
    Cycle,
    MissingSupervisor,
    DepthLimit,
}

/// One declared `supervisor` value, in the single namespace it is written in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEdge {
    /// A migrated child names its parent by immutable agent ID, exactly as authored.
    Id(String),
    /// An unmigrated child names its parent by legacy POSITIONAL key `<host>.<identity>` — the
    /// declaration slot, which is neither a mutable address nor an explicit ID.
    Positional(String),
}

/// The namespace a declaration's `supervisor` value is written in, decided by the CHILD's
/// migration state.
///
/// Catalog migration rewrites every `supervisor` reference to its parent's migrated ID in the same
/// atomic transition that adds the child's own `id`, so the child's declaration decides the
/// namespace without guessing:
///
/// - the child carries an explicit `id` → the value is the parent's immutable agent ID;
/// - the child is unmigrated → the value is a legacy positional reference: a bare `<identity>` on
///   the child's own host, or a qualified `<host>.<identity>` on any admitted host.
///
/// This is the one place that decision is made, and the three namespaces stay disjoint. A legacy
/// edge resolves against declaration slots only: matching it against explicit IDs would let a
/// subject that merely claims those bytes as its ID capture the edge, and matching it against
/// mutable addresses would lose a retired (non-routable) or renamed parent.
pub fn supervisor_edge(
    specs: &[AgentSpec],
    spec: &AgentSpec,
    local_host: &str,
) -> Option<SupervisorEdge> {
    let reference = spec.supervisor.as_deref()?;
    if spec.id.is_some() {
        return Some(SupervisorEdge::Id(reference.to_owned()));
    }
    let child_host = spec.resolved_host(local_host);
    // A dotted prefix is a host only when this snapshot actually admits that logical host;
    // anything else is a bare identity on the child's own host, dots included.
    let qualified = match reference.split_once('.') {
        Some((prefix, _))
            if specs
                .iter()
                .any(|candidate| candidate.resolved_host(local_host) == prefix) =>
        {
            reference.to_owned()
        }
        _ => format!("{child_host}.{reference}"),
    };
    Some(SupervisorEdge::Positional(qualified))
}

/// Resolve one typed edge to exactly one declaration in this snapshot.
///
/// More than one match is a broken catalog rather than a choice to make, so it refuses.
pub fn resolve_edge<'a>(
    specs: &'a [AgentSpec],
    edge: &SupervisorEdge,
    local_host: &str,
) -> Option<&'a AgentSpec> {
    let mut matches = specs.iter().filter(|spec| match edge {
        SupervisorEdge::Id(id) => spec.agent_id(local_host) == *id,
        SupervisorEdge::Positional(key) => spec.legacy_bus_identity(local_host) == *key,
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

/// Resolve one declaration's supervisor edge to the parent's spec, in the child's own namespace.
pub fn resolve_supervisor_spec<'a>(
    specs: &'a [AgentSpec],
    child: &AgentSpec,
    local_host: &str,
) -> Option<&'a AgentSpec> {
    resolve_edge(specs, &supervisor_edge(specs, child, local_host)?, local_host)
}

/// Resolve one already-typed selector to exactly one spec in this snapshot.
pub fn resolve_selector<'a>(
    specs: &'a [AgentSpec],
    selector: &AgentSelector,
    local_host: &str,
) -> Option<&'a AgentSpec> {
    let book = address_book(specs, local_host).ok()?;
    resolve_in(specs, &book, selector, local_host)
}

/// Resolve against a caller-held book so one walk and its uniqueness proof describe one snapshot.
fn resolve_in<'a>(
    specs: &'a [AgentSpec],
    book: &AddressBook,
    selector: &AgentSelector,
    pinned_host: &str,
) -> Option<&'a AgentSpec> {
    let id = book.resolve(selector).ok()?.id.as_str().to_owned();
    // The book answers with one subject even for a catalog that declares an ID twice; the walk
    // stays fail-closed on that catalog by refusing an edge it cannot attribute to one spec.
    let mut matches = specs
        .iter()
        .filter(|spec| spec.agent_id(pinned_host) == id);
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

/// Whether a declaration occupies its host's root slot in the org chart: no supervisor, and not
/// retired. Retirement — either spelling; the folded `AgentDesiredState` normalizes legacy
/// `retired #true` and `desired-state "retired"` — removes a declaration from the org chart, so a
/// retired root does not hold the slot. Suspension keeps the declaration in the chart, so a
/// suspended root still counts (#402).
pub fn is_counted_root(spec: &AgentSpec) -> bool {
    spec.supervisor.is_none() && !spec.desired_state.is_retired()
}

/// Every spec from `start` to the root inclusive, `start` first.
pub fn chain<'a>(
    specs: &'a [AgentSpec],
    start: &'a AgentSpec,
    this_host: &str,
) -> Result<Vec<&'a AgentSpec>, SupervisorChainError> {
    let mut chain = Vec::new();
    let mut visited = HashSet::new();
    let mut current = start;
    // One admission proof for the whole walk: a catalog this snapshot cannot project into one
    // address book cannot attribute an edge either, so the walk fails rather than silently
    // skipping over an inadmissible declaration.
    address_book(specs, this_host).map_err(|_| SupervisorChainError::MissingSupervisor)?;

    for _ in 0..SUPERVISOR_CHAIN_LIMIT {
        // The guard keys on the immutable ID: two declarations reachable under different addresses
        // are the same node in this graph exactly when they are the same subject.
        if !visited.insert(current.agent_id(this_host)) {
            return Err(SupervisorChainError::Cycle);
        }
        chain.push(current);
        let Some(edge) = supervisor_edge(specs, current, this_host) else {
            return Ok(chain);
        };
        current =
            resolve_edge(specs, &edge, this_host).ok_or(SupervisorChainError::MissingSupervisor)?;
    }

    Err(SupervisorChainError::DepthLimit)
}

/// The immutable agent IDs of [`chain`], `start` first. What DING's relationship marker keys on:
/// a marker must not change when either party's address does.
pub fn chain_agent_ids(
    specs: &[AgentSpec],
    start: &AgentSpec,
    this_host: &str,
) -> Result<Vec<String>, SupervisorChainError> {
    Ok(chain(specs, start, this_host)?
        .into_iter()
        .map(|spec| spec.agent_id(this_host))
        .collect())
}

/// Every ancestor of `start`, nearest first, excluding `start` itself.
pub fn ancestors<'a>(
    specs: &'a [AgentSpec],
    start: &'a AgentSpec,
    this_host: &str,
) -> Result<Vec<&'a AgentSpec>, SupervisorChainError> {
    let mut chain = chain(specs, start, this_host)?;
    chain.remove(0);
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// One declaration. `id`/`address` are written only when supplied, so a legacy subject is
    /// expressed exactly as an unmigrated catalog spells it.
    fn declare(
        root: &Path,
        host: &str,
        identity: &str,
        id: Option<&str>,
        address: Option<&str>,
        supervisor: Option<&str>,
    ) {
        let directory = root.join(host).join(identity);
        std::fs::create_dir_all(&directory).unwrap();
        let id = id.map(|id| format!("  id {id:?}\n")).unwrap_or_default();
        let address = address
            .map(|address| format!("  address {address:?}\n"))
            .unwrap_or_default();
        let supervisor = supervisor
            .map(|value| format!("  supervisor {value:?}\n"))
            .unwrap_or_default();
        std::fs::write(
            directory.join("agent.kdl"),
            format!(
                "agent {identity:?} {{\n  identity {identity:?}\n{id}{address}  host {host:?}\n{supervisor}  type \"service\"\n  pty \"agent\" {{ command \"x\" }}\n}}\n"
            ),
        )
        .unwrap();
    }

    fn specs(root: &Path) -> Vec<AgentSpec> {
        let discovered = crate::discover(root);
        assert!(discovered.errors.is_empty(), "{:?}", discovered.errors);
        discovered.specs
    }

    fn parent_of(specs: &[AgentSpec], identity: &str, host: &str) -> String {
        let child = specs
            .iter()
            .find(|spec| spec.identity == identity)
            .expect("child declared");
        resolve_supervisor_spec(specs, child, host)
            .map(|parent| parent.identity.clone())
            .unwrap_or_else(|| "<unresolved>".to_owned())
    }

    fn edge_for(specs: &[AgentSpec], identity: &str, host: &str) -> Option<SupervisorEdge> {
        let child = specs
            .iter()
            .find(|spec| spec.identity == identity)
            .expect("child declared");
        supervisor_edge(specs, child, host)
    }

    /// A migrated child's `supervisor` is its parent's immutable ID, so a bystander whose mutable
    /// ADDRESS is byte-equal to that ID must not capture the edge.
    #[test]
    fn a_migrated_child_resolves_its_supervisor_only_in_the_id_namespace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        declare(root, "h", "parent", Some("boss-id"), Some("boss"), None);
        // The bystander's ADDRESS is exactly the parent's ID.
        declare(root, "h", "decoy", Some("decoy-id"), Some("boss-id"), None);
        declare(
            root,
            "h",
            "child",
            Some("child-id"),
            Some("child"),
            Some("boss-id"),
        );
        let specs = specs(root);

        assert_eq!(
            parent_of(&specs, "child", "h"),
            "parent",
            "a migrated child's supervisor ID must not be answered by a byte-equal address"
        );
        assert_eq!(
            edge_for(&specs, "child", "h"),
            Some(SupervisorEdge::Id("boss-id".to_owned()))
        );
    }

    /// An unmigrated child's `supervisor` is a legacy POSITIONAL reference, so a migrated
    /// bystander whose explicit ID is byte-equal to the bare reference must not capture the edge,
    /// a bare reference stays on the child's own host, and a qualified one reaches another host.
    #[test]
    fn an_unmigrated_child_resolves_its_supervisor_only_as_a_positional_reference() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        declare(root, "h", "boss", None, None, None);
        // The bystander's immutable ID is exactly the bare reference the child authored.
        declare(root, "h", "decoy", Some("boss"), Some("decoy"), None);
        // A same-identity subject on another host must not answer a bare local reference.
        declare(root, "other", "boss", None, None, None);
        declare(root, "h", "child", None, None, Some("boss"));
        // A qualified legacy reference reaches the declaration it names on any admitted host.
        declare(root, "h", "remote-child", None, None, Some("other.boss"));
        let specs = specs(root);

        assert_eq!(
            edge_for(&specs, "child", "h"),
            Some(SupervisorEdge::Positional("h.boss".to_owned())),
            "a bare legacy reference is qualified with the child's own host, never routed"
        );
        let parent = resolve_supervisor_spec(
            &specs,
            specs
                .iter()
                .find(|spec| spec.identity == "child")
                .expect("child declared"),
            "h",
        )
        .expect("local boss resolves");
        assert_eq!(parent.identity, "boss");
        assert_eq!(
            parent.agent_id("h"),
            "h.boss",
            "the legacy parent keeps its frozen positional ID, not the bystander's"
        );
        assert_eq!(parent_of(&specs, "remote-child", "h"), "boss");
        assert_eq!(
            edge_for(&specs, "remote-child", "h"),
            Some(SupervisorEdge::Positional("other.boss".to_owned()))
        );
        assert_eq!(
            chain_agent_ids(
                &specs,
                specs
                    .iter()
                    .find(|spec| spec.identity == "child")
                    .expect("child declared"),
                "h"
            )
            .unwrap(),
            vec!["h.child".to_owned(), "h.boss".to_owned()],
            "the walk itself must use the child's own namespace at every hop"
        );
    }
}
