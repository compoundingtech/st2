//! Address authoring: the mutable host-local address, cut over atomically, never the immutable ID.
//!
//! Every case here runs against a real temporary catalog and proves its claim by rediscovering that
//! catalog and resolving through the same address book ordinary references use. Nothing is modelled.

use std::fs;
use std::path::Path;

use st2::agent_author::{
    AuthorOutcome, DesiredStateValue, refuse_agent_id_change, set_address, set_desired_state,
};
use st2::{AddressBook, AgentAddress, AgentSelector, ResolveError, spec::address_book};

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

/// One canonical KDL declaration with an unrelated comment and an unrelated `meta` value, so every
/// byte-preservation claim has something to preserve.
fn declaration(identity: &str, host: &str, extra: &str) -> String {
    format!(
        "// unrelated comment\nagent {identity:?} {{\n  host {host:?}\n  meta {{ managed-by \"catalog\"; owner \"platform\" }}\n{extra}  command \"sleep 300\"\n}}\n"
    )
}

fn book(root: &Path, this_host: &str) -> AddressBook {
    let found = st2::discover(root);
    assert!(found.errors.is_empty(), "{:?}", found.errors);
    address_book(&found.specs, this_host).unwrap()
}

fn resolved_id(root: &Path, this_host: &str, reference: &str) -> Result<String, ResolveError> {
    book(root, this_host)
        .resolve_address(reference, None)
        .map(|subject| subject.id.as_str().to_owned())
}

fn address(value: &str) -> AgentAddress {
    AgentAddress::parse(value).unwrap()
}

#[test]
fn first_explicit_address_cuts_over_immediately_and_releases_the_identity_fallback() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let source = declaration("worker", "h", "");
    write(root, "h/worker/agent.kdl", &source);

    assert_eq!(
        resolved_id(root, "h", "worker").unwrap(),
        "h.worker",
        "the positional identity is the effective address before any explicit one"
    );

    let receipt = set_address(root, "h.worker", "h", None, Some(&address("build.owner"))).unwrap();
    assert_eq!(receipt.result, AuthorOutcome::Changed);
    assert_eq!(receipt.id, "h.worker");
    assert_eq!(receipt.address, "build.owner");
    assert_eq!(receipt.bus_address.as_deref(), Some("h.build.owner"));
    assert!(receipt.explicit);

    assert_eq!(resolved_id(root, "h", "build.owner").unwrap(), "h.worker");
    assert_eq!(resolved_id(root, "h", "h.build.owner").unwrap(), "h.worker");
    assert!(
        matches!(
            resolved_id(root, "h", "worker"),
            Err(ResolveError::UnknownAddress { .. })
        ),
        "the prior effective address stops resolving the moment the new generation is visible"
    );

    let subject = book(root, "h").resolve_id("h.worker").unwrap().clone();
    assert_eq!(
        subject.id.as_str(),
        "h.worker",
        "an address cutover never moves the immutable id"
    );
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl"))
            .unwrap()
            .replace("  address \"build.owner\"\n", ""),
        source,
        "only the address span changed"
    );
}

#[test]
fn a_later_address_change_releases_the_previous_one() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/worker/agent.kdl", &declaration("worker", "h", ""));

    set_address(root, "h.worker", "h", None, Some(&address("first"))).unwrap();
    let receipt = set_address(root, "h.worker", "h", None, Some(&address("second"))).unwrap();
    assert_eq!(receipt.result, AuthorOutcome::Changed);
    assert_eq!(receipt.address, "second");

    assert_eq!(resolved_id(root, "h", "second").unwrap(), "h.worker");
    assert!(matches!(
        resolved_id(root, "h", "first"),
        Err(ResolveError::UnknownAddress { .. })
    ));
    assert_eq!(
        set_address(root, "h.worker", "h", None, Some(&address("second")))
            .unwrap()
            .result,
        AuthorOutcome::Unchanged,
    );
}

#[test]
fn clear_restores_the_identity_fallback() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let source = declaration("worker", "h", "");
    write(root, "h/worker/agent.kdl", &source);

    set_address(root, "h.worker", "h", None, Some(&address("build.owner"))).unwrap();
    let receipt = set_address(root, "h.worker", "h", None, None).unwrap();

    assert_eq!(receipt.result, AuthorOutcome::Changed);
    assert_eq!(receipt.address, "worker");
    assert_eq!(receipt.bus_address.as_deref(), Some("h.worker"));
    assert!(!receipt.explicit);
    assert_eq!(resolved_id(root, "h", "worker").unwrap(), "h.worker");
    assert!(matches!(
        resolved_id(root, "h", "build.owner"),
        Err(ResolveError::UnknownAddress { .. })
    ));
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        source,
        "clearing restores the original bytes exactly"
    );
}

#[test]
fn clear_is_refused_when_the_restored_fallback_is_already_claimed() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/worker/agent.kdl", &declaration("worker", "h", ""));
    write(root, "h/other/agent.kdl", &declaration("other", "h", ""));

    set_address(root, "h.worker", "h", None, Some(&address("build.owner"))).unwrap();
    // `other` claims the address `worker` just released, so the fallback is no longer free.
    set_address(root, "h.other", "h", None, Some(&address("worker"))).unwrap();
    let before = fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap();

    let error = set_address(root, "h.worker", "h", None, None).unwrap_err();
    assert_eq!(error.code(), "address-fallback-conflict");
    assert_eq!(
        fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap(),
        before,
        "a refused clear writes nothing"
    );
    assert_eq!(resolved_id(root, "h", "worker").unwrap(), "h.other");
}

#[test]
fn a_host_local_collision_is_refused_against_an_explicit_address_and_an_identity_fallback() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/worker/agent.kdl", &declaration("worker", "h", ""));
    write(root, "h/helper/agent.kdl", &declaration("helper", "h", ""));
    write(root, "h/router/agent.kdl", &declaration("router", "h", ""));
    set_address(root, "h.router", "h", None, Some(&address("edge"))).unwrap();

    let fallback = set_address(root, "h.worker", "h", None, Some(&address("helper"))).unwrap_err();
    assert_eq!(fallback.code(), "address-conflict");
    assert!(
        fallback.to_string().contains("h.helper"),
        "the refusal names the subject already holding the address: {fallback}"
    );

    let explicit = set_address(root, "h.worker", "h", None, Some(&address("edge"))).unwrap_err();
    assert_eq!(explicit.code(), "address-conflict");

    assert_eq!(resolved_id(root, "h", "helper").unwrap(), "h.helper");
    assert_eq!(resolved_id(root, "h", "edge").unwrap(), "h.router");
    assert_eq!(resolved_id(root, "h", "worker").unwrap(), "h.worker");
}

#[test]
fn the_same_address_is_admitted_on_two_different_hosts() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h1/worker/agent.kdl", &declaration("worker", "h1", ""));
    write(root, "h2/worker/agent.kdl", &declaration("worker", "h2", ""));

    set_address(root, "h1.worker", "h1", None, Some(&address("edge"))).unwrap();
    set_address(root, "h2.worker", "h1", None, Some(&address("edge"))).unwrap();

    let book = book(root, "h1");
    assert_eq!(
        book.resolve_address("edge", Some("h1")).unwrap().id.as_str(),
        "h1.worker"
    );
    assert_eq!(
        book.resolve_address("h2.edge", None).unwrap().id.as_str(),
        "h2.worker"
    );
    assert!(
        matches!(
            book.resolve_address("edge", None),
            Err(ResolveError::AmbiguousAddress { .. })
        ),
        "uniqueness is per logical host; an unpinned bare reference across hosts stays ambiguous"
    );
}

#[test]
fn an_id_and_an_address_with_equal_bytes_are_separate_namespaces() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/alpha/agent.kdl",
        &declaration("alpha", "h", "  id \"ops.router\"\n"),
    );
    write(root, "h/beta/agent.kdl", &declaration("beta", "h", ""));

    let receipt = set_address(root, "h.beta", "h", None, Some(&address("ops.router"))).unwrap();
    assert_eq!(receipt.result, AuthorOutcome::Changed);

    let book = book(root, "h");
    assert_eq!(
        book.resolve_id("ops.router").unwrap().id.as_str(),
        "ops.router",
        "the id namespace still answers with alpha"
    );
    assert_eq!(
        book.resolve_id("ops.router").unwrap().effective_address,
        "alpha"
    );
    assert_eq!(
        book.resolve_address("ops.router", None).unwrap().id.as_str(),
        "h.beta",
        "the address namespace answers with beta"
    );
    assert_eq!(
        book.resolve_id("h.beta").unwrap().effective_address,
        "ops.router",
        "beta stays reachable by its own id and reports its new address"
    );
}

#[test]
fn an_exact_id_lookup_never_falls_through_to_address_lookup() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/worker/agent.kdl", &declaration("worker", "h", ""));

    let error = set_address(root, "worker", "h", None, Some(&address("edge"))).unwrap_err();
    assert_eq!(
        error.code(),
        "target-not-found",
        "`worker` is a live address, never an id: {error}"
    );
    assert_eq!(resolved_id(root, "h", "worker").unwrap(), "h.worker");
}

#[test]
fn unsupported_formats_and_nix_owned_declarations_fail_closed() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/toml/agent.toml",
        "identity = \"toml\"\nhost = \"h\"\ncommand = \"sleep 300\"\n",
    );
    write(
        root,
        "h/json/agent.json",
        "{\"identity\":\"json\",\"host\":\"h\",\"command\":\"sleep 300\"}\n",
    );
    write(
        root,
        "h/nix/agent.kdl",
        "agent \"nix\" {\n  host \"h\"\n  meta { managed-by \"nix\" }\n  command \"sleep 300\"\n}\n",
    );

    for id in ["h.toml", "h.json"] {
        let error = set_address(root, id, "h", None, Some(&address("edge"))).unwrap_err();
        assert_eq!(error.code(), "unsupported-declaration-format", "{id}");
    }
    let error = set_address(root, "h.nix", "h", None, Some(&address("edge"))).unwrap_err();
    assert_eq!(error.code(), "nix-managed-declaration");
}

#[test]
fn direct_id_authoring_is_refused_and_address_authoring_never_moves_an_id() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration("worker", "h", "  id \"0199b8f4-8d3a-7c21-9a44-6f85b7320ea1\"\n"),
    );

    let refusal = refuse_agent_id_change("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1", "h.worker")
        .unwrap_err();
    assert_eq!(refusal.code(), "immutable-agent-id");
    assert!(
        refusal.to_string().contains("retire"),
        "the refusal names the supported replacement path: {refusal}"
    );

    set_address(
        root,
        "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1",
        "h",
        None,
        Some(&address("build.owner")),
    )
    .unwrap();
    let source = fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap();
    assert!(source.contains("id \"0199b8f4-8d3a-7c21-9a44-6f85b7320ea1\""));
    assert_eq!(
        book(root, "h")
            .resolve_address("build.owner", None)
            .unwrap()
            .id
            .as_str(),
        "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1",
        "the subject kept its id across the cutover"
    );
}

#[test]
fn unrelated_bytes_and_unknown_declaration_shapes_survive_a_cutover() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let source = concat!(
        "// leading comment kept verbatim\n",
        "agent \"worker\" {\n",
        "  host \"h\"\n",
        "  meta { managed-by \"catalog\"; owner \"platform\"; ticket \"DELTA-003\" }\n",
        "  name \"Build owner\" // trailing note\n",
        "  command \"sleep 300\"\n",
        "}\n",
    );
    write(root, "h/worker/agent.kdl", source);

    set_address(root, "h.worker", "h", None, Some(&address("build.owner"))).unwrap();
    let after = fs::read_to_string(root.join("h/worker/agent.kdl")).unwrap();

    assert_eq!(
        after.replace("  address \"build.owner\"\n", ""),
        source,
        "exactly one inserted line differs"
    );
    assert!(after.contains("ticket \"DELTA-003\""));
    assert!(after.contains("// trailing note"));
}

#[test]
fn id_keyed_authority_admits_self_and_a_descendant_and_refuses_a_non_descendant() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(root, "h/root/agent.kdl", &declaration("root", "h", ""));
    write(
        root,
        "h/child/agent.kdl",
        &declaration("child", "h", "  supervisor \"root\"\n"),
    );
    write(
        root,
        "h/grandchild/agent.kdl",
        &declaration("grandchild", "h", "  supervisor \"h.child\"\n"),
    );
    write(root, "h/stranger/agent.kdl", &declaration("stranger", "h", ""));

    // Both edges are unmigrated legacy positional references, so the grandparent reaches the
    // grandchild through the declaration slots that migration will rewrite to immutable IDs.
    set_address(
        root,
        "h.grandchild",
        "h",
        Some(&AgentSelector::id("h.root")),
        Some(&address("grandchild.edge")),
    )
    .unwrap();
    set_address(
        root,
        "h.child",
        "h",
        Some(&AgentSelector::id("h.child")),
        Some(&address("child.edge")),
    )
    .unwrap();

    // An address cutover does not change a legacy declaration slot. The grandparent therefore
    // remains authorized until migration rewrites the edge to the same immutable parent ID.
    set_address(
        root,
        "h.grandchild",
        "h",
        Some(&AgentSelector::id("h.root")),
        Some(&address("grandchild.edge2")),
    )
    .unwrap();

    let error = set_address(
        root,
        "h.stranger",
        "h",
        Some(&AgentSelector::id("h.child")),
        Some(&address("stranger.edge")),
    )
    .unwrap_err();
    assert_eq!(error.code(), "address-not-authorized");
    assert_eq!(resolved_id(root, "h", "stranger").unwrap(), "h.stranger");

    let by_address = set_address(
        root,
        "h.child",
        "h",
        Some(&AgentSelector::id("h.child.edge")),
        Some(&address("child.edge2")),
    )
    .unwrap_err();
    assert_eq!(
        by_address.code(),
        "address-not-authorized",
        "an actor is an id; its own bus address never authorizes it"
    );
}

#[test]
fn leaving_retirement_validates_effective_address_uniqueness() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    write(
        root,
        "h/worker/agent.kdl",
        &declaration(
            "worker",
            "h",
            "  desired-state \"retired\" reason=\"replaced\"\n",
        ),
    );
    write(root, "h/other/agent.kdl", &declaration("other", "h", ""));

    // A retired subject is non-routable and releases its address, so `other` may claim it.
    set_address(root, "h.other", "h", None, Some(&address("worker"))).unwrap();

    let error = set_desired_state(
        root,
        "h.worker",
        "h",
        None,
        DesiredStateValue::Running,
        None,
    )
    .unwrap_err();
    assert_eq!(error.code(), "address-conflict");
    assert!(
        st2::discover(root)
            .specs
            .iter()
            .any(|spec| spec.identity == "worker" && spec.desired_state.is_retired()),
        "a refused transition leaves the declaration retired"
    );

    // Giving the retired subject its own address makes the same transition admissible.
    let retired_cutover =
        set_address(root, "h.worker", "h", None, Some(&address("worker.next"))).unwrap();
    assert_eq!(
        retired_cutover.bus_address, None,
        "a retired subject is non-routable and holds no bus address, only an id"
    );
    assert_eq!(retired_cutover.address, "worker.next");
    assert!(
        matches!(
            resolved_id(root, "h", "worker.next"),
            Err(ResolveError::UnknownAddress { .. })
        ),
        "and it does not occupy the address namespace while retired"
    );
    let receipt = set_desired_state(
        root,
        "h.worker",
        "h",
        None,
        DesiredStateValue::Running,
        None,
    )
    .unwrap();
    assert_eq!(receipt.result, AuthorOutcome::Changed);
    assert_eq!(resolved_id(root, "h", "worker.next").unwrap(), "h.worker");
}
