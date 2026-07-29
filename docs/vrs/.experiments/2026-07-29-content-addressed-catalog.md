# Experiment: content-addressed catalog admission

Status: implemented prototype

Date: 2026-07-29

Decision under test:
[`0002-content-addressed-catalog-root-selects-admitted-seats.md`](../.decisions/0002-content-addressed-catalog-root-selects-admitted-seats.md)

## Question

Can current st2 resolve and materialize exact content-addressed Agent Spec bytes while all
mutable agent resources remain at a stable `agent_dir`, and can it publish
multiple staged seats with one atomic root visibility change?

## Prototype Surface

Library: `src/catalog_store.rs`

Experimental JSON CLI:

```text
st2 --catalog ROOT catalog prepare SPEC
st2 --catalog ROOT catalog stage SPEC --manager M --state-relative PATH \
  --operation-id OP [--expected-ref COMMIT] [--binding-parent COMMIT]
st2 --catalog ROOT catalog admit REQUEST.json
st2 --catalog ROOT catalog publish SPEC --manager M --state-relative PATH \
  --operation-id OP [--expected-ref COMMIT] [--expected-root COMMIT]
st2 --catalog ROOT catalog head
st2 --catalog ROOT catalog inspect
```

`publish` is only a one-seat convenience composition of `stage` and `admit`.
`prepare` imports exact bytes and changes no ref or root. `stage` changes no
catalog root. An admit request is:

```json
{
  "expectedRoot": null,
  "manager": "eval",
  "operationId": "run-42:root",
  "selections": [
    {
      "refCommit": "sha256-...",
      "resourceBindingCommit": "sha256-..."
    }
  ]
}
```

## Executable Claims

Focused tests cover:

1. exact-byte object preservation, content-addressed source resolution, stable
   message/context/status paths, inline materialization, and absence of a
   projection;
2. atomic two-seat admission plus rejection of a cross-seat join without
   changing the selected root;
3. a test-scoped failure after root-commit publication leaving the old root visible,
   failure after head publication leaving the new root visible, and
   operation-id replay bound to its original expected parent;
4. competing ref publishers producing one CAS winner plus manager fencing.
5. a dynamic manager adding its owned seat to a Nix-authored root while
   preserving the untouched Nix admission byte-identically, and rejection when
   it tries to admit the Nix-owned seat itself.
6. strict digest grammar before digest-derived paths, including traversal
   negatives;
7. rejection of reserved, shared, or existing-symlink-crossing state roots; and
8. inspect resolving one captured root even when the selected head changes
   between root capture and graph resolution.

These are process-level atomic-visibility tests, not power-loss durability
proofs. The selected reachable graph is verified; commit ancestry is not
recursively audited.

## Result

Green in the isolated prototype worktree: 8 focused catalog-store tests, 1 CLI
transaction integration test, and all 157 library tests pass. This evidence
does not promote the proposed decision; acceptance remains a separate human
decision.

## Known Gaps

- The recursive discovery/reconcile path does not consume the experimental root.
- Full validation here means complete graph, digest, join, path, parse, and
  runnable validation. The legacy validator's path-layout warnings and
  host-local external filesystem checks are not yet adapted to immutable
  object provenance.
- External `render copy` inputs are not bundled with the content-addressed
  declaration. `prepare` stores exact KDL bytes, not a self-contained closure.
- Content-addressed publication and digest verification assume trusted
  same-user store custody. Verify/use sealing through one file descriptor or
  stronger filesystem mechanisms is future hardening.
- Manager strings are logical fencing labels, not authentication.
- No GC, replication, replacement API, daemon socket, typed resource contract,
  public failpoint API, or authorization framework is included.
