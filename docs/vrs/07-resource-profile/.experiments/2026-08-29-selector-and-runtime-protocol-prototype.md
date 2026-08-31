# Resource selector and runtime protocol prototype

Date: 2026-08-29

## Question

Can the remaining selector-encoding and runtime-restart blockers be resolved with existing KDL, JSON, channel framing, and ownership-fencing patterns rather than introducing a KDL-to-JSON mapping or a second lifecycle model?

## Method

Three disposable Rust drivers used the repository's `kdl`, `serde`, and `serde_json` dependencies.

The selector driver serialized normalized JSON, selected the smallest KDL raw-string hash fence that did not collide with the payload, parsed the generated Resource node with KDL 6, decoded the property as JSON, and compared the resulting value with the input. Inputs covered:

- a concise GitHub topic selector;
- a nested non-GitHub path/event selector;
- an adversarial string containing one- and two-hash raw-string terminators.

The runtime wire driver round-tripped length-bounded newline-delimited JSON shapes for `register`, `unregister`, `publish`, and `health`. Each message carried a runtime owner claim. Binding-scoped messages also carried a registration token.

The ownership driver modeled runtime claims, binding registrations, publications, and restarts. It exhaustively enumerated every action sequence through depth seven over two runtime incarnations, two bindings, registration, and stale publication attempts.

## Evidence

All selector values round-tripped exactly. Representative canonical KDL was:

```kdl
resource "work" uri="github-pr://example/1" reason="Review." selector=#"{"topics":["ci.failure","mergeability.conflict","review.requested"]}"#
```

The adversarial JSON automatically selected a three-hash fence:

```kdl
selector=###"{"literal":"a\"#b\"##c"}"###
```

Dotfiles already has the matching generator precedent: `kdlRawStr context (builtins.toJSON value)` in `nixpkgs/st2/catalog.nix`. Normal quoted KDL strings are not viable there because `kdlStr` rejects embedded quotes and backslashes.

The wire messages round-tripped as tagged JSON lines without a new framing format. The ownership model checked 335,923 sequences. A publication was accepted only when both conditions held:

1. its `(incarnation, claim)` named the current runtime owner;
2. its registration token matched the current registration for that binding.

A new runtime claim cleared registrations and fenced every prior process and binding token. Shared and per-binding topology used the same model; a per-binding runtime is a shared runtime with one registration.

## Result

Agent Spec KDL can encode the descriptor-validated selector as one raw JSON string property. The canonical renderer must choose the smallest safe raw-string hash fence. The in-memory and JSON/TOML projections retain the normalized JSON value, not its KDL spelling.

The observable runtime protocol needs only four semantic messages: `register`, `unregister`, `publish`, and `health`. Process EOF is termination. The runtime owns observation and therefore needs no host `reconcile` command. Supervisor process lifecycle owns shutdown and therefore needs no protocol `shutdown` command.

Every runtime-to-host message is fenced by the current runtime owner claim. Every binding-scoped message is additionally fenced by the current registration token. This reuses the directional ownership pattern from harness state and the JSON-line framing pattern from native harness channels.

## Conclusion

Two complexity reductions survive the prototypes:

- use raw JSON in KDL instead of inventing a generic KDL-to-JSON type system;
- reduce the runtime protocol from six messages to four and use existing process EOF, supervision, and directional ownership rather than protocol-level shutdown, reconcile, or separate shared/per-binding reducers.

## VRS Impact

- Resolve DQ-P3 in favor of `selector=<raw-json-string>` with canonical minimum safe hash fencing.
- Resolve DQ-P6's ownership and topology question with one owner claim plus per-binding registration tokens.
- Remove `reconcile` and `shutdown` from the normative runtime protocol.
- Specify EOF as runtime termination and supervisor lifecycle as the only shutdown authority.
- Keep restart/backoff policy in existing task lifecycle machinery rather than the profile protocol.
