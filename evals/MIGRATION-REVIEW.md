# Source eval migration review

Date: 2026-08-28

Source: `compoundingtech/evals` at `3db48ab56d40ce27dfd94f89d2db9b692d93836a`.

This review covers all 58 active source cells. It also considers the four cells that the source already retired.

## Recommendation

Archive the source repository after the selected cells and evidence enter this repository.

Do not copy every cell as a KDL eval. Product behavior with strong Rust coverage belongs in the Rust suite.

Use one logical eval for one behavior. A Claude or Codex variant is a harness choice, not a new eval.

The proposed result has 26 logical evals:

- Three logical evals already exist here.
- Twenty-three new logical evals come from 25 source cells.
- One source cell merges into an existing eval.
- Twelve source cells remain as Rust coverage.
- Fourteen research or rejected-control cells do not enter the active corpus.

The source catalog lists accepted pass evidence for seven cells. Most cells have no structured run record.

Selection therefore depends on the product contract. It does not depend only on a historic green result.

## Decision terms

`Represented` means that the behavior already has an st2 and an st3 form in this repository.

`Keep` means that we should import the fixture and rewrite its KDL for the current design.

`Merge` means that another logical eval should absorb the unique assertions.

`Rust` means that we should not keep a second KDL copy. The named Rust tests own the contract.

`Archive` means that the source Git history remains the record. The active corpus does not copy the cell.

## Complete classification

| Source cell | Decision | Target or owner | Reason |
| --- | --- | --- | --- |
| `adopt-only-migration` | Rust | `tests/reconcile.rs`, `tests/run.rs` | The Rust suite covers live adoption, absent hold, dead hold, and explicit replacement. |
| `agent-spec-resource-bindings` | Rust | `crates/agent-spec/tests/discovery.rs`, `tests/validate.rs`, `tests/reconcile.rs` | The Rust suite covers parsing, stable projection, and no-restart resource edits. |
| `assignment-contract-cold-assignment` | Archive | Source history | This old Assignment wrapper was an experimental control. It is not an st3 plan assignment. |
| `assignment-contract-cold-focus` | Archive | Source history | Focus was an experimental selector control. The source project did not select it. |
| `assignment-contract-cold-resources` | Keep, wave 1 | `resource-cold-start` | Preserve cold discovery from one named work resource. Add an explicit st3 plan assignment. |
| `assignment-contract-handoff-assignment` | Archive | Source history | This old Assignment wrapper was an experimental control. It is not an st3 plan assignment. |
| `assignment-contract-handoff-focus` | Archive | Source history | Focus was an experimental selector control. The source project did not select it. |
| `assignment-contract-handoff-resources` | Keep, wave 1 | `resource-handoff` | Preserve revoke-before-grant handoff and post-revocation safety. |
| `assignment-contract-hot-assignment` | Archive | Source history | This old Assignment wrapper was an experimental control. It is not an st3 plan assignment. |
| `assignment-contract-hot-focus` | Archive | Source history | Focus was an experimental selector control. The source project did not select it. |
| `assignment-contract-hot-resources` | Keep, wave 1 | `resource-retarget` | Preserve live retarget, removal, and idle behavior. |
| `context-resource-continuity` | Keep, wave 1 | `context-resource-continuity` | This fast cell crosses the real lifecycle boundary and checks durable state together. |
| `crash-ding` | Keep, wave 1 | `crash-escalation` | Keep a free synthetic core. Add a rare native-harness probe for provider exit behavior. |
| `ding-mode` | Merge | `license-mit` | License MIT already covers delegated work over Small Talk. Keep only missing message assertions. |
| `ding-reply` | Keep, wave 1 | `small-talk-reply` | Preserve the requirement that an agent replies on the original thread. Remove the obsolete no-MCP claim. |
| `docs` | Keep, wave 2 | `docs-cold-reader` | The cold-reader judge tests usable documentation, not keyword coverage. |
| `exec-lifecycle-logging` | Rust | `tests/exec_backend.rs` | The Rust suite covers process lifetime, logs, rotation, adoption, and group teardown. |
| `feature-fit` | Keep, wave 2 | `feature-fit` | This tests convention discovery in an existing codebase. |
| `fork-in-the-road` | Keep, wave 1 | `fork-in-the-road` | Merge the provider twins. The scenario tests parallel analysis, synthesis, and a real tradeoff. |
| `fork-in-the-road-codex` | Keep, wave 1 | `fork-in-the-road` | This is the same logical eval and carries accepted pass evidence. |
| `ghost-bug` | Represented | `ghost-bug` | The local Codex pair represents both source provider forms. |
| `ghost-bug-codex` | Represented | `ghost-bug` | The local Codex pair represents both source provider forms. |
| `hook-integrity` | Rust | `tests/hooks.rs`, `tests/materialize.rs`, `tests/claude_hooks.rs`, `tests/codex_hooks.rs` | The Rust suite covers receipts, verification, native overlays, and idempotency. |
| `host-lock-health-negatives` | Rust | `tests/doctor.rs` | The Rust suite covers required, stale, foreign, and bounded-failure lock states. |
| `inbox-hygiene` | Keep, wave 1 | `small-talk-redelivery` | Rewrite it around graph work IDs and idempotent claims. Do not use an agent-private ledger as truth. |
| `incident-response` | Keep, wave 2 | `incident-response` | It tests mitigation, root cause, and a mutation-valid regression. |
| `license-mit` | Represented | `license-mit` | The local Claude pair represents both source provider forms. |
| `license-mit-codex` | Represented | `license-mit` | The local Claude pair represents both source provider forms. |
| `migration` | Keep, wave 2 | `dependency-migration` | It tests migration breadth and preservation of a removed capability. |
| `poisoned-pr` | Keep, wave 2 | `poisoned-pr` | Merge the provider twins. This gives the corpus a strong review-only lane. |
| `poisoned-pr-codex` | Keep, wave 2 | `poisoned-pr` | This is the same logical eval and carries accepted pass evidence. |
| `presence-ding-matrix` | Rust | `src/ding/mod.rs` tests | The Rust suite covers busy, away, DND, stale DND, FIFO, and retry behavior. |
| `pty-attach-machine-stream` | Keep, wave 1 | Model-free PTY contract | This verifies the installed PTY stream through the boundary that st2 and st3 use. |
| `pty-attach-only` | Keep, wave 1 | Model-free PTY contract | This verifies that attach never creates or restarts a session. |
| `pty-send-peek` | Keep, wave 1 | Model-free PTY contract | This gives a real byte round trip and has accepted pass evidence. |
| `reconcile-retire-keep` | Rust | `tests/reconcile.rs` | The Rust suite covers missing, dead, keep, retired, and adopt lifecycle rules. |
| `render-target-safety` | Rust | `tests/materialize.rs` | The Rust suite covers directives, idempotency, tracked targets, and unsafe paths. |
| `restart-continuity` | Keep, wave 1 | `restart-continuity` | Lift the full work plan and progress into the st3 graph before the injected restart. |
| `security-audit` | Keep, wave 2 | `security-audit` | This tests whole-repository analysis, severity, and false-positive discipline. |
| `shared-workspace-render-ownership` | Rust | `tests/materialize.rs`, `tests/validate.rs` | The Rust suite covers conflicting ownership and identical shared claims. |
| `signal-rename` | Represented | `signal-rename` | The local Codex pair represents both source provider forms. |
| `signal-rename-codex` | Represented | `signal-rename` | The local Codex pair represents both source provider forms. |
| `skill-inheritance` | Keep, wave 2 | `claude-skill-inheritance` | This is a useful Claude package and project-skill integration check. |
| `st2-doctor-structure` | Rust | `tests/doctor.rs` | The Rust suite checks healthy and broken catalogs with mutation-valid failures. |
| `st2-network` | Keep, wave 1 | Model-free network smoke | Keep one black-box proof that hosting and message delivery work together. |
| `strict-validation-json` | Rust | `tests/validate.rs` | The Rust suite covers stable issue data and strict warning promotion. |
| `targeted-reconcile-isolation` | Rust | `tests/targeted_reconcile.rs` | The Rust suite covers exact selection and sibling isolation. |
| `test-writing` | Keep, wave 2 | `test-writing` | Mutation scoring makes this stronger than a test-count eval. |
| `two-networks-coexist` | Keep, wave 1 | Model-free network isolation | Keep the concurrent catalog, message, and PTY partition proof. |
| `vrs-cross-file-absent` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-cross-file-present` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-definition-of-done-absent` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-definition-of-done-present` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-scope-drift-absent` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-scope-drift-present` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-scope-pressure-absent` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `vrs-scope-pressure-present` | Archive | Source research history | This matched pair tests a governance document treatment, not runtime behavior. |
| `weird-git-setup` | Keep, wave 2 | `weird-git-setup` | This is a real launch environment edge case and has accepted pass evidence. |

## Why the selected resource cells survive

The nine assignment cells form a three-by-three experiment. The source project selected direct named resources.

The Focus and old Assignment forms remain controls. They should not become new st3 product layers.

This decision does not remove plan assignment from st3. An st3 plan step still has an explicit agent assignment.

The new cells should test three different events:

- A cold agent discovers ready work.
- A live agent receives a new resource target and later becomes idle.
- One agent loses authority before another agent receives authority.

The st3 forms should use plans, steps, resources, claims, and Small Talk. They should not copy the old controller tricks.

## Why the VRS pairs do not enter the active corpus

The eight VRS cells form four matched prompt experiments. They test the effect of governance documents on model behavior.

That research can remain useful. It is not a runtime acceptance contract, and none of the pairs has structured run evidence.

The archived repository preserves the fixtures and judges at the pinned commit. We can later create a separate research corpus.

We can also reuse an individual task after we define a clear runtime question. We should not carry the A/B structure by default.

## Migration rules

Each selected agent eval must use a native `harness` block. A fixture process can still use `command`.

Each logical agent eval gets an st2 and an st3 form when both runtimes can express the same outcome.

A runtime-independent model-free contract needs one form. Its report must name the tested binary versions.

The st3 form must lift the work plan, step state, assignment, products, and judges into the graph.

The graph must hold durable work state. Agent prose and a private todo file must not be the only state.

The st3 form must use Small Talk for direct coordination. It must not add a private message channel.

Codex `gpt-5.6-sol` is the default model judge. A specific eval can choose a Claude judge and record the reason.

Do not add an old-KDL adapter. Translate a selected st2 KDL before st3 publishes it.

Every paid run must add a dated report. The report records duration, token use, graph transitions, messages, and judge results.

## Migration order

Wave 1 proves the runtime:

1. Import the six model-free lifecycle, PTY, and network contracts.
2. Rewrite restart continuity and Small Talk redelivery around durable graph state.
3. Rewrite the three selected resource scenarios.
4. Add crash escalation and threaded reply.
5. Merge the fork-in-the-road provider twins and lift its parallel plan into the graph.

Wave 2 broadens agent work:

1. Import poisoned PR, docs, test writing, and weird Git setup.
2. Import feature fit, incident response, dependency migration, and security audit.
3. Import Claude skill inheritance as a provider integration cell.

Rust-owned cells do not wait for these waves. Before source archival, run their named suites and close any missing assertion.

## Archive gate

The source repository can become read-only when all items below are true:

- Each `Keep` row exists here or has an explicit removal decision.
- Each `Rust` row has green named coverage on supported platforms.
- Imported source evidence includes its source commit and original run receipts.
- The local inventory generates from the embedded corpus.
- The old overnight runner has no unique production duty.
- The source repository README points to this repository and its exact corpus location.
- One final script proves that all 58 active source cells have one decision in this review.

The four source-retired cells remain retired: `clean-compose`, `compose-config-load`, `compose-global-skill`, and `team-standup`.
