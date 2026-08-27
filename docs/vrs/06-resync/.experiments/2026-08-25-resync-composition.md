# Resync composition prototype: watch → classify → occurrence-keyed emit → inbox

## Question

Does the composed mechanism work end to end against the real filesystem and the
real stream ingress — a carrier change produces exactly one superseded inbox
event; equal-byte rewrites stay silent; repeated digest transitions remain
distinct occurrences; failed publication retries one immutable occurrence;
silent-class stores never notify; whole-file replacement by rename stays
visible?

## Method

Direct test-first implementation on the feature branch (decision Q6), no
throwaway spike. Three integration tests in `tests/resync.rs` exercise
`ResyncSupervisor::spawn` + `refresh` against a real temp catalog, the real
inotify backend, and the unchanged `st2::event::emit` ingress:

1. `carrier_change_emits_one_superseded_resync_event_and_silent_stores_stay_quiet`
   — seeds baselines silently, then asserts: goal write → exactly one event
   (`stream: resync`, `binding: goal`, subject `resource goal changed`);
   equal-content rewrite → no second event; write into
   `resources/context/` → no event; changed content → fresh event under the
   same key (event-id changes, supersession retires the predecessor).
2. `whole_file_declaration_replacement_by_rename_notifies_immediately`
   — configuration-management style write-then-rename over `agent.kdl`
   notifies through the parent-directory watch.
3. `declaring_the_reserved_resync_stream_is_refused` — a declared
   `stream "resync" {}` fails discovery with the reservation error.

Plus unit tests in `src/resync.rs` for watch-set resolution (`file://`,
catalog-relative, non-local schemes), classification (goal immediate,
agent-authored stores excluded), and the built-in-stream carve-out is covered
by tests 1–2 passing through real `emit`.

Q13 occurrence identity is covered by four focused unit tests in
`src/resync.rs`:

4. `repeated_identical_transitions_receive_distinct_occurrence_identities` —
   drives A→B, B→A, A→B through the real event ingress and proves the repeated
   A→B legs have distinct IDs and subscription sequences 1 and 3.
5. `failed_emit_retains_digest_and_schedules_the_same_transition_for_retry` —
   proves a failed retry retains the exact immutable body, event ID, and
   sequence.
6. `subscribers_advance_occurrence_sequences_independently` — proves two
   subscriptions each capture their first transition at sequence 1.
7. `supervisor_restart_incarnation_changes_the_occurrence_namespace` — proves
   an incarnation change produces a different body and ID even when the
   per-subscription sequence and digest transition repeat.

Three design constraints were discovered by running the code, not by reasoning:

- **Directory-creation blind spot.** Writing a file into a newly created
  subdirectory generates events on the new directory's inode, which carries no
  watch yet. The fix dirties all carriers beneath any mutated path that is an
  ancestor of carriers and extends watches at that moment
  (`Worker::mark_mutated`). Reasoned-through designs missed this; the failing
  test found it.
- **Shutdown self-deadlock.** The worker's own watcher holds the last mailbox
  `Sender`, so channel disconnection could never fire while `join` waits. The
  worker exits on an explicit `Msg::Shutdown` sent by `Drop`.
- **Refresh reseeding erases in-flight events.** A reconcile pass can land
  between a carrier mutation and its flush window — declaration changes wake
  reconcile immediately, so this fires every time a declaration is replaced.
  Reseeding digests on watch-set application silently dropped that event; the
  eval cell caught what the Rust tests missed because only the resident
  supervisor loop races refresh against mutation processing.

## Result

The three integration tests and focused resync unit suite pass, including the
four Q13 occurrence tests. Pre-existing suite results remain recorded in the
PR description; unrelated formatting drift was left untouched.

## Conclusion

The composition holds without new delivery semantics: DING, archive, ring
dedup, and supersession are inherited untouched. The two discoveries above are
now pinned by the tests that exposed them.

## VRS Impact

Supports [`06-resync/requirements.md`](../../06-resync/requirements.md)
RESYNC-R01/R03/R04/R06/R07 and [`spec.md`](../../06-resync/spec.md). The Q13
evidence supports the approved RESYNC-R06 occurrence-identity amendment and
decision record 0008. The catalog-relative resource URI form remains an st2
extension pending canonical Agent Spec adoption (noted in
[`02-agent-spec`](../../02-agent-spec/requirements.md) terms).
