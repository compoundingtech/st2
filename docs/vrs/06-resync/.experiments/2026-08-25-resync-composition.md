# Resync composition prototype: watch → classify → digest-keyed emit → inbox

## Question

Does the composed mechanism work end to end against the real filesystem and the
real stream ingress — a carrier change produces exactly one deduplicated,
superseded inbox event; equal-byte rewrites stay silent; silent-class stores
never notify; whole-file replacement by rename stays visible?

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

All three integration tests pass (4.3 s wall). Unit tests pass
(`cargo test --lib resync::`). Pre-existing suite: verified against baseline in
the same run (results recorded in the PR description); the only formatting
drift (`src/ding/mod.rs`, `src/eval_run.rs`) predates this branch and was left
untouched.

## Conclusion

The composition holds without new delivery semantics: DING, archive, ring
dedup, and supersession are inherited untouched. The two discoveries above are
now pinned by the tests that exposed them.

## VRS Impact

Supports [`06-resync/requirements.md`](../../06-resync/requirements.md)
RESYNC-R01/R03/R04/R06/R07 and [`spec.md`](../../06-resync/spec.md). No
protected-document change proposed. The catalog-relative resource URI form is
an st2 extension pending canonical Agent Spec adoption (noted in
[`02-agent-spec`](../../02-agent-spec/requirements.md) terms).
