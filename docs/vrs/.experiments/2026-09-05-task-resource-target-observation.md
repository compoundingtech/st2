# Task resource target observation

Date: 2026-09-05
Fixture: deterministic process-generation seams plus a live Darwin process

## Question

Can task inventory expose a useful Linux cgroup-v2 or Darwin process-tree
locator without treating PID, systemd scope naming, or retained st2 state as
stable task identity, and without associating a locator after process exit or
PID-generation replacement?

## Method

The inventory resource-target boundary was split into two narrow operations:
strict parsing of the Linux `/proc/<pid>/cgroup` payload and a process-start
fence around target acquisition. Deterministic unit fixtures supplied stable,
changed, and disappearing start tokens around a candidate target. A PTY fixture
then reused the same numeric PID with a different backend creation generation
between the first snapshot and token capture; a confirming second snapshot
tested that exact pre-fence window. Parser
fixtures covered a unified-only record, a hybrid record, root membership,
spaces and colons, duplicate unified entries, missing v2 entries, malformed
fields, carriage returns, repeated/trailing separators, and traversal
components.

A live-process fixture read the current process start token through st2's
platform process observer and asked the production target observer to associate
that exact generation. On Darwin it asserted the strict
`darwinProcessTree/rootPid` wire shape. CLI integration reused a live legacy
exec record to prove the target is joined to the correct runtime row without
rewriting exec state. Existing complete, missing-root, malformed, timeout,
park, declaration-drift, and generation-replacement inventory cases remained
on the same command surface.

Focused commands:

```sh
nix develop -c cargo test --lib task_inventory::tests
nix develop -c cargo test --lib run::tests::pty_pid_reuse_between_snapshot_and_start_token_capture_is_rejected
nix develop -c cargo test --test task_inventory_cli
```

## Result

| Probe | Observation |
| --- | --- |
| Unified cgroup parser | Preserved one exact slash-prefixed `0::` path, including spaces and colons; rejected ambiguous, absent, relative, or traversal-capable shapes. |
| Stable start token | Published the candidate resource target. |
| Changed start token | Published only `unavailable/generationChanged`; the candidate locator was discarded. |
| PTY reuse before token capture | A second backend snapshot observed the changed creation generation and returned `unavailable/generationChanged`; the replacement token was never admitted. |
| Process disappeared during read | Published only `unavailable/processUnavailable`; the candidate locator was discarded. |
| Stable target read failed | Published the bounded degraded reason `unavailable/cgroupV2Unavailable`. |
| Live Darwin process | Published `{"type":"darwinProcessTree","rootPid":<current-pid>}` for the proven current generation. |
| Wire shape | Schema v2 requires one of three tagged variants; non-running and indeterminate runtime states also carry explicit unavailable reasons. |
| Existing inventory behavior | The same fail-closed envelope and non-zero behavior remains for unprovable catalog or runtime evidence; resource-target degradation alone remains a complete bounded observation. |

## Conclusion

A resource target can remain a pure observation. PTY first captures the kernel
token and then proves the backend generation unchanged before admitting it;
the token subsequently fences the target read. This prevents publication
across the measured pre-fence reuse, exit, and generation-change races. Strict
cgroup parsing supplies the exact kernel locator without scope-name
parsing. Darwin exposes only the weaker process-tree root the platform can
support. Runtime ID remains the task identity and no persistent registry or
sampling loop is required. This supports [R23 fail-closed task inventory](../requirements.md)
and [decision 0017](../.decisions/0017-task-resource-targets-are-strict-observations.md).

## VRS Impact

- `requirements.md` requires a fenced, explicit resource target while keeping
  inventory fail-closed and read-only.
- `ontology.md` distinguishes runtime resource targets and observation locators
  from task identity.
- `spec.md` fixes schema version 2, exact tagged shapes, parser rules, bounded
  unavailable reasons, and platform behavior.
- Decision 0017 records the strict-shape and no-registry/no-sampler boundary.
