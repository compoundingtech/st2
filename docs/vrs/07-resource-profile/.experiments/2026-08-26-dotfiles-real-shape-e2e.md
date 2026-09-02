# Dotfiles real-shape Resource Profile end-to-end proof

## Question

Does the feature-gated wasm Resource Profile work through the full resident st2
runtime against the real Nix-generated standing-seat declaration shape —
profile parsing, URI resolution, watch installation, whole-file replacement,
event delivery, equal-byte suppression, teardown, and deterministic replay?

## Method

The experiment used the built `wasm-resolver` st2 binary and the packaged
agent-goal resolver module. It assembled a fresh scratch catalog at
`/tmp/resync-real-e2e/net` from the real standing-seat shape:

```kdl
catalog {
  pty-root "pty"
}

profile "dev.schickling.agent-goal" {
  wasm "/nix/store/...-agent-goal-resolver-wasm-0.1.0/lib/agent-goal-resolver.wasm"
  class "immediate"
}
```

The agent declaration retained the production-generated structure: `meta {
managed-by "nix" }`, identity `cos`, the ordinary argv and DING tasks, and:

```kdl
resource "goal" \
  reason="The durable mission, deliverables, acceptance criteria, and boundaries for this agent." \
  uri="dev.schickling.agent-goal://dev3/cos"
```

`resources/goal.md` began as `mission v1`. Each run performed:

1. `st2 validate --catalog net --host e2eresync --strict`.
2. Start resident `st2 up --catalog /tmp/resync-real-e2e/net` and verify the
   agent PTY and DING exec task were live through `st2 tasks --json` plus the
   child process.
3. Stage `mission v2` in a sibling file and rename it over `goal.md`, matching
   Nix activation's whole-file write-then-rename shape.
4. Wait up to 20 seconds for the inbox event, then assert its producer, stream,
   key, subject, event identity, binding/path body, and old/new file digests.
5. Replace the file again with byte-identical `mission v2`, wait two seconds,
   and assert the inbox still contained exactly one event.
6. Stop the resident supervisor, run `st2 down`, and assert no scratch PTY,
   DING, or child process survived.
7. Delete and rebuild the scratch state, then repeat the complete flow to check
   determinism across fresh runs.

The authority in the binding URI intentionally remained `dev3/cos`: the
profile is selected by scheme, and the guest maps the logical goal identity to
the local seat's `resources/goal.md`. Only the execution host label changed for
the safety reason below.

## Result

Both fresh runs passed every step.

| Stage | Proof |
| --- | --- |
| Strict declaration | `0 errors, 0 warnings across 1 agent`, exit 0, with the top-level profile block |
| Resident runtime | agent PTY and DING exec tasks running with fresh scratch processes |
| Wasm resolution | scheme URI resolved to `/tmp/resync-real-e2e/net/agents/e2eresync/cos/resources/goal.md` with class `immediate` |
| Rename visibility | first event appeared about **600 ms** after whole-file replacement, below the 20 s bound |
| Cardinality | exactly **one** inbox event for the transition |
| Equal rewrite | still exactly one event after the second, byte-identical replacement and a 2 s wait |
| Event semantics | stream `resync`, key `goal`, subject `resource goal changed`, producer `e2eresync.cos/resync`; old/new SHA-256 values matched the actual files |
| Teardown | supervisor exited cleanly; `st2 down` reported one torn-down agent; no scratch child remained |
| Replay | second fresh run produced byte-identical event content and the same deterministic event id |

The emitted record in both runs was:

```text
---
from: e2eresync.cos/resync
subject: resource goal changed
stream: resync
event-id: 567fdb37ca198bb3cb495854eb3f8ea98bde0342948b2c0cbc3c6524febebada
key: goal
---
resource `goal` changed

binding: goal
path: /tmp/resync-real-e2e/net/agents/e2eresync/cos/resources/goal.md
old: b337c4feb516944eee1f133cc2e392f631bed00ae4a426561d1f139b9ce4daec
new: 8533e115be1140f2f303fd1c257474ad5ad8b4a787b814252d4f7f65a717faad
```

The identical event id across runs is expected: both began from the same seeded
old digest and transitioned to the same new digest, so transition identity was
recomputed from the same inputs. The equal rewrite produced no transition and
therefore no second record.

## Production-safety deviation

The first attempt used execution host label `dev3`, matching the real
declaration literally. It exposed a safety hazard unrelated to Resource Profile
resolution: st2 exec/PTY runner state is keyed by host in a shared
`$XDG_STATE_HOME/st2/<host>/exec` directory, not by catalog. The scratch
supervisor therefore **adopted existing production runtime entries** whose task
IDs matched the live `dev3.cos` seat. A subsequent scratch `st2 down` could have
terminated that production seat.

The attempt was stopped before reconcile mutated anything; the existing live
seat remained untouched. The experiment was rebuilt under isolated execution
host label `e2eresync`, giving it a fresh exec-state directory and PTY namespace,
while preserving the production-shaped Resource URI
`dev.schickling.agent-goal://dev3/cos` and every other declaration element.
This is a deliberate safety deviation, not a weakening of the resolver proof:
the profile contract uses the URI scheme and the local agent directory, not the
execution host label, and the full resolver/watch/event path still ran.

The host-state collision was recorded through the agent-feedback path during
the experiment so future scratch-catalog work can fail closed before adoption.

## Conclusion

The wasm-only Resource Profile composes with the real dotfiles standing-seat
shape end to end. A scheme URI gained a contained local denotation, rename-style
activation remained observable through the parent-directory watch, exactly one
deterministic event arrived, and an equal rewrite stayed silent in two fresh
runs. The one deviation protected production runtime state and did not alter
the Resource Profile input or behavior under test.

## VRS Impact

Supports [`PROFILE-R03..R09`](../requirements.md), especially feature-mode
catalog parsing, profile-to-resync class handoff, contained local resolution,
and nondisruptive event composition. It also supplies real-runtime evidence for
[`06-resync`](../../06-resync/requirements.md) rename observation, seeded
baselines, event delivery, and equal-byte suppression. The shared host runner-
state hazard is operational feedback outside the Resource Profile contract.
