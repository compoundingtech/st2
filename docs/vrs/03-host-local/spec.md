# Host-local supervision specification

This document maps the [host-local requirements](requirements.md) to current
code and tests. It does not define transport, remote attachment, deployment,
or Doctor health policy.

## Current proof

| Requirement | Mechanism and evidence |
| --- | --- |
| HOST-R01 | Reconciliation filters declarations by resolved host. It does not act on remote work. See [reconciliation](../../../src/reconcile.rs) and [host tests](../../../tests/reconcile.rs). |
| HOST-R02 | One pass reads declarations and task state. It then adopts or starts local work. A failed session read stops the pass. See [run](../../../src/run.rs). |
| HOST-R03 | `HostLock` records one live owner for the selected catalog and host. See [code and tests](../../../src/host_lock.rs). |
| HOST-R04 | Agent tasks survive normal exit, forced exit, and binary replacement. A new process adopts the same task generation. See [survival tests](../../../tests/nomad_survival.rs). |
| HOST-R05 | Normal exit leaves tasks alive. Retirement and teardown use separate commands. See [run](../../../src/run.rs) and [survival tests](../../../tests/nomad_survival.rs). |
| HOST-R06 | The loop reports bounded crash-loop failures. Root requirement R04 owns root-agent recovery. See [run](../../../src/run.rs) and [root requirements](../requirements.md). |
| HOST-R07 | Agent state and `HostLock` state are separate. Adoption uses current local state. A DING-only task does not make an agent live. st2 has no global catalog-liveness classifier. |
| HOST-R08 | Stable root paths are an accepted constraint. The relocation command is not implemented. See [issue #85](https://github.com/compoundingtech/st2/issues/85). |

## Liveness and path boundary

A live canonical agent keeps its catalog live while the resident st2 process is
down. Process loss can delay convergence. It cannot mark the agent dead, erase
the applied catalog, or authorize teardown. A DING sidecar is not a canonical
agent. An incomplete view cannot prove that the catalog is globally stopped.

The catalog root and PTY root remain at stable mounted paths while the catalog
is live or the resident process runs. File edits and sync can continue. `up`,
`doctor`, and reconcile use only the selected paths. Root relocation requires
the coordinated operation in [issue #85](https://github.com/compoundingtech/st2/issues/85).

## Partition and plain-folder boundary

Root requirements R18 and R28 keep the last complete, validated local catalog
authoritative during transport loss. Hosts can use different catalog versions.
Peer absence is neutral unless an explicit local dependency says otherwise.

A plain synced folder and direct KDL remain complete st2 inputs. They do not
require compare-and-swap, a content-addressed store, or an authoring service.
Current transaction commands are optional. They protect local publication, but
they do not identify ordered complete versions from a partial folder sync. They
also do not retain a durable last-known-good receipt for that case.

## Open gaps

- Define how a host identifies and orders complete synced-folder versions.
- Define a durable last-known-good receipt without requiring optional authoring.
- Define an explicit local dependency on a peer or source.
- Replace the check-then-write first-start step in
  [`HostLock`](../../../src/host_lock.rs) with one atomic ownership operation.
