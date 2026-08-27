# st2 to st3 parity inventory

This document records the remaining st2 and st3 behavior differences.

It is not an implementation plan or a release promise.

The coverage notes describe the active st2 catalog observed on 2026-08-27.
They will become stale as that catalog changes.

The st3 state describes the implementation observed on the same date.

## Current setup coverage

The observed catalog had 11 active agent declarations.

Six declarations used the native Claude driver. Five declarations used the native Codex driver.
Every active declaration used a render block.

The active declarations contained 73 `copy` operations, 12 `json-upsert` operations, and 22
`git-exclude` operations.

No active declaration used `stream`, nested `resource`, `keep`, `deliver`, `ding`, `file`,
`ensure-line`, Pi, OpenCode, `name`, `description`, or `role`.

The catalog contained retired generic DING and app-server delivery declarations. These declarations
do not exercise delivery during normal operation.

The message archive contained 660 sent reply receipts. This gives good data for message and thread
compatibility tests.

One Linux user service was active. `st2 doctor --require-supervisor` passed on that host.

Strict catalog validation reported no issues. The task inventory refused a complete snapshot because
retired backup declarations shared runtime IDs.

That refusal is a useful migration case. A proof fixture must keep retired duplicates absent without
creating a live runtime conflict.

## Coverage terms

`Strong` means that normal active use can expose a regression.

`Weak` means that only retired declarations, indirect use, or uncommon operations cover the feature.

`Absent` means that the observed setup has no live example.

`Host-specific` means that one host cannot prove the complete behavior.

## Parity table

| Area | st2 behavior | st3 state | Current setup coverage | Future proof |
|---|---|---|---|---|
| Streams | Declared adapters, external ingress, deduplication, supersession, and receipts. | Missing. The syntax is accepted but inert. | Absent. | Use local adapter and external ingress fixtures. Prove duplicate and superseded event receipts. |
| Stream authoring | An agent can add or remove its declared streams. | Missing. | Absent. | Use concurrent catalog mutations and authority checks. |
| Nested resources | An agent declaration publishes resource bindings. | Missing. Dynamic `st3 resource` commands work. | Absent. | Declare active and inactive bindings. Compare the roster and resource reads. |
| Collection pins | `keep` prevents agent or task collection. | Missing. st3 validates the field but does not retain it. | Absent. | Let a kept task exit. Prove its dead record remains. Clear the pin and prove normal reconciliation. |
| Generic delivery | `ding` and `deliver` create delivery companions for command-based agents. | Partial. Native Claude and Codex delivery works. | Weak. Only retired declarations use these forms. | Run an opaque PTY agent through idle, busy, DND, restart, and backlog cases. |
| DING safety | Delivery uses FIFO order, DND deferral, restart recovery, retry ownership, and composer checks. | Partial. Generic st3 delivery sends once and records delivery. | Weak. Native delivery hides most generic DING behavior. | Use a controlled PTY with ambiguous sends and restarts. No paid model is necessary. |
| Render operations | Render supports `copy`, `file`, `json-upsert`, `ensure-line`, and `git-exclude`. | Partial. st3 implements all operations. | Strong for `copy`, `json-upsert`, and `git-exclude`. Absent for active `file` and `ensure-line`. | Use temporary workspaces for each operation and mode. |
| Render safety | Render checks ownership conflicts, tracked files, all writes before mutation, variables, and hook replacement. | Partial. st3 has atomic writes and workspace path checks. | Weak. The active setup covers successful writes, not most refusal paths. | Use two owners, a tracked destination, a late failure, variables, and managed hooks. |
| Host service | The CLI installs, checks, and removes a persistent supervisor service. | Linux parity. `st3 service` manages a systemd user unit with a resolved configuration, `PATH`, restart policy, and 1 GiB memory limit. | Strong for service status. Weak for install and removal. | Use a disposable user service and an isolated state directory. Prove install and removal without replacing the active service. |
| Driver setup | The CLI installs hooks and the Claude channel plugin. | Missing from the st3 CLI. The native driver can use existing host state. | Host-specific. The active catalog uses Claude, but the installation belongs to another host. | Use isolated home directories for plugin install, update, detection, fallback, and removal. |
| PTY operations | `pty`, `shell`, and environment helpers target the selected catalog. | Partial. `st3 pty` lists, attaches, peeks, sends, signals, and opens the local PTY interface. It does not duplicate all lower-level PTY commands. | Weak. Catalog declarations cannot show operator helper use. | Use pseudo-terminal CLI tests with an isolated registry. |
| Diagnostics | `doctor` and `tasks` compare desired state with runtime state. | Partial. `st3 doctor`, `inspect`, `trace`, `wait`, generation logs, and health metadata cover normal graph debugging. A full task inventory comparison remains missing. | Strong. The active setup uses both healthy and incomplete inventory cases. | Reproduce live, absent, duplicate, unknown, unreachable, stale generation, and log rotation cases. |
| Recovery | `unpark` restarts one repaired task without restarting its peers. | Missing. | Absent. No current task is parked. | Cause a controlled crash loop and unpark only that task. |
| Lifecycle commands | `down`, suspend, resume, retirement, and targeted desired-state changes control members. | Partial. Graph and scope stops replace part of this behavior. | Strong for retirement and eval teardown. Absent for suspension and resume. | Use a graph with running, suspended, retired, stopped, and sibling members. |
| Presentation | `rename`, `describe`, and `away` update human-facing state. | Missing. | Absent. Active declarations have no presentation fields or away state. | Seed each field and verify updates without identity changes. |
| Enriched roster | The roster includes presentation, resources, activity, inbox count, and desired state. | Partial. st3 enrichment currently adds reachability. | Strong for roster use. Weak for optional fields because most values are empty. | Seed every optional field and compare stable JSON output. |
| Request transport | Typed service requests and replies are durable and idempotent. | Missing. The st2 design already marks this feature for possible retirement. | Absent. No request state exists in the observed catalog. | Decide whether migration retains or retires it. If retained, use a local service principal fixture. |
| Message trees | `message thread --tree` renders reply relationships. | Partial. st3 accepts `--tree` but prints a flat list. | Strong. The archive has many replies. | Import a branched thread and compare text and JSON output. |
| Process containment | Linux PTY tasks use systemd user scopes when available. | Implemented for Linux and other Unix hosts. st2 and st3 share the same scope or detached-process selection. st3 records its isolation mode and adopts surviving exec members after a daemon restart. | Host-specific. Linux scope use is active, while other hosts use fallback behavior. | Keep the daemon-restart survival test. Add a macOS detached-process test and a Linux degraded-mode fixture. |
| Catalog utilities | Snapshot, diff, bootstrap, digest, and targeted publishing manage the filesystem catalog. | Partial. Graph history plus `plan`, `run`, and `import` replace the main workflow. | Strong for a large catalog. Weak for safe tests of destructive utilities. | Use a copied catalog with retired duplicate runtime IDs and shared documents. |

## Features that need purpose-built coverage

The active setup does not adequately cover these features:

- Nested agent resources.
- `keep` collection pins.
- Generic command-agent delivery and complete DING safety.
- Active `file` and `ensure-line` rendering.
- Render conflict and refusal paths.
- Service installation and removal against a disposable user unit.
- Claude channel installation on each supported host type.
- PTY operator helpers against an isolated registry.
- Park and targeted unpark behavior.
- Suspension and resume.
- Presentation fields and the `away` state.
- Typed service requests, if st3 retains them.
- Pi and OpenCode native drivers.
- macOS and degraded Linux process containment.

Most of these proofs can use local commands and synthetic PTYs. They do not need paid model runs.

Claude, Codex, Pi, and OpenCode readiness or native delivery proofs require their actual provider.
The current setup gives routine coverage only for Claude and Codex.

## Deliberate replacements

These differences are not parity gaps:

- st3 accepts only the new KDL format. `st3-migrate` performs the one-time translation.
- API publication replaces the watched filesystem catalog.
- `st3 plan` replaces most standalone catalog validation.
- Native driver setup replaces ambient workspace pre-trust.
- st3 has no Fabric-specific behavior.

## Migration proof boundary

A general migration claim needs two separate proofs.

First, st3 must import the current translated catalog and reconcile it without unnecessary work.

Second, synthetic fixtures must cover the features that the current catalog does not use.

The current fleet alone cannot prove that every st2 user can migrate. It can prove the common native
Claude, native Codex, render, message, context, retirement, service, and catalog paths.
