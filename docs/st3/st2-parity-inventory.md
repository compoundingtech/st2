# st2 to st3 parity inventory

Status: current product decision and implementation audit.

This inventory asks one question: can st3 run the current agent network safely?

It does not require st3 to preserve each st2 command or storage detail.

## Migration boundary

The migration moves live declarations and current work. It does not move st2 history.

The st2 catalog remains a searchable archive. st3 does not import old inbox files, archived messages, contexts, or runtime records.

st2 and st3 do not exchange messages. The migration moves one agent at a time while both systems can run.

The st3 claims database starts with only new st3 claims. A cutover does not replay old delivery.

`st3-migrate` is experimental. Every migration report says that a person or coding agent must review the generated KDL before publication.

## Language decisions

st3 keeps one strict language. It has no compatibility parser.

The current language removes these st2 concepts:

- `checkpoint`, because explicit mission steps and gates replace it;
- `supervisor`, because mission placement and `under` metadata replace it;
- `link`, because direct subject references and mission edges replace it;
- `role` and `meta`, because the current roster does not need them;
- `keep` and authored `lifecycle`, because mission-run ownership controls runtime life;
- implicit `deliver` and `ding`, because a DING sidecar is an explicit child exec;
- nested agent resources, because resources are first-class graph subjects;
- generic streams and events, because resources, messages, and claims cover current needs.

Unknown grammar is an error. Extensible subjects use `custom/`, extensible claims use `custom.`, and extensible resource kinds use `custom.`.

## Execution and ownership

| Need | st3 design | State |
| --- | --- | --- |
| Start an agent | Publish and run a mission that contains the agent | Implemented |
| Keep a chat agent available | Use an open mission with no completion frontier | Implemented |
| Stop an agent | Publish mission-run cancellation and finish cleanup | Implemented |
| Start nested tasks | Declare mission-owned `exec` or `pty` members | Implemented |
| Recover surviving runtimes | Observe exact PTY and exec incarnations after daemon start | Implemented |
| Recover a parked runtime | Use `st3 runtime reset SUBJECT --reason TEXT` | Implemented |
| Inspect runtimes | Use `st3 runtime ls` | Implemented |
| Prevent broad workspace creation | Require an existing workspace by default | Implemented |
| Create an owned workspace | Add `create=#true` to the workspace declaration | Implemented |
| Carry a stable identity | Use the mission-run-owned full agent subject | Implemented |
| Show grouping | Use repeated `under` metadata with an optional reason | Implemented |

Every runtime belongs to one mission run. An agent subject has the form `agent/RUN/LOCAL_ID`.

A mission run owns its runtime lifetime. A replacement mission run does not reparent an existing runtime.

## Environment

st3 injects the mission, run, generation, step, requester, and workspace context.

`ST3_SUBJECT` is the current runtime subject. `ST_AGENT` is the owning agent subject.

A nested task gets its parent agent in `ST_AGENT`. An agentless runtime has no `ST_AGENT`.

`${PATH}` expands from the daemon path. Native services use a deterministic path that includes common user and system binary directories.

This behavior supports shipped `git` and `gh` shims without baking the installer shell path into the service.

## Harnesses and delivery

| Need | st3 design | State |
| --- | --- | --- |
| Claude | Native typed driver and st3 channel | Implemented |
| Codex | Native typed driver and app-server delivery | Implemented |
| Pi | Native typed driver | Unit contract implemented |
| OpenCode | Native typed driver | Unit contract implemented |
| OMP | Native typed driver | Unit contract implemented |
| Generic terminal wake | Explicit `exec "ding"` child | Implemented |
| Claude channel setup | `st3 claude-channel install/status/uninstall` | Implemented |

Pi, OpenCode, and OMP need later provider-backed evals. Their model-free argument and graph contracts are in the unit suite now.

The explicit DING child checks the local st3 API once per second. It writes one incarnation-fenced terminal line and records `message.delivered`.

The DING child does not use an st2 filesystem bus. The mission run owns and cleans up the child.

## Messages and work

| st2 need | st3 surface | Decision |
| --- | --- | --- |
| Send and reply | Durable `message.*` claims and `st3 message` | Keep |
| Read and archive | Read and close lifecycle claims | Keep |
| Message thread | Reply references and tree output | Keep focused tree output |
| Sender receipt list | No separate command | Defer |
| Old archive import | No importer | Remove |
| st2 to st3 bridge | No bridge | Remove |
| Generic events | Registered claims and messages | Remove |
| Generic streams | Observed resources and messages | Remove |

The work queue is graph state. Notifications only tell an agent that the queue can contain new work.

The roster shows each full subject, display name, state, reachability, driver, owner run, incarnation, and `under` metadata.

The roster has no role selector. An agent can inspect or filter the stable full subjects.

## Resources and documents

| Need | st3 design | State |
| --- | --- | --- |
| Immutable text | `doc` with a content hash | Implemented |
| Git repository | `vcs.repository` | Registered |
| Git commit | `vcs.commit` | Registered |
| Pull request | `vcs.pull-request` | Registered |
| CI run | `ci.run` | Registered |
| Human review | `human.review` | Registered |
| Local file metadata | `filesystem.file` with `local.file` | Implemented |
| Harness session file | `harness.session-file` | Registered |
| Harness usage | `harness.usage` | Registered |
| Custom resource | `custom.*` kind | Implemented |

`local.file` returns status, path, hash, size, mode, and an optional reason. It never publishes file content.

`st3 resource refresh` requests one immediate observer attempt. It waits on events and returns after that exact attempt.

An unchanged refresh is successful. It adds an observer result but no new resource observation.

The migrator does not silently convert an st2 file resource. It prints the exact `st3 doc put` command for a required document import.

## Rendering

st3 supports copy, inline file content, deterministic executable modes, JSON upsert, exact line insertion, and Git exclusion.

One reconcile pass preflights every selected render write before it changes a file.

The preflight rejects escaping paths, conflicting owners, conflicting duplicate writes, and changes to tracked Git files.

Identical duplicate writes are one write. Unchanged content keeps its existing modification time.

All planned writes commit as one transaction. A later write failure restores every earlier destination.

No runtime starts before the complete render transaction succeeds. Successful writes create a `render.applied` receipt with hashes and modes.

## Replication and service management

Each st3 daemon has one local claims store and one user-owned Unix socket.

The optional peer listener accepts only IPv4 or IPv6 loopback addresses. A non-loopback bind is a configuration error.

Cross-host replication uses Fabric or another trusted local port exposer. st3 does not expose its peer API directly to the network.

The test suite runs two real loopback daemons with separate SQLite stores. It proves claim replication in both directions.

Linux uses a systemd user service. macOS uses a launchd agent with restart, log, and file-limit settings.

Service installation waits for the configured socket. A failed installation restores the previous native service definition.

`st3 service reset` is intentionally destructive. It needs three interactive confirmations before it erases st3 state.

The reset stops the service and owned runtimes. It retains the binary, service definition, configuration, workspaces, and rendered files.

## Migrator behavior

The catalog migrator performs these actions:

- it marks every report as experimental and review-required;
- it rewrites legacy `$PATH` references to `${PATH}`;
- it removes fields that st3 intentionally does not support;
- it converts legacy DING intent into an explicit nested DING exec;
- it validates all deferred mission and step runtime graphs;
- it reports each legacy file resource with an exact document import command;
- it does not import history, bus state, contexts, or live runtime records.

The migrator is an aid. A coding agent can perform a direct migration when the generated result needs substantial changes.

## Current acceptance boundary

The branch is ready for a fleet trial when these checks are green:

1. The complete Rust test suite passes.
2. Every model-free st3 eval passes with the exact release binary.
3. The explicit DING eval proves delivery without an st2 mailbox.
4. The local file eval proves exact refresh and content privacy.
5. An isolated Hetz daemon passes health, runtime, resource, render, and cleanup checks beside st2.
6. A macOS build passes the launchd unit contract before the Silber trial.

The live fleet cutover remains a separate operation. It must move one agent at a time and must not change st2 history.

## Deliberate later work

These items do not block the current agent network trial:

- provider-backed Pi, OpenCode, and OMP evals;
- transport authentication beyond a trusted local port exposer;
- a richer visual mission and agent tree;
- more local resource providers;
- provider-specific quota behavior for account subjects.
