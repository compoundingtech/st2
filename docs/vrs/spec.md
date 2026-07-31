# st2 specification

This document specifies st2's current implementation contract. It builds on
[requirements.md](./requirements.md).

## Status

Active. This is a concise map to the implementation and its evidence, not a
replacement for the README, CLI help, KDL examples, or tests.

## Scope

st2 validates a declared agent fleet, materializes agent workspaces, launches
host-local work, adopts and supervises independently surviving tasks, and
delivers messages. The agent grammar and harness-facing contract remain
canonical in
[`compoundingtech/evals/AGENT-SPEC.md`](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md).

## Canonical Agent Spec eval teams

An eval may opt into `canonical-agents` after its fixture copy and deterministic
run steps have populated the hermetic temporary catalog. The directive is
mutually exclusive with compact `team` / `agent` declarations. st2 recursively
discovers the catalog, preserves explicit `identity` and `host` authority
independent of organizational placement, and projects only declarations
resolved to the eval host. Each declaration parent remains its native
state/resource anchor. The resulting local Agent Spec vector flows unchanged
through launch admission, kickoff resolution, supervision, logging, and
declaration-driven teardown; remote-host declarations remain inert.

Admission applies `validate_for_host` strictly, then fails before spawn when
discovery is malformed or warning-bearing, when the selected local projection
is empty, duplicate, retired, root-overriding, or unrunnable, or when any
resolved local task runtime ID is empty or duplicates another local task.
Materialization warnings and backend launch errors are fatal. The kickoff
target must resolve to exactly one local agent. The eval owns one native
`CATALOG` / `ST_ROOT` and its `<catalog>/pty` registry; declarations cannot
override those roots. Local workspace renders are materialized before any
agent task starts.

Native inbox/archive paths are derived once from the admitted Agent Spec paths
and carried as frozen data; routing never re-discovers the mutable catalog.
The requester alone is an explicit eval-owned flat mailbox. Multi-agent
completion retains the worker-report-before-supervisor-confirmation ordering.
For a singleton, the eval snapshots the requester inbox before kickoff and
completes only for a newly appearing interviewer reply whose timestamp is
at-or-after the exact kickoff receipt. The filename snapshot rejects
future-dated pre-seeded messages while `>=` accepts a causally new same-ms
reply. Canonical completion is a gating judge, so a timeout cannot pass on
unrelated final-state checks alone.

Without `canonical-agents`, fixture declarations are not discovered or launched
and compact evals retain their catalog-less flat bus. This explicit opt-in keeps
ordinary fixtures inert while allowing the same canonical declaration to be
exercised in an eval and real work. Parser and admission evidence lives in
`eval_spec::tests::canonical_agents_is_bare_once_and_excludes_compact_agents` and
the `canonical_*` unit tests. Named-PTY end-to-end cases prove strict
pre-spawn refusals, poisoned ambient-root isolation, real render
materialization, frozen routing after declaration removal, singleton
completion, custom task-ID supervision/logging/teardown, and the no-opt-in
legacy control in `tests/eval_run_e2e.rs`.

## Resource bindings (R20-R21)

An agent may directly declare zero or more generic Resource bindings:

```kdl
resource "work" _tag="github-issue" uri="github-issue://example/project/123"
```

The positional name is an agent-local semantic role, `_tag` is the concrete
type discriminator, and `uri` is the exact RFC 3986 absolute resource identity,
preserved byte-for-byte without normalization.
Declaration order has no meaning and binding names are unique within one
agent. The public `agent-spec` read model preserves the bindings in name order
across canonical KDL and the supported TOML/JSON forms. `st2 agents --json`
projects the same descriptors for language-neutral inspection.

st2 validates only this portable envelope. It does not define downstream type
schemas, resolve targets, infer authority from URI possession, or attach
required/optional, access, readiness, or lifecycle semantics. Those concerns
remain outside the generic binding contract. A Resource binding is declaration
metadata and is absent from task launch targets; changing only Resource
bindings adopts an already-live task without stop, replacement, or relaunch.

The unresolved-resource runtime discussion remains tracked in
[st2#60](https://github.com/compoundingtech/st2/issues/60), read-oriented
renderer integration in [st2#61](https://github.com/compoundingtech/st2/issues/61),
and the portable Agent Spec envelope in
[evals#41](https://github.com/compoundingtech/evals/issues/41).

## Transactional catalog authoring

`st2 agent digest (--spec FILE | --bundle DIR)` captures a source through
retained no-follow file descriptors and returns its authoritative digest.
`st2 agent publish --catalog ROOT (--spec FILE | --bundle DIR)
--input-sha256 HEX (--expect-absent | --expect-sha256 HEX) --json` binds
publication to that exact capture. It accepts exactly one canonical KDL `agent` node with an
explicit, path-safe host and identity. st2 no longer exposes an intent compiler:
external renderers own the transformation from human intent to exact Agent Spec
bytes or a create-only publication bundle.

The persistent `<catalog>/.st2/catalog-authoring.lock` defines one cooperative
read/write transaction domain:

```text
publisher (EX)  : snapshot input -> CAS -> full-catalog admission -> atomic publish + fsync
reader (SH)     : discover -> materialize/observe -> plan -> execute
bulk apply (EX) : root CAS -> durable stage+marker -> converge -> verify+clear
state plane     : message | context | Resource | status                    (unlocked)
```

The lock file is a persistent real inode: replacing or removing it would split
the lock domain for a process that already has it open. Consequently, the first
coherent declaration reader may initialize exactly `.st2` and this lock even
when its requested operation later refuses. Refusal still performs no
declaration, workspace, or state mutation.

The publisher derives the destination from the captured declaration, replaces
only `agent.kdl` for a hash-authorized update, and preserves all sibling runtime
state. A bundle is create-only and is renamed from a hidden same-filesystem
stage; retry reports `unchanged` only when every projected bundle file already
matches. `--expect-absent` is idempotent for identical input.
`--input-sha256` rejects a caller/source swap and `--expect-sha256` rejects a
stale declaration writer. Full-catalog admission rejects any
structural validation error before publication. The typed result is
`published` or `unchanged`.

`st2 catalog snapshot --catalog ROOT --output DIR --json` holds SH while it
captures the canonical declaration projection: `catalog.kdl`, exact
`agents/<host>/<identity>/agent.kdl` files, static files inside those bounded
agent bundles, and every regular file in `_templates` whether or not a current
render references it. `_templates` is bounded to depth 8 below its root, 256
files, 1 MiB per file, and 32 MiB total; symlinks, hard links, special nodes,
and reserved control/state names are rejected. Runtime state, `.git`, `.st2`,
the native `pty` registry, and workspace content are excluded. A
catalog-contained Agent `workspace` or Task `cwd` is valid only when it names
that agent bundle's canonical real `.workspace`; the empty directory itself is
an exact declaration fact, while its descendants are never traversed. The
classification uses launch-equivalent variable expansion, resolves relative
values from the Agent Spec bundle, and lexically normalizes before comparing
against the logical catalog. A relative spelling is accepted only when it
normalizes to that bundle's canonical `.workspace`; unresolved variables and
every other effective relative path fail closed. The
scanner always excludes a canonical `.workspace` subtree, including an orphan
left after an agent move or removal. External workspaces remain valid and are
not part of the projection. The output is a create-only durable directory; an
identical retry is `unchanged`. Its domain-separated, path-sorted root SHA-256
covers normalized relative paths, file bytes, executable bits, and empty
workspace directory facts.

`st2 catalog project --catalog ROOT --snapshot DIR --expect-sha256 HEX
--output DIR --json` derives one create-only, atomically published migration
bundle from one retained snapshot capture. `ROOT` is read only and supplies
the canonical logical address needed to interpret absolute catalog-local
workspace facts; the receipt binds that address and the source root digest.
The snapshot argument must be its canonical absolute path, and the resolved
create-only output must be beneath neither the snapshot nor `ROOT`, including
through a symlinked parent.
The bundle is closed and restart-safe:

```text
DIR/
  service/           exact retained declaration projection
  adopt-only/        exact declarations; every PTY lifecycle is adopt-only
  provider-witness/  all Agent identities and Resources; active PTYs only,
                     all exec/Ding and retired desired-absent PTYs suppressed
  receipt.json       source/child roots, typed predecessors, identity sets,
                     active/retired partition
  bundle.sha256      domain hash of receipt bytes and the three child roots
```

Projection parses and transforms canonical KDL nodes; it never edits KDL as
text or reconstructs direct argv through a shell. `service` and `adopt-only`
retain every Agent identity, Resource, provider task ID, exec task ID, and
retired provider declaration. `provider-witness` retains every Agent identity
and Resource, while its provider task IDs equal exactly the source's active
provider partition. The receipt separately records all Agent IDs,
provider-bearing Agent IDs, every provider task ID, active provider task IDs,
retired desired-absent provider task IDs, exec task IDs, and the equality
proofs between projections. A pre-existing output is an error rather than an
implicit recovery mode.
Immediately before receipt construction, all three materialized private-stage
children are independently reprojected and their actual roots and identity
sets become the attested values.

Projection admission may carry one exact pre-existing error class,
`render-owner-conflict`, as sorted path/agent/message evidence and sets
`admission.applyAdmissible` to false. Every other structural, parsing, path,
identity, or task error rejects projection. This exception creates evidence;
it grants no apply or reconcile authority. Ordinary `catalog apply` retains
full admission and rejects the bundle until those conflicts are resolved.
Regular files directly beneath `agents/` or `agents/<host>/` are outside the
canonical host/identity address grammar and excluded; symlinks and special
nodes at those positions still fail closed.

`st2 catalog apply` has exactly three closed modes: ordinary
`--prepared DIR --expect-sha256 HEX`, typed projected
`--projection-bundle DIR --projection-child service|adopt-only
--expect-bundle-sha256 HEX --expect-sha256 HEX`, or `--resume`. The mandatory
bundle digest is the caller-held capability returned by `catalog project`; the
bundle's own digest file is never sufficient authority. Projected apply first
requires the exact five-entry outer shape, then captures the complete public
bundle once through retained no-follow descriptors with explicit depth,
filesystem-entry-count, per-file, and total-byte bounds. It reparses the private typed
receipt, verifies the exact receipt bytes and outer bundle digest, requires
`expect-sha256` to equal the selected child's receipt-bound predecessor and the
receipt's canonical `logicalCatalog` to equal `ROOT`, and reprojects all three
materialized private children against their receipt roots, entry counts,
identity sets, and projection admission. Only then does the selected child
enter the same ordinary full-admission/CAS transaction without reopening the
public bundle. `provider-witness` is
deliberately not apply authority. A child root digest alone cannot authorize
reuse at another catalog address.

The receipt's closed transition graph is:

```text
service/source root --apply adopt-only--> adopt-only root
adopt-only root     --apply service-----> service/source root
provider-witness                         no apply predecessor
```

Each edge names exactly one predecessor digest; arbitrary child transitions
are rejected before ordinary apply. A completed-edge retry remains idempotent
because ordinary apply returns `unchanged` when the current root already equals
the selected target before evaluating the predecessor CAS. Once a durable apply
marker exists, generic `catalog apply --resume` uses only its captured
content-addressed stage and predecessor record; replaying the bundle is rejected
until recovery completes.

`st2 catalog apply --catalog ROOT --prepared DIR --expect-sha256 HEX --json`
rejects any prepared state/control path, symlink, special node, unprojected
file/directory, malformed declaration, nonempty prepared workspace fact,
catalog-local/default PTY root, or effective PTY-root change. Hash-CAS captures
and validates exact prepared bytes, takes EX, rechecks the canonical live root,
and either reports `unchanged` for exact equality or creates a durable
content-addressed stage before publishing the marker. Version 1 requires an
explicit PTY root outside the canonical catalog. Fresh bootstrap is a separate
cross-producer transaction because catalog EX cannot reserve a PTY registry
against external producers. Hash-CAS permits declared live workspace facts and
their real ancestry to contain content. It changes
declaration leaves only; desired workspace facts must already exist, and
workspace content and canonical state are never traversed, deleted, or hashed.
When an identity path is absent, its complete bundle uses an exclusive
directory rename. When its declared workspace skeleton already exists, the
durable marker fences declaration readers and marker-time state routing until
every declaration leaf has been published and verified. Applied leaves and
their parents are fsynced, the live root is re-hashed and fully admitted, then
the marker is unlinked and `.st2` is fsynced.
The catalog parent is fsynced when `.st2` is first created, including the
concurrent create/observe race. Retained source capture rejects a staging
destination contained by its source before enumerating that source.

The lock file is never removed: replacing its inode would split the transaction
domain for processes that already hold it open. Reconciliation holds SH from
discovery through execution. Validation, doctor, roster, listing,
materialization-only, targeted reconciliation, and catalog teardown take a
coherent SH snapshot. State-plane commands deliberately do not: their atomic
files remain live while a declaration is admitted.

`<catalog>/.st2/catalog-apply-incomplete` is the durable whole-catalog
transaction fence. Any presence is authoritative, including malformed content.
The reserved canonical record is:

```json
{"schema":"st2.catalog-apply-incomplete.v1","stageName":"catalog-apply-stage-<prepared-root-sha256>","expectedRootSha256":"<previous-root-sha256>","preparedRootSha256":"<prepared-root-sha256>","originalPaths":["<sorted-owned-declaration-leaf>", "..."]}
```

After taking its authoring lock, st2 refuses publication, validation,
materialization, teardown, roster, doctor, and catalog listing while the marker
exists. One-shot and selected reconcile fail explicitly. A resident supervisor
instead remains alive, reports a skipped/incomplete pass, and performs no
runtime observation or lifecycle action, avoiding a service restart storm.
Message, context, Resource, and status operations remain available. While the
marker exists they resolve canonical state independently of live declaration
discovery; an existing state-only orphan remains addressable, an incomplete new
identity does not fall back to a flat bus. A dotted bare identity is tried as
the complete local identity alongside every possible qualified bus-address
split; exactly one distinct canonical address must exist. Only real state
directories and a real regular status file can establish marker-time
addressability. Only `catalog apply --resume --catalog ROOT --json` may open an
existing marker. The closed marker and internal content-addressed stage are
sufficient recovery authority; the original prepared path and CAS precondition are
neither required nor consulted. Marker authority proves the original
precondition already passed, so recovery converges the partial live tree from
the durable desired stage and original owned-leaf list without re-enforcing
that stale precondition. Malformed or mismatched records remain fenced.
External lock execution and bypass flags are not part of the contract.

`st2 tasks --desired-state running|absent --json` optionally scopes the typed
runtime receipt to one closed desired-state set. Omission retains the original
all-task behavior and serializes `selection.desiredStates` as
`["running","absent"]`; a selected receipt serializes its one selected value.
Catalog parse errors and duplicate runtime IDs are derived globally before
selection. Selection then filters desired rows before backend observation, so
the observer receives only selected runtime IDs and completeness/runtime
errors are scoped to that selected set. A retired desired-absent task therefore
cannot make a `running` receipt incomplete, while an indeterminate active task
still does. Selection never turns a global declaration error into success.

## Host-local scheduling and supervision

```text
hand-authored KDL
       │
       ▼
validate ──► materialize ──► host-local st2 scheduler/reconciler
                                      │
                             ┌────────┴────────┐
                             ▼                 ▼
                        PTY / exec       DING sidecar
                             │                 │
                             └──── state + bus ┘
                                      ▲
                                      │
                            one intelligent root agent
                         observes · recovers · escalates
```

- **R01–R03:** Fleet validation separates structural errors from selected-host
  runtime facts. Materialization is inspectable and host reconciliation starts
  only declarations pinned to the local host. Discovery is recursive: an
  explicit `identity` and `host` pair is authoritative independent of the
  declaration's path, whose parent remains the state/resource anchor. When
  either field is omitted, the path supplies defaults and mismatches remain
  diagnostic. Dot-prefixed folders, including `.managed` and `.retired`, are
  ordinary declaration space; only `.git` and `.st2` directories at any depth,
  the catalog root's `pty` child, and a declaration parent's `resources`,
  `archive`, and `inbox` children are excluded.
  A resolved workspace-relative render destination has one coherent desired
  state across the active local fleet: byte-equivalent idempotent claims may
  share it, while incompatible
  claims fail every conflicting owner before the first workspace write.
  Targeted reconciliation checks the selected owner against the full fleet, so
  selection cannot bypass this ownership boundary.
- **R04:** Each machine schedules and reconciles only its pinned work. The st2
  loop is deterministic; exactly one declared root agent provides intelligent
  host-local supervision, bounded recovery, and escalation. Filesystem reads
  never wake reconciliation; only create, modify, rename, or remove events may
  wake it before the bounded timer.
- **R06:** st2 passes the complete effective task definition to the underlying
  launcher so manual and supervised restarts are equivalent. Harness readiness
  that depends on a dynamically selected account belongs to that declared
  command. In particular, reconciliation never mutates an ambient Codex config
  before launch: the command may select an account-specific `CODEX_HOME` only
  after st2 starts it. `st2 pretrust` remains an explicit operator utility for
  commands that intentionally use the ambient Claude and Codex configs.
- **R07:** Hook bundles are explicit, content-addressed, installed separately,
  and verified before materialization references them. Their receipts use the
  same resolved build identity as the binary's version surfaces for both
  hermetic package builds and source builds. Installation automatically accepts
  an ordered upgrade; `--replace` is the explicit exact-state authority for a
  downgrade, an unorderable build, or an unreadable receipt. Shipped hooks
  resolve Bash through `PATH`; the Nix package executes their integration gate
  with Bash and `jq` declared. Runtime materialization verifies the invoking
  binary's own content-addressed set, independent of which installed binary the
  receipt currently selects, so old and new supervisors can overlap during
  cutover. `hooks verify-own` exposes that read-only capability to package
  activation tooling.
- **R11:** `st2 up` is a replaceable control plane, not the lifetime owner of
  its agents. Normal exit, forced termination, binary replacement, and restart
  leave every running agent PID and creation identity unchanged. The new
  control plane adopts those processes and starts only missing work; it does
  not duplicate them. Agent stop or retirement requires a separate explicit
  lifecycle action.

  Executable acceptance starts an agent, terminates `st2 up` normally and with
  a forced kill, verifies the agent remains alive and usable, replaces the st2
  binary, starts the control plane again, and proves adoption with the same
  agent PID/creation identity and no duplicate process.

- **Adopt-only migration fence:** A compact agent or explicit task may declare
  `lifecycle "adopt-only"`. Reconciliation adopts an already-live generation,
  but classifies a dead or absent generation as `held` without garbage
  collection or launch. Returning the declaration to the default `service`
  lifecycle is the explicit authority to resume ordinary replacement.
  `retired #true` remains the separate explicit teardown path.

- **R23:** `st2 tasks --json` is the read-only adoption boundary. It emits one
  `st2.task-inventory.v1` envelope while holding the catalog shared lock across
  declaration discovery and runtime observation. Rows are sorted by agent,
  task, and runtime id and cover both PTY and terminal-free exec tasks.
  `complete=false` plus a non-zero exit is a closed result: a consumer must not
  turn a missing row into absence. A running row always carries the backend's
  PID, creation time, and an opaque generation id derived from stable backend
  evidence. A missing runtime root is positively empty and remains absent on
  disk; malformed state, PID reuse, timeouts, duplicate ids, and observer
  failures are indeterminate.

  A zero-interruption control-plane cutover is two catalog transactions, not an
  optimistic normal-service restart:

  1. apply a prospective catalog in which every active task is `adopt-only`;
  2. record the complete baseline inventory behind that no-launch fence;
  3. stop only the old supervisor and start the replacement;
  4. require a complete inventory whose desired active rows are all running at
     exactly the recorded generations and whose retired rows are absent;
  5. compare-and-swap the ordinary `service` lifecycle catalog only after that
     proof.

  An absent task remains `held` throughout the uncertain interval, so the new
  supervisor cannot manufacture the evidence the gate is trying to measure.
  Failure leaves the fence in place; it never authorizes teardown or launch.

  Historical exec PID files are observed read-only. Same-tick or otherwise
  ambiguous legacy evidence remains indeterminate: it is never silently
  promoted. No safe per-task rollover command exists yet for that evidence, so
  the cutover remains blocked before step 1 until the sidecar exits naturally or a separate,
  explicitly authorized rollover seam is implemented and proved. The record
  must not be rewritten as a workaround, and PTY-backed agent sessions must
  not be restarted to satisfy this gate.

- **Session registry:** A catalog owns the `pty` registry holding its tasks.
  `<catalog>/pty` is the default; a catalog may declare another so that one host
  can share a single registry across catalogs. Resolution is an exported
  `PTY_ROOT`, then the catalog's declaration, then the default, applied
  uniformly to spawn, list, kill, and the bus environment st2 hands to native
  tools, so every reader that can resolve the catalog agrees about where its
  sessions are. A declaration whose field set does not match fails `st2
  validate` rather than resolving silently back to the default. Runtime
  observation has a short outer deadline so a wedged client fails the pass
  closed instead of hanging reconciliation. The deadline is containment, not
  the mechanism for admitting a larger fleet.

## Message lifecycle

```text
atomic inbox file → DING attempt → agent reads → archive receipt
       └──────── archive with same filename wins ────────┘
```

- **R05:** A matching archive filename makes an inbox copy handled; stale
  duplicates are removed without another DING. Fresh `dnd` suppresses delivery;
  `busy` does not. Failed delivery remains retryable. Sidecar restart emits a
  bounded recovery notice instead of replaying the inbox. Delivery may wake an
  agent while it is working, but an active or uncertain human composer must be
  left untouched. Unsafe delivery retries use a bounded backoff so an active
  composer cannot create a short-lived PTY probe on every inbox poll. Inbox
  reads do not wake the sidecar; only mutations bypass its bounded poll cadence.

## State and scope

- **R08:** Presence and activity status are separate signals. The catalog must
  also expose the agent's current plan and step with explicit freshness so a
  human or supervising agent can understand progress without PTY inspection.
  Current presence/status files provide only part of this contract; the
  canonical plan-progress shape is not yet specified.
- **R09:** Durable work state is external to the model transcript and is
  restored into replacement sessions through declared workspace files and
  verified hooks.
- **R10:** Fleet identities are agents. General-purpose identity kinds are
  unsupported.

The owner updates this spec whenever implementation changes.
Changing [vision.md](./vision.md) or [requirements.md](./requirements.md)
requires Nathan's explicit approval.

## Event contracts (R13–R15)

An event is evidence, not permission to run the world. The reconciler retains
path and kind, maps them to the affected identity or template dependency set,
and computes the smallest desired-versus-actual delta. One declaration affects
only that agent; a template affects only its dependents. A no-op performs zero
PTY queries, launches, teardowns, materialization, or writes.

Watchers are deny-by-default. The classifier/action contract is:

| Event | Minimal action |
| --- | --- |
| declaration-space `**/agent.kdl` create/modify/remove | validate, materialize, and converge that agent and derived tasks |
| referenced `_templates/**` mutation | converge dependent agents only |
| inbox create/archive/remove | DING consumer only; supervisor no-op |
| plan/resource/status mutation | specialized consumer only; supervisor no-op |
| PTY/exec/log/PID/socket/lock/temp/backup/read/open/unknown | no-op |

Startup, timer, watcher overflow/loss, and ambiguity are bounded full-audit
fallbacks. Accepted streams use head/tail coalescing: immediate head response,
one quiet tail after a burst, and a hard maximum. Executable proof covers
positive declaration/template wakes, negative runtime/bus events, bounded
discovery/materialization/PTY queries and writes, continuous-event starvation,
and no-op desired-equals-actual behavior.

## Targeted reconciliation (R19)

`st2 up --materialize-only --task <host.agent.task>` resolves one exact local
task before writing and renders only its owning agent. `st2 up --once --task
<host.agent.task>` performs the same owner-only materialization, then inspects
PTY/exec state and executes a plan containing only that task. Unknown,
ambiguous, and wrong-host selectors refuse before writes or runner inspection;
unrelated discovery diagnostics remain visible without preventing the selected
owner/task path.

`st2 up --materialize-only --agent <id>` remains the agent-wide rendering
selector. Targeted task reconciliation is intentionally bounded to `--once`;
the resident supervisor continues to reconcile the complete local catalog.

## Exact exec retirement (R24)

The general closed operator entry point is prepare/apply:

```text
st2 --catalog <catalog> exec retirement prepare \
  --host <host> \
  --id <runtime-id> \
  --expect-catalog-sha256 <lowercase-hex> \
  --output <create-only-plan> \
  --json

st2 --catalog <catalog> exec retirement apply \
  --plan <plan> \
  --expect-plan-sha256 <lowercase-hex> \
  --json
```

`--catalog` identifies the logical declaration plane. The expected catalog
digest is caller-held authority produced from a coherent canonical snapshot;
the create-only plan externalizes the exact runtime authority derived by st2.
Apply accepts only the caller-held plan digest. Repeating the same apply resumes
or returns the byte-identical completed receipt; there is no separate resume
verb.

The temporary legacy migration replaces `--id` with `--legacy-set`. It
enumerates the complete host-local exec state namespace under the host lock and
emits one exact-set digest plus one capability for every numeric record.
Preparation fails without an artifact if any record is ambiguous, foreign, or
otherwise unplanned. Live records carry exact process and dedicated-scope
authority; positively stale/reused records carry exact record authority and a
proof that signaling is unnecessary. Apply checks that the namespace is still
exactly the original set minus durably completed entries. This is the only
legacy-classification authority; consumers do not list files, parse PIDs, or
derive generation ids.

The operation writes a durable per-request journal under the host-local exec
state directory and returns `st2.exec-retirement.v1`. Repeating the same request
resumes or returns the completed receipt. A different request for the same
runtime id conflicts while a journal is incomplete. The state transition is:

```text
prepared -> frozen -> killed -> record-retired -> completed
```

At every arrow, recovery reopens and revalidates capabilities rather than
trusting path names. The generation record moves to a create-only private slot;
it is never unlinked. Logs are diagnostics, not generation authority, and are
not part of exact retirement.

Only a Linux live generation with a dedicated cgroup-v2 systemd scope is
retirable. The legacy-set seam additionally permits record-only retirement
after a positive stale/reused proof and performs no signal or cgroup write for
that entry. Other platforms and degraded isolation remain observable but
return a typed unsupported error. PTY tasks continue to use the PTY backend and
never enter this transaction.

## Open design questions

- **DQ1 Scheduled work:** The vision includes per-machine schedulers that form a
  distributed workflow engine, but the KDL shape, event inbox, deduplication
  boundary, and execution receipts are not yet specified. A successful
  executable eval and Nathan's approval should resolve this before adding
  scheduler requirements.
- **DQ2 Safe DING delivery:** Bounded observation now replaces the fixed
  paste-to-Return delay: maintained Codex and Claude composers must be
  positively empty before paste and show the exact staged notice twice before
  a separate Return. Human, modal, active, changed, timed-out, and unknown
  states fail closed, with staged-payload ownership preventing duplicate paste.
  This measured screen heuristic is still not an evented proof and renderer
  changes may defer delivery. Resolve the remaining gap with a stronger evented
  signal or other measured classifier; a small on-device model is an optional
  experiment, not a required architecture.
- **DQ3 Catalog agent state:** Define the catalog paths, schemas, freshness
  rules, and atomic update semantics for presence, activity status, current
  plan, and current plan step. Prove that stale state is distinguishable and
  that a supervisor can follow plan progress without inspecting a PTY before
  adding the shape to `AGENT-SPEC.md`.
