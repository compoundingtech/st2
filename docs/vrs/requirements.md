# st2 requirements

## Context

st2 implements the executable agent contract in
[`compoundingtech/evals/AGENT-SPEC.md`](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md).
The vision is defined in [vision.md](./vision.md). These requirements are the
current ratified product floor, not the entire future scheduler roadmap; new
scheduling and workflow constraints belong here only after their behavior is
accepted.

## Assumptions

- **A01 Declared fleet:** Each runnable agent has one current authored
  declaration and one pinned host.
- **A02 Trusted private fleet:** The catalog, runtime state, and participating
  hosts belong to one trusted operator; st2 is not an adversarial multi-tenant
  boundary.
- **A03 Durable host state:** Each host preserves its catalog and runtime state
  across process restarts. Whole-disk loss and backup are outside st2.
- **A04 Eventual transport:** Hosts may disconnect; Fabric eventually resumes
  transport when connectivity returns. st2 does not guarantee network
  availability.

## Acceptable Tradeoffs

- **T01 Explicit limits:** A documented unsupported case is preferable to a
  hidden distributed guarantee.

## Requirements

### Must implement the agent contract

- **R01 Agent-spec compliance:** st2 validates and implements every agent-spec
  capability it claims to support, and identifies unsupported capabilities.
- **R02 Canonical KDL and declaration identity:** Hand-authored KDL is the
  canonical declaration; any generator is optional and its output is
  inspectable before reconciliation. Declarations are discovered recursively.
  An explicit `identity` and `host` pair is authoritative independent of its
  folder path, while either omitted field retains path-derived defaults and
  mismatch diagnostics. The declaration's parent remains its state/resource
  anchor. Dot-prefixed and other organizational folders have no implicit
  lifecycle meaning; discovery excludes `.git` and `.st2` control directories
  at any depth, the catalog-root `pty` runtime directory, and state namespaces
  directly owned by a declaration.
- **R03 Host-pinned placement:** Every runnable agent or task resolves to its
  declared host; host-local roots own reconciliation.

### Must provide intelligent host supervision

- **R04 Root supervision:** Every machine has exactly one root agent. The
  deterministic st2 reconciler keeps declared local processes converged; the
  root observes host-local runtime health, diagnoses failures, performs bounded
  recovery, and escalates what it cannot resolve.

### Must preserve delivery and launch behavior

- **R05 DING/archive semantics:** Inbox delivery, archive precedence, retries,
  suppression, and restart recovery are deterministic and tested. DING may
  interrupt agent work, but it must not alter or submit a human's active draft;
  an unknown interaction state defers delivery.
- **R06 Restartable launch definitions:** A restarted PTY or exec receives the
  complete effective launch definition, including environment and supported
  launch fields.
- **R07 Verified hooks:** Required hook content is installed explicitly and
  verified before a rendered agent depends on it. The selected receipt carries
  the binary's real source identity regardless of build system. Ordered
  upgrades are automatic; selecting an older, unorderable, or unreadable exact
  hook set requires explicit replacement authority. A selection change does not
  invalidate another running binary's previously installed immutable set. Hook
  interpreters and runtime dependencies are portable and explicit on every
  supported package environment.
- **R11 Control-plane replacement safety:** Stopping or killing `st2 up` must
  not stop, restart, or replace any agent it launched. st2 can be reinstalled
  and restarted while running agents continue unchanged; the replacement
  control plane adopts those existing processes by stable identity and starts
  only genuinely missing work. Stopping an agent is a separate, explicit
  lifecycle action.

### Must externalize agent state and scope

- **R08 Catalog observability:** Catalog-backed state exposes each agent's
  presence, declared activity status, current plan, and current plan step
  without PTY or transcript inspection. Presence and activity status are
  distinct, and stale state is identifiable.
- **R09 State continuity:** An agent's current work and durable decisions
  can survive process replacement without depending on its transcript.
- **R10 Agent-only identity:** st2 models agents. Non-agent identities are
  unsupported.

- **R13 Shortest-path reconciliation:** An event is evidence, not permission
  to run the world. st2 classifies source, path, kind, and affected identity,
  then takes the shortest correct path from observed state to desired state.
- **R14 Explicit filesystem-event contracts:** Every watcher is deny-by-default
  with exact roots, paths, mutation kinds, semantic meaning, debounce policy,
  and consumer. Reads, opens, unknown paths, and runtime output never trigger
  generic reconciliation.
- **R15 Bounded event coalescing:** Accepted event streams use tested
  head/tail coalescing: immediate head response, one quiet tail, and a hard
  maximum preventing starvation or unbounded scans, PTY queries, launches,
  delivery attempts, or writes.

- **R16 Supervisor declaration:** Every non-root agent declares exactly one
  supervisor; root is the only agent without a supervisor.
- **R17 Durable error propagation:** Lifecycle, harness/eval, provider-turn,
  task/exec/PTY, hook, and delivery errors are durably reported to the
  responsible supervisor with agent/task identity and actionable context.
- **R19 Targeted reconciliation:** An exact agent/task selector resolves its
  identity and pinned host before mutation; unknown, ambiguous, and wrong-host
  targets refuse before writes, listing, or actions. Materialization, hook
  gates, PTY inspection, and plan execution are limited to the selected
  owner/task; unrelated diagnostics remain visible while unrelated workspaces,
  tasks, and live PTY PID/generation stay unchanged.
- **R20 Portable Resource bindings:** An agent may directly carry zero or more
  order-independent Resource bindings. Each binding has a non-empty, agent-local
  unique name and preserves a non-empty, opaque type discriminator and an RFC
  3986 absolute URI byte-for-byte without normalization. The generic envelope
  does not imply resolution, access, readiness, or lifecycle semantics;
  declarations that add such unsupported policy are rejected rather than
  silently ignored.
- **R21 Nondisruptive Resource observation:** Machine-readable catalog
  inspection exposes every Resource binding without interpreting its type or URI.
  Resource-only declaration changes do not alter a task's effective launch
  definition and do not stop, replace, or relaunch healthy work.
- **R22 Transactional catalog authoring:** One st2 publication operation admits
  exactly one canonical KDL Agent Spec, with explicit host and identity, against
  the complete prospective catalog. Publication is compare-and-swap, durable,
  and atomic: readers observe either the previous declaration set or the next
  complete set. Reconciliation holds one coherent declaration snapshot through
  materialization, runtime observation, planning, and execution, so a retirement
  cannot commit and then be followed by a launch from stale catalog input.
  A durable incomplete-apply marker fences every declaration-plane snapshot and
  action after a crashed whole-catalog apply; a resident supervisor stays alive
  but performs zero lifecycle actions until the transaction is completed.
  Presence, messages, context, and Resource state remain independently writable
  and are never serialized behind catalog authoring.
  A caller binds single-agent publication to the exact no-follow source capture
  with an authoritative input digest. A canonical whole-catalog snapshot
  externalizes the declaration-root digest while excluding runtime state and
  workspace content. Its closed projection includes every regular file in a
  bounded `_templates` library and exact declared canonical workspace directory
  facts. Whole-catalog apply accepts only that projection, rechecks the root
  digest under the exclusive lock, durably stages the desired bytes, and resumes
  after interruption solely from a closed marker and its content-addressed
  stage. Version 1 requires one explicit external PTY root and rejects effective
  PTY-root changes. Fresh-catalog bootstrap is a separate cross-producer
  transaction, not a catalog-apply mode. Apply never traverses, hashes, deletes,
  or relocates workspace or runtime state. An absent canonical identity becomes
  visible only as a complete bundle; a preexisting declared workspace skeleton
  remains safe because the durable marker fences declaration readers and
  marker-time state routing throughout leaf publication and verification.
- **R23 Typed adoption inventory:** One read-only machine command must join a
  coherent declaration snapshot to one complete runtime observation and expose
  every desired local PTY and exec task by agent identity, task name, runtime
  id, kind, lifecycle, retirement, desired state, runtime state, PID, creation
  time, and opaque runtime-generation id. Unknown, duplicate, malformed,
  unreadable, timed-out, or PID-reused evidence is indeterminate and makes the
  versioned envelope incomplete and the command unsuccessful; it is never
  reported as absence. Observation holds the shared catalog boundary, does not
  create a missing runtime root, and performs no reconciliation or cleanup.
  A staged control-plane replacement must apply `adopt-only` to every active
  task, record a complete baseline inventory behind that no-launch fence, stop
  only the old supervisor and start the replacement, prove every desired task
  running at its baseline generation through a second complete inventory, and
  only then separately compare-and-swap the ordinary `service` lifecycle.
- **R24 Exact exec retirement:** A caller may retire one exact `exec`
  generation only while holding both its opaque runtime-generation capability
  and the exact canonical declaration-root digest. On Linux, retirement is one
  durable, restartable transaction over a dedicated cgroup-v2 systemd scope:
  it pins the leader with a pidfd, opens the recorded cgroup without symlink or
  mount traversal, freezes it, revalidates the generation, record inode, scope,
  and complete membership, uses `cgroup.kill`, proves the cgroup empty, then
  moves the exact generation record with `renameat2(RENAME_NOREPLACE)` into a
  private retirement slot and verifies the moved inode and bytes. A raced
  replacement is restored without replacement; if restoration conflicts, both
  objects are preserved and the transaction reports conflict. Every durable
  phase is recoverable and the typed receipt binds the request digest,
  declaration root, record before/after evidence, process generation, cgroup
  authority, membership, freeze, kill, and journal.

  Missing pidfd, cgroup-v2, dedicated scope authority, writable controls,
  exact evidence, or supported generation schema fails closed. There is no
  numeric PID/process-group, path-unlink, PTY, whole-state-directory, or
  best-effort fallback. Historical exec records remain read-only and cannot be
  retired by this seam. Whole-state-directory rotation is a one-shot consumer
  operation, not general task removal.
