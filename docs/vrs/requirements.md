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
- **R22 Quiet coordination after events:** A network with minimal or default
  personas stays quiet while useful work continues. Agents coordinate only after
  an inbox DING, a durable failure, a real blocker, a completion or decision
  handoff, or a declared schedule with a name. They continue until they resolve
  the need or hand it off. Repeated status messages and peer polling are not
  substitutes. Normal supervisors handle failures and blocked work. They do not
  continuously manage healthy work. A custom supervisor persona can require
  more frequent coordination. CoS is only an example. st2 does not define or
  require that role, and it gives the role no standard authority. Transport
  loss does not stop independent host-local work.

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
  Stable IDs alone select and authorize automation; presentation values never
  resolve a message, Resource, status, lifecycle, or authoring target.
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
- **R23 Fail-closed task inventory:** One read-only machine command exposes
  every desired local PTY and exec task by agent identity, task name, runtime
  id, kind, lifecycle, retirement, desired state, runtime state, PID, creation
  time, and opaque runtime-generation id. Unknown, duplicate, malformed,
  unreadable, timed-out, PID-reused, or otherwise unprovable evidence is
  indeterminate and makes the versioned envelope incomplete and the command
  unsuccessful; it is never reported as absence. Observation detects semantic
  declaration drift across its runtime probe, does not invoke a backend for a
  root positively absent at admission, and performs no reconciliation, cleanup,
  lifecycle change, or state rewrite. An admitted PTY root that changes
  filesystem identity during the backend probe makes the observation
  incomplete; the external backend may already have recreated a concurrently
  removed registry. This diagnostic boundary is not transactionally serialized
  with catalog or runtime writers and is not control-plane cutover authority.
- **R24 Stable identity and bounded presentation:** The positional Agent Spec
  identity and its host-qualified bus identity remain the sole stable keys for
  routing, ownership, adoption, lifecycle, and automation. Agent Specs may
  declare optional, non-empty `name` and `description` strings in canonical KDL
  and the readable TOML/JSON forms. `name` is a non-unique mutable human label,
  limited to 160 Unicode scalars; `description` is an enduring responsibility
  boundary, limited to 1,000. Both are single-line: Cc control characters and
  U+2028/U+2029 are invalid. Omission means absence. Presentation is never an
  alias, and the declaration is its sole source of truth; a sibling `name` file
  is ignored without migration or compatibility behavior.
- **R25 Constrained presentation authoring:** `st2 rename` and `st2 describe`
  set or clear only their corresponding direct field in one canonical KDL
  declaration selected by stable identity. They preserve unrelated source
  bytes, serialize cooperating local writers through the persistent private
  `.st2/presentation-authoring.lock`, reject a stale source before atomic
  replacement, fsync the result, and return classified receipts. The lock inode
  is never removed or stale-recovered and defines one local POSIX
  filesystem/kernel exclusion domain; it is not cross-host coordination or OS
  isolation from direct external writers. TOML, JSON, Nix-owned declarations,
  stable-ID changes, and malformed or ambiguous targets fail closed. A catalog
  agent may edit itself or a descendant reached through declared supervisor
  edges; an operator outside a managed agent context may use the same operation.
- **R26 Live PTY presentation projection:** For every healthy managed PTY, st2
  reconciles a versioned owned tag snapshot containing the stable actor identity
  plus optional description through one exact task-ID metadata patch. The
  primary `agent` task additionally maps optional name to native PTY display
  metadata; secondary PTYs preserve their task-specific display convention.
  Projection preserves unrelated tags, removes absent owned values, reports and
  retries failure, and is idempotent. It never uses display-name resolution or
  enters launch, teardown, garbage collection, replacement, or flapping
  accounting.
