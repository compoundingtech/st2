# st3 engineering design

Status: current design.

The letters `ST` in `st3` mean Small Talk.

st3 is a resident, event-driven reconciler for one claims graph. It stores immutable claims, reduces them to current state, and makes bounded runtime changes. The CLI is a client of the daemon API.

This document defines the system design. [mission-graph-runtime.md](./mission-graph-runtime.md) defines the complete mission language and planning workflow.

[schema.md](./schema.md) lists the generated public vocabulary. [data-authority.md](./data-authority.md) classifies durable authority and projections.

## Product outcome

st3 gives a person or an agent one durable graph for these objects:

- mission definitions, mission-owned runtimes, messages, and observed resources;
- immutable documents and mission revisions;
- mission runs, immutable run generations, revision proposals, products, gates, and reviews;
- Small Talk delivery and work ownership;
- runtime observations and operation evidence;
- peer replication and historical reads.

An intent changes only the subjects that it names. Omission never means deletion or stop.

A runtime `stop` can occur only inside its owning mission. Root control cancels a complete mission run.

A published mission is a definition. Publication does not start a run.

A mission run has one immutable initial revision and one current generation. Each generation binds to one immutable mission revision.

The daemon does not watch a catalog folder. `st3 import`, `st3 eval`, and other explicit client commands read local files and post their bytes to the API.

## Design decisions

### One graph

st3 uses one graph for desired state, observations, work, and evidence.

Every graph item has a stable subject such as `agent/abc/builder`, `mission/release`, `mission-run/abc`, `run-generation/def`, or `step-run/def/test`.

A mission run is the sole execution owner. Its runtimes use subjects such as `agent/RUN/LOCAL_ID` and `exec/RUN/LOCAL_ID`.

Missions and agents do not support in-place reparenting. A replacement mission run creates a new ownership boundary.

### Immutable claims

Every accepted fact is an immutable claim. A reducer derives the selected desired and actual state from claims.

Each claim has these main fields:

- a content-derived ID;
- a subject and registered kind;
- an accepting origin;
- an optional actor;
- a body and evidence references;
- causal predecessors;
- a local store index and acceptance time.

Wall-clock time is diagnostic. It does not select a winner between concurrent graph writes.

### SQLite store

Each host has one SQLite store. The store uses transactions, WAL mode, foreign keys, and full synchronous writes.

SQLite provides these required properties:

- atomic claim batches;
- per-subject compare-and-swap;
- crash recovery;
- fixed-snapshot reads;
- indexed status and history queries;
- durable mission runs, run generations, revision proposals, and planning sessions.

Documents and other byte content use content hashes. The database stores the binding from a document name to each immutable version.

### One authority for each fact

The st3 database stores each authoritative fact once.

A projection can repeat a fact only when claims or blobs can rebuild it exactly. A projection never accepts an independent write.

Each schema field must have one declared class: authority, reference, derived projection, local cursor, or short-lived capability.

The data review must prove these conditions:

- Every repeated value names its authority and derivation.
- A clean database can rebuild every durable projection from claims and blobs.
- The rebuilt public snapshot equals the original public snapshot.
- Public mutations append an authority record before they change a projection.
- `doctor` detects a projection that differs from its authority.

[data-authority-review.md](./data-authority-review.md) records the current risk areas and the required proof.

### Per-subject compare-and-swap

`POST /v1/intent/mission` returns the current leaf claim IDs for each named subject and mission definition.

`POST /v1/intent/apply` must return those exact tokens. A changed token causes a `stale-subject` conflict. Independent subjects do not conflict.

A mission preview uses the same rule. Planning approval also names the exact preview hash. A new candidate or graph head makes an older approval stale.

A revision approval names the exact proposal preview hash. A proposal also names its source generation.

### Event-driven reconciliation

A state-bearing claim wakes the reconciler. Audit-only claims do not wake it by themselves.

The daemon also performs one recovery pass when it starts. Runtime exit observation, file observation, operation completion, and declared one-shot deadlines create new state-bearing claims.

The reconciler does not scan a catalog and does not use a periodic discovery sweep.

`GET /v1/events` returns immediately when a matching event exists. Otherwise, it waits for a change or for a bounded 30-second server timeout. Controllers can use this endpoint as a token-free event wait.

### One vocabulary for gates

`gate` means a condition that can pass or fail. It covers graph predicates, mechanical commands, bounded LLM evaluation, and human review.

Missions and steps use repeated `gate` nodes. Sibling gates form an AND relation.

The old `judges` block, `judge` node, `judgement` CLI, and judgement API are not part of st3.

The public running-gate surfaces are:

- CLI: `st3 gate-result`;
- API: `POST /v1/gate-results`;
- claims: `gate.requested` and `gate.result`;
- environment: `ST_GATE` and operation capability fields.

### Mission contracts are flat

A mission or step can declare goals, baselines, products, and gates directly. st3 has no `outcome` wrapper.

`baseline` describes state that must already be true before work starts. `produces` describes graph state that the work promises to create. `gate` evaluates acceptance after the promised work and products are present.

Mission-level declarations apply to the full run. Step-level declarations apply to one step attempt.

### Explicit dependencies

KDL source order is display order only. A step is a root only when it has no `depends-on` declaration.

Every non-root ordering edge is explicit. st3 rejects references to missing steps and dependency cycles.

### Non-owning agent grouping

An agent can repeat `under` metadata:

```kdl
agent "release.test" {
  under "release.lead" reason="the lead combines the release result"
  under "quality" reason="the quality group owns the test standard"
  workspace "/work/release-test"
  command "run-tests"
}
```

`under` is visible to agents and roster clients. It does not grant authority, require reporting, delay work, or control lifecycle.

A missing target, self-reference, or cycle is a preview warning. It is not a publication blocker. This loose relation lets a TUI render partial or temporarily inconsistent team trees without coupling agent availability.

### Automatic run context

st3 supplies `ST_*` context to step members and running gates. The same values support KDL interpolation where that execution context exists.

Exact built-in names are reserved. An authored `env` block cannot replace them. Other `ST_*` names are allowed.

The complete table is in [mission-graph-runtime.md](./mission-graph-runtime.md#automatic-context).

### One generated boot contract

An agent harness prompt is optional.

st3 appends one exact instruction that tells the agent to read `.st3/boot.md` and claim current work.

The shared render transaction writes the canonical boot file before any native harness starts.

The boot file contains universal graph, work, message, wait, and diagnostic guidance. Mission goals do not belong in it.

### Mission-specific constraints

A mission or step can repeat `constraint`. st3 presents the effective inherited list with assigned work.

Constraints record real mission rules. They do not repeat universal boot behavior or disable harness features to help an eval.

### Host context documents

A host declaration can repeat exact `doc/NAME@SHA256` references.

The referenced documents contain stable host facts. They do not contain current work or raw private measurements.

Publication fails until the local store contains every exact document version.

### External resource observation

An external resource watch contains a resource, one observer, and zero or more delivery subscriptions.

The observer normalizes provider facts without using an agent turn. A subscription selects fields and a message target.

`local.file` observes metadata for one absolute local path. It never publishes file content.

`st3 resource refresh` requests an immediate observation. It waits for that exact observer attempt through the event API.

An unchanged refresh succeeds without a new `resource.observed` claim.

The first observation establishes a baseline. A later selected change creates one observation claim and one idempotent message.

Providers can use webhooks, streams, or conditional requests with one-shot deadlines. st3 does not use a periodic discovery sweep.

[resource-subscriptions.md](./resource-subscriptions.md) defines the provider contract, graph shape, lifecycle, and acceptance proof.

### Accounts

An account is a root graph identity for an external provider account.

```kdl
account "claude/team-a" {
  provider "anthropic"
  external-account "team-a"
  auth-type "subscription"
}
```

The declaration creates `account/claude/team-a`. The supported authentication types are `subscription` and `api-key`.

An account is not mission-owned. A mission or step cannot declare one.

An agent records its selected account with an `agent.account` state transition. The agent must write its own association.

```sh
st3 claim agent/RUN/worker agent.account \
  --actor agent/RUN/worker \
  --field account=account/claude/team-a
```

The account reference must have valid `account/` syntax. The referenced account can arrive later.

This contract records identity and association only. It does not define quota, usage, rotation, alerts, adapters, or automatic behavior.

## System boundary

st3 has these components:

1. The CLI reads explicit user input and sends API requests.
2. The local API listens on a user-owned Unix socket.
3. The claims store appends immutable batches and serves snapshot queries.
4. Reducers derive desired state, actual state, gaps, work, and warnings.
5. The reconciler requests bounded runtime changes.
6. Native drivers run Codex, Claude, and other supported harnesses.
7. Process and PTY adapters observe runtime state.
8. Small Talk maps durable message claims to native harness delivery or an explicit DING child.
9. Gate runners execute bounded mechanical or LLM checks.
10. The peer adapter exchanges causal claim batches between trusted nodes.

The CLI contains no independent reducer or reconciler. A CLI connection does not start a second daemon.

st3 and st2 do not share a live control loop. They can share an existing PTY registry during a measured cutover, but st3 remains the only writer for its claims store.

## KDL intent boundary

[kdl-lifecycle.md](./kdl-lifecycle.md) is the operational guide for publish, start, revise, approve, refresh, cancel, and print-only workflows.

Every st3 intent starts with `version 2`. Declarations follow the version directly.

The root can contain accounts, mission definitions, durable resources, people, documents, messages, and mission-run cancellation.

An `agent`, `exec`, `pty`, `observer`, `subscription`, or `schedule` that performs execution must occur in a mission or step.

A runtime `stop` must occur in the same owning mission. It cannot target a runtime from another mission run.

The parser is strict. Unknown fields, duplicate single fields, invalid identifiers, and invalid child types are errors.

A mission occurs at the root. A nested mission occurs inside one step.

The mission runtime is the only execution model. st3 has no checkpoint, supervisor, or link node.

### Workspaces and member environment

A member workspace must exist by default. `workspace "/path" create=#true` lets st3 create that exact directory.

st3 supplies `ST3_SUBJECT` with the current runtime subject. It supplies `ST_AGENT` only when an agent owns the runtime.

A nested agent task receives its owning agent subject in `ST_AGENT`. An agentless runtime has no `ST_AGENT` value.

`${PATH}` expands from the service path recorded at installation.

The installer includes the st3 directory, common user directories, and common system directories.

It also resolves each supported harness program and includes only those program directories.

It does not copy unrelated entries from the installer's shell path.

### Explicit DING delivery

A harness without native message delivery can declare one DING child:

```kdl
agent "worker" {
  workspace "/work/worker"
  command "worker-harness"
  exec "ding" {
    argv "st3" "driver" "ding"
    restart "on-failure"
  }
}
```

The mission run owns the DING child. The child checks the local st3 API once per second and sends one incarnation-fenced terminal line.

The DING child then records `message.delivered`. It does not read or write an st2 mailbox.

A provider or runtime fault creates `harness.diagnostic`. The roster and mission views show the current fault.

st3 does not require a special supervisor, root, or chief-of-staff agent. The runtime handles supervision and graph recovery.

## Identity and authority

The subject is the durable identity. A display name, file path, owner field, or runtime PID is not an identity.

Each runtime start has a new incarnation ID. A restart does not change the member subject.

The origin names the host that accepted a claim. The actor names the person, agent, or external identity that performed the action.

Per-subject writers and causal predecessor heads control graph updates. A claimed step binds work changes to one agent and runtime incarnation.

Mission revision authority comes only from graph placement in the current generation.

- An agent declared directly in a step can revise that step subtree.
- An agent declared directly in a mission can revise that mission.
- An agent adjacent to direct missions can revise those missions.

A work selector does not grant revision authority. The candidate revision cannot grant authority to its own author.

`revisions="human-only"` adds a human approval boundary. `revision-reviewer` selects a reviewer, or the run requester reviews by default.

`under` is visible grouping metadata. It does not participate in authority or runtime ownership.

## Runtime reconciliation

One reconcile pass performs these operations in order:

1. Read one fixed store snapshot.
2. Select current desired heads and mission definitions.
3. Reduce actual state and registered observations.
4. Record gaps and warnings.
5. Advance active mission runs.
6. Preflight all selected render writes as one transaction.
7. Commit all render writes, or roll back all writes after one failure.
8. Request required start, stop, review, or gate operations.
9. Record operation results as new claims.

No runtime starts before the complete render transaction succeeds. Each successful render records exact paths, hashes, and modes.

Every external action is idempotent or fenced by an incarnation, capability, expected subject head, or stable operation key.

Stopping a member requests TERM first. A shutdown deadline can cause a fenced hard kill of the same incarnation. A replacement incarnation is not killed by an older stop result.

Nonterminal runtime exits follow the declared restart type and intensity. The store records each request, result, observation, and parking decision.

`st3 runtime ls` shows all agent, exec, and PTY runtime subjects. `st3 runtime reset SUBJECT --reason TEXT` clears one restart window.

The reset claim binds to the current desired token and current incarnation. An older reset cannot affect a replacement desire or incarnation.

`st3 service install`, `status`, `restart`, and `uninstall` use a systemd user service on Linux and a launchd agent on macOS.

Installation uses a deterministic path and waits for the configured socket. A failed install restores the prior unit or property list.

`st3 service reset` needs three interactive confirmations. It stops the service and owned runtimes, erases st3 state, and starts an empty service.

Service reset retains the binary, service definition, configuration, workspaces, and rendered files.

## Mission execution

A ready mission starts only through a published `mission-run` declaration. `st3 mission start` generates and publishes that declaration.

A mission run records its initial revision, current generation, root revision, root run, workspace, requester, status, and phase.

A run generation records one mission revision, its predecessor, its actor, its reason, and its generation-specific step runs.

A mission or step selects work with `assigned-to`, repeated `available-to`, or bare `agentless`.

The nearest selector wins. A local selector replaces its inherited selector.

The first eligible pool claim wins one step atomically. One agent can claim multiple ready steps.

An absent selector means agentless work. A missing eligible agent creates a warning instead of a publication blocker.

Normal execution has these boundaries:

1. Mission baselines must hold before root steps can start.
2. Step dependencies must hold.
3. Step baselines must hold before each attempt becomes ready.
4. At least one eligible desired agent exists when the step needs an agent.
5. The step becomes ready with a new readiness epoch.
6. The step declarations converge.
7. A claimant reports completion when the step needs an agent.
8. Declared step products must exist.
9. All step gates must pass.
10. The explicit completion frontier holds.
11. Mission products must exist.
12. All mission gates must pass.
13. Steps in the adjacent `finally` block run after success, failure, or cancellation.

A false baseline blocks. It does not fail the mission or spend an attempt. A failed running gate fails its step or mission. A pending predicate gate keeps the current boundary pending.

`completion { when "all-steps-exhausted" }` is the common completion shortcut. A completion block can use explicit dependencies instead.

A mission without a completion block remains open. It becomes `standing` when no step can move and no failure blocks movement.

All missions use this state machine. st3 has no separate standing mission type.

The quick Codex and Claude commands publish deterministic zero-step missions. Their runs become standing and continue to assert their agents.

Graph cancellation revokes active claims and enters `finally`. The run then enters cleanup.

Cancellation cascades to descendant runs. A run becomes terminal only after all owned runtimes stop.

## Run revisions and generations

A mission revision does not mutate an active generation. st3 creates one successor generation in an atomic transaction.

The transaction marks the old generation as superseded. It creates new step-run subjects and moves the mission run pointer to the successor.

Mission and step members carry their owner run and generation. After cutover, the reconciler stops members that remain only in the predecessor lineage.

A compatible runtime keeps its stable run-local subject across generations. st3 does not transfer that runtime to another mission run.

A restart cutover cancels active descendant mission runs that started from predecessor steps. An idle cutover waits for active descendant work before it makes the same cancellation.

Exact compatible step definitions carry their state. A changed step and every transitive dependent restart without prior completion.

Compatible active work restarts in ready state. Late work updates against the superseded generation fail with `stale-run-generation`.

The default cutover is `restart-active`. `revision-cutover="when-idle"` stops new claims and waits for active work to settle. The current generation selects this rule. A candidate revision cannot select its own cutover.

The event-driven reconciler performs an idle cutover. It does not use a polling worker or a wall-clock selection rule.

A mission run accepts one pending revision proposal. Each proposal binds the source generation, candidate revision, compatible step set, reviewers, and preview hash.

All distinct affected reviewers must approve the exact preview. Cancellation returns a draining run to its normal phase.

## Planning mode

Planning mode is a durable review workflow. It is not a mission run.

`st3 planning start` creates one planning session, stores the request as an immutable document, starts one Codex planner, and sends the request through Small Talk.

The planner can submit multiple named variants. Each variant contains one Markdown document and one complete ready KDL mission.

`st3 planning preview` validates one named variant, renders a dependency graph and diff, and records one preview hash.

A planning session can target a current mission run. The session stores the exact source generation and rejects proposal after that generation changes.

The requester can revise, approve, or cancel:

- Revision stores feedback as an immutable document, sends it through Small Talk, and invalidates the old preview.
- Approval requires the current preview hash and current subject tokens. Its approval claim links the Markdown and KDL documents. It never starts a run.
- Cancellation publishes no mission.

The requester can compare named variants. The requester can then propose one previewed variant as a revision of the target run.

Approval and cancellation stop the planner. Planning lifecycle changes emit registered `planning-session.*` events.

## Local API

All JSON responses use the `st3.v1` envelope. The envelope includes a request ID, snapshot host, and store index.

Main endpoint groups are:

- intent: `/v1/intent/mission`, `/v1/intent/apply`;
- planning: `/v1/planning-sessions` and its session actions;
- missions and work: `/v1/mission-runs`, `/v1/run-generations`, `/v1/revision-proposals`, `/v1/work`, and `/v1/gate-results`;
- graph data: `/v1/claims`, `/v1/claims/by-id/{id}`, `/v1/status`, `/v1/events`, and `/v1/resource-watches`;
- runtime repair: `/v1/runtimes/reset/{subject}` and `/v1/resources/refresh/{resource}`;
- schema: `/v1/schema`;
- documents: `/v1/documents` and `/v1/documents/content`;
- Small Talk: `/v1/messages` and message lifecycle actions;
- sessions: `/v1/sessions/...` for logs, screens, input, signals, and attach;
- evaluation: `/v1/evals`;
- replication: `/v1/peer/...`.

The Unix socket mode is `0600`. A configured TCP peer listener must bind to an IPv4 or IPv6 loopback address.

Cross-host replication uses a trusted local port exposer such as Fabric. st3 does not accept a direct non-loopback peer listener.

## Claim vocabulary

The `st3-schema` crate defines accepted subjects, resource kinds, claims, fields, write policies, cardinality, projections, and reconciliation effects.

The daemon exports the same registry through `/v1/schema`. The CLI exposes it through `st3 schema`.

Important mission and gate kinds include:

- `agent.account`;
- `mission.published` and `mission.produced`;
- `mission-run.created` and `mission-run.state`;
- `run-generation.created`, `run-generation.superseded`, and `run-generation.state`;
- `revision-proposal.created`, `revision-proposal.approved`, `revision-proposal.cancelled`, and `revision-proposal.applied`;
- `step-run.carried`, `step-run.state`, and `step-run.retried`;
- `gate.requested` and `gate.result`;
- `planning-session.started`, `planning-session.candidate-submitted`, and `planning-session.previewed`;
- `planning-session.revision-requested`, `planning-session.approved`, and `planning-session.cancelled`;
- `observer.observed`, `observer.state`, `resource.observed`, and `subscription.state`.
- `runtime.restart-window-reset`, `render.applied`, and `file.observed`;
- `daemon.started` and `daemon.diagnostic`.

The registry pins the exact subject, resource, and claim manifests. A registry change must update the generated schema document.

A subject-reference field validates its subject syntax. It does not require the referenced subject to exist.

Every local writer uses the same field and cardinality checks. A replicated concurrent claim stays in history so reducers can report its conflict.

The `authorized-participant` policy requires a dedicated operation to verify participation. The generic claim endpoint cannot grant that authority.

An unknown replicated claim remains in history and makes its subject indeterminate. A node does not silently interpret an unknown kind.

## Peer replication

Each host accepts local writes and sequences local replica batches. Peers exchange missing causal batches and referenced blobs.

Different subject changes merge. Concurrent changes to one subject create multiple visible leaves. Every node uses the same deterministic selected revision while it reports all conflicts.

A later authorized write cites all current leaves and resolves the conflict.

A network partition does not stop local work. Each host continues from the last claims it accepted.

## Security properties

- The local API uses a user-owned Unix socket.
- Apply, revision, review, work, terminal, and gate operations use explicit authority or one-use capabilities.
- Revision authority uses the current graph placement and cannot come from a candidate revision.
- Human revision approval binds one source generation and one preview hash.
- Mission work claims bind to one agent incarnation.
- Terminal input and signals cite the expected incarnation.
- Mechanical and LLM gates have bounded execution time. LLM gates also require a positive token budget and an explicit tool set.
- Immutable document references use exact SHA-256 hashes.
- A mission approval binds the exact candidate, preview, and subject heads.
- Runtime stops do not target an unverified replacement process.
- Git repositories and shared st3 documents do not store credentials or raw private measurements.
- Durable evidence uses summaries, redacted samples, hashes, or restricted external storage.

The configured peer transport is for a trusted network. Authentication, encryption, and peer authorization remain outside the first protocol.

## Failure handling

st3 fails closed on malformed intent, missing documents, stale generations, stale preview hashes, invalid capabilities, unknown revisions, and invalid work claims.

Runtime failure stays visible as claims. It is not removed from history after retry or recovery.

A missing `under` target is different. It is presentation metadata, so it produces a warning and does not fail unrelated work.

## Delivery sequence

The rollout order is:

1. Make st3 a behavioral drop-in for the selected production path.
2. Prove the exact build on the first fleet.
3. Prove the same build on the second fleet.
4. Remove the old st2 path only after both fleets are green.
5. Rename the product surface last.

This sequence preserves one working control plane during migration. It does not add legacy syntax to st3.

## Non-goals

- No filesystem catalog watcher.
- No implicit stop by omission.
- No implicit step order.
- No ownership or permission effect from `under`.
- No revision authority from a work selector.
- No implicit mission completion after step exhaustion.
- No mutable mission revision inside a run generation.
- No automatic mission run after planning approval.
- No alias for removed st3 mission syntax.
- No unrestricted gate runner.
- No conflict resolution by wall-clock time.
