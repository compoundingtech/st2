# st3 engineering design

Status: current design.

The letters `ST` in `st3` mean Small Talk.

st3 is a resident, event-driven reconciler for one claims graph. It stores immutable claims, reduces them to current state, and makes bounded runtime changes. The CLI is a client of the daemon API.

This document defines the system design. [plan-graph-runtime.md](./plan-graph-runtime.md) defines the complete plan language and planning workflow.

## Product outcome

st3 gives a person or an agent one durable graph for these objects:

- desired agents, processes, scopes, messages, and observed resources;
- immutable documents and plan revisions;
- plan runs, immutable run generations, revision proposals, products, gates, and reviews;
- Small Talk delivery and work ownership;
- runtime observations and operation evidence;
- peer replication and historical reads.

An intent changes only the subjects that it names. Omission never means deletion or stop. An explicit `stop` declaration changes a subject or scope to stopped state.

A published plan is a definition. Publication does not start a run.

A plan run has one immutable initial revision and one current generation. Each generation binds to one immutable plan revision.

The daemon does not watch a catalog folder. `st3 import`, `st3 eval`, and other explicit client commands read local files and post their bytes to the API.

## Design decisions

### One graph

st3 uses one graph for desired state, observations, work, and evidence.

Every graph item has a stable subject such as `agent/node.builder`, `plan/release`, `plan-run/abc`, `run-generation/def`, or `step-run/def/test`.

A scope groups subjects for observation and teardown. It is not a second graph namespace.

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
- durable plan runs, run generations, revision proposals, and planning sessions.

Documents and other byte content use content hashes. The database stores the binding from a document name to each immutable version.

### Per-subject compare-and-swap

`POST /v1/intent/plan` returns the current leaf claim IDs for each named subject and plan definition.

`POST /v1/intent/apply` must return those exact tokens. A changed token causes a `stale-subject` conflict. Independent subjects do not conflict.

A plan preview uses the same rule. Planning approval also names the exact preview hash. A new candidate or graph head makes an older approval stale.

A revision approval names the exact proposal preview hash. A proposal also names its source generation.

### Event-driven reconciliation

A state-bearing claim wakes the reconciler. Audit-only claims do not wake it by themselves.

The daemon also performs one recovery pass when it starts. Runtime exit observation, file observation, operation completion, and declared one-shot deadlines create new state-bearing claims.

The reconciler does not scan a catalog and does not use a periodic discovery sweep.

`GET /v1/events` returns immediately when a matching event exists. Otherwise, it waits for a change or for a bounded 30-second server timeout. Controllers can use this endpoint as a token-free event wait.

### One vocabulary for gates

`gate` means a condition that can pass or fail. It covers graph predicates, mechanical commands, bounded LLM evaluation, and human review.

Plans, steps, and checkpoint stages use repeated `gate` nodes. Sibling gates form an AND relation.

The old `judges` block, `judge` node, `judgement` CLI, and judgement API are not part of st3.

A supervisor's bounded screen-input feature uses `terminal-control`. It is not a gate.

The public running-gate surfaces are:

- CLI: `st3 gate-result`;
- API: `POST /v1/gate-results`;
- claims: `gate.requested` and `gate.result`;
- environment: `ST_GATE` and operation capability fields.

### Plan contracts are flat

A plan or step can declare goals, baselines, products, and gates directly. st3 has no `outcome` wrapper.

`baseline` describes state that must already be true before work starts. `produces` describes graph state that the work promises to create. `gate` evaluates acceptance after the promised work and products are present.

Plan-level declarations apply to the full run. Step-level declarations apply to one step attempt.

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

The complete table is in [plan-graph-runtime.md](./plan-graph-runtime.md#automatic-context).

## System boundary

st3 has these components:

1. The CLI reads explicit user input and sends API requests.
2. The local API listens on a user-owned Unix socket.
3. The claims store appends immutable batches and serves snapshot queries.
4. Reducers derive desired state, actual state, gaps, work, and warnings.
5. The reconciler requests bounded runtime changes.
6. Native drivers supervise Codex, Claude, and other supported harnesses.
7. Process and PTY adapters observe runtime state.
8. Small Talk maps durable message claims to native harness delivery.
9. Gate runners execute bounded mechanical or LLM checks.
10. The peer adapter exchanges causal claim batches between trusted nodes.

The CLI contains no independent reducer or reconciler. A CLI connection does not start a second daemon.

st3 and st2 do not share a live control loop. They can share an existing PTY registry during a measured cutover, but st3 remains the only writer for its claims store.

## KDL intent boundary

Every st3 intent starts with `version 2` and contains exactly one untyped `subgraph` root.

The root can contain these subject nodes:

- controlled members: `agent`, `exec`, and `pty`;
- structural nodes: `scope`, `host`, `supervisor`, `link`, `message`, `schedule`, and `plan`;
- observed nodes: `resource`, `person`, and `account`;
- explicit `stop` state.

The parser is strict. Unknown fields, duplicate single fields, invalid identifiers, and invalid child types are errors.

A plan can be at the root or inside a plan-only scope. A scope cannot mix plan definitions with immediate desired members.

The graph also supports ordered checkpoint stages for direct desired-state convergence. Checkpoint stages now use repeated named `gate` nodes. The durable plan runtime is the work execution model and does not use a second plan-specific checkpoint language.

## Identity and authority

The subject is the durable identity. A display name, file path, scope tag, or runtime PID is not an identity.

Each runtime start has a new incarnation ID. A restart does not change the member subject.

The origin names the host that accepted a claim. The actor names the person, agent, or external identity that performed the action.

Per-subject writers and causal predecessor heads control graph updates. A plan step lease also binds work changes to one assignee and runtime incarnation.

Plan revision authority comes only from graph placement in the current generation.

- An agent in a step subgraph can revise that step subtree.
- An agent in a plan subgraph can revise that plan.
- An agent adjacent to direct plans can revise those plans.

`assigned-to` does not grant revision authority. The candidate revision cannot grant authority to its own author.

`revisions="human-only"` adds a human approval boundary. `revision-reviewer` selects a reviewer, or the run requester reviews by default.

`under` does not participate in authority. `supervisor` remains the lifecycle and policy relation for controlled members.

## Runtime reconciliation

One reconcile pass performs these operations in order:

1. Read one fixed store snapshot.
2. Select current desired heads and plan definitions.
3. Reduce actual state and registered observations.
4. Record gaps and warnings.
5. Advance active plan runs.
6. Request required start, stop, message, review, or gate operations.
7. Record operation results as new claims.

Every external action is idempotent or fenced by an incarnation, capability, expected subject head, or stable operation key.

Stopping a member requests TERM first. A shutdown deadline can cause a fenced hard kill of the same incarnation. A replacement incarnation is not killed by an older stop result.

Nonterminal runtime exits follow the declared restart type and intensity. The store records each request, result, observation, and parking decision.

## Plan execution

A ready plan starts only through `POST /v1/plan-runs` or `st3 run`.

A plan run records its initial revision, current generation, root revision, root run, workspace, requester, status, and phase.

A run generation records one plan revision, its predecessor, its actor, its reason, and its generation-specific step runs.

Normal execution has these boundaries:

1. Plan baselines must hold before root steps can start.
2. Step dependencies must hold.
3. Step baselines must hold before each attempt becomes ready.
4. The step subgraph converges.
5. An assigned worker reports completion when required.
6. Declared step products must exist.
7. All step gates must pass.
8. Every normal step must complete.
9. Plan products must exist.
10. All plan gates must pass.
11. Final steps run after success, failure, or cancellation.

A false baseline blocks. It does not fail the plan or spend an attempt. A failed running gate fails its step or plan. A pending predicate gate keeps the current boundary pending.

## Run revisions and generations

A plan revision does not mutate an active generation. st3 creates one successor generation in an atomic transaction.

The transaction marks the old generation as superseded. It creates new step-run subjects and moves the plan run pointer to the successor.

Plan and step members carry an internal generation scope. After cutover, the reconciler stops members that remain only in the predecessor lineage. A member reused by the successor moves to the successor scope.

A restart cutover cancels active descendant plan runs that started from predecessor steps. An idle cutover waits for active descendant work before it makes the same cancellation.

Exact compatible step definitions carry their state. A changed step and every transitive dependent restart without prior completion.

Compatible active work restarts in ready state. Late work updates against the superseded generation fail with `stale-run-generation`.

The default cutover is `restart-active`. `revision-cutover="when-idle"` stops new claims and waits for active work to settle. The current generation selects this rule. A candidate revision cannot select its own cutover.

The event-driven reconciler performs an idle cutover. It does not use a polling worker or a wall-clock selection rule.

A plan run accepts one pending revision proposal. Each proposal binds the source generation, candidate revision, compatible step set, reviewers, and preview hash.

All distinct affected reviewers must approve the exact preview. Cancellation returns a draining run to its normal phase.

## Planning mode

Planning mode is a durable review workflow. It is not a plan run.

`st3 plan start` creates one planning session, stores the request as an immutable document, starts one Codex planner, and sends the request through Small Talk.

The planner can submit multiple named variants. Each variant contains one Markdown document and one complete ready KDL plan.

`st3 plan preview` validates one named variant, renders a dependency graph and diff, and records one preview hash.

A planning session can target a current plan run. The session stores the exact source generation and rejects proposal after that generation changes.

The requester can revise, approve, or cancel:

- Revision stores feedback as an immutable document, sends it through Small Talk, and invalidates the old preview.
- Approval requires the current preview hash and current subject tokens. It publishes one ready plan and links the Markdown and KDL document references. It never starts a run.
- Cancellation publishes no plan.

The requester can compare named variants. The requester can then propose one previewed variant as a revision of the target run.

Approval and cancellation stop the planner. Planning lifecycle changes emit registered `planning-session.*` events.

## Local API

All JSON responses use the `st3.v1` envelope. The envelope includes a request ID, snapshot host, and store index.

Main endpoint groups are:

- intent: `/v1/intent/plan`, `/v1/intent/apply`;
- planning: `/v1/planning-sessions` and its session actions;
- plans and work: `/v1/plan-runs`, `/v1/run-generations`, `/v1/revision-proposals`, `/v1/work`, and `/v1/gate-results`;
- graph data: `/v1/claims`, `/v1/status`, `/v1/events`;
- documents: `/v1/documents` and `/v1/documents/content`;
- Small Talk: `/v1/messages` and message lifecycle actions;
- sessions: `/v1/sessions/...` for logs, screens, input, signals, and attach;
- evaluation: `/v1/evals`;
- replication: `/v1/peer/...`.

The Unix socket mode is `0600`. A configured TCP peer listener assumes a trusted private network. The first version has no TLS or peer ACL protocol.

## Claim vocabulary

The claim registry defines accepted kinds and whether they wake reconciliation.

Important plan and gate kinds include:

- `plan.published`, `plan.documents`, and `plan.produced`;
- `plan-run.created` and `plan-run.state`;
- `run-generation.created`, `run-generation.superseded`, and `run-generation.state`;
- `revision-proposal.created`, `revision-proposal.approved`, `revision-proposal.cancelled`, and `revision-proposal.applied`;
- `step-run.carried`, `step-run.state`, and `step-run.retry`;
- `gate.requested` and `gate.result`;
- `review.requested` and `review.decision`;
- `planning-session.started`, `planning-session.candidate-submitted`, `planning-session.previewed`, and `planning-session.variant-proposed`;
- `planning-session.revision-requested`, `planning-session.approved`, and `planning-session.cancelled`.

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
- Plan work leases bind to one agent incarnation.
- Terminal input and signals cite the expected incarnation.
- Mechanical and LLM gates have bounded execution time. LLM gates also require a positive token budget and an explicit tool set.
- Immutable document references use exact SHA-256 hashes.
- A plan approval binds the exact candidate, preview, and subject heads.
- Runtime stops do not target an unverified replacement process.

The configured peer transport is for a trusted network. Authentication, encryption, and peer authorization remain outside the first protocol.

## Failure handling

st3 fails closed on malformed intent, missing documents, stale generations, stale preview hashes, invalid capabilities, unknown revisions, and invalid work leases.

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
- No revision authority from `assigned-to`.
- No mutable plan revision inside a run generation.
- No automatic plan run after planning approval.
- No alias for removed st3 plan syntax.
- No unrestricted gate runner.
- No conflict resolution by wall-clock time.
