# st3 engineering design

Status: this document records the first engineering design.

The letters `ST` in `st3` mean Small Talk.

The plan runtime superseded its authored checkpoint model on 2026-08-27.

See [plan-graph-runtime.md](./plan-graph-runtime.md) for the implemented plan grammar and commands.

The claims, reconciliation, transport, and safety sections remain useful design history.

## Outcome

st3 is a resident, event-driven reconciler for one graph. It has a claims store and a thin CLI.

A client publishes a subgraph through an API. The subgraph can contain one subject or many subjects.

The publish can also contain an ordered checkpoint list. The reconciler moves only toward the next checkpoint.

The first checkpoint describes the admitted initial graph. The last checkpoint is the final graph.

Plans, imported catalogs, and evals publish this same structure. Plan subjects stay desired after completion.

An eval tags its subjects with a temporary scope. Its last checkpoint publishes the scope as empty.

A publish changes only the subjects that it names. An absent subject means unchanged, never stopped.

st3 has no synced catalog folder. The daemon does not watch or discover files.

The CLI reads a folder only when a person gives it to `st3 import` or `st3 eval`.

## Decisions

### One graph accepts subgraph publishes

st3 has one graph. An intent publishes the complete desired state of each named subject.

The daemon derives a patch by comparing each published state with its selected current revision.

Publishing the same desired state again is a no-op. Document absence has no meaning.

An explicit `stop` publishes desired stopped state. It does not delete the subject.

Compare-and-swap applies to each named subject on one node. Independent subject changes do not conflict.

A scope is a subject with a desired member set. Membership also tags each member claim without creating another graph namespace.

### 1. Use SQLite for the claims store

Use SQLite as the authoritative local claims store.

SQLite gives st3 atomic append batches, compare-and-swap writes, crash recovery, fixed-snapshot queries, and useful indexes.

A content-addressed store does not give those operations by itself. It would require a transaction index and a query database beside it.

Each claim still gets a content hash. This hash provides identity, replication deduplication, and
integrity without making blobs the state model.

Use one SQLite database per host. Use WAL mode, foreign keys, and `synchronous=FULL`.

### 2. Ship mechanical and llm judges in the MVP

The MVP runs both judge types.

Every llm judge declares a token budget and a time limit. Missing limits make the declaration invalid.

An llm judge is a real headless agent. It can read the diff, run tests, and use its declared tools.

The token budget and time limit are its only automatic execution limits in version 1.

Sandboxes remain wanted for all agent types, including workers and judges. Version 1 defers that common design.

Mechanical judges remain the deterministic first choice when a command can decide the result.

### 3. Exchange claims through a configured peer port

st3 owns the claim replication protocol on a configured HTTP port.

The operator supplies the private network. The network can be a VPN, a VPC, Tailscale, or another trusted path.

st3 has no Fabric dependency or Fabric-specific behavior.

Version 1 does not authenticate peers, use TLS, or enforce ACLs. The daemon warns about this limit at startup.

Every authorized host accepts writes. Replication is bidirectional and master-master.

Hosts replicate replica batches after a partition. Different subject writes combine without a conflict.

Concurrent writes to one subject create visible leaf revisions. Every host selects the same deterministic winner.

Reads and reconciliation continue from that winner. The API also returns every losing revision.

A later changed desired state cites all current leaves and removes the conflict. Reconciliation never waits for that change.

A partition never blocks local intent writes. Each host also keeps working from the last intent it accepted.

### 4. Use one ordered `checkpoints` block

Use one `checkpoints` block with one repeated `checkpoint` node.

The first node is checkpoint zero and describes the initial graph. The last node is the final graph.

Position defines both roles. The format has no separate `initial`, `transition`, or `final` node type.

This shape matches the source intent and keeps an eval and a plan structurally equal.

## Required interpretations

### Reconcile-trigger claims

The definition says that a new claim starts a reconcile. It also requires one decision claim per agent on every pass.

Those statements create an infinite loop if every audit claim triggers another pass.

This design therefore distinguishes state-bearing claims from audit claims. Only a state-bearing claim starts a reconcile.

The claim kind registry sets this property. A client cannot mark its own claim as a trigger.

Decision claims, action request claims, and judge audit claims do not trigger by themselves. Their
result claims can trigger when they change state.

### No periodic timer

st3 has no sweep, poll, retry, or discovery timer.

An operation can still have a one-shot deadline. Shutdown, eval, and judge limits require such deadlines.

A deadline belongs to one recorded operation. It is not a way to discover changed state.

A declared schedule uses the same rule. The clock adapter emits a claim when a declared time arrives.

The reconciler does not poll the clock or wake itself to search for work.

### Explicit filesystem input

`st3 import ./catalog` reads the named catalog. `st3 eval ./eval` reads the named eval.

The CLI makes a deterministic input from that explicit path. It does not read paths named by `doc/` references or discover other files.

`st3 doc put FILE --as doc/NAME` is the separate, explicit operation that reads and posts document bytes.

The daemon has no synced catalog folder. It never watches, discovers, or interprets filesystem changes as intent.

## System boundary

st3 is a new binary in this repository. st2 and st3 do not share a live control loop.

st3 has these components:

1. The CLI converts command arguments into API requests and renders API responses.
2. The local API listens on a user-owned Unix socket.
3. The claims store appends immutable claim batches and serves fixed-snapshot queries.
4. The reducer derives desired state, actual state, gaps, checkpoints, and reachability from claims.
5. The dispatcher starts a reconcile after a state-bearing claim commits.
6. The reconciler selects the next checkpoint and requests bounded actions.
7. Runtime drivers operate PTYs, processes, messages, harness readers, and judges.
8. The peer adapter exchanges claims with configured peers and reports peer-drop claims.
9. The terminal endpoint records input before it proxies that input to an attached PTY.
10. The clock adapter turns declared schedule occurrences into claims.

The CLI contains no reducer, planner, reconciler, or judge logic.

The daemon does not read source repositories or catalog folders. A client sends KDL bytes or an explicit eval bundle.

The import and eval CLI commands read only the path that the person supplied.

The host service manager owns the API socket and activates one resident daemon. A CLI connection does not poll or start a second daemon.

## Identity and authority

st3 has one graph. Every item in that graph has a stable subject.

Lifecycle ownership sorts subjects into three kinds:

| Subject kind | Lifecycle owner | Subject types |
|---|---|---|
| Member | st3 | `agent`, `exec`, `pty` |
| Structure | Nobody; it exists because st3 exists | `scope`, `link`, `supervisor`, `checkpoints`, `message`, `schedule` |
| Observed | The world | `resource`, `host`, `person`, `account` |

Examples include `agent/build/worker`, `scope/eval/restart-42`, `person/nathan`, and `account/claude/team-a`.

A resource is anything in the world that st3 observes but does not control.

A pull request, CI run, repository, file, and harness session file are resource kinds.

A repository is not a member. An agent changes it, and a claim records the observed result.

A session file is not a subject type. It is a resource with kind `harness.session-file`.

A session file and PID have no one-to-one lifecycle. A context clear can replace the file inside one PID.

`codex resume` can continue one session file identity under a later PID and incarnation.

A person is an observed subject. A claim's origin records who observed an action, while its actor records who performed it.

A host is an observed subject for placement, health, and `transport.peer` claims.

An account records its provider, external account identifier, and `subscription` or `api-key` authentication type.

Quota state is an observation claim. An agent publishes which account it uses, so account rotation becomes visible state.

An `account.quota` claim uses `available`, `limited`, `exhausted`, or `unknown`. It can include remaining units and a UTC reset time.

An `agent.account` claim names one account subject. An `actor.action-observed` claim names the doer in its `actor` field.

A supervisor and link are structure subjects. Supervisor decisions cite the supervisor subject and its normalized policy hash.

A display name, file name, and scope tag are not identities.

Each runtime incarnation has a new `incarnation_id`. A restart does not change the member subject.

Each claim has a recorded origin and an optional actor. Version 1 does not authenticate that origin.

The Unix socket permissions restrict local access to its owner. A configured peer label records the replica host.

An agent can publish only its allowed subjects and claim kinds.

The origin names the recorder. The actor names the person, agent, or external identity that performed the observed action.

A separate replica ID names the node that accepted and sequenced the batch.

The same author can write through two nodes during a partition without forking one sequence chain.

The host trust policy grants graph-author authority to user principals. A subject's first intent claim records its allowed writers.

A later writer change must cite the current subject heads and come from an allowed writer.

The requester publishes checkpoint and judge definitions as their own subjects. A worker cannot replace or write its own judge.

Wall-clock time is diagnostic. It never resolves two competing claims.

Replica sequence, causal predecessors, per-subject heads, and declared authority determine order.

An authorized offline writer can extend the subject heads that it knows. A later concurrent head is a visible conflict.

The revision generation and stable revision key select one deterministic winner. Losing revisions remain readable.

## Local API

The local API uses HTTP with JSON over a Unix socket. Terminal attachment upgrades one request to a binary WebSocket.

Peer replication uses the same claim envelope over a configured plain HTTP port.

Every JSON response has this envelope:

```text
Response<T> {
    api_version: "st3.v1"
    request_id: UUID
    snapshot_host: HostId
    store_index: u64
    value: T
}

ApiError {
    api_version: "st3.v1"
    request_id: UUID
    snapshot_host: HostId
    store_index: u64
    code: String
    message: String
    details: Object
}
```

The API uses these common failures:

- `409 stale-subject` returns the subject, expected heads, and current heads.
- `409 conflicting-authority` names the allowed writers for the subject.
- `422 invalid-intent` returns stable KDL diagnostics.
- `422 unsupported-capability` rejects a declared feature that this daemon cannot run.

### Endpoint list

| Method and path | Accepts | Returns |
|---|---|---|
| `POST /v1/documents` | A document name, UTF-8 bytes, an expected document token, and an idempotency key | The blob hash, canonical reference, document token, and binding claim ID |
| `POST /v1/intent/plan` | Subgraph KDL with stored document references and an optional snapshot index | The hash-pinned intent, desired states, derived patch, subject tokens, predicted actions, and blockers |
| `POST /v1/intent/apply` | Hash-pinned KDL, subject tokens, and an idempotency key | New subject tokens, the batch, claim IDs, and reconcile subjects |
| `GET /v1/status` | Optional subject, scope, and local snapshot selectors | Selected desired state, losing revisions, actual state, gaps, reachability, and checkpoints |
| `GET /v1/claims` | A subject or scope, `after_index`, `before_index`, `asc` or `desc` order, and a bounded page size | Immutable claim envelopes and the next cursor |
| `POST /v1/claims` | A typed observation, optional actor, evidence claim IDs, and idempotency key | The durable claim ID and store index |
| `GET /v1/events` | An `after_index` cursor and optional subject or scope filter | A server event stream of committed claim batches |
| `GET /v1/doctor` | No body | Typed store, state, PTY, isolation, ownership, runtime drift, and driver checks |
| `POST /v1/messages` | Sender, recipient, content, close judges, subject tokens, and an idempotency key | The message claim, token, and delivery action ID |
| `POST /v1/messages/{message_id}/claims` | An agent lifecycle claim or a judge close verdict | The lifecycle claim and current message token |
| `POST /v1/judgements` | A running judge verdict, its operation capability, reason, and evidence claim IDs | The durable judge result claim and store index |
| `POST /v1/evals` | A deterministic eval bundle, expected subject tokens, and an idempotency key | The eval scope, subject tokens, reconcile subjects, and event cursor |
| `GET /v1/evals/{scope}` | An eval scope | Its lifecycle, active checkpoint, verdict, and cleanup state |
| `POST /v1/claude` | A subject, worktree, driver options, expected subject token, and idempotency key | The agent subject, token, reconcile subject, and event cursor |
| `POST /v1/sessions/{subject}/context/clear` | An expected incarnation and idempotency key | The recorded control action and result cursor |
| `POST /v1/sessions/{subject}/signal` | An expected incarnation, signal, and idempotency key | The recorded action and result cursor |
| `POST /v1/sessions/{subject}/attach` | A live subject and terminal dimensions | A short-lived terminal capability and WebSocket path |
| `GET /v1/sessions/logs/{subject}` | Byte offset, limit up to 64 KiB, current or previous generation, and optional long wait | A generation-bound base64 log chunk, offsets, EOF state, and exit status |
| `GET /v1/sessions/screen/{subject}` | A terminal subject | Its current plain screen and incarnation |
| `POST /v1/sessions/input/{subject}` | An expected incarnation, line, raw, or key input, and an idempotency key | The recorded input action and result cursor |
| `POST /v1/peer/claims` | Ordered replica batches from one configured peer | Accepted and missing replica sequence ranges |
| `POST /v1/peer/claims/query` | Per-replica sequence heads | A stream of missing replica batches |

Clients percent-encode a full subject when the subject occupies one path segment.

The context-clear control supports a live terminal Claude driver in version 1. The adapter translates the typed request to Claude's clear control.

The session-signal control accepts `interrupt`, `hangup`, `user-1`, or `user-2`. Lifecycle stop still uses desired state, not this endpoint.

The health response includes the st3 version and active process isolation mode.

An exec log keeps the current generation and one previous generation. A client cannot join bytes from two generations.

The input endpoint stores the input bytes as a blob. It records request and result claims before it returns.

### Intent request types

```text
SubjectToken {
    subject: Subject
    intent_heads: SortedSet<Hash>
}

DocumentToken {
    subject: Subject
    binding_heads: SortedSet<Hash>
}

SubjectDesiredState {
    subject: Subject
    kind: DesiredKind
    body: CanonicalCbor
}

IntentInput {
    media_type: "application/vnd.st3.intent+kdl"
    source_name: String
    bytes: Bytes
}

DocumentPutRequest {
    name: String
    media_type: "text/plain; charset=utf-8"
    bytes: Bytes
    expected_document: DocumentToken
    idempotency_key: String
}

DocumentPutResponse {
    blob_hash: Hash
    canonical_reference: String
    document_token: DocumentToken
    binding_claim_id: Hash
    store_index: u64
}

PlanIntentRequest {
    intent: IntentInput
    at_index: Option<u64>
}

ApplyIntentRequest {
    intent: IntentInput
    expected_subjects: [SubjectToken]
    idempotency_key: String
}

PlanIntentResponse {
    snapshot_index: u64
    resolved_intent: IntentInput
    subject_tokens: [SubjectToken]
    normalized_subgraph_digest: Hash
    normalized_desired_states: [SubjectDesiredState]
    normalized_diff: [SubjectDiff]
    proposed_claims: [ClaimDraft]
    predicted_actions: [ActionPreview]
    blockers: [Reason]
}

ApplyIntentResponse {
    changed: bool
    store_index: u64
    subject_tokens: [SubjectToken]
    batch_id: Option<Hash>
    claim_ids: [Hash]
    reconcile_subjects: [Subject]
}
```

`POST /v1/intent/plan` never appends a claim and never starts a driver.

`POST /v1/intent/apply` parses and validates again inside the write transaction.

`POST /v1/documents` stores the blob and publishes its name binding atomically. It publishes no intent subject.

Posting the same bytes under the same name is a no-op. Posting different bytes requires the current document token.

The document response returns the exact hash token that KDL references use after `@`.

Any host can plan and accept an authorized publish from the claims that it currently knows.

A new subject has an empty head set. An existing subject token contains every current intent head known at that snapshot.

Apply compares each named subject separately. It appends all changed desired states in one local batch.

Actual-state changes do not make an intent token stale. Predicted actions are advisory, and reconciliation reads a fresh state frontier after apply.

A desired state that matches the selected revision returns no new claim. An absent subject stays unchanged.

A host can accept an offline write when its expected heads match its local heads.

If replication later adds a concurrent leaf, the reducer marks only that subject conflicted and selects its deterministic winner.

Reads and reconciliation continue from the winner. A later changed desired state cites every current leaf and replaces them.

The server stores source bytes as an optional audit blob. The normalized intent claims remain the authority.

### Status response

```text
StatusView {
    snapshot_index: u64
    subjects: [SubjectStatus]
    scopes: [ScopeStatus]
    checkpoint_sequences: [CheckpointStatus]
    provenance: [Hash]
}

SubjectStatus {
    subject: String
    intent_token: SubjectToken
    document_token: Option<DocumentToken>
    intent_state: "clean" | "conflicted"
    winning_revision: Option<Hash>
    losing_revisions: [Hash]
    scopes: [Subject]
    desired: DesiredState
    actual: ActualState
    gap: Option<Gap>
    reachability: "reachable" | "unreachable" | "indeterminate"
    reason: Option<Reason>
    restart_policy: RestartPolicyView
    provenance: [Hash]
}
```

The response always separates desired state, actual state, and the gap reason for each subject.

`snapshot_index` belongs to the responding host. It gives a reproducible local query.

`SubjectToken` uses claim hashes. It is stable across hosts and gives wall-clock time no ordering meaning.

This token is the per-subject compare-and-swap index for a write on one node.

`DocumentToken` uses the selected document binding heads. It provides the same compare-and-swap rule for `st3 doc put`.

Two writes to different subjects never conflict. Audit claims do not change an intent head.

A supervision log append therefore cannot make a planned subject change stale.

### Other request types

```text
MessageRequest {
    message_subject: Subject
    sender: Subject
    recipient: Subject
    content: Bytes | BlobHash
    close_judges: [JudgeDefinition]
    expected_subjects: [SubjectToken]
    idempotency_key: String
}

ClaimRequest {
    subject: Subject
    kind: String
    actor: Option<Subject>
    body: Object
    evidence_claim_ids: [Hash]
    idempotency_key: String
}

ClaimResponse {
    claim_id: Hash
    store_index: u64
}

MessageResponse {
    store_index: u64
    message_token: SubjectToken
    message_claim_id: Hash
    delivery_action_id: Hash
}

MessageClaimRequest {
    transition: "accepted" | "closed"
    actor: Option<Subject>
    evidence_claim_ids: [Hash]
    expected_message: SubjectToken
    idempotency_key: String
}

MessageClaimResponse {
    lifecycle_claim_id: Hash
    store_index: u64
    message_token: SubjectToken
}

JudgementRequest {
    operation_capability: Secret
    verdict: "pass" | "fail"
    reason: String
    evidence_claim_ids: [Hash]
    idempotency_key: String
}

JudgementResponse {
    judge_result_claim_id: Hash
    store_index: u64
}

EvalRequest {
    bundle: DeterministicArchive
    expected_subjects: [SubjectToken]
    idempotency_key: String
}

EvalResponse {
    scope: Subject
    store_index: u64
    subject_tokens: [SubjectToken]
    reconcile_subjects: [Subject]
    event_cursor: u64
}

ClaudeRequest {
    subject: Subject
    worktree: String
    model: Option<String>
    effort: Option<String>
    expected_subject: SubjectToken
    idempotency_key: String
}

ClaudeResponse {
    subject: Subject
    store_index: u64
    subject_token: SubjectToken
    reconcile_subject: Subject
    event_cursor: u64
}

AttachRequest {
    rows: u16
    columns: u16
}

AttachResponse {
    subject: Subject
    incarnation_id: UUID
    capability: Secret
    websocket_path: String
    expires_at: Time
}

ClearContextRequest {
    expected_incarnation_id: UUID
    expected_subject: SubjectToken
    idempotency_key: String
}

ClearContextResponse {
    action_request_id: Hash
    store_index: u64
    subject_token: SubjectToken
    event_cursor: u64
}

PeerClaimAppendRequest {
    batches: [ReplicaBatch]
}

PeerClaimQueryRequest {
    replica_heads: Map<ReplicaId, { sequence: u64, batch_id: Hash }>
}

PeerClaimResponse {
    accepted_heads: Map<ReplicaId, { sequence: u64, batch_id: Hash }>
    missing_ranges: [ReplicaRange]
}
```

`GET /v1/claims` returns a bounded page plus `next_cursor`. `GET /v1/events` returns bounded long-poll batches in the response envelope.

The event long poll and the reconcile queue use separate wake channels.

An event subscriber cannot consume a reconcile permit.

### Every change has a claim

The write path records a durable request before it changes a process, PTY, harness, message, or graph.

The generic terminal WebSocket accepts ordered input frames. Each frame becomes a private
`terminal.input.requested` claim before the PTY driver writes its bytes.

The claim records the user, target incarnation, frame sequence, byte hash, and encrypted blob reference.

The driver appends `terminal.input.result` after it writes the frame. A crash resumes or rejects the same frame by its sequence.

Control claims use `operation_status`. They do not replace the member lifecycle `status` in the graph view.

Graph control does not use terminal text. Context clearing, lifecycle control, and agent messages use typed API requests.

Each harness driver filters its versioned control sequences from the data channel. It returns a typed API instruction instead of forwarding them.

A context generation change without a matching control action becomes a state-bearing harness claim. The member then becomes indeterminate.

For example, context clearing records `context.clear.requested` before the harness driver acts. The driver then records `context.clear.result`.

A session signal uses the same request-before-effect rule. An external signal still produces a process observation with an unknown external cause.

The message reducer recognizes this ordered lifecycle:

```text
message.sent -> message.delivered -> message.accepted -> message.closed
```

The sender or message API writes `message.sent`. That claim includes any sender-owned close judge hashes.

The transport sends the message, but transport acceptance does not prove delivery to the conversation.

The native harness driver watches its declared session file. It publishes `message.delivered` when the message appears there.

Only the recipient knows that it accepted a message. The recipient publishes `message.accepted` through the API.

An agent acceptance names that agent as actor. A person interface records itself as origin and the person subject as actor.

The recipient can publish `message.closed`. A declared held-out judge can also publish a close verdict.

Both close forms cite their evidence. Detecting delivery never auto-publishes accepted or closed.

The reducer rejects a transition that skips a lifecycle step. A closed claim waits until an accepted claim exists.

The driver also publishes typed claims for work start, idle, turn usage, turn cost, compaction, clear, and harness errors.

Each appended turn produces its usage and cost claim. This observation needs no timer.

An open delivered message with an idle recipient is a visible gap. The idle claim starts reconciliation without a timer.

Status can show that delivery occurred forty minutes ago, no close exists, and the agent is idle.

A weekly quota query is different because only `claude -p "/usage"` can answer it at that time.

Use a scheduled message for that query. Do not add a usage poll.

### CLI mapping

Every command accepts `--endpoint`. It selects a local socket or a configured HTTP endpoint.

`st3 run FILE` reads one KDL file. It never reads a filesystem path from a document reference.

The command does not retry a stale subject token. It prints the changed subject and asks the caller to review a new plan.

To stop a member, the document publishes desired stopped state. No command or API deletes its declaration.

```text
run(file, expected_subjects?, json):
    bytes = read_exact_file_or_stdin(file)
    preview = POST /v1/intent/plan { intent: { bytes } }
    if preview.blockers is not empty:
        print(preview.blockers)
        exit 3
    if expected_subjects is absent:
        expected_subjects = preview.subject_tokens
    result = POST /v1/intent/apply {
        intent: preview.resolved_intent,
        expected_subjects,
        idempotency_key: hash(preview.resolved_intent, expected_subjects, caller_origin)
    }
    print(result)
```

`st3 plan FILE` calls only the plan endpoint. It shows each bare document name as its resolved name-and-hash reference.

It returns zero when the document is valid, even when changes exist.

```text
plan(file, at_index?, json):
    bytes = read_exact_file_or_stdin(file)
    result = POST /v1/intent/plan { intent: { bytes }, at_index }
    print(result.resolved_intent, result.normalized_diff, result.predicted_actions, result.blockers)
```

`st3 import CATALOG` reads only the named folder. It combines its new-format KDL files into one deterministic publish.

```text
import(catalog, json):
    files = read_declared_kdl_tree(catalog)
    intent = normalize_import(files)
    return run(intent, json)
```

The daemon receives the resulting bytes. It does not retain, sync, or watch the source folder.

`st3 doc put FILE --as doc/NAME` reads only `FILE`. It posts the document before any intent references it.

```text
doc_put(file, name, json):
    bytes = read_exact_utf8_file(file)
    expected_document = GET /v1/status { subject: name }.subject_token_or_empty
    result = POST /v1/documents {
        name,
        bytes,
        expected_document,
        idempotency_key: hash(name, bytes, expected_document, caller_origin)
    }
    print(result.blob_hash)
```

The command refuses an absolute name, `..`, a symbolic-link source, invalid UTF-8, and an oversized document.

The command hashes the local bytes before posting. It warns when that hash differs from the selected binding for the name.

The warning shows both hashes. The post still succeeds and returns the new hash for the authored KDL reference.

`st3 status [SUBJECT]` calls the status endpoint. `--at INDEX` gives a reproducible historical view.

```text
status(subject?, scope?, at_index?, json):
    result = GET /v1/status { subject, scope, at_index }
    print(result)
    exit 4 if result has a selected unreachable subject or terminal failed eval else 0
```

A conflict does not change the exit code. Status shows the selected winner and each losing revision.

`st3 claim SUBJECT KIND [--actor SUBJECT] [--field KEY=VALUE]...` posts one typed observation.

The daemon supplies the recorded origin. The command cannot select another origin or bypass the claim kind registry.

The server records acceptance time. It validates the optional actor and every claim body field against the selected kind schema.

`st3 eval EVAL` builds a deterministic archive from the explicitly named eval. It does not discover sibling evals.

```text
eval(eval_dir, json):
    bundle = archive_explicit_eval(eval_dir)
    started = POST /v1/evals {
        bundle,
        expected_subjects: empty_tokens_for_generated_subjects(bundle)
    }
    for event in GET /v1/events {
        after_index: started.event_cursor,
        scope: started.scope
    }:
        render(event)
        if event is eval.verdict for started.scope:
            verdict = event.verdict
        if event is checkpoint.reached for started.scope stop:
            return verdict_exit_code(verdict)
```

The CLI reports a verdict when it arrives, but it waits for the judged scope stop before it exits.

`st3 judgement pass|fail --reason TEXT [--evidence CLAIM_ID]...` posts one running judge result.

The runner supplies the endpoint and a single-operation capability. The server derives the checkpoint and judge identity from that capability.

The command cannot post for another judge definition. Repeating an identical request returns the first result.

`st3 claude` calls the dedicated endpoint, waits on events, and attaches through the terminal endpoint.

Its complete path appears in the `st3 claude` section.

`st3 exec -- COMMAND` creates a normal standalone `exec` declaration. It uses `restart "never"` and the normal plan and apply endpoints.

The command waits for the member, follows its log, and returns the remote exit status. `--detach` returns after publication.

An interrupt stops only the local log follow by default. `--cancel-on-interrupt` publishes explicit stop intent before it returns.

`st3 logs SUBJECT` shows the last 64 KiB. `--all`, `--follow`, and `--previous` select the full log, live output, or prior generation.

`st3 pty ls|attach|peek|send|signal` uses graph subjects. Each mutation uses the incarnation that the API returned.

`st3 pty ui` starts the installed PTY interface with the configured registry. It refuses an HTTP endpoint.

`st3 inspect SUBJECT` shows the subject status and its 20 newest claims. `st3 trace` shows bounded claim history and can follow events.

`st3 wait SUBJECT --for CONDITION` checks status before it waits on events. Conditions include `running`, `ready`, `exited`, and `stopped`.

The command also accepts `checkpoint=NAME` and `verdict=pass|fail|void`. The default timeout is ten minutes, and zero disables it.

`st3 doctor` prints typed checks. Warnings fail only with `--strict`, while failed checks always return an error.

`st3 service install|status|uninstall` manages a Linux systemd user service. Install writes the resolved configuration, executable, and `PATH` into the unit.

The service has a 1 GiB memory limit. Install refuses a Linux host that cannot create transient user scopes.

### CLI exit codes

| Code | Meaning |
|---|---|
| `0` | The command completed successfully. |
| `2` | Input or KDL validation failed. |
| `3` | A local compare-and-swap write was stale. |
| `4` | A selected subject or eval reached a terminal failure, or `st3 wait` reached its timeout. |
| `5` | The API was unavailable. |

`st3 exec` returns the remote exit status. A signal result uses `128 + signal`.

## Claims store

### SQLite schema

The database contains immutable truth tables and rebuildable operational tables.

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE batches (
    store_index          INTEGER PRIMARY KEY,
    batch_id             BLOB NOT NULL UNIQUE,
    replica_id           TEXT NOT NULL,
    replica_sequence     INTEGER NOT NULL,
    previous_replica_batch BLOB,
    origin               TEXT NOT NULL,
    accepted_at_ns       INTEGER NOT NULL,
    idempotency_key      TEXT,
    UNIQUE(replica_id, replica_sequence),
    UNIQUE(replica_id, origin, idempotency_key)
);

CREATE TABLE claims (
    claim_id              BLOB PRIMARY KEY,
    store_index          INTEGER NOT NULL REFERENCES batches(store_index),
    ordinal              INTEGER NOT NULL,
    subject              TEXT NOT NULL,
    kind                 TEXT NOT NULL,
    reconcile_input      INTEGER NOT NULL CHECK(reconcile_input IN (0, 1)),
    schema_version       INTEGER NOT NULL,
    occurred_at_ns       INTEGER NOT NULL,
    origin               TEXT NOT NULL,
    actor                TEXT,
    replica_id           TEXT NOT NULL,
    replica_sequence     INTEGER NOT NULL,
    authority            TEXT NOT NULL,
    cause_claim_id        BLOB,
    dedupe_key           TEXT,
    body_cbor            BLOB NOT NULL,
    body_hash            BLOB NOT NULL,
    UNIQUE(store_index, ordinal),
    UNIQUE(origin, dedupe_key)
);

CREATE INDEX claims_subject_index ON claims(subject, store_index);
CREATE INDEX claims_kind_index ON claims(kind, store_index);
CREATE INDEX claims_origin_index ON claims(origin, store_index);

CREATE TABLE claim_predecessors (
    claim_id              BLOB NOT NULL REFERENCES claims(claim_id),
    predecessor_claim_id  BLOB NOT NULL,
    PRIMARY KEY(claim_id, predecessor_claim_id)
);

CREATE INDEX predecessor_reverse_index
    ON claim_predecessors(predecessor_claim_id, claim_id);

CREATE TABLE claim_scopes (
    claim_id              BLOB NOT NULL REFERENCES claims(claim_id),
    scope_subject        TEXT NOT NULL,
    PRIMARY KEY(claim_id, scope_subject)
);

CREATE INDEX claim_scopes_scope_index ON claim_scopes(scope_subject, claim_id);

CREATE TABLE blobs (
    blob_hash            BLOB PRIMARY KEY,
    media_type           TEXT NOT NULL,
    size_bytes           INTEGER NOT NULL,
    bytes                BLOB NOT NULL
);

CREATE TABLE peer_cursors (
    peer                  TEXT NOT NULL,
    replica_id            TEXT NOT NULL,
    replica_sequence      INTEGER NOT NULL,
    replica_batch_id      BLOB,
    PRIMARY KEY(peer, replica_id)
);

CREATE TABLE reducer_cache (
    cache_key             TEXT PRIMARY KEY,
    through_index         INTEGER NOT NULL,
    value_cbor            BLOB NOT NULL
);
```

`peer_cursors` and `reducer_cache` are not truth. Deleting them causes replay, not a different answer.

Database permissions deny other users. SQLite triggers reject `UPDATE` and `DELETE` on every truth table.

Posted documents and large eval assets live in `blobs`. A claim names each required blob by hash.

The server derives `reconcile_input` from the claim kind registry. It does not trust a value from a client or peer.

### Claim envelope

```text
Claim {
    claim_id: Hash
    subject: String
    scopes: SortedSet<Subject>
    kind: ClaimKind
    schema_version: u32
    ordinal: u32
    occurred_at_ns: i64
    origin: Origin
    actor: Option<Subject>
    replica_id: ReplicaId
    replica_sequence: u64
    authority: Authority
    cause_claim_id: Option<Hash>
    predecessor_claim_ids: SortedSet<Hash>
    dedupe_key: Option<String>
    body: CanonicalCbor
}

claim_id = sha256(canonical_cbor(
    subject,
    scopes,
    kind,
    schema_version,
    ordinal,
    occurred_at_ns,
    origin,
    actor,
    replica_id,
    replica_sequence,
    authority,
    cause_claim_id,
    predecessor_claim_ids,
    dedupe_key,
    body
))

revision_order_key = sha256(canonical_cbor(
    subject,
    scopes,
    kind,
    schema_version,
    origin,
    actor,
    replica_id,
    replica_sequence,
    authority,
    predecessor_claim_ids,
    body
))
```

Every claim has a subject, time, and recorder origin. An observed action can also name its actor.

The replica sequence, predecessors, and claim ordinal determine causal order.

An internal decision, request, result, or observation can also have a stable deduplication key.

The key names one logical event. It does not depend on wall-clock time or the local ingestion index.

Every reconciler draft has this key. A driver result uses the request ID plus its terminal result class.

The store rejects a missing or incorrect previous replica batch. Replication can then name the exact missing range.

### Claim kinds

The first implementation needs these state-bearing claim families:

- `intent.subject` publishes desired state for one subject and cites the intent heads that it replaces.
- `intent.stop` publishes desired stopped state without deleting intent.
- `intent.scope` publishes the desired member set for one scope subject. The set can be empty.
- `intent.schedule` publishes one declared message schedule.
- `intent.checkpoints`, `intent.link`, and `intent.supervisor` publish their typed policy subjects.
- `intent.resource`, `intent.host`, `intent.person`, and `intent.account` publish observed-subject declarations.
- `resource.binding` binds a role-named resource subject to a concrete external resource ID or document blob hash.
- `resource.file-observed` records an on-demand file read for a judge.
- `resource.session-bound` relates a harness session-file resource to one member incarnation.
- `actor.action-observed` records an external action and names its person or account actor.
- `agent.account` records which account one agent currently uses.
- `account.quota` records provider quota state for one account.
- `pid.observed` records a runtime incarnation and its state.
- `presence.observed` records agent presence.
- `harness.work-started` and `harness.idle` record session activity transitions.
- `harness.turn-usage` records tokens and cost for one appended turn.
- `harness.compacted` and `harness.cleared` record context-generation events.
- `harness.error` records a typed native harness error.
- `gate.observed` records a declared gate entering or leaving the visible screen.
- `transport.peer` records an up or down event on a host subject from the peer adapter.
- `message.sent` records accepted message content.
- `message.delivered` records that the native driver accepted the message into its durable delivery bridge.
- `message.accepted` records the recipient's lifecycle claim.
- `message.closed` records an agent close claim or a held-out judge close verdict.
- `action.result` records a driver result that changes actual state.
- `context.clear.result` records a harness mutation.
- `judge.result` records a checkpoint verdict.
- `checkpoint.reached` records a current checkpoint pass.
- `reachability.changed` records a reachable, unreachable, or indeterminate subject or scope state.
- `eval.verdict` records pass, fail, or void.
- `clock.reached` records one declared schedule occurrence.
- `deadline.reached` records one operation deadline.
- `daemon.started` starts recovery reconciliation after a daemon restart.

Each `pid.observed` body includes the OS PID and `incarnation_id`. PID reuse never reuses the incarnation identity.

These audit families do not trigger reconciliation by themselves:

- `supervision.decision` records what one pass observed, decided, and requested.
- `action.requested` gives a side effect a durable idempotency key.
- `terminal.input.requested` and `terminal.input.result` record terminal delivery without starting a pass for every byte.
- `context.clear.requested` records a control request before its effect.
- `clock.wake.requested` records one declared schedule occurrence before the clock adapter arms it.
- `clock.wake.cancel.requested` records schedule cancellation before the adapter changes its wake.
- `clock.adjusted` records an operating-system clock change without ordering other claims.
- `judge.requested` records the held-out definition and input snapshot.
- `replication.accepted` records peer exchange diagnostics.

The claim kind registry fixes authority rules and trigger behavior in code. KDL cannot add a claim kind.

The registry also marks declaration head kinds. These include members, structures, observed-subject declarations, and explicit stop intent.

Observation, result, lifecycle, and audit claims never become intent heads.

An unknown peer claim stays opaque and advances the replica chain. The receiver treats it as state-bearing and marks its subject indeterminate.

### Compare-and-swap append

```text
append_intent_batch(expected_subjects, origin, idempotency_key, desired_states):
    BEGIN IMMEDIATE
    replica_id = local_replica_id()

    current_store = SELECT COALESCE(MAX(store_index), 0) FROM batches
    for expected in expected_subjects:
        current = reduce_intent_heads(expected.subject, current_store)
        if current != expected.intent_heads:
            ROLLBACK
            return StaleSubject(expected.subject,
                                expected.intent_heads,
                                current)

    prior = SELECT replica_sequence, batch_id FROM batches
            WHERE replica_id = :replica_id
            ORDER BY replica_sequence DESC LIMIT 1
    prior = prior or { sequence: 0, batch_id: None }
    sequence = prior.sequence + 1
    normalized = normalize_named_desired_states(desired_states)
    changed = derive_subject_patch(normalized, current_store)
    if changed is empty:
        ROLLBACK
        return NoChange(current_store, current_subject_tokens)
    claims = make_intent_claims(changed)
    claims = attach_predecessors(claims, expected_subjects)
    claims = validate_authority_and_hash(origin, replica_id, sequence, claims)
    batch_id = hash(replica_id, sequence, prior.batch_id, origin, claims)

    INSERT batches(current_store + 1, batch_id, replica_id, sequence, origin, ...)
    INSERT claims(current_store + 1, claims...)
    COMMIT

    notify_dispatcher(current_store + 1, trigger_kinds(claims))
    return current_store + 1, batch_id, claim_ids(claims)
```

An idempotent replay returns the original batch. Reusing a key with different content fails.

One client batch can change many named subjects. Every named state must have a matching expected subject token.

A subject absent from the publish gets no claim. The apply path never infers a stop from absence.

A normalized desired state equal to the selected current revision produces no patch and no claim.

A local stale token returns to the client. Replication can still reveal a concurrent same-subject head later.

Different-subject revisions combine by union. Multiple leaf revisions for one subject produce a visible conflict.

The reducer selects the same winning leaf on every host. Reconciliation continues from its desired state.

Losing leaves remain visible. A later changed desired state cites all current leaves and creates one new leaf.

The internal event intake revalidates an unchanged observation against the current subject state.

The intake then appends that observation through the serialized store writer. It uses no delayed retry.

Peer replication does not run local compare-and-swap. It verifies replica chains, preserves predecessors, and deduplicates claim IDs.

The reconciler uses a separate state frontier for one derived workset. Its decision batch commits only if that frontier is unchanged.

This state check prevents stale actions without turning unrelated subject activity into an intent conflict.

### Current-state query

A query always selects one local store index. Every returned field carries its source claim IDs.

```text
derive_view(selectors, at_index):
    subjects = expand_subjects_scopes_links_and_checkpoints(selectors, at_index)
    claims = SELECT * FROM claims
            WHERE subject IN subjects
              AND store_index <= at_index
            ORDER BY store_index, ordinal

    statuses = []
    for subject in subjects:
        leaves = reduce_authorized_intent_leaves(subject, claims)
        winner = select_winner_by_generation_and_revision_key(leaves)
        losing = leaves without winner
        desired = reduce_desired(subject, winner, claims)
        actual = reduce_actual_from_authoritative_origin(subject, claims)
        restart_state = reduce_restart_policy_state(subject, claims)
        gap = explain_gap(desired, actual, restart_state, claims)
        reachability = reduce_reachability(subject, gap, claims)
        statuses.push(SubjectStatus(subject,
                                    intent_state = conflicted if losing else clean,
                                    winning_revision = winner,
                                    losing_revisions = losing,
                                    desired, actual, restart_state, gap, reachability))

    return StatusView(statuses,
                      reduce_scopes(claims),
                      reduce_checkpoint_sequences(claims),
                      provenance(claims))
```

The reducer does not select the newest wall-clock time across origins.

Each revision has a generation equal to one plus its highest predecessor generation.

The leaf with the highest `(generation, revision_order_key)` tuple wins. The stable revision key breaks a generation tie.

The revision key hashes the subject, scopes, origin, actor, replica fields, authority, predecessors, schema, kind, and body.

It excludes wall-clock time.

All hosts therefore select the same winner after they hold the same claims.

An authorized intent claim supersedes only the predecessor heads that it cites.

A missing predecessor makes that subject indeterminate. Concurrent uncited leaves make it conflicted but still actionable.

The selected desired state drives reconciliation. Losing revisions stay visible in status and claim queries.

A scope-empty revision normally descends from its active revision. Its higher generation wins after a partition heals.

The placement host is authoritative for a member's process state. The member origin is authoritative for its own presence.

The declared driver grants the host reader authority for typed claims from that harness's own files.

It does not grant access to another harness's content.

A cache can hold reducer output through an index. The query checks the index and replays later claims before returning.

## Reconciliation

### Trigger and recovery

The dispatcher receives committed batches from the store. It selects only subjects affected by state-bearing claims.

There is no debounce timer. The dispatcher can coalesce batches already present in its queue before one fixed-snapshot pass.

On startup, the daemon appends one `daemon.started` claim for its daemon subject.

It then queries locally placed subjects and durable requests with unprocessed input. This bounded startup query is not a periodic discovery sweep.

```text
on_batch_committed(batch):
    affected = trigger_registry.affected_subjects(batch.claims)
    for subject in affected:
        ready_queue.insert(subject)

worker_loop():
    while seed_subject = ready_queue.next():
        snapshot = store.current_index()
        workset = expand_reconcile_workset(seed_subject, snapshot)
        reconcile(workset, snapshot)
```

The in-memory queue is disposable. The decision log shows the last processed input index for each local member.

Startup compares those indexes with state-bearing claims. It rebuilds missing work without a sweep.

A workset is a derived closure over the seed subject, its scope tags, active checkpoint sequences, and required links.

It exists for one pass. It is not a graph identifier or a stored namespace.

Two active checkpoint sequences can name the same subject. Different desired revisions use the normal deterministic winner rule.

A subject outside a checkpoint sequence gets one implicit direct target. Its `control_subject` is the subject itself.

The runtime owns `ST_AGENT`. It sets this value to the canonical graph subject for every launched member.

The runtime also adds the active st3 executable directory to the child `PATH`.

A native driver wrapper uses the exact executable that started the daemon. The stored desired state keeps the portable `st3` name.

### One reconcile pass

```text
reconcile(workset, snapshot_index):
    view = derive_workset(workset, snapshot_index)
    drafts = []

    checkpoint = view.active_checkpoint_or_direct_intent()

    if checkpoint.desires_scope_empty:
        for member in view.local_recorded_scope_members(checkpoint.scope):
            observation = collect_free_evidence(member, view, snapshot_index)
            decision = StopRecordedIncarnation() if observation.is_live else None
            action = choose_one_bounded_action(decision)
            drafts.add(supervision_decision_once(
                member,
                snapshot_index,
                observation.claim_ids,
                decision,
                action.id if action else None
            ))
            if action:
                drafts.add_missing(action_request(action.id, action))

        for member in view.unreachable_recorded_scope_members(checkpoint.scope):
            drafts.add(scope_reachability_if_changed(
                checkpoint.scope,
                "unreachable",
                "cannot reach intent for " + member.subject
            ))

        if view.scope_actual_members(checkpoint.scope) is empty:
            frontier = relevant_evidence_frontier(checkpoint, snapshot_index)
            drafts.add_missing(judge_observation_and_run_requests(checkpoint, frontier))
            if all_current_judges_pass(checkpoint, frontier):
                drafts.add(checkpoint_reached_once(checkpoint, frontier))

        commit = append_pass_once(view.state_frontier, drafts)
        if commit is not StaleState:
            for request in commit.new_requests:
                driver.submit(request)
        return

    for member in view.local_unblocked_members:
        observation = collect_free_evidence(member, view, snapshot_index)
        decision = decide_member(member, observation, checkpoint, view)
        action = choose_one_bounded_action(decision)
        action_id = hash(checkpoint.control_subject, checkpoint.id,
                         member.subject,
                         observation.state_frontier, action) if action else None

        drafts.add(supervision_decision_once(
            member,
            snapshot_index,
            observation.claim_ids,
            decision,
            action_id
        ))
        if decision is Raise or decision is Indeterminate:
            drafts.add(member_reachability_if_changed(member, decision))
        if action:
            drafts.add_missing(action_request(action_id, action))

    if desired_subgraph_holds(checkpoint, snapshot_index):
        frontier = relevant_evidence_frontier(checkpoint, snapshot_index)
        drafts.add_missing(judge_observation_and_run_requests(checkpoint, frontier))
        if all_current_judges_pass(checkpoint, frontier):
            drafts.add(checkpoint_reached_once(checkpoint, frontier))

    commit = append_pass_once(view.state_frontier, drafts)
    if commit is StaleState:
        return

    for request in commit.new_requests:
        driver.submit(request)
```

The reconciler requests at most one lifecycle action per member in one pass.

Every pass records one supervision decision for each local member in its derived workset.

This includes the first checkpoint and selected winners with losing revisions. The decision cites all visible leaves.

A conflict stays observable but does not stall the subject. The selected winning desired state drives the normal pass.

Publishing a scope as empty is idempotent. A second identical publish produces no claim.

The empty desired state replicates after a partition. A returning host then stops its own recorded scope members.

This process needs no orphan search. Each driver targets only an incarnation recorded for that scope.

The last checkpoint judges actual scope membership as empty. The scope stop is judged instead of assumed.

An unreachable member keeps the scope actual state non-empty. Status reports `cannot reach intent` until that host returns.

An unrelated subject does not join that workset.

One pass commits all decision and request claims in one batch. It starts side effects only after that batch commits.

A driver result appends a new state-bearing claim. That claim starts the next pass.

If a target action is required and no safe action exists, the member becomes `unreachable` or `indeterminate` with a reason.

The reconciler does not retry unchanged input.

Reachability writes are idempotent by state and cause. An unchanged reason does not append another triggering claim.

Judge requests use a hash of relevant evidence claims. A failed result does not request the same judge again without new relevant evidence.

### Idempotent actions

```text
append_pass_once(expected_state_frontier, drafts):
    unique = drafts without origin and dedupe keys already in the store
    if unique is empty:
        return Committed(new_requests = [])

    batch = append_reconcile_batch(
        expected_state_frontier,
        daemon_origin,
        hash(expected_state_frontier, dedupe_keys(unique)),
        unique
    )
    return Committed(new_requests = request_claims(batch))

driver.submit(request):
    observed = driver.inspect(request.subject)
    if action_already_satisfied(request.action, observed):
        append_state_claim("action.result", request.id, "adopted", observed)
        return

    result = driver.perform(request.action)
    append_state_claim("action.result", request.id, result)
```

`append_reconcile_batch` recomputes the workset state frontier inside the write transaction.

It returns `StaleState` when a relevant state-bearing claim changed. An unrelated subject does not change this frontier.

The runtime driver must adopt a matching live incarnation. It must not start a duplicate after a daemon crash.

The decision and its action request commit in one batch. A result claim links to the request through `cause_claim_id`.

A stale append runs no driver. The new state-bearing claim already queued a fresh pass.

On daemon startup, each driver resumes durable action requests that have no terminal result.

### Shutdown deadline

```text
stop_member(member, timeout):
    append action.requested(Terminate, deadline = now + timeout)
    send SIGTERM to the recorded incarnation

    when process_exit_event arrives:
        append pid.observed(exited, cause = Terminate)

    when the recorded one-shot deadline arrives first:
        recheck the incarnation
        append deadline.reached
        send SIGKILL only to that same incarnation
```

After a daemon restart, the driver rebuilds pending one-shot deadlines from action claims. An
expired deadline runs immediately after reinspection.

### Scheduled messages

A schedule is an intent subject. Version 1 supports one UTC `at` time or a fixed `every` interval with a UTC anchor.

The schedule declares one clock host. Only that placement host arms the occurrence wake.

Each recurring schedule declares `catch-up "all"`, `catch-up "latest"`, or `catch-up "skip"`.

`catch-up "all"` also declares `max-catch-up`. Exceeding that limit makes the schedule unreachable with a reason.

The clock adapter owns one recorded wake registration for the next occurrence. The reconciler does not inspect elapsed time.

```text
reconcile_schedule(schedule, claims):
    occurrence = next_unrecorded_occurrence(schedule, claims)
    if occurrence exists and no wake request exists:
        append clock.wake.requested(
            schedule = schedule.subject,
            revision = schedule.intent_head,
            occurrence = occurrence.number,
            scheduled_at = occurrence.time,
            dedupe = hash(schedule.intent_head, occurrence.number)
        )

clock_adapter.on_reached(request):
    append clock.reached(
        schedule = request.schedule,
        revision = request.revision,
        occurrence = request.occurrence,
        scheduled_at = request.scheduled_at,
        cause = request.claim_id
    )
```

`clock.reached` is state-bearing. Its commit starts reconciliation, which creates the scheduled message through the normal message lifecycle.

The schedule declaration grants the reconciler narrow authority to create only that template's occurrence messages.

The message subject derives from the schedule subject, revision, and occurrence number.

The occurrence number and schedule revision make delivery idempotent. Wall-clock time selects a declared slot but never resolves claim order.

On daemon startup, the clock adapter rebuilds its wake registrations from claims.

A schedule edit or stop records `clock.wake.cancel.requested` before the adapter cancels the old revision.

A conflicted schedule follows its selected winning revision. A winner change cancels the old wake before it arms the new wake.

A late `clock.reached` for an old revision remains an audit trail. It cannot create a message for the current schedule.

A missed occurrence follows the declared catch-up policy. Recovery emits bounded occurrence claims instead of polling historical time.

An operating-system clock-change event records `clock.adjusted` before it re-arms the one-shot wake. It does not order intent claims.

The hourly report refresh uses `every "1h"` and `catch-up "latest"`.

### Restart type, shutdown deadline, and intensity

Every member has a restart type, a shutdown timeout, and the current st2 restart intensity block.

`restart "always"` permits a relaunch after any exit. `restart "on-failure"` permits it only after an unsuccessful exit.

`restart "never"` prevents an automatic relaunch after the first terminal process observation.

A terminal exit satisfies a `never` member target, so checkpoint judges can assert its exit fields.

A successful exit satisfies an `on-failure` member target. An unsuccessful exit remains a relaunch gap while its budget permits.

`shutdown-timeout` sets the delay between SIGTERM and SIGKILL for the recorded incarnation.

The intensity block answers how often an allowed relaunch can occur. It does not select which exit types allow one.

Its `attempts`, `interval`, `delay`, and `mode` fields keep their current st2 meanings.

Normalization makes all three controls explicit. An omitted field receives its current effective default.

`delay` is the minimum spacing between launches in both modes. The daemon uses one recorded deadline for the next eligible launch.

In `mode "delay"`, `attempts` limits allowed launches during the sliding `interval`. Exhaustion waits until one launch leaves the window.

In `mode "fail"`, `attempts` is a terminal launch budget. Full observed recovery through `interval` resets that budget.

An adopted process does not consume a launch. A parked member needs an explicit control claim or a new desired revision.

A fault injector uses `restart "never"`. Its successful or unsuccessful exit cannot start it again automatically.

Budget exhaustion makes the member unreachable and records the last crash reason.

A required link propagates that state to its dependent subject. An eval can then record `void` and enter its final stop checkpoint.

## Supervision

Supervision runs inside reconciliation. A supervisor declaration is a policy boundary, not a process or runtime member.

The implicit root supervisor owns every member without an explicit supervisor. Nobody can replace or delete it.

It has the reserved subject `supervisor/root`. The reducer synthesizes its shipped policy without an intent claim.

Each local member gets one `supervision.decision` claim per pass. The claim includes observation IDs, a decision, and an action request ID.

```text
SupervisionDecisionClaim {
    supervisor: Subject
    member: Subject
    input_frontier: Hash
    policy_hash: Hash
    observation_claim_ids: [Hash]
    judgement: "healthy" | "held" | "action-needed" | "unreachable" | "indeterminate"
    decision: Decision
    requested_action_id: Option<Hash>
}
```

The supervisor subject attributes the decision. The policy hash lets an eval replay the exact rules that produced it.

Presence freshness never means an elapsed wall-clock age. A fresh presence claim matches the current incarnation and transport connection epoch.

Presence answers the ladder only when that current claim reports the needed busy, idle, or unavailable state.

A process exit, peer drop, or harness session replacement invalidates that presence through a new claim.

Declaring a native driver grants it permission to read that harness's own session files.

The driver declares each session file as a `harness.session-file` resource and records its member-incarnation binding.

It reads the current session resource at startup. File notifications process each later append without polling.

It publishes message delivery, work start, idle, turn usage, turn cost, compaction, clear, and error claims.

The session file proves that a message reached the conversation. It cannot prove that the agent accepted it.

Harness claims answer only when their session generation matches the current incarnation.

The PTY output path matches only declared gate profiles. A gate entering or leaving the screen appends `gate.observed`.

The driver never publishes a general screen-state claim.

The cheap ladder is deterministic:

```text
collect_free_evidence(member, view, snapshot):
    process = view.actual_process(member)
    if process is not alive:
        return Evidence(ProcessDead, process.claim_id)

    presence = view.current_presence(member)
    if presence answers the supervision question:
        return Evidence(PresenceState, process.claim_id, presence.claim_id)

    harness = view.current_harness_metadata(member)
    if harness answers the supervision question:
        return Evidence(HarnessState, process.claim_id, harness.claim_id)

    if member.has_declared_gates:
        gate = view.current_declared_gate(member)
        if gate matches the current incarnation and frame sequence:
            return Evidence(DeclaredGate, gate.policy_hash, gate.claim_id)

    return Evidence(Unresolved, process.claim_id)

decide_member(member, evidence, checkpoint, view):
    if view.required_dependency_is_unreachable(member):
        return Hold("required dependency is unreachable")

    if desired_lifecycle_and_process_differ:
        return lifecycle_action_within_budget(member, view)

    if evidence is DeclaredGate and evidence.input_count < evidence.gate.maximum:
        return SendNextDeclaredKey()

    if evidence is DeclaredGate:
        return Raise("declared gate remained after bounded input")

    if owner_is_idle_for_open_checkpoint(evidence, checkpoint):
        return SendMessage("Owner idle", dedup = checkpoint_and_activity)

    if evidence is healthy:
        return None

    return AskResidueModel(bounded_choices) if configured else Raise(reason)
```

The model receives only structured residue and a bounded action list. It receives no shell capability and no free-form PTY capability.

A screen frame is never activity evidence. The supervisor uses it only to match a declared gate.

An unmatched or stale frame produces no conclusion. The residue model never receives that frame.

A healthy graph makes zero model calls. The MVP can call the bounded residue model only after every cheaper rung is unresolved.

An unreachable required target holds its dependent scope. The reconciler requests no new action in that scope until a relevant claim changes reachability.

## Checkpoints and judges

### Core structure

```text
SubgraphPublish {
    desired_states: NonEmpty<[SubjectDesiredState]>
    expected_subjects: [SubjectToken]
}

ScopeIntent {
    subject: Subject
    retention: "temporary"
    desired_members: SortedSet<Subject>
}

ResourceIntent {
    subject: Subject
    resource_kind: String
    binding: Option<ResourceBinding>
}

ResourceBinding {
    role_subject: Subject
    concrete_resource_id: String
    binding_claim_id: Hash
}

CheckpointSequence {
    subject: Subject
    scopes: [Subject]
    controlled_subjects: NonEmpty<[Subject]>
    checkpoints: NonEmpty<[Checkpoint]>
}

Checkpoint {
    id: String
    desired_subgraph: Option<[SubjectDesiredState]>
    judges: NonEmpty<[JudgeDefinition]>
}
```

The first checkpoint is the initial admission point. Its judges decide whether the sequence can start.

Every later checkpoint is a target. The reconciler publishes only the desired states in its optional `subgraph`.

A checkpoint without a `subgraph` only waits for its judges. It asks the reconciler to change nothing.

Every checkpoint must have a non-empty `judges` block.

Publish refuses a checkpoint that has a `subgraph` without `judges`. Such a target cannot prove that it closed.

The last checkpoint is final by position. No separate final field exists.

Normalization expands `controlled_subjects` from each nested subgraph and the sequence's current scope members.

A sequence cannot control an unnamed subject through document absence.

### Checkpoint progression

```text
select_target(sequence, claims):
    if no checkpoint has reached:
        return sequence.checkpoints[0] as InitialTarget

    reached = longest_ordered_prefix_with_current_pass_claims(sequence.checkpoints, claims)
    if reached.len == sequence.checkpoints.len:
        return FinalReached(sequence.checkpoints.last)

    return sequence.checkpoints[reached.len]
```

A current pass claim binds to the checkpoint definition hash and its relevant evidence frontier.

A later relevant claim invalidates that current result. An unrelated claim does not invalidate it.

A changed judge definition creates a new intent head for the checkpoint sequence. Old pass claims do not satisfy it.

Persistent subjects maintain their final checkpoint conditions after the first pass. A new gap makes the final checkpoint active again.

A temporary eval records its verdict after its last work checkpoint passes, fails at its deadline, or becomes void.

Any terminal verdict selects the final stop checkpoint. That checkpoint publishes the scope desired member set as empty.

The stop checkpoint passes only when its held-out judges confirm that the actual member set is empty.

An ordinary judge failure means that the checkpoint is not reached. It does not erase desired state.

A planner blocker or exhausted restart budget marks the affected subject or scope unreachable. A later relevant claim can make it reachable again.

### Judge results and patterns

```text
JudgeResultClaim {
    checkpoint_sequence: Subject
    scope: Option<Subject>
    checkpoint_id: String
    judge_id: String
    judge_kind: "pattern" | "exec" | "llm" | "deadline"
    definition_hash: Hash
    input_index: u64
    verdict: "pass" | "fail"
    reason: String
    evidence_claim_ids: [Hash]
    elapsed_ms: u64
    token_usage: Option<u64>
    judge_origin: Origin
}
```

Each judge result cites the exact snapshot and evidence claims that produced it.

A pattern judge reads one reduced subject view at `input_index`. It does not start a process or select a runner host.

The built-in predicates are `exists`, `empty`, `field`, `has`, and `lacks`. The `deadline` node is the built-in clock predicate.

`exists` tests that the subject has current actual or observed state. Its desired declaration alone does not pass this predicate.

`empty` tests that a scope has no recorded actual members.

`field` selects a dotted field path. It accepts `is`, `starts-with`, or `contains` as its comparison operator.

`has` and `lacks` test one content value. Version 1 permits them on file, document, and message subjects.

Every subject view derives from immutable claims. A later relevant claim can invalidate a prior pattern result.

A `deadline` is relative to checkpoint activation. It lets the other judges pass until the declared duration expires.

The clock adapter records the deadline operation before it arms the one-shot wake. Expiry gives the checkpoint a bounded failure reason.

A pattern over a file subject requests an observation from the host encoded in that subject. The reader records content, time, and origin.

That durable `resource.file-observed` claim becomes the pattern evidence. Later readers can reconstruct what the judge observed.

Only a running judge declares `host` and `workspace`. A remote host outage leaves that judge pending until its deadline.

A running judge time limit produces `fail` with a bounded reason. The runner stops the operation.

### Mechanical judge

A mechanical judge keeps the current `exec` shell command form. Exit zero passes, and any other exit fails.

The reconciler starts the command through the durable exec runtime and returns to other reconciliation work.

Later passes observe completion or enforce the time limit. The command does not block the reconcile loop.

```text
run_mechanical_judge(definition, snapshot):
    assert definition.authority == judge_request.requester_authority
    result = exec_shell(
        definition.command,
        host = definition.host,
        workspace = materialize_declared_workspace(definition, snapshot),
        environment = definition.environment,
        timeout = definition.time_limit,
        stdout_limit = 1 MiB,
        stderr_limit = 1 MiB
    )

    return JudgeResult(
        verdict = pass if result.exit_code == 0 else fail,
        reason = bounded_output(result),
        evidence = declared_inputs_and_outputs(result)
    )
```

The held-out judge bundle belongs to the requester. Worker write credentials cannot replace it.

A mechanical judge receives its declared host, workspace, and environment. The declaration can use a disposable copy or the live worktree.

The default mechanical judge time limit is 120 seconds. A declared `time-limit` replaces that default.

### LLM judge

The MVP uses this bounded runner:

```text
run_llm_judge(definition, snapshot):
    assert definition.authority == judge_request.requester_authority
    assert definition.token_budget is declared
    assert definition.time_limit is declared
    call = headless_agent.start(
        model = definition.model,
        prompt = definition.prompt,
        workspace = materialize_declared_workspace(definition, snapshot),
        tools = definition.tools,
        environment = definition.environment,
        max_total_tokens = definition.token_budget,
        deadline = now + definition.time_limit
    )

    judgement = wait_for_matching_judgement_claim(call.subject, definition.hash)
    if call.tokens > definition.token_budget:
        return fail("token budget exceeded")
    if call.deadline_exceeded:
        return fail("time limit exceeded")

    return judgement
```

The llm judge can read a diff, run tests, use the network, and publish claims allowed for its operation capability.

It is not limited to a fixed claim query or a prompt-only call. Its token budget and time limit remain mandatory.

The llm judge posts `pass` or `fail` through `st3 judgement`.

The runner reads the provider's structured usage output after exit. It does not parse judgment prose from process output.

Missing structured usage fails the judge. Usage above the declared token budget also fails the judge.

The implementation should later add one sandbox model for all agents. Version 1 does not impose a judge-only sandbox.

### Eval verdict and scope stop

An eval is a temporary subgraph with work judges, a terminal verdict, and a final stop checkpoint.

All gating judges passing gives `pass`. Any gating judge failing at the eval deadline gives `fail`.

A required member becoming unreachable under its declared policy gives `void`. A void eval is not a product failure.

After a verdict claim commits, the last checkpoint publishes desired state for the scope as empty.

Drivers stop and remove only the incarnations recorded as actual members of that scope.

Publishing empty twice is a no-op. An offline host applies the same empty state when replication resumes.

If one member is unreachable, the scope stays actually non-empty with a `cannot reach intent` reason.

The stop judges pass only after no recorded member remains. The verdict stays in the claims store after the scope stops.

### External-state convergence eval

A gate must not treat the first affirmative external liveness reading as settled evidence.

An observed process can still report `running` briefly after it exits. A slow observer can hide this window for years. A faster replacement can expose it without changing the provider's state transition.

A liveness gate requires two ordered observations of the same incarnation with no terminal observation between them. The second observation is a bounded revalidation action, not a periodic timer.

The eval corpus must start one process that exits immediately. The eval permits an initial `running` observation and requires convergence to `exited` before its deadline.

The eval fails if a gate advances from the first `running` observation. It also fails if the process remains incorrectly `running` until the deadline.

Long-lived fixtures such as `cat` or `sleep 30` do not cover this class. A passing suite with only those fixtures gives no evidence about immediate-exit convergence.

This rule comes from the st2 boot gate. Node `pty list --json` took 33 milliseconds and stepped over a five-millisecond stale window. Rust answered in 1.2 milliseconds and exposed the single-reading decision.

## Complete KDL specification

st3 keeps the current st2 agent declaration shape. It places that shape inside one explicit desired-state block.

The grammar below is normative for `application/vnd.st3.intent+kdl` version `st3.v1`.

### Document rules

An input is UTF-8 KDL 2. One input contains one `version 2` declaration and exactly one root `subgraph` node.

The version declaration contains one integer and no property, type, or child. A missing declaration means st2 version zero.

The root has no argument, property, or type annotation. Its only permitted sibling is the version declaration.

Comments do not affect normalization.

The root must contain at least one named desired-state node. An empty root is invalid.

A root `subgraph` accepts these desired-state nodes:

- `agent`, `exec`, `pty`, `scope`, `host`, `resource`, `person`, and `account`;
- `supervisor`, `link`, `checkpoints`, `message`, and `schedule`;
- `stop`.

A checkpoint `subgraph` accepts the same nodes except `checkpoints`. A checkpoint sequence cannot contain another checkpoint sequence.

The document order affects only checkpoints, render operations, task arguments, driver arguments, gate keys, and repeated judge declarations.

Other child maps normalize by name. Reordering those children produces the same desired-state hash.

Every named subject must occur at most once in one publish. This rule includes subjects inside checkpoint subgraphs and host groups.

A missing subject means unchanged. A client must publish `stop` to change a lifecycle subject to stopped state.

`stop "SUBJECT"` accepts only an agent, exec, or PTY subject. It refuses every structure or observed subject.

`scope "NAME" { stop }` is the second accepted scope-stop form. Its `stop` child takes no argument or property.

Unknown nodes, properties, type annotations, extra positional values, and wrong scalar types are errors unless this specification permits them.

A singular child can occur once. A repeated child can occur only where this specification marks it as repeated.

An empty required string is invalid. A non-empty string is otherwise accepted unless a node gives a narrower value set.

A Boolean is `#true` or `#false`. An integer must fit its specified unsigned or signed range.

A duration accepts a non-negative integer with `ms`, `s`, `m`, `h`, or `d`. A bare integer means seconds.

Normalization writes durations as integer milliseconds. A required duration must be greater than zero.

An absolute time is an RFC 3339 string with a `Z` offset. Version 1 refuses local offsets and named time zones.

A relative path resolves against the uploaded bundle root. An absolute path stays absolute on its declared host.

Environment expansion occurs at action time. The runtime expands declared `$NAME` references from the action environment.

### Subject names and defaults

The node type gives the subject namespace. A name that already has that namespace stays unchanged.

A normal subject is 1 through 512 ASCII bytes. It uses letters, digits, `.`, `_`, `-`, `@`, and `/`.

It must start with a letter or digit. It cannot contain an empty path segment, `..`, or a trailing slash.

Other names receive the namespace during normalization. Member namespaces are `agent/`, `exec/`, and `pty/`.

Other namespaces are `scope/`, `host/`, `resource/`, `person/`, `account/`, `supervisor/`, `link/`, `checkpoint/`, `message/`, and `schedule/`.

`file/` and `doc/` are resource shorthand namespaces. They identify resource kinds `file` and `document`.

An agent name without a dot receives its resolved host and a dot. Thus `release-owner` on `worker-1` becomes `agent/worker-1.release-owner`.

An agent name with a dot is already a bus identity. Thus `team.dev-42` becomes `agent/team.dev-42`.

A full subject string is required in judges, links, stop nodes, resource bindings, and checkpoint `scope` properties.

The message `from` and `to` children also accept a bare agent identity. Normalization adds `agent/` to that identity.

The reserved sender `requester` resolves to the recorded publisher subject. No other reserved subject word exists.

### Desired-state blocks

| Node | Arguments and properties | Children | Default and effect |
|---|---|---|---|
| `subgraph` | None | Desired-state nodes | Publishes only its named subjects. |
| `host "NAME"` | One host name | Agent, exec, and PTY declarations | Declares `host/NAME` and supplies a placement default. |
| `scope "NAME" retention=VALUE` | One scope name; optional `retention` | Agent, exec, and PTY declarations, or bare `stop` | `retention` defaults to `persistent`; accepted values are `persistent` and `temporary`. |
| `stop "SUBJECT"` | One full member subject | None | Publishes stopped state for one agent, exec, or PTY. |

A `host` group can contain agent, exec, and PTY nodes. It cannot contain another host group or a non-member subject.

An explicit member `host` child replaces the enclosing host default. An empty host group only declares the observed host subject.

A live scope contains its nested member subjects. A checkpoint sequence `scope` property also tags every subject in its active subgraph.

Nested scopes are invalid. A subject can still belong to multiple scopes through separate publishes and checkpoint sequence tags.

A scope cannot mix its bare `stop` child with member declarations. A bare stop must be the only scope child.

A scope stop ignores `retention`. The previous selected scope revision supplies that policy and its recorded member set.

### Agent and task nodes

`agent "NAME" { ... }` declares one aggregate member. Its nested `pty` and `exec` tasks are also members.

The aggregate agent subject controls all its task members. Stopping the agent stops only its recorded task incarnations.

| Agent child | Values | Default and rule |
|---|---|---|
| `identity "ID"` | One string | The node name; if present, this value replaces the positional name. |
| `name "TEXT"` | One string of at most 160 characters | No display name. |
| `description "TEXT"` | One string of at most 1,000 characters | No description. |
| `role "TEXT"` | One string | No role metadata. The runtime does not use this value. |
| `type "service"` | Only `service` | `service`; the removed `batch` value is invalid. |
| `host "NAME"` | One host name | The enclosing host, then the API host. `local` resolves to the receiving API host. |
| `workspace "PATH"` | One path | The uploaded bundle root on the resolved host. |
| `supervisor "NAME"` | A full or bare supervisor name | `supervisor/root`. |
| `keep BOOL` | One Boolean | `#false`; true exempts the agent tasks from normal garbage collection. |
| `lifecycle "VALUE"` | `service` or `adopt-only` | `service`; `adopt-only` never creates or reaps a missing generation. |
| `restart "VALUE"` | `always`, `on-failure`, or `never` | `always`. |
| `shutdown-timeout "DURATION"` | A positive duration | `5s`. |
| `restart { ... }` | One restart intensity block | `attempts 3`, `interval "60s"`, `delay "0s"`, and `mode "delay"`. |
| `deliver "VALUE"` | `mcp`, `app-server`, or `pi-channel` | No explicit transport. This is for a hand-authored launch. |
| `command "SHELL"` | One shell string | No compact command. This creates the primary PTY task. |
| `argv "PROGRAM" "ARG"...` | One or more strings | No compact argument vector. This creates the primary PTY task. |
| `ding` | No values or children | Disabled; this creates the legacy derived delivery task only when no native delivery owner exists. |
| `env { ... }` | One environment map | Empty. |
| `meta { ... }` | One metadata map | Empty. |
| `render { ... }` | One ordered render block | Empty. |
| `claude`, `codex`, `pi`, or `opencode` | One typed driver block | No driver. Exactly one driver type can occur. |
| `pty "NAME" { ... }` | Repeated, with unique task names | No explicit PTY tasks. |
| `exec "NAME" { ... }` | Repeated, with unique task names | No explicit exec tasks. |
| `resource "NAME" ...` | Repeated agent resource bindings | No bindings. |
| `stream "NAME" { ... }` | Repeated event sources | No streams. |

The compact `command` and `argv` forms are mutually exclusive. They are also mutually exclusive with a typed driver.

A typed driver and `deliver` are mutually exclusive. A typed driver or `deliver` takes precedence over `ding`, which adds no task. `ding` remains mutually exclusive with an explicit task named `ding`.

An agent must have one non-derived launch. A typed driver, compact launch, explicit PTY, or explicit exec satisfies this rule.

Task names must be unique across PTY and exec tasks.

`retired`, `desired-state`, and `suspended` are invalid agent children. Migration writes an explicit `stop` node instead.

The two `restart` forms are distinguished by shape. The string sets exit behavior, and the block sets restart intensity.

The restart intensity block accepts these singular children:

| Child | Accepted value | Default |
|---|---|---|
| `attempts INTEGER` | A positive `u32` | `3` |
| `interval "DURATION"` | A positive duration | `60s` |
| `delay "DURATION"` | A non-negative duration | `0s` |
| `mode "VALUE"` | `delay` or `fail` | `delay` |

Omitted restart children use their listed defaults. An empty restart block equals the full default block.

`mode "fail"` parks the member after the budget ends. `mode "delay"` resumes restarts when the interval permits them.

An `env` block accepts unique child names with one string value. An empty block is valid.

The runtime replaces an authored `ST_AGENT` value with the canonical member subject at launch.

A `meta` block accepts unique child names with one KDL scalar value. The reducer preserves this map but does not interpret it.

### Typed driver blocks

Each typed driver creates the primary PTY member. Driver arguments keep their declared order.

Claude supplies MCP delivery. Codex supplies app-server delivery, Pi supplies channel delivery, and OpenCode supplies server delivery.

| Driver | Children | Defaults and refusals |
|---|---|---|
| `claude` | Optional `model`, optional `effort`, optional `dev-channels`, required `prompt`, optional `args` | `dev-channels` is `#false`; `args` is empty. |
| `codex` | Optional `model`, optional `effort`, required `prompt`, optional `args` | `args` is empty; `dev-channels` is invalid. |
| `pi` | Optional `model`, optional `effort`, required `prompt`, optional `args` | `args` is empty; `dev-channels` is invalid. |
| `opencode` | Optional `model`, required `prompt`, optional `args` | `args` is empty; `effort` and `dev-channels` are invalid. |

`model`, `effort`, and `prompt` each accept one string. `args` accepts zero or more string arguments.

`dev-channels #true` selects Claude's prompt-based development channel. A declared supervisor gate can approve that prompt for a managed agent.

The provider validates supported model and effort strings. An unavailable value produces `unsupported-capability` during planning.

Every unsupported driver child is invalid. A driver child cannot repeat.

### PTY and exec task blocks

`pty` allocates a terminal. `exec` starts a process without a terminal.

Both task blocks accept these children:

| Task child | Accepted value | Default |
|---|---|---|
| `id "ID"` | One runtime ID | `<host>.<agent>.<task>`. |
| `command "SHELL"` | One shell string, run through `sh -c` | No shell command. |
| `argv "PROGRAM" "ARG"...` | One or more strings, run directly | No argument vector. |
| `cwd "PATH"` | One path | The agent workspace, then the bundle root. |
| `keep BOOL` | One Boolean | The agent `keep` value. |
| `lifecycle "VALUE"` | `service` or `adopt-only` | The agent lifecycle. |
| `tags PROP=STRING...` | Unique named string properties | Empty. |
| `env { ... }` | Unique child names with one string value | The agent environment. Task values replace agent values. |
| `unset "NAME"...` | One or more environment names | Empty. These names are removed after the environment merge. |

A task must contain exactly one of `command` and `argv`. Unknown task children and unnamed `tags` entries are invalid.

Environment names must match `[A-Za-z_][A-Za-z0-9_]*`. A task cannot both set and unset the same name.

A nested task normalizes to `pty/<agent-bus-id>/<task-name>` or `exec/<agent-bus-id>/<task-name>`.

The driver-created primary task uses the name `agent`. Derived delivery and stream tasks use their declared stable names.

### Standalone exec and PTY members

A root or scope can declare `exec "NAME" { ... }` or `pty "NAME" { ... }` without an aggregate agent.

The block accepts every task child above. It also accepts `host`, `workspace`, `supervisor`, both restart forms, `shutdown-timeout`, and `render`.

The added children use the agent defaults. Host resolves from the enclosing host or API host, and workspace defaults to the bundle root.

The portable host name `local` resolves to the receiving API host for agents, tasks, judges, and schedules.

Supervisor defaults to `supervisor/root`. Restart type defaults to `always`, and shutdown timeout defaults to `5s`.

Restart intensity uses `attempts 3`, `interval "60s"`, `delay "0s"`, and `mode "delay"`.

The task `cwd` defaults to the standalone workspace. `keep` defaults to `#false`, and lifecycle defaults to `service`.

The normalized subjects are `exec/NAME` and `pty/NAME`. A direct member name does not receive an agent host prefix.

A standalone member requires exactly one command or argument vector. It cannot contain a typed driver, task block, ding, resource, or stream.

An eval run step migrates to a standalone exec with an explicit restart type. Its exit pattern preserves the old nonzero rule.

### Agent resource and stream blocks

An agent resource binding has this complete form:

```kdl
resource "NAME" uri="ABSOLUTE-URI" reason="TEXT" inactive-reason="TEXT"
```

`uri` and `reason` are required string properties. `inactive-reason` is optional and marks only this binding inactive.

The URI must have an absolute scheme. Each reason must contain 1 through 160 UTF-8 bytes without control characters.

This nested node is agent configuration. A root resource node declares an observed graph subject instead.

A stream block accepts either `command "SHELL"` or `argv "PROGRAM" "ARG"...`. It accepts neither when external ingress owns the source.

A stream accepts no other child. In particular, `every` is invalid because scheduled work uses a `schedule` subject.

A launched stream derives one exec member under the agent. A command-less stream is a configured ingress endpoint.

### Render block

Render operations run in source order before any task starts. A failed content operation blocks that agent start.

| Operation | Arguments and properties | Effect and default |
|---|---|---|
| `copy` | `copy "SOURCE" "DESTINATION" executable=BOOL` | Copies one file; `executable` defaults to `#false`. |
| `file` | `file "DESTINATION" "CONTENT" executable=BOOL` | Writes inline content; `executable` defaults to `#false`. |
| `json-upsert` | `json-upsert "DESTINATION" "JSON" arrays=VALUE executable=BOOL` | Merges an object; `arrays` defaults to `replace`; accepted values are `replace` and `union`. |
| `ensure-line` | `ensure-line "DESTINATION" "LINE" executable=BOOL` | Adds one missing exact line; `executable` defaults to `#false`. |
| `git-exclude` | One or more path strings | Adds each path to the repository exclusion file. |

`file` and `json-upsert` also accept their content as one `content "TEXT"` child instead of the second argument.

The two content forms are mutually exclusive. A missing destination or content value is invalid.

A source path resolves against the uploaded bundle. A destination path resolves against the agent workspace.

Every content operation sets mode `0644` when `executable=#false`. It sets mode `0755` when `executable=#true`.

The mode result does not depend on a source file mode, an existing destination mode, or the process umask.

`json-upsert` requires a JSON object. `union` appends unequal array values and preserves existing array values.

`git-exclude` is advisory. Its failure records a warning and does not block the agent start.

### Observed subject declarations

Observed declarations name world-owned subjects. They never grant st3 lifecycle authority over those subjects.

| Node | Children | Defaults and rules |
|---|---|---|
| `resource "NAME"` | Required `kind`; optional `binding` | A name containing `@` defaults to late binding; another name defaults to concrete. |
| `person "NAME"` | None | Declares one actor identity. |
| `account "NAME"` | Required `provider`, `external-account`, and `auth-type` | No fields have defaults. |

`resource.kind` accepts one non-empty registered kind string. Version 1 includes these kinds:

- `vcs.pull-request`, `ci.run`, and `repository`;
- `file`, `document`, `harness.session-file`, and `human.review`.

An unknown resource kind produces `unsupported-capability`. A future daemon can add a kind without changing the node shape.

`binding` accepts only `late`. It is redundant for a role name that contains `@`.

A late-bound resource receives its concrete ID through a `resource.binding` claim. KDL cannot forge that observation.

`person` accepts one name and no properties or children. Display data belongs in observation claims.

An account has this complete body:

```kdl
account "NAME" {
  provider "PROVIDER"
  external-account "EXTERNAL-ID"
  auth-type "subscription"
}
```

`provider` and `external-account` accept one non-empty string. `auth-type` accepts `subscription` or `api-key`.

Quota is observed state and is invalid in the account declaration. The account adapter publishes `account.quota` claims.

An agent publishes an `agent.account` claim when it selects an account. The claim changes when the agent rotates accounts.

The host group declares its observed host subject. The peer adapter attaches `transport.peer` claims to that subject.

A repository and a harness session file are resource kinds. Neither is a separate subject type.

### Supervisor and gate nodes

`supervisor "NAME" { ... }` declares one policy subject. An empty block uses the shipped base policy.

The normalized supervisor definition produces its policy hash. Every `supervision.decision` claim names the supervisor subject and that hash.

A supervisor accepts repeated, uniquely named gate blocks. It accepts no other KDL children in version 1.

A gate has this form:

```kdl
gate "NAME" driver="claude" {
  contains "SUBSTRING"
  selected "EXACT LINE"
  key "enter"
  max-inputs 2
}
```

The `driver` property is required. It accepts `claude`, `codex`, `pi`, or `opencode`.

`contains` can repeat and supplies required normalized substrings. `selected` is optional and can occur once.

`key` can repeat and preserves order. Version 1 accepts `enter`, `escape`, `tab`, `space`, `up`, `down`, `left`, and `right`.

A gate needs at least one matcher and one key. `max-inputs` is a positive integer and defaults to the key count.

`max-inputs` can exceed the declared key count for a repeated prompt. It cannot be smaller than the key count.

Unknown screen content produces no input. A gate cannot execute a shell command.

### Link node

`link "NAME" { ... }` declares one directed dependency subject.

| Child | Accepted value | Default |
|---|---|---|
| `from "SUBJECT"` | One full subject | Required. |
| `to "SUBJECT"` | One full subject | Required. |
| `required BOOL` | One Boolean | `#true`. |
| `on-unreachable "VALUE"` | `hold` or `void` | `hold`. |

`from` and `to` must differ. A required link propagates the target reachability state to its source.

`void` is valid only when the source belongs to a temporary scope. An advisory link requires `required #false` and `hold`.

The validator refuses a direct or transitive required-link cycle. It returns every link subject in that cycle.

### Message node

`message "NAME" { ... }` declares one message structure subject.

It requires exactly one `to` and `content` child. An optional `from` child defaults to `requester`.

`from` and `to` each accept one subject string. `content` accepts one inline string or one `doc/` reference.

The recipient can be an agent or person subject. An agent uses native delivery, while a person message remains visible for an external interface.

An observation about person action records the interface as origin and the person as actor. st3 does not assign an agent to that message.

The default message lifecycle is open. Delivery, accepted, and closed state come only from claims.

A repeated unchanged message declaration is a no-op. A changed content value creates a new intent revision for the same message subject.

An empty recipient or content value is invalid. Version 1 KDL accepts no attachment or close-judge child.

An inline message is UTF-8 text of at most 4 KiB. A longer instruction must use a document reference.

### Document references

A document is posted before an intent references it:

```text
st3 doc put ./stage-1.md --as doc/release-work/stage-1
-> 4e81b7a0

content "doc/release-work/stage-1@4e81b7a0"
```

A `content` value can use `doc/NAME@HASH`. `HASH` is the exact token returned by `st3 doc put`.

The part before `@` must be a valid `doc/` subject. The reference cannot contain a filesystem path.

A hash-pinned reference names its immutable blob directly. A later binding for the same name does not change that reference.

Planning and apply always honor a valid hash-pinned reference when its blob is available.

Apply does not compare the hash with the selected name binding. It never uploads or rebinds a document.

A bare `doc/NAME` is valid authoring shorthand for the selected binding at the planning snapshot.

The plan response replaces every bare reference with `doc/NAME@HASH`. The published form always contains that hash.

`st3 run` applies the plan response. A binding change after planning does not change or invalidate the resolved reference.

The normalized message revision stores the document name and full blob hash. It never stores an unpinned name.

`st3 doc put` accepts valid UTF-8 text of at most 1 MiB. An intent KDL input can contain at most 16 MiB.

The `doc/` name is a resource role with kind `document`. Its binding claim records the selected blob hash.

Posting different bytes under the same document name creates a new binding claim. Prior bindings and message revisions remain visible.

Document text is opaque. It cannot declare subjects, create graph edges, or expand another document reference.

### Schedule node

`schedule "NAME" { ... }` declares one clock-owned message schedule.

`schedule "NAME" { stop }` disables that schedule. The bare stop must be its only child.

| Child | Accepted value | Default |
|---|---|---|
| `host "NAME"` | One host name | The API host. |
| `at "TIME"` | One absolute UTC time | No one-time occurrence. |
| `every "DURATION"` | One positive duration | No recurring interval. |
| `anchor "TIME"` | One absolute UTC time | Required with `every`. |
| `catch-up "VALUE"` | `all`, `latest`, or `skip` | `latest` for a recurring schedule. |
| `max-catch-up INTEGER` | A positive `u32` | Required with `catch-up "all"`. |
| `message { ... }` | One message template | Required. |

Exactly one of `at` and `every` must occur. `anchor` is invalid with `at` and required with `every`.

The one-time or recurring requirement does not apply to the stop form. The prior selected revision supplies its clock host.

`catch-up` and `max-catch-up` are invalid with `at`. `max-catch-up` is invalid with `latest` or `skip`.

The message template requires `to` and `content`. `from` defaults to `requester`.

The template takes no name because the schedule derives each occurrence subject.

The occurrence subject contains the schedule subject, selected revision, and occurrence number. The same occurrence can be delivered only once.

### Checkpoint sequence

`checkpoints "NAME" scope="SUBJECT" { ... }` declares one ordered checkpoint sequence subject.

The `scope` property is optional. When present, it must name one scope subject and tags every active checkpoint subject.

The sequence contains one or more `checkpoint "NAME"` children. Names must be unique inside the sequence.

A checkpoint name contains 1 through 160 characters. Authors use a sentence that states what is true when the checkpoint passes.

The first checkpoint is checkpoint zero. The last checkpoint is final by position.

A checkpoint accepts an optional `subgraph` child and one required `judges` child. It accepts no properties.

The name states the result, the subgraph asks for work, and the judges prove the result.

A wait-only checkpoint omits `subgraph`. Its judges can observe work that st3 cannot bring about itself.

A checkpoint `subgraph` publishes when that checkpoint becomes active. It contains complete desired state for each named subject.

The reconciler waits until the active subgraph holds before it evaluates non-deadline judges.

A native driver agent holds after it reports `ready`, `working`, or `idle`.

A message holds after delivery. A stop holds after the selected member or scope is not live.

The checkpoint deadline still starts at activation. It can expire while the active subgraph converges.

Work that needs an agent or person must include a message to that subject.

The planner reports an unaddressed-work blocker when it can prove this gap.

A direct exec or PTY action needs no message because the reconciler performs that declared lifecycle action.

Every checkpoint must contain a non-empty `judges` block. A checkpoint with a subgraph and no judges is invalid at publish time.

The validator refuses every node or property that the checkpoint grammar does not list.

The sequence moves only to the next checkpoint. It never activates a later subgraph before all earlier judges pass.

Concurrent stages use separate checkpoint sequences. KDL has no loop, repeat, branch, or backward-edge node.

For a temporary eval, a terminal pass, deadline failure, or void condition selects the last checkpoint.

The final checkpoint can publish `scope "NAME" { stop }`. Its judges must prove that the recorded scope membership is empty.

### Judges block

`judges { ... }` is a conjunction. Every pattern and running judge must pass before its checkpoint passes.

Cheap stored patterns run first. File observations run next, mechanical judges follow, and llm judges run last.

A failing ordinary judge leaves the checkpoint pending. A reached deadline gives a bounded checkpoint failure.

A running judge uses its declared name as its ID. Other judge IDs derive from their normalized node and source ordinal.

The block accepts these repeated nodes:

- `exists`, `empty`, `field`, `has`, and `lacks` pattern nodes;
- `deadline` clock patterns;
- running `judge` nodes.

It accepts no `subgraph` or desired-state node.

### Subject predicates

Every built-in pattern starts with its predicate. Its subject follows, and a comparison operator follows when needed.

```kdl
exists "resource/pull-request@release-work"
empty "scope/eval/team-review-42"
field "status" "agent/team.dev-42" is "idle"
field "title" "resource/pull-request@release-work" starts-with "Release"
field "labels" "resource/pull-request@release-work" contains "ready"
has "file/worker-1:/srv/work/release/LICENSE" "Permission is hereby granted"
lacks "message/team-42/result" "unfinished"
```

`exists "SUBJECT"` accepts one full subject and no properties or children. It tests current actual or observed state.

An intent declaration alone does not satisfy `exists`. A resource needs a current binding or observation, and a member needs current actual state.

`empty "SUBJECT"` accepts one full scope subject. It passes only when the recorded actual member set is empty.

`field "PATH" "SUBJECT" OPERATOR VALUE` accepts one dotted field path and one full subject.

A field path has one or more identifier segments separated by dots. A segment uses letters, digits, `_`, or `-` and starts with a letter.

The accepted field operators are `is`, `starts-with`, and `contains`. `is` performs exact normalized scalar equality.

`starts-with` accepts string operands. `contains` accepts a string substring or one exact array element.

Pattern values can be strings, Booleans, signed integers, or `#null`. Floating-point values are invalid in version 1.

A syntactically valid unknown field path is accepted. It does not pass until the current subject view supplies that field.

Member status fields accept `absent`, `starting`, `running`, `ready`, `working`, `idle`, `stopping`, `stopped`, `exited`, or `parked`.

Member reachability accepts `reachable`, `unreachable`, or `indeterminate`. Host status accepts `online`, `offline`, or `unknown`.

Host transport accepts `up`, `down`, or `unknown`. Account quota accepts `available`, `limited`, `exhausted`, or `unknown`.

Message status accepts `sent`, `delivered`, `accepted`, or `closed`. Scope retention accepts `persistent` or `temporary`.

`has "SUBJECT" "TEXT"` and `lacks "SUBJECT" "TEXT"` each accept two strings and no properties or children.

They operate only on a subject with one text content value. Version 1 supports file, document, and message subjects.

`has` tests for a substring. `lacks` tests its absence after a successful content read.

A missing subject or failed content read fails both predicates. It never makes `lacks` pass.

Every subject view derives from immutable claims at the fixed input index. A later relevant claim can invalidate a prior result.

A predicate queries current state, not raw history. Its judge result cites the exact claim IDs that built the selected view.

### File subject observations

A file subject has the form `file/HOST:ABSOLUTE-PATH`. It is a resource subject with kind `file`.

The first file predicate at one input index requests a read from the encoded host. No process judge is required.

The reader records `resource.file-observed` with the subject, path, mode, content hash, blob reference, time, and daemon origin.

A `field` predicate over a structured file parses its recorded content. Version 1 parses JSON files and uses dotted object paths.

Unreadable files, invalid structured content, and missing fields fail their predicates with a bounded reason.

A harness session file uses resource kind `harness.session-file`. Its PID and file identities keep their independent lifecycles.

### Deadline clock pattern

`deadline "DURATION"` accepts one positive duration and no properties or children. It can occur once in a judges block.

The duration starts when the checkpoint becomes active. Passing all other judges before expiry satisfies the deadline.

Expiry records a clock claim and fails the checkpoint. Restarting the daemon restores the same one-shot operation from its claims.

### Running judges

A mechanical judge has this complete shape:

```kdl
judge "NAME" {
  exec "SHELL COMMAND"
  host "HOST"
  workspace "PATH"
  env { NAME "VALUE" }
  time-limit "120s"
}
```

`exec`, `host`, and `workspace` are required and singular. `env` and `time-limit` are optional and singular.

The `exec` string runs through `sh -c`. Exit zero passes, and any other exit fails.

The time limit defaults to `120s`. The environment defaults to empty and replaces no inherited judge-runner variables.

An llm judge has this complete shape:

```kdl
judge "NAME" type="llm" {
  model "MODEL"
  host "HOST"
  workspace "PATH"
  tools "TOOL" "TOOL"
  env { NAME "VALUE" }
  token-budget 8192
  time-limit "10m"
  prompt "PROMPT"
}
```

The `type` property accepts only `llm`. A mechanical judge has no `type` property.

`model`, `host`, `workspace`, `tools`, `token-budget`, `time-limit`, and `prompt` are required and singular.

An optional singular `env` block uses the same string map as an agent environment. Its default is empty.

Version 1 registers `shell`, `git`, `gh`, and `network`. A host can advertise more tools through its capability registry.

`token-budget` is a positive `u64`. `time-limit` is a positive duration.

`model`, `host`, `workspace`, and `prompt` accept one string. The selected host must advertise the model and every declared tool.

An llm judge is a real headless agent. It receives the endpoint and one single-operation judgement capability.

It must run `st3 judgement pass|fail --reason TEXT`. It can add repeated `--evidence CLAIM-ID` arguments.

The runner does not parse standard output for a verdict. Exit without a matching judgement stays pending until the time limit.

Exceeding the token or time limit records `fail`. The runner then stops that judge incarnation.

Judge names must be unique inside one checkpoint. A running judge cannot omit its host or workspace.

### Complete validation refusals

The parser refuses every shape not accepted above. These cross-node refusals are also normative:

1. The input has no root subgraph, an empty root, multiple root nodes, or a nested checkpoint sequence.
2. One publish declares the same normalized subject more than once.
3. A checkpoint omits judges, contains empty judges, or contains a subgraph without judges.
4. A wait-only checkpoint contains a placeholder or empty subgraph.
5. A judges block contains desired state, or a subgraph contains a claim assertion.
6. A checkpoint mixes desired-state declarations with assertions in the same block.
7. A member has no launch, multiple compact launches, multiple drivers, or duplicate task names.
8. A task has both command and argv, has neither launch, or uses an unknown lifecycle value.
9. A render operation has an unknown property, a wrong argument count, invalid JSON, or both content forms.
10. A link has a cycle, a missing endpoint, an invalid policy, or `void` outside a temporary scope.
11. A schedule has an invalid time combination, catch-up combination, host, or message template.
12. A running judge lacks its placement, workspace, command, model limit, or required capability.
13. A pattern uses an unknown operator, a wrong scalar type, an invalid field path, or an unsupported subject type.
14. A direct stop targets an observed or structure subject. Scope and schedule stops must use their typed block forms.
15. Any singular field repeats, or any unknown node, property, child, type annotation, or extra argument occurs.
16. A document reference has an invalid name or hash, or its immutable blob is unavailable to the authorized reader.

Planning also reports unresolved referenced subjects and offline hosts. These are blockers or reachability states, not inferred declarations.

st3 never infers a member. An authoring skill or user interface must produce the same complete document that a person can publish.

### Plain agent example

The following three examples use the settled reference KDL.

The multi-stage comments include Nathan's later correction that a hash-pinned reference always names its immutable blob.

```kdl
// One agent. Nothing here is a default.
//
// A subgraph declares subjects and the properties they should have.
// Configuration is a property, so claude {} and render {} belong here.

subgraph {
  agent "release-owner" {
    host "worker-1"
    workspace "/srv/work/release"

    meta {
      user-scope "team"
      shared-ownership #true
      merge-policy "review-required"
    }

    env {
      PATH "/opt/team-tools:$PATH"
    }

    claude {
      model "opus"
      effort "xhigh"
      dev-channels #true
      prompt "Run the boot ritual, read the inbox, and own release work."
      args "--dangerously-skip-permissions" "--remote-control" "worker-1.release-owner"
    }

    render {
      copy "_templates/HANDBOOK.worker.md" ".st2/HANDBOOK.md"
      copy "_templates/team.AGENTS.md" "AGENTS.md"
      copy "_templates/wrappers/git" ".st2/bin/git" executable=#true
      copy "_templates/wrappers/gh" ".st2/bin/gh" executable=#true
      git-exclude ".st2" "AGENTS.md"
    }
  }
}

// The subject is agent/worker-1.release-owner.
// Publishing this again unchanged records nothing.
//
// Every field here is one this agent needs. Defaults are not written down.
```

The declaration uses the current agent shape. Omitted restart, shutdown, supervisor, and delivery values use their documented defaults.

### Multi-stage plan example

```kdl
// A plan over one agent that already exists and a pull request that does not.
//
// Each checkpoint does three things. Its name says what is true when it is
// done. Its subgraph asks for the work. Its judges say how we know.

subgraph {
  resource "pull-request@release-work" {
    kind "vcs.pull-request"
  }

  checkpoints "checkpoint/release-work" {

    checkpoint "the release owner has accepted the work" {
      subgraph {
        message "release-work/kickoff" {
          to "agent/worker-1.release-owner"
          content "doc/release-work/brief@9f2a3c1d"
        }
      }
      judges {
        field "status" "message/release-work/kickoff" is "accepted"
      }
    }

    checkpoint "the release owner has opened a pull request" {
      subgraph {
        message "release-work/stage-1" {
          to "agent/worker-1.release-owner"
          content "doc/release-work/stage-1@4e81b7a0"
        }
      }
      judges {
        exists "resource/pull-request@release-work"
        field "title" "resource/pull-request@release-work" starts-with "Release"
      }
    }

    // No ask. CI runs without being asked, so this waits on the outside world.
    // The agent publishes what it sees.
    checkpoint "the tests pass on the current revision" {
      judges {
        field "ci.status" "resource/pull-request@release-work" is "success"
      }
    }

    checkpoint "the licence is still MIT" {
      judges {
        has "file/worker-1:/srv/work/release/LICENSE" "Permission is hereby granted"
        lacks "file/worker-1:/srv/work/release/LICENSE" "proprietary"
        field "license" "file/worker-1:/srv/work/release/package.json" is "MIT"
      }
    }

    checkpoint "the pull request can merge and a reviewer approves the change" {
      judges {
        field "mergeable" "resource/pull-request@release-work" is "true"

        judge "the change is good" type="llm" {
          model "claude-sonnet-5"
          host "worker-1"
          workspace "/srv/work/release"
          tools "shell" "git" "gh"
          token-budget 8192
          time-limit "10m"
          prompt "Read the diff of pull-request@release-work and run its tests. Post your verdict with st3 judgement."
        }
      }
    }
  }
}

// A document is posted before it is referenced:
//
//     st3 doc put ./stage-1.md --as doc/release-work/stage-1   ->  4e81b7a0
//
// A pinned reference always names that exact blob, whatever doc/release-work/
// stage-1 points at now. A new version is a new hash: post, take the hash,
// update the reference. st3 doc put warns when the local file no longer
// matches the hash the plan cites.
//
// Concurrent stages are separate documents and separate checkpoints.
//
// field on a structured file parses it. field on a resource reads the state
// built from claims about that resource.
```

The resource name is a role. A resource observation later ties it to the concrete pull request and its current fields.

The first two checkpoints ask the owner through messages. The observation-only checkpoints contain no subgraph.

Each `doc/` reference selects a previously posted immutable blob. The LLM judge posts its verdict through the typed API.

### Team eval example

```kdl
// Three agents on three hosts that message each other.
//
// It starts by asserting the scope is empty and ends by setting it empty
// again, so teardown is a checkpoint rather than a mechanism.

subgraph {
  supervisor "team-eval" {
    gate "claude-workspace-trust" driver="claude" {
      contains "Quick safety check: Is this a project you created or one you trust?"
      key "enter"
      max-inputs 1
    }

    gate "claude-development-channel" driver="claude" {
      contains "WARNING: Loading development channels"
      contains "Channels: server:st3"
      key "enter"
      max-inputs 1
    }
  }

  link "eval-requires-supervisor" {
    from "scope/eval/team-review-42"
    to "agent/team.sup-42"
    required #true
    on-unreachable "void"
  }

  checkpoints "checkpoint/team-review-42" scope="scope/eval/team-review-42" {

    // Checkpoint zero. Its judges decide whether this may start at all.
    checkpoint "the scope is empty and this may start" {
      judges {
        empty "scope/eval/team-review-42"
      }
    }

    checkpoint "three agents are running and the work has been asked for" {
      subgraph {
        scope "eval/team-review-42" retention="temporary" {
          agent "team.sup-42" {
            host "control-1"
            workspace "./fixture/sup"
            supervisor "team-eval"
            claude {
              model "claude-sonnet-5"
              effort "medium"
              args "--permission-mode" "bypassPermissions"
              prompt "Accept message/team-42/kickoff. Send message/team-42/delegate to team.dev-42. Close both after the work returns."
            }
          }

          agent "team.dev-42" {
            host "worker-1"
            workspace "./fixture/work"
            supervisor "team-eval"
            claude {
              model "claude-sonnet-5"
              effort "medium"
              args "--permission-mode" "bypassPermissions"
              prompt "Act on message/team-42/delegate. Implement the task. Send message/team-42/review to team.review-42, then close the delegate message."
            }
          }

          agent "team.review-42" {
            host "worker-2"
            workspace "./fixture/work"
            supervisor "team-eval"
            claude {
              model "claude-sonnet-5"
              effort "medium"
              args "--permission-mode" "bypassPermissions"
              prompt "Review message/team-42/review. Run the tests. Send message/team-42/result to team.sup-42 and close the review."
            }
          }
        }

        message "message/team-42/kickoff" {
          from "requester"
          to "team.sup-42"
          content "doc/team-review-42/task@b30f5ce2"
        }
      }
      judges {
        field "status" "agent/team.sup-42" is "ready"
        field "status" "agent/team.dev-42" is "ready"
        field "status" "agent/team.review-42" is "ready"
      }
    }

    // No ask. The kickoff above is what set this in motion, and three agents
    // talking to each other is not something the reconciler can do.
    checkpoint "the work has been delegated, reviewed and reported" {
      judges {
        deadline "20m"

        field "status" "message/team-42/delegate" is "closed"
        field "status" "message/team-42/review" is "closed"
        field "status" "message/team-42/result" is "closed"

        field "status" "agent/team.sup-42" is "idle"
        field "status" "agent/team.dev-42" is "idle"
        field "status" "agent/team.review-42" is "idle"

        judge "the requested result is correct" type="llm" {
          model "claude-sonnet-5"
          host "worker-1"
          workspace "./fixture/work"
          tools "shell" "git"
          token-budget 8192
          time-limit "5m"
          prompt "Read the work in this tree and the message chain. Decide whether the requested task was done correctly. Post your verdict with st3 judgement."
        }
      }
    }

    checkpoint "the scope is empty again" {
      subgraph {
        scope "eval/team-review-42" { stop }
      }
      judges {
        empty "scope/eval/team-review-42"
      }
    }
  }
}

// scope "..." { stop } is one claim about the scope subject.
//
// It is idempotent, and a machine that was offline during teardown cleans up
// its own members when it returns.
```

The first checkpoint asserts empty state and changes nothing. The second publishes the complete team configuration and kickoff document.

The coordination checkpoint only observes work. The final checkpoint publishes and proves scope teardown.

## One-shot migration rewrite

The server accepts only the new declaration format. It has no compatibility adapter for st2 catalogs or evals.

A temporary repository script rewrites the current catalog declarations and evals before cutover.

The script preserves `host`, `workspace`, `supervisor`, `render`, `env`, `meta`, every driver field, tasks, `ding`, and restart intensity.

It omits values that equal the documented st3 defaults. It writes every non-default restart type, shutdown timeout, and intensity field.

The reviewed one-shot and fault-injector members receive `restart "never"`. It converts each retired declaration to an explicit stop publish.

For evals, it converts teams to scopes, kickoff messages to message subjects, and ordered run steps to checkpoints.

It converts each old run step to a standalone exec member. It preserves workspace, environment changes, retry behavior, and exit expectations.

It keeps native driver blocks as native driver blocks. It does not infer a driver from an opaque shell command.

The source owner must replace each opaque provider command with a typed driver block before the final migration.

It splits each old stage into a desired `subgraph` and an assertion-only `judges` block.

It removes a subgraph when the checkpoint only waits. It writes the scope stop as the last checkpoint subgraph.

It adds an explicit completion checkpoint after the team starts. Every named worker must report before the supervisor confirms completion.

It converts file checks to `has` and `lacks` over full file subjects. It converts JSON checks to `field` predicates.

It moves the old eval deadline to `deadline` inside the applicable judges block.

It posts long message text under deterministic `doc/` names and writes each returned hash into the converted KDL.

The parity report compares each source text with its posted blob.

It preserves every mechanical `exec` shell command. It adds the explicit host and workspace that ran the old judge.

It converts each `ask` judge to an llm judge with the same model, tools, prompt, token budget, and time limit.

The converted prompt tells the judge to post through `st3 judgement`. No adapter parses its process output.

The script writes new-format files beside a generated parity report. The old files remain in git as the before state.

The parity check compares normalized subjects, render output, commands, environments, tasks, all restart controls, messages, and judge behavior.

After parity passes, delete the rewrite script. The committed new-format files become the only import and eval input.

`st3 import ./catalog` and `st3 eval ./eval` then read those explicit new-format folders.

During a no-downtime cutover, `st3 up --pty-root PATH` observes the existing PTY registry. A matching desired member adopts that exact runtime incarnation.

The daemon passes the selected `PTY_ROOT` to each new member. It keeps the st3 claim store separate from the old catalog and state roots.

## `st3 claude`

### Command

```text
st3 claude [--name NAME] [--worktree PATH]
            [--model MODEL] [--effort EFFORT]
            [--endpoint SOCKET] [--json]
```

The default worktree is the current directory. The command sends that explicit path and does not scan its parent.

The default name is a generated local name. A supplied name finds or creates the same stable member subject.

The server applies these defaults:

- The desired state is running.
- The placement is the local host.
- The supervisor is the implicit root.
- The restart type is `always`.
- The restart intensity uses the current st2 default fields.
- The shutdown timeout is five seconds.
- The driver is Claude.
- The root supervisor gets the versioned Claude gate profile that ships with this st3 binary.

### End-to-end path

```text
st3_claude(args):
    api = connect_local_unix_socket(args.endpoint)
    subject = stable_claude_subject(args.name) if args.name else new_subject()
    existing = api.status(subject = subject)
    expected = existing.intent_token if existing else empty_token(subject)

    created = api.post_claude({
        subject: subject,
        worktree: canonicalize_explicit_path(args.worktree),
        model: args.model,
        effort: args.effort,
        expected_subject: expected,
        idempotency_key: stable_claude_key(args)
    })

    cursor = created.event_cursor
    while true:
        event = api.next_event(cursor, subject = created.subject)
        cursor = event.store_index

        if event is subject.unreachable:
            print(event.reason)
            exit 4

        if event is harness.ready for created.subject:
            break

    attachment = api.attach_session({
        subject: created.subject,
        rows: terminal.rows,
        columns: terminal.columns
    })

    enter_raw_mode()
    proxy_recorded_terminal_websocket(
        attachment.path,
        attachment.capability,
        created.subject,
        attachment.incarnation_id
    )
    restore_terminal_mode()
```

`POST /v1/claude` expands the request into one ordinary named desired state. It does not create a second lifecycle path.

The apply transaction appends the desired subject claim. That claim starts reconciliation.

The reconciler observes no process and requests one PTY start. The PTY driver appends a `pid.observed` result.

The declared Claude driver grants access to its session files. It publishes the typed native harness claims when their records appear.

The event stream wakes the CLI. The CLI does not poll process state or inspect a session file.

The attach endpoint rechecks the exact incarnation. It returns a capability bound to that subject and incarnation.

The WebSocket sends each input frame through the recorded terminal write path. It never writes directly to the PTY.

Terminal output is an observation stream. The PTY transcript keeps its existing history and logging policy.

Terminal disconnect does not publish stop intent. The agent remains supervised and keeps its history.

If the process dies, the runtime emits an exit claim. The restart type and intensity determine the next action.

A later `st3 claude --name NAME` plans an idempotent desired subject and attaches to the current incarnation.

## Host replication and partitions

Each host accepts authorized writes and keeps the claim set for the subjects and scopes that it verifies.

It retains every replica batch header and claim hash needed to verify the chain. It can omit unselected private bodies after verification.

Every node serializes its own replica chain. Placement hosts publish actual-state claims for their local members.

Peer exchange uses replica chains:

```text
replicate(peer):
    local_heads = store.replica_heads()
    stream = POST peer /v1/peer/claims/query { replica_heads: local_heads }

    for batch in stream:
        verify_configured_replica_label(peer, batch.replica_id)
        verify_origin_authority(batch.origin, batch.claims)
        verify_previous_replica_batch(batch)
        verify_subject_predecessors_or_request_missing(batch)
        verify_claim_hashes(batch)
        insert_replica_batch_idempotently(batch)
        dispatch_state_bearing_claims(batch)
```

The receiver assigns a local ingestion index. It preserves the sender's replica sequence and chain.

The peer adapter reports a connection failure. It appends `transport.peer state="down"` once per connection transition.

st3 does not infer a peer drop from elapsed time. It does not poll a sleeping peer.

During a partition, each host reconciles its local members from the last accepted intent. It
records remote actual state as unknown after a transport drop.

An authorized host can publish intent while partitioned. The local subject token guards against a locally stale write.

Replication is bidirectional. After reconnection, different-subject revisions combine directly.

Concurrent leaves for one subject remain visible. Every host selects the same winner and continues reconciliation.

An authorized later desired-state change cites all current leaves and removes the conflict. Progress does not require that change.

The host does not stop healthy local work because a remote host is absent. A required link can make only its dependent subgraph unreachable.

## Failure handling

| Failure | Recorded result | Next behavior |
|---|---|---|
| Locally stale subject write | No claim | Return `409 stale-subject`. |
| Concurrent same-subject writes replicate | Winning and losing leaves | Select the deterministic winner and expose all leaves. |
| Daemon crash after action request | Durable `action.requested` | Reinspect and adopt or complete the same action. |
| Runtime exits | `pid.observed` with exit reason | Apply the restart type, then the intensity accounting. |
| Shutdown timeout expires | `deadline.reached` | Reinspect and kill only the recorded incarnation. |
| Restart budget is exhausted | Member unreachable with crash reason | Stop retries and propagate required links. |
| Gate remains after three inputs | Supervision decision and unreachable reason | Raise once and take no more input. |
| Judge exceeds a limit | `judge.result` fail | Keep the reason and stop the judge operation. |
| Required eval member is unreachable | Eval verdict `void` | Select the final stop and report `cannot reach intent` until cleanup succeeds. |
| A scheduled time arrives | `clock.reached` | Create one idempotent message occurrence. |
| A configured peer drops | `transport.peer` down | Continue local intent and mark remote actual state unknown. |
| Missing replica sequence | Replica batch rejected | Request the exact missing replica range. |
| Reducer cache is lost | No truth is lost | Replay claims through the requested index. |

## Security properties

Version 1 is not secure by default.

The local socket uses mode `0600`. The peer port has no TLS, peer authentication, or ACL enforcement.

An operator must bind the peer port only on a trusted private network or leave it disabled.

TLS and ACL support are planned. They remain outside version 1.

st3 does not know which network product carries the traffic. A later access layer can use node identities supplied by that network.

Version 1 cannot hide held-out judge definitions from another process that runs as the same local operating-system user. The local user remains trusted.

A later authenticated query view will withhold judge prompts, executables, and private evidence selectors from worker identities.

The daemon database and judge bundle stay outside worker workspaces. The judge runner receives them through a separate capability.

Mechanical judges run a held-out shell command in their declared host, workspace, and environment.

LLM judges are headless agents. They can read diffs, run tests, use declared tools, and publish claims allowed for their origins.

Each LLM judge gets one capability for its exact definition hash. That capability can post one idempotent judgement result.

Every llm judge has a token budget and a time limit. Version 1 adds no other judge-only execution restriction.

The project still wants sandboxes for every agent type, including workers and judges. A later common sandbox design supplies them.

The clock adapter can publish only occurrences for a current schedule that names its host.

Gate actions use declared literal screen text and declared fixed keys. A model cannot invent terminal input.

Attach capabilities expire quickly and bind to one subject, incarnation, and local user.

The server validates bundle paths, file modes, sizes, hashes, and link targets before it stores an eval bundle.

Document blobs inherit the message access policy. A file observation blob inherits its resource and judge evidence policy.

Replication sends a private blob only to an authorized subject reader. Other peers receive its hash and claim metadata.

Terminal input blobs use a user-scoped encryption key outside SQLite. Replication sends their hashes and metadata unless subject policy allows the private blob.

## Delivery sequence

The implementation should follow the vertical checkpoints in the source plan.

1. Build SQLite, the local API, subject publishes, one PTY driver, and `st3 claude`.
2. Add desired, actual, gap, plan, status, and per-subject compare-and-swap behavior.
3. Add state-bearing dispatch, message lifecycles, scheduled messages, peer-drop claims, and startup recovery.
4. Add decision claims, the cheap ladder, the bounded residue model, restart controls, links, and gate actions.
5. Add checkpoint progression and both bounded judge types.
6. Write the temporary migration script, rewrite the eval corpus, and prove parity for existing `ask` judges.
7. Add generic peer replication, offline subject writes, import each rewritten declaration, and prove graph parity before cutover.

Each step must end with the source plan's stated checkpoint. st2 remains the live reconciler until the final parity proof.

## Non-goals

The first release has no TUI. A later TUI uses the same API.

st3 never infers a member. Every published checkpoint names each agent, exec, and PTY that its subgraph needs.

Authoring help lives outside st3. A skill or later interface button produces the same fully specified KDL document as a person.

st3 does not execute plan loops or repetition. An authoring tool expands them into finite documents and checkpoints before publish.

The design has no catalog watcher, periodic sweep, polling loop, open-ended restart language, child spec, or supervision tree.

The design does not require an st2 behavior change. st2 remains separate until migration completes.

Version 1 schedules support fixed UTC times and intervals. Calendar and cron expressions are later intent syntax.
