# st2 to st3 drop-in replacement gaps

This chart compares st2 with st3 at commit `c0f6561` on 2026-08-29.

The chart uses the current source as the implementation authority.
The st3 design document describes some behavior that the source does not implement.

No eval ran during this audit.

## How to read the chart

`Parity` means that st3 has an operational replacement for normal use.

`Deliberate` means that st3 selected a different model.
Nathan can accept the difference or request a compatibility layer.

`Unfinished` means that the st3 direction needs more implementation.
Some unfinished fields parse successfully but have no runtime effect.

`New` means that st3 adds behavior and has no st2 compatibility duty.

Effort is engineering time for code, focused tests, and short documentation.
It assumes one engineer who knows both systems.

| Effort | Estimate |
|---|---:|
| XS | Less than one day |
| S | One to two days |
| M | Three to five days |
| L | One to two weeks |
| XL | More than two weeks |

The estimates overlap where one implementation closes more than one row.
Do not add every row to calculate a schedule.

## Decision summary

st3 is not a command-compatible replacement for st2.
Its API and claims store intentionally replace the watched filesystem catalog.
A shell script that changes `st2` to `st3` will not work.

The common native-agent path exists.
st3 can import KDL, start Claude or Codex agents, render files, exchange messages, and supervise processes.

The largest unfinished runtime gaps are these items:

- `keep`, nested resources, streams, `deliver`, `ding`, and `meta` parse but have incomplete or no runtime effect.
- Suspended and retired lifecycle states both become a generic stopped subject.
- Generic DING delivery lacks the st2 retry, deferral, and staged-ownership rules.
- The message CLI lacks sent history, explicit retry keys, time filters, and real tree output.
- Render lacks fleet ownership checks, tracked-file protection, variable expansion, and full preflight.
- Driver child processes do not have a proved reap boundary on every agent exit.
- st3 still depends on st2 for the approved Claude channel installation and shared Codex control code.
- The operator surface lacks a complete task inventory, targeted unpark, and full catalog diagnostics.

A practical current-fleet cutover is smaller than full product parity.
It needs the active declaration audit, driver cleanup, render safety, message migration, and operator recovery commands.

An exact drop-in product needs compatibility commands and the retired st2 features.
That program is approximately 12 to 18 engineer-weeks after the direction decisions below.

## CLI command map

This table names every top-level st2 command.
Nested command rows follow it.

| st2 command | st3 replacement now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `ls` | `agents`, `status`, and `plan` | Unfinished | No command discovers a source tree and reports every parse warning. Add an offline discovery view over a migration input. | S |
| `up` | `up` | Parity | Both commands run a long-lived reconciler. st3 uses API events instead of a folder watcher and timer. | None |
| `up --once` | None | Unfinished | Add one bounded reconcile pass with a stable result. It must not leave the API or background readers running. | M |
| `up --materialize-only` | None | Unfinished | Add a render plan and apply command that starts no member. Reuse document resolution and render safety. | M |
| `up --host` | `up --node` plus member placement | Parity | The daemon node selects members whose resolved host matches that node. | None |
| `up --agent` and `up --task` | Subject intent plus `wait` | Deliberate | st3 selects graph subjects, not catalog files. Add compatibility selectors only if operators need the old shortest path. | S |
| `up --interval` | None | Deliberate | st3 reconciles committed claims and daemon recovery events. Add only a low-rate repair trigger if event loss becomes possible. | S |
| `message` | `message` | Parity | The main send, list, read, reply, archive, and thread names exist. Their detailed gaps appear below. | None |
| `event` | None | Unfinished | Claims do not implement declared event ingress, producer IDs, deduplication, supersession, or event receipts. Add a typed ingress API and delivery reducer. | L |
| `stream` | None | Unfinished | Parsed stream blocks do not create ingress endpoints or adapter members. Add self-authoring, authority checks, and supervised adapter subjects. | L |
| `request` | None | Deliberate | st3 uses typed claims, messages, work, and review requests. Decide whether the old service-principal transport remains a product requirement. | Decision |
| `context` | `context` | Parity | Read, write, and append exist on immutable document versions. One option gap appears below. | None |
| `resource` | `resource` | Parity | Dynamic resources exist as claims. Their identifiers and JSON shapes differ. | None |
| `service` | `service` | Parity | Both manage a Linux systemd user service. st3 uses a config file and a fixed 1 GiB limit. | None |
| `claude-channel` | `st2 claude-channel` | Unfinished | st3 verifies and uses the channel, but it cannot install it. Move the shared installer behind an st3 command. | M |
| `hooks` | None | Deliberate | Native st3 drivers keep state inside the wrapper. Add a compatibility hook manager only for opaque launches that still need workspace hooks. | M |
| `driver` | Hidden `driver` plus `plan` | Unfinished | Runtime wrappers exist, but no supported public expansion or inspection command exists. Add stable driver diagnostics and expansion output. | M |
| `ding` and `ping` | Automatic generic terminal delivery | Deliberate | st3 has no standalone inbox sidecar. The reconciler writes a terminal notice and records delivery once. Full safety needs the DING work below. | L |
| `codex-app-server` | Hidden `driver codex` | Parity | st3 reuses the controlled Codex path. Cleanup and version gaps appear in the driver table. | None |
| `claude-mcp` | Hidden `driver claude-mcp` | Parity | st3 supplies its own channel server through the hidden driver entry. | None |
| `status` | `status` | Unfinished | Get and set exist. Add `away`, derived `unknown` rules, and bare agent-identity normalization on reads. | S |
| `rename` | Edit `name` through `run` | Unfinished | No targeted authoring command exists. Add a compare-and-swap patch command with the st2 length and ownership rules. | S |
| `describe` | Edit `description` through `run` | Unfinished | No targeted authoring command exists. Share the presentation patch path with `rename`. | S |
| `agent` | `plan` and `run` | Deliberate | Subject-token compare-and-swap replaces file digests and targeted publication. A compatibility wrapper can preserve old receipts. | M |
| `catalog` | `doc`, `plan`, `run`, `import`, and claim history | Deliberate | The claims store replaces declaration-root transactions. Exact snapshot and repair gaps appear below. | L |
| `down` | Publish member stops or a scope stop | Deliberate | st3 never ties member lifetime to daemon lifetime. Add a helper that plans explicit stops for one selected host or scope. | S |
| `env` | `ST3_ENDPOINT` and config | Unfinished | No command prints shell exports for the selected daemon and PTY registry. Add a quoted export command. | XS |
| `pretrust` | Native driver startup | Deliberate | Typed drivers own trust and channel setup. Add an operator command only for opaque harness commands. | S |
| `eval` | `eval` | Unfinished | The version-2 runner exists. Complete opaque-driver migration and decide whether old host and retained-run options need compatibility forms. | L across corpus |
| `pty` | `pty ls`, `attach`, `peek`, `send`, `signal`, and `ui` | Parity | The normal operator actions exist. st3 does not pass arbitrary lower-level PTY arguments through. | S |
| `shell` | None | Unfinished | Add a shell wrapper that exports the endpoint and PTY registry. | XS |
| `validate` | `plan` | Unfinished | `plan` validates one intent. It does not validate a complete source tree, host paths, render ownership, or installed driver dependencies. | M |
| `doctor` | `doctor` | Unfinished | st3 checks the store, state, PTY, isolation, ownership, drift, and drivers. It lacks several catalog and task classifications. | M |
| `agents` | `agents` | Unfinished | Basic status and reachability exist. Presentation, resources, inbox count, desired state, last activity, and stable st2 JSON are incomplete. | M |
| `tasks` | `status`, `inspect`, and `trace` | Unfinished | No fail-closed desired-task and runtime-generation snapshot exists. Add one versioned inventory response and CLI view. | M |
| `unpark` | None | Unfinished | Fail-mode restart can park a member, but no targeted reset command exists. Add an incarnation-fenced restart-reset claim and endpoint. | S |
| `completions` | `completions` | Parity | st3 supports Bash, Zsh, and Fish. Add other Clap shells only if they are used. | XS |

### Message subcommands

| st2 subcommand | st3 replacement now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `message send` | `message send` | Unfinished | Basic fields match. st3 generates a fresh retry key and has no caller-supplied `--idempotency-key`. Add the option and preserve exact retries. | S |
| `message reply` | `message reply` | Unfinished | Basic threading works. Add caller retry keys and match st2 subject normalization. | S |
| `message ls` | `message ls` | Unfinished | Count and sender filtering exist. `--archive` currently includes open messages. Add exact archive selection, time filters, bodies, orphan recovery, and coverage rules. | M |
| `message sent` | None | Unfinished | Add a sender history query with complete or partial coverage metadata. Peer replication must preserve the sender index. | M |
| `message read` | `message read` | Unfinished | `--archive` closes an open message instead of selecting archived input. Restore selection semantics and the raw Markdown envelope. | S |
| `message archive` | `message archive` | Parity | st3 records accepted and closed lifecycle claims instead of moving a file. The normal user result is equivalent. | None |
| `message thread` | `message thread` | Unfinished | st3 finds the transitive thread. The `--tree` flag is accepted but the CLI still prints the flat JSON list. | S |
| Filesystem message import | `message export` only | Unfinished | Export creates a compatibility mailbox. No importer preserves existing IDs, threads, archives, receipts, and sent history. | L |

### Event, stream, and request subcommands

| st2 subcommand | st3 replacement now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `event emit` | None | Unfinished | Add recipient stream validation, producer event IDs, key supersession, durable receipts, and message delivery. | L |
| `stream add` | None | Unfinished | Add an authority-checked intent patch and optional adapter launch. The adapter needs a stable subject and restart policy. | M |
| `stream rm` | None | Unfinished | Add an authority-checked removal that stops only the owned adapter and preserves unrelated intent. | M |
| `request send` | Claims or `message send` | Deliberate | Select a replacement request schema before implementation. A compatibility service can translate requests to claims. | Decision |
| `request read` | Claim or message query | Deliberate | A translator must preserve the typed JSON envelope and declared principal authority. | S after decision |
| `request reply` | Claim or message lifecycle | Deliberate | A translator must preserve reply-once behavior and the original idempotency key. | S after decision |
| `request status` | `status` or `inspect` | Deliberate | A compatibility query must map the replacement claim to the old pending or replied union. | S after decision |

### Context and resource subcommands

| st2 subcommand | st3 replacement now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `context read` | `context read` | Unfinished | Decisions and full output exist. Add `--fresh-within` and preserve its empty-result behavior. | XS |
| `context write` | `context write` | Parity | st3 stores a new immutable `now` document version. | None |
| `context append` | `context append` | Parity | st3 stores each decision as an immutable document. | None |
| `resource add` | `resource add` | Unfinished | URL, title, tags, and relation exist. Add body input and a stable compatibility receipt if old scripts need it. | S |
| `resource ls` | `resource ls` | Unfinished | The normal list exists. Add the st2 JSON shape and declared-resource merge. | S |
| `resource read` | `resource read` | Parity | st3 returns the selected resource subject and claim data. | None |
| `resource remove` | `resource remove` | Parity | st3 records removed state instead of deleting history. | None |

### Service, setup, and driver subcommands

| st2 subcommand | st3 replacement now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `service install` | `service install` | Unfinished | The main behavior matches. Add an adjustable memory limit and a dry-run unit view if exact operator parity matters. | S |
| `service status` | `service status` | Parity | Both call the systemd user manager. | None |
| `service uninstall` | `service uninstall` | Parity | Both disable and remove the user unit. | None |
| `claude-channel install` | Use the st2 installer | Unfinished | Expose the shared approved-plugin installer through st3 and keep the development-channel fallback explicit. | M |
| `claude-channel status` | Driver startup verification | Unfinished | Add a read-only st3 setup check with stable diagnostics. | S |
| `claude-channel uninstall` | Use the st2 installer | Unfinished | Expose removal through st3 after ownership moves to the st3 package. | S |
| Hidden `claude-channel install-policy` | Use the st2 installer | Unfinished | Keep this as an internal operation when the installer moves to st3. | S shared |
| Hidden `claude-channel uninstall-policy` | Use the st2 installer | Unfinished | Keep policy cleanup under the same owned installer transaction. | S shared |
| `hooks install` | None | Deliberate | Native wrappers do not need the old workspace hook set. A compatibility mode needs versioned publication and receipts. | M |
| `hooks verify` | Driver checks in `doctor` | Deliberate | Add only when compatibility hooks remain supported. | S |
| `hooks verify-own` | None | Deliberate | Add only when st3 publishes immutable hook sets. | S |
| `driver expand` | `plan --json` | Unfinished | Planning shows normalized intent, but it does not print one lowered Agent Spec. Add a stable driver expansion view. | S |
| `driver codex` | Hidden `driver codex` | Parity | The wrapper exists. Protocol and cleanup gaps remain. | None |
| `driver claude-mcp` | Hidden `driver claude-mcp` | Parity | The wrapper exists as an implementation command. | None |
| Hidden `driver claude` | None | Deliberate | st2 keeps this deprecated alias. st3 can omit it after all rendered configurations migrate. | None |
| `driver claude-session` | Hidden `driver claude` | Parity | The st3 wrapper records readiness, presence, delivery, and terminal state. | None |
| `driver claude-observe` | Internal st3 channel observations | Deliberate | st3 does not expose the old hook event ingestion command. | None |
| `driver pi-session` | Hidden `driver pi` | Unfinished | The wrapper and channel exist. Complete a real provider lifecycle and delivery proof. | M |
| `driver pi-channel` | Hidden `driver pi-channel` | Unfinished | The channel exists. Prove restart recovery, duplicate delivery, and terminal cleanup. | M |
| `driver opencode-session` | Hidden `driver opencode` | Unfinished | The wrapper and server bridge exist. Complete a real provider lifecycle and delivery proof. | M |

### Agent and catalog subcommands

| st2 subcommand | st3 replacement now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `agent desired-state` | Publish `stop` or running intent | Deliberate | st3 uses desired revisions. Exact parity needs suspended and retired states, reasons, and targeted authoring receipts. | M |
| `agent digest` | `plan` source hash and subject tokens | Deliberate | The token model replaces the file capability hash. A compatibility wrapper can print the old digest shape. | S |
| `agent publish` | `run` | Deliberate | One subgraph publish can change many subjects. Exact parity needs a single-agent input guard and old receipt shape. | S |
| `catalog digest` | `plan` source hash | Deliberate | st3 hashes normalized desired state, not a declaration-root filesystem projection. | None |
| `catalog diff` | `plan` | Parity | Planning returns normalized changes, predicted actions, blockers, and subject tokens. | None |
| `catalog bootstrap` | `import` into an empty store | Unfinished | Add an empty-store guard and one typed bootstrap receipt. | S |
| `catalog snapshot` | Claim and document queries | Unfinished | Add a canonical version-2 KDL exporter at one store index. Include every referenced document hash. | L |
| `catalog apply` | `run` or `import` | Deliberate | Subject-token transactions replace declaration-root compare-and-swap. Exact repair and resume semantics need a compatibility transaction service. | L |

### New st3 CLI surfaces

These commands are st3 additions or graph-native replacements.
They do not have direct st2 command contracts.

| st3 command | Purpose |
|---|---|
| `claude` and `codex` | Publishes one quick native agent and attaches to its terminal. |
| `preview` and `run` | Previews or applies explicit version-2 intent with subject tokens. |
| `plan start`, `show`, `preview`, `submit`, `revise`, `approve`, and `cancel` | Operates a durable Codex planning session and exact-hash review. |
| `import` | Combines and applies one version-2 KDL tree and its staged documents. |
| `exec` and `logs` | Runs one graph exec member and reads its current or prior log. |
| `inspect` and `trace` | Reads one subject and its immutable claim history. |
| `wait` | Waits for a subject condition, checkpoint, or eval verdict. |
| `doc put`, `doc get`, and `doc list` | Manages immutable document versions and selected name bindings. |
| `claim` | Publishes one registered typed observation. |
| `review approve`, `reject`, and `revise` | Records a human review decision on a resource. |
| `work ls`, `show`, `claim`, `renew`, `progress`, `complete`, `fail`, and `release` | Operates durable plan-step leases and results. |
| `work publish-plan` and `work revise` | Publishes a produced plan or a reviewed plan revision. |
| `gate-result` | Posts a capability-bound running gate result. |

## Declaration format gaps

The migration tool handles one-time syntax changes.
It does not make every accepted field operational.

| st2 declaration surface | st3 state now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| KDL version 1 and top-level `agent` | Version 2 with one `subgraph` | Deliberate | `st3-migrate file` and `catalog` wrap declarations and rewrite lifecycle intent. Keep this as a migration boundary. | None |
| TOML and JSON Agent Specs | KDL only | Deliberate | The catalog migrator discovers these specs but emits only KDL declarations. Add an explicit conversion or refusal report. | M |
| `identity` | Implemented | Parity | st3 resolves the canonical `agent/<host>.<identity>` subject. | None |
| `name` | Preserved in intent and member display | Unfinished | Expose it in the roster. Match st2 Unicode character limits instead of the current UTF-8 byte limit. | S |
| `description` | Preserved but not presented | Unfinished | Expose it through status, agents, and inspect. Match the st2 Unicode character limit. | S |
| `role` | Preserved but not interpreted or presented | Unfinished | Expose declared metadata in the roster. Keep runtime selection independent from role. | S |
| `type "service"` | Implemented | Parity | Both reject the retired batch type. | None |
| `host` | Implemented | Parity | `local` resolves to the receiving API node. | None |
| `workspace` | Implemented | Parity | st3 uses it for rendering, launch, and default task working directories. | None |
| `supervisor` agent identity | Supervisor policy subject | Deliberate | The same word has a different meaning. Rename one surface or migrate the old crash recipient into an explicit alert link. | M |
| `desired-state` with running, suspended, or retired | Running intent or generic stop | Deliberate | Migration loses the suspended or retired distinction and its reason. Add typed stopped states if operators still need that distinction. | M |
| Legacy `retired` and `suspended` | Explicit `stop` | Deliberate | The migrator performs the rewrite. It must report the lost state and reason. | S |
| Agent `keep` | Accepted but ignored | Unfinished | Carry the pin into member specs. Prevent normal garbage collection until an explicit pin change. | M |
| Compact agent `lifecycle` | Inherited by every st3 member | Unfinished | st2 applies this field only to its compact primary task. Preserve that scope during migration. | M |
| Restart intensity block | Implemented | Parity | Attempts, interval, delay, and fail or delay mode run in st3. | None |
| Scalar restart type | st3 adds `always`, `on-failure`, and `never` | Unfinished | st2 does not give this scalar its st3 meaning. Migration must not silently activate an old ignored or malformed scalar. | S |
| `shutdown-timeout` | Implemented only in st3 | Unfinished | st2 ignores this agent extension. Migration must report the new effect before it preserves the node. | S shared |
| `deliver` | Accepted but no member transport is derived | Unfinished | Lower each legacy transport into the correct wrapper and delivery bridge, or refuse it during planning. | M |
| Compact `command` | Implemented | Parity | It creates the primary terminal member. | None |
| Compact `argv` | Implemented | Parity | It creates the primary terminal member. | None |
| `ding` | Accepted but no sidecar is derived | Unfinished | Lower it into the generic delivery policy and implement the complete DING safety contract. | L |
| Agent-level `env` in KDL | Inherited by every st3 member | Unfinished | The st2 KDL runner ignores this node. Migration must report or strip it instead of silently activating old inert content. | S |
| Task environment expansion | Implemented with a different base | Unfinished | Define the st3 action environment and test every supported variable. Preserve the st2 owner identity where compatibility needs it. | M |
| `ST_AGENT` launch value | Graph member subject | Unfinished | st2 gives every task the owner bus ID. Give nested tasks that value and add a separate `ST3_SUBJECT` for the graph member. | M |
| `CATALOG`, `ST_ROOT`, and `PTY_ROOT` | Endpoint and runtime configuration | Deliberate | Keep `PTY_ROOT` where local tools need it. Add compatibility exports only for translated filesystem tools. | S |
| `meta` | Accepted and preserved only | Deliberate | st3 does not interpret it. Expose it through inspection if external tools use it. | S |
| Legacy `model`, `persona`, `permissions`, `transport`, and `strategy` | Rejected | Deliberate | st2 accepts and ignores these runner-external nodes. The migrator must strip or report each one. | S |
| Unknown extension nodes | Rejected | Deliberate | st2 ignores unknown runner fields. Keep st3 strict, but make every migration refusal explicit and source-located. | S |
| `render` | Implemented with safety gaps | Unfinished | Operation details appear in the render table. | See below |
| Typed `claude`, `codex`, `pi`, and `opencode` | `harness "PROVIDER"` | Deliberate | The migrator rewrites the node name. Fix the design document, which still shows the older node names in places. | S |
| Nested `pty` | Implemented | Parity | Terminal allocation, launch, environment, and lifecycle work. | None |
| Nested `exec` | Implemented | Parity | Non-terminal launch and observation work. | None |
| Explicit task `id` | Implemented | Parity | st3 uses it as the runtime ID. | None |
| Default nested task ID | Includes the `pty.` or `exec.` subject prefix | Unfinished | Match `<host>.<agent>.<task>`, or make the migrator write every old resolved ID explicitly. | S |
| Task `command` and `argv` | Implemented | Parity | Shell and direct argument-vector launches work. The two forms remain exclusive. | None |
| Task `cwd` | Implemented | Parity | It defaults through the agent workspace. | None |
| Task `tags` | Implemented | Parity | st3 passes tags to the shared runtime. | None |
| Task `env` | Implemented | Parity | Task values replace inherited values. The action-environment gap remains above. | None |
| Task `unset` | Implemented only in st3 | New | This node removes selected inherited names. Migration must avoid activating an old ignored extension accidentally. | S |
| Task `lifecycle "adopt-only"` | Implemented | Parity | st3 observes an existing generation and does not create a missing one. | None |
| Task `keep` | Accepted but ignored | Unfinished | Carry the pin into the member and collection reducer. | M with agent `keep` |
| Nested `resource` | Accepted but not projected | Unfinished | Publish each binding as a graph relation. Merge declared and dynamic resources in roster queries. | M |
| Nested `stream` | Accepted but not projected | Unfinished | Create declared ingress configuration and optional adapter members. | L with stream commands |
| Source-relative catalog assets | Immutable documents | Deliberate | Migration posts assets first and rewrites copies to `doc/NAME@HASH`. Keep raw filesystem reads outside the daemon boundary. | None |
| Declaration omission | Subject remains unchanged | Deliberate | st3 requires explicit `stop`. It does not treat a missing KDL file as deletion. | None |
| Catalog path identity | Graph subject identity | Deliberate | Move and rename behavior follows the subject, not a folder path. | None |

### New st3 declaration surfaces

These surfaces are additions, not st2 parity gaps.

| st3 surface | Purpose |
|---|---|
| `scope` | Groups desired members and gives them one explicit stop boundary. |
| Standalone `exec` and `pty` | Runs a member without an aggregate agent declaration. |
| Root `resource`, `person`, and `account` | Declares observed graph subjects and actor identities. |
| `supervisor` and `terminal-control` | Applies declared bounded screen input and durable supervision decisions. |
| `link` | Holds or voids work when a required subject is unreachable. |
| `plan` and plan steps | Runs durable work graphs with claims, leases, reviews, and nested plans. |
| `message` and `schedule` | Declares graph messages and clock-triggered messages. |
| Checkpoints and gates | Controls ordered readiness, completion, and eval verdicts. |

## Render and materialization gaps

| Surface | st3 state now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| `copy` | Implemented | Parity | Local sources and immutable document references work. | None |
| `file` positional content | Implemented | Parity | Inline content and deterministic mode work. | None |
| `file { content ... }` | Implemented | Parity | The child content form works. | None |
| `json-upsert` | Implemented | Parity | Object merge and replace or union array modes work. | None |
| `ensure-line` | Implemented | Parity | Exact-line idempotency works. | None |
| `git-exclude` | Implemented | Parity | Failures become warnings and do not block start. | None |
| `executable` Boolean | Implemented | Parity | st3 writes deterministic `0755` or `0644` modes. | None |
| Render variables | Missing | Unfinished | Expand the declared action environment, including `ST_AGENT`, endpoint, driver state, and supported compatibility names. | M |
| Relative copy resolution | Documents or process-relative paths | Unfinished | Refuse ambiguous local paths. Import must convert catalog assets to immutable document references. | S |
| Destination boundary | Relative paths reject `..`; absolute paths are allowed | Unfinished | st2 requires workspace-relative destinations. Decide whether absolute writes remain an explicit capability or become a refusal. | S |
| Workspace existence | st3 creates it | Deliberate | st2 requires an existing workspace. Decide whether st3 owns workspace creation. | Decision |
| Per-file atomicity | Implemented | Parity | st3 writes a temporary file, syncs it, sets mode, and renames it. | None |
| Whole-plan preflight | Missing | Unfinished | Resolve every source, destination, document, JSON patch, and mode before the first write. | M |
| Multi-owner conflict checks | Missing | Unfinished | Compare all active claims for a workspace target. Refuse incompatible content or modes before any write. | M |
| Git-tracked target protection | Missing | Unfinished | Detect tracked files and allow writes only when bytes and mode already match. | M |
| Hook-set verification | Missing in render | Deliberate | Native wrappers remove most hook rendering. Compatibility assets need an immutable, verified document set. | M if retained |
| Driver-generated render | Migrator stages documents | Deliberate | st3 drivers no longer expand workspace hook files. Keep driver setup inside the wrapper and channel package. | None |
| Materialize-only inspection | Missing | Unfinished | Add `plan render` and `apply render` views with predicted file effects. | M |
| Render receipts | Claims record warnings and launch failure | Unfinished | Add operation-level content hashes, modes, and refusal codes for operator diagnosis. | M |
| Agent render before nested tasks | No explicit render barrier | Unfinished | Create one owner render action. Block every nested task until that action succeeds. This also covers agents without a primary member. | M |

## Driver and delivery gaps

| Surface | st3 state now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| Claude typed launch | Implemented | Parity | Model, effort, prompt, arguments, readiness, presence, and channel delivery exist. | None |
| Claude approved channel setup | Depends on the st2 installer | Unfinished | Package installation, verification, policy, and removal under the st3 command surface. | M |
| Claude fallback channel | Implemented | Deliberate | The development-channel fallback can require confirmation. Keep the warning and refuse unattended use when policy requires it. | S |
| Codex typed launch | Implemented with shared st2 code | Parity | Controlled app-server delivery and readiness exist. | None |
| Codex protocol compatibility | Exact version allow-list | Unfinished | Generate compatibility from the installed protocol schema and add a tested upgrade process. Avoid a startup outage after an automatic CLI update. | M |
| Codex process cleanup | Not proved | Unfinished | Put the wrapper and app-server in one owned scope. Reap the complete process group on success, failure, signal, and daemon recovery. | L |
| Codex orphan recovery | Socket ownership refusal only | Unfinished | Diagnose stale holders, prove the owner is absent, clear stale records, and retry without killing a live controller. | M |
| Pi typed launch and channel | Implemented but not provider-proved | Unfinished | Run lifecycle, delivery, restart, and cleanup tests with the real provider. | M |
| OpenCode typed launch and server delivery | Implemented but not provider-proved | Unfinished | Run lifecycle, delivery, restart, and cleanup tests with the real provider. | M |
| Generic command-agent delivery | One terminal notice | Unfinished | Implement FIFO, DND deferral, startup backlog coalescing, staged ownership, receipt inspection, and safe retries. | L |
| Presence vocabulary | `available`, `busy`, `dnd`, and `offline` | Unfinished | Add `away` or formally retire it. Define stale presence and automatic recovery rules. | S |
| Harness state history | Claims | Parity | st3 records readiness, work, idle, errors, compaction, and terminal state as typed claims. | None |
| Driver capability diagnostics | Partial `doctor` checks | Unfinished | Report executable version, protocol support, channel setup, socket ownership, and cleanup scope before launch. | M |

A fleet incident found one cohort of 50 orphaned Codex app-server processes.
One orphan held a control socket and prevented two agents from starting.
This evidence makes the cleanup row a cutover blocker, not optional hardening.

## Bus, messaging, and peer gaps

| Surface | st3 state now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| Storage model | Immutable message lifecycle claims | Deliberate | st3 does not use inbox Markdown as authority. Keep filesystem export as a compatibility projection. | None |
| Send and reply | Implemented | Parity | Sender, recipient, content, subject, tags, and reply links exist. | None |
| Inbox and archive | Open and closed lifecycle queries | Parity | Archive is a durable close claim, not a file move. | None |
| Explicit retry key | Missing in CLI | Unfinished | Accept a caller key and return the first exact result. Reject key reuse with different content. | S |
| Sender history | Missing | Unfinished | Add an indexed query with replication coverage metadata. | M |
| Message tree presentation | Flat output | Unfinished | Build a parent-child tree and use `--tree` in text and JSON. | S |
| Existing st2 archive migration | Missing | Unfinished | Import inbox, archive, sent receipts, IDs, dates, tags, and reply edges without replaying delivery. | L |
| Filesystem compatibility export | Implemented | Parity | `message export` writes a disposable mailbox tree for translated tools. | None |
| Native driver delivery | Claude and Codex implemented | Parity | Pi and OpenCode need provider proofs. | M |
| Generic PTY delivery | Incomplete | Unfinished | Apply the full DING safety contract described above. | L |
| Message acceptance | Read records acceptance | Deliberate | st2 read does not add the same lifecycle claim. Preserve this richer state in st3. | None |
| Message close lifecycle | Implemented | New | This st3 lifecycle is not an st2 replacement requirement. | None |
| Peer replication | Configured HTTP peers | New | st2 has no claims peer protocol. st3 v1 trusts configured peers and has no TLS or ACLs. | Security decision |
| Partition behavior | Local claims continue | Unfinished | Complete conflict, replay, reachability, and recovery tests across partitions. Add transport authentication before untrusted networks. | XL |

## Supervision, restart, and adoption gaps

| Surface | st3 state now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| Reconcile trigger | Committed claims and daemon startup | Deliberate | st3 has no folder watcher or required timer. Add a repair trigger only for state that can change without a claim. | S |
| Daemon restart adoption | Implemented | Parity | st3 observes surviving PTY and exec members and records their isolation mode. | None |
| Process containment | Shared `st-runtime` scopes or detached groups | Parity | Keep Linux scope and non-Linux detached-process tests. | None |
| Driver subprocess containment | Incomplete | Unfinished | Extend member ownership through every provider child and transport helper. | L |
| Restart intensity | Implemented | Parity | Sliding windows, delay, and fail-mode parking exist. | None |
| Restart type | Implemented | Parity | Always, on-failure, and never work. | None |
| Targeted unpark | Missing | Unfinished | Add a fenced reset claim and command. | S |
| `adopt-only` | Implemented | Parity | A missing member remains absent, and an existing member can be adopted. | None |
| Keep pin | Ignored | Unfinished | Prevent normal garbage collection while pinned. Define how explicit stop and scope teardown interact with the pin. | M |
| Suspended state | Generic stop | Deliberate | Add a distinct reversible state if operators need intent separate from retirement. | M |
| Retired state | Generic stop | Deliberate | Add a distinct terminal declaration state if roster and audit readers need it. | M |
| Explicit teardown | Stop intent and scope stop | Parity | st3 does not kill members when the daemon exits. This matches the durable runtime direction. | None |
| st2 supervisor crash recipient | No direct equivalent | Unfinished | Publish a typed crash alert message to an explicit agent or supervision policy. | M |
| st3 terminal controls | Implemented | New | Terminal controls can send bounded input and record decisions. | None |
| Required links | Implemented | New | A link can hold work or void an eval when a dependency is unreachable. | None |
| Health repair | Claim-driven observation | Unfinished | Add periodic or external observation for provider sockets, stale presence, and leaked child processes. | M |
| Cross-host reachability | Peer claims | Unfinished | Prove duplicate, delayed, missing, and conflicting peer batches. Define indeterminate status during a partition. | L |

## Catalog, documents, and materialization gaps

| Surface | st3 state now | Class | Gap and required work | Effort |
|---|---|---|---|---:|
| Source of truth | SQLite claims store | Deliberate | The watched filesystem catalog is not authoritative in st3. | None |
| Source publication | `plan`, `run`, and `import` | Parity | st3 plans and applies explicit bytes through the API. | None |
| Continuous catalog watch | None | Deliberate | A changed source file does nothing until publication. Keep this boundary to prevent ambient mutation. | None |
| One-file migration | `st3-migrate file` | Parity | It emits version-2 KDL and a report. | None |
| Catalog migration | `st3-migrate catalog` | Unfinished | KDL agents and render documents migrate. Add explicit reporting for skipped non-KDL declarations and unsupported runtime state. | M |
| Eval migration | `st3-migrate evals` | Unfinished | It rewrites supported evals. Opaque provider commands still need owner conversion to typed drivers. | L across corpus |
| Immutable documents | `doc put`, `doc get`, and `doc list` | Parity | Versioned hashes replace ambient catalog file reads. | None |
| Document staging during import | Implemented | Parity | Import posts staged documents before applying references. | None |
| Live catalog snapshot | Missing | Unfinished | Export canonical KDL plus all referenced document versions at one store index. | L |
| Semantic diff | `plan` | Parity | It returns selected tokens, changes, actions, and blockers. | None |
| Whole-catalog filesystem CAS | Subject-token CAS | Deliberate | st3 can apply a subgraph transaction without locking a source directory. | None |
| Crash-resumable catalog rename swap | Not applicable | Deliberate | SQLite transaction durability replaces the directory swap and incomplete marker. | None |
| Raw-preimage repair | Claim history and database recovery | Unfinished | Define a supported store backup, integrity check, restore, and point-in-time export process. | L |
| Targeted agent publish | General subgraph run | Unfinished | Add a guard that accepts exactly one agent when compatibility scripts require it. | S |
| Catalog selection flags | Endpoint selection | Deliberate | `--endpoint` and config replace `--catalog`, `CATALOG`, and `ST_ROOT`. | None |
| PTY registry selection | Config and `up --pty-root` | Parity | st3 can adopt the existing registry during cutover. | None |
| Render materialization | Before a member with its own render starts | Unfinished | Add the agent-owner render barrier described above. Keep every nested task behind that result. | M shared |
| Render-only update | Desired revision reconciliation | Unfinished | Prove when a render-only revision restarts, holds, or updates a member. Expose the predicted action in `plan`. | M |
| Rollback | Claim history without a rollback command | Unfinished | Add a command that republishes a selected prior desired revision with fresh subject tokens. | M |

## Specification and proof gaps

| Gap | Class | Required work | Effort |
|---|---|---|---:|
| The design still shows old typed driver node names in parts of the KDL section. | Unfinished | Make `harness "PROVIDER"` normative everywhere, or change the parser and migrator back. | S |
| The design root-node list names `checkpoints`, while the parser accepts root `plan`. | Unfinished | Align the complete KDL grammar and examples with the current plan runtime. | S |
| The design names some accepted fields as operational when the reducer ignores them. | Unfinished | Mark `keep`, nested resources, streams, `deliver`, `ding`, and `meta` with exact implementation state. | S |
| Pi and OpenCode lack live provider proofs. | Unfinished | Add bounded lifecycle, delivery, restart, and cleanup tests for each provider. | M each |
| Generic DING has no complete transport proof. | Unfinished | Port the model-free st2 delivery fixtures before claiming command-agent parity. | L |
| Non-Linux containment has limited proof. | Unfinished | Add detached-process, restart, and child-reap tests on macOS. | M |
| Message archive import has no proof or tool. | Unfinished | Import a branched archive and compare IDs, dates, status, and thread output. | L |
| No cutover rollback rehearsal exists. | Unfinished | Rehearse st2 stop, st3 adoption, message migration, failure, and return to st2. | L |

## Recommended decision order

1. Decide whether “drop-in” means current-fleet migration or complete st2 product parity.
2. Decide whether st3 keeps suspended, retired, away, request transport, and generic DING.
3. Decide whether the `supervisor` name can keep its different st3 meaning.
4. Close Codex cleanup and protocol upgrade handling before another fleet cutover.
5. Close render safety and message archive migration before st3 owns persistent agents.
6. Add task inventory, unpark, snapshot, rollback, and full driver diagnostics.
7. Prove Pi, OpenCode, partitions, and non-Linux containment after the common path is safe.

The first decision controls the schedule.
The current-fleet path can omit unused st2 features after an explicit catalog audit.
The complete product path must implement or formally retire every unfinished row.
