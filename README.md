# Small Talk

Small Talk runs declarative networks of coding agents.

You publish KDL missions. The st3 runtime stores their claims, starts their members, offers ready
steps to eligible agents, and records the result. A daemon restart does not terminate mission-owned
PTY or exec processes. The restarted daemon adopts the exact surviving incarnation.

The `st` command is the normal product command. During migration, `st3` remains the exact daemon
name and both command names run the same installed binary.

## The model

A mission is a durable graph declaration. A mission can contain:

- goals and constraints;
- ordered or parallel steps;
- standing agents and temporary agents;
- exec processes and terminal processes;
- mechanical gates and human gates;
- immutable documents and observed resources;
- nested missions, queues, schedules, and cleanup work.

Publishing is an atomic upsert. Removing text from a later KDL file does not cancel the earlier
declaration. Publish an explicit cancellation or replacement when the graph must change.

Agents claim eligible work. The reconciler does not choose work for an agent. It starts declared
runtimes, delivers graph changes, enforces leases, evaluates gates, and cleans up owned runtimes.

An agent status keeps runtime facts in `actual` and current harness facts in `harness`. A harness
observation applies only to the current runtime incarnation. A stale ready claim cannot make a new
runtime ready.

## Install

The Nix package installs `st`, `st3`, and `st3-migrate`:

```sh
nix profile install .#st3
st --help
st3 --help
```

For repository development:

```sh
cargo build -p st3 -p st3-migrate --locked
target/debug/st3 --help
```

The runtime needs `pty` on `PATH`. A native agent also needs its selected harness, such as Codex,
Claude, pi, OMP, or OpenCode.

## Start the daemon

The default local configuration needs no file:

```sh
st service install
st service status
st doctor
```

The default state directory is `${XDG_STATE_HOME:-$HOME/.local/state}/st3`. The default Unix socket
is below `${XDG_RUNTIME_DIR}` when that variable is set. Otherwise, it is below the state directory.

Fleet replication is optional. A fleet configuration uses authenticated loopback endpoints. Fabric
or another loopback port exposer carries those endpoints between machines.

See [fleet replication](docs/st3/replication.md) for the peer configuration and repair commands.

The service installer keeps the control processes separate from mission runtimes. Linux starts each
runtime in its own systemd user scope. macOS starts each runtime in its own process session. A main
daemon restart does not stop an existing runtime. The replication worker also has its own service.

The macOS main service uses the interactive launchd class because it starts interactive agent
sessions. The replication worker stays in the background class. The Linux services use the normal
user priority. They do not lower the priority of the runtime scopes.

The service files contain the absolute st3 path. They do not contain a captured or guessed `PATH`.
Before each new PTY or exec starts, st3 runs the account's default shell as an interactive login
shell in a short-lived real terminal. This matches startup files that require a TTY. st3 captures the
shell's current exported environment. The mission environment then overrides those values. A
`${PATH}` value expands against the fresh shell path. st3 starts the declared command directly after
this probe, so a shell wrapper does not change its arguments, signals, or exit status. The probe has
a ten-second timeout. A broken shell startup cannot stop reconciliation forever.

On macOS, run `st service permissions` for the one-time Full Disk Access and Developer Tools steps.
Use `st service permissions --open` to open the matching System Settings pages. macOS does not let a
launchd property list grant these permissions.

Finite client commands have bounded waits. A connection gets three seconds. An ordinary request gets
15 seconds in total. A replication import or export gets 120 seconds. A 30-second event wait gets 35
seconds. A terminal WebSocket handshake gets ten seconds. An attached terminal and an explicit
follow remain open after their connection succeeds. A timeout names the endpoint and phase, exits
nonzero, and tells the operator to retry.

## Publish and run a mission

Start with an example or a KDL file from your private network repository:

```sh
st preview mission.kdl
st publish mission.kdl --as person/operator
st mission start fleet/example --as person/operator --follow
```

The preview validates the complete declaration without changing the graph. The publication applies
all declarations in one transaction. The run pins the exact mission revision and its exact inputs.

Use an explicit run ID when an external system needs a stable name:

```sh
st mission start fleet/example \
  --id release-candidate-42 \
  --input request="Prepare release 42" \
  --as person/operator
```

The default mission permits one nonterminal run. Add `concurrent-runs` when independent runs can
overlap.

## Do mission work

Every native harness receives a generated `.st3/boot.md`. The runtime also appends one short prompt
that tells the harness to read that file and claim its current work.

An agent uses the exact binary from `ST3_BIN`:

```sh
"$ST3_BIN" work ls
"$ST3_BIN" work claim step-run/RUN/STEP
"$ST3_BIN" work progress step-run/RUN/STEP --summary "The tests now pass."
"$ST3_BIN" work complete step-run/RUN/STEP --summary "The change is ready."
```

Use `work release` when another eligible agent should take the step. Use `work fail` when the work
cannot meet its goal. Use `st wait` only when claimed work needs a graph condition.

See the [mission runtime](docs/st3/mission-graph-runtime.md) for the complete KDL language.

## Messages and human attention

Small Talk messages are durable graph records:

```sh
st message send agent/fleet/example/standing/worker \
  --from person/operator \
  --subject "Check the new request" \
  --body "A new mission step is ready."

st message ls --as agent/fleet/example/standing/worker
st message read MESSAGE --as agent/fleet/example/standing/worker
st message archive MESSAGE --as agent/fleet/example/standing/worker
```

Human gates, launch approvals, revision approvals, unread person messages, and explicit faults
appear in one inbox:

```sh
st attention ls --as person/operator
st attention ls --as person/operator --json
```

The JSON form contains typed items and exact action arguments for a TUI or native application.

## Launch and revision

A launch stores its request, planner, candidate missions, feedback, and approval in the
claims database:

```sh
st launch start request.md --id release --as person/operator
st launch show launch/release/SESSION
st launch preview launch/release/SESSION --variant default
st launch revise launch/release/SESSION feedback.md --as person/operator
st launch approve launch/release/SESSION PREVIEW_TOKEN --as person/operator
```

A running mission can move to a new revision through a successor generation. Compatible completed
work remains complete. Changed work and its dependants become ready again.

See the [KDL lifecycle guide](docs/st3/kdl-lifecycle.md) for authoring, publication, revision, review,
resource refresh, and cancellation workflows.

## Inspect and repair

These commands provide the normal operational views:

```sh
st status
st agents
st mission show fleet/example
st work ls --all
st doctor --strict
st replication status
st schema list
```

Invalid replicated records remain visible and cannot halt replication. An explicit repair names the
bad record and publishes its replacement. See [data authority](docs/st3/data-authority.md) and
[fleet replication](docs/st3/replication.md) for the guarantees.

## CLI output and terminals

Structured commands print a readable view by default. Add the global `--json` option when a program
needs the stable data shape:

```sh
st pty ls
st --json pty ls
st --json status
```

Commands that return document bytes, logs, terminal screens, or completion scripts keep their raw
payload output.

`st pty attach SUBJECT` uses the same Rust terminal client as `pty attach`. Type normally in the
attached session. Press Ctrl+\ once to detach without stopping the session. The client restores
terminal modes before it exits. It forwards terminal resize and Kitty key sequences without a text
translation layer. A nested attachment is refused by default. Use `--force` only when the nested
attachment is intentional.

## Documentation

- [Documentation index](docs/st3/README.md)
- [Architecture](docs/st3/design.md)
- [Mission graph runtime](docs/st3/mission-graph-runtime.md)
- [KDL lifecycle](docs/st3/kdl-lifecycle.md)
- [Schema registry](docs/st3/schema.md)
- [Agent migration](docs/st3/agent-migration.md)
- [Examples](examples/st3/README.md)
- [Product roadmap](docs/st3/roadmap.md)

## st2

st2 remains available during the fleet migration. Its catalog, file bus, CLI, and service continue
to use their existing contracts.

Read the [st2 legacy guide](README.st2.md) before changing or operating st2.
