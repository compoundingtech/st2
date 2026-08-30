# Live st3 eval graph runbook

This runbook starts an isolated st3 daemon and watches one eval in a full-screen terminal graph.

Use the demonstration script from an interactive shell on the host:

```sh
./scripts/live-graph-demo.sh
./scripts/live-graph-demo.sh weird-git-setup
```

The script defaults to `weird-git-setup`. It also accepts an eval directory path.

The script checks the host, builds and copies one binary, and starts the daemon in the background. It then runs the graph in the foreground.

The script uses a fresh root for each run. It stops the daemon and demo sessions, then removes that root on normal exit or Control-C.

The manual workflow below explains each action that the script performs.

The daemon must run before the eval starts. The graph command must write to an interactive terminal.

The eval does not need registration or viewer configuration. st3 reads the complete eval directory when the eval command starts.

## Prerequisites

Use a Linux host with an interactive terminal.

Use a clean st2 checkout on the `design/st3-engineering` branch. The checkout must contain commit `95ad704` or a descendant.

The host must have Rust, Codex, PTY, Git, and network access for Codex. The Codex account must have model access and sufficient quota.

This proof used these versions:

```text
cargo 1.97.0
rustc 1.97.0
codex-cli 0.146.0
pty 0.12.0+500eab2
```

Run these checks from the repository root:

```sh
test "$(git branch --show-current)" = design/st3-engineering
git merge-base --is-ancestor 95ad704 HEAD
git status --short
command -v cargo rustc codex pty git
cargo --version
rustc --version
codex --version
pty --version
```

The first two commands must exit successfully. `git status --short` must print nothing.

The remaining commands must print one path or version each. Stop if a command fails.

## Build an immutable demo binary

Run these commands in terminal 1:

```sh
cargo build -p st3 --locked
DEMO_ROOT="${XDG_RUNTIME_DIR:-/tmp}/st3-live-demo"
test ! -e "$DEMO_ROOT" || { echo "$DEMO_ROOT already exists; choose a fresh DEMO_ROOT"; exit 1; }
mkdir -m 700 "$DEMO_ROOT"
install -m 0755 target/debug/st3 "$DEMO_ROOT/st3"
sha256sum "$DEMO_ROOT/st3"
```

The build takes a few seconds after a warm build. The last command prints the binary hash.

If the root exists, add a suffix to `DEMO_ROOT`. Use the same new value in both terminals.

Do not rebuild or replace this copied binary during the eval. The daemon uses the same binary for every native driver.

## Start the daemon

Continue in terminal 1:

```sh
"$DEMO_ROOT/st3" up \
  --node live-demo \
  --state-dir "$DEMO_ROOT/state" \
  --socket "$DEMO_ROOT/st3.sock"
```

The daemon prints a security warning. It then prints this line within one second:

```text
st3: local API listening at .../st3-live-demo/st3.sock
```

Keep terminal 1 open. Do not press Control-C until the eval reaches its terminal screen.

## Check the daemon

Set the same root in terminal 2:

```sh
DEMO_ROOT="${XDG_RUNTIME_DIR:-/tmp}/st3-live-demo"
"$DEMO_ROOT/st3" \
  --endpoint "$DEMO_ROOT/st3.sock" \
  doctor --strict
```

The command prints checks for the claim store, state directory, PTY runtime, process isolation, runtime ownership, runtime drift, and driver readiness.

Every check must say `pass`. Stop if the command exits with an error.

## Run the short demonstration

Terminal 2 must be a real terminal. A plain `fabric exec` call is not sufficient because its output is not a TTY.

Run this command in terminal 2:

```sh
"$DEMO_ROOT/st3" \
  --endpoint "$DEMO_ROOT/st3.sock" \
  eval ./evals/st3/weird-git-setup --graph
```

The command first prints a scope such as this one:

```text
started scope/eval/weird-git-setup/RUN_ID
```

The command then clears the terminal and draws `ST3 EVAL GRAPH`. The first frame appears in less than one second.

The graph shows lifecycle, phase, verdict, cleanup, progress, root work, nested work, assignees, blocked reasons, elapsed time, and recent transitions.

The 2026-08-30 proof showed these changes:

- The fixture completed at `00:00`.
- The Codex worker became ready at `00:02`.
- The worker claimed its parent work at `00:27`.
- The graph expanded six nested steps under the parent work.
- Each nested step moved through ready, claimed, and completed states.
- The held-out judges and cleanup completed at `03:15`.

The final frame showed this summary:

```text
STATE      completed · terminal
VERDICT    pass
CLEANUP    complete
PROGRESS   11/11 completed · 0 active · 0 blocked
```

Allow three to six minutes for this demonstration. Model startup and service latency can change the duration.

The viewer exits automatically after the terminal frame appears. Press Control-C in terminal 1 to stop the daemon.

Keep the state directory if you need the run record. Remove the fresh demo directory only after you no longer need that record.

## Author a new eval during the session

The daemon can run while Nathan and Johannes write a new eval directory. This workflow does not require a known eval name.

Complete `eval.kdl` and every referenced fixture before starting the eval. Then replace the path in the demonstration command:

```sh
"$DEMO_ROOT/st3" \
  --endpoint "$DEMO_ROOT/st3.sock" \
  eval ./path/to/new-eval --graph
```

st3 archives the directory when this command starts. Later file changes do not change that running eval.

The viewer cannot show a partial eval during authoring. It starts after st3 accepts the complete eval bundle.

If the viewer disconnects, keep the daemon running. Attach again from a real terminal with the printed scope:

```sh
"$DEMO_ROOT/st3" \
  --endpoint "$DEMO_ROOT/st3.sock" \
  graph scope/eval/EVAL_NAME/RUN_ID
```

## Why the first demonstration is not fork-in-the-road

`fork-in-the-road` is a useful four-agent acceptance eval. It is not a good first live demonstration.

Three prior runs took approximately 43 to 68 minutes. They used approximately 4.9 to 7.3 million model tokens each.

Run it only when the longer design-panel workflow and its cost are part of the demonstration.
