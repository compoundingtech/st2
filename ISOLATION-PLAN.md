# Isolated-task-spawn — the permanent fleet-fragility fix

**Status:** design (gate before the big build). **Owner:** st2 (task spawn). **Coordinates with:** fabric
(agent spawn) — shared mechanism.

## The incident, and the root cause (verified on hetz)

A `convoy up` (fabric) restart — and a supervisor restart/crash — killed the **whole hetz fleet**. Not
because st2 lacks decoupling: st2 already `setsid`s every task and survives its own SIGTERM/SIGKILL
(`tests/nomad_survival.rs`). It died because of a mechanism that decoupling doesn't touch — **cgroups**.

Probed live on hetz:

```
# every agent + task runs inside ONE systemd service cgroup:
$ cat /proc/self/cgroup
0::/user.slice/user-1000.slice/user@1000.service/app.slice/convoy-up.service
```

- `setsid` changes a process's **session / process-group** — it does **not** change its **cgroup**. A
  forked child inherits the parent's cgroup and stays there.
- systemd tears a unit down by **cgroup**: `systemctl stop/restart convoy-up.service` SIGTERMs (then
  SIGKILLs) **every process in that cgroup** — agents, tasks, pty-daemons, all of it.
- So today's tasks — correctly `setsid`'d, correctly surviving st2's *process* death — were still sitting
  inside `convoy-up.service`'s cgroup, and the cgroup kill cascaded through all of them.

The existing `setsid` defense and this cgroup defense are **orthogonal**. We need both.

## The fix (one goal, per-OS mechanism)

Every task st2 spawns must live in its **own OS supervision domain**, independent of BOTH the supervisor
AND fabric — so nothing above it can signal it as a side effect of its own teardown.

### Linux — own transient systemd scope (own cgroup)

```
systemd-run --user --scope --unit=st2-<id>-<nonce>.scope --collect --quiet -- <task>
```

Verified on hetz — a scope created from *inside* `convoy-up.service` lands as a **sibling**, not a child:

```
task cgroup = …/app.slice/st2-<id>.scope     ← its own cgroup
me   cgroup = …/app.slice/convoy-up.service   ← the transport unit, a SIBLING
```

`systemctl restart convoy-up.service` kills only `convoy-up.service`'s cgroup subtree; the `st2-*.scope`
siblings are untouched. That is the whole fix.

Why `--scope` and not `--service`: a scope is *only* a cgroup container — st2 stays the logical
supervisor (adoption via pidfiles/`pty list`, teardown via the retired-spec path, `restart{}` policy).
A `--service` would hand lifecycle to systemd and fight st2's own supervision. `--collect` auto-GCs the
scope once its cgroup empties (on normal teardown via the existing kill paths).

### Both backends ride the same wrapper (verified)

- **exec** (st2's own `sh -c` + `setsid`): wrap the argv → `systemd-run … -- sh -c '<command>'`. Keep
  `setsid` for the tty-free property; the scope adds the cgroup.
- **pty** (st2 shells to `pty run -d`): wrap the client → `systemd-run … -- pty run -d --force --id … --
  sh -c '<command>'`. **Proven on hetz:** pty is a *per-session daemon* model, and the `pty-daemon`
  inherits the cgroup of whoever ran `pty run` (double-fork-to-init does not move cgroups). So wrapping
  the client captures the whole tree:

  ```
  $ systemctl --user status st2-test-pty-*.scope
      CGroup: …/app.slice/st2-test-pty-*.scope
              ├─ pty-daemon
              └─ sleep 40        ← the session itself, in the scope
  ```

  This is the **shared scope-wrapper** the CoS flagged: st2 applies it to task spawns; fabric applies the
  same wrapper to agent spawns. One mechanism, two callers.

### Teardown is unchanged (important)

The scope is for **survival only**. Teardown keeps going through today's paths — `pty kill <id>` and the
exec process-group kill (`kill(-pgid)`, `tests/exec_backend.rs`). Probe note: `systemctl stop <scope>`
does *not* promptly kill a pty session (pty doesn't honor SIGTERM in the graceful window), so we do **not**
route teardown through the scope. `--collect` reaps the emptied scope after the real kill. This preserves
the **Clean exec teardown** invariant verbatim.

### macOS — daemonize (setsid + reparent to launchd/init)

No cgroups; launchd does **not** cascade-kill detached children (proven today: the Mac fleet survived the
Mac fabric restart while hetz's systemd fleet died). So `setsid` + an explicit double-fork to reparent to
init/launchd **is** the whole defense — likely already what pty does (its daemons show `ppid=1`,
reparented to `systemd`/launchd). Verify pty's exact mechanism on macOS at build; the exec backend adds an
explicit double-fork so reparenting happens at spawn, not only on st2's death.

### Fallback — degrade honestly, never silently

If `systemd-run --user` is unreachable (no user manager / no D-Bus / non-systemd Linux / container),
fall back to today's `setsid`-detach **and log a loud WARN** that cgroup-cascade isolation is unavailable
— degrade honestly (the maintainer's "no silent caps"). Detect once and cache; don't spam every spawn.

## Interface — one primitive, OS-dispatched

```rust
/// Places a spawned task in its own OS supervision domain so it outlives BOTH the spawner (st2) AND
/// the transport/fabric daemon. The permanent fix for cgroup-cascade fleet death.
pub trait IsolatedSpawn {
    /// Build the launch Command for `inner` (program + args), isolated under `unit` (a fresh,
    /// id-derived + nonce'd scope name — see the naming note below). Caller sets env/cwd/stdio on the
    /// returned Command; those inherit through `--scope` (scope mode runs in the caller's env).
    fn wrap(&self, unit: &str, inner: Argv) -> Command;
    /// The realized isolation, for reporting + test assertions.
    fn realized(&self) -> Isolation;
}

pub enum Isolation {
    Scope,               // Linux: own transient systemd scope (own cgroup, sibling of transport)
    Detached,            // macOS / no-systemd: setsid + double-fork, reparented to init/launchd
    DegradedDetached,    // isolation requested but unavailable — Detached + a logged WARN
}

// Chosen once by probing target_os + `systemd-run --user` reachability; cached.
pub fn detect() -> Box<dyn IsolatedSpawn>;
```

Two call sites, both already build a `Command` today:

- `ExecBackend::spawn` — `wrap(scope_unit(id), ("sh", ["-c", command]))`.
- `PtyCli::spawn` — `wrap(scope_unit(id), ("pty", ["run","-d","--force","--id",id,…,"--","sh","-c",command]))`.

**Naming — UNIQUE, not deterministic (as-shipped, supersedes the earlier draft).** The scope name is
**write-only**: st2 references it only at spawn (`--unit=`); teardown is by process group / `pty kill`
and adoption is by pidfile / `pty list`, neither of which needs the scope name. So it must be unique,
not deterministic — a deterministic name collides whenever a scope of that name still lingers (a
stale/failed scope on re-spawn, or a concurrent spawn of the same declared id), and `systemd-run
--unit=<X>` fails hard on an existing `<X>`. `scope_unit(id)` therefore appends a per-process nonce
(`st2-<sanitized-id>-<pid>-<seq>.scope`), keeping the id for greppability. `--collect` reaps each scope
once its task exits, so unique names never accumulate.

## Test plan — tests-first, real spawn + real cgroup kill

The load-bearing invariant today's suite does **not** cover: *a running task survives the death/restart of
its supervisor AND of the fabric/transport daemon.* Written as a NAMED test **before** the impl, per OS.
Heavier than a unit test (real spawn + real kill) — the right level; today proved this cannot live in
reasoning alone.

### Linux — `tests/transport_isolation.rs::task_survives_transport_cgroup_cascade`

1. Create a **transport scope** `st2-test-transport-<n>.scope` (stand-in for `convoy-up.service`) and run
   a small spawner inside it. This reproduces "st2/pty running inside fabric's cgroup."
2. The spawner uses st2's primitive → the task lands in its **own** scope `st2-test-task-<n>.scope`.
   Record the task pid; **assert precondition:** `/proc/<pid>/cgroup` is the task's scope, NOT the
   transport scope (a test that skips this could pass vacuously).
3. **Fire the cascade:** `systemctl --user stop st2-test-transport-<n>.scope` — cgroup-kills the whole
   transport subtree, exactly like a `convoy up` restart.
4. **Assert survival:** the task pid is still alive and still in its own scope.
5. **CONTROL (proves the cascade is real):** the spawner *also* forks a naive task that stays in the
   transport cgroup; after the cascade, assert **that** one is DEAD. Without this, the survival assertion
   could pass even if the stop did nothing.

### macOS — `tests/transport_isolation.rs::task_survives_spawner_group_kill`

Spawn via the primitive from a parent; SIGKILL the parent's whole process group; assert the task survived
and reparented (`ppid == 1`). Control: a same-group naive child dies.

### Gating + hygiene

- Fail-loud gate like `pty_gate`: on Linux require `systemd-run`/`systemctl --user` + a reachable user
  manager; **HARD FAIL** unless a dev sets `ST2_ALLOW_ISOLATION_SKIP` on a box without systemd. CI/gating
  (hetz) has systemd, so it runs for real — never a silent green skip.
- Panic-safe `Drop`: `systemctl --user stop` + `reset-failed` both scopes and SIGKILL any leftover pid,
  even on a failing assertion. Unique unit names per run; throwaway `PTY_ROOT`/`XDG_STATE_HOME`.

When green, add a row to `INVARIANTS.md`:

> **Transport-decoupled lifecycle** — a task survives a cgroup-cascade kill of its supervisor/transport
> unit (systemd scope isolation), not just the supervisor's process death. Proof:
> `tests/transport_isolation.rs::task_survives_transport_cgroup_cascade`.

## Sequencing

1. Write the named test first (red) — the cascade + control.
2. `detect()` + the Linux `Scope` backend; wire `ExecBackend::spawn`. Go green for exec.
3. Wire `PtyCli::build_run_command` through the same wrapper. Green for pty.
4. macOS `Detached` (double-fork) + fallback WARN path; the macOS test.
5. INVARIANTS.md row; README/CHANGELOG note.

Small iterations are ungated; this design + the big wire-up are gated by this doc.

## Coordination with fabric

The `systemd-run --scope` wrapper is the shared mechanism. Split of ownership: **st2 isolates the TASKS it
spawns** (exec + pty, this doc); **fabric isolates the AGENTS it spawns** (its exec-of-pty proof). To keep
them coherent we agree: (a) unit-name convention (`st2-*` for tasks, fabric picks its own prefix), (b)
identical fallback semantics (degrade-to-detached + WARN, never silent), (c) `--user --scope --collect`
as the canonical invocation. I own st2's repo only; fabric's change lands in fabric's repo via its owner.
