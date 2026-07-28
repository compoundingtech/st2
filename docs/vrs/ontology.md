# st2 ontology

The canonical vocabulary of st2: what each term means, which terms st2 owns,
and where one word carries more than one meaning.

This document governs the vocabulary of the VRS documents and of anything
written about st2. It does not require code identifiers to change; where a code
name diverges from the canonical term, that divergence is recorded here rather
than hidden.

## What st2 owns

st2 implements the executable agent contract in
[`compoundingtech/evals/AGENT-SPEC.md`](https://github.com/compoundingtech/evals/blob/main/AGENT-SPEC.md).
The agent grammar and the harness-facing contract are canonical there, so the
terms describing *what an agent is* are inherited, not coined here. st2 owns the
terms describing *how a declared fleet is run on a host*: the catalog and its
layout, reconciliation and materialization, supervision, sessions and their
backends, message delivery into a terminal, health reporting, and the eval
runtime.

Terms belonging to the underlying domain are borrowed with their established
meanings, and a borrowed word is preferred over a coined one. Three domains
supply most of the vocabulary: POSIX (process, process group, session, signal,
pid, PATH), the terminal (pty, composer, bracketed paste, Return, and the
session lifecycle words the terminal tool reports), and the scheduler tradition.

The scheduler borrowing is explicit and load-bearing: a declaration reads like a
Nomad job, where **the agent is the job** and its **tasks** are what run. That
one sentence is the origin of two things this document has to flag — *task* as a
homograph, and **job** and **agent** as synonyms for one concept, both in use.
It is also where the job-type vocabulary comes from, of which only the
long-running kind survives.

**One caveat on the seam.** That st2 defers to the agent-spec is stated in both
directions in its own documents, and a shape may have to be proven here before
becoming canonical there — so st2 can originate a term and push it upward.
Which words the agent-spec actually defines cannot be settled from this
repository, because that document is not vendored here. The direction of
deference is established; the exact inherited list is not, and drawing that line
precisely requires reading the upstream document.

Consumers that render or wrap st2 add their own vocabulary. Terms that appear
only in a consumer's tooling, such as an eval **cell** directory layout, are not
st2 terms and are not defined here, even where st2's own help text shows them in
an example path.

## Language

### The catalog

- **Catalog** — the folder that holds a fleet's agent declarations, each agent's
  runtime state, and each agent's inbox. It is both the authored source of truth
  and the place the runtime writes. A single-file fleet spec may stand in for
  one.
- **Declaration** — one authored agent, hand-written in KDL. Canonical: any
  generator's output is inspectable before it is reconciled. A declaration is
  edited, never deleted, to decommission it.
- **Template** — shared content under the catalog's `_templates/`, owned by the
  catalog and referenced by declarations. A template edit converges its
  dependents only.
- **Workspace** — the repo or worktree a declaration names, which supplies the
  default working directory for each of its tasks.

### Identity and placement

- **Agent** — the modeled actor. st2 models agents and nothing else; non-agent
  identities are unsupported. **Job** is a synonym, inherited from the scheduler
  analogy the declaration is built on: the agent *is* the job, and its tasks are
  what run. Both words are in use. Prefer **agent**, and read *job* as pointing
  at the analogy rather than at a second concept.
- **Identity** — an agent's bare name, unique within the fleet.
- **Bus id** — an agent's address, `<host>.<identity>`. The identity qualified by
  the host that runs it.
- **Host** — the machine that runs an agent. Placement is host-pinned: a
  declaration resolves to its declared host, and a host reconciles only its own.
- **Fleet** — the set of agents supervised together: a whole catalog, or a
  single-file spec's team.

### Running work

- **Task** — a unit of work a declaration launches. Two kinds, distinguished by
  whether the work needs a terminal: **terminal-backed** (an interactive harness)
  and **terminal-free** (a plain process — the ding sidecar, a daemon).
- **Session** — the running instance of a task, addressable by the runtime.
- **Supervisor** — two distinct things share this word; see the flagged
  ambiguity. The **control plane** is the resident process that reconciles a
  host's declared work and keeps its sessions running. It is replaceable:
  stopping, killing, or replacing it never stops the agents it launched. An
  agent's **declared supervisor** is a different agent, named in the
  declaration, that receives its crash notifications.
- **Adoption** — a replacement control plane taking ownership of already-running
  sessions by stable identity, rather than launching duplicates. The property
  that makes the control plane replaceable.
- **Restart policy** — a declaration's rule for what happens when a task exits:
  how many attempts, how long between them, and whether exhaustion stops or
  keeps retrying.
- **Flapping** — the crash-loop bookkeeping driven by that policy.
- **Parked** — the terminal state within flapping: attempts exhausted under a
  policy that stops. A parked task is surfaced rather than retried, and its
  evidence is kept on purpose.
- **Host lock** — the per-host, per-catalog record of which process is currently
  supervising. Its absence means manual operation, not failure; a lock whose
  owner is dead is evidence of an unclean exit.
- **Reconcile** — take the shortest correct path from observed state to declared
  state. An event is evidence that something may have changed, not permission to
  converge the whole world.
- **Materialize** — render a declaration's workspace content from the catalog so
  the declared work has what it needs before it launches.
- **Retirement** — a declaration marked decommissioned by an edit. A retired
  declaration is expected to have nothing left running and is not expected to
  report presence.

### The bus

- **Inbox** — the folder where an agent's unread messages arrive, one atomic
  file per message.
- **Message** — one durable, addressed unit of communication between agents.
- **Archive** — the folder an agent moves a handled message into. An archive
  entry whose filename matches an inbox entry makes that message handled, and is
  the receipt that prevents redelivery.
- **DING** — the delivery of an unread message into a running agent's terminal,
  performed by a sidecar that watches the inbox. Named for the poke rather than
  the message, because the message has already arrived; the DING is what makes
  the agent notice.
- **Ping** — the same thing as DING. The command accepts both names, and the
  stated direction is to settle on *ping*, since performing the poke is the
  runner's job rather than a separate act. The rest of the vocabulary has not
  moved: the authored keyword, the derived task name that code matches as a
  literal, and the wire prefix all still say *ding*. Until those move, write
  **DING** for the mechanism and treat *ping* as the name it is becoming.
- **Poke** — one attempt to put a notice into a composer. The act; the DING is
  the arrangement that performs it.

### Delivery into a terminal

These terms are refined per harness; see the DING subsystem's nodes.

- **Harness** — the interactive program running in a terminal-backed task, whose
  screen DING must read before typing. A **maintained** harness is one st2 can
  positively recognize.
- **Composer** — the harness's text-input area. The **live composer** is the one
  that will receive keystrokes; it is the lowest one on the screen, since
  scrollback sits above it. This sense of *live* is introduced by the DING
  subsystem and is not the established one — see the flagged ambiguity below.
- **Notice** — the normalized single-line text one message becomes.
- **Staged** — pasted into the composer but not submitted. A staged payload is
  owned by the attempt that pasted it and is resolved by inspection, never by
  pasting again.
- **Observation** — one reading of the rendered screen, classified against the
  exact expected notice.

### Externalized state

- **Presence** — an agent's declared availability, written by the agent and
  readable from the catalog without opening a terminal. A presence reading that
  has decayed past its freshness window is derived as unknown rather than
  trusted.
- **Context** — an agent's durable working state, held outside its transcript so
  a replacement session can resume without it.
- **Resource** — a durable, listable record of high-value output an agent
  produced, so a peer can find it.

### Health and evidence

- **Doctor** — the on-demand health check for one catalog as seen from one host.
  It diagnoses and never repairs.
- **Check** — one thing doctor inspects, reported as one line.
- **Problem** — one failed check. Problems are flat: any one of them fails the
  run.
- **Eval** — an executable run of an agent spec, used as evidence that a
  declaration behaves as claimed.
- **Judge** — one assertion over an eval's outcome. All judges must pass.
- **Verdict** — an eval's overall result, derived from its judges.

## Structure

Most of the vocabulary is a flat controlled vocabulary. Three places carry
weight and are drawn explicitly.

**The catalog contains, transitively.** `partOf` all the way down, which is why
the part names read within the whole rather than standing alone:

```text
catalog
  ├── declaration ── task ── session
  ├── inbox ── message ── archive entry
  └── agent state ── presence · context · resource
```

**Task kind and lifecycle are independent facets.** A task is terminal-backed or
terminal-free; independently, it is declared, live, or retired. The two axes
combine freely, and no name may imply they are coupled — a retired terminal-free
task is an ordinary combination, not a special case.

**DING is a pipeline, and its stage names are the vocabulary.** Each stage is a
distinct term because each is a distinct state a payload can be stuck in:

```text
message → notice → staged → observed → submitted
                     └── owned by the attempt until it changes or disappears
```

Followers carry their anchor's word only where the follower's own word is
generic. **Bus id** keeps `id` because an unqualified "bus" says nothing about
addressing; **archive** and **inbox** drop any shared stem because each borrows a
specific, self-informing domain word.

## Flagged ambiguities

Words carrying more than one meaning, where the senses can genuinely be
confused. A word that is merely reused in unrelated places is not listed.

### `root` — the worst one

At least six senses, four of which appear in the VRS documents themselves:

| Sense | What it means |
| --- | --- |
| root agent | the one intelligent supervising agent per machine |
| catalog root | the catalog directory |
| session root | the directory holding the session registry |
| bus root | the directory the native bus resolves against |
| hook root | the machine-local directory hooks install into |
| watch root | a directory a filesystem watcher is scoped to |

These collide rather than coexist: `vision.md` uses bare *root* for the root
agent in a list of actors, while `requirements.md` uses bare *roots* for
host-local scopes in one requirement and for watcher scopes in another. A reader
cannot resolve which is meant from the sentence alone. Four of the senses are
directories, and three of those are printed together by the command that exports
the bus environment — so seeing them side by side is routine.

A second hazard sits inside the directory senses: the bus root is normally
*equal to* the catalog root, since no nested bus directory is synthesized. Two
names for one path that can nonetheless diverge is worse than two names for two
paths, because equality holds until it doesn't and nothing in the vocabulary
signals which is being relied on.

**Always qualify.** Never write bare *root*.

One place cannot be fixed by convention. A ratified requirement assigns
reconciliation to "host-local roots", and no reading of it is clean. Read as root
agents it contradicts the neighbouring requirement, which explicitly gives
reconciliation to the deterministic reconciler and observation to the root agent.
Read as a directory it says a directory owns reconciliation, which is not a thing
a directory does. The only coherent reading is a third, informal sense — the
host-local st2 instance — which appears nowhere else and is defined nowhere. The
specification does not settle it either: its gloss of that requirement drops the
word, and its gloss of the neighbour again separates the loop from the root
agent.

Read it as the host-local st2 instance. That is the reading which does not
contradict its neighbour, and it is the owner's to confirm or correct.

### `supervisor`

The control plane (`st2 up`), and the agent named in a declaration that receives
another agent's crash notifications. Both are user-facing — one is a KDL keyword
the operator authors, the other is printed in health output — and both are
routinely called "the supervisor". Write **control plane** for the process and
**declared supervisor** for the agent.

### `status`

The most reused word in the system. It names the presence value, the presence
file on disk, the command that gets or sets presence, the roster field, the
roster filter, a session's lifecycle state as the terminal tool reports it
(running, exited, vanished), and the systemd unit state of the control plane.

Two pairs genuinely collide. Presence and the session lifecycle are both "the
status of an agent" in plain English while answering different questions — is it
declaring itself available, versus is its process still there. And the command
that reads presence differs from the one that reads the unit state by a single
word. Write **presence** for the declared signal and **session state** for the
lifecycle, and reserve bare *status* for the file and the commands that name it.

### `receipt`

Four senses: an archive entry proving a message was handled; the record naming
which hook set is installed; the exit evidence a session's lifecycle owner
records; and a boot marker used in control-plane replacement testing. Only the
first two are st2's own mechanisms. Write **archive receipt** and **hook
receipt**; the others belong to the terminal tool and to test scaffolding.

### `service`

An agent's job type, authored in KDL, and the systemd unit that runs the control
plane. Unrelated concepts, both user-facing. Write **job type** for the former.

### `generation`

One instance of a reused session id, and one rotated log file of which exactly
two are retained. Both answer "what survives a restart", both are
bounded-retention concepts, and both live in lifecycle code a few files apart —
one warning that an old generation's cleanup can unlink a new generation's
socket, the other counting retained log generations. Qualify as **session
generation** or **log generation**; bare *generation* is not safe here.

### `session`

Three senses: the runtime's record of a running task; the terminal tool's
addressable session, which an operator names on the command line; and the POSIX
session created when a task is detached. The first two appear together whenever
a task's liveness is discussed.

### `live`

The mildest entry here, kept because the second sense is new. Established usage
is liveness — a live host lock, and *alive* for sessions and processes, which the
implementation already keeps apart. The DING subsystem introduces **live
composer**, meaning the composer that will receive keystrokes as opposed to one
drawn in scrollback. The senses collide only in prose, so the fix is editorial:
the composer sense always carries its noun.

### `keep`

Three senses, and the third is the dangerous one. It is a garbage-collection pin
the operator authors on a declaration or task; an unrelated flag that preserves
an eval's temporary catalog; and a tag the runtime *forces on* for tasks named
`agent` or `ding`, so their exit evidence survives.

The first and third share a name and a mechanism but not their provenance: an
operator who authors the pin as false on a ding still gets the forced tag. The
authored word therefore does not describe the resulting behaviour, and a reader
who knows only the KDL keyword will predict the wrong outcome.

### `agent`

The declared entity, and a *task* literally named `agent` — the one the compact
declaration form derives, which the runtime special-cases by comparing the name
as a literal string. So a declaration named `mix` has a task named `agent`, and
that task's id is `<host>.mix` rather than anything containing the word. Write
**declaration** or **the agent** for the entity and **the agent task** for the
task whenever both are in scope.

### `identity`

The bare agent name, and — in the machine-readable roster — the field that
carries the fully-qualified bus id. Both are user-facing, and a consumer reading
the roster gets a value that is not what the term elsewhere denotes. Write
**identity** for the bare name and **bus id** for the qualified address, and
never rely on a field's name to tell you which one it holds.

### `spec`

Three senses: the eval-authoring KDL format; the internal lowered model of one
agent's runnable job; and the external agent-spec document that st2 implements.
Write **eval spec**, **agent spec**, and **AGENT-SPEC** respectively. Bare *spec*
in a VRS document means the `spec.md` artifact and nothing else.

### `check`

The declarative assertion inside an eval judge, and one line item of doctor's
health report. Both are user-facing, and both appear in operator-visible output.
Write **judge check** and **doctor check** wherever both could be meant.

### `run`

The act stage of an eval, the eval execution flow, and the process-launch
backend. Prefer **run step** for the eval stage; avoid bare *run* as a noun for
the execution flow.

## Terms without a code home

Some load-bearing terms exist only in prose. That is worth knowing when looking
for their implementation, and it is not by itself a defect — a directory
convention needs no type.

- **Declaration**, **fleet**, **template**, **catalog** — directory conventions
  and command arguments rather than named types.
- **Converge** — describes what reconciliation achieves; there is no separate
  mechanism by that name.
- **Coalescing** — describes how a burst of events is drained into one pass.
- **Root agent** — defined in the vision and requirements, with no
  implementation. It names intent, not a shipped mechanism.
- **Activity status** — required to be distinct from presence, but no field
  carries that name; the nearest shipped signal is a last-activity timestamp in
  the enriched roster.
- **Hook bundle** — the specification's word for what the implementation calls a
  hook set. Prefer **hook set**, which is also what appears on disk.

Two more are undefined rather than merely unimplemented, and both are
load-bearing where they appear:

- **Lifecycle owner** — named once, as the subject of a rule about when exit
  evidence may be removed. Nothing says who it is. By elimination it is presumably
  the control plane, but the reconciler is never called this, so the sentence
  introduces an actor the vocabulary does not otherwise have. Note that
  *lifecycle* does qualify two nouns that are pinned — lifecycle hooks and a
  lifecycle id — which makes the third use read as established when it is not.
- **`face607`** — a bare codename appearing in three places, standing for
  external behaviour: a bounded delivery delay, and a clean-exit reaping rule.
  It is defined nowhere in this repository, yet it is the stated justification
  for both a DING timing constant and a retention decision. A codename that
  cannot be resolved is worse than an unexplained constant, because it looks
  like a reference.

The first three are divergences between the specification's vocabulary and the
implementation's, not merely missing types. They are recorded here so a reader
does not go looking for something that was never built, or conclude that two
words name two things.

## Words that do not bind

A declaration may carry keywords the parser accepts and the runtime ignores:
among them **harness**, **model**, **persona**, **permissions**, **transport**,
**strategy**, and a free-form metadata block. Dropping unknown keys is
deliberate — it is how st2 stays independent of whatever rendered the
declaration — but the consequence is a vocabulary hazard rather than a
convenience: these read as authored configuration and change nothing, and the
grammar gives an author no way to tell a binding keyword from an inert one.

**Harness** and **persona** are the sharp cases, because the same words *do*
bind elsewhere as flags on the generator that writes declarations. One word,
authored in two places, effective in one.

Treat these as metadata, never as configuration, and never cite one as evidence
that a declaration is configured a particular way.
