# st3 agent migration

Status: current isolated migration guide.

This guide moves one agent from st2 to st3. It does not import st2 history or share a live catalog.

Do not use this guide as authority to change a live agent. A person must select each agent and authorize its cutover.

## Source repository

Keep st3 declarations in a private repository that is separate from the st2 catalog.

Store mission KDL, host KDL, redacted host documents, and migration notes there. Do not store the claims database or runtime workspaces.

Do not store credentials, raw private measurements, message history, or personal data in the repository.

## Prepare one host document

Write stable facts that an agent on the host needs. Do not put current work in this document.

Store the bytes first:

```sh
st3 doc put hosts/local.md --as doc/hosts/local
```

The command returns the exact SHA-256 reference. Put that reference in a host declaration:

```kdl
version 2

host "local" {
  document "doc/hosts/local@SHA256"
}
```

Preview and publish the host declaration. Publication fails when the exact document is absent.

## Prepare a standing mission

Use an open mission when the agent must remain available after its current work is exhausted.

```kdl
version 2

mission "agents/example" state="ready" {
  goal "Keep one agent available for current graph work."

  agent "example" {
    workspace "/work/example"
    harness "codex" {}
    restart "always"
  }
}
```

Do not add a completion block. The run becomes standing and continues to assert the agent.

The harness prompt is optional. st3 generates `.st3/boot.md` and appends the required boot instruction.

Put current work in mission steps. Do not put it in the boot file, host document, or harness prompt.

## Review before start

Run these commands before any cutover:

```sh
st3 preview hosts/local.kdl
st3 preview missions/example.kdl
st3 publish hosts/local.kdl --as person/operator
st3 publish missions/example.kdl --as person/operator
```

Publication does not start the mission. Inspect the subject diff and document hashes before the next action.

## Rehearse in isolation

Run the `agent-migration-rehearsal` eval before a live cutover. The eval uses a separate run workspace and no st2 state.

The rehearsal proves these facts:

- the exact host document exists;
- the runtime renders the canonical boot file;
- an agent claims and completes normal graph work;
- mission cleanup stops the test agent.

## Start one st3 run

Use a readable run ID for the first trial:

```sh
st3 mission start agents/example \
  --id agents/example/pilot \
  --workspace /work/st3-runs/example \
  --as person/operator \
  --follow
```

Verify the exact mission run, agent subject, runtime incarnation, generated boot file, and work queue.

The new agent subject has the form `agent/agents/example/pilot/example`.

## Cut over the live identity

Stop the old st2 agent only after the authorized operator accepts the isolated st3 proof.

Use the st2 supervision mechanism for the old agent. Do not remove its history or catalog files during the trial.

Verify the old process is stopped. Keep the verified st3 run active, and verify its process arguments and graph state again.

Run both systems during the fleet migration. Do not bridge their messages or copy old inbox state.

## Revise or cancel

Publish a new mission revision when the agent needs new work. A revision creates a successor run generation.

Publish a named mission-run cancellation to stop the st3 agent:

```kdl
version 2

mission-run "agents/example/pilot" {
  cancellation "operator-stop" {
    reason "The migration trial ended."
  }
}
```

Omission never cancels a run. Deleting a local KDL file never changes graph state.

## Fleet sequence

Move one low-risk agent first. Keep each successful agent on st3 while the next agent moves.

For each agent, repeat the same preview, isolated proof, start, verification, and old-runtime stop sequence.

Do not create a root, supervisor, or chief-of-staff runtime for host control. The st3 daemon owns runtime supervision.
