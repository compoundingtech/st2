# signal-rename-codex — a graph-owned cross-package rename

This st3 eval uses four native Codex agents.

The st3 plan owns decomposition, assignment, sequence, nested progress, required revisions, final verification, and cleanup.

The agents rename the Signal product to Beacon across a base package, two consumers, and the root configuration.

The agents preserve `AbortSignal`, `controller.signal`, signal cancellation options, `SIGTERM`, and `SIGINT`.

## Work graph

```text
materialize
    |
start-team
    |
open-base-compatibility           sig.base
    +--------------------+
migrate-relay              migrate-hub
sig.relay                  sig.hub
    +----------+-----------+
       update-root-and-config     sig.sup
                 |
       close-base-compatibility   sig.base
                 |
       integrate-and-verify       sig.sup
                 |
         held-out-judges
                 |
        publish-final-report      sig.sup

cleanup runs after every terminal result
```

Each assigned parent step contains an up-front nested plan. st3 publishes one Small Talk message when the parent becomes ready.

Inherited nested steps use that parent message. An explicit nested reassignment publishes a new Small Talk message.

The native driver only transports Small Talk graph messages. It does not create a separate message type or source of truth.

Each lane publishes a `vcs.revision` resource claim. The plan does not infer completion from prose or exported messages.

The final report remains a message because communication is its product. A resource claim records its publication.

## Run

```sh
st3 eval ./evals/signal-rename-codex
```

`materialize.sh` creates a bare origin and four authored clones from the frozen synthetic graph.

The script holds the end-to-end test outside every agent workspace. It also copies each persona to its clone as `AGENTS.md`.

Five mechanical judges grade the integrated clone. A bounded Codex judge inspects the plan claims, work state, revision products, and Git history.

The `receipts/` directory contains historical runs. Each receipt records the exact KDL hash that produced it.
