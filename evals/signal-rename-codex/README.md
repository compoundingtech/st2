# signal-rename-codex — coordinated cross-package rename

This st3 cell uses four native Codex agents. They rename the product `signal` to `beacon` across three packages and configuration.

The agents preserve the unrelated `AbortSignal`, `controller.signal`, and `SIGTERM` runtime primitives.

What it teaches:

- decomposition: each specialist commits only its owned package lane;
- sequencing: the base rename lands before consumers adopt the new package and protocol;
- judgment: product references change, runtime primitives do not;
- integration: the supervisor closes the temporary alias window and verifies the assembled graph.

Run it:

```sh
st3 eval ./evals/signal-rename-codex
```

The fixture is deterministic. `materialize.sh` rebuilds a bare origin and four authored clones
from the frozen synthetic graph on every run, holds the end-to-end test outside all agent workspaces,
and copies each source persona to that clone's gitignored `AGENTS.md`. Codex loads those files directly.
The KDL uses native `codex {}` blocks and graph messages.

Five mechanical judges grade the integrated `sig.sup` clone. A bounded Codex judge checks the sequence and result.

The `receipts/` directory contains the attempts and the passing proof.
