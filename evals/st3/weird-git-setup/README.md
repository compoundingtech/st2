# Weird Git Setup eval for st3

This eval materializes a linked Git worktree before it starts one Codex worker.

The graph stores checkout discovery, failure reproduction, repair, verification, revision, and report state.

The held-out judges require a feature commit and reject changes to `main` or its sibling worktree.

Start the st3 daemon. Then run `st3 eval ./evals/st3/weird-git-setup`.

The source fixture came from `compoundingtech/evals` commit `3db48ab56d40ce27dfd94f89d2db9b692d93836a`.
