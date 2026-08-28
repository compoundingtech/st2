# License MIT — the smallest team loop

This st3 cell runs the license MIT task with two native Claude Sonnet agents.

- **Task** (`task.md`): one instruction — "the license should be MIT" — into `lmc.sup`'s inbox.
- **Team/persona mechanism**: the fixture pre-seeds `CLAUDE.md` and `PERSONA.md` in both workspaces.
  The KDL uses native `harness "claude" {}` blocks. `lmc.sup` coordinates and owns no repo;
  `lmc.worker` owns the `widget` repo and makes/commits the change; `lmc.sup` verifies read-only and
  confirms. Every eval starts from the frozen `worker/_git` snapshot, rehydrated as `.git` only inside
  the throwaway catalog.
- **Judges** (all held-out): structural isolation (sup owns no repo), the coordination loop on the bus
  (delegate → report → verified-confirm post-dating the report), `LICENSE` is canonical MIT, `package.json`
  declares MIT, the change is committed with a clean worktree, and a Codex judge that the confirmation
  cites real evidence (not a bare "done!").

Start the daemon with `st3 up`. Run the cell with `st3 eval ./evals/st3/license-mit`.

The `receipts/` directory contains the attempts and the passing proof.
