# ghost-bug-codex — the ghost-bug debug loop, run by codex seats

This st3 cell asks two native Codex agents to find a shared-default mutation bug in `labelkit`.
This teaches a bounded delegate-debug-verify loop where a shallow patch or a test that never failed
cannot pass.

Start the daemon with `st3 up`. Run the cell with `st3 eval ./evals/ghost-bug-codex`.

Held-out judges (identical logic to ghost-bug): isolation (author-gated to `gbx.fix`), suite-green,
root-cause (two blind probes), **regression mutation-valid** (RED on the buggy BASE src — the integrity
bar, ported verbatim), coordination.

Fixture `worker/` reuses ghost-bug's labelkit (owner-pinned `gbx.fix`); `worker/AGENTS.md` +
`sup/AGENTS.md` are intentionally pre-seeded complete Codex personas. The frozen `worker/_git`
snapshot rehydrates as `.git` only inside the temporary eval root. The KDL uses native `codex {}` blocks.

The `receipts/` directory contains the passing proof.
