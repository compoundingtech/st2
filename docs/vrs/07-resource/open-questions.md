# Resource open questions

Each entry links a spec `DQ-R*`. Questions leave this file when resolved — into
[spec.md](./spec.md) as decisions or `.experiments/` as tested hypotheses.

- **DQ-R4 Runtime bindings on a generated declaration.**
  [Decision 0010](../.decisions/0010-resource-is-a-mediated-write-surface.md)
  gives `st2 resource add|remove|rename` mediated read-modify-CAS-publish, which
  covers a declaration st2 owns. It does not cover a declaration generated
  read-only by configuration management, where the next activation overwrites the
  runtime edit — [#305](https://github.com/compoundingtech/st2/issues/305) lists
  four candidate shapes (overlay file, dedicated fragment, split field ownership,
  stable container Resources) and chooses none. On the catalog measured this is
  not yet live: declarations are writable regular files carrying
  `meta { managed-by "agent-spec-authoring" }`, with no `/nix/store` symlinks.
  Resolves by: a downstream that actually generates declarations stating which
  shape it needs, or a decision that st2 does not model this.

- **DQ-R7 What clears the 987 placeholder reasons?** 987 of 1161 live bindings
  carry the literal string
  `reason="Legacy binding retained without recorded rationale."`, a mechanical
  backfill from 2026-08-22 when
  [#307](https://github.com/compoundingtech/st2/pull/307) made `reason`
  required. Declarations authored since carry real prose — 26 of 27 touched on
  2026-08-26 and 2026-08-27 are placeholder-free — so the field works and this is
  cleanup, not redesign.
  [Decision 0010](../.decisions/0010-resource-is-a-mediated-write-surface.md)
  makes the rewrite cheap. `inactive-reason` is used once fleet-wide, too little
  evidence to judge. Resolves by: a cleanup pass, or a decision to let the
  placeholders age out through ordinary republication.

- **DQ-R8 When does the canonical Agent Spec get synced?** Upstream
  `evals/AGENT-SPEC.md` was last touched 2026-08-20; #307 landed 2026-08-23. The
  canonical document still describes the envelope as closed at name and `uri`,
  and still says unsupported properties such as `_tag` fail validation, without
  mentioning that `reason` is now required. st2 did not diverge deliberately —
  the document is three days stale, and it is the authority st2's own ontology
  cites. Resolves by: an upstream sync. Until then, treat
  [`crates/agent-spec/src/spec.rs`](../../../crates/agent-spec/src/spec.rs) as
  the live contract.

- **DQ-R10 Do the 241 orphaned linked-record files get removed?**
  [Decision 0008](../.decisions/0008-the-linked-record-plane-is-retired.md)
  leaves them in place: most belong to retired declarations, and migrating them
  into `axe work` is blocked because it writes to a gitignored per-worktree path
  those declarations no longer have. They are unreferenced once the verb is gone.
  Resolves by: a decision to sweep `resources/links/` during some later catalog
  maintenance, or to leave them as inert history.
