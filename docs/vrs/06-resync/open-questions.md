# Resync open questions

- **DQ-R1 Window tuning.** The 500 ms immediate and 5 s coalesced windows are
  provisional constants (RESYNC-T02). Resolve by observing real notification
  volume and wake latency on a working catalog, then fixing tested bounds —
  same method as the stream ring bound ([DQ-S3](../04-stream/open-questions.md)).
- **DQ-R2 Per-binding `notify` override.** Whether a
  `notify "immediate"|"coalesced"|"silent"` attribute on resource bindings is
  ever worth its cost: it extends the Agent Spec grammar (canonical evals
  authoring authority) and couples to #305's Nix-regeneration question.
  Deferred until profile defaults are observed to be wrong somewhere (Q3).
- **DQ-R3 Write attribution beyond static classes.** If external edits into
  agent-authored stores turn out to need notification (or st2-mediated writes
  to immediate carriers turn out to be noisy), classification needs real
  authorship detection. No current evidence; revisit with RESYNC-T01.
