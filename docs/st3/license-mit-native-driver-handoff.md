# `license-mit` native-driver design

Status: historical st2-to-st3 migration handoff. This document uses the vocabulary of the source st2 eval. It is not the current st3 KDL specification.

The authoritative st2 eval is `evals/st2/license-mit` in this repository. The st2 repository owner maintains this eval.

The eval was copied on 2026-08-26 from `compoundingtech/evals` commit
`3db48ab56d40ce27dfd94f89d2db9b692d93836a`.

The remaining `compoundingtech/evals` copy is a read-only fossil.

## Eval changes

The native-driver change affects `evals/st2/license-mit/eval.kdl` and
`evals/st2/license-mit/judges/coordination.sh`.

Do not add `canonical-agents`. This eval uses the compact eval grammar with native driver blocks.

Replace each Claude `command` and `ding` pair with this shape:

```kdl
harness "claude" {
  model "claude-sonnet-5"
  effort "medium"
  prompt "THE EXISTING BOOT PROMPT"
  args "--permission-mode" "THE EXISTING MODE"
}
```

Use `bypassPermissions` for `mix.sup`. Use `auto` for `mix.worker`.

Replace the judge `command` and `ding` pair with this shape:

```kdl
harness "codex" {
  model "gpt-5.6-sol"
  effort "medium"
  prompt "THE EXISTING JUDGE BOOT PROMPT"
  args "--dangerously-bypass-hook-trust"
}
```

Keep the workspaces, environment, kickoff, timeout, and six held-out judges unchanged.

The native drivers own message delivery. The eval must not declare a `ding` sidecar for these agents.

The eval runner must not write ambient Claude or Codex trust configuration before it starts the agents.

## Coordination wait

The coordination judge must wait for the complete causal loop. It must not grade one early snapshot.

The loop contains these ordered facts:

1. `mix.sup` sends a delegation to `mix.worker`.
2. `mix.worker` sends a report to `mix.sup`.
3. `mix.sup` sends a confirmation to `requester` after the worker report.

Poll the inbox and archive records until all three facts hold or the judge timeout expires.

Use the message filename timestamp to compare the confirmation and report. Keep the existing host-prefix tolerance.

When `ST3_ENDPOINT` is set, refresh the read-only message projection before each poll.

Use `st3 message export "$ST_ROOT"` for that refresh. The normal st2 path needs no refresh.

Keep the autonomy count as a non-gating signal after the loop closes or times out.

## st2 baseline proof

Build the candidate st2 binary. Run only this paid eval from the st2 repository:

```sh
st2 eval --json ./evals/st2/license-mit/
```

The command must exit zero. The JSON report must set `done` to `true`.

The report must contain six gating judges. Every gating judge must set `passed` to `true`.

The coordination output must show the delegation, report, and later confirmation. It must show no post-kick rescue.

The run must leave no eval PTY or exec process. The eval runner must not infer a provider or batch trust entries before launch.

Archive the JSON report, the candidate revision, and the provider versions as the baseline receipt.

## st3 translation proof

Run `st3-migrate evals` after the st2 baseline passes.

The translated KDL must retain two `harness "claude" {}` blocks.

This eval uses the default Codex `gpt-5.6-sol` model judge.

The first checkpoint waits for native readiness and kickoff delivery before it starts non-deadline judges.

The migration writes an explicit completion checkpoint. Its judge uses the coordination script to wait for the causal loop.

Mechanical judges run through the asynchronous exec runtime. The coordination wait does not block reconciliation.

Run only the translated `license-mit` eval. It must produce a pass verdict and an empty final eval scope.
