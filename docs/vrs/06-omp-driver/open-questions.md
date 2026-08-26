# omp driver open questions

Each entry links a spec `DQ-OMP-*`. Questions leave this file when resolved — into
[spec.md](./spec.md) as decisions or `.experiments/` as tested hypotheses.

- **DQ-OMP-1 Deny-path semantics.** What omp does after
  `tool_approval_resolved { approved: false }` — whether the turn ends (Claude's deny path:
  turn ends eventlessly) or the model continues with a denial result. Until captured,
  OMP-T01 accepts a possibly brief misprojection of activity after a denial.
- **DQ-OMP-2 Ask-axis discrimination.** Whether an AskUserQuestion-equivalent surface in omp
  arrives as `tool_approval_requested` with a distinguishable `toolName`, so v1's coarse
  `ask: permission` can be split into question vs permission like the Claude driver does.
  Needs one capture of each prompt kind under forced approval mode.
- **DQ-OMP-3 Mid-turn steer in the live TUI.** `deliverAs: "steer"` was accepted without
  error in print mode; the interactive visual (message lands as a queued steer, not a lost
  send) is unconfirmed. Resolves by repeating the delivery experiment with the trigger fired
  during an active turn and reading the resulting transcript.
- **DQ-OMP-4 Modal interaction.** pi's capture showed an open `/model` modal does not corrupt
  idle delivery; omp's equivalent has not been tested. Same method: open a picker, deliver
  while idle-true, verify the modal survives.
- **DQ-OMP-5 Update-banner suppression.** Interactive boots showed the update banner;
  whether `PI_OFFLINE` / `PI_SKIP_VERSION_CHECK` suppress it was not establishable in print
  mode. Resolves by one interactive boot with the env set. The wrapper ships the env either
  way (harmless if inert).
