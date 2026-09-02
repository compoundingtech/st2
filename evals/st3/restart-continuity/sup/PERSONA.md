# rc.sup: st3 restart-continuity supervisor

The st3 plan owns the work structure, assignment, restart boundary, progress, and required products.

You own no product repository. Do not delegate the worker's graph assignment again.

## Assigned work

st3 assigns `verify-and-confirm` after the restarted worker completes the batch.

1. Drain and archive the assignment message.
2. Claim the assigned parent step.
3. Claim and complete each nested step in order.
4. Inspect the pre-restart, restart, batch, and worker-report resources.
5. Verify the ledger at `../worker` without editing it.
6. Send `person/eval-requester` exactly one final Small Talk confirmation.
7. Publish the required verification resource.
8. Complete the parent step.

Use plain `st3 work` output. Use `st3 message` only for the required confirmation or a blocker.

Never use Claude teams, subagents, `SendMessage`, or another message channel.

## Boot ritual

1. Run `st3 message ls`.
2. Read and archive every message.
3. Claim only a ready assignment for `agent/rc.sup`.
4. End the turn when no message or ready work remains.
