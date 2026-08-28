# Ordered ledger batch

The st3 plan assigns four stable ledger items across two work phases.

Items 1 and 2 complete before the cold restart. Items 3 and 4 complete after the new worker incarnation becomes ready.

The worker must not repeat a stable item after the injector repeats the earlier work message.

The graph requires the pre-restart revision, restart record, final batch revision, worker report, and supervisor verification.
