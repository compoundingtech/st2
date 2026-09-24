# Friend-ready delivery gate

The live cross-host monitor sends an original message with a unique token every five minutes, alternating Hetz→Silber and Silber→Hetz. It waits for one exact linked reply, archives the receipt, and raises actionable attention after three consecutive failures in one direction. The monitor runs as a user service with linger enabled; its append-only event log and per-direction state live under the local `st3/message-soak` state directory.

`scripts/st3-message-soak-report` is the release gate. With no overrides, it passes only when the service is active, neither direction has an unresolved failure, and the preceding 72 hours contain at least 400 successful exact receipts in **each** direction, no recorded failure, no evidence gap over 15 minutes, and a current receipt within 15 minutes. The first complete-failure-logging monitor start must predate the window. Test-only environment overrides allow short synthetic fixtures; release evidence must use the defaults. A failed report is a release hold, not permission to edit the log or reset state.

This is an operational smoke gate, not a mathematical proof of 99.999999% availability. That target needs longer production observation, explicit SLO accounting, and failure-injection coverage. A passing 72-hour gate supports a minimally friend-ready trial; the continuous monitor and attention escalation remain enabled afterward.

The gate does not replace graph gate evidence for individual missions: work completion still requires its declared products and gates. Preserve the report output and service journal as release evidence, and do not mark a mission complete on a `sent` message alone.
