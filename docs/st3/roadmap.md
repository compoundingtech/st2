# st3 product roadmap

Status: planning input, not a release schedule.

This file records work that needs a future design or implementation session. Current fleet work remains in st3 missions.

## Product surface

- [ ] Make the root README describe Small Talk and st3 first. Keep an `st2` section that links to a separate legacy README until st2 is removed.
- [ ] Expose an `st` executable that runs the st3 command surface. Keep the `st3` executable during the migration.
- [ ] Reduce the general design document to stable system decisions. Keep detailed technical contracts in focused documents.

## Agent continuity and presentation

- [ ] Build agent session freeze-drying and rehydration into st3. A long graph wait can suspend a harness and retain its resumable session state. A later graph change can restore that session before work resumes. A human gate is one important suspension case. The design must define safe suspension points, stored state, timing policy, and recovery failure behavior.
- [ ] Define a harness-neutral session view contract in the harness drivers. The contract must support custom conversation views in SwiftUI, React, and other native clients. It must describe ordered messages, roles, tool activity, runtime status, errors, usage, and incremental updates. It must not expose driver-specific storage as the public contract.

## User interfaces

- [ ] Hold one product design session for `stui` and the Small Talk mobile app. Define the shared product model before the two interfaces diverge. Specify separate v0 and v1 outcomes, supported views, human actions, live-update behavior, transport, authentication, and offline behavior.

## Examples and migration knowledge

- [ ] Add a tested st3 examples collection. Show standing agents, queued work, human gates, concurrent intake, recurring work, planning, revision generations, nested missions, resource observation, attention, and cleanup. Keep each example smaller than an eval and validate every KDL file in tests.
- [ ] Capture the st2 repository agent's current responsibilities before its st3 migration. Store current work in missions, stable product knowledge in repository documentation, and behavioral guarantees in tests or examples. Do not copy raw conversation history into a permanent prompt.
