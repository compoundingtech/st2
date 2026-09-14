# st3 product roadmap

This file records accepted future work. Active fleet work belongs in st3 missions.

## Agent continuity

- [ ] Freeze-dry an idle harness session at a long graph wait or human gate.
- [ ] Rehydrate the exact session when the graph makes work ready again.
- [ ] Define safe suspension points, stored state, timing, and recovery failure behavior.
- [ ] Add a harness-neutral session view for SwiftUI, React, and other native clients.
- [ ] Include ordered messages, roles, tool activity, status, errors, usage, and incremental updates.

## User interfaces

- [ ] Hold a product design session for `stui` and the Small Talk mobile application.
- [ ] Define separate v0 and v1 outcomes for views, actions, live updates, transport, authentication, and offline behavior.
- [ ] Build structured sitrep data from graph queries and exec missions instead of a permanent Markdown report.

## Runtime containment

- [ ] Add optional CPU, memory, process, and disk limits for each mission-owned runtime.
- [ ] Use cgroups on Linux where available.
- [ ] Evaluate the macOS sandbox and resource-limit surfaces before selecting an enforcement contract.

## Harness accounts

- [ ] Design quota-aware account pools for Codex and OMP harnesses.
- [ ] Keep each conversation sticky to its selected account.
- [ ] Select an eligible account from current usage and health information.
- [ ] Support round-robin or threshold-based selection without moving active conversations.
- [ ] Keep credentials local and make account failure isolate to one account.
- [ ] Evaluate [Comradex](https://github.com/nicosuave/comradex) as a reference for Codex account routing.
