# Friend-ready candidate status — 2026-09-25

**Release hold.** Source `ea74722` for `st3` and `2af1b1b` for `stui` is
deployed as host-native binaries on Hetz and Silber. The direct-network iOS
source is merged at `e49c097`. The earlier 11:40:44 UTC soak windows were
invalidated by the CPU hotfix rollout. New windows began at 13:55:44 UTC after
both daemons restarted; their full 24-hour idle and 72-hour bidirectional
delivery reports are still due.
Do not describe the friend-ready trial as released until both reports pass and
their retained evidence is reviewed. The continuously running gate watcher
notifies the standing st3 operator of post-due transitions; it does not turn a
short diagnostic into release evidence.
The [friend trial handoff](friend-trial-handoff.md) is staged for that review,
not an invitation to start the trial while this hold is active.

## Four product checks for today's friend-ready decision

| Capability | Current evidence | Remaining check |
| --- | --- | --- |
| Declarative agents | Standing and mission-owned seats are visible in the graph; OMP and OpenCode were declared in `st3-network` main and started on their assigned hosts. The restarted OMP seat is ready; both strict doctors pass. | Keep the OMP seat ready overnight. |
| Addressable inboxes | The continuous monitor has current exact linked receipts in both directions; OpenCode and the restarted OMP seat have sent linked native graph replies. | Keep the monitor running through the new window. |
| Clean-session recovery | Codex's controlled fresh-thread restart retained graph work; the latest controlled OMP restart produced a new ready incarnation and an exact linked native inbox reply. | Keep the new incarnation ready overnight. |
| Visible work | CLI mission/work detail and iOS Control expose current runs; the installed TUI has cards, actions, readable mission labels, active-first nested Chat agents, and cleaned channel wrappers. | Nathan retests the exact installed TUI build. |

These checks are provisional. The 24-hour idle and 72-hour delivery reports remain
the release gates after the final tested rollout.

## 13:55 UTC Hetz idle CPU hot-loop repair

- At the two-hour diagnostic point, Hetz had 82 quiet intervals with median
  daemon CPU 78.3% of one core, above its unchanged 15% gate. Silber had 81
  quiet intervals at 16.3%, below its 50% gate. RSS did not grow over the
  short window; replication and both strict doctors stayed healthy. This was
  an early risk alert, not a 24-hour verdict.
- The ready product step's work-wake messages were read and closed at 11:49,
  but the wake projection ignored that durable acknowledgement and kept its
  retry deadline in the past after the step was released. Hetz's reconciler
  repeatedly rescanned the graph. Claiming the step at 13:45 removed the
  overdue deadline and CPU fell from roughly 80% to roughly 20% of a core.
  Regression `work_actions_require_an_active_incarnation_bound_lease` failed
  before `f9a9116` and passes after it; the full locked `st3` suite passed
  413 library, 88 CLI, 5 client CLI, 21 contract, 27 example, and 12
  operational tests. The fix treats a read or closed wake in the current
  incarnation as acknowledged when scheduling reconcile work.
- `ea74722` temporarily reverts the separate staged OMP model gate so this CPU
  hotfix can be installed while Nathan's declared model remains absent from
  OMP's registry. The model gate patch is retained at `5e3b80f` for reapplication
  with his chosen exact model. Hetz installed `st3` SHA-256
  `7a2be4eedd262274a2a0d7a42f9b915b237acff8b8d21d4fa2f2ad948278404e`
  (daemon PID `3356763`, replication PID `3356765`); Silber installed
  `fdc71b0edebd6d8b9418cabc6079bad9f754ba3154aa12ddc3024aa74b54cd37`
  (daemon PID `32095`, replication PID `32097`). Each host retains the previous
  daemon binary in its local `rollout-backups/ea74722-20260925-1354/`.
  The installed `stui` binaries remain at `2af1b1b`.
- Both strict doctors pass after the restart: signed peers up, zero unresolved
  or unhealthy replication records, and the same ready OMP incarnation. Direct
  LAN, `.local`, and tailnet gateway routes still return unauthenticated 403.
  The exact delivery monitor logged a Hetz-to-Silber linked receipt during the
  rollout at 13:55:15 UTC. New release markers are Unix `1790344544`
  (13:55:44 UTC), the first healthy post-rollout sample. The one-sample
  preflight passes on both hosts; the full windows and a live ready-step CPU
  comparison are still pending.

## 11:40 UTC OMP and macOS TUI repair rollout

- `8696c2e` stops the ST2 launch placeholder from overwriting the live OMP/PI
  extension-channel observation after 15 minutes, and stops a startup deadline
  from firing for an incarnation that was already ready. Both regressions failed
  before the patch and pass after it. `2af1b1b` adds a macOS terminal watcher
  because Crossterm can stay inside its event poll on a closed tmux PTY. The
  macOS release PTY smoke now passes quit, signal, delayed getter, direct PTY
  hangup, and tmux session close; the installed binaries pass the same smoke on
  both hosts. A Claude conversation fixture verifies that Chat hides ST3 channel
  wrappers and delivery markers. The locked suites passed 413 `st3` library,
  88 CLI, 5 client CLI, 21 contract, 27 example, 12 operational, and 40 `stui`
  tests (two live benchmarks ignored).
- Hetz installed SHA-256 `ecbfea8e8627e66d48fd6cf991c44aa363cdfad0ff0133cbd2e42d5b7a75e0d5`
  for `st3` and `b959b33267a69e3dac212b3a345c538b6e838a488a8d12d9b453492a02eac431`
  for `stui`; daemon PID `1833382`, replication PID `1833384`. Silber installed
  `1f0ccb7a80b403f3db77a5a414448d251880227ae9e1675f4a33dde37a24b0fc`
  and `977a58e49f96fb80eca847332070528e20074c6d35ad0726915abf738f1f0cb5`;
  daemon PID `17396`, replication PID `17403`. Each host retains its previous
  tested pair under its local `rollout-backups/8696c2e-20260925-1132/`.
- Both strict doctors pass: current native harnesses ready, signed peers up,
  zero unresolved or unhealthy replication records. The OMP seat restarted at
  11:39:21 UTC with a new ready incarnation. Messages held during its stale
  observation were delivered after the restart. OMP answered the first two
  probes as terminal text; after an explicit CLI instruction it sent linked
  graph reply `message/f408450501cf4b88`, which reached and was closed by COS.
  The exact-token delivery monitor recorded another Silber-to-Hetz
  receipt at 11:40:52 UTC, after both daemon restarts. Silber's gateway returns
  unauthenticated 403 over direct LAN, `.local`, and tailnet HTTP. The native
  iOS simulator used its existing paired credential over direct LAN HTTP after
  rollout: Now showed four actionable items, all collection reads succeeded,
  the live ST3 timeline rendered, and six connections remained established
  through 30 seconds of long polling with no transport errors.
- The 24-hour idle and 72-hour delivery markers were reset to Unix
  `1790336444` (11:40:44 UTC). The early preflight passes with three
  continuous healthy post-rollout samples per host. The full windows are still
  the release gates; early CPU/RSS samples include restart warm-up and do not
  establish a flat 24-hour result. OMP 18.1.22 silently fell back from its
  declared `openai-codex/gpt-6-sol` model to `gpt-5.6-sol` because the former
  is absent from its registry. COS is obtaining Nathan's model choice before
  changing the `st3-network` declaration. The declared model must be honored
  or fail visibly before this seat can be called friend-ready.
- Source commit `5e3b80f` adds an exact OMP registry check before the wrapper
  claims its seat. The new regression rejects the unavailable requested model
  and accepts an exact custom selector; all 836 active `st2` library tests pass
  (one ignored). This commit was temporarily reverted by `ea74722` so the CPU
  hotfix could deploy while the current declaration remains unsupported.
  Reapply it with Nathan's selected model, then reset the soak windows after
  the host restart.

## 11:13 UTC earlier TUI usability rollout

- Source `82fe455` integrates the TUI usability fixes and corrects the macOS
  PTY smoke test to drain output while the child exits. The Mac terminal guard
  reported successful alternate-screen, raw-mode, and cursor restoration; the
  corrected debug and installed-release smokes pass quit, signal, delayed
  getter, and hangup on both hosts, plus the debug panic case. The `stui` unit
  suite passes 40 tests (two live benchmarks ignored); the earlier full locked
  `st3` suite and Nix package build passed for the same production changes.
- Hetz installed SHA-256 `6f74516e5bddd599440be7efd624b1cb1edb12c737bbb1e0c2e2837db214f309`
  for `st3` and `7d9945b618c455a5455779ec0d07aa27f5fc539a46b4f019d393e863f89c70a9`
  for `stui`; daemon PID `1777376`, worker PID `1777378`. Silber installed
  `68718549bd4ddb9d2f1622cfe8bb4024a57eed5392317ae0e9cf96f38c071b14`
  and `61eaefd811552227b58379233a8e648790607df3de896c40a83ace741702aa2a`;
  daemon PID `46725`, worker PID `46727`. Each host retains its previous pair
  under `~/.local/state/st3/rollout-backups/82fe455-20260925-1110/`.
- Both services are active. Both doctors report signed peers up, zero unresolved
  records, zero unhealthy projections, and one shared pre-existing warning:
  `agent/fleet/pty-rust/omp` has a stale driver observation. This is a strict
  doctor failure and remains a release hold; COS is investigating that seat.
  The post-restart delivery monitor logged an exact Hetz-to-Silber receipt at
  11:14:11 UTC. The Silber OpenCode agent linked its exact-token reply to
  `message/0c1da1f938d3f1e9` after the restart. The OMP post-rollout reply is
  pending.
- Silber's paired gateway returned unauthenticated HTTP 403 through its LAN IP,
  `.local` name, and tailnet IP after
  the restart. The rebuilt iPhone 18 Pro simulator app used its existing paired
  credential over direct LAN HTTP after the rollout: Now showed four actionable
  items, the ST3 conversation rendered with the older-history marker above its
  content, and three long polls remained connected for 30 seconds with no
  transport error. Nathan's TUI retest remains pending. The final 24-hour and
  72-hour markers were pinned to Unix
  `1790334798` (11:13:18 UTC), after both service restarts and at the first
  healthy two-host sample. The early idle evidence preflight passes with three
  continuous, healthy samples per host. This short preflight is not a release
  gate pass; the full 24-hour and 72-hour reports remain due.
- The OMP readiness warning is a real delivery defect. At 11:05:13 UTC the
  graph recorded the live OMP harness as indeterminate and emitted a readiness
  deadline fault after its 11:05:03 idle observation. All OMP/PTY processes
  remained alive, but both a COS message and the final-build native inbox probe
  remained `sent` without a reply. This seat has no current work; remediation
  is required before friend-ready release.

## 10:08 UTC TUI, iOS, and direct-network rollout

- `5c41edc` merges the TUI and iOS presentation/refresh repairs with bounded
  reads. Hetz and Silber now run host-linked release `st3` and `stui` binaries
  built from that commit. A first Hetz attempt copied the Nix-linked binaries
  into a service environment with a system allocator preload; its runtime
  linker rejected that combination. The prior binaries were restored, services
  returned to health, and the host-linked build was tested before the final
  install. Both hosts retain rollback copies of the pre-rollout binaries.
- Hetz installed SHA-256 `010bd61a3dc64a2e1cf08b4e3bd83b76ee98c3b435e1fac790c4280dcd3866eb`
  for `st3` and `6179df84d985ab13381f7ec738c2fb221976920dc9fe919086b4699094aa8ecd`
  for `stui`; daemon PID `1465196`, replication PID `1465194`. Silber installed
  `ccebf57f6225aeb0ced164a17361da11fc75a09973ced9c0a15fbecade6b1322`
  and `33f95879745cc7c540baff2a4e43330a7f2941bea7b5d339c45605ee4efa1e89`;
  daemon PID `31613`, replication PID `31615`. Both strict doctors pass after
  one graph-authorized repair of an unrelated stale PTY wake contradiction.
  Signed peers are up with zero unresolved and unhealthy records.
- Native messages reached the iOS worker from Hetz and its linked reply reached
  Hetz from Silber after the service restarts. The paired gateway returns the
  expected unauthenticated 403 through the LAN IP, `.local`, and tailnet routes;
  no privileged daemon socket is exposed. A rebuilt native simulator app using
  the direct LAN route remained Connected, showed seven actionable Now items,
  and held timeline and event long polls for 60 seconds without transport errors
  after the Silber restart. The `.local` pairing and direct tailnet app paths
  were proven before the restart. Final app source `e49c097` adds scoped local
  and tailnet HTTP acceptance and was pushed after this daemon build; its
  TypeScript typecheck, 13 logic test files, and iOS export of 610 modules pass.
  Physical-device Local Network permission presentation remains untested.
- The follow-on TUI usability feedback was added by revision
  `9f7f6bc711b514a4bb221cfb53b99039a1c39097b65e7987497c67d323a10845`
  to the existing UI run. The completed initial fixes carried forward; the
  active integration step restarted under its original `restart-active` policy
  and was completed with the same rollout evidence. The new TUI layout step is
  claimed. The revised source is committed on `st3-network` main at
  `e19d61d`. It selects `when-idle` for future
  revisions; that setting did not change this cutover.
- All prior 24-hour idle and 72-hour delivery markers remain diagnostic only.
  Start new complete windows only after the new TUI work is integrated, installed,
  and checked. Do not infer a soak pass from the short post-restart checks above.

## 08:55 UTC Codex recovery and bounded-read rollouts

- `b4b23f0` fixed fresh-thread Codex restarts, raised the control frame guard
  to 64 MiB, and stopped repeated short-exit relaunches with one durable
  attention request. The controlled restart of
  `agent/fleet/st3/recovery-probe-20260925` changed its thread ID from
  `01a0d7b9-0a5a-7400-8c2a-084b4056fbd5` to
  `01a0d7b9-6291-7050-9e1e-ac6a7c3eeb16`; the probe was stopped. Both
  hosts passed strict doctor and COS's independent checks after this rollout.
- `623f9cb` stopped repeated full-history `thread/read` calls. Status reads
  now omit turns; live events and a hard-capped 2 MiB transcript tail settle
  native delivery. The focused 81 Codex tests include delivery and exact
  receipt on transcripts larger than the former control limit, with bounded
  request and transcript bytes. The full st2 suite passed with the documented
  optional otel skip, and st3 passed 410 library and 87 CLI tests.
- Hetz installed `623f9cb` SHA
  `42dbf325460808bc88b47240e4e81deabda599f9024b071dcb722f5ee8134ca8`
  at 08:54 UTC, daemon PID `1297290`, replication worker PID `1297308`.
  Silber installed SHA
  `6681e3c7ff337dd27b0e3717064e86fdfa35146587b7c1c010718d48b24b60aa`
  at 08:55 UTC, daemon PID `50189`, replication worker PID `50198`.
  Strict doctors passed on both hosts, signed peers were up, and unresolved
  and unhealthy replication counts were zero. The delivery monitor recorded
  a Hetz-to-Silber exact linked receipt at 08:54:44 UTC. COS independently
  checked both hosts at 08:55:50 and 08:56:09 UTC: both new PIDs were live,
  both signed peers were up with zero pending, invalid, or unhealthy records,
  both standing seats were reachable, and the Silber restart notice arrived
  natively. `nix build .#st3 --no-link` passed from `623f9cb`, including
  release-profile checks for 410 st3 library, 87 CLI, and 17 stui tests.
- The old 05:08:50 UTC idle window now has two daemon PIDs on each host and
  a retained Silber bad sample. Its alert is an expected release hold, not
  evidence for the next window. The original append-only logs remain intact.

Monitoring commit `8817b15` adds an early post-rollout evidence preflight. It
notifies the operator of a bad sample, PID change, stale host, or excessive gap
before the 24-hour deadline, with retry after a failed notification send. The
preflight is currently healthy; it cannot pass the release gate.
Watcher update `cfe036f` also sends an early CPU-risk notice after two hours
and 60 quiet intervals per host if the unchanged CPU limits are exceeded. It
is installed on Hetz; its own fixture test and first service run passed. The
24-hour report remains the authority.
The final-audit mission is published but not started early. Once both complete
windows are due, the gate watcher queues one pair-of-windows-keyed claimable
audit run even if either report fails, so diagnosis or final review survives a missed
notification. Its step reruns both full reports as mechanical gates before
completion. The watcher and queue helper passed duplicate-start,
lost-response, and reset-window fixtures; this does not preempt either live soak gate.

At 04:17:39 UTC the delivery monitor recorded a Silber-to-Hetz `stage=send`
failure. The exact request (`message/b67a6fcad1e2b29e`) and one linked reply
(`message/992b7fc53bef47b0`) were already durable, but the Fabric `exec`
caller timed out without a response after the send committed. The retained
evidence does not establish whether that exact remote CLI process exited
promptly. The failed monitor line remains in the retained log. Commits
`602e090` and `506cd51` recover an exact-token
request from the recipient's durable mailbox and then require its linked
reply, allowing the full receipt deadline for recovery. A deterministic test
discarded the Fabric response after a real remote commit and logged one
recovered request and one successful receipt. The repaired monitor restarted
at 04:25:22 UTC and logged a fresh exact receipt in both directions by
04:30:54 UTC. Its 72-hour marker was reset to that monitor start; the old
window cannot pass. A six-minute one-per-direction diagnostic passed, but it
is not release evidence.

The same response-loss recovery was extended to the local Hetz send path in
`86d7df0`. Two isolated live probes deliberately discarded a successful local
send response and substituted malformed JSON after a successful send;
both recovered the single exact-token durable request and its linked reply in
26 seconds. The continuing monitor was stopped while sleeping and restarted
on the new script at 05:48:41 UTC. The first new exact Hetz→Silber receipt
arrived at 05:49:08 UTC, 2 minutes 23 seconds after the preceding successful
Silber→Hetz receipt; the append-only log has no failed probe or excessive gap
from this restart. The 05:08:50 UTC release markers were not reset. The full
72-hour report remains the only release verdict.

## Candidate and recovery proof

- The separate replication workers were restarted under COS's prior ACK after
  the operator recovered on a fresh thread. Hetz changed from PID `79278` to
  `1160828`, running the installed `3db863d` binary SHA
  `4659b2d9183f03942d3577232505795d86382a47e268e7fe79f529f5dfec3c61`.
  Silber changed from PID `2106` to `90745`, running its installed binary SHA
  `0d90adb81844c35a4cee28beb42138a07fa9073deb0a62d9dcb9998d33b7fbc1`.
  The daemon PIDs stayed `595654` and `45998`. Both immediate strict doctors
  passed, each signed peer was `up`, and both replicas had zero pending,
  invalid, or unhealthy records. The gate watcher reported a healthy idle
  preflight at 08:15 UTC; the delivery monitor remained active and its last
  pre-restart exact receipt was at 08:14:16 UTC. COS was asked to check each
  host independently after its worker restart. Neither gate window was reset.
- A connected iOS simulator exposed that the old paired-device grant contained
  only projection and terminal reads plus attention and launch control. Chat
  sends, mission/work actions, and terminal input could not work through that
  credential. `3db863d` preserves that limited default and adds an explicit
  `--full-control` choice made by the initiating person on the trusted local
  socket. A paired-gateway contract test proves the remote cannot initiate it,
  the chosen scopes are granted, and revocation cuts access. Existing limited
  devices are not silently widened. After the connected simulator check, the
  exact temporary full-control test device was revoked; `devices --all ls`
  confirmed it revoked while both pre-existing limited devices stayed active.
- The new daemon was installed on Hetz at 05:03:33 UTC (PID `595654`, binary SHA
  `4659b2d9183f03942d3577232505795d86382a47e268e7fe79f529f5dfec3c61`)
  and Silber at 05:05:59 UTC (PID `45998`, binary SHA
  `0d90adb81844c35a4cee28beb42138a07fa9073deb0a62d9dcb9998d33b7fbc1`).
  Prior binaries are retained locally under each host's
  `rollout-backups/3db863d-20260925-0502/st3-before`. Both strict doctors
  passed after convergence; Hetz briefly reported operation-projection drift
  during replication and passed on the next check. COS independently confirmed
  each new PID, both original agent incarnations, the same Codex thread, signed
  replication with zero pending/invalid/unhealthy records, and native receipt
  of `message/1ea7e8bccb7ce333` after the Silber restart.
- Each daemon restart was preceded by a normalized ACK from the independent COS
  seat. Rollback binaries are retained locally on the corresponding host.
- The prior `0da07d7` window reached a diagnostic Hetz quiet CPU median of
  19.7% over 26 quiet intervals, above the unchanged 15% limit. `b6837bb`
  keeps usage samples and lease renewals durable, replicated, and visible to
  clients without waking the full reconciler for those claims alone. It
  retains full reconciliation for harness readiness, mission transitions,
  non-quiet replicated claims, and projection recovery. The full st3/stui
  suites passed (408 st3 library, 86 CLI, and 17 stui tests; 2 live stui
  tests ignored). A new full-day measurement is required to establish whether
  this reduces whole-daemon idle CPU enough.
- `b6837bb` was installed on Hetz at 04:09:08 UTC (PID `482626`, binary SHA
  `567f1a2c48517ba86b326a2d6f629c330915172cca7579e20e9a1177e1dafcc8`)
  and Silber at 04:13:59 UTC (PID `76944`, binary SHA
  `d1527811ff7b4f08ed42f0e3f0a5c9fa1512328133bc0527be41862e3cf324ee`).
  Both doctors passed. COS independently confirmed each restart left both
  seats reachable, signed replication healthy, and native inbound delivery
  working (receipts `message/d6a01a230f02e039` and
  `message/dc64cdd57e21d3ae`). The prior binaries remain in each host's
  `rollout-backups/b6837bb-20260925-0415/st3-before`.
- The previous `0da07d7` rollout had both doctors pass; both signed peers were up with
  zero pending, invalid, or unhealthy replication records. Exact linked message
  receipts passed Silber-to-Hetz in 24 seconds and Hetz-to-Silber in 22 seconds.
  COS independently confirmed both seat incarnations remained reachable and a
  new message reached it natively through the restarted Silber daemon.
- The live Hetz trace before `326904a` saw about 16,328 preparations of the same
  mailbox version SQL in 10 seconds. After the change, that SQL was absent from
  the top 20 preparation counts in an equivalent 10-second trace. The previous
  harness-history fix reduced its hot query from roughly 198,000 to roughly 150
  SQLite steps per 10 seconds. These are path-specific traces, not availability
  or whole-system benchmarks.
- `8866c11` limits work-wake reconciliation to indexed sent-message candidates
  with work tags while retaining closed attempts. The focused regression and
  full st3 test suite passed. `0da07d7` additionally limits deadline wake-history
  enrichment to the next ready item for each local agent. The parity regression
  and full st3 test suite passed. The combined whole-daemon idle CPU is now
  being measured on the new `b6837bb` window.

## Product checks

- `cargo test -p st3`, `cargo fmt --all -- --check`, both soak-report fixture
  tests, `cargo test -p stui`, and the TUI PTY smoke passed.
- `nix build .#st3 --no-link` passed on packaging commit `3cdbe84`, including
  its release-profile tests and the `stui` suite; `checks.x86_64-linux.st3-help`
  passed for the packaged commands. The packaged `st3`, `st`, and `st3-migrate`
  binaries ran with this shell's non-Nix allocator preload removed from their
  environment; the inherited preload is not compatible with the Nix runtime.
  The package also contains the pinned `pty` executable. An isolated,
  local-only daemon started from the package and passed every doctor check,
  including PTY runtime discovery, before it was stopped. The packaged `stui`
  passed normal exit, SIGTERM restoration, navigation latency, and a
  stalled-connection PTY smoke against the live daemon. Its debug-only panic
  restoration check passed separately on the debug binary.
- The package was rebuilt and its help check passed again from the deployed
  `b6837bb` daemon source (tree at `2d861e6`); release-profile checks passed
  408 st3 library, 86 CLI, and 17 stui tests, with two live stui tests ignored.
- The final source at `db99329` also passed `nix build .#st3 --no-link` and
  `nix build .#checks.x86_64-linux.st3-help --no-link`. Its release-profile
  checks passed 408 st3 library, 87 CLI, and 17 stui tests, with two live stui
  tests ignored. This verifies packaging; the 24-hour and 72-hour live gates
  remain pending.
- Live TUI bootstrap was 121 ms and full snapshot 437 ms against the local
  daemon. The iOS Chat terminal now offers capability-gated line and key input
  with a fresh fence, bounded stale-fence retry, and runtime-incarnation guard;
  it does not queue offline input or retry an ambiguous transport failure.
  The generated TypeScript client's canonical pairing-ID route was corrected
  and regression-tested. The Debug simulator then completed a real
  full-control pairing through the paired-only HTTPS gateway, reconnected,
  and displayed usable terminal controls above the live screen. The app also
  now lets a new credential refresh while an old request is in flight. No keys were
  sent to a working agent merely for UI proof. All eight iOS logic tests, five
  TypeScript client tests, the TypeScript typecheck, and `npm run export:ios`
  passed; the offline export produced a 1.6 MiB iOS Hermes bundle. The Swift
  client generator's reserved identifier and async WebSocket receive defects
  were fixed, and its six macOS tests passed. A physical iPhone installation is
  optional for this mission and remains unproven.
- The earlier `326904a` build had a three-minute **diagnostic only** with no
  sample errors, unchanged daemon PIDs, and quiet median CPU of 13.3% on Hetz
  and 17.4% on Silber. Those figures must not be attributed to `0da07d7` or
  used to pass the default gate.

## Pending release evidence

- The former post-rollout two-host idle window started at 2026-09-25 05:08:50 UTC,
  after both `3db863d` restarts and COS's independent check. It is invalidated
  by the later daemon restarts. The next complete 24-hour window must show one stable PID per host, no missing or
  bad samples, enough quiet intervals, CPU within the stated limits, and flat
  last-hour RSS relative to the first hour.
- The former complete-failure-logging delivery window started at 2026-09-25
  05:08:50 UTC and is retained only for diagnosis. The next complete 72-hour
  report requires at least 400 exact receipts per direction, no failures or
  excessive gaps, and an active, current monitor.
- Keep inspecting peer status, last successful exchange, and errors throughout
  the soak. The current status API does not expose exact live TCP dial counts;
  the retained-client regression proves connection reuse in a focused TCP test,
  not an invented production counter. The earlier
  [idle baseline](idle-stability-2026-09-23.md) is not a matched 24-hour window.
- Long-running Fabric `exec` clients were observed hanging after remote
  commands had stopped, including one build response during this rollout and
  an independent COS check at 05:07 UTC. Short commands and st3 signed
  replication kept working; the build artifact and doctors were checked
  separately. The Fabric owner has requested a graph-queued diagnosis and
  regression; this operator-path issue remains open rather than being silently
  counted as a passed st3 messaging check.

The exact gate definitions and override rules are in the
[friend-ready delivery gate](friend-ready-delivery-gate.md). Any failure is a
release hold requiring diagnosis and a new complete window after remediation;
do not delete evidence or lower thresholds to obtain a pass.
