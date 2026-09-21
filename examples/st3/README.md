# st3 examples

These examples show small mission patterns. Each KDL file passes the normative st3 parser in the
test suite.

Validate every example with the repository contract test:

```sh
cargo test -p st3 --test examples
```

The public CLI intentionally has no generic KDL publish escape hatch. A person launches a planning
conversation with `st3 launch start ... --as person/NAME`, reviews its exact candidate with
`st3 launch preview`, and approves it with `st3 launch approve-and-launch`. An authorized agent can
publish a generated nested mission with `st3 work publish-mission` only while it owns the declared
producing step.

Keep durable mission KDL in a Git repository even when a planner authored it. The examples use local
names and workspaces; change those before production use.

## Patterns

- [`standing-owner.kdl`](standing-owner.kdl) keeps one agent available without a special mission type.
- [`queued-work.kdl`](queued-work.kdl) gives one agent an explicit ordered queue.
- [`queued-nested-work.kdl`](queued-nested-work.kdl) runs nested jobs in a strict sequence; agentless container steps start automatically and are not claimed by workers.
- [`concurrent-intake.kdl`](concurrent-intake.kdl) starts isolated runs from observed resource changes.
- [`human-review.kdl`](human-review.kdl) stops finite work at an exact human gate.
- [`review-remediation.kdl`](review-remediation.kdl) records an independent review as a completed report so findings do not cancel their remediation step.
- [`recurring-stewardship.kdl`](recurring-stewardship.kdl) schedules repeated finite cycles.
- [`nested-mission.kdl`](nested-mission.kdl) delegates a step into an inline child mission.
- [`resource-observation.kdl`](resource-observation.kdl) gates work on a local observed resource.
- [`mission-revision.kdl`](mission-revision.kdl) prepares a mission for controlled run generations.
- [`loop-until-green.kdl`](loop-until-green.kdl) repeats one child mission until its exit gates pass or its round limit ends.

`concurrent-intake.kdl` and `recurring-stewardship.kdl` contain a zero revision placeholder. Publish
the child mission first. Replace the placeholder with the exact revision from `st3 missions show`.

Mission goals describe the work. The generated `.st3/boot.md` describes how every agent uses st3.
Do not copy universal boot instructions into a harness prompt.
