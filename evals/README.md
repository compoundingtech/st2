# Local eval corpus

This repository keeps an st2 and an st3 form of each selected eval.

The initial st2 corpus came from `compoundingtech/evals` commit `3db48ab56d40ce27dfd94f89d2db9b692d93836a`.

Future imports require an explicit eval review before they enter this corpus.

The [source eval migration review](./MIGRATION-REVIEW.md) classifies all 58 active evals in the old eval repository.

| Eval | st2 | st3 |
| --- | --- | --- |
| License MIT | `st2/license-mit` | `st3/license-mit` |
| Ghost bug | `st2/ghost-bug` | `st3/ghost-bug` |
| Signal rename | `st2/signal-rename` | `st3/signal-rename` |
| Resource cold start | `st2/resource-cold-start` | `st3/resource-cold-start` |
| Resource retarget | `st2/resource-retarget` | `st3/resource-retarget` |
| Resource handoff | `st2/resource-handoff` | `st3/resource-handoff` |
| Context and resource continuity | `st2/context-resource-continuity` | `st3/context-resource-continuity` |
| Crash escalation | `st2/crash-escalation` | `st3/crash-escalation` |
| PTY attach machine stream | `st2/pty-attach-machine-stream` | `st3/pty-attach-machine-stream` |
| PTY attach only | `st2/pty-attach-only` | `st3/pty-attach-only` |
| PTY send and peek | `st2/pty-send-peek` | `st3/pty-send-peek` |
| Network smoke | `st2/network-smoke` | `st3/network-smoke` |
| Network isolation | `st2/network-isolation` | `st3/network-isolation` |

The License MIT pair uses Claude Sonnet teams in both runtimes.

The Ghost bug and Signal rename pairs use Codex in both runtimes.

The other ten pairs are model-free. They use deterministic processes and mechanical judges.

Each eval KDL starts with a document version. A missing version means version zero.

st2 accepts versions zero and one. st3 accepts version two.

## Harness inventory

The seat counts include every native agent seat. The LLM judge counts are separate.

| Runtime | Eval | Native agent seats | LLM judges |
| --- | --- | --- | --- |
| st2 | License MIT | Claude Sonnet × 2, Codex × 1 | None |
| st2 | Ghost bug | Codex × 2 | None |
| st2 | Signal rename | Codex × 4 | None |
| st3 | License MIT | Claude Sonnet × 2 | Codex × 1 |
| st3 | Ghost bug | Codex × 2 | Codex × 1 |
| st3 | Signal rename | Codex × 4 | Codex × 1 |

The current corpus has four Claude seats and 13 Codex seats. It also has three Codex LLM judges.

The ten model-free pairs add no model seats and no LLM judges.

All Claude seats use `claude-sonnet-5`.

Model agents must use a native `harness` block. A setup or fixture process can use `command`.

Codex `gpt-5.6-sol` is the default model judge.

An eval can use a Claude judge for a specific reason. Record the choice in this inventory and the run report.

## Small Talk message discipline

The graph owns planned work, assignment, dependencies, progress, products, and judgement state.

st3 sends one durable Small Talk message when an assigned parent becomes ready.

Inherited nested steps use the parent message. A nested step sends a new message only when its assignee changes.

The native driver transports a graph message. It does not create a second message source.

An agent sends a direct message for a blocker, an exception, a requested result, or explicit coordination work.

An agent does not send routine progress when graph work state already expresses that progress.

An eval that tests coordination must judge the required Small Talk message sequence.

An eval must keep enough work state in the graph to survive an agent or daemon restart.

## Run reports

Each eval run gets one concise Markdown report in its eval folder.

Use `reports/YYYY-MM-DD-<run-id>.md`. Copy [run-report-template.md](./run-report-template.md) as the starting form.

Record exact values when the runtime exposes them. Mark an unavailable value and give the reason instead of estimating it.

Commit the report with the run evidence. Do not leave the only report in terminal output or temporary state.

## Commands

```sh
st2 eval ./evals/st2/license-mit
st3 eval ./evals/st3/license-mit
st2 eval ./evals/st2/network-smoke
st3 eval ./evals/st3/network-smoke
```

Use the matching command for the selected runtime directory.
