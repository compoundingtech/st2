# Codex plan model notes

These notes describe the plan interface visible to this Codex session on 2026-08-27.

They do not describe private OpenAI implementation details. The session cannot inspect that code.

## Shape

The working plan is structured data, not a Markdown file in the repository.

One plan update contains an optional explanation and the complete ordered list of plan items.

Each item has two fields:

```text
step: string
status: pending | in_progress | completed
```

The interface permits at most one `in_progress` item.

An example update is equivalent to this JSON:

```json
{
  "explanation": "The runtime boundary comes first.",
  "plan": [
    {"step": "Protect runtime members", "status": "in_progress"},
    {"step": "Add operator commands", "status": "pending"},
    {"step": "Run verification", "status": "pending"}
  ]
}
```

## Updates

The assistant explicitly submits a new complete list when progress changes.

The plan does not inspect the repository or mark its own items complete.

Tests and tool results do not update status automatically. The assistant interprets the result and
then submits the next plan state.

Replacing the complete list avoids patch-order ambiguity. It also lets the interface enforce the
single-active-item rule on every update.

## Storage and durability

The plan is conversation state managed by the product. It is not a tracked workspace file.

The session cannot prove how the service stores that state or how long it retains it.

Repository files, commits, and the st2 context document provide separate durable state. They remain
necessary when work must survive outside this conversation.

## Relationship to chat

Plan state and user commentary are separate channels.

The structured plan gives a small progress view. Commentary explains current work, findings, and
changes that need user attention.

A detailed proposed plan is also separate. It is Markdown written for review before implementation.
It can contain interfaces, assumptions, and test cases that do not fit the small progress model.

## Useful ideas for st3

The small model works because it keeps these concerns separate:

- A stable ordered list of work items.
- One explicit active item.
- An explicit status update after observed evidence.
- A human-readable explanation for each list replacement.
- Durable implementation evidence outside the progress view.

An st3 planning feature could model each item as a graph subject. Claims could record status changes,
evidence, blockers, and review decisions without rewriting one shared Markdown file.

The graph should not infer completion from process exit alone. A test receipt, a judge result, or an
explicit human claim should provide the completion evidence.
