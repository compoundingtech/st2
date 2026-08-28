# Local eval corpus

This repository keeps an st2 and an st3 form of each selected eval.

The initial st2 corpus came from `compoundingtech/evals` commit `3db48ab56d40ce27dfd94f89d2db9b692d93836a`.

Future imports require an explicit eval review before they enter this corpus.

| Eval | st2 | st3 |
| --- | --- | --- |
| License MIT | `st2/license-mit` | `st3/license-mit` |
| Ghost bug | `st2/ghost-bug` | `st3/ghost-bug` |
| Signal rename | `st2/signal-rename` | `st3/signal-rename` |

The License MIT pair preserves the paid st2 Claude baseline. Its st3 form uses Codex.

The Ghost bug and Signal rename pairs use Codex in both runtimes.

Each eval KDL starts with a document version. A missing version means version zero.

st2 accepts versions zero and one. st3 accepts version two.

## Small Talk message discipline

The graph owns planned work, assignment, dependencies, progress, products, and judgement state.

st3 sends one durable Small Talk message when an assigned parent becomes ready.

Inherited nested steps use the parent message. A nested step sends a new message only when its assignee changes.

The native driver transports a graph message. It does not create a second message source.

An agent sends a direct message for a blocker, an exception, a requested result, or explicit coordination work.

An agent does not send routine progress when graph work state already expresses that progress.

An eval that tests coordination must judge the required Small Talk message sequence.

An eval must keep enough work state in the graph to survive an agent or daemon restart.

## Commands

```sh
st2 eval ./evals/st2/license-mit
st3 eval ./evals/st3/license-mit
```

Use the matching command for the selected runtime directory.
