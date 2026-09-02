# Claude Skill Inheritance eval for st3

This eval tests project and plugin skill loading in one native Claude Sonnet seat.

The graph stores skill discovery, both invocations, effect verification, and the final message receipt.

Each skill owns a secret token that the task and plan do not reveal. Held-out gates verify both effects.

Start the st3 daemon. Then run `st3 eval ./evals/st3/claude-skill-inheritance`.

The source fixture came from `compoundingtech/evals` commit `3db48ab56d40ce27dfd94f89d2db9b692d93836a`.
