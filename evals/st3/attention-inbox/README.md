# Human attention inbox eval

This model-free eval proves the complete human attention inbox.

The controller creates one current item for each supported kind. It checks the global list and the selected person list.

The planning workspace contains a tracked boot-file conflict. This prevents the temporary Codex planner from starting. The controller submits the fixed candidate directly as that planner and creates no model request.

The controller approves or closes each item. It then proves that the selected person's inbox is empty.

Run it with `st3 eval ./evals/st3/attention-inbox`.
