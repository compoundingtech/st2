# Test Writing eval for st3

This eval tests a Codex supervisor and developer with a mutation-scored test task.

The graph stores the brief, the test plan, the revision, both reports, and verification state.

The developer can change tests only. A held-out battery requires at least 10 killed mutations from 12.

Start the st3 daemon. Then run `st3 eval ./evals/st3/test-writing`.

The source fixture came from `compoundingtech/evals` commit `3db48ab56d40ce27dfd94f89d2db9b692d93836a`.
