# Work wake reliability

This black-box eval keeps one real Codex worker alive across six ordinary assigned steps:

- two fresh mission runs;
- an initial standing-mission step;
- two successive live mission revisions;
- a worker incarnation replacement followed by another fresh mission run.

It then cancels the standing run and requires its agentless `finally` step to complete. Every
assigned step must expose a successful durable wake projection. Held-out gates reject graph or
executable terminal input, so success comes from the native work-message path.
