# Run generation revision

This mechanical st3 eval proves one protected revision of a live plan run.

The controller completes one stable step before it proposes a replacement plan revision.
The requester approves the exact preview hash.
st3 creates one successor generation and keeps the prior generation unchanged.

The gates prove these results:

- The plan run keeps its initial revision and selects the successor revision.
- The prior generation becomes `superseded` and remains readable.
- The successor names the prior generation as its predecessor.
- The stable completed step carries to the successor.
- The changed step does not carry completion.
- A new mechanical gate receives `ST_RUN_GENERATION` without an authored `env` mapping.
- The inner plan reaches completion after the proof captures both generations.

This eval uses no model run.

Run it with `st3 eval ./evals/st3/run-generation-revision`.
