# Mission Document Lift

This st3-only eval proves the repository mission workflow.

The mission baseline requires the exact immutable source mission before any planner starts.

One native Codex planner reads an exact pre-published Markdown document. It converts the document into a complete ready KDL mission.

The producing step binds the exact immutable mission revision. A later step uses only that revision and starts a linked child mission run.

Mechanical gates prove publication order, exact revision binding, inherited assignment, complete graph work, the final graph product, and the file result.

Run the eval with `st3 eval ./evals/st3/mission-document-lift`.
