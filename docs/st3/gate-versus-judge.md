# Gate vocabulary decision

Status: resolved.

st3 uses `gate` for plan, step, and checkpoint acceptance conditions.

The supervisor screen-input feature is named `terminal-control`. This removes the former language collision.

The complete public vocabulary is:

- KDL: repeated `gate "NAME" { ... }` nodes;
- CLI: `st3 gate-result`;
- API: `POST /v1/gate-results`;
- claims: `gate.requested` and `gate.result`;
- running-gate environment: `ST_GATE`.

st3 does not accept `judges`, `judge`, `judgement`, or an `outcome` wrapper. There is no compatibility alias because st3 has no released grammar to preserve.

`produces` remains a separate product contract. It names graph state that work promises to create. A gate evaluates acceptance. A step or plan must satisfy both its products and all its gates.

See [plan-graph-runtime.md](./plan-graph-runtime.md#gates) for the full syntax and execution rules.
