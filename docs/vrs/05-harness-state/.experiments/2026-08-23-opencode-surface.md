# OpenCode's server surface, measured for the driver

2026-08-23, OpenCode 1.18.19 (`/home/schickling/.nix-profile/bin/opencode`), Linux, isolated
`XDG_DATA_HOME`/`XDG_CONFIG_HOME`, headless `opencode serve --port 43123 --print-logs`. The free
anonymous model (`opencode/big-pickle`) answered prompts with no credentials, so every claim below
is reproducible without an API key.

## What was established

**The TUI is a server.** `opencode` (TUI, the default command) starts a server on `--port` /
`--hostname` exactly like `opencode serve`; `opencode attach <url>` exists for the reverse
direction. A driver therefore launches the interactive seat with a wrapper-allocated loopback port
and speaks HTTP to its own child — no screen scraping anywhere in the driver path.

**Observation** rides `GET /event` (SSE). First event is `server.connected`, `server.heartbeat` is
periodic, and a connect replays ~45 `plugin.added` events — subscribers must tolerate noise and
duplicates. The API self-describes at `GET /doc` (OpenAPI 3.1, 94 event schemas). Measured and
schema-verified signals, as projected by the driver:

- `session.status` with a three-arm status union `busy | idle | retry` (measured firing at turn
  start and end; `retry` carries `attempt`/`next`/`message`);
- `session.idle` fires beside the idle status (measured);
- `permission.asked` / `permission.replied` and `question.asked` / `question.replied|rejected`
  carry stable `^per` / `^que` ids — the blocked-on-human exit edge is id-matched, with none of the
  Claude batching ambiguity (schema-verified; a live `permission.asked` capture is still owed —
  with `{"permission":{"bash":"ask"}}` PATCHed into config, the free model's bash ran without
  asking in one run and emitted no tool part in another);
- `session.error` is an eight-arm union; `ProviderAuthError` is terminal for the seat, the others
  leave the session promptable;
- `GET /session/status` returns `{sessionID: status}` and **omits idle sessions** — measured `{}`
  when idle, so absence-of-key is the idle proof only over a proven-live server.

**Delivery** is `POST /session/{id}/prompt_async` (measured: returns 200 immediately, empty body),
which accepts a caller-supplied `messageID` (`^msg`) — idempotent and receipt-correlatable. The
receipt is the message read back (`GET /session/{id}/message/{messageID}` / the `message.updated`
event). Prompts sent mid-turn queue natively. **`/tui/append-prompt` and `/tui/submit-prompt`
returned `true` on a headless server with no TUI attached** — they are broadcast, not receipt, and
must never count as delivery. Auth is `OPENCODE_SERVER_PASSWORD` + basic auth (user `opencode`),
unsecured by default on loopback.

**Sessions** are `ses_*`; storage moved to sqlite (`$XDG_DATA_HOME/opencode/opencode.db`,
`opencode db` exists) — the API is the only sane read path. Exact resume is `--session <id>` /
`--continue`, forking is `--fork`. No native incarnation concept: st2's runtime generation, the
pinned port, and the wrapper pid supply fencing.

**Pinning**: `opencode --version` prints the bare version; `Session.version` also rides the wire.
Because the server serves its own OpenAPI document, a live `/doc` subset check at wrapper start
covers the shape while a version list covers the semantics — the hybrid of the Codex
`SUPPORTED_CODEX_CLI_VERSIONS` pattern and the pi type-check pattern.

## Reproduction

```
XDG_DATA_HOME=$S/data XDG_CONFIG_HOME=$S/config opencode serve --port 43123 --print-logs
curl -s http://127.0.0.1:43123/doc | jq '.paths | keys'
curl -sN http://127.0.0.1:43123/event            # SSE capture
curl -s -XPOST http://127.0.0.1:43123/session    # create ses_…
curl -s -XPOST http://127.0.0.1:43123/session/<id>/prompt_async \
  -H 'content-type: application/json' \
  -d '{"messageID":"msg0000000000000000000000000","parts":[{"type":"text","text":"hi"}]}'
curl -s http://127.0.0.1:43123/session/<id>/message/msg0000000000000000000000000
curl -s http://127.0.0.1:43123/session/status    # {} idle · {"ses_…":{"type":"busy"}} mid-turn
curl -s -XPOST http://127.0.0.1:43123/tui/append-prompt -d '{"text":"x"}'   # true, no TUI attached
```

Original captures: session `ses_fd078983affefGxfpkGr2u44LJ`, files `serve.log`, `openapi.json`,
`events{,2,3,4}.sse`, `session.json`, `prompt-response.json` (session scratchpad, not committed).

## Limits

- A live `permission.asked` has not been captured; the blocked edges are schema-backed only
  (`DQ-H6` keeps this open until a TUI-seat capture with a real permission prompt exists).
- A v2 surface (`/api/event`, `/api/session/{id}/wait`, `permission.v2.*`) coexists with the
  legacy one probed here; the driver pins the legacy arms via the `/doc` check.
- Docs move fast (the site showed "Last updated Aug 23, 2026"); the `/doc` gate is the defense.

## VRS Impact

Resolves `DQ-H6`'s source question: the OpenCode producer is evented (server SSE), not screen
observation, and uniquely offers an id-matched blocked-on-human exit edge. Feeds the OpenCode
producer section of `spec.md` (mapping table, aggregate-session rule, receipt semantics, the
two-gate fail-closed rule) and requirement `OHS-R08`. The un-captured permission edge stays fenced
in `DQ-H6`.
