// Runtime smoke of the shipped omp extension: drives the channel-open path far enough that a
// use-before-declaration (TDZ), a broken import, or a top-level throw fails the check — the
// classes a type-only gate is provably blind to. The channel binary is `true`, so the open
// times out its hello and resolves empty; any thrown error fails the smoke. Mirrors smoke.mjs,
// minus `agent_settled`: omp has no such event (measured 2026-08-25), so the idle edge is the
// agent_end poll, which this smoke exercises with an immediately-true isIdle.
import assert from "node:assert";

process.env.ST2_OMP_CHANNEL_BIN = process.env.SMOKE_TRUE_BIN ?? "/bin/true";
process.env.ST2_OMP_CHANNEL_CATALOG = "/tmp/st2-smoke-catalog";
process.env.ST2_OMP_CHANNEL_IDENTITY = "smoke.worker";
process.env.ST2_OMP_CHANNEL_RUNTIME_ID = "smoke.worker";
process.env.ST2_OMP_CHANNEL_SESSION = "smoke-session";
process.env.ST2_OMP_CHANNEL_SEQ = "1";

const mod = await import("./smoke-out/omp-channel.mjs");
assert.strictEqual(typeof mod.default, "function", "extension exports its entry point");

const handlers = new Map();
const pi = {
  on: (name, handler) => handlers.set(name, handler),
};
mod.default(pi);
for (const name of [
  "session_start",
  "session_shutdown",
  "agent_start",
  "agent_end",
  "tool_approval_requested",
  "tool_approval_resolved",
]) {
  assert.ok(handlers.has(name), `extension registers ${name}`);
}
assert.ok(!handlers.has("agent_settled"), "omp has no agent_settled event");

const ctx = {
  isIdle: () => true,
  ui: { notify: () => {} },
};
// Two session starts in a row: the second exercises the predecessor close-and-await path.
await handlers.get("session_start")({}, ctx);
await handlers.get("session_start")({}, ctx);
await handlers.get("tool_approval_requested")({ toolName: "bash" }, ctx);
await handlers.get("tool_approval_resolved")({ approved: true }, ctx);
await handlers.get("agent_start")({}, ctx);
await handlers.get("agent_end")({}, ctx);
await handlers.get("session_shutdown")({ reason: "smoke" }, ctx);
console.log("omp extension smoke: ok");
process.exit(0);
