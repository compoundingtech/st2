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
// The harness-context producer must be registered on the events it reads, or it observes nothing.
for (const name of ["message_end", "turn_end", "session_compact"]) {
  assert.ok(handlers.has(name), `extension registers ${name}`);
}

// A ctx with NOTHING the context producer wants: the fail-open half. An older omp build, or any
// ctx whose telemetry surface moved, must load and deliver mail exactly as before.
const bareCtx = {
  isIdle: () => true,
  ui: { notify: () => {} },
};
// A ctx carrying the surfaces measured on omp 18.0.9 (and reproduced on 18.0.3). `tokens` is the
// prompt figure — deliberately not this message's `totalTokens`. Without this ctx the producer's
// body never executes, and a use-before-declaration inside it would ship green through both the
// type gate and the old smoke.
const fullCtx = {
  ...bareCtx,
  model: { id: "fake-1", provider: "fakelab", contextWindow: 4000 },
  getContextUsage: () => ({ tokens: 22500, contextWindow: 4000, percent: 562.5 }),
  sessionManager: { getEntries: () => [{ type: "message" }, { type: "compaction" }] },
};
// And the hostile ctx: every telemetry pull throws. A guarded producer withholds; an unguarded one
// takes a turn down with it.
const throwingCtx = {
  ...bareCtx,
  get model() {
    throw new Error("smoke: model is not readable");
  },
  getContextUsage: () => {
    throw new Error("smoke: usage is not readable");
  },
  sessionManager: {
    getEntries: () => {
      throw new Error("smoke: entries are not readable");
    },
  },
};

const messageEvent = {
  message: {
    role: "assistant",
    usage: { input: 22400, output: 25, totalTokens: 22525, cost: { total: 0.067605 } },
  },
};

for (const ctx of [bareCtx, fullCtx, throwingCtx]) {
  // Two session starts in a row: the second exercises the predecessor close-and-await path.
  await handlers.get("session_start")({}, ctx);
  await handlers.get("session_start")({}, ctx);
  await handlers.get("tool_approval_requested")({ toolName: "bash" }, ctx);
  await handlers.get("tool_approval_resolved")({ approved: true }, ctx);
  await handlers.get("agent_start")({}, ctx);
  await handlers.get("message_end")(messageEvent, ctx);
  await handlers.get("turn_end")(messageEvent, ctx);
  await handlers.get("agent_end")(messageEvent, ctx);
  // omp's event names no reason: the producer must withhold the trigger, never invent one.
  await handlers.get("session_compact")({ compactionEntry: { id: "86c8955c" } }, ctx);
  await handlers.get("session_shutdown")({ reason: "smoke" }, ctx);
}
console.log("omp extension smoke: ok");
process.exit(0);
