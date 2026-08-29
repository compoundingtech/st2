// Runtime smoke of the shipped pi extension: drives the channel-open path far enough that a
// use-before-declaration (TDZ), a broken import, or a top-level throw fails the check — the
// classes a type-only gate is provably blind to. The channel binary is `true`, so the open
// times out its hello and resolves empty; any thrown error fails the smoke.
import assert from "node:assert";

process.env.ST2_PI_CHANNEL_BIN = process.env.SMOKE_TRUE_BIN ?? "/bin/true";
process.env.ST2_PI_CHANNEL_CATALOG = "/tmp/st2-smoke-catalog";
process.env.ST2_PI_CHANNEL_IDENTITY = "smoke.worker";
process.env.ST2_PI_CHANNEL_RUNTIME_ID = "smoke.worker";
process.env.ST2_PI_CHANNEL_SESSION = "smoke-session";
process.env.ST2_PI_CHANNEL_SEQ = "1";

const mod = await import("./smoke-out/pi-channel.mjs");
assert.strictEqual(typeof mod.default, "function", "extension exports its entry point");

const handlers = new Map();
const pi = {
  on: (name, handler) => handlers.set(name, handler),
};
mod.default(pi);
for (const name of ["session_start", "session_shutdown", "agent_start", "agent_settled"]) {
  assert.ok(handlers.has(name), `extension registers ${name}`);
}

// The harness-context producer must be registered on the events it reads, or it observes nothing.
for (const name of ["message_end", "turn_end", "agent_end", "session_compact"]) {
  assert.ok(handlers.has(name), `extension registers ${name}`);
}

// A ctx with NOTHING the context producer wants. This is the fail-open half and it is the one a
// bare smoke used to cover on its own: an older pi build, or any ctx whose telemetry surface moved,
// must load and deliver mail exactly as before.
const bareCtx = {
  isIdle: () => true,
  ui: { notify: () => {} },
};
// A ctx carrying the surfaces measured on pi 0.84.2 — verbatim shapes from the credential-free lab.
// Without this the context producer's body never executes at all, and a use-before-declaration
// inside it would ship green through both the type gate and the old smoke, which is the exact
// defect class this file exists for.
const fullCtx = {
  ...bareCtx,
  model: { id: "fake-1", provider: "fakelab", contextWindow: 4000 },
  getContextUsage: () => ({ tokens: 23425, contextWindow: 4000, percent: 585.625 }),
  sessionManager: {
    getEntries: () => [{ type: "message" }, { type: "compaction" }, { type: "compaction" }],
  },
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

// Override one pull without spreading: `throwingCtx`'s `model` is a throwing getter, and a spread
// would fire it here instead of inside the code under test.
const withNulledUsage = (ctx) =>
  Object.create(ctx, {
    getContextUsage: {
      value: () => ({ tokens: null, contextWindow: 4000, percent: null }),
    },
  });

const messageEvent = {
  message: {
    role: "assistant",
    usage: { input: 23300, output: 25, totalTokens: 23425, cost: { total: 0.070305 } },
  },
};

for (const ctx of [bareCtx, fullCtx, throwingCtx]) {
  // Two session starts in a row: the second exercises the predecessor close-and-await path — the
  // exact region the TDZ regression lived in.
  await handlers.get("session_start")({}, ctx);
  await handlers.get("session_start")({}, ctx);
  await handlers.get("agent_start")({}, ctx);
  await handlers.get("message_end")(messageEvent, ctx);
  await handlers.get("turn_end")(messageEvent, ctx);
  await handlers.get("agent_end")({}, ctx);
  // pi withholds tokens and percent here for real; the producer must forward that, not fill it in.
  await handlers.get("session_compact")(
    { reason: "overflow", willRetry: false },
    withNulledUsage(ctx),
  );
  await handlers.get("agent_settled")({}, ctx);
  await handlers.get("session_shutdown")({ reason: "smoke" }, ctx);
}
console.log("pi extension smoke: ok");
process.exit(0);
