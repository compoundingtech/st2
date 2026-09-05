// Runtime smoke of the shipped pi extension: drives the channel-open path far enough that a
// use-before-declaration (TDZ), a broken import, or a top-level throw fails the check — the
// classes a type-only gate is provably blind to. The channel binary is `true`, so the open
// times out its hello and resolves empty; any thrown error fails the smoke.
import assert from "node:assert";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// A recorder standing in for `st2 driver pi-channel`, so this smoke can assert what the extension
// EMITS and not merely that it loads. With `true` as the channel binary the frames go into a pipe
// nobody reads, which cannot tell a working producer from one that writes nothing at all — and a
// producer that silently writes nothing is indistinguishable from the pre-producer state, where
// every declaration's context reads null. That is the failure this file has to be able to see.
//
// It speaks the two things the extension needs: a hello it can negotiate against — the offer is
// selected by `ST2_SMOKE_HELLO`, so one recorder can stand in for this build's control plane, for
// one that never learned protocol 2, and for one that speaks nothing this asset does — and an
// append of every frame line to the file named by `ST2_SMOKE_FRAMES`. Both ride the environment
// rather than being baked in, because the channel is spawned fresh on every `session_start` and
// inherits this process's environment at that moment, which is what lets one loaded extension be
// driven against several control planes.
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "st2-pi-smoke-"));
const framesPath = path.join(dir, "frames.jsonl");
const recorder = path.join(dir, "recorder");
fs.writeFileSync(
  recorder,
  `#!${process.execPath}
import fs from "node:fs";
const offers = {
  // What this build's hello carries: the floor a published asset compares for strict equality,
  // beside the set a newer asset negotiates over.
  negotiated: { protocol: 1, protocols: [1, 2] },
  // A control plane that never learned protocol 2 — the bytes this channel shipped with.
  legacy: { protocol: 1 },
  // A wire this asset does not speak at all.
  foreign: { protocol: 9, protocols: [9] },
};
const offer = offers[process.env.ST2_SMOKE_HELLO ?? "negotiated"];
process.stdout.write(JSON.stringify({ type: "hello", ...offer, sessionContext: "" }) + "\\n");
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => fs.appendFileSync(process.env.ST2_SMOKE_FRAMES, chunk));
`,
  { mode: 0o755 },
);
const readFrames = (file = framesPath) =>
  (fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "")
    .split("\n")
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line));

process.env.ST2_PI_CHANNEL_BIN = recorder;
process.env.ST2_PI_CHANNEL_CATALOG = "/tmp/st2-smoke-catalog";
process.env.ST2_PI_CHANNEL_IDENTITY = "smoke.worker";
process.env.ST2_PI_CHANNEL_RUNTIME_ID = "smoke.worker";
process.env.ST2_PI_CHANNEL_SESSION = "smoke-session";
process.env.ST2_PI_CHANNEL_SEQ = "1";
// Read by the recorder, not by the extension: the channel inherits this environment when the
// extension spawns it, so a phase below can redirect the sink and the hello together.
process.env.ST2_SMOKE_FRAMES = framesPath;

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

// The harness-context producer must be registered on the events it reads, or it observes nothing;
// `session_compact_failed` is the fault axis's only pi-specific source.
for (const name of [
  "message_end",
  "turn_end",
  "agent_end",
  "session_compact",
  "session_compact_failed",
]) {
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
// The `agent_end` payloads pi ACTUALLY emits: `{type:"agent_end", messages: AgentMessage[]}`
// (0.84.2 dist/core/extensions/types.d.ts:542-544, 0.84.4 :555-558), with `stopReason` drawn from
// pi's own closed vocabulary `pending | stop | length | toolUse | error | aborted | deferred`
// (pi-ai 0.84.4 dist/types.d.ts:287, 0.84.2 :277). These fixtures are the reason this smoke can
// see the defect class it exists for: a producer reading a `message` singular, or comparing
// against a stop word pi never emits, classifies nothing at all — no raise, no clear, false idle.
//
// Each run's tail is the LAST assistant message, so every fixture carries a tool result after a
// tool-use assistant message: a producer scanning forwards, or reading `messages[0]`, gets the
// wrong verdict here rather than passing by luck.
const toolLeg = [
  { role: "assistant", stopReason: "toolUse", usage: { totalTokens: 100 } },
  { role: "toolResult", toolName: "bash", content: [] },
];
const failedRun = {
  type: "agent_end",
  messages: [
    ...toolLeg,
    { role: "assistant", stopReason: "error", errorMessage: "401 Unauthorized from fakelab" },
  ],
};
const completedRun = {
  type: "agent_end",
  messages: [...toolLeg, { role: "assistant", stopReason: "stop" }],
};
// A person pressed escape. Neither health nor fault, so neither a raise nor a clear.
const abortedRun = {
  type: "agent_end",
  messages: [...toolLeg, { role: "assistant", stopReason: "aborted" }],
};
// A run whose last assistant message never resolved: also no verdict.
const pendingRun = {
  type: "agent_end",
  messages: [{ role: "assistant", stopReason: "pending" }],
};
// How many times the loop below drives one full turn sequence — the exact count of raises, clears,
// and clearAlls the negotiated phase must produce.
const drives = 3;

for (const ctx of [bareCtx, fullCtx, throwingCtx]) {
  // Two session starts in a row: the second exercises the predecessor close-and-await path — the
  // exact region the TDZ regression lived in.
  await handlers.get("session_start")({}, ctx);
  await handlers.get("session_start")({}, ctx);
  await handlers.get("agent_start")({}, ctx);
  await handlers.get("message_end")(messageEvent, ctx);
  await handlers.get("turn_end")(messageEvent, ctx);
  // Every `agent_end` shape pi produces. Only the failed and the completed run say anything
  // about the condition axis; a run with no messages at all, an aborted one, and one whose tail
  // never resolved each emit nothing.
  await handlers.get("agent_end")({ type: "agent_end", messages: [] }, ctx);
  await handlers.get("agent_end")(failedRun, ctx);
  await handlers.get("agent_end")(completedRun, ctx);
  await handlers.get("agent_end")(abortedRun, ctx);
  await handlers.get("agent_end")(pendingRun, ctx);
  // A real compaction failure, and a cancelled one: `aborted` is what separates a harness out of
  // context from a person changing their mind, and only the former is a fault.
  await handlers.get("session_compact_failed")(
    { reason: "overflow", aborted: false, errorMessage: "summarizer returned no content", willRetry: false, fromExtension: false },
    ctx,
  );
  await handlers.get("session_compact_failed")(
    { reason: "manual", aborted: true, willRetry: false, fromExtension: false },
    ctx,
  );
  // pi withholds tokens and percent here for real; the producer must forward that, not fill it in.
  await handlers.get("session_compact")(
    { reason: "overflow", willRetry: false },
    withNulledUsage(ctx),
  );
  await handlers.get("agent_settled")({}, ctx);
  await handlers.get("session_shutdown")({ reason: "smoke" }, ctx);
}

// Give the recorder a moment to drain what was written to its stdin, then assert the wire.
await new Promise((resolve) => setTimeout(resolve, 500));
const frames = readFrames();
const context = frames.filter((frame) => frame.type === "context");
assert.ok(context.length > 0, "the producer must emit context frames, not merely load");

// The exact keys `src/pi_channel.rs::context_frame` decodes. Both halves of this contract live in
// different languages and different files, so nothing but this assertion couples them: flatten the
// reading or rename a key and every Rust fixture still passes while the record is never written.
const reading = context.find((frame) => frame.reading);
assert.ok(reading, "a context frame must carry a `reading` object");
// Every leg is always present, `null` where withheld — one absence convention on the wire, so the
// Rust decoder never has to tell "absent" from "the harness said it does not know".
for (const key of ["usedTokens", "windowTokens", "usedPercent", "model", "costUsd"]) {
  assert.ok(key in reading.reading, `reading carries ${key}`);
}

// Selected by predicate, not by position: the contexts are driven in a fixed order but which one
// produced a given frame is not what is under test, and asserting on `frames[0]` would make this
// break whenever the loop gains a case.
const known = context.find((frame) => typeof frame.reading?.usedTokens === "number");
assert.ok(known, "a populated context must produce a reading with real numbers");
assert.strictEqual(known.reading.usedTokens, 23425, "pi's numerator is totalTokens");
assert.strictEqual(known.reading.windowTokens, 4000);
assert.strictEqual(known.reading.usedPercent, 585.625, "carried raw, never clamped");
assert.strictEqual(known.reading.model, "fake-1");

// The cost hold, on the wire. `session_start` legitimately carries `costUsd: null` — the hold is
// cleared on session replacement, so a fresh session does not restate its predecessor's cost — and
// a later frame from an event carrying no cost of its own must still restate the held one, or the
// record's wholesale field replacement would erase it.
const withCost = context.find((frame) => typeof frame.reading?.costUsd === "number");
assert.ok(withCost, "the held cost must be restated on frames after a message-bearing event");
assert.strictEqual(withCost.reading.costUsd, 0.070305);

const edges = context.filter((frame) => frame.compaction);
assert.ok(edges.length > 0, "a compaction edge must ride a context frame");
for (const edge of edges) {
  assert.strictEqual(edge.compaction.trigger, "overflow", "pi names its own trigger");
}
// A context whose session store cannot be read still sends the edge, with no count: st2 then
// counts it itself. The edge is never what gets dropped.
assert.ok(
  edges.some((edge) => edge.compaction.count === null),
  "an unreadable session store must still send the edge, countless",
);
const durable = edges.find((edge) => typeof edge.compaction.count === "number");
assert.ok(durable, "a readable session store must supply the durable count");
assert.strictEqual(durable.compaction.count, 2, "the count is getEntries() filtered to compactions");
// The pairing the write guard makes load-bearing: the withheld reading must be in the SAME frame
// as the edge, or it lands in no write until the heartbeat comes due.
assert.strictEqual(durable.reading.usedTokens, null, "the withheld reading rides the edge");
assert.strictEqual(durable.reading.usedPercent, null);
assert.strictEqual(durable.reading.windowTokens, 4000, "pi still knows its denominator");

// The condition axis, on the wire. Everything below is the only place these emitters actually
// execute, so nothing else couples the words this asset writes to the words
// `src/pi_channel.rs::condition_frame` decodes.
const conditions = frames.filter((frame) => frame.type === "condition");
// Told apart from a mapping defect deliberately. Every condition assertion below is an exact
// count, and there are two ways to reach zero: the classification is broken, or the recorder's
// hello never arrived inside the asset's 5 s window on a loaded machine and no channel ever
// negotiated protocol 2. The second is this harness's problem, not the asset's, and it must not
// masquerade as the first.
assert.ok(
  conditions.length > 0 || frames.length === 0,
  "frames arrived but no condition frame did: either the classification is broken or no channel " +
    "negotiated protocol 2 (a hello that timed out leaves every condition frame gated off)",
);
// A condition frame states the fault axis and NOTHING else. An activity claim here would
// fabricate one from an event that observed none, and it is what made a wedged seat publish a
// clean idle.
for (const condition of conditions) {
  assert.ok(!("state" in condition), "a condition frame never restates activity");
  assert.ok(!("ask" in condition), "a condition frame never claims an ask");
  assert.notStrictEqual(condition.op, "ended", "no frame from this asset can end a session");
}

const raises = conditions.filter((frame) => frame.op === "raise");
const assistantErrors = raises.filter((frame) => frame.code === "pi/assistantError");
assert.strictEqual(
  assistantErrors.length,
  drives,
  "an error-ended assistant tail raises exactly one fault",
);
for (const raise of assistantErrors) {
  // The teeth: prose that says `401` still yields `harness`. pi ships no error-classification
  // field, so `authentication` here would be inferred from the very string carried as detail.
  assert.strictEqual(raise.category, "harness", "pi's untyped turn failure is a harness fault");
  assert.strictEqual(raise.recovery, "unknown", "pi says nothing about who clears it");
  assert.strictEqual(
    raise.detail,
    "401 Unauthorized from fakelab",
    "the prose is diagnostic only",
  );
}

const clearAlls = conditions.filter((frame) => frame.op === "clearAll");
assert.strictEqual(clearAlls.length, drives, "a clean agent_end is pi's only unkeyed clear");
for (const clear of clearAlls) {
  assert.strictEqual(clear.proof, "turnCompleted", "an unkeyed clear names the progress it saw");
}

const compactFailures = raises.filter((frame) => frame.code === "pi/session_compact_failed");
assert.strictEqual(
  compactFailures.length,
  drives,
  "only the non-aborted compaction failure raises; a cancelled /compact is not a fault",
);
for (const raise of compactFailures) {
  assert.strictEqual(raise.category, "context");
  assert.strictEqual(raise.recovery, "human", "nothing in pi retries a failed compaction");
  assert.strictEqual(
    raise.detail,
    "summarizer returned no content",
    "the typed event's own errorMessage rides as detail",
  );
}

const paired = conditions.filter((frame) => frame.op === "clear");
assert.strictEqual(paired.length, drives, "a successful compaction clears its own failure once");
for (const clear of paired) {
  assert.strictEqual(clear.category, "context");
  // Keyed on the FULL code, never the category alone: a category-only clear would also wipe any
  // other context fault standing on the seat.
  assert.strictEqual(clear.code, "pi/session_compact_failed");
}

// Four condition frames per drive and not a fifth. Everything else driven above is deliberately
// silent: an `agent_end` with no messages, an aborted run, a run whose tail is still pending, and
// a cancelled compaction each emit nothing in either direction.
assert.strictEqual(
  conditions.length,
  4 * drives,
  "only a failed run, a completed run, a real compaction failure, and a compaction success speak",
);

// And the idle edge is untouched: `agent_settled` still sends its plain state frame, which is the
// honest activity beside a standing fault rather than a suppressed one.
const idles = frames.filter((frame) => frame.type === "state" && frame.state === "idle");
assert.ok(idles.length >= drives, "agent_settled still publishes a plain idle");
for (const idle of idles) {
  assert.deepStrictEqual(
    Object.keys(idle).sort(),
    ["state", "type"],
    "pi's state frame carries no ask and no condition",
  );
}

// The whole point of raising from `agent_end`: it is measured to fire BEFORE `agent_settled`, so
// the fault lands first and the idle that follows carries it forward instead of publishing a
// clean yield for a wedged seat.
const firstRaise = frames.findIndex(
  (frame) => frame.type === "condition" && frame.code === "pi/assistantError",
);
const idleAfterRaise = frames.findIndex(
  (frame, index) => index > firstRaise && frame.type === "state" && frame.state === "idle",
);
assert.ok(firstRaise >= 0, "the error-tailed turn must raise");
assert.ok(
  idleAfterRaise > firstRaise,
  "the standing fault must precede the idle it is published beside",
);

// The state vocabulary stays closed to what pi's own turn boundaries prove. `ended` is the outer
// session wrapper's word — it alone sees the provider die — and no frame from this asset may
// claim it on any axis.
for (const frame of frames.filter((frame) => frame.type === "state")) {
  assert.ok(
    frame.state === "active" || frame.state === "idle",
    `a state frame states only active or idle: ${JSON.stringify(frame)}`,
  );
}
assert.strictEqual(
  frames.filter((frame) => frame.state === "ended" || "exit" in frame).length,
  0,
  "the channel's asset never writes a terminal record",
);

// Phase 2: a control plane that never learned protocol 2. Every frame this asset sends must be
// one that wire can carry — the condition frames are gated on the negotiated version, not on the
// code being installed.
const legacyFrames = path.join(dir, "frames-legacy.jsonl");
process.env.ST2_SMOKE_HELLO = "legacy";
process.env.ST2_SMOKE_FRAMES = legacyFrames;
await handlers.get("session_start")({}, fullCtx);
await handlers.get("agent_start")({}, fullCtx);
await handlers.get("agent_end")(failedRun, fullCtx);
await handlers.get("agent_end")(completedRun, fullCtx);
await handlers.get("session_compact_failed")(
  { reason: "overflow", aborted: false, willRetry: false, fromExtension: false },
  fullCtx,
);
await handlers.get("agent_settled")({}, fullCtx);
await new Promise((resolve) => setTimeout(resolve, 500));
const legacy = readFrames(legacyFrames);
assert.ok(legacy.length > 0, "a protocol-1 control plane still receives the frames it understands");
assert.strictEqual(
  legacy.filter((frame) => frame.type === "condition").length,
  0,
  "no condition frame may reach a wire that cannot carry one",
);
assert.ok(
  legacy.some((frame) => frame.type === "state" && frame.state === "idle"),
  "protocol 1 keeps its exact existing state frames",
);
assert.ok(
  legacy.some((frame) => frame.type === "context"),
  "protocol 1 keeps its exact existing context frames",
);

// Phase 3: a control plane offering nothing this asset speaks. Refusing is the honest outcome —
// presence decays and the seat reads as unreachable — and it must be a NOTIFIED refusal, not a
// silent one.
const notices = [];
const foreignCtx = Object.create(fullCtx, {
  ui: { value: { notify: (message) => notices.push(message) } },
});
const foreignFrames = path.join(dir, "frames-foreign.jsonl");
process.env.ST2_SMOKE_HELLO = "foreign";
process.env.ST2_SMOKE_FRAMES = foreignFrames;
await handlers.get("session_start")({}, foreignCtx);
await handlers.get("agent_start")({}, foreignCtx);
await handlers.get("agent_settled")({}, foreignCtx);
await new Promise((resolve) => setTimeout(resolve, 500));
assert.strictEqual(readFrames(foreignFrames).length, 0, "a refused channel receives no frames");
assert.ok(
  notices.some((notice) => notice.includes("protocol")),
  "a refused negotiation tells the operator why",
);

fs.rmSync(dir, { recursive: true, force: true });
console.log("pi extension smoke: ok");
process.exit(0);
