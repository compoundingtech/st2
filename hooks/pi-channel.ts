// st2 native message delivery for the pi harness.
//
// pi has no MCP and no app-server. Its integration point is an extension loaded into the live
// interactive process, which is strictly better than either: `pi.sendUserMessage()` is a typed
// injection point and `ctx.isIdle()` is a positive idle proof, so st2 never inspects a screen to
// decide whether delivery is safe.
//
// This asset is published as part of st2's immutable content-addressed hook set and referenced from
// a launch as `<verified set>/pi-channel.ts`. It deliberately holds no policy: st2 decides which
// message is delivered, how (`deliverAs` travels on the frame), and what a starting session is told
// about its own durable state (`sessionContext` travels on the hello). This file only moves frames.
//
// It fails open in both directions. An unmanaged pi session — one started by a human, with none of
// the channel environment set — loads this extension and does nothing. A channel that does not
// answer in time starts the session anyway, without restored context, rather than hanging it.
import childProcess from "node:child_process";
import type {
  ExtensionAPI,
  ExtensionContext,
  SessionShutdownEvent,
} from "@earendil-works/pi-coding-agent";

const PROTOCOL = 1;

const BIN = "ST2_PI_CHANNEL_BIN";
const CATALOG = "ST2_PI_CHANNEL_CATALOG";
const IDENTITY = "ST2_PI_CHANNEL_IDENTITY";
const RUNTIME_ID = "ST2_PI_CHANNEL_RUNTIME_ID";
const SESSION = "ST2_PI_CHANNEL_SESSION";
const SEQ = "ST2_PI_CHANNEL_SEQ";

// pi starts the session even if st2 is slow to answer. Restored context is worth a short wait and
// never worth a hung agent.
const HELLO_TIMEOUT_MS = 5000;

type Frame = {
  type?: string;
  protocol?: number;
  sessionContext?: string;
  content?: string;
  deliverAs?: "steer" | "followUp";
  meta?: Record<string, unknown>;
};

/**
 * Process-wide channel state.
 *
 * Both halves must outlive an extension instance. pi re-instantiates extensions on session
 * replacement, so an instance-local `current` would leave the previous session's channel running
 * beside the new one — measured: `/new` produced two live channels watching one inbox, each with
 * its own delivered set, which is the duplicate-delivery shape this extension exists to avoid.
 */
type Stash = {
  bin?: string;
  catalog?: string;
  identity?: string;
  runtimeId?: string;
  session?: string;
  seq?: string;
  child?: childProcess.ChildProcess;
};

/**
 * Read the channel configuration once and unexport it.
 *
 * pi puts its own environment in front of every bash-tool child, so leaving these set would hand a
 * nested pi its parent's bus identity — and, if that nested pi loaded this extension, its parent's
 * inbox. But pi re-instantiates extensions on session replacement, so the values cannot simply be
 * captured in module scope: a second instantiation would find the variables already deleted and
 * silently run as an unmanaged session. Measured — it stopped all delivery after `/new`. The stash
 * outlives re-instantiation; the environment does not.
 */
const stash = (): Stash => {
  const globals = globalThis as { __st2PiChannel?: Stash };
  if (!globals.__st2PiChannel) {
    // EVERY ST2_PI_CHANNEL_* value is stashed and unexported — the ownership pair included: a
    // leaked runtime id or session token would hand a nested pi (or any tool child) this seat's
    // registry key and record ownership. The channel subprocess receives them explicitly below.
    globals.__st2PiChannel = {
      bin: process.env[BIN],
      catalog: process.env[CATALOG],
      identity: process.env[IDENTITY],
      runtimeId: process.env[RUNTIME_ID],
      session: process.env[SESSION],
      seq: process.env[SEQ],
    };
    delete process.env[BIN];
    delete process.env[CATALOG];
    delete process.env[IDENTITY];
    delete process.env[RUNTIME_ID];
    delete process.env[SESSION];
    delete process.env[SEQ];
  }
  return globals.__st2PiChannel;
};

export default function (pi: ExtensionAPI) {
  const state = stash();
  const { bin, catalog, identity, runtimeId, session, seq } = state;

  // Always close a NAMED channel, never "whatever is current". A session replacement (/new,
  // /resume, /fork) tears the old session down around the new one's start, so a teardown handler
  // that closed `current` would reap the successor it just opened — measured, and it silently
  // stopped all delivery after `/new`.
  const closeChild = (child: childProcess.ChildProcess | undefined) => {
    if (!child) return;
    if (state.child === child) state.child = undefined;
    // The channel treats EOF on its stdin as the session boundary, so ending this pipe is what
    // reaps it. That happens on its own whenever pi exits, however it exits.
    if (!child.stdin?.destroyed) child.stdin?.end();
  };

  /** Open a channel and resolve with the hello's restored context (empty if none, or on timeout). */
  const open = async (ctx: ExtensionContext): Promise<string> => {
    if (!bin || !catalog || !identity) return Promise.resolve("");
    if (typeof ctx.isIdle !== "function") {
      // Refuse rather than degrade. Without a positive idle proof this extension cannot choose
      // between an idle send and a steer, and guessing would deliver into a running turn. No
      // channel means presence decays, so the agent reads as unreachable instead of quietly
      // mis-delivering.
      ctx.ui?.notify?.(
        "st2: this pi build exposes no ctx.isIdle(); refusing to open the st2 channel",
        "error",
      );
      return Promise.resolve("");
    }
    // Closes the channel opened by the PREVIOUS session, whichever extension instance opened
    // it — and WAITS (bounded) for it to exit before the replacement spawns: the successor
    // shares the seat's record, and a predecessor draining its queued frames after the new
    // session's seed would land stale state into fresh records.
    const previous = state.child;
    closeChild(previous);
    await awaitExit(previous, 2000);

    const awaitExit = (child: childProcess.ChildProcess | undefined, ms: number) =>
      new Promise<void>((resolve) => {
        if (!child || child.exitCode !== null || child.signalCode !== null) return resolve();
        const timer = setTimeout(resolve, ms);
        timer.unref?.();
        child.once("exit", () => {
          clearTimeout(timer);
          resolve();
        });
      });
    const channelEnv: NodeJS.ProcessEnv = { ...process.env };
    if (runtimeId) channelEnv[RUNTIME_ID] = runtimeId;
    if (session) channelEnv[SESSION] = session;
    if (seq) channelEnv[SEQ] = seq;
    const child = childProcess.spawn(
      bin,
      ["--catalog", catalog, "driver", "pi-channel", "--identity", identity],
      { stdio: ["pipe", "pipe", "inherit"], env: channelEnv },
    );
    state.child = child;

    return new Promise<string>((resolve) => {
      let settled = false;
      const settle = (value: string) => {
        if (settled) return;
        settled = true;
        resolve(value);
      };
      const timer = setTimeout(() => settle(""), HELLO_TIMEOUT_MS);
      timer.unref?.();
      child.on("error", () => {
        if (state.child === child) state.child = undefined;
        settle("");
      });
      // An observability pipe must never take pi down: a channel that closed its stdin mid-write
      // surfaces EPIPE on the stream, which without a listener is an uncaught exception in the
      // host process. Retire the channel instead — frames simply stop, fail-open.
      child.stdin.on("error", () => {
        if (state.child === child) state.child = undefined;
      });
      child.on("exit", () => settle(""));

      const send = (frame: Record<string, unknown>) => {
        if (child.stdin.destroyed) return;
        child.stdin.write(JSON.stringify(frame) + "\n");
      };

      const handle = async (line: string) => {
        let frame: Frame;
        try {
          frame = JSON.parse(line);
        } catch {
          return;
        }
        if (frame.type === "hello") {
          // A newer control plane may speak a wire this asset was not written against. Refusing is
          // the honest outcome: presence still decays, so the agent reads as unreachable rather
          // than silently never receiving mail.
          if (frame.protocol !== PROTOCOL) {
            closeChild(child);
            ctx.ui?.notify?.(
              `st2: pi channel protocol ${frame.protocol} is not understood by this extension (expected ${PROTOCOL}); reinstall st2's hook set`,
            );
            settle("");
            return;
          }
          clearTimeout(timer);
          settle(typeof frame.sessionContext === "string" ? frame.sessionContext : "");
          return;
        }
        if (frame.type !== "message" || typeof frame.content !== "string") return;
        try {
          // `deliverAs` is required only while a turn is streaming, and an idle send that carries
          // one is rejected, so the idle proof selects the call shape. It never selects the policy.
          // Not optional-chained: `ctx.isIdle?.() ?? true` would read a missing idle proof as
          // "idle" and silently turn every mid-turn delivery into a plain send. st2 pins the
          // surfaces whose skew is silent, and this is the one such surface in this file.
          if (ctx.isIdle()) {
            await pi.sendUserMessage(frame.content);
          } else {
            await pi.sendUserMessage(frame.content, { deliverAs: frame.deliverAs ?? "steer" });
          }
          send({ type: "delivered", meta: frame.meta });
        } catch (error) {
          send({ type: "failed", meta: frame.meta, error: String(error) });
        }
      };

      // Split on LF only. A generic line reader also splits on Unicode separators, which can appear
      // inside a message body and would corrupt the frame.
      let pending = "";
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        pending += chunk;
        let index = pending.indexOf("\n");
        while (index >= 0) {
          const line = pending.slice(0, index);
          pending = pending.slice(index + 1);
          if (line.trim()) void handle(line);
          index = pending.indexOf("\n");
        }
      });
    });
  };

  // Observed harness state, extension side. pi's own turn boundaries are the positive signal,
  // and the idle edge is `agent_settled`, not `agent_end`: measured against the repo's own pi
  // captures, `ctx.isIdle()` is still false through `agent_end`, and a queued follow-up turn
  // starts exactly at that boundary — an `agent_end` emit would blip a spurious idle before it.
  // `agent_settled` is the first point pi is provably idle. The frame is observational — st2
  // decides what becomes of it — and a closed channel drops it silently, matching the fail-open
  // rule this file already follows. pi 0.84.2 exposes no typed waiting-on-a-human event, so no
  // frame here ever claims one.
  const sendState = (word: "active" | "idle") => {
    const child = state.child;
    if (!child || !child.stdin || child.stdin.destroyed) return;
    child.stdin.write(JSON.stringify({ type: "state", state: word }) + "\n");
  };
  pi.on("agent_start", async () => sendState("active"));
  pi.on("agent_settled", async () => sendState("idle"));

  pi.on("session_start", async (_event, ctx) => {
    // Awaited before the session's first turn, which is what makes restored context reach the boot
    // prompt rather than the turn after it.
    const restored = await open(ctx);
    const opened = state.child;
    // Seed the observed state with the idle proof's answer at open time, so the record does not
    // wait for the first turn boundary to exist.
    if (opened && typeof ctx.isIdle === "function") {
      sendState(ctx.isIdle() ? "idle" : "active");
    }
    if (restored.trim()) {
      // A custom message participates in LLM context without triggering a turn of its own — the
      // closest pi equivalent to the other harnesses' `additionalContext` hook output.
      pi.sendMessage(
        { customType: "st2-session-start", content: restored, display: true },
        { deliverAs: "nextTurn" },
      );
    }
    // Bound to the channel this session opened, so a later session's abort cannot reap it.
    ctx.signal?.addEventListener?.("abort", () => closeChild(opened));
  });
  // Only a real quit closes from here. A session replacement also fires `session_shutdown`, and it
  // fires around the successor's `session_start`, so closing on those reasons reaps the channel the
  // new session just opened — measured: delivery stopped entirely after `/new`. Replacement is
  // handled by `open()` closing its predecessor instead.
  pi.on("session_shutdown", async (event: SessionShutdownEvent) => {
    if (event.reason === "quit") closeChild(state.child);
  });
}
