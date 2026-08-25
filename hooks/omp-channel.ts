// st2 native message delivery for the omp harness.
//
// Forked from pi-channel.ts: omp is pi-family and loads the same extension shape, but the two
// diverge where it matters (measured 2026-08-25, omp v18.0.3 — see
// docs/vrs/06-omp-driver/.experiments/). omp has no `agent_settled` event, so the idle edge is
// `agent_end` followed by bounded polling of `ctx.isIdle()`; and omp exposes
// `tool_approval_requested`/`tool_approval_resolved`, which carry the blocked-on-human axis pi
// cannot express. Like the pi asset this file holds no policy: st2 decides which message is
// delivered, how, and what a starting session is told. It fails open in both directions — an
// unmanaged omp session loads this extension and does nothing; a slow channel starts the session
// without restored context rather than hanging it.
import childProcess from "node:child_process";
import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";

const PROTOCOL = 1;

const BIN = "ST2_OMP_CHANNEL_BIN";
const CATALOG = "ST2_OMP_CHANNEL_CATALOG";
const IDENTITY = "ST2_OMP_CHANNEL_IDENTITY";
const RUNTIME_ID = "ST2_OMP_CHANNEL_RUNTIME_ID";
const SESSION = "ST2_OMP_CHANNEL_SESSION";
const SEQ = "ST2_OMP_CHANNEL_SEQ";

// omp starts the session even if st2 is slow to answer. Restored context is worth a short wait
// and never worth a hung agent.
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
 * Both halves must outlive an extension instance: omp re-instantiates extensions on session
 * replacement, so an instance-local `current` would leave the previous session's channel running
 * beside the new one, each with its own delivered set — the duplicate-delivery shape this
 * extension exists to avoid.
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
 * Every ST2_OMP_CHANNEL_* value is stashed and unexported: a leaked runtime id or session token
 * would hand a nested harness child this seat's registry key and record ownership. The values
 * cannot live in module scope because a second instantiation would find them already deleted and
 * silently run as an unmanaged session; the stash outlives re-instantiation, the environment does
 * not.
 */
const stash = (): Stash => {
  const globals = globalThis as { __st2OmpChannel?: Stash };
  if (!globals.__st2OmpChannel) {
    globals.__st2OmpChannel = {
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
  return globals.__st2OmpChannel;
};

/**
 * ctx.isIdle() guarded: omp's proof must never take the session down, and a throw reads as "not
 * proven idle", the conservative answer for every caller.
 */
const idleProof = (ctx: ExtensionContext): boolean => {
  try {
    return ctx.isIdle();
  } catch {
    return false;
  }
};

export default function (pi: ExtensionAPI) {
  const state = stash();
  const { bin, catalog, identity, runtimeId, session, seq } = state;

  // Always close a NAMED channel, never "whatever is current". A session replacement tears the
  // old session down around the new one's start, so a teardown handler that closed `current`
  // would reap the successor it just opened (measured on pi; same lifecycle shape here).
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

  const closeChild = (child: childProcess.ChildProcess | undefined) => {
    if (!child) return;
    if (state.child === child) state.child = undefined;
    // The channel treats EOF on its stdin as the session boundary, so ending this pipe is what
    // reaps it. That happens on its own whenever omp exits, however it exits.
    if (!child.stdin?.destroyed) child.stdin?.end();
  };

  /** Open a channel and resolve with the hello's restored context (empty if none, or on timeout). */
  const open = async (ctx: ExtensionContext): Promise<string> => {
    if (!bin || !catalog || !identity) return Promise.resolve("");
    if (typeof ctx.isIdle !== "function") {
      // Refuse rather than degrade. Without a positive idle proof this extension cannot choose
      // between an idle send and a steer, and guessing would deliver into a running turn.
      ctx.ui?.notify?.(
        "st2: this omp build exposes no ctx.isIdle(); refusing to open the st2 channel",
        "error",
      );
      return Promise.resolve("");
    }
    // Close the PREVIOUS session's channel and wait (bounded) before spawning: the successor
    // shares the seat's record, and a predecessor draining its queued frames after the new
    // session's seed would land stale state into fresh records.
    const previous = state.child;
    closeChild(previous);
    await awaitExit(previous, 2000);

    const channelEnv: NodeJS.ProcessEnv = { ...process.env };
    if (runtimeId) channelEnv[RUNTIME_ID] = runtimeId;
    if (session) channelEnv[SESSION] = session;
    if (seq) channelEnv[SEQ] = seq;
    const child = childProcess.spawn(
      bin,
      ["--catalog", catalog, "driver", "omp-channel", "--identity", identity],
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
      // An observability pipe must never take the host down: EPIPE on a closed stdin is an
      // uncaught exception without a listener. Retire the channel instead — fail-open.
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
          // A newer control plane may speak a wire this asset was not written against. Refusing
          // is the honest outcome: presence still decays, so the agent reads as unreachable
          // rather than silently never receiving mail.
          if (frame.protocol !== PROTOCOL) {
            closeChild(child);
            ctx.ui?.notify?.(
              `st2: omp channel protocol ${frame.protocol} is not understood by this extension (expected ${PROTOCOL}); reinstall st2's hook set`,
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
          // one is rejected, so the idle proof selects the call shape. It never selects the
          // policy. Not optional-chained: reading a missing idle proof as "idle" would silently
          // turn every mid-turn delivery into a plain send.
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

      // Split on LF only. A generic line reader also splits on Unicode separators, which can
      // appear inside a message body and would corrupt the frame.
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

  // Frames are observational — st2 decides what becomes of them — and a closed channel drops
  // them silently.
  const sendState = (frame: Record<string, unknown>) => {
    const child = state.child;
    if (!child || !child.stdin || child.stdin.destroyed) return;
    child.stdin.write(JSON.stringify(frame) + "\n");
  };

  // The idle edge without `agent_settled`: `ctx.isIdle()` is still false AT `agent_end` and
  // flips true within ~250ms (measured), so idle is the first true sample of a bounded poll
  // after `agent_end`. A queued follow-up turn keeps it false, so no spurious idle blip. A
  // budget exhausted without an idle proof emits nothing: a record nobody can prove ages out
  // rather than restating a stale active.
  const IDLE_POLL_MS = 100;
  const IDLE_POLL_BUDGET_MS = 5000;

  const watchSettle = (ctx: ExtensionContext) => {
    const startedAt = Date.now();
    const poller = setInterval(() => {
      const idle = idleProof(ctx);
      if (!idle && Date.now() - startedAt < IDLE_POLL_BUDGET_MS) return;
      clearInterval(poller);
      if (idle) sendState({ type: "state", state: "idle" });
    }, IDLE_POLL_MS);
    poller.unref?.();
  };

  pi.on("agent_start", async () => sendState({ type: "state", state: "active" }));
  pi.on("agent_end", async (_event, ctx) => watchSettle(ctx));

  // omp exposes these two events but the pinned pi typings (0.84.2, which this asset imports)
  // do not declare them — measured live on omp v18.0.3. Registered through a widened view of
  // the same bound `on`; the payload shape is the one captured in
  // docs/vrs/06-omp-driver/.experiments/2026-08-25-omp-harness-integration.md.
  type ApprovalFrame = { toolName?: unknown };
  const onApproval = pi.on.bind(pi) as unknown as (
    event: "tool_approval_requested" | "tool_approval_resolved",
    handler: (event: ApprovalFrame, ctx: ExtensionContext) => void | Promise<void>,
  ) => void;
  onApproval("tool_approval_requested", async (event) => {
    const tool = typeof event.toolName === "string" ? event.toolName : "unknown";
    sendState({
      type: "state",
      state: "active",
      blockedOn: "human",
      ask: "permission",
      reason: tool,
    });
  });
  onApproval("tool_approval_resolved", async (_event, ctx) => {
    if (idleProof(ctx)) {
      sendState({ type: "state", state: "idle" });
      return;
    }
    sendState({ type: "state", state: "active" });
    watchSettle(ctx);
  });

  pi.on("session_start", async (_event, ctx) => {
    // Awaited before the session's first turn, which is what makes restored context reach the boot
    // prompt rather than the turn after it.
    const restored = await open(ctx);
    const opened = state.child;
    // Seed the observed state with the idle proof's answer at open time, so the record does not
    // wait for the first turn boundary to exist.
    if (opened) {
      sendState({
        type: "state",
        state: idleProof(ctx) ? "idle" : "active",
      });
    }
    if (restored.trim()) {
      // A custom message participates in LLM context without triggering a turn of its own.
      pi.sendMessage(
        { customType: "st2-session-start", content: restored, display: true },
        { deliverAs: "nextTurn" },
      );
    }
  });
  // Only a real quit closes from here. A session replacement also fires `session_shutdown`, and
  // it fires around the successor's `session_start`; closing on those reasons would reap the
  // channel the new session just opened. Replacement is handled by `open()` closing its
  // predecessor instead.
  pi.on("session_shutdown", async (event) => {
    if ((event as { reason?: string })?.reason === "quit") closeChild(state.child);
  });
}
