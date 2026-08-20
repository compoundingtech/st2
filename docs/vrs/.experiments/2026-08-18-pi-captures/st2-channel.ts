// PROTOTYPE — st2 native channel for pi, as a pi extension.
// Proves: an out-of-process inbox drop reaches a live interactive pi session
// via pi.sendUserMessage(), and that pi's own lifecycle events are observable.
import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const INBOX = process.env.ST2_LAB_INBOX ?? "/tmp/pilab/inbox";
const EVENTS = process.env.ST2_LAB_EVENTS ?? "/tmp/pilab/log/events.jsonl";

const log = (event: string, data: unknown = {}) => {
  try {
    fs.appendFileSync(EVENTS, JSON.stringify({ t: Date.now(), event, ...(data as object) }) + "\n");
  } catch {}
};

export default function (pi: ExtensionAPI) {
  const delivered = new Set<string>();

  const drain = async (ctx: any, why: string) => {
    let names: string[];
    try {
      names = fs.readdirSync(INBOX).filter((n) => n.endsWith(".md")).sort();
    } catch {
      return;
    }
    for (const name of names) {
      if (delivered.has(name)) continue;
      delivered.add(name);
      const body = fs.readFileSync(path.join(INBOX, name), "utf8").trim();
      const idle = ctx.isIdle?.() ?? true;
      log("deliver.attempt", { name, why, idle });
      try {
        if (idle) {
          await pi.sendUserMessage(body);
        } else {
          await pi.sendUserMessage(body, { deliverAs: "followUp" });
        }
        log("deliver.ok", { name, idle });
      } catch (error) {
        log("deliver.error", { name, idle, error: String(error) });
      }
    }
  };

  pi.on("session_start", async (event: any, ctx: any) => {
    fs.mkdirSync(INBOX, { recursive: true });
    log("session_start", { reason: event.reason, cwd: ctx.cwd, mode: ctx.mode, hasUI: ctx.hasUI, pid: process.pid });
    let timer: NodeJS.Timeout | undefined;
    let watcher: fs.FSWatcher | undefined;
    try {
      watcher = fs.watch(INBOX, (kind, name) => {
        log("fs.watch", { kind, name });
        void drain(ctx, "fs.watch");
      });
    } catch (error) {
      log("fs.watch.error", { error: String(error) });
    }
    timer = setInterval(() => void drain(ctx, "timer"), 5000);
    ctx.signal?.addEventListener?.("abort", () => {
      log("abort");
      watcher?.close();
      if (timer) clearInterval(timer);
    });
    void drain(ctx, "startup");
  });

  for (const name of [
    "agent_start", "agent_end", "agent_settled", "turn_start", "turn_end",
    "session_shutdown", "message_end", "input",
  ]) {
    pi.on(name as any, async (event: any, ctx: any) => {
      log(name, { idle: ctx?.isIdle?.(), pending: ctx?.hasPendingMessages?.() });
    });
  }

  pi.on("tool_call" as any, async (event: any, ctx: any) => {
    log("tool_call", { tool: event?.toolName ?? event?.name, idle: ctx?.isIdle?.() });
  });
}
