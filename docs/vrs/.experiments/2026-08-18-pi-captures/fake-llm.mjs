// Minimal OpenAI-completions-compatible streaming server for pi experiments.
// Streams a short canned assistant reply; logs every request to /tmp/pilab/log/llm.jsonl.
import http from "node:http";
import fs from "node:fs";

const LOG = "/tmp/pilab/log/llm.jsonl";
const server = http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    let parsed = null;
    try { parsed = JSON.parse(body); } catch {}
    fs.appendFileSync(LOG, JSON.stringify({
      at: Date.now(), url: req.url,
      messages: parsed?.messages?.map((m) => ({
        role: m.role,
        text: typeof m.content === "string" ? m.content : JSON.stringify(m.content).slice(0, 400),
      })),
    }) + "\n");

    if (req.url.includes("/models")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ data: [{ id: "fake-1" }] }));
      return;
    }
    const id = "chatcmpl-fake";
    const chunk = (delta, finish) => `data: ${JSON.stringify({
      id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000),
      model: "fake-1",
      choices: [{ index: 0, delta, finish_reason: finish ?? null }],
    })}\n\n`;
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    res.write(chunk({ role: "assistant", content: "" }));
    const last = parsed?.messages?.[parsed.messages.length - 1];
    const seen = typeof last?.content === "string" ? last.content : JSON.stringify(last?.content ?? "");
    const all = JSON.stringify(parsed?.messages ?? "");
    const slow = all.includes("SLOW");
    // One real tool call, once: emitted when the prompt asks for it and no tool result exists yet.
    const wantsBash = all.includes("RUNBASH");
    const alreadyRan = (parsed?.messages ?? []).some((m) => m.role === "tool");
    if (wantsBash && !alreadyRan) {
      res.write(chunk({
        tool_calls: [{
          index: 0,
          id: "call_env",
          type: "function",
          function: {
            name: "bash",
            arguments: JSON.stringify({
              command:
                "echo LEAK_CHANNEL=$(env | grep -c '^ST2_PI_CHANNEL_' || true); " +
                "echo PI_SESSION_ID_SET=$([ -n \"$PI_SESSION_ID\" ] && echo yes || echo no)",
            }),
          },
        }],
      }));
      res.write(chunk({}, "tool_calls"));
      res.write(`data: [DONE]\n\n`);
      res.end();
      return;
    }
    res.write(chunk({ content: "ACK:" + seen.replace(/\s+/g, " ").slice(0, 120) }));
    let n = 0;
    const tick = () => {
      if (!slow || n >= 20) {
        res.write(chunk({}, "stop"));
        res.write(`data: [DONE]\n\n`);
        res.end();
        return;
      }
      n += 1;
      res.write(chunk({ content: " tok" + n }));
      setTimeout(tick, 750);
    };
    tick();
  });
});
server.listen(8917, "127.0.0.1", () => console.log("fake-llm on 8917"));
