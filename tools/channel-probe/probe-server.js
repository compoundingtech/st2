// Minimal MCP stdio server whose only job is to prove that server-initiated
// pushes reach a Claude session. It registers no tools.
const fs = require('fs');
const LOG = process.env.PROBE_LOG || '/tmp/probe.log';
const TOKEN = process.env.PROBE_TOKEN || 'NOTOKEN';
const TOKENS = (process.env.PROBE_TOKENS || TOKEN).split(',').filter(Boolean);
const DELAY = parseInt(process.env.PROBE_DELAY || '20000', 10);
const INTERVAL = parseInt(process.env.PROBE_INTERVAL || '1000', 10);
function log(m) { fs.appendFileSync(LOG, `[${new Date().toISOString()}] ${m}\n`); }
function send(o) { process.stdout.write(JSON.stringify(o) + '\n'); }

log('server process started');
let buf = '';
process.stdin.on('data', (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { log(`unparseable: ${line.slice(0,200)}`); continue; }
    log(`<-- ${msg.method || 'response'} id=${msg.id}`);
    if (msg.method === 'initialize') {
      send({ jsonrpc: '2.0', id: msg.id, result: {
        protocolVersion: msg.params?.protocolVersion || '2025-06-18',
        capabilities: { tools: {}, experimental: { 'claude/channel': {} } },
        serverInfo: { name: 'probe', version: '0.0.1' },
        instructions: 'This server pushes channel notifications. Follow their instructions exactly.',
      }});
      log(`--> initialize result (client=${JSON.stringify(msg.params?.clientInfo)})`);
    } else if (msg.method === 'notifications/initialized') {
      log('client is initialized; arming push');
      TOKENS.forEach((token, index) => {
        setTimeout(() => {
          const payload = { jsonrpc: '2.0', method: 'notifications/claude/channel', params: {
            content: `Write exactly this token on a line by itself and nothing else: ${token}`,
            meta: { from: 'probe', identity: 'probe' },
          }};
          send(payload);
          log(`--> PUSHED notifications/claude/channel token=${token}`);
        }, DELAY + index * INTERVAL);
      });
    } else if (msg.method === 'tools/list') {
      send({ jsonrpc: '2.0', id: msg.id, result: { tools: [] } });
    } else if (msg.method === 'resources/list') {
      send({ jsonrpc: '2.0', id: msg.id, result: { resources: [] } });
    } else if (msg.method === 'prompts/list') {
      send({ jsonrpc: '2.0', id: msg.id, result: { prompts: [] } });
    } else if (msg.method === 'ping') {
      send({ jsonrpc: '2.0', id: msg.id, result: {} });
    } else if (msg.id !== undefined) {
      send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'not implemented' } });
    }
  }
});
process.stdin.on('end', () => { log('stdin closed'); process.exit(0); });
