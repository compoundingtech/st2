import assert from 'node:assert/strict';
import { normalizeGatewayUrl } from './gatewayUrl.ts';

// HTTPS gateways stay supported anywhere.
assert.equal(normalizeGatewayUrl(' https://gateway.example.ts.net:8443/ '), 'https://gateway.example.ts.net:8443');
// Plain HTTP is accepted only for Tailscale's IPv4 range, 100.64.0.0/10.
assert.equal(normalizeGatewayUrl('http://100.64.0.1:4102'), 'http://100.64.0.1:4102');
assert.equal(normalizeGatewayUrl('http://100.127.255.254:4102/'), 'http://100.127.255.254:4102');
for (const rejected of [
  'http://100.63.255.255:4102', // just below the range
  'http://100.128.0.1:4102', // just above the range
  'http://10.0.0.5:4102',
  'http://192.168.1.2:4102',
  'http://gateway.example.com',
  'http://100.64.0.1.evil.example:4102',
  'http://[fd7a:115c:a1e0::1]:4102',
  'http://user:pass@100.64.0.1:4102',
  'ftp://100.64.0.1',
  'not a url',
  '',
]) assert.equal(normalizeGatewayUrl(rejected), null, rejected);
