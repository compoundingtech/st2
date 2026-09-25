// A paired gateway is reached over HTTPS, or over plain HTTP only when the host is a Tailscale
// IPv4 address (100.64.0.0/10). Tailscale encrypts that path; everything else stays HTTPS-only,
// matching the app's scoped App Transport Security exception.
export function isTailnetIPv4(host: string): boolean {
  const parts = host.split('.');
  if (parts.length !== 4 || !parts.every(part => /^(0|[1-9]\d{0,2})$/.test(part) && Number(part) <= 255)) return false;
  const [first, second] = parts.map(Number);
  return first === 100 && second >= 64 && second <= 127;
}

export function normalizeGatewayUrl(input: string): string | null {
  let url: URL;
  try { url = new URL(input.trim()); } catch { return null; }
  if (url.username || url.password) return null;
  if (url.protocol !== 'https:' && !(url.protocol === 'http:' && isTailnetIPv4(url.hostname))) return null;
  return `${url.protocol}//${url.host}${url.pathname.replace(/\/+$/, '')}`;
}
