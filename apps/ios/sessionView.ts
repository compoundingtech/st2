import type { Page, Session } from '../../clients/typescript/st3-client';

// The gateway's discovery fields are additional properties on the generated
// st3.client.v0 Session resource. Keep this interpretation at the UI boundary.
export type SessionView = Session & {
  managed?: false;
  driver?: string;
  native_session_id?: string | null;
  title?: string | null;
  process?: { pid: number; exact_session: boolean } | null;
};

export function isUnmanaged(session: SessionView): boolean {
  return session.managed === false;
}

export function isUnresolved(session: SessionView): boolean {
  return isUnmanaged(session) && session.native_session_id == null;
}

export function sessionLabel(session: SessionView, sourceHost: string): string {
  if (!isUnmanaged(session)) return `Managed · ${session.owner_id}`;
  const driver = session.driver ?? 'Native harness';
  if (isUnresolved(session)) return `Unmanaged · unresolved ${driver} process · ${sourceHost}`;
  return `Unmanaged · ${driver} session · ${sourceHost}`;
}

export function sessionDetail(session: SessionView): string {
  if (isUnresolved(session)) return `Running process${session.process ? ` PID ${session.process.pid}` : ''} · exact session unknown`;
  return `${session.state}${session.title ? ` · ${session.title}` : ''} · ${session.id}`;
}

export async function listSessionPages(
  list: (options: { limit: number; cursor?: string; history?: boolean }) => Promise<{ value: Page }>,
  limit: number,
  history = false,
): Promise<SessionView[]> {
  const sessions: SessionView[] = [];
  const seen = new Set<string>();
  let cursor: string | undefined;
  for (let pageNumber = 0; pageNumber < 5; pageNumber++) {
    const page = (await list({ limit, cursor, history })).value;
    sessions.push(...page.items.filter((item): item is Session => item.kind === 'session'));
    if (!page.page.has_more) break;
    const next = page.page.next_cursor;
    if (!next || seen.has(next)) throw new Error('Session pagination did not advance.');
    seen.add(next);
    cursor = next;
  }
  return sessions;
}
