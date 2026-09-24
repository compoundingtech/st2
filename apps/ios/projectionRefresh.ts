import type { ProjectionEvent } from '../../clients/typescript/st3-client';

// Conversation changes have their own visible-session subscription. They must not
// force a full fleet projection refresh on every transcript event.
export function projectionEventsRequireRefresh(events: Pick<ProjectionEvent, 'resource_ids'>[]): boolean {
  return events.some(event => !event.resource_ids.length || event.resource_ids.some(id => !id.startsWith('session/')));
}
