import assert from 'node:assert/strict';
import { projectionEventsRequireRefresh } from './projectionRefresh.ts';

assert.equal(projectionEventsRequireRefresh([]), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['session/current'] }]), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['session/current', 'message/1'] }]), true);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['agent/one'] }]), true);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: [] }]), true);
