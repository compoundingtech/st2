import assert from 'node:assert/strict';
import { projectionEventsRequireRefresh } from './projectionRefresh.ts';

assert.equal(projectionEventsRequireRefresh([]), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['session/current'], body: {} }]), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['session/current', 'message/1'], body: {} }]), true);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['agent/one'], body: {} }]), true);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: [], body: {} }]), true);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['step-run/one'], body: { change: 'work.renewed' } }], 'Control'), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['step-run/one'], body: { change: 'work.claimed' } }], 'Control'), true);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['step-run/one'], body: { change: 'work.claimed' } }], 'Chat'), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['agent/one'], body: { change: 'harness.observed', state: 'idle' } }], 'Chat'), false);
assert.equal(projectionEventsRequireRefresh([{ resource_ids: ['agent/one'], body: { change: 'harness.observed', state: 'failed' } }], 'Chat'), true);
