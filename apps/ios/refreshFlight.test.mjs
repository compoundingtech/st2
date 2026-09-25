import assert from 'node:assert/strict';
import { RefreshFlight } from './refreshFlight.ts';

const flight = new RefreshFlight();
assert.equal(flight.start(0), true);
assert.equal(flight.start(0), false);
assert.equal(flight.start(1), true, 'a new credential generation must not wait for the old request');
flight.finish(0);
assert.equal(flight.isCurrent(1), true, 'the old request must not clear the new flight');
assert.equal(flight.start(1), false);
flight.finish(1);
assert.equal(flight.start(1), true);
