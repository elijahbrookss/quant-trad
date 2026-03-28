import test from 'node:test';
import assert from 'node:assert/strict';

import {
  formatSignalIdSuffix,
  formatSignalLabelWithId,
  resolveSignalChartEpoch,
  resolveSignalId,
} from '../src/components/indicatorSignalDebug.js';

test('signal debug helpers use backend signal ids for display', () => {
  const signal = {
    signal_id: 'sig_1234567890abcdefabcd',
    event_key: 'balance_breakout_long',
  };

  assert.equal(resolveSignalId(signal), 'sig_1234567890abcdefabcd');
  assert.equal(formatSignalIdSuffix(signal), 'abcd');
  assert.equal(formatSignalLabelWithId('Balance Breakout Long', signal), 'Balance Breakout Long · abcd');
});

test('signal label formatting leaves labels clean when backend id missing', () => {
  assert.equal(formatSignalIdSuffix({ event_key: 'balance_breakout_long' }), null);
  assert.equal(formatSignalLabelWithId('Balance Breakout Long', {}), 'Balance Breakout Long');
});

test('signal chart epoch prefers event time over known_at', () => {
  const signal = {
    event_time: '2026-03-24T02:00:00Z',
    known_at: '2026-03-24T02:17:00Z',
  };

  assert.equal(resolveSignalChartEpoch(signal), 1774317600);
});
