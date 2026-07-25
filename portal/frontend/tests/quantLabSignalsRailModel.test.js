import test from 'node:test';
import assert from 'node:assert/strict';

import { buildSearchHaystack, flattenSignals } from '../src/components/quantLabSignalsRailModel.js';

test('flattenSignals preserves outputName for generated signal rows', () => {
  const rows = flattenSignals(
    {
      'indicator-1': [
        {
          signal_id: 'signal-1',
          output_name: 'confirmed_balance_breakout',
          event_key: 'confirmed_balance_breakout_long',
          known_at: '2026-01-01T00:00:00Z',
          direction: ' long ',
          series_key: 'BTCUSDT:5m',
        },
      ],
    },
    {
      'indicator-1': { id: 'indicator-1', name: 'Profile', type: 'market_profile' },
    },
  );

  assert.equal(rows.length, 1);
  assert.equal(rows[0].outputName, 'confirmed_balance_breakout');
  assert.match(buildSearchHaystack(rows[0]), /confirmed_balance_breakout/);
});
