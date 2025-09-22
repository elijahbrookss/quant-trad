import test from 'node:test';
import assert from 'node:assert/strict';

import { runSignalGeneration } from '../src/components/indicatorSignals.js';

const START = '2024-01-01T00:00:00Z';
const END = '2024-01-01T02:00:00Z';

function createSignalsAdapter(response) {
  return async (id, payload) => {
    assert.equal(id, 'ind-1');
    assert.deepEqual(payload, {
      start: START,
      end: END,
      interval: '1h',
      symbol: 'ES',
      config: { pivot_breakout_confirmation_bars: 2 },
    });
    return response;
  };
}

test('runSignalGeneration merges overlays and toggles loading flag', async () => {
  const indicator = {
    id: 'ind-1',
    params: { pivot_breakout_confirmation_bars: 2 },
  };

  const chartState = {
    symbol: 'ES',
    interval: '1h',
    signalsConfig: { pivotBreakoutConfirmationBars: 2 },
  };

  const indColors = { 'ind-1': '#facc15' };

  let currentState = {
    overlays: [
      { ind_id: 'ind-1', source: 'indicator', payload: { markers: [], price_lines: [] } },
      { ind_id: 'ind-1', source: 'signals', payload: { markers: [{ time: 0, price: 90, color: '#fff' }], price_lines: [] } },
    ],
    signalResults: { 'ind-1': [{ legacy: true }] },
  };

  const updateCalls = [];
  const updateChart = (chartId, patch) => {
    assert.equal(chartId, 'chart-1');
    updateCalls.push(patch);
    currentState = { ...currentState, ...patch };
  };

  const getChart = () => currentState;

  const adapterResponse = {
    signals: [
      { type: 'breakout', symbol: 'ES', time: START },
    ],
    overlays: [
      {
        type: 'pivot_level',
        payload: {
          markers: [
            { time: 1704067200, price: 120, color: '#6b7280' },
          ],
          price_lines: [
            { price: 120, color: '#6b7280' },
          ],
        },
      },
    ],
  };

  let errorMsg = 'seed';
  const setError = (msg) => { errorMsg = msg; };

  const success = await runSignalGeneration({
    indicator,
    chartId: 'chart-1',
    chartState,
    startISO: START,
    endISO: END,
    indColors,
    getChart,
    updateChart,
    setError,
    signalsAdapter: createSignalsAdapter(adapterResponse),
  });

  assert.equal(success, true);
  assert.equal(errorMsg, null);
  assert.deepEqual(updateCalls[0], { signalsLoading: true });
  assert.deepEqual(updateCalls.at(-1), { signalsLoading: false });

  const overlayPatch = updateCalls.find(call => Object.prototype.hasOwnProperty.call(call, 'overlays'));
  assert.ok(overlayPatch, 'expected overlays patch to be emitted');
  assert.equal(overlayPatch.overlays.length, 2);

  const signalOverlay = overlayPatch.overlays.find(ov => ov.source === 'signals');
  assert.ok(signalOverlay, 'expected signal overlay to be present');
  assert.equal(signalOverlay.payload.markers[0].color, '#facc15');

  const finalState = getChart();
  assert.equal(finalState.signalResults['ind-1'].length, 1);
  assert.equal(finalState.signalResults['ind-1'][0].type, 'breakout');
});

test('runSignalGeneration exits early when chart context missing', async () => {
  const indicator = { id: 'ind-1', params: {} };
  let errorMsg = null;
  const updateCalls = [];
  const success = await runSignalGeneration({
    indicator,
    chartId: 'chart-1',
    chartState: { symbol: null, interval: null },
    startISO: START,
    endISO: END,
    indColors: {},
    getChart: () => ({ overlays: [] }),
    updateChart: (id, patch) => updateCalls.push({ id, patch }),
    setError: (msg) => { errorMsg = msg; },
    signalsAdapter: async () => ({ signals: [], overlays: [] }),
  });

  assert.equal(success, false);
  assert.equal(errorMsg, 'Cannot generate signals: missing chart symbol or interval.');
  assert.equal(updateCalls.length, 0);
});
