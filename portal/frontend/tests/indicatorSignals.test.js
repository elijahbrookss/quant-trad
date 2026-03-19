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
    });
    return response;
  };
}

test('runSignalGeneration stores signals and toggles loading flag', async () => {
  const indicator = {
    id: 'ind-1',
    params: { pivot_breakout_confirmation_bars: 2 },
  };

  const chartState = {
    symbol: 'ES',
    interval: '1h',
  };

  let currentState = {
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
  };

  let errorMsg = 'seed';
  const setError = (msg) => { errorMsg = msg; };

  const success = await runSignalGeneration({
    indicator,
    chartId: 'chart-1',
    chartState,
    startISO: START,
    endISO: END,
    getChart,
    updateChart,
    setError,
    signalsAdapter: createSignalsAdapter(adapterResponse),
  });

  assert.equal(success, true);
  assert.equal(errorMsg, null);
  assert.deepEqual(updateCalls[0], {
    signalsLoading: true,
    signalsLoadingFor: 'ind-1',
    signalsLoadingByIndicator: { 'ind-1': true },
    signalsLoadingCount: 1,
  });
  assert.deepEqual(updateCalls.at(-1), {
    signalsLoading: false,
    signalsLoadingFor: null,
    signalsLoadingByIndicator: null,
    signalsLoadingCount: 0,
  });

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
    getChart: () => ({ overlays: [] }),
    updateChart: (id, patch) => updateCalls.push({ id, patch }),
    setError: (msg) => { errorMsg = msg; },
    signalsAdapter: async () => ({ signals: [] }),
  });

  assert.equal(success, false);
  assert.equal(errorMsg, 'Cannot generate signals: missing chart symbol or interval.');
  assert.equal(updateCalls.length, 0);
});
