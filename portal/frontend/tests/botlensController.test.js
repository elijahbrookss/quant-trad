import test from 'node:test'
import assert from 'node:assert/strict'

import {
  resolveSelectedSymbolVisualRefreshIntervalMs,
  shouldRetryBotLensRunBootstrap,
  shouldRetryBotLensSelectedSymbolBootstrap,
  shouldLoadOlderBotLensHistory,
  shouldPollSelectedSymbolVisual,
} from '../src/features/bots/botlens/hooks/useBotLensController.js'

test('load older history stays blocked while a chart retrieval request is already in flight', () => {
  assert.equal(
    shouldLoadOlderBotLensHistory({
      activeRunId: 'run-1',
      selectedSymbolKey: 'instrument-btc|1m',
      chartCandles: [{ time: 1767225600, open: 1, high: 1, low: 1, close: 1 }],
      chartHistoryStatus: 'loading',
    }),
    false,
  )
})

test('load older history requires an active run, selected symbol, and at least one candle', () => {
  assert.equal(
    shouldLoadOlderBotLensHistory({
      activeRunId: 'run-1',
      selectedSymbolKey: 'instrument-btc|1m',
      chartCandles: [{ time: 1767225600, open: 1, high: 1, low: 1, close: 1 }],
      chartHistoryStatus: 'ready',
    }),
    true,
  )
  assert.equal(
    shouldLoadOlderBotLensHistory({
      activeRunId: 'run-1',
      selectedSymbolKey: 'instrument-btc|1m',
      chartCandles: [],
      chartHistoryStatus: 'ready',
    }),
    false,
  )
})

test('selected-symbol visual refresh polling only runs for a live selected symbol with a base state', () => {
  assert.equal(
    shouldPollSelectedSymbolVisual({
      open: true,
      activeRunId: 'run-1',
      transportEligible: true,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: true,
    }),
    true,
  )
  assert.equal(
    shouldPollSelectedSymbolVisual({
      open: true,
      activeRunId: 'run-1',
      transportEligible: false,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: true,
    }),
    false,
  )
  assert.equal(
    shouldPollSelectedSymbolVisual({
      open: true,
      activeRunId: 'run-1',
      transportEligible: true,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: false,
    }),
    false,
  )
})

test('selected-symbol visual refresh interval uses the contract hint when present', () => {
  assert.equal(
    resolveSelectedSymbolVisualRefreshIntervalMs({ refresh: { interval_ms: 4500 } }),
    4500,
  )
  assert.equal(
    resolveSelectedSymbolVisualRefreshIntervalMs({ refresh: { interval_ms: 0 } }),
    4000,
  )
})

test('run bootstrap retries while startup is still progressing', () => {
  assert.equal(
    shouldRetryBotLensRunBootstrap({
      state: 'awaiting_first_snapshot',
      scope: { run_id: 'run-1' },
      run: { lifecycle: { status: 'starting' } },
    }),
    true,
  )
  assert.equal(
    shouldRetryBotLensRunBootstrap({
      state: 'inactive',
      scope: { run_id: null },
      run: { lifecycle: { status: 'idle' } },
    }),
    false,
  )
})

test('selected-symbol bootstrap retries while projector snapshot is still unavailable', () => {
  assert.equal(
    shouldRetryBotLensSelectedSymbolBootstrap({
      contract_state: 'unavailable',
      scope: { run_id: 'run-1', symbol_key: 'instrument-btc|1m' },
      unavailable_reason: 'symbol_snapshot_unavailable',
    }),
    true,
  )
  assert.equal(
    shouldRetryBotLensSelectedSymbolBootstrap({
      contract_state: 'snapshot_ready',
      scope: { run_id: 'run-1', symbol_key: 'instrument-btc|1m' },
    }),
    false,
  )
})
