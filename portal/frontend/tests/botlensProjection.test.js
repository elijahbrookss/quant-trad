import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applyCandleDelta,
  applySymbolSnapshot,
  applyDecisionDelta,
  applyHistoryPage,
  applyLogDelta,
  applyOpenTradesDelta,
  applyOverlayDeltaMessage,
  applyRuntimeDelta,
  applySummaryDelta,
  applyTradeDelta,
  canonicalSeriesKey,
  createRunStore,
  getSelectedSymbolSnapshot,
  mergeCanonicalCandles,
  normalizeSeriesKey,
  selectSymbol,
} from '../src/components/bots/botlensProjection.js'
import { setLogSink } from '../src/utils/logger.js'

test('createRunStore builds normalized run summary and selected symbol snapshot cache', () => {
  const store = createRunStore({
    schema_version: 4,
    seq: 20,
    run_meta: { run_id: 'run-1', strategy_name: 'Momentum' },
    lifecycle: { phase: 'live', status: 'running' },
    health: {
      status: 'running',
      warning_count: 1,
      warnings: [
        {
          warning_id: 'indicator_overlay_payload_exceeded::typed_regime::instrument-btc|1m::indicator_guard',
          warning_type: 'indicator_overlay_payload_exceeded',
          indicator_id: 'typed_regime',
          title: 'Overlay payload budget exceeded',
          message: 'typed_regime exceeded the overlay payload budget.',
          count: 4,
          last_seen_at: '2026-01-01T00:04:00Z',
        },
      ],
    },
    symbol_summaries: [
      {
        symbol_key: 'instrument-btc|1M',
        symbol: 'btc',
        timeframe: '1M',
        display_label: 'BTC · 1m',
      },
    ],
    open_trades: [{ trade_id: 't-1', symbol: 'BTC', symbol_key: 'instrument-btc|1M' }],
    selected_symbol_key: 'instrument-btc|1M',
    detail: {
      symbol_key: 'instrument-btc|1M',
      symbol: 'btc',
      timeframe: '1M',
      candles: [{ time: '2026-01-01T00:00:00Z', open: 1, high: 1, low: 1, close: 1 }],
      overlays: [{ type: 'regime_overlay', payload: { regime_blocks: [{ x1: 1, x2: 2 }] } }],
    },
  })

  assert.equal(store.seq, 20)
  assert.equal(store.selectedSymbolKey, 'instrument-btc|1m')
  assert.equal(store.health.warnings[0].warning_id, 'indicator_overlay_payload_exceeded::typed_regime::instrument-btc|1m::indicator_guard')
  assert.equal(store.symbolIndex['instrument-btc|1m'].symbol, 'BTC')
  assert.equal(store.openTradesIndex['t-1'].symbol_key, 'instrument-btc|1m')
  assert.equal(getSelectedSymbolSnapshot(store).overlays[0].overlay_id, 'type:regime_overlay')
  assert.equal(getSelectedSymbolSnapshot(store).overlays[0].payload.regime_blocks[0].x1, 1)
})

test('summary and open-trade deltas update independent run-level slices', () => {
  let store = createRunStore({
    seq: 1,
    run_meta: { run_id: 'run-1' },
    symbol_summaries: [{ symbol_key: 'instrument-btc|1m', symbol: 'BTC', timeframe: '1m' }],
    open_trades: [],
    selected_symbol_key: 'instrument-btc|1m',
    detail: { symbol_key: 'instrument-btc|1m', candles: [] },
  })

  store = applySummaryDelta(store, {
    seq: 2,
    payload: {
      health: {
        status: 'running',
        warning_count: 1,
        warnings: [
          {
            warning_id: 'indicator_time_budget_exceeded::typed_regime::instrument-eth|5m::indicator_guard',
            warning_type: 'indicator_time_budget_exceeded',
            indicator_id: 'typed_regime',
            title: 'Execution budget exceeded',
            message: 'typed_regime exceeded the indicator execution budget repeatedly.',
            count: 2,
            last_seen_at: '2026-01-01T00:02:00Z',
          },
        ],
      },
      symbol_upserts: [{ symbol_key: 'instrument-eth|5m', symbol: 'ETH', timeframe: '5m', has_open_trade: true }],
    },
  })
  store = applyOpenTradesDelta(store, {
    seq: 3,
    payload: {
      upserts: [{ trade_id: 't-2', symbol: 'ETH', symbol_key: 'instrument-eth|5m' }],
      removals: [],
    },
  })

  assert.equal(store.seq, 3)
  assert.equal(store.health.status, 'running')
  assert.equal(store.health.warnings[0].warning_type, 'indicator_time_budget_exceeded')
  assert.equal(store.symbolIndex['instrument-eth|5m'].symbol, 'ETH')
  assert.equal(store.openTradesIndex['t-2'].symbol_key, 'instrument-eth|5m')
})

test('typed symbol reducers update only the targeted symbol cache entry', () => {
  let store = createRunStore({
    seq: 1,
    run_meta: { run_id: 'run-1' },
    symbol_summaries: [{ symbol_key: 'instrument-btc|1m', symbol: 'BTC', timeframe: '1m' }],
    open_trades: [],
    selected_symbol_key: 'instrument-btc|1m',
    detail: {
      symbol_key: 'instrument-btc|1m',
      candles: [{ time: 10, open: 1, high: 1, low: 1, close: 1 }],
      overlays: [{ type: 'regime_overlay', payload: { regime_blocks: [{ x1: 10, x2: 15 }] } }],
      recent_trades: [],
      logs: [],
      decisions: [],
    },
  })

  store = applySymbolSnapshot(store, {
    seq: 2,
    detail: {
      symbol_key: 'instrument-eth|5m',
      symbol: 'ETH',
      timeframe: '5m',
      candles: [{ time: 20, open: 2, high: 2, low: 2, close: 2 }],
      overlays: [{ type: 'regime_overlay', payload: { regime_blocks: [{ x1: 20, x2: 21 }] } }],
    },
  })
  store = selectSymbol(store, 'instrument-eth|5m')
  store = applyCandleDelta(store, {
    seq: 3,
    symbol_key: 'instrument-eth|5m',
    event_time: '2026-01-01T00:00:25Z',
    payload: { candle: { time: 25, open: 3, high: 3, low: 3, close: 3 } },
  })
  store = applyOverlayDeltaMessage(store, {
    seq: 4,
    symbol_key: 'instrument-eth|5m',
    event_time: '2026-01-01T00:00:26Z',
    payload: {
      overlay_delta: {
        ops: [
          {
            op: 'upsert',
            key: 'overlay:regime',
            overlay: { type: 'regime_overlay', payload: { state: 'risk_on', regime_blocks: [{ x1: 20, x2: 21 }] } },
          },
        ],
      },
    },
  })
  store = applyTradeDelta(store, {
    seq: 5,
    symbol_key: 'instrument-eth|5m',
    event_time: '2026-01-01T00:00:27Z',
    payload: { upserts: [{ trade_id: 't-3', symbol_key: 'instrument-eth|5m', symbol: 'ETH' }], removals: [] },
  })
  store = applyLogDelta(store, {
    seq: 6,
    symbol_key: 'instrument-eth|5m',
    event_time: '2026-01-01T00:00:28Z',
    payload: { append: [{ id: 'log-1', message: 'delta log' }] },
  })
  store = applyDecisionDelta(store, {
    seq: 7,
    symbol_key: 'instrument-eth|5m',
    event_time: '2026-01-01T00:00:29Z',
    payload: { append: [{ event_id: 'decision-1', event: 'decision' }] },
  })
  store = applyRuntimeDelta(store, {
    seq: 8,
    symbol_key: 'instrument-eth|5m',
    event_time: '2026-01-01T00:00:30Z',
    payload: {
      runtime: { status: 'running', worker_count: 2 },
    },
  })

  const selected = getSelectedSymbolSnapshot(store)
  assert.deepEqual(selected.candles.map((row) => row.time), [20, 25])
  assert.equal(selected.overlays[0].type, 'regime_overlay')
  assert.equal(selected.overlays[0].payload.regime_blocks[0].x1, 20)
  assert.equal(selected.recent_trades[0].trade_id, 't-3')
  assert.equal(selected.logs[0].message, 'delta log')
  assert.equal(selected.decisions[0].event, 'decision')
  assert.equal(selected.runtime.status, 'running')
  assert.equal(selected.seq, 8)
})

test('typed symbol delta without a cached base snapshot is logged and ignored', () => {
  const events = []
  setLogSink((event) => events.push(event))
  try {
    const store = createRunStore({
      seq: 1,
      run_meta: { run_id: 'run-1' },
      symbol_summaries: [{ symbol_key: 'instrument-btc|1m', symbol: 'BTC', timeframe: '1m' }],
      open_trades: [],
      selected_symbol_key: 'instrument-btc|1m',
      detail: {
        symbol_key: 'instrument-btc|1m',
        candles: [{ time: 10, open: 1, high: 1, low: 1, close: 1 }],
      },
    })

    const next = applyCandleDelta(store, {
      seq: 2,
      symbol_key: 'instrument-eth|5m',
      payload: { candle: { time: 20, open: 2, high: 2, low: 2, close: 2 } },
    })

    assert.equal(next, store)
    assert.equal(events.at(-1)?.event, 'botlens_symbol_delta_dropped_missing_base')
    assert.equal(events.at(-1)?.context?.symbol_key, 'instrument-eth|5m')
  } finally {
    setLogSink(null)
  }
})

test('history paging prepends overlapping candles without duplication', () => {
  let store = createRunStore({
    seq: 5,
    run_meta: { run_id: 'run-1' },
    symbol_summaries: [{ symbol_key: 'instrument-btc|1m', symbol: 'BTC', timeframe: '1m' }],
    open_trades: [],
    selected_symbol_key: 'instrument-btc|1m',
    detail: {
      symbol_key: 'instrument-btc|1m',
      candles: [
        { time: 1767225600, open: 1, high: 1, low: 1, close: 1 },
        { time: 1767225660, open: 2, high: 2, low: 2, close: 2 },
      ],
    },
  })

  store = applyHistoryPage(store, {
    symbolKey: canonicalSeriesKey('instrument-btc', '1m'),
    candles: [
      { time: 1767225540, open: 0, high: 0, low: 0, close: 0 },
      { time: 1767225600, open: 1.5, high: 1.5, low: 1.5, close: 1.5 },
    ],
  })

  assert.deepEqual(getSelectedSymbolSnapshot(store).candles.map((row) => row.time), [1767225540, 1767225600, 1767225660])
})

test('series identity helpers reject non-canonical legacy keys', () => {
  assert.equal(normalizeSeriesKey('bot'), '')
  assert.equal(normalizeSeriesKey('BOT|'), '')
  assert.equal(canonicalSeriesKey('instrument-btc', ''), '')
  assert.equal(canonicalSeriesKey('instrument-btc', '1M'), 'instrument-btc|1m')
})

test('mergeCanonicalCandles normalizes identity and replaces duplicates', () => {
  const merged = mergeCanonicalCandles(
    [{ time: '2026-01-01T00:00:00Z', open: 1, high: 1, low: 1, close: 1 }],
    [{ time: 1767225600, open: 2, high: 2, low: 2, close: 2 }],
    [{ time: 1767225660, open: 3, high: 3, low: 3, close: 3 }],
  )

  assert.deepEqual(merged.map((row) => row.time), [1767225600, 1767225660])
  assert.equal(merged[0].close, 2)
})
