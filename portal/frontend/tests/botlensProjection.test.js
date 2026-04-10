import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applyDetailDelta,
  applyDetailSnapshot,
  applyHistoryPage,
  applyOpenTradesDelta,
  applySummaryDelta,
  canonicalSeriesKey,
  createRunStore,
  getSelectedDetail,
  mergeCanonicalCandles,
  normalizeSeriesKey,
  selectSymbol,
} from '../src/components/bots/botlensProjection.js'
import { setLogSink } from '../src/utils/logger.js'

test('createRunStore builds normalized run summary and selected detail cache', () => {
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
      overlays: [{ type: 'regime_overlay', payload: { state: 'risk_on' } }],
    },
  })

  assert.equal(store.seq, 20)
  assert.equal(store.selectedSymbolKey, 'instrument-btc|1m')
  assert.equal(store.health.warnings[0].warning_id, 'indicator_overlay_payload_exceeded::typed_regime::instrument-btc|1m::indicator_guard')
  assert.equal(store.symbolIndex['instrument-btc|1m'].symbol, 'BTC')
  assert.equal(store.openTradesIndex['t-1'].symbol_key, 'instrument-btc|1m')
  assert.equal(getSelectedDetail(store).overlays[0].overlay_id, 'type:regime_overlay')
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

test('detail snapshots and deltas update only the targeted symbol cache entry', () => {
  let store = createRunStore({
    seq: 1,
    run_meta: { run_id: 'run-1' },
    symbol_summaries: [{ symbol_key: 'instrument-btc|1m', symbol: 'BTC', timeframe: '1m' }],
    open_trades: [],
    selected_symbol_key: 'instrument-btc|1m',
    detail: {
      symbol_key: 'instrument-btc|1m',
      candles: [{ time: 10, open: 1, high: 1, low: 1, close: 1 }],
      overlays: [],
      recent_trades: [],
      logs: [],
      decisions: [],
    },
  })

  store = applyDetailSnapshot(store, {
    seq: 2,
    detail: {
      symbol_key: 'instrument-eth|5m',
      symbol: 'ETH',
      timeframe: '5m',
      candles: [{ time: 20, open: 2, high: 2, low: 2, close: 2 }],
    },
  })
  store = selectSymbol(store, 'instrument-eth|5m')
  store = applyDetailDelta(store, {
    seq: 3,
    symbol_key: 'instrument-eth|5m',
    payload: {
      detail_seq: 3,
      candle: { time: 25, open: 3, high: 3, low: 3, close: 3 },
      overlay_delta: {
        ops: [
          { op: 'upsert', key: 'overlay:regime', overlay: { type: 'regime_overlay', payload: { state: 'risk_on' } } },
        ],
      },
      trade_upserts: [{ trade_id: 't-3', symbol_key: 'instrument-eth|5m', symbol: 'ETH' }],
      log_append: [{ id: 'log-1', message: 'delta log' }],
      decision_append: [{ event_id: 'decision-1', event: 'decision' }],
    },
  })

  const selected = getSelectedDetail(store)
  assert.deepEqual(selected.candles.map((row) => row.time), [20, 25])
  assert.equal(selected.overlays[0].overlay_id, 'overlay:regime')
  assert.equal(selected.recent_trades[0].trade_id, 't-3')
  assert.equal(selected.logs[0].message, 'delta log')
  assert.equal(selected.decisions[0].event, 'decision')
})

test('detail delta without a cached base snapshot is logged and ignored', () => {
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

    const next = applyDetailDelta(store, {
      seq: 2,
      symbol_key: 'instrument-eth|5m',
      payload: { detail_seq: 2, candle: { time: 20, open: 2, high: 2, low: 2, close: 2 } },
    })

    assert.equal(next, store)
    assert.equal(events.at(-1)?.event, 'botlens_detail_delta_dropped_missing_base')
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

  assert.deepEqual(getSelectedDetail(store).candles.map((row) => row.time), [1767225540, 1767225600, 1767225660])
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
