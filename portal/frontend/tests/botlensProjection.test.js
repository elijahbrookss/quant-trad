import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applyTypedSymbolDelta,
  applyCandleDelta,
  applyOpenTradesDelta,
  applyRunFaultDelta,
  applyRunHealthDelta,
  applyRunLifecycleDelta,
  applyRunSymbolCatalogDelta,
  applySelectedSymbolBootstrap,
  createRunStore,
  getSelectedSymbolState,
  normalizeSeriesKey,
  selectSymbol,
} from '../src/components/bots/botlensProjection.js'
import { setLogSink } from '../src/utils/logger.js'

function runBootstrapPayload() {
  return {
    contract: 'botlens_run_bootstrap',
    schema_version: 4,
    state: 'ready',
    contract_state: 'bootstrap_ready',
    readiness: {
      catalog_discovered: true,
      snapshot_ready: true,
      symbol_live: true,
      run_live: true,
    },
    bootstrap: {
      scope: 'run',
      ready: true,
      bootstrap_seq: 20,
      base_seq: 20,
      selected_symbol_snapshot_required: true,
    },
    run: {
      meta: { run_id: 'run-1', strategy_name: 'Momentum' },
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
      open_trades: [{ trade_id: 't-1', symbol: 'BTC', symbol_key: 'instrument-btc|1M' }],
    },
    navigation: {
      selected_symbol_key: 'instrument-btc|1M',
      symbols: [
        {
          symbol_key: 'instrument-btc|1M',
          identity: {
            instrument_id: 'instrument-btc',
            symbol: 'btc',
            timeframe: '1M',
            display_label: 'BTC · 1m',
          },
          activity: {
            status: 'running',
            last_event_at: '2026-01-01T00:05:00Z',
            candle_count: 1,
          },
          open_trade: { present: true, count: 1 },
          stats: { total_trades: 1 },
          readiness: {
            catalog_discovered: true,
            snapshot_ready: true,
            symbol_live: true,
          },
        },
      ],
    },
    live_transport: { eligible: true, stream_session_id: 'stream-1' },
  }
}

function selectedSymbolBootstrapPayload({ symbolKey = 'instrument-btc|1M', seq = 22 } = {}) {
  return {
    contract: 'botlens_selected_symbol_snapshot',
    contract_state: 'snapshot_ready',
    readiness: {
      catalog_discovered: true,
      snapshot_ready: true,
      symbol_live: true,
      run_live: true,
    },
    scope: {
      bot_id: 'bot-1',
      run_id: 'run-1',
      symbol_key: symbolKey,
    },
    bootstrap: {
      scope: 'selected_symbol_snapshot',
      ready: true,
      bootstrap_seq: seq,
      run_bootstrap_seq: 20,
      base_seq: seq,
    },
    selection: {
      selected_symbol_key: symbolKey,
      display_label: 'BTC · 1m',
    },
    selected_symbol: {
      metadata: {
        symbol_key: symbolKey,
        instrument_id: 'instrument-btc',
        symbol: 'btc',
        timeframe: '1M',
        display_label: 'BTC · 1m',
        status: 'running',
        seq,
        readiness: {
          catalog_discovered: true,
          snapshot_ready: true,
          symbol_live: true,
          run_live: true,
        },
      },
      current: {
        candles: [{ time: '2026-01-01T00:00:00Z', open: 1, high: 1, low: 1, close: 1 }],
        overlays: [{ overlay_id: 'overlay-1', type: 'regime_overlay', payload: { regime_blocks: [{ x1: 1, x2: 2 }] } }],
        recent_trades: [],
        decisions: [],
        signals: [],
        logs: [],
        runtime: { status: 'running' },
        stats: { total_trades: 1 },
        continuity: {
          candle_count: 1,
          detected_gap_count: 0,
          continuity_ratio: 1,
          series_key: symbolKey,
          timeframe: '1m',
        },
      },
    },
    live_transport: {
      eligible: true,
      stream_session_id: 'stream-1',
      selected_symbol_key: symbolKey,
      subscribe_after_bootstrap: true,
    },
  }
}

test('createRunStore builds run bootstrap state without selected-symbol state leakage', () => {
  const store = createRunStore(runBootstrapPayload())

  assert.equal(store.seq, 20)
  assert.equal(store.contractState, 'bootstrap_ready')
  assert.equal(store.transportEligible, true)
  assert.deepEqual(store.readiness, {
    catalog_discovered: true,
    run_live: true,
  })
  assert.equal(store.selectedSymbolKey, 'instrument-btc|1m')
  assert.equal(store.health.warnings[0].warning_id, 'indicator_overlay_payload_exceeded::typed_regime::instrument-btc|1m::indicator_guard')
  assert.equal(store.symbolIndex['instrument-btc|1m'].symbol, 'BTC')
  assert.deepEqual(store.symbolIndex['instrument-btc|1m'].readiness, {
    catalog_discovered: true,
    snapshot_ready: true,
    symbol_live: true,
  })
  assert.equal(store.openTradesIndex['t-1'].symbol_key, 'instrument-btc|1m')
  assert.equal(getSelectedSymbolState(store), null)
})

test('selected-symbol bootstrap seeds only the requested symbol base state', () => {
  let store = createRunStore(runBootstrapPayload())
  store = applySelectedSymbolBootstrap(store, selectedSymbolBootstrapPayload())

  const selected = getSelectedSymbolState(store)
  assert.equal(selected.symbol_key, 'instrument-btc|1m')
  assert.deepEqual(selected.readiness, {
    catalog_discovered: true,
    snapshot_ready: true,
    symbol_live: true,
    run_live: true,
  })
  assert.equal(selected.continuity?.candle_count, 1)
  assert.equal(selected.overlays[0].overlay_id, 'overlay-1')
  assert.deepEqual(selected.logs, [])
  assert.equal(store.seq, 22)
})

test('run bootstrap can seed selected symbol state and replay cursor in one response', () => {
  const payload = runBootstrapPayload()
  payload.bootstrap.base_seq = 18
  payload.selected_symbol = selectedSymbolBootstrapPayload().selected_symbol

  const store = createRunStore(payload)
  const selected = getSelectedSymbolState(store)

  assert.equal(store.streamSessionId, 'stream-1')
  assert.equal(store.lastStreamSeq, 18)
  assert.equal(store.transportEligible, true)
  assert.equal(selected.symbol_key, 'instrument-btc|1m')
  assert.equal(selected.overlays[0].overlay_id, 'overlay-1')
})

test('run deltas and selected-symbol bootstrap stay on separate client boundaries', () => {
  let store = createRunStore(runBootstrapPayload())
  store = applySelectedSymbolBootstrap(store, selectedSymbolBootstrapPayload())
  store = applyRunLifecycleDelta(store, {
    stream_session_id: 'stream-1',
    stream_seq: 23,
    scope_seq: 21,
    payload: { lifecycle: { phase: 'degraded' } },
  })
  store = applyRunHealthDelta(store, {
    stream_session_id: 'stream-1',
    stream_seq: 24,
    scope_seq: 22,
    payload: { health: { status: 'degraded', warning_count: 0, warnings: [] } },
  })
  store = applyRunFaultDelta(store, {
    stream_session_id: 'stream-1',
    stream_seq: 25,
    scope_seq: 23,
    payload: { entries: [{ event_id: 'fault-1', fault_code: 'runtime_fault' }] },
  })
  store = applyRunSymbolCatalogDelta(store, {
    stream_session_id: 'stream-1',
    stream_seq: 26,
    scope_seq: 24,
    payload: {
      upserts: [{ symbol_key: 'instrument-eth|5m', symbol: 'ETH', timeframe: '5m', status: 'degraded' }],
      removals: [],
    },
  })
  store = applyOpenTradesDelta(store, {
    stream_session_id: 'stream-1',
    stream_seq: 27,
    scope_seq: 25,
    payload: {
      upserts: [{ trade_id: 't-2', symbol: 'ETH', symbol_key: 'instrument-eth|5m' }],
      removals: [],
    },
  })

  assert.equal(store.lifecycle.phase, 'degraded')
  assert.equal(store.health.status, 'degraded')
  assert.equal(store.faults[0].event_id, 'fault-1')
  assert.equal(store.symbolIndex['instrument-eth|5m'].symbol, 'ETH')
  assert.equal(store.openTradesIndex['t-2'].symbol_key, 'instrument-eth|5m')
  assert.equal(getSelectedSymbolState(store).symbol_key, 'instrument-btc|1m')
})

test('typed symbol delta without a bootstrap base state is logged and ignored', () => {
  const events = []
  setLogSink((event) => events.push(event))
  try {
    const store = createRunStore(runBootstrapPayload())
    const next = applyCandleDelta(store, {
      stream_session_id: 'stream-1',
      stream_seq: 21,
      scope_seq: 21,
      symbol_key: 'instrument-eth|5m',
      payload: { candle: { time: 20, open: 2, high: 2, low: 2, close: 2 } },
    })

    assert.equal(next.streamSessionId, 'stream-1')
    assert.equal(next.lastStreamSeq, 20)
    assert.deepEqual(next.symbolStates, store.symbolStates)
    assert.equal(events.at(-1)?.event, 'botlens_symbol_delta_dropped_missing_base')
    assert.equal(events.at(-1)?.context?.symbol_key, 'instrument-eth|5m')
  } finally {
    setLogSink(null)
  }
})

test('selected-symbol handoff applies bootstrap at seq N and live deltas from N+1 onward', () => {
  let store = createRunStore(runBootstrapPayload())
  store = selectSymbol(store, 'instrument-eth|5m')

  const beforeBootstrap = applyTypedSymbolDelta(store, {
    type: 'botlens_symbol_candle_delta',
    stream_session_id: 'stream-1',
    stream_seq: 21,
    scope_seq: 21,
    symbol_key: 'instrument-eth|5m',
    payload: {
      candle: { time: 21, open: 2, high: 2, low: 2, close: 2 },
    },
  })

  assert.equal(beforeBootstrap.lastStreamSeq, 20)
  assert.equal(getSelectedSymbolState(beforeBootstrap), null)

  store = applySelectedSymbolBootstrap(
    beforeBootstrap,
    selectedSymbolBootstrapPayload({ symbolKey: 'instrument-eth|5M', seq: 25 }),
  )
  store = applyTypedSymbolDelta(store, {
    type: 'botlens_symbol_candle_delta',
    stream_session_id: 'stream-1',
    stream_seq: 26,
    scope_seq: 26,
    symbol_key: 'instrument-eth|5m',
    payload: {
      candle: { time: 1767225660, open: 3, high: 3, low: 3, close: 3 },
    },
  })

  assert.equal(store.lastStreamSeq, 26)
  assert.equal(getSelectedSymbolState(store).symbol_key, 'instrument-eth|5m')
  assert.deepEqual(
    getSelectedSymbolState(store).candles.map((entry) => entry.time),
    [1767225600, 1767225660],
  )
})

test('overlay delta updates selected symbol state without refresh polling', () => {
  const payload = runBootstrapPayload()
  payload.selected_symbol = selectedSymbolBootstrapPayload().selected_symbol
  let store = createRunStore(payload)

  store = applyTypedSymbolDelta(store, {
    type: 'botlens_symbol_overlay_delta',
    stream_session_id: 'stream-1',
    stream_seq: 21,
    scope_seq: 26,
    symbol_key: 'instrument-btc|1m',
    payload: {
      ops: [
        {
          op: 'upsert',
          key: 'overlay-2',
          overlay: { type: 'ema_overlay', payload: { values: [1, 2, 3] } },
        },
      ],
    },
  })

  const selected = getSelectedSymbolState(store)
  assert.equal(selected.overlays.length, 2)
  assert.equal(selected.overlays[1].overlay_id, 'overlay-2')
})

test('selected-symbol state preserves compact historical overlay summaries', () => {
  const payload = runBootstrapPayload()
  payload.selected_symbol = selectedSymbolBootstrapPayload().selected_symbol
  payload.selected_symbol.current.overlays = [
    {
      overlay_id: 'overlay-summary',
      type: 'regime_overlay',
      pane_key: 'volatility',
      pane_views: ['polyline'],
      detail_level: 'summary',
      payload_summary: {
        geometry_keys: ['polylines'],
        payload_counts: { polylines: 1 },
        point_count: 12,
      },
    },
  ]

  const store = createRunStore(payload)
  const selected = getSelectedSymbolState(store)

  assert.equal(selected.overlays.length, 1)
  assert.equal(selected.overlays[0].overlay_id, 'overlay-summary')
  assert.equal(selected.overlays[0].detail_level, 'summary')
  assert.deepEqual(selected.overlays[0].payload_summary, {
    geometry_keys: ['polylines'],
    payload_counts: { polylines: 1 },
    point_count: 12,
  })
})

test('selectSymbol retains bounded client state and waits for explicit bootstrap on cache miss', () => {
  let store = createRunStore(runBootstrapPayload())
  store = applySelectedSymbolBootstrap(store, selectedSymbolBootstrapPayload())
  store = applySelectedSymbolBootstrap(store, selectedSymbolBootstrapPayload({ symbolKey: 'instrument-eth|5m', seq: 23 }))
  store = selectSymbol(store, 'instrument-sol|15m')

  assert.equal(store.selectedSymbolKey, 'instrument-sol|15m')
  assert.equal(getSelectedSymbolState(store), null)
  assert.equal(store.symbolStates['instrument-btc|1m'].symbol_key, 'instrument-btc|1m')
  assert.equal(store.symbolStates['instrument-eth|5m'].symbol_key, 'instrument-eth|5m')
})
