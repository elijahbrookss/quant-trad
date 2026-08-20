import test from 'node:test'
import assert from 'node:assert/strict'

import {
  selectSelectedSymbolChartCandles,
  selectSelectedSymbolChartTrades,
  selectSelectedSymbolOverlays,
  selectSelectedSymbolState,
} from '../src/features/bots/botlens/state/botlensRuntimeSelectors.js'
import {
  MAX_CHART_HISTORY_CANDLES,
  createInitialBotLensState,
  reduceBotLensState,
} from '../src/features/bots/botlens/state/botlensRuntimeState.js'

function runBootstrapPayload({ runId = 'run-1', selectedSymbolKey = 'instrument-btc|1M' } = {}) {
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
      meta: { run_id: runId, strategy_name: 'Momentum' },
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
      selected_symbol_key: selectedSymbolKey,
      symbols: [
        {
          symbol_key: selectedSymbolKey,
          identity: {
            instrument_id: String(selectedSymbolKey).split('|')[0],
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

function selectedSymbolBootstrapPayload({
  runId = 'run-1',
  symbolKey = 'instrument-btc|1M',
  seq = 22,
  baseSeq = seq,
  streamSessionId = 'stream-1',
} = {}) {
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
      run_id: runId,
      symbol_key: symbolKey,
    },
    bootstrap: {
      scope: 'selected_symbol_snapshot',
      ready: true,
      bootstrap_seq: seq,
      run_bootstrap_seq: 20,
      base_seq: baseSeq,
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
    refresh: { interval_ms: 4000 },
    live_transport: {
      eligible: true,
      stream_session_id: streamSessionId,
      selected_symbol_key: symbolKey,
      subscribe_after_bootstrap: true,
    },
  }
}

function bootstrapState() {
  let state = createInitialBotLensState({ botId: 'bot-1' })
  state = reduceBotLensState(state, {
    type: 'run/bootstrapReady',
    runBootstrap: runBootstrapPayload(),
    statusMessage: 'BotLens run bootstrap ready.',
  })
  return reduceBotLensState(state, {
    type: 'selection/bootstrapReady',
    bootstrapPayload: selectedSymbolBootstrapPayload(),
    statusMessage: 'BotLens selected-symbol snapshot ready.',
  })
}

test('run bootstrap creates separated runtime and retrieval ownership', () => {
  const state = reduceBotLensState(createInitialBotLensState({ botId: 'bot-1' }), {
    type: 'run/bootstrapReady',
    runBootstrap: runBootstrapPayload(),
    statusMessage: 'BotLens run bootstrap ready.',
  })

  assert.equal(state.status, 'ready')
  assert.equal(state.runState.runMeta.run_id, 'run-1')
  assert.deepEqual(state.runState.readiness, {
    catalog_discovered: true,
    run_live: true,
  })
  assert.equal(state.runState.transportEligible, true)
  assert.equal(state.selectedSymbolKey, 'instrument-btc|1m')
  assert.deepEqual(state.runState.symbolStates, {})
  assert.deepEqual(state.retrieval.chartHistoryBySymbol, {})
  assert.equal(state.live.connectionState, 'connecting')
})

test('run bootstrap can seed selected symbol state and live cursor without a second bootstrap fetch', () => {
  const payload = runBootstrapPayload()
  payload.bootstrap.base_seq = 18
  payload.selected_symbol = selectedSymbolBootstrapPayload().selected_symbol

  const state = reduceBotLensState(createInitialBotLensState({ botId: 'bot-1' }), {
    type: 'run/bootstrapReady',
    runBootstrap: payload,
    statusMessage: 'BotLens run bootstrap ready.',
  })

  assert.equal(selectSelectedSymbolState(state).symbol_key, 'instrument-btc|1m')
  assert.equal(selectSelectedSymbolState(state).readiness.snapshot_ready, true)
  assert.equal(selectSelectedSymbolState(state).readiness.symbol_live, true)
  assert.equal(selectSelectedSymbolState(state).continuity?.candle_count, 1)
  assert.equal(state.live.sessionId, 'stream-1')
  assert.equal(state.live.lastStreamSeq, 18)
  assert.equal(state.symbolBootstrapStatusByKey['instrument-btc|1m'], 'ready')
})

test('selected-symbol bootstrap seeds base state without touching retrieval caches', () => {
  const state = bootstrapState()

  assert.equal(selectSelectedSymbolState(state).symbol_key, 'instrument-btc|1m')
  assert.equal(selectSelectedSymbolState(state).candles.length, 1)
  assert.deepEqual(state.retrieval.chartHistoryBySymbol, {})
  assert.equal(selectSelectedSymbolChartCandles(state).length, 1)
})

test('symbol switch stays local until explicit bootstrap fills the cache miss', () => {
  const state = reduceBotLensState(bootstrapState(), {
    type: 'selection/requested',
    symbolKey: 'instrument-sol|15m',
  })

  assert.equal(state.selectedSymbolKey, 'instrument-sol|15m')
  assert.equal(selectSelectedSymbolState(state), null)
  assert.equal(state.runState.symbolStates['instrument-btc|1m'].symbol_key, 'instrument-btc|1m')
})

test('symbol switch keeps cached base state and live transport state when the target symbol is already loaded', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, { type: 'live/connectionStateChanged', connectionState: 'open' })
  state = reduceBotLensState(state, {
    type: 'selection/bootstrapReady',
    runId: 'run-1',
    symbolKey: 'instrument-eth|5m',
    bootstrapPayload: selectedSymbolBootstrapPayload({ symbolKey: 'instrument-eth|5M', seq: 25, baseSeq: 25 }),
    statusMessage: 'BotLens selected-symbol snapshot ready.',
  })

  state = reduceBotLensState(state, {
    type: 'selection/requested',
    symbolKey: 'instrument-btc|1m',
  })

  assert.equal(state.selectedSymbolKey, 'instrument-btc|1m')
  assert.equal(selectSelectedSymbolState(state).symbol_key, 'instrument-btc|1m')
  assert.equal(state.symbolBootstrapStatusByKey['instrument-btc|1m'], 'ready')
  assert.equal(state.live.connectionState, 'open')
})

test('stale selected-symbol bootstrap is ignored after the selection changes', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'selection/requested',
    symbolKey: 'instrument-eth|5m',
  })
  state = reduceBotLensState(state, {
    type: 'selection/bootstrapReady',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    bootstrapPayload: selectedSymbolBootstrapPayload({ symbolKey: 'instrument-btc|1M', seq: 24, baseSeq: 24 }),
    statusMessage: 'stale bootstrap ignored',
  })

  assert.equal(state.selectedSymbolKey, 'instrument-eth|5m')
  assert.equal(selectSelectedSymbolState(state), null)
  assert.equal(state.live.lastStreamSeq, 22)
})

test('selected-symbol handoff seeds a new base cursor before live deltas resume', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'selection/requested',
    symbolKey: 'instrument-eth|5m',
  })

  state = reduceBotLensState(state, {
    type: 'selection/bootstrapReady',
    runId: 'run-1',
    symbolKey: 'instrument-eth|5m',
    bootstrapPayload: selectedSymbolBootstrapPayload({ symbolKey: 'instrument-eth|5M', seq: 25, baseSeq: 25 }),
    statusMessage: 'BotLens selected-symbol snapshot ready.',
  })

  state = reduceBotLensState(state, {
    type: 'live/messageReceived',
    message: {
      type: 'botlens_symbol_candle_delta',
      stream_session_id: 'stream-1',
      stream_seq: 26,
      scope_seq: 26,
      symbol_key: 'instrument-eth|5m',
      payload: {
        candle: { time: 1767225660, open: 2, high: 2, low: 2, close: 2 },
      },
    },
  })

  assert.equal(state.selectedSymbolKey, 'instrument-eth|5m')
  assert.equal(selectSelectedSymbolState(state).candles.length, 2)
  assert.equal(state.live.lastStreamSeq, 26)
})

test('selected-symbol snapshot unavailable is tracked explicitly instead of fabricating base state', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'selection/requested',
    symbolKey: 'instrument-eth|5m',
  })

  state = reduceBotLensState(state, {
    type: 'selection/bootstrapUnavailable',
    symbolKey: 'instrument-eth|5m',
    statusMessage: 'BotLens selected-symbol snapshot is unavailable because projector state has not been built yet.',
  })

  assert.equal(state.selectedSymbolKey, 'instrument-eth|5m')
  assert.equal(selectSelectedSymbolState(state), null)
  assert.equal(state.symbolBootstrapStatusByKey['instrument-eth|5m'], 'unavailable')
  assert.match(state.ui.statusMessage, /snapshot is unavailable/)
})

test('chart retrieval stays out of base symbol state and composes at selector time', () => {
  const state = reduceBotLensState(bootstrapState(), {
    type: 'retrieval/chartSuccess',
    symbolKey: 'instrument-btc|1m',
    candles: [
      { time: 1767225540, open: 0, high: 0, low: 0, close: 0 },
    ],
    range: {
      returned_start_time: '2025-12-31T23:59:00Z',
      returned_end_time: '2026-01-01T00:00:00Z',
    },
    evidenceSource: { kind: 'frozen_dataset', dataset_id: 'mds-frozen' },
  })

  assert.equal(selectSelectedSymbolState(state).candles.length, 1)
  assert.equal(state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].candles.length, 1)
  assert.deepEqual(
    state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].evidenceSource,
    { kind: 'frozen_dataset', dataset_id: 'mds-frozen' },
  )
  assert.deepEqual(
    selectSelectedSymbolChartCandles(state).map((row) => row.time),
    [1767225540, 1767225600],
  )
})

test('valid candle and trade history becomes ready even when overlay evidence is incomplete', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'retrieval/chartRequest',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
  })
  assert.equal(state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].status, 'loading')

  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 1767225540, open: 99, high: 102, low: 98, close: 101 }],
    trades: [{ event_id: 'trade-1', trade_id: 'trade-1', event_ts: '2025-12-31T23:59:00Z' }],
    overlays: [],
    range: { has_more_before: true, has_more_after: false },
    tradeEvidence: { complete_for_returned_candles: true, trade_count: 1 },
    overlayEvidence: {
      coverage: 'bounded',
      complete_for_returned_candles: false,
      reason_codes: ['overlay_timeline_gap_or_order_violation'],
    },
  })

  const history = state.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.equal(history.status, 'ready')
  assert.equal(history.candles.length, 1)
  assert.equal(selectSelectedSymbolChartTrades(state).length, 1)
  assert.equal(history.tradeEvidence.complete_for_loaded_candles, true)
  assert.equal(history.overlayEvidence.complete_for_loaded_candles, false)
})

test('chart retrieval ignores stale responses from a previous run after the session changes', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, { type: 'session/reset', botId: 'bot-1' })
  state = reduceBotLensState(state, {
    type: 'run/bootstrapReady',
    runBootstrap: runBootstrapPayload({ runId: 'run-2', selectedSymbolKey: 'instrument-eth|5M' }),
    statusMessage: 'BotLens run bootstrap ready.',
  })
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 1767225540, open: 0, high: 0, low: 0, close: 0 }],
    range: {
      returned_start_time: '2025-12-31T23:59:00Z',
      returned_end_time: '2026-01-01T00:00:00Z',
    },
  })

  assert.deepEqual(state.retrieval.chartHistoryBySymbol, {})
})

test('warnings without canonical warning_id are dropped instead of aliased', () => {
  const payload = runBootstrapPayload()
  payload.run.health.warnings = [
    {
      id: 'legacy-warning-id',
      warning_type: 'indicator_overlay_payload_exceeded',
      indicator_id: 'typed_regime',
      message: 'legacy warning shape',
    },
  ]

  const state = reduceBotLensState(createInitialBotLensState({ botId: 'bot-1' }), {
    type: 'run/bootstrapReady',
    runBootstrap: payload,
    statusMessage: 'BotLens run bootstrap ready.',
  })

  assert.deepEqual(state.runState.health.warnings, [])
})


test('chart history merges newest-first trade pages without reopening a closed trade', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 120, open: 100, high: 102, low: 99, close: 101 }],
    trades: [{
      trade_id: 'trade-1',
      symbol_key: 'instrument-btc|1m',
      trade_state: 'closed',
      status: 'closed',
      entry_time: '1970-01-01T00:01:00Z',
      entry_price: 100,
      exit_time: '1970-01-01T00:02:00Z',
      exit_price: 101,
      position_commit_seq: 2,
    }],
    tradeEvidence: { complete_for_returned_candles: true, trade_count: 1 },
    overlayEvidence: { complete_for_returned_candles: false, coverage: 'live_viewport_only' },
  })
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 60, open: 99, high: 101, low: 98, close: 100 }],
    trades: [{
      trade_id: 'trade-1',
      symbol_key: 'instrument-btc|1m',
      trade_state: 'open',
      status: 'open',
      entry_time: '1970-01-01T00:01:00Z',
      entry_price: 100,
      position_commit_seq: 1,
    }],
  })

  const trades = selectSelectedSymbolChartTrades(state)
  assert.equal(trades.length, 1)
  assert.equal(trades[0].trade_state, 'closed')
  assert.equal(trades[0].exit_time, '1970-01-01T00:02:00Z')
  assert.equal(selectSelectedSymbolState(state).recent_trades.length, 0)
  assert.equal(
    state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].tradeEvidence.complete_for_returned_candles,
    true,
  )
  assert.equal(
    state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].overlayEvidence.coverage,
    'live_viewport_only',
  )
})


test('terminal BotLens uses bounded durable overlay pages while active runs prefer live projection', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 120, open: 100, high: 102, low: 99, close: 101 }],
    overlays: [{ overlay_id: 'history:newest', detail_level: 'bounded_historical_render', payload: { markers: [{ time: 120, price: 101 }] } }],
    range: { returned_start_time: '1970-01-01T00:02:00Z', returned_end_time: '1970-01-01T00:02:00Z' },
    overlayEvidence: { complete_for_returned_candles: true, coverage: 'complete', fingerprint: 'page-newest' },
  })

  assert.equal(selectSelectedSymbolOverlays(state)[0].overlay_id, 'overlay-1')

  state = {
    ...state,
    runState: {
      ...state.runState,
      lifecycle: { phase: 'completed', status: 'completed' },
    },
  }
  assert.deepEqual(
    selectSelectedSymbolOverlays(state).map((entry) => entry.overlay_id),
    ['history:newest'],
  )

  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 60, open: 99, high: 101, low: 98, close: 100 }],
    overlays: [{ overlay_id: 'history:older', detail_level: 'bounded_historical_render', payload: { markers: [{ time: 60, price: 100 }] } }],
    range: { returned_start_time: '1970-01-01T00:01:00Z', returned_end_time: '1970-01-01T00:01:00Z' },
    overlayEvidence: { complete_for_returned_candles: true, coverage: 'complete', fingerprint: 'page-older' },
  })

  assert.deepEqual(
    selectSelectedSymbolOverlays(state).map((entry) => entry.overlay_id),
    ['history:newest', 'history:older'],
  )
  assert.equal(
    state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].overlayEvidence.complete_for_loaded_candles,
    true,
  )
})


test('chart history is a bounded sliding window and focused replacements discard unrelated history', () => {
  let state = bootstrapState()
  const recent = Array.from({ length: MAX_CHART_HISTORY_CANDLES + 200 }, (_, index) => ({
    time: index + 10000,
    open: 1,
    high: 1,
    low: 1,
    close: 1,
  }))
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: recent,
    mergeMode: 'replace',
    focusTime: '1970-01-01T03:00:00Z',
    focusToken: 'trade-1:1',
    focusTradeId: 'trade-1',
  })
  let history = state.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.equal(history.candles.length, MAX_CHART_HISTORY_CANDLES)
  assert.equal(history.candles.at(-1).time, recent.at(-1).time)
  assert.equal(history.focusToken, 'trade-1:1')
  assert.equal(history.focusTradeId, 'trade-1')

  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 42, open: 2, high: 2, low: 2, close: 2 }],
    mergeMode: 'replace',
    focusTime: '1970-01-01T00:00:42Z',
    focusToken: 'trade-2:1',
    focusTradeId: 'trade-2',
  })
  history = state.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.deepEqual(history.candles.map((row) => row.time), [42])
  assert.equal(history.focusToken, 'trade-2:1')
  assert.equal(history.focusTradeId, 'trade-2')
})

test('bidirectional chart pages preserve the combined loaded-window boundaries', () => {
  let state = reduceBotLensState(bootstrapState(), {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 120, open: 1, high: 1, low: 1, close: 1 }],
    range: { has_more_before: true, has_more_after: true },
    mergeMode: 'replace',
  })
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 180, open: 2, high: 2, low: 2, close: 2 }],
    range: { has_more_before: true, has_more_after: false },
    mergeMode: 'append',
  })

  let history = state.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.deepEqual(history.candles.map((row) => row.time), [120, 180])
  assert.equal(history.range.has_more_before, true)
  assert.equal(history.range.has_more_after, false)
  assert.equal(history.range.returned_start_time, 120)
  assert.equal(history.range.returned_end_time, 180)

  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 60, open: 3, high: 3, low: 3, close: 3 }],
    range: { has_more_before: false, has_more_after: true },
    mergeMode: 'prepend',
  })

  history = state.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.deepEqual(history.candles.map((row) => row.time), [60, 120, 180])
  assert.equal(history.range.has_more_before, false)
  assert.equal(history.range.has_more_after, false)
  assert.equal(history.range.returned_start_time, 60)
  assert.equal(history.range.returned_end_time, 180)
})

test('chart history rejects stale success and failure actions by request identity', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'retrieval/chartRequest',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    requestId: 41,
  })
  state = reduceBotLensState(state, {
    type: 'retrieval/chartRequest',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    requestId: 42,
  })
  const afterStaleSuccess = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    requestId: 41,
    candles: [{ time: 60, open: 1, high: 1, low: 1, close: 1 }],
    mergeMode: 'replace',
  })
  const afterStaleFailure = reduceBotLensState(afterStaleSuccess, {
    type: 'retrieval/chartFailed',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    requestId: 41,
    error: 'stale failure',
  })

  assert.equal(afterStaleFailure.retrieval.chartHistoryBySymbol['instrument-btc|1m'].status, 'loading')
  assert.equal(afterStaleFailure.retrieval.chartHistoryBySymbol['instrument-btc|1m'].candles, undefined)

  const committed = reduceBotLensState(afterStaleFailure, {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    requestId: 42,
    candles: [{ time: 120, open: 2, high: 2, low: 2, close: 2 }],
    mergeMode: 'prepend',
  })
  const history = committed.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.equal(history.status, 'ready')
  assert.equal(history.requestId, 42)
  assert.equal(history.lastUpdateMode, 'prepend')
  assert.equal(history.lastUpdateToken, 42)
})

test('chart history retains only trades overlapping the bounded candle window', () => {
  const state = reduceBotLensState(bootstrapState(), {
    type: 'retrieval/chartSuccess',
    runId: 'run-1',
    symbolKey: 'instrument-btc|1m',
    candles: [
      { time: 100, open: 1, high: 1, low: 1, close: 1 },
      { time: 200, open: 1, high: 1, low: 1, close: 1 },
    ],
    trades: [
      {
        trade_id: 'active-before-window',
        status: 'open',
        entry_time: '1970-01-01T00:00:50Z',
      },
      {
        trade_id: 'closed-before-window',
        status: 'closed',
        entry_time: '1970-01-01T00:00:50Z',
        exit_time: '1970-01-01T00:01:30Z',
      },
      {
        trade_id: 'inside-window',
        status: 'closed',
        entry_time: '1970-01-01T00:02:00Z',
        exit_time: '1970-01-01T00:03:00Z',
      },
      {
        trade_id: 'future-trade',
        status: 'open',
        entry_time: '1970-01-01T00:05:00Z',
      },
    ],
    tradeEvidence: { complete_for_returned_candles: true, trade_count: 4 },
    mergeMode: 'replace',
  })

  const history = state.retrieval.chartHistoryBySymbol['instrument-btc|1m']
  assert.deepEqual(history.trades.map((trade) => trade.trade_id), [
    'active-before-window',
    'inside-window',
  ])
  assert.equal(history.tradeEvidence.loaded_trade_count, 2)
})

test('batched live messages preserve reducer ordering in one render action', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'live/messagesReceived',
    messages: [
      {
        type: 'botlens_run_health_delta',
        stream_session_id: 'stream-1',
        scope_seq: 23,
        stream_seq: 23,
        payload: { health: { status: 'running', warning_count: 0, warnings: [] } },
      },
      {
        type: 'botlens_run_health_delta',
        stream_session_id: 'stream-1',
        scope_seq: 24,
        stream_seq: 24,
        payload: { health: { status: 'degraded', warning_count: 1, warnings: [] } },
      },
    ],
  })
  assert.equal(state.runState.health.status, 'degraded')
  assert.equal(state.live.lastStreamSeq, 24)
})

test('batched live messages coalesce growing symbol concerns without losing facts or cursor order', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'live/messagesReceived',
    messages: [
      {
        type: 'botlens_symbol_candle_delta',
        symbol_key: 'instrument-btc|1m',
        stream_session_id: 'stream-1',
        scope_seq: 23,
        stream_seq: 23,
        payload: { candle: { time: 1767225660, open: 2, high: 3, low: 1, close: 2 } },
      },
      {
        type: 'botlens_symbol_decision_delta',
        symbol_key: 'instrument-btc|1m',
        stream_session_id: 'stream-1',
        scope_seq: 24,
        stream_seq: 24,
        payload: { entries: [{ event_id: 'decision-24' }] },
      },
      {
        type: 'botlens_symbol_candle_delta',
        symbol_key: 'instrument-btc|1m',
        stream_session_id: 'stream-1',
        scope_seq: 25,
        stream_seq: 25,
        payload: { candle: { time: 1767225720, open: 2, high: 4, low: 2, close: 3 } },
      },
    ],
  })

  const selected = selectSelectedSymbolState(state)
  assert.deepEqual(selected.candles.slice(-2).map((candle) => candle.time), [1767225660, 1767225720])
  assert.equal(selected.decisions.at(-1).event_id, 'decision-24')
  assert.equal(state.live.lastStreamSeq, 25)
  assert.equal(selected.live_cursors.scope_seq_by_concern.candles, 25)
  assert.equal(selected.live_cursors.scope_seq_by_concern.decisions, 24)
})

test('batched live messages coalesce contiguous overlay commits without delaying geometry', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'live/messagesReceived',
    messages: [
      {
        type: 'botlens_symbol_overlay_delta',
        symbol_key: 'instrument-btc|1m',
        stream_session_id: 'stream-1',
        scope_seq: 23,
        stream_seq: 23,
        payload: {
          overlay_commit_seq: 1,
          base_overlay_commit_seq: 0,
          overlay_commit_seq_status: 'overlay_scoped',
          checkpoint_kind: 'full_state',
          ops: [{
            op: 'upsert',
            key: 'overlay-live',
            overlay: {
              overlay_id: 'overlay-live',
              payload: { markers: [{ time: 1, price: 10 }] },
            },
          }],
        },
      },
      {
        type: 'botlens_symbol_overlay_delta',
        symbol_key: 'instrument-btc|1m',
        stream_session_id: 'stream-1',
        scope_seq: 24,
        stream_seq: 24,
        payload: {
          overlay_commit_seq: 2,
          base_overlay_commit_seq: 1,
          overlay_commit_seq_status: 'overlay_scoped',
          ops: [{
            op: 'patch',
            key: 'overlay-live',
            payload_patch: {
              replace: { markers: [{ time: 2, price: 11 }] },
            },
          }],
        },
      },
    ],
  })

  const selected = selectSelectedSymbolState(state)
  const overlay = selected.overlays.find((entry) => entry.overlay_id === 'overlay-live')
  assert.equal(overlay.payload.markers[0].time, 2)
  assert.equal(selected.live_cursors.overlay_commit_seq, 2)
  assert.equal(selected.live_cursors.scope_seq_by_concern.overlays, 24)
  assert.equal(state.live.lastStreamSeq, 24)
})
