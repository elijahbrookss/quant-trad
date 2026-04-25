import test from 'node:test'
import assert from 'node:assert/strict'

import {
  selectSelectedSymbolChartCandles,
  selectSelectedSymbolState,
} from '../src/features/bots/botlens/state/botlensRuntimeSelectors.js'
import {
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
  })

  assert.equal(selectSelectedSymbolState(state).candles.length, 1)
  assert.equal(state.retrieval.chartHistoryBySymbol['instrument-btc|1m'].candles.length, 1)
  assert.deepEqual(
    selectSelectedSymbolChartCandles(state).map((row) => row.time),
    [1767225540, 1767225600],
  )
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
