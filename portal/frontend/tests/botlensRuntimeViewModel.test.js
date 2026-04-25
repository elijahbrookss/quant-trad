import test from 'node:test'
import assert from 'node:assert/strict'

import { buildBotLensRuntimeViewModel } from '../src/features/bots/botlens/buildBotLensRuntimeViewModel.js'
import {
  selectActiveRunId,
  selectChartHistoryCacheCount,
  selectOpenTrades,
  selectSelectedSymbolBootstrapStatus,
  selectSelectedSymbolChartCandles,
  selectSelectedSymbolChartHistory,
  selectSelectedSymbolChartHistoryStatus,
  selectSelectedSymbolDecisions,
  selectSelectedSymbolKey,
  selectSelectedSymbolLogs,
  selectSelectedSymbolMetadata,
  selectSelectedSymbolOverlays,
  selectSelectedSymbolRecentTrades,
  selectSelectedSymbolSignals,
  selectSelectedSymbolState,
  selectSelectedSymbolSummary,
  selectSymbolOptions,
  selectWarningItems,
} from '../src/features/bots/botlens/state/botlensRuntimeSelectors.js'
import {
  createInitialBotLensState,
  reduceBotLensState,
} from '../src/features/bots/botlens/state/botlensRuntimeState.js'

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
            warning_id: 'warning-1',
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
          stats: { total_trades: 1, net_pnl: 12.5 },
          readiness: {
            catalog_discovered: true,
            snapshot_ready: true,
            symbol_live: true,
          },
        },
      ],
    },
    live_transport: { eligible: true },
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
        recent_trades: [{ trade_id: 'trade-recent', symbol: 'BTC', symbol_key: symbolKey }],
        decisions: [{ event_id: 'decision-1', decision_id: 'decision-1' }],
        signals: [{ signal_id: 'signal-1' }],
        logs: [],
        runtime: { status: 'running' },
        stats: { total_trades: 1, net_pnl: 12.5 },
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
  state = reduceBotLensState(state, {
    type: 'selection/bootstrapReady',
    bootstrapPayload: selectedSymbolBootstrapPayload(),
    statusMessage: 'BotLens selected-symbol snapshot ready.',
  })
  return state
}

function buildControllerLike(state, overrides = {}) {
  const selectedSymbolKey = selectSelectedSymbolKey(state)
  const selectedSummary = selectSelectedSymbolSummary(state)
  const selectedLabel = selectSelectedSymbolMetadata(state)?.display_label
    || selectedSummary?.display_label
    || selectedSymbolKey
    || '—'

  return {
    activeRunId: selectActiveRunId(state),
    bot: {
      id: 'bot-1',
      name: 'Momentum Runner',
      mode: 'live',
      playback_speed: 1,
    },
    chartCandles: selectSelectedSymbolChartCandles(state),
    chartHistory: selectSelectedSymbolChartHistory(state),
    chartHistoryCacheCount: selectChartHistoryCacheCount(state),
    chartHistoryStatus: selectSelectedSymbolChartHistoryStatus(state),
    chartOverlays: selectSelectedSymbolOverlays(state),
    chartTrades: selectSelectedSymbolRecentTrades(state),
    error: state.ui.error,
    logs: selectSelectedSymbolLogs(state),
    openTrades: selectOpenTrades(state),
    runState: state.runState,
    runtimeStatus: state.status,
    selectedLabel,
    selectedSymbolBootstrapStatus: selectSelectedSymbolBootstrapStatus(state),
    selectedSymbolDecisions: selectSelectedSymbolDecisions(state),
    selectedSymbolKey,
    selectedSymbolMetadata: selectSelectedSymbolMetadata(state),
    selectedSymbolSignals: selectSelectedSymbolSignals(state),
    selectedSymbolState: selectSelectedSymbolState(state),
    selectedSummary,
    statusMessage: state.ui.statusMessage,
    streamState: state.live.connectionState,
    symbolOptions: selectSymbolOptions(state),
    warningItems: selectWarningItems(state),
    ...overrides,
  }
}

test('runtime view model keeps current-state rows separate from retrieval-backed chart state', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'retrieval/chartSuccess',
    symbolKey: 'instrument-btc|1m',
    candles: [{ time: 1767225540, open: 0, high: 0, low: 0, close: 0 }],
    range: {
      returned_start_time: '2025-12-31T23:59:00Z',
      returned_end_time: '2026-01-01T00:00:00Z',
    },
  })
  state = reduceBotLensState(state, {
    type: 'live/messageReceived',
    message: {
      type: 'botlens_symbol_decision_delta',
      symbol_key: 'instrument-btc|1m',
      scope_seq: 23,
      stream_seq: 21,
      payload: {
        entries: [{ event_id: 'decision-2' }],
      },
    },
  })

  const model = buildBotLensRuntimeViewModel(buildControllerLike(state))

  assert.equal(model.mode, 'ready')
  assert.equal(model.currentStatePanels.overview.selectedRows.find((row) => row.key === 'base-candles')?.value, '1')
  assert.equal(model.currentStatePanels.overview.selectedRows.find((row) => row.key === 'snapshot-ready')?.value, 'Yes')
  assert.equal(model.inspection.diagnostics.checks.find((row) => row.key === 'transport')?.value, 'Yes')
  assert.equal(model.currentStatePanels.tradeActivity.logs.length, 0)
  assert.equal(model.retrievalPanels.chart.historyCount, 1)
  assert.deepEqual(
    model.retrievalPanels.chart.candles.map((row) => row.time),
    [1767225540, 1767225600],
  )
  assert.equal(model.currentStatePanels.overview.selectedRows.find((row) => row.key === 'decisions')?.value, '1')
  assert.equal(model.currentStatePanels.warnings.count, 1)
})

test('runtime view model shows symbol-switch loading without leaking prior selected-symbol chart state', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'selection/requested',
    symbolKey: 'instrument-sol|15m',
  })
  state = reduceBotLensState(state, {
    type: 'selection/bootstrapStarted',
    symbolKey: 'instrument-sol|15m',
    statusMessage: 'Loading symbol snapshot for instrument-sol|15m...',
  })

  const model = buildBotLensRuntimeViewModel(buildControllerLike(state))

  assert.equal(model.mode, 'ready')
  assert.equal(model.symbolSelector.selectedKey, 'instrument-sol|15m')
  assert.equal(model.retrievalPanels.chart.status, 'loading')
  assert.equal(model.retrievalPanels.chart.candles.length, 0)
  assert.equal(model.currentStatePanels.overview.selectedRows.find((row) => row.key === 'bootstrap-status')?.value, 'loading')
  assert.equal(model.currentStatePanels.overview.selectedRows.find((row) => row.key === 'snapshot-ready')?.value, 'No')
  assert.match(model.retrievalPanels.chart.emptyMessage, /Loading symbol snapshot/)
})

test('runtime view model surfaces explicit selected-symbol snapshot unavailability', () => {
  let state = bootstrapState()
  state = reduceBotLensState(state, {
    type: 'selection/requested',
    symbolKey: 'instrument-sol|15m',
  })
  state = reduceBotLensState(state, {
    type: 'selection/bootstrapUnavailable',
    symbolKey: 'instrument-sol|15m',
    statusMessage: 'BotLens selected-symbol snapshot is unavailable because projector state has not been built yet.',
  })

  const model = buildBotLensRuntimeViewModel(buildControllerLike(state))

  assert.equal(model.retrievalPanels.chart.status, 'unavailable')
  assert.equal(model.currentStatePanels.overview.selectedRows.find((row) => row.key === 'bootstrap-status')?.value, 'unavailable')
  assert.match(model.retrievalPanels.chart.emptyMessage, /snapshot is unavailable/)
})

test('runtime view model merges signal decision and trade events into the decision ledger', () => {
  const state = bootstrapState()
  const base = buildControllerLike(state)
  const stats = {
    quote_currency: 'USDT',
    net_pnl: 12.5,
    gross_pnl: 15.5,
    fees_paid: 3,
    completed_trades: 1,
    win_rate: 1,
  }

  const model = buildBotLensRuntimeViewModel({
    ...base,
    statusMessage: 'BotLens selected-symbol snapshot ready.',
    chartTrades: [
      {
        event_id: 'trade-entry-1',
        event_ts: '2026-01-01T00:02:00Z',
        trade_id: 'trade-1',
        symbol: 'BTC',
        direction: 'long',
        trade_state: 'open',
        entry_price: 101,
        qty: 0.5,
      },
      {
        event_id: 'trade-close-1',
        event_ts: '2026-01-01T00:03:00Z',
        trade_id: 'trade-1',
        symbol: 'BTC',
        direction: 'long',
        trade_state: 'closed',
        entry_price: 101,
        exit_price: 106,
        trade_net_pnl: 12.5,
        qty: 0.5,
      },
    ],
    selectedSymbolSignals: [
      {
        event_id: 'signal-1',
        signal_id: 'signal-1',
        event_ts: '2026-01-01T00:00:00Z',
        signal_type: 'strategy_signal',
        direction: 'long',
        signal_price: 100,
        symbol: 'BTC',
        timeframe: '1m',
      },
    ],
    selectedSymbolDecisions: [
      {
        event_id: 'decision-1',
        decision_id: 'decision-1',
        event_ts: '2026-01-01T00:01:00Z',
        decision_state: 'accepted',
        direction: 'long',
        signal_price: 100,
        symbol: 'BTC',
        timeframe: '1m',
      },
    ],
    selectedSymbolState: {
      ...base.selectedSymbolState,
      stats,
    },
    selectedSummary: {
      ...base.selectedSummary,
      open_trade_count: 1,
      stats,
    },
  })

  assert.deepEqual(
    model.inspection.decisions.entries.map((entry) => entry.event_type),
    ['signal', 'decision', 'execution', 'execution'],
  )
  assert.equal(model.tabs.find((tab) => tab.key === 'decisions')?.badge, '4')
  assert.equal(model.inspection.decisions.walletRows.find((row) => row.key === 'quote-currency')?.value, 'USDT')
  assert.equal(model.inspection.decisions.walletRows.find((row) => row.key === 'net-pnl')?.value, '+12.50')
  assert.equal(model.inspection.decisions.walletRows.find((row) => row.key === 'closed-trades')?.value, '1')
  assert.equal(model.inspection.decisions.walletRows.find((row) => row.key === 'open-trades')?.value, '1')
  assert.equal(model.inspection.decisions.walletRows.find((row) => row.key === 'trade-events')?.value, '2')
  assert.equal(new Set(model.inspection.trades.recentTrades.map((row) => row.key)).size, 2)
})

test('runtime view model suppresses generic ready notices in the top-level modal strip', () => {
  const model = buildBotLensRuntimeViewModel(buildControllerLike(bootstrapState()))

  assert.deepEqual(model.notices, [])
})
