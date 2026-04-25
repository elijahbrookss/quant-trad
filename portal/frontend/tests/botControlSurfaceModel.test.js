import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildBotCardViewModel,
  buildBotFleetSummary,
  sortBots,
} from '../src/features/bots/fleet/buildBotFleetViewModel.js'
import { getBotCardDisplayState } from '../src/features/bots/state/botRuntimeStatus.js'

function buildBot(overrides = {}) {
  return {
    id: 'bot-1',
    name: 'CL Trend',
    status: 'idle',
    strategy_id: 'strategy-1',
    run_type: 'backtest',
    mode: 'instant',
    updated_at: '2026-04-06T12:00:00Z',
    controls: {
      can_start: true,
      can_stop: false,
      can_open_lens: false,
      can_delete: true,
      start_label: 'Start',
    },
    lifecycle: {
      status: 'idle',
      phase: 'idle',
      reason: 'idle',
      message: '',
      metadata: {},
      failure: {},
      updated_at: '2026-04-06T12:00:00Z',
    },
    runtime: {
      status: 'idle',
      stats: {},
    },
    ...overrides,
  }
}

function buildStrategyLookup() {
  return new Map([
    [
      'strategy-1',
      {
        id: 'strategy-1',
        name: 'Breakout Ladder',
        timeframe: '1h',
        instrument_slots: [
          { symbol: 'BTC' },
          { symbol: 'ETH' },
          { symbol: 'XRP' },
        ],
      },
    ],
  ])
}

function buildDenseStrategyLookup() {
  return new Map([
    [
      'strategy-1',
      {
        id: 'strategy-1',
        name: 'Breakout Ladder',
        timeframe: '1h',
        instrument_slots: [
          { symbol: 'BTC' },
          { symbol: 'ETH' },
          { symbol: 'XRP' },
          { symbol: 'SOL' },
          { symbol: 'AVAX' },
          { symbol: 'LINK' },
        ],
      },
    ],
  ])
}

test('system state strip counts are derived from projected bot states', () => {
  const summary = buildBotFleetSummary(
    [
      buildBot({
        id: 'starting-bot',
        status: 'starting',
        lifecycle: {
          status: 'starting',
          phase: 'awaiting_container_boot',
          reason: 'container_start_pending',
          updated_at: '2026-04-06T12:04:50Z',
        },
      }),
      buildBot({
        id: 'failed-bot',
        status: 'startup_failed',
        lifecycle: {
          status: 'startup_failed',
          phase: 'startup_failed',
          reason: 'startup_failed',
          failure: { message: 'worker exited' },
          updated_at: '2026-04-06T12:04:55Z',
        },
      }),
      buildBot({
        id: 'live-bot',
        status: 'running',
        active_run_id: 'run-live',
        lifecycle: {
          status: 'running',
          phase: 'live',
          reason: 'live_runtime',
          updated_at: '2026-04-06T12:05:00Z',
        },
      }),
      buildBot({
        id: 'idle-bot',
      }),
    ],
    { nowEpochMs: Date.parse('2026-04-06T12:05:10Z') },
  )

  assert.equal(summary.starting, 1)
  assert.equal(summary.failed, 1)
  assert.equal(summary.live, 1)
  assert.equal(summary.idle, 1)
  assert.equal(summary.lastUpdatedAt, '2026-04-06T12:05:00Z')
  assert.equal(typeof summary.lastUpdatedLabel, 'string')
  assert.notEqual(summary.lastUpdatedLabel, '—')
})

test('failed rows remain diagnostics-first and starting rows remain cancel-oriented', () => {
  const failedState = getBotCardDisplayState(
    buildBot({
      status: 'startup_failed',
      active_run_id: 'run-failed',
      lifecycle: {
        status: 'startup_failed',
        phase: 'startup_failed',
        reason: 'startup_failed',
        failure: { message: 'docker launch failed' },
        updated_at: '2026-04-06T12:04:55Z',
      },
    }),
  )
  const startingState = getBotCardDisplayState(
    buildBot({
      status: 'starting',
      controls: {
        can_start: false,
        can_stop: true,
        can_open_lens: false,
        can_delete: false,
        start_label: 'Starting',
      },
      lifecycle: {
        status: 'starting',
        phase: 'container_booting',
        reason: 'container_start_pending',
        updated_at: '2026-04-06T12:04:55Z',
      },
    }),
  )

  assert.deepEqual(
    failedState.allowedActions.map((action) => action.label),
    ['Restart', 'View Report', 'View Diagnostics', 'Delete'],
  )
  assert.deepEqual(
    startingState.allowedActions.map((action) => action.label),
    ['View Report', 'Cancel'],
  )
  assert.deepEqual(
    failedState.allowedActions.map((action) => action.variant),
    ['primary', 'secondary', 'diagnostic', 'danger'],
  )
})

test('bot row view model shapes metadata, symbols, and warnings for faster scanability', () => {
  const view = buildBotCardViewModel(
    buildBot({
      id: 'bot-1234567890abcdef',
      status: 'failed_start',
      active_run_id: 'run-1234567890abcdef',
      lifecycle: {
        status: 'startup_failed',
        phase: 'startup_failed',
        reason: 'startup_failed',
        failure: { message: 'Worker worker-1 exited with code 1' },
        updated_at: '2026-04-06T12:04:55Z',
        telemetry: {
          warning_count: 2,
        },
      },
    }),
    {
      strategyLookup: buildDenseStrategyLookup(),
      nowEpochMs: Date.parse('2026-04-06T12:05:10Z'),
    },
  )

  assert.equal(view.strategyLabel, 'Breakout Ladder')
  assert.equal(view.headerMetaText, 'Breakout Ladder · Backtest · Fast · 1H')
  assert.equal(view.symbols.summaryLabel, '6 symbols')
  assert.equal(view.symbols.trackedLabel, '6 symbols tracked')
  assert.equal(view.symbols.preview, 'BTC, ETH, XRP, SOL +2 more')
  assert.equal(view.metadataItems.find((item) => item.key === 'bot-id')?.value, 'bot-123…bcdef')
  assert.equal(view.metadataItems.find((item) => item.key === 'run-id')?.value, 'run-123…bcdef')
  assert.equal(view.metadataItems.find((item) => item.key === 'run-id')?.copyable, true)
  assert.equal(view.metadataItems.find((item) => item.key === 'activity')?.label, 'Failed')
  assert.equal(view.metadataItems.find((item) => item.key === 'activity')?.value, '15s ago')
  assert.deepEqual(view.metricStats.map((item) => item.key), ['open-trades', 'total-trades', 'warnings', 'net-pnl'])
  assert.deepEqual(view.stateFacts.map((item) => item.key), [])
  assert.equal(view.bodyStats.some((item) => item.key === 'snapshot'), false)
  assert.equal(view.warningSummary.count, 2)
  assert.equal(view.warningSummary.label, '2 warnings active')
  assert.equal(view.operationalRows[0].label, 'Phase')
  assert.equal(view.actionHint.includes('diagnostics'), true)
})

test('completed bot rows keep completion timing in metadata instead of repeating it in the stats block', () => {
  const view = buildBotCardViewModel(
    buildBot({
      status: 'completed',
      active_run_id: 'run-complete',
      lifecycle: {
        status: 'completed',
        phase: 'completed',
        reason: 'run_completed',
        updated_at: '2026-04-06T12:04:55Z',
      },
      last_run_artifact: {
        started_at: '2026-04-06T11:59:55Z',
        ended_at: '2026-04-06T12:04:55Z',
      },
    }),
    {
      strategyLookup: buildStrategyLookup(),
      nowEpochMs: Date.parse('2026-04-06T12:05:10Z'),
    },
  )

  assert.equal(view.metadataItems.find((item) => item.key === 'activity')?.label, 'Completed')
  assert.equal(view.metadataItems.find((item) => item.key === 'activity')?.value, '15s ago')
  assert.deepEqual(view.stateFacts.map((item) => item.key), [])
})

test('bot sort order lives with the fleet view model instead of the card component', () => {
  const ordered = sortBots([
    buildBot({ id: 'older', name: 'Zulu', created_at: '2026-04-05T12:00:00Z' }),
    buildBot({ id: 'newer', name: 'Alpha', created_at: '2026-04-06T12:00:00Z' }),
    buildBot({ id: 'same-time-b', name: 'Bravo', created_at: '2026-04-05T12:00:00Z' }),
  ])

  assert.deepEqual(ordered.map((bot) => bot.id), ['newer', 'same-time-b', 'older'])
})
