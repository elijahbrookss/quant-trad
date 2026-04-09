import test from 'node:test'
import assert from 'node:assert/strict'

import { buildBotCardViewModel, buildBotFleetSummary } from '../src/components/bots/botControlSurfaceModel.js'
import { getBotCardDisplayState } from '../src/components/bots/botStatusModel.js'

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
    ['View Diagnostics', 'Restart', 'Delete'],
  )
  assert.deepEqual(
    startingState.allowedActions.map((action) => action.label),
    ['Cancel'],
  )
})

test('bot row view model preserves essential strategy, symbol, timeframe, and temporal context', () => {
  const view = buildBotCardViewModel(
    buildBot({
      status: 'failed_start',
      active_run_id: 'run-12345678',
      lifecycle: {
        status: 'startup_failed',
        phase: 'startup_failed',
        reason: 'startup_failed',
        failure: { message: 'Worker worker-1 exited with code 1' },
        updated_at: '2026-04-06T12:04:55Z',
      },
    }),
    {
      strategyLookup: buildStrategyLookup(),
      nowEpochMs: Date.parse('2026-04-06T12:05:10Z'),
    },
  )

  assert.equal(view.strategyLabel, 'Breakout Ladder')
  assert.equal(view.symbolsLabel, 'BTC, ETH, XRP')
  assert.equal(view.metaItems.find((item) => item.key === 'timeframe')?.value, '1H')
  assert.equal(view.metaItems.find((item) => item.key === 'run')?.value, 'run-1234')
  assert.equal(view.contextItems[0].label, 'Phase')
  assert.equal(view.contextItems[1].label, 'Failed')
  assert.equal(view.contextItems.some((item) => item.value === 'No last trace'), false)
})
