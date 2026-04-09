import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildBotStartupConsoleState,
  buildCurrentConsoleLine,
  classifyLifecycleTone,
  formatLifecyclePhaseLabel,
} from '../src/components/bots/botStartupConsoleModel.js'

function buildBot(overrides = {}) {
  return {
    id: 'bot-1',
    status: 'starting',
    active_run_id: 'run-12345678',
    lifecycle: {
      status: 'starting',
      phase: 'awaiting_container_boot',
      message: 'Waiting for runtime container bootstrap.',
      metadata: {},
      failure: {},
      updated_at: '2026-04-06T07:43:37Z',
      checkpoint_at: '2026-04-06T07:43:37Z',
    },
    runtime: {
      status: 'starting',
      run_id: 'run-12345678',
    },
    ...overrides,
  }
}

test('formats backend lifecycle phase labels for operator stream rendering', () => {
  assert.equal(formatLifecyclePhaseLabel('awaiting_container_boot'), 'Awaiting container boot')
  assert.equal(formatLifecyclePhaseLabel('telemetry_degraded'), 'Telemetry degraded')
})

test('builds an active current line from backend lifecycle truth', () => {
  const current = buildCurrentConsoleLine(buildBot())

  assert.equal(current.phase, 'awaiting_container_boot')
  assert.equal(current.label, 'Awaiting container boot')
  assert.equal(current.animated, true)
  assert.equal(current.message, 'Waiting for runtime container bootstrap.')
  assert.equal(current.tone, 'sky')
})

test('records lifecycle entries and series progress lines into a local stream session', () => {
  const booting = buildBot({
    lifecycle: {
      status: 'starting',
      phase: 'spawning_series_workers',
      message: 'Spawning runtime workers for planned symbol shards.',
      metadata: {
        series_progress: {
          total_series: 2,
          workers_planned: 2,
          workers_spawned: 2,
          bootstrapped_series: [],
          awaiting_first_snapshot_series: [],
          live_series: [],
          failed_series: [],
          series: {
            CL: {
              symbol: 'CL',
              status: 'spawned',
              worker_id: 'worker-1',
              series_key: 'instrument-cl|1m',
              updated_at: '2026-04-06T07:43:40Z',
            },
            NG: {
              symbol: 'NG',
              status: 'warming_up',
              worker_id: 'worker-2',
              series_key: 'instrument-ng|1m',
              updated_at: '2026-04-06T07:43:41Z',
            },
          },
        },
      },
      failure: {},
      updated_at: '2026-04-06T07:43:39Z',
      checkpoint_at: '2026-04-06T07:43:39Z',
    },
  })

  const state = buildBotStartupConsoleState(null, booting)

  assert.equal(state.entries[0].phase, 'spawning_series_workers')
  assert.equal(state.entries.some((entry) => entry.kind === 'series' && entry.symbol === 'CL'), true)
  assert.equal(state.entries.some((entry) => entry.kind === 'series' && entry.symbol === 'NG'), true)
  assert.match(state.current.meta, /2 series/)
  assert.match(state.current.meta, /2\/2 workers/)
})

test('failure phases stay diagnostic and preserve the failed checkpoint', () => {
  const failed = buildBot({
    status: 'startup_failed',
    lifecycle: {
      status: 'startup_failed',
      phase: 'startup_failed',
      message: 'Worker bootstrap failed before live state.',
      metadata: {},
      failure: {
        message: 'GC degraded during warm-up.',
      },
      updated_at: '2026-04-06T07:43:52Z',
      checkpoint_at: '2026-04-06T07:43:52Z',
    },
    runtime: {
      status: 'startup_failed',
      run_id: 'run-12345678',
    },
  })

  const state = buildBotStartupConsoleState(null, failed)

  assert.equal(classifyLifecycleTone({ phase: 'startup_failed', status: 'startup_failed', failure: { message: 'boom' } }), 'rose')
  assert.equal(state.current.animated, false)
  assert.equal(state.current.tone, 'rose')
  assert.equal(state.entries.at(-1)?.message, 'GC degraded during warm-up.')
})

test('live transition preserves prior stream history and resolves the active line', () => {
  const warming = buildBot({
    lifecycle: {
      status: 'starting',
      phase: 'awaiting_first_snapshot',
      message: 'Waiting for first merged runtime snapshot.',
      metadata: {
        series_progress: {
          total_series: 1,
          workers_planned: 1,
          workers_spawned: 1,
          bootstrapped_series: ['CL'],
          awaiting_first_snapshot_series: ['CL'],
          live_series: [],
          failed_series: [],
          series: {
            CL: {
              symbol: 'CL',
              status: 'awaiting_first_snapshot',
              worker_id: 'worker-1',
              series_key: 'instrument-cl|1m',
              updated_at: '2026-04-06T07:43:54Z',
            },
          },
        },
      },
      failure: {},
      updated_at: '2026-04-06T07:43:54Z',
      checkpoint_at: '2026-04-06T07:43:54Z',
    },
  })
  const warmingState = buildBotStartupConsoleState(null, warming)

  const live = buildBot({
    status: 'running',
    lifecycle: {
      status: 'running',
      phase: 'live',
      message: 'Runtime is live and emitting snapshots.',
      metadata: {
        series_progress: {
          total_series: 1,
          workers_planned: 1,
          workers_spawned: 1,
          bootstrapped_series: ['CL'],
          awaiting_first_snapshot_series: [],
          live_series: ['CL'],
          failed_series: [],
          series: {
            CL: {
              symbol: 'CL',
              status: 'live',
              worker_id: 'worker-1',
              series_key: 'instrument-cl|1m',
              updated_at: '2026-04-06T07:44:01Z',
            },
          },
        },
      },
      failure: {},
      updated_at: '2026-04-06T07:44:01Z',
      checkpoint_at: '2026-04-06T07:44:01Z',
    },
    runtime: {
      status: 'running',
      run_id: 'run-12345678',
    },
  })

  const liveState = buildBotStartupConsoleState(warmingState, live)

  assert.equal(liveState.entries.length > warmingState.entries.length, true)
  assert.equal(liveState.current.label, 'Live')
  assert.equal(liveState.current.animated, false)
  assert.equal(liveState.current.tone, 'emerald')
  assert.equal(liveState.entries.some((entry) => entry.kind === 'series' && entry.symbol === 'CL' && entry.phase === 'live'), true)
})

