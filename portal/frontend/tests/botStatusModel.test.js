import test from 'node:test'
import assert from 'node:assert/strict'

import { getBotCardDisplayState } from '../src/features/bots/state/botRuntimeStatus.js'

function buildBot(overrides = {}) {
  return {
    id: 'bot-1',
    name: 'CL Trend',
    status: 'idle',
    strategy_id: 'strategy-1',
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
    },
    runtime: {
      status: 'idle',
      stats: {},
    },
    ...overrides,
  }
}

test('maps granular startup phases into a single Starting card state', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'starting',
      controls: {
        can_start: false,
        can_stop: false,
        can_open_lens: false,
        can_delete: false,
        start_label: 'Starting',
      },
      lifecycle: {
        status: 'starting',
        phase: 'awaiting_container_boot',
        reason: 'container_start_pending',
        message: 'Waiting for runtime container bootstrap.',
      },
      runtime: {
        status: 'starting',
      },
    }),
  )

  assert.equal(state.displayStatus, 'Starting')
  assert.equal(state.tone, 'sky')
  assert.equal(state.detail, 'Waiting for runtime bootstrap')
})

test('surfaces granular series bootstrap progress while runtime is still starting', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'starting',
      lifecycle: {
        status: 'starting',
        phase: 'awaiting_first_snapshot',
        reason: 'awaiting_first_snapshot',
        message: 'Series bootstrap completed; waiting for first live runtime facts.',
        metadata: {
          series_progress: {
            total_series: 3,
            bootstrapped_series: ['BTC', 'ETH', 'SOL'],
            live_series: [],
          },
        },
      },
      runtime: {
        status: 'starting',
      },
    }),
  )

  assert.equal(state.displayStatus, 'Starting')
  assert.equal(state.detail, 'Bootstrap complete (0/3 series live)')
})

test('treats degraded runtime as a distinct Degraded card state', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'degraded',
      active_run_id: 'run-live-degraded',
      controls: {
        can_start: false,
        can_stop: true,
        can_open_lens: true,
        can_delete: false,
        start_label: 'Restart',
      },
      lifecycle: {
        status: 'degraded',
        phase: 'degraded',
        reason: 'runtime_degraded',
        message: 'One worker degraded.',
        telemetry: { warning_count: 2 },
      },
      runtime: {
        status: 'degraded',
        run_id: 'run-live-degraded',
        started_at: '2026-04-06T12:00:00Z',
      },
    }),
    { nowEpochMs: Date.parse('2026-04-06T12:05:00Z') },
  )

  assert.equal(state.displayStatus, 'Degraded')
  assert.equal(state.tone, 'amber')
  assert.equal(state.detail, '2 runtime warnings active')
  assert.deepEqual(
    state.allowedActions.map((action) => action.key),
    ['open', 'report', 'stop'],
  )
})

test('surfaces completed runs as Completed with rerun and lens actions', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'completed',
      controls: {
        can_start: true,
        can_stop: false,
        can_open_lens: true,
        can_delete: true,
        start_label: 'Rerun',
      },
      lifecycle: {
        status: 'completed',
        phase: 'completed',
        reason: 'run_completed',
        message: 'Run completed.',
      },
      runtime: {
        status: 'completed',
      },
      last_run_artifact: {
        started_at: '2026-04-06T12:00:00Z',
        ended_at: '2026-04-06T12:03:30Z',
      },
    }),
    { nowEpochMs: Date.parse('2026-04-06T12:05:00Z') },
  )

  assert.equal(state.displayStatus, 'Completed')
  assert.equal(state.detail, 'Run completed in 3m 30s')
  assert.deepEqual(
    state.allowedActions.map((action) => action.label),
    ['Rerun', 'View Report', 'Delete'],
  )
})

test('surfaces startup_failed lifecycle as Startup failed', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'startup_failed',
      controls: {
        can_start: false,
        can_stop: false,
        can_open_lens: true,
        can_delete: false,
        start_label: 'Starting',
      },
      active_run_id: 'run-boot-1',
      lifecycle: {
        status: 'startup_failed',
        phase: 'startup_failed',
        reason: 'startup_failed',
        message: 'Container launch failed.',
        failure: { message: 'docker launch failed' },
      },
      runtime: {
        status: 'startup_failed',
        run_id: 'run-boot-1',
      },
    }),
  )

  assert.equal(state.displayStatus, 'Startup failed')
  assert.equal(state.tone, 'rose')
  assert.equal(state.detail, 'docker launch failed')
  assert.deepEqual(
    state.allowedActions.map((action) => action.label),
    ['Restart', 'View Report', 'View Diagnostics', 'Delete'],
  )
  assert.deepEqual(
    state.allowedActions.map((action) => action.variant),
    ['primary', 'secondary', 'diagnostic', 'danger'],
  )
})

test('ignores runtime-only failure drift when lifecycle still reports Starting', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'starting',
      controls: {
        can_start: false,
        can_stop: false,
        can_open_lens: false,
        can_delete: false,
        start_label: 'Starting',
      },
      active_run_id: 'run-boot-2',
      lifecycle: {
        status: 'starting',
        phase: 'awaiting_container_boot',
        reason: 'container_start_pending',
        message: 'Awaiting runtime bootstrap.',
      },
      runtime: {
        status: 'failed',
        run_id: 'run-boot-2',
      },
    }),
    { pendingStart: true },
  )

  assert.equal(state.displayStatus, 'Starting')
  assert.equal(state.detail, 'Waiting for runtime bootstrap')
  assert.equal(state.allowedActions.some((action) => action.label === 'Starting…'), true)
})

test('surfaces crashed runs as Crashed and routes detail to diagnostics instead of Bot Lens', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'crashed',
      active_run_id: 'run-crashed-1',
      controls: {
        can_start: true,
        can_stop: false,
        can_open_lens: true,
        can_delete: true,
        start_label: 'Restart',
      },
      lifecycle: {
        status: 'crashed',
        phase: 'crashed',
        reason: 'container_exited',
        message: 'Container exited before terminal sync.',
      },
      runtime: {
        status: 'crashed',
        run_id: 'run-crashed-1',
      },
    }),
  )

  assert.equal(state.displayStatus, 'Crashed')
  assert.equal(state.tone, 'rose')
  assert.equal(state.detail, 'Container exited unexpectedly')
  assert.deepEqual(
    state.allowedActions.map((action) => action.label),
    ['Restart', 'View Report', 'View Diagnostics', 'Delete'],
  )
  assert.deepEqual(
    state.allowedActions.map((action) => action.variant),
    ['primary', 'secondary', 'diagnostic', 'danger'],
  )
})

test('maps watchdog/container crash after healthy runtime to Crashed', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'running',
      controls: {
        can_start: false,
        can_stop: false,
        can_open_lens: true,
        can_delete: false,
        start_label: 'Stop',
      },
      active_run_id: 'run-live-1',
      lifecycle: {
        status: 'running',
        phase: 'live',
        reason: 'runner_stale',
        message: 'Backend lost heartbeat.',
        heartbeat: { state: 'stale' },
        telemetry: { seq: 42 },
      },
      runtime: {
        status: 'running',
        run_id: 'run-live-1',
        seq: 42,
      },
    }),
  )

  assert.equal(state.displayStatus, 'Crashed')
  assert.equal(state.detail, 'Runtime heartbeat lost')
  assert.deepEqual(
    state.allowedActions.map((action) => action.label),
    ['Restart', 'View Report', 'View Diagnostics', 'Delete'],
  )
  assert.deepEqual(
    state.allowedActions.map((action) => action.variant),
    ['primary', 'secondary', 'diagnostic', 'danger'],
  )
})

test('keeps terminal completion over stale lifecycle crash residue after refresh', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'completed',
      controls: {
        can_start: true,
        can_stop: false,
        can_open_lens: true,
        can_delete: true,
        start_label: 'Rerun',
      },
      active_run_id: 'run-complete-1',
      lifecycle: {
        status: 'running',
        phase: 'live',
        reason: 'runner_stale',
        message: 'Backend lost heartbeat.',
        heartbeat: { state: 'stale' },
        telemetry: { run_id: 'run-complete-1' },
      },
      runtime: {
        status: 'completed',
        run_id: 'run-complete-1',
      },
      last_run_artifact: {
        started_at: '2026-04-06T12:00:00Z',
        ended_at: '2026-04-06T12:03:30Z',
      },
    }),
    { nowEpochMs: Date.parse('2026-04-06T12:05:00Z') },
  )

  assert.equal(state.displayStatus, 'Completed')
  assert.equal(state.detail, 'Run completed in 3m 30s')
  assert.deepEqual(
    state.allowedActions.map((action) => action.label),
    ['Rerun', 'View Report', 'Delete'],
  )
})

test('does not expose diagnostics from runtime.run_id alone without projected active run state', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'startup_failed',
      controls: {
        can_start: true,
        can_stop: false,
        can_open_lens: true,
        can_delete: true,
        start_label: 'Restart',
      },
      lifecycle: {
        status: 'startup_failed',
        phase: 'startup_failed',
        reason: 'startup_failed',
        message: 'Lifecycle recorded startup failure.',
        failure: { message: 'worker exited' },
      },
      runtime: {
        status: 'startup_failed',
        run_id: 'runtime-only-run-id',
      },
    }),
  )

  assert.equal(state.displayStatus, 'Startup failed')
  assert.equal(state.runId, null)
  assert.equal(state.allowedActions.some((action) => action.label === 'View Diagnostics'), false)
})

test('promotes runtime-only live telemetry into Running without waiting for a hard refresh', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'idle',
      controls: {
        can_start: false,
        can_stop: true,
        can_open_lens: true,
        can_delete: false,
        start_label: 'Stop',
      },
      lifecycle: {
        status: 'idle',
        phase: 'idle',
        reason: 'idle',
        message: '',
      },
      runtime: {
        status: 'running',
        phase: 'live',
        run_id: 'runtime-live-1',
        last_snapshot_at: '2026-04-06T12:05:00Z',
      },
    }),
    { nowEpochMs: Date.parse('2026-04-06T12:05:10Z') },
  )

  assert.equal(state.displayStatus, 'Running')
  assert.equal(state.runId, 'runtime-live-1')
  assert.equal(state.allowedActions.some((action) => action.label === 'Open Lens'), true)
  assert.equal(state.allowedActions.some((action) => action.label === 'View Report'), true)
})

test('restart clicks immediately surface Starting while the new run is being requested', () => {
  const state = getBotCardDisplayState(
    buildBot({
      status: 'completed',
      controls: {
        can_start: true,
        can_stop: false,
        can_open_lens: true,
        can_delete: true,
        start_label: 'Rerun',
      },
      lifecycle: {
        status: 'completed',
        phase: 'completed',
        reason: 'run_completed',
        message: 'Run completed.',
      },
      runtime: {
        status: 'completed',
      },
    }),
    { pendingStart: true },
  )

  assert.equal(state.displayStatus, 'Starting')
  assert.equal(state.allowedActions.some((action) => action.label === 'Starting…'), true)
})
