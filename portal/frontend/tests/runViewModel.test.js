import test from 'node:test'
import assert from 'node:assert/strict'

import {
  mapErrorToViewModel,
  mapRunToViewModel,
  normalizeComparisonStatus,
  normalizeHealthState,
  normalizeLifecycleState,
  normalizeReportStatus,
} from '../src/features/bots/viewModels/runViewModel.js'

test('run view model maps missing future backend fields to neutral unknown states', () => {
  const view = mapRunToViewModel({
    id: 'bot-1',
    name: 'CL Trend',
    status: 'completed',
    active_run_id: 'run-1',
    run: {
      summary: {
        net_pnl: 12.5,
        total_trades: 4,
      },
    },
  })

  assert.equal(view.lifecycleState, 'completed')
  assert.equal(view.healthState, 'unknown')
  assert.equal(view.reportStatus, 'unknown')
  assert.equal(view.comparisonStatus, 'unknown')
  assert.equal(view.runId, 'run-1')
  assert.equal(view.pnl, 12.5)
  assert.equal(view.totalTrades, 4)
})

test('completed lifecycle does not imply report readiness or comparison eligibility', () => {
  const view = mapRunToViewModel({
    id: 'bot-1',
    status: 'completed',
    report_status: undefined,
    comparison_status: undefined,
  })

  assert.equal(view.lifecycleState, 'completed')
  assert.equal(view.reportStatus, 'unknown')
  assert.equal(view.comparisonStatus, 'unknown')
})

test('normalizers preserve explicit semantic backend states', () => {
  assert.equal(normalizeLifecycleState('startup_failed'), 'failed')
  assert.equal(normalizeHealthState('degraded'), 'warning')
  assert.equal(normalizeReportStatus('ready'), 'ready')
  assert.equal(normalizeComparisonStatus('eligible'), 'eligible')
})

test('untyped backend error strings render as generic error view models', () => {
  const error = mapErrorToViewModel('worker exited during boot')

  assert.equal(error.code, '')
  assert.equal(error.title, 'Unexpected error')
  assert.equal(error.message, 'worker exited during boot')
  assert.equal(error.severity, 'unknown')
  assert.equal(error.category, 'unknown')
})

test('missing backend error fields do not create a primary error', () => {
  const view = mapRunToViewModel({
    id: 'bot-1',
    lifecycle: {
      failure: {},
    },
  })

  assert.equal(view.primaryError, null)
})
