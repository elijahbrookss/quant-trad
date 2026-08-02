import test from 'node:test'
import assert from 'node:assert/strict'

import {
  assertRunInspectionScope,
  initialRunInspection,
  projectRunAsBot,
  safeRunLensOrigin,
} from '../src/features/operations/runLensRouting.js'

test('direct run links start without trusting absent navigation state', () => {
  assert.equal(initialRunInspection(null, 'run-1'), null)
})

test('matching navigation state paints immediately but stale run state is rejected', () => {
  const hint = { run: { run_id: 'run-1', bot_id: 'bot-1' }, definition: { id: 'bot-1' } }
  assert.equal(initialRunInspection(hint, 'run-1').schema_version, 'navigation_hint.v1')
  assert.equal(initialRunInspection(hint, 'run-2'), null)
})

test('authoritative background refresh must match the route run identity', () => {
  const inspection = { run: { run_id: 'run-1' }, definition: { id: 'bot-1' } }
  assert.equal(assertRunInspectionScope(inspection, 'run-1'), inspection)
  assert.throws(
    () => assertRunInspectionScope(inspection, 'run-2'),
    /mismatched identity/,
  )
})

test('run projection keeps the exact route run id even for historical runs', () => {
  const bot = projectRunAsBot({
    run: { run_id: 'historical-run', bot_id: 'bot-1', status: 'completed', run_type: 'backtest' },
    definition: { id: 'bot-1', name: 'Breakout' },
  })
  assert.equal(bot.active_run_id, 'historical-run')
  assert.equal(bot.status, 'completed')
  assert.equal(bot.name, 'Breakout')
})

test('origin-aware close navigation permits Overview and Operations only', () => {
  assert.equal(safeRunLensOrigin('/overview'), '/overview')
  assert.equal(safeRunLensOrigin('/operations?tab=research'), '/operations?tab=research')
  assert.equal(safeRunLensOrigin('/studio'), '/operations?tab=runs')
  assert.equal(safeRunLensOrigin(undefined), '/operations?tab=runs')
})
