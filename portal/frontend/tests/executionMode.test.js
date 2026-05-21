import test from 'node:test'
import assert from 'node:assert/strict'

import {
  executionModeDescription,
  executionModeUsesIntrabar,
  formatExecutionModeLabel,
  normalizeExecutionMode,
  resolveExecutionMode,
} from '../src/features/bots/executionMode.js'

test('execution mode helpers normalize supported contract values', () => {
  assert.equal(normalizeExecutionMode('FULL'), 'full')
  assert.equal(normalizeExecutionMode('walk-forward'), 'full')
  assert.equal(normalizeExecutionMode('instant'), 'fast')
  assert.equal(normalizeExecutionMode(null), 'fast')
  assert.equal(formatExecutionModeLabel('full'), 'FULL (intrabar)')
  assert.equal(formatExecutionModeLabel('fast'), 'FAST')
  assert.equal(executionModeDescription('full'), 'Slower, more realistic execution')
  assert.equal(executionModeUsesIntrabar('full'), true)
})

test('resolveExecutionMode reads persisted and runtime-shaped payloads', () => {
  assert.equal(resolveExecutionMode({ risk: { execution_mode: 'full' } }), 'full')
  assert.equal(resolveExecutionMode({ run: { config_snapshot: { execution_mode: 'full' } } }), 'full')
  assert.equal(resolveExecutionMode({ runtime_metadata: { execution_mode: 'full' } }), 'full')
  assert.equal(resolveExecutionMode({}), 'fast')
})
