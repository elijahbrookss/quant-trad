import test from 'node:test'
import assert from 'node:assert/strict'

import { chooseBotLensRunSelection } from '../src/components/bots/botlensRunSelection.js'

test('defaults to the active run when selection is automatic', () => {
  const result = chooseBotLensRunSelection({
    currentRunId: 'old-run',
    runs: [
      { run_id: 'new-run', is_active: true },
      { run_id: 'old-run', is_active: false },
    ],
    activeRunId: 'new-run',
    selectionMode: 'auto',
    previousActiveRunId: 'old-run',
  })

  assert.deepEqual(result, {
    runId: 'new-run',
    selectionMode: 'auto',
  })
})

test('preserves an explicitly selected archived run', () => {
  const result = chooseBotLensRunSelection({
    currentRunId: 'archived-run',
    runs: [
      { run_id: 'active-run', is_active: true },
      { run_id: 'archived-run', is_active: false },
    ],
    activeRunId: 'active-run',
    selectionMode: 'manual',
    previousActiveRunId: 'older-active-run',
  })

  assert.deepEqual(result, {
    runId: 'archived-run',
    selectionMode: 'manual',
  })
})

test('snaps to the replacement active run after restart when the prior active run was selected', () => {
  const result = chooseBotLensRunSelection({
    currentRunId: 'run-a',
    runs: [
      { run_id: 'run-b', is_active: true },
      { run_id: 'run-a', is_active: false },
    ],
    activeRunId: 'run-b',
    selectionMode: 'manual',
    previousActiveRunId: 'run-a',
  })

  assert.deepEqual(result, {
    runId: 'run-b',
    selectionMode: 'auto',
  })
})

