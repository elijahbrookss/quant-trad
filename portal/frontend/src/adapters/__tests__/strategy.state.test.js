import test from 'node:test'
import assert from 'node:assert/strict'

import { mergeStrategyState } from '../../hooks/strategy/useStrategyData.js'

test('mergeStrategyState ignores older strategy payloads so saved rules are not overwritten', () => {
  const current = {
    id: 'strategy-1',
    updated_at: '2026-04-05T10:05:00Z',
    rules: [
      { id: 'rule-1', name: 'Existing' },
      { id: 'rule-2', name: 'Newly Saved' },
    ],
    decision: {
      rules: [
        { id: 'rule-1', name: 'Existing' },
        { id: 'rule-2', name: 'Newly Saved' },
      ],
    },
  }

  const staleIncoming = {
    id: 'strategy-1',
    updated_at: '2026-04-05T10:04:00Z',
    rules: [
      { id: 'rule-1', name: 'Existing' },
    ],
    decision: {
      rules: [
        { id: 'rule-1', name: 'Existing' },
      ],
    },
  }

  const merged = mergeStrategyState(current, staleIncoming)

  assert.equal(merged, current)
  assert.equal(merged.rules.length, 2)
  assert.equal(merged.decision.rules.length, 2)
})

test('mergeStrategyState accepts newer strategy payloads', () => {
  const current = {
    id: 'strategy-1',
    updated_at: '2026-04-05T10:04:00Z',
    rules: [{ id: 'rule-1', name: 'Existing' }],
  }

  const incoming = {
    id: 'strategy-1',
    updated_at: '2026-04-05T10:05:00Z',
    rules: [
      { id: 'rule-1', name: 'Existing' },
      { id: 'rule-2', name: 'Newly Saved' },
    ],
  }

  const merged = mergeStrategyState(current, incoming)

  assert.notEqual(merged, current)
  assert.equal(merged.rules.length, 2)
})
