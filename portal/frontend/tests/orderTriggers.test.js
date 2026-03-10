import test from 'node:test'
import assert from 'node:assert/strict'

import { buildTriggerRows } from '../src/components/strategy/utils/orderTriggers.js'

test('buildTriggerRows flattens buy and sell signals with rule names', () => {
  const instrumentResult = {
    buy_signals: [
      {
        rule_id: 'r1',
        signals: [{ time: '2024-01-01T00:00:00Z', trigger_type: 'entry' }],
      },
    ],
    sell_signals: [
      {
        rule_id: 'r2',
        matched: true,
        trigger_type: 'exit',
        timestamp: '2024-01-02T00:00:00Z',
      },
    ],
    window: { symbol: 'ES' },
  }
  const rules = [
    { id: 'r1', name: 'Long Entry' },
    { id: 'r2', name: 'Exit' },
  ]

  const rows = buildTriggerRows({ instrumentResult, rules, symbol: 'ES' })
  assert.equal(rows.length, 2)
  const buyRow = rows.find((r) => r.direction === 'BUY')
  assert.equal(buyRow.ruleName, 'Long Entry')
  assert.equal(buyRow.triggerType, 'entry')
  const sellRow = rows.find((r) => r.direction === 'SELL')
  assert.equal(sellRow.ruleName, 'Exit')
  assert.equal(sellRow.triggerType, 'exit')
})

test('buildTriggerRows handles missing instrumentResult gracefully', () => {
  const rows = buildTriggerRows()
  assert.deepEqual(rows, [])
})
