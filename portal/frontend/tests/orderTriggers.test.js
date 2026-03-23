import test from 'node:test'
import assert from 'node:assert/strict'

import { buildTriggerRows } from '../src/components/strategy/utils/orderTriggers.js'

test('buildTriggerRows flattens typed preview trigger rows with rule names', () => {
  const instrumentResult = {
    trigger_rows: [
      {
        strategy_rule_id: 'r1',
        row_id: 'row-1',
        action: 'buy',
        event_key: 'balance_breakout_long',
        trigger_indicator_id: 'market_profile',
        trigger_output_name: 'balance_breakout',
        timestamp: '2024-01-01T00:00:00Z',
        epoch: 1704067200,
      },
      {
        strategy_rule_id: 'r2',
        row_id: 'row-2',
        action: 'sell',
        event_key: 'balance_breakout_short',
        trigger_indicator_id: 'market_profile',
        trigger_output_name: 'balance_breakout',
        timestamp: '2024-01-02T00:00:00Z',
        epoch: 1704153600,
        guards: [{ type: 'context_match', output_name: 'market_regime', actual: 'trend' }],
      },
    ],
    window: { symbol: 'ES', instrument_id: 'inst-1' },
  }
  const rules = [
    { id: 'r1', name: 'Long Entry' },
    { id: 'r2', name: 'Exit' },
  ]

  const rows = buildTriggerRows({ instrumentResult, rules, symbol: 'ES' })
  assert.equal(rows.length, 2)
  const buyRow = rows.find((r) => r.ruleId === 'r1')
  assert.equal(buyRow.ruleName, 'Long Entry')
  assert.equal(buyRow.triggerType, 'balance_breakout_long')
  assert.equal(buyRow.outputRef, 'balance_breakout')
  assert.equal(buyRow.indicatorRef, 'market_profile')
  const sellRow = rows.find((r) => r.ruleId === 'r2')
  assert.equal(sellRow.ruleName, 'Exit')
  assert.equal(sellRow.triggerType, 'balance_breakout_short')
  assert.equal(sellRow.guardCount, 1)
  assert.equal(rows[0].ruleId, 'r2')
})

test('buildTriggerRows handles missing instrumentResult gracefully', () => {
  const rows = buildTriggerRows()
  assert.deepEqual(rows, [])
})
