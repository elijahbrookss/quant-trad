import test from 'node:test'
import assert from 'node:assert/strict'

import { countIndicatorRuleUsage, requiresDetachConfirm, findBrokenRuleIndicators } from '../src/components/strategy/utils/indicatorUsage.js'

const rules = [
  { id: 'r1', conditions: [{ indicator_id: 'a' }, { indicator_id: 'b' }] },
  { id: 'r2', conditions: [{ indicator_id: 'a' }] },
]

test('countIndicatorRuleUsage tallies conditions per indicator', () => {
  const usage = countIndicatorRuleUsage(rules)
  assert.equal(usage.get('a'), 2)
  assert.equal(usage.get('b'), 1)
  assert.equal(usage.get('missing'), undefined)
})

test('requiresDetachConfirm flags indicators referenced by rules', () => {
  assert.equal(requiresDetachConfirm('a', rules), true)
  assert.equal(requiresDetachConfirm('b', rules), true)
  assert.equal(requiresDetachConfirm('c', rules), false)
})

test('findBrokenRuleIndicators returns set of referenced-but-detached ids', () => {
  const broken = findBrokenRuleIndicators(['a'], rules)
  assert.equal(broken.has('b'), true)
  assert.equal(broken.has('a'), false)
})
