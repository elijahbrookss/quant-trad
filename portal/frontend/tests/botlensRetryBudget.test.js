import test from 'node:test'
import assert from 'node:assert/strict'

import { consumeRetryBudget } from '../src/components/bots/botlensRetryBudget.js'

test('consumeRetryBudget blocks once attempts exceed the configured windowed limit', () => {
  const first = consumeRetryBudget([], 1000, { limit: 2, windowMs: 30000 })
  assert.equal(first.blocked, false)
  assert.equal(first.attemptCount, 1)

  const second = consumeRetryBudget(first.history, 2000, { limit: 2, windowMs: 30000 })
  assert.equal(second.blocked, false)
  assert.equal(second.attemptCount, 2)

  const third = consumeRetryBudget(second.history, 3000, { limit: 2, windowMs: 30000 })
  assert.equal(third.blocked, true)
  assert.equal(third.attemptCount, 3)
})

test('consumeRetryBudget drops attempts that fall outside the retry window', () => {
  const seeded = consumeRetryBudget([1000, 2000], 45000, { limit: 2, windowMs: 30000 })
  assert.deepEqual(seeded.history, [45000])
  assert.equal(seeded.blocked, false)
})
