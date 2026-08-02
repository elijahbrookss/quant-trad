import test from 'node:test'
import assert from 'node:assert/strict'

import { deriveCollectorHealth } from '../src/features/collectors/collectorHealth.js'

const NOW = Date.parse('2026-08-02T12:00:00Z')

function definition(overrides = {}) {
  return {
    enabled: true,
    poll_interval_seconds: 60,
    next_scheduled_at: '2026-08-02T11:59:30Z',
    ...overrides,
  }
}

test('collector health is healthy only when enabled, has a recorded success, and is neither overdue nor stale', () => {
  const attempts = [
    { started_at: '2026-08-02T11:58:00Z', finished_at: '2026-08-02T11:58:05Z', status: 'succeeded' },
  ]
  const health = deriveCollectorHealth(definition(), attempts, NOW)

  assert.equal(health.status, 'healthy')
  assert.equal(health.schedulerEnabled, true)
  assert.equal(health.overdue, false)
  assert.equal(health.stale, false)
  assert.equal(health.processLivenessUnknown, true)
})

test('collector health is failed when the latest recorded attempt failed, even without an earlier success', () => {
  const attempts = [{ started_at: '2026-08-02T11:58:00Z', status: 'failed' }]
  const health = deriveCollectorHealth(definition(), attempts, NOW)

  assert.equal(health.status, 'failed')
  assert.equal(health.lastSuccessAt, null)
})

test('collector health is unknown, never healthy, when there are no attempts recorded at all', () => {
  const health = deriveCollectorHealth(definition(), [], NOW)

  assert.equal(health.status, 'unknown')
})

test('collector health is unknown, never healthy, when the definition has no next_scheduled_at', () => {
  const attempts = [
    { started_at: '2026-08-02T11:58:00Z', finished_at: '2026-08-02T11:58:05Z', status: 'succeeded' },
  ]
  const health = deriveCollectorHealth(definition({ next_scheduled_at: null }), attempts, NOW)

  assert.equal(health.status, 'unknown')
})

test('collector health is overdue when now is well past next_scheduled_at plus grace', () => {
  const attempts = [
    { started_at: '2026-08-02T10:00:00Z', finished_at: '2026-08-02T10:00:05Z', status: 'succeeded' },
  ]
  // next expected 30 minutes ago, poll interval 60s -> grace is only 2 minutes, so this is overdue.
  const health = deriveCollectorHealth(
    definition({ next_scheduled_at: '2026-08-02T11:30:00Z' }),
    attempts,
    NOW,
  )

  assert.equal(health.status, 'overdue')
})

test('collector health is stale when the last success is far older than the stale window, even if not overdue', () => {
  const attempts = [
    { started_at: '2026-08-02T09:00:00Z', finished_at: '2026-08-02T09:00:05Z', status: 'succeeded' },
  ]
  // next_scheduled_at still in the future relative to NOW, so not overdue -- but last
  // success is 3 hours old against a 60s interval (stale window = 3 minutes).
  const health = deriveCollectorHealth(
    definition({ next_scheduled_at: '2026-08-02T12:05:00Z' }),
    attempts,
    NOW,
  )

  assert.equal(health.status, 'stale')
})

test('collector health never reads healthy when the scheduler is disabled, even with a recent success', () => {
  const attempts = [
    { started_at: '2026-08-02T11:58:00Z', finished_at: '2026-08-02T11:58:05Z', status: 'succeeded' },
  ]
  const health = deriveCollectorHealth(definition({ enabled: false }), attempts, NOW)

  assert.notEqual(health.status, 'healthy')
})

test('collector health picks the most recent succeeded attempt as lastSuccessAt, ignoring later failures', () => {
  const attempts = [
    { started_at: '2026-08-02T11:59:00Z', status: 'failed' },
    { started_at: '2026-08-02T11:55:00Z', finished_at: '2026-08-02T11:55:05Z', status: 'succeeded' },
  ]
  const health = deriveCollectorHealth(definition(), attempts, NOW)

  assert.equal(health.lastSuccessAt, '2026-08-02T11:55:05Z')
  assert.equal(health.lastAttemptStatus, 'failed')
})
