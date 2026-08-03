import test from 'node:test'
import assert from 'node:assert/strict'

import {
  ATTENTION_CONTRACT,
  buildCurrentOperations,
  rankAttentionItems,
  resolveGreeting,
} from '../src/features/overview/buildOverviewViewModel.js'

const NOW = Date.parse('2026-08-02T15:00:00Z')

function run(overrides = {}) {
  return {
    run_id: 'run-1',
    bot_id: 'bot-1',
    run_type: 'paper',
    status: 'running',
    started_at: new Date(NOW - 60_000).toISOString(),
    definition: { id: 'bot-1', name: 'Breakout paper' },
    ...overrides,
  }
}

function collectorEntry({ status = 'succeeded', enabled = true, minutesAgo = 1, nextScheduledAt } = {}) {
  const attemptAt = new Date(NOW - minutesAgo * 60_000).toISOString()
  return {
    definition: {
      id: 'col-1',
      provider: 'COINBASE',
      fact_type: 'derivatives.open_interest',
      enabled,
      poll_interval_seconds: 60,
      next_scheduled_at: nextScheduledAt || new Date(NOW + 60_000).toISOString(),
      worker_health: { status: 'alive' },
    },
    attempts: [{ id: 'a1', status, started_at: attemptAt, finished_at: status === 'running' ? null : attemptAt }],
    attemptsAvailable: true,
  }
}

test('resolveGreeting stays role-based and uses only morning, afternoon, or evening', () => {
  assert.equal(resolveGreeting(new Date('2026-08-02T08:00:00').getTime()), 'Good morning')
  assert.equal(resolveGreeting(new Date('2026-08-02T14:00:00').getTime()), 'Good afternoon')
  assert.equal(resolveGreeting(new Date('2026-08-02T20:00:00').getTime()), 'Good evening')
  assert.equal(resolveGreeting(new Date('2026-08-02T02:00:00').getTime()), 'Good morning')
})

test('attention is severity then evidence recency across run, collector, research, and market sources', () => {
  const items = rankAttentionItems({
    runs: [
      run({ run_id: 'failed-old', status: 'failed', ended_at: new Date(NOW - 120_000).toISOString() }),
      run({ run_id: 'degraded', status: 'degraded', started_at: new Date(NOW - 30_000).toISOString() }),
    ],
    collectors: [{
      ...collectorEntry({ status: 'failed' }),
      attempts: [{ id: 'a1', status: 'failed', started_at: new Date(NOW - 90_000).toISOString() }],
    }],
    researchItems: [{
      id: 'check-1',
      kind: 'research_check',
      status: 'blocked',
      title: 'Coverage check',
      created_at: new Date(NOW - 60_000).toISOString(),
    }],
    postureRows: [{
      id: 'bip_btc',
      label: 'BIP / BTC',
      coverage: { value: 'invalid', label: 'Invalid coverage' },
      book: { value: 'open_valid', label: 'Book valid' },
      unavailableStatusCount: 0,
      latestEvidenceAt: new Date(NOW - 45_000).toISOString(),
    }],
    nowEpochMs: NOW,
  })

  assert.deepEqual(items.map((item) => item.severity), ['critical', 'critical', 'critical', 'critical', 'warning'])
  assert.deepEqual(items.slice(0, 4).map((item) => item.id), [
    'market:bip_btc',
    'research:check-1',
    'collector:col-1',
    'run:failed-old',
  ])
})

test('attention excludes healthy evidence, disabled schedules, and terminal failures outside the lookback', () => {
  const old = new Date(NOW - (ATTENTION_CONTRACT.lookbackHours + 1) * 3_600_000).toISOString()
  const items = rankAttentionItems({
    runs: [
      run({ run_id: 'running', status: 'running' }),
      run({ run_id: 'old-failure', status: 'failed', ended_at: old }),
    ],
    collectors: [
      collectorEntry(),
      collectorEntry({ enabled: false, status: 'failed' }),
    ],
    nowEpochMs: NOW,
  })
  assert.deepEqual(items, [])
})

test('attention deduplicates repeated canonical evidence identities', () => {
  const check = {
    id: 'check-1',
    kind: 'research_check',
    status: 'blocked',
    title: 'Blocked check',
    created_at: new Date(NOW - 60_000).toISOString(),
  }
  const items = rankAttentionItems({ researchItems: [check, check], nowEpochMs: NOW })
  assert.equal(items.length, 1)
  assert.equal(items[0].id, 'research:check-1')
})

test('current operations contains run instances, in-flight attempts, and leased sessions—not healthy schedules', () => {
  const operations = buildCurrentOperations({
    runs: [
      run({ run_id: 'active-run', status: 'running' }),
      run({ run_id: 'completed-run', status: 'completed' }),
    ],
    collectors: [
      collectorEntry({ status: 'running' }),
      collectorEntry({ status: 'succeeded' }),
    ],
    streamRows: [
      { id: 'def:session', leaseCurrent: true, productId: 'BIP-20DEC30-CDE', channels: ['market_trades'], eventLabel: 'Connected', eventType: 'connected', occurredAt: new Date(NOW - 20_000).toISOString() },
      { id: 'def:old', leaseCurrent: false, productId: 'ETH-USD', channels: ['level2'], eventLabel: 'Disconnected', eventType: 'disconnected' },
    ],
  })

  assert.deepEqual(new Set(operations.map((item) => item.kind)), new Set(['run', 'collector attempt', 'stream session']))
  assert.equal(operations.some((item) => item.id === 'run:completed-run'), false)
  assert.equal(operations.filter((item) => item.kind === 'collector attempt').length, 1)
})
