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

function collectorEntry({ state = 'HEALTHY', configuredState = 'enabled', active = false, minutesAgo = 1 } = {}) {
  const evidenceAt = new Date(NOW - minutesAgo * 60_000).toISOString()
  return {
    collector_id: 'col-1',
    collector_kind: 'scheduled_fact',
    provider: 'COINBASE',
    configured_state: configuredState,
    desired_state: active ? 'running' : 'stopped',
    actual_state: state,
    subjects: [{ instrument_id: 'btc-perp', symbol: 'BTC-PERP' }],
    fact_schemas: [{ fact_type: 'derivatives.open_interest', schema_version: 'derivatives.open_interest.v1' }],
    runtime: { active, restart_count: 0 },
    acquisition: { last_attempt_at: evidenceAt, last_accepted_fact_at: evidenceAt, freshness_seconds: 60 },
    worker: { alive: true, heartbeat_at: evidenceAt },
    throughput: { accepted_last_minute: 1 },
    error: { active: state === 'FAILED', message: state === 'FAILED' ? 'provider failed' : null },
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
    collectors: [collectorEntry({ state: 'FAILED', minutesAgo: 1.5 })],
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
    'collector:scheduled_fact:col-1',
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
      collectorEntry({ configuredState: 'disabled', state: 'DISABLED' }),
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

test('current operations contains active run and collector instances, not inactive registered intent', () => {
  const operations = buildCurrentOperations({
    runs: [
      run({ run_id: 'active-run', status: 'running' }),
      run({ run_id: 'completed-run', status: 'completed' }),
    ],
    collectors: [
      collectorEntry({ state: 'STARTING', active: true }),
      collectorEntry({ state: 'STOPPED', active: false }),
    ],
  })

  assert.deepEqual(new Set(operations.map((item) => item.kind)), new Set(['run', 'collector']))
  assert.equal(operations.some((item) => item.id === 'run:completed-run'), false)
  assert.equal(operations.filter((item) => item.kind === 'collector').length, 1)
})
