import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { buildCollectorCardViewModel } from '../src/features/collectors/buildCollectorCardViewModel.js'

const here = path.dirname(fileURLToPath(import.meta.url))
const srcRoot = path.join(here, '..', 'src')
const source = (relative) => readFileSync(path.join(srcRoot, relative), 'utf8')

function collector(overrides = {}) {
  return {
    collector_id: 'collector-1',
    collector_kind: 'continuous_stream',
    provider: 'COINBASE',
    actual_state: 'DEGRADED',
    operational_state: 'RUNNING',
    health_status: 'DELAYED',
    needs_attention: true,
    desired_state: 'running',
    configured_state: 'enabled',
    subjects: [{ provider_product_id: 'BTC-USD' }],
    fact_schemas: [{ fact_type: 'market.trade', schema_version: 'market.trade.v1' }],
    worker: { alive: false, heartbeat_at: '2026-08-10T12:00:00Z' },
    runtime: { active: true, restart_count: 2 },
    acquisition: { last_accepted_fact_at: '2026-08-10T11:59:00Z', freshness_seconds: 60 },
    throughput: { accepted_last_minute: 42 },
    ...overrides,
  }
}

test('collector view model keeps backend operational state and health separate', () => {
  const vm = buildCollectorCardViewModel(collector())
  assert.equal(vm.state, 'RUNNING')
  assert.equal(vm.health, 'DELAYED')
  assert.equal(vm.needsAttention, true)
  assert.equal(vm.route, '/operations/market/continuous_stream/collector-1')
  assert.equal(vm.throughputLabel, '42/min')
})

test('missing running freshness is unknown rather than zero seconds', () => {
  const vm = buildCollectorCardViewModel(collector({
    acquisition: { last_accepted_fact_at: null, freshness_seconds: null },
  }))
  assert.equal(vm.freshnessLabel, 'Freshness unknown')
})

test('collector frontend uses one canonical operational adapter and no legacy health model', () => {
  const adapter = source(path.join('adapters', 'marketData.adapter.js'))
  const feed = source(path.join('features', 'collectors', 'useCollectorsFeed.js'))
  const lens = source(path.join('v2', 'rooms', 'CollectorLensRoom.jsx'))
  assert.match(adapter, /operations\/collector-providers\/snapshot/)
  assert.match(adapter, /operations\/collector-providers\/stream/)
  assert.match(adapter, /operations\/collector-search/)
  assert.match(feed, /One lightweight provider-level stream/)
  assert.match(lens, /fetchCollectorOperationsDetail/)
  assert.match(lens, /executeCollectorAction/)
  assert.doesNotMatch(feed + lens, /deriveCollectorHealth|worker_health|next_scheduled_at/)
})

test('collector action dialog requires an operator reason and sends the canonical confirmation identity', () => {
  const content = source(path.join('features', 'collectors', 'CollectorLensContent.jsx'))
  const room = source(path.join('v2', 'rooms', 'CollectorLensRoom.jsx'))
  assert.match(content, /Operator reason/)
  assert.match(content, /health_probe: \{ label: 'Probe'/)
  assert.match(content, /disabled=\{busy \|\| !reason\.trim\(\)\}/)
  assert.match(room, /confirmation: `\$\{collectorKind\}:\$\{collectorId\}:\$\{action\}`/)
})
