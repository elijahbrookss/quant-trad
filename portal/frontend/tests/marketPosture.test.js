import test from 'node:test'
import assert from 'node:assert/strict'

import { buildMarketPostureRows } from '../src/features/market-structure/buildMarketPosture.js'

const NOW = Date.parse('2026-08-02T12:00:00Z')

test('market posture never infers BIP/BTC admission from configuration or recent collection', () => {
  const rows = buildMarketPostureRows({
    definitions: [{
      id: 'stream-1',
      enabled: true,
      production_admitted: false,
      provider_product_id: 'BIP-20DEC30-CDE',
      instrument_id: 'bip-future',
      config: { pair_id: 'bip_btc' },
    }],
    collectors: [{
      definition: {
        id: 'collector-1',
        enabled: true,
        provider: 'COINBASE',
        fact_type: 'derivatives.open_interest',
        instrument_id: 'bip-future',
        poll_interval_seconds: 60,
        next_scheduled_at: '2026-08-02T12:01:00Z',
        worker_health: { status: 'alive' },
      },
      attempts: [{ status: 'succeeded', started_at: '2026-08-02T11:59:00Z', finished_at: '2026-08-02T11:59:05Z' }],
    }],
    nowEpochMs: NOW,
  })

  assert.equal(rows[0].label, 'BIP / BTC')
  assert.equal(rows[0].collection.value, 'recent_success')
  assert.match(rows[0].collection.detail, /worker heartbeat current/)
  assert.equal(rows[0].admission.value, 'not_admitted')
  assert.equal(rows[0].coverage.value, 'unavailable')
})

test('market posture reports invalid coverage independently from archive availability', () => {
  const rows = buildMarketPostureRows({
    definitions: [{
      id: 'stream-1',
      enabled: true,
      production_admitted: false,
      provider_product_id: 'ETP-20DEC30-CDE',
      config: { pair_id: 'etp_eth' },
    }],
    statusByDefinition: {
      'stream-1': {
        available: true,
        value: {
          manifest_count: 2,
          archive_mapping_lag_records: 0,
          coverage_intervals: [{ status: 'invalid', archive_status: 'loss' }],
        },
      },
    },
  })
  assert.equal(rows[0].coverage.value, 'invalid')
  assert.equal(rows[0].archive.value, 'available')
})
