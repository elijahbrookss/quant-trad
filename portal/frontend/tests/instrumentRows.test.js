import test from 'node:test'
import assert from 'node:assert/strict'

import { computeInstrumentRow } from '../src/components/strategy/utils/instrumentRows.js'

test('computeInstrumentRow marks missing metadata as missing status', () => {
  const instrumentMap = new Map()
  instrumentMap.set('ES', { symbol: 'ES' })
  const row = computeInstrumentRow({ symbol: 'ES', instrumentMap })
  assert.equal(row.status, 'missing')
  assert.equal(row.hasMetadata, false)
})

test('computeInstrumentRow marks errors when instrumentMessages present', () => {
  const instrumentMap = new Map()
  instrumentMap.set('ES', { symbol: 'ES', tick_size: 0.25, tick_value: 12.5, contract_size: 50 })
  const row = computeInstrumentRow({
    symbol: 'ES',
    instrumentMap,
    instrumentMessages: [{ symbol: 'ES', message: 'Bad data' }],
  })
  assert.equal(row.status, 'error')
})

test('computeInstrumentRow uses refreshStatus updatedAt for freshness', () => {
  const instrumentMap = new Map()
  instrumentMap.set('ES', { symbol: 'ES', tick_size: 0.25, tick_value: 12.5, contract_size: 50 })
  const updatedAt = '2024-01-01T00:00:00Z'
  const row = computeInstrumentRow({
    symbol: 'ES',
    instrumentMap,
    refreshStatus: { ES: { updatedAt } },
  })
  assert.ok(row.staleLabel.includes('Updated'))
  assert.equal(row.status, 'valid')
})
