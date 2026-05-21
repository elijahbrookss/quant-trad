import test from 'node:test'
import assert from 'node:assert/strict'

import { normalizeStrategySummary, normalizeStrategyDetail } from '../strategy.adapter.js'

test('normalizeStrategySummary flattens nested strategy read summary', () => {
  const payload = {
    strategy: {
      id: 'strat-1',
      name: 'Breakout',
      timeframe: '15m',
      datasource: 'BINANCE',
      exchange: 'futures',
      atm_template_id: 'atm-1',
      atm_template: { name: 'Base ATM' },
      risk_config: { base_risk_per_trade: 100 },
    },
    bindings: {
      instrument_slots: [{ symbol: 'BTCUSDT', enabled: true }],
      instruments: [{ id: 'inst-1', symbol: 'BTCUSDT' }],
      indicator_ids: ['ind-1'],
      indicators: [{ id: 'ind-1', name: 'RSI' }],
    },
  }

  const normalized = normalizeStrategySummary(payload)

  assert.equal(normalized.id, 'strat-1')
  assert.equal(normalized.name, 'Breakout')
  assert.equal(normalized.atm_template_id, 'atm-1')
  assert.deepEqual(normalized.atm_template, { name: 'Base ATM' })
  assert.deepEqual(normalized.instrument_slots, [{ symbol: 'BTCUSDT', enabled: true }])
  assert.deepEqual(normalized.indicator_ids, ['ind-1'])
  assert.deepEqual(normalized.strategy.atm_template, { name: 'Base ATM' })
  assert.deepEqual(normalized.strategy.risk_config, { base_risk_per_trade: 100 })
  assert.deepEqual(normalized.bindings.instruments, [{ id: 'inst-1', symbol: 'BTCUSDT' }])
})

test('normalizeStrategyDetail retains detail-only sections', () => {
  const payload = {
    strategy: {
      id: 'strat-2',
      name: 'Mean Reversion',
      timeframe: '1h',
      atm_template: {},
      risk_config: {},
    },
    bindings: {
      instrument_slots: [],
      instruments: [],
      indicator_ids: [],
      indicators: [],
    },
    decision: {
      rules: [{ id: 'rule-1', name: 'Entry' }],
    },
    read_context: {
      missing_indicators: ['ind-missing'],
      instrument_messages: ['tick metadata unavailable'],
    },
    variants: [{ id: 'variant-1', name: 'Default', is_default: true }],
  }

  const normalized = normalizeStrategyDetail(payload)

  assert.deepEqual(normalized.rules, [{ id: 'rule-1', name: 'Entry' }])
  assert.deepEqual(normalized.missing_indicators, ['ind-missing'])
  assert.deepEqual(normalized.instrument_messages, ['tick metadata unavailable'])
  assert.deepEqual(normalized.variants, [{ id: 'variant-1', name: 'Default', is_default: true }])
  assert.deepEqual(normalized.decision, payload.decision)
  assert.deepEqual(normalized.read_context, payload.read_context)
})
