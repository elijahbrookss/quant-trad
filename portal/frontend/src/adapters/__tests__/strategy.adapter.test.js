import test from 'node:test'
import assert from 'node:assert/strict'

import {
  STRATEGY_AUTHORING_DISABLED_MESSAGE,
  createStrategy,
  normalizeStrategyDetail,
  normalizeStrategySummary,
} from '../strategy.adapter.js'

test('normalizeStrategySummary maps strategy inventory rows', () => {
  const payload = {
    id: 'strat-1',
    name: 'Breakout',
    timeframe: '15m',
    datasource: 'BINANCE',
    exchange: 'futures',
    symbols: ['BTCUSDT'],
    instrument_count: 1,
    indicator_count: 2,
    rule_count: 3,
    variant_count: 4,
    readiness: { missing_indicator_count: 0 },
    atm_template_id: 'atm-1',
    atm_template: { name: 'Base ATM' },
    risk_config: { base_risk_per_trade: 100 },
  }

  const normalized = normalizeStrategySummary(payload)

  assert.equal(normalized.id, 'strat-1')
  assert.equal(normalized.name, 'Breakout')
  assert.deepEqual(normalized.symbols, ['BTCUSDT'])
  assert.equal(normalized.instrument_count, 1)
  assert.equal(normalized.indicator_count, 2)
  assert.equal(normalized.rule_count, 3)
  assert.equal(normalized.variant_count, 4)
  assert.deepEqual(normalized.strategy.atm_template, { name: 'Base ATM' })
  assert.deepEqual(normalized.strategy.risk_config, { base_risk_per_trade: 100 })
})

test('normalizeStrategyDetail composes split strategy read contracts', () => {
  const definition = {
    schema_version: 'strategy_definition.v1',
    strategy: {
      id: 'strat-2',
      name: 'Mean Reversion',
      timeframe: '1h',
      atm_template: {},
      risk_config: {},
    },
    read_context: {
      missing_indicators: ['ind-missing'],
      instrument_messages: ['tick metadata unavailable'],
    },
    counts: { instrument_count: 0, indicator_count: 0, rule_count: 0 },
  }
  const bindings = {
    schema_version: 'strategy_bindings.v1',
    bindings: {
      symbols: ['ETHUSDT'],
      instrument_slots: [{ symbol: 'ETHUSDT' }],
      instruments: [{ id: 'inst-1', symbol: 'ETHUSDT' }],
      indicator_ids: ['ind-1'],
      indicators: [{ id: 'ind-1', name: 'RSI' }],
    },
  }
  const rules = {
    schema_version: 'strategy_rules.v1',
    rules: [{ id: 'rule-1', name: 'Entry' }],
  }
  const variants = {
    schema_version: 'strategy_variants.v1',
    variants: [{ id: 'variant-1', name: 'Default', is_default: true }],
  }

  const normalized = normalizeStrategyDetail(definition, { bindings, rules, variants })

  assert.deepEqual(normalized.symbols, ['ETHUSDT'])
  assert.deepEqual(normalized.instrument_slots, [{ symbol: 'ETHUSDT' }])
  assert.deepEqual(normalized.indicator_ids, ['ind-1'])
  assert.deepEqual(normalized.rules, [{ id: 'rule-1', name: 'Entry' }])
  assert.deepEqual(normalized.missing_indicators, ['ind-missing'])
  assert.deepEqual(normalized.instrument_messages, ['tick metadata unavailable'])
  assert.deepEqual(normalized.variants, [{ id: 'variant-1', name: 'Default', is_default: true }])
  assert.equal(normalized.counts.variant_count, 1)
  assert.deepEqual(normalized.decision, { rules: [{ id: 'rule-1', name: 'Entry' }] })
})

test('strategy authoring adapter calls fail while frontend authoring is dormant', async () => {
  await assert.rejects(
    () => createStrategy({ name: 'Nope' }),
    (error) => {
      assert.equal(error.name, 'StrategyAuthoringDisabledError')
      assert.equal(error.code, 'strategy_authoring_disabled')
      assert.equal(error.message, STRATEGY_AUTHORING_DISABLED_MESSAGE)
      return true
    },
  )
})
