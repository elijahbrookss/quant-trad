import test from 'node:test'
import assert from 'node:assert/strict'

import { buildTriggerDetail, buildTriggerRows } from '../src/components/strategy/utils/orderTriggers.js'

test('buildTriggerRows flattens typed preview trigger rows with rule names', () => {
  const instrumentResult = {
    machine: {
      signals: [
        {
          signal_id: 'signal-1',
          source_type: 'strategy_preview',
          source_id: 'preview-1',
          decision_id: 'row-1',
        },
      ],
      decision_artifacts: [
        {
          decision_id: 'row-1',
          evaluation_result: 'matched_selected',
          rule_id: 'r1',
          emitted_intent: 'enter_long',
          trigger: {
            event_key: 'balance_breakout_long',
            output_ref: 'market_profile.balance_breakout',
          },
          bar_time: '2024-01-01T00:00:00Z',
          bar_epoch: 1704067200,
        },
        {
          decision_id: 'row-2',
          evaluation_result: 'matched_selected',
          rule_id: 'r2',
          emitted_intent: 'enter_short',
          trigger: {
            event_key: 'balance_breakout_short',
            output_ref: 'market_profile.balance_breakout',
          },
          guard_results: [{ type: 'context_match', output_ref: 'market_regime', field: 'state', actual: 'trend' }],
          bar_time: '2024-01-02T00:00:00Z',
          bar_epoch: 1704153600,
        },
      ],
    },
    window: { symbol: 'ES', instrument_id: 'inst-1' },
  }
  const rules = [
    { id: 'r1', name: 'Long Entry' },
    { id: 'r2', name: 'Exit' },
  ]

  const rows = buildTriggerRows({ instrumentResult, rules, symbol: 'ES' })
  assert.equal(rows.length, 2)
  const buyRow = rows.find((r) => r.ruleId === 'r1')
  assert.equal(buyRow.ruleName, 'Long Entry')
  assert.equal(buyRow.triggerType, 'balance_breakout_long')
  assert.equal(buyRow.outputRef, 'balance_breakout')
  assert.equal(buyRow.indicatorRef, 'market_profile')
  assert.equal(buyRow.signalId, 'signal-1')
  assert.equal(buyRow.sourceId, 'preview-1')
  const sellRow = rows.find((r) => r.ruleId === 'r2')
  assert.equal(sellRow.ruleName, 'Exit')
  assert.equal(sellRow.triggerType, 'balance_breakout_short')
  assert.equal(sellRow.guardCount, 1)
  assert.equal(rows[0].ruleId, 'r2')
})

test('buildTriggerRows handles missing instrumentResult gracefully', () => {
  const rows = buildTriggerRows()
  assert.deepEqual(rows, [])
})

test('buildTriggerDetail extracts useful observed state and reference fields', () => {
  const [row] = buildTriggerRows({
    instrumentResult: {
      machine: {
        signals: [
          {
            signal_id: 'signal-1',
            source_type: 'strategy_preview',
            source_id: 'preview-1',
            decision_id: 'row-1',
          },
        ],
        decision_artifacts: [
          {
            decision_id: 'row-1',
            evaluation_result: 'matched_selected',
            rule_id: 'r1',
            rule_name: 'Long Entry',
            emitted_intent: 'enter_long',
            trigger: {
              event_key: 'balance_breakout_long',
              output_ref: 'market_profile.balance_breakout',
            },
            guard_results: [
              {
                type: 'context_match',
                output_ref: 'market_regime',
                field: 'state',
                expected: ['trend'],
                actual: 'trend',
                ready: true,
                matched: true,
              },
              {
                type: 'metric_match',
                output_ref: 'profile_stats',
                field: 'width',
                operator: '>=',
                expected: 10,
                actual: 12.5,
                ready: true,
                matched: true,
              },
            ],
            observed_outputs: {
              'market_profile.balance_breakout': {
                type: 'signal',
                ready: true,
                bar_time: '2024-01-01T00:00:00Z',
                event_count: 1,
                event_keys: ['balance_breakout_long'],
                events: [
                  {
                    key: 'balance_breakout_long',
                    direction: 'long',
                    known_at: '2024-01-01T00:00:00Z',
                  },
                ],
              },
              'market_regime': {
                type: 'context',
                ready: true,
                bar_time: '2024-01-01T00:00:00Z',
                fields: {
                  state: 'trend',
                  bias: 'long',
                  regime_meta: {
                    state_key: 'range',
                    fields: {
                      value_area: 'inside',
                      balance_width: 12.5,
                    },
                  },
                },
              },
              profile_stats: {
                type: 'metric',
                ready: true,
                bar_time: '2024-01-01T00:00:00Z',
                fields: {
                  width: 12.5,
                  poc: 100.25,
                },
              },
            },
            referenced_outputs: {
              'market_profile.balance_breakout': {
                type: 'signal',
                ready: true,
                bar_time: '2024-01-01T00:00:00Z',
                event_count: 1,
                event_keys: ['balance_breakout_long'],
                events: [
                  {
                    key: 'balance_breakout_long',
                    direction: 'long',
                    known_at: '2024-01-01T00:00:00Z',
                  },
                ],
              },
              profile_stats: {
                type: 'metric',
                ready: true,
                bar_time: '2024-01-01T00:00:00Z',
                fields: {
                  width: 12.5,
                },
              },
            },
            bar_time: '2024-01-01T00:00:00Z',
            bar_epoch: 1704067200,
          },
        ],
      },
      window: { symbol: 'ES', instrument_id: 'inst-1' },
    },
    rules: [{ id: 'r1', name: 'Long Entry' }],
    symbol: 'ES',
  })

  const detail = buildTriggerDetail(row)
  const indicatorLookup = new Map([
    ['market_profile', { type: 'market_profile' }],
    ['market_regime', { type: 'market_regime' }],
    ['profile_stats', { type: 'profile_stats' }],
  ])
  const labeledDetail = buildTriggerDetail(row, { indicatorLookup })

  assert.equal(detail.summary.ruleName, 'Long Entry')
  assert.equal(detail.summary.triggerDisplay, 'Balance Breakout Long')
  assert.deepEqual(detail.references.map((entry) => entry.label), ['Signal ID', 'Decision ID', 'Preview ID'])
  assert.deepEqual(
    labeledDetail.observedOutputs.map((entry) => entry.outputRef),
    ['market_regime', 'profile_stats'],
  )
  assert.deepEqual(
    labeledDetail.referencedOutputs.map((entry) => entry.outputRef),
    [
      'profile_stats',
    ],
  )
  assert.deepEqual(
    labeledDetail.observedOutputs.map((entry) => entry.label),
    ['market_regime', 'profile_stats'],
  )
  assert.deepEqual(
    labeledDetail.observedOutputs[0].fields.map((entry) => entry.label),
    [
      'state',
      'bias',
      'regime_meta',
    ],
  )
  assert.equal(labeledDetail.observedOutputs[0].fields[0].kind, 'scalar')
  assert.equal(labeledDetail.observedOutputs[0].fields[0].value, 'trend')
  assert.equal(labeledDetail.observedOutputs[0].fields[1].kind, 'scalar')
  assert.equal(labeledDetail.observedOutputs[0].fields[1].value, 'long')
  assert.equal(labeledDetail.observedOutputs[0].fields[2].kind, 'object')
  assert.equal(labeledDetail.observedOutputs[0].fields[2].summary, '2 fields')
  assert.deepEqual(
    labeledDetail.observedOutputs[0].fields[2].children.map((entry) => entry.label),
    ['state_key', 'fields'],
  )
  assert.equal(labeledDetail.observedOutputs[0].fields[2].children[0].kind, 'scalar')
  assert.equal(labeledDetail.observedOutputs[0].fields[2].children[0].value, 'range')
  assert.deepEqual(
    detail.guardChecks.map((entry) => entry.status),
    ['matched', 'matched'],
  )
})
