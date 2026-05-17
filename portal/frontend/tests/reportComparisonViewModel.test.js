import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildRunComparisonView,
  metricDeltaState,
  reportComparableForSelection,
} from '../src/components/reports/reportComparisonViewModel.js'

function comparisonPayload(overrides = {}) {
  return {
    contract_version: 'run_report_comparison_v1',
    left_run_id: 'left-run',
    right_run_id: 'right-run',
    comparison_status: 'ready',
    comparison_verdict: 'semantic_match_operational_drift',
    can_compare: true,
    blocked_reason: null,
    trust_comparison: {
      lifecycle_status_left: 'completed',
      lifecycle_status_right: 'completed',
      readiness_status_left: 'ready',
      readiness_status_right: 'ready',
      golden_status_left: 'certified',
      golden_status_right: 'certified',
      semantic_fingerprint_match: true,
      operational_fingerprint_match: false,
      data_snapshot_hash_match: true,
      runtime_ordering_status_left: 'gapless',
      runtime_ordering_status_right: 'gapless',
      wallet_trace_complete_left: true,
      wallet_trace_complete_right: true,
      candle_continuity_left: 'source_sparse',
      candle_continuity_right: 'source_sparse',
      observer_safety_left: 'safe',
      observer_safety_right: 'safe',
    },
    performance_delta: {
      net_pnl: { left: 10, right: 15, delta: 5, valid: true, unit: 'currency' },
      sharpe: { left: null, right: 1.2, delta: null, valid: false, invalid_reason: 'left:zero_return_stddev' },
    },
    behavior_delta: {
      decision_count_delta: 0,
      accepted_delta: 0,
      rejected_delta: 0,
      trade_lifecycle_equal: null,
      trade_lifecycle_source: 'golden_artifact_not_integrated',
      golden_artifact_status: 'not_integrated',
    },
    wallet_comparison: { wallet_trace_complete_left: true, wallet_trace_complete_right: true },
    golden_evidence: {
      available: true,
      status: 'available',
      artifact_path: 'logs/reports/golden-repeatability/pair/comparison_summary.json',
      verdict: 'PASS',
      semantic_fingerprint_match: true,
      operational_fingerprint_match: false,
      decision_count_left: 103,
      decision_count_right: 103,
      missing_decision_count: 0,
      extra_decision_count: 0,
      missing_decision_ids: [],
      extra_decision_ids: [],
      decision_diff_full_lists_available: true,
      verdict_change_count: 0,
      verdict_changes: [],
      verdict_changes_full_available: true,
      trade_lifecycle_equal: true,
      wallet_trace_missing_left: 0,
      wallet_trace_missing_right: 0,
      wallet_market_time_overtake_left: 0,
      wallet_market_time_overtake_right: 0,
      runtime_ordering_left: { status: 'ready', gap_count: 0 },
      runtime_ordering_right: { status: 'ready', gap_count: 0 },
    },
    symbol_deltas: [],
    coordinator_wait_delta: {},
    operational_drift: {
      operational_fingerprint_match: false,
      operational_drift_summary: 'operational_drift_only',
      statement: 'Operational drift is diagnostic-only here because semantic fingerprints match.',
    },
    first_divergence: {
      present: false,
      divergence_type: 'none',
      explanation: 'No semantic divergence detected by materialized report fingerprints.',
      source: 'report_comparison',
    },
    raw_refs: { cold_build_triggered: false },
    ...overrides,
  }
}

test('comparison view model renders semantic pass with operational drift', () => {
  const view = buildRunComparisonView(comparisonPayload())

  assert.equal(view.supported, true)
  assert.equal(view.canCompare, true)
  assert.equal(view.comparisonVerdict, 'semantic_match_operational_drift')
  assert.equal(view.trustRows.find((row) => row.key === 'semantic').status, 'match')
  assert.equal(view.trustRows.find((row) => row.key === 'operational').status, 'mismatch')
  assert.equal(view.operationalDrift.operational_drift_summary, 'operational_drift_only')
  assert.equal(view.goldenEvidence.available, true)
  assert.equal(view.goldenEvidence.verdict, 'PASS')
  assert.equal(view.goldenEvidence.decisionDiffFullListsAvailable, true)
  assert.equal(view.goldenEvidence.verdictChangesFullAvailable, true)
  assert.deepEqual(view.goldenEvidence.missingDecisionIds, [])
  assert.deepEqual(view.goldenEvidence.extraDecisionIds, [])
  assert.deepEqual(view.goldenEvidence.verdictChanges, [])
  assert.equal(view.goldenEvidence.tradeLifecycleEqual, true)
  assert.equal(view.goldenEvidence.walletMarketTimeOvertakeLeft, 0)
  assert.equal(view.firstDivergence.present, false)
})

test('comparison view model preserves blocked building state', () => {
  const view = buildRunComparisonView(comparisonPayload({
    comparison_status: 'blocked',
    comparison_verdict: 'blocked',
    can_compare: false,
    blocked_reason: 'right_report_building',
  }))

  assert.equal(view.canCompare, false)
  assert.equal(view.blockedReason, 'right_report_building')
})

test('metric delta state marks invalid metrics not comparable', () => {
  const state = metricDeltaState({ left: null, right: 1.2, valid: false, invalid_reason: 'left:zero_return_stddev' })

  assert.equal(state.valid, false)
  assert.equal(state.delta, null)
  assert.equal(state.invalidReason, 'left:zero_return_stddev')
})

test('comparison view model renders golden evidence unavailable state', () => {
  const view = buildRunComparisonView(comparisonPayload({
    golden_evidence: {
      available: false,
      status: 'not_available',
      first_divergence: {
        present: false,
        divergence_type: 'not_available',
        explanation: 'Golden evidence not available.',
        source: 'golden',
      },
    },
    first_divergence: {
      present: false,
      divergence_type: 'not_available',
      explanation: 'Golden evidence not available.',
      source: 'golden',
    },
  }))

  assert.equal(view.goldenEvidence.available, false)
  assert.equal(view.goldenEvidence.status, 'not_available')
  assert.equal(view.firstDivergence.divergenceType, 'not_available')
})

test('comparison view model preserves golden first divergence details', () => {
  const view = buildRunComparisonView(comparisonPayload({
    comparison_verdict: 'semantic_drift',
    first_divergence: {
      present: true,
      divergence_type: 'decision_divergence',
      symbol: 'BTC',
      timeframe: '1h',
      bar_time: '2026-01-01T00:00:00Z',
      decision_id: 'decision-1',
      field_path: 'decisions[0].status',
      left_value: 'accepted',
      right_value: 'rejected',
      explanation: 'Golden comparison first divergence in decisions[0].status.',
      source: 'golden',
    },
  }))

  assert.equal(view.firstDivergence.present, true)
  assert.equal(view.firstDivergence.source, 'golden')
  assert.equal(view.firstDivergence.decisionId, 'decision-1')
  assert.equal(view.firstDivergence.fieldPath, 'decisions[0].status')
})

test('report selection requires terminal ready materialized report state', () => {
  assert.equal(reportComparableForSelection({ lifecycleStatus: 'completed', reportStatus: 'ready', canViewReport: true }), true)
  assert.equal(reportComparableForSelection({ lifecycleStatus: 'running', reportStatus: 'ready', canViewReport: true }), false)
  assert.equal(reportComparableForSelection({ lifecycleStatus: 'completed', reportStatus: 'building', canViewReport: false }), false)
})
