const UNKNOWN = 'unknown'

export const normalizeComparisonLabel = (value) => {
  if (value === true) return 'Match'
  if (value === false) return 'Mismatch'
  if (value === null || value === undefined || value === '') return 'Unknown'
  return String(value)
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

export const comparisonTone = (value) => {
  const normalized = String(value ?? '').toLowerCase()
  if (!normalized || ['unknown', 'not_available', 'not_computed', 'unavailable'].includes(normalized)) return 'neutral'
  if (
    ['ready', 'semantic_match', 'match', 'matched', 'pass', 'passed', 'certified', 'completed', 'true', 'none', 'clean', 'gapless', 'can_compare', 'comparable'].includes(normalized)
  ) {
    return 'good'
  }
  if (['ready_with_caveats', 'semantic_match_operational_drift', 'operational_drift_only', 'partial', 'warning', 'source_sparse'].includes(normalized)) {
    return 'warn'
  }
  if (['blocked', 'failed', 'semantic_drift', 'mismatch', 'false', 'run_not_terminal'].includes(normalized)) return 'bad'
  return 'neutral'
}

export const metricDeltaState = (delta = {}) => {
  const valid = delta?.valid === true
  return {
    left: delta?.left ?? null,
    right: delta?.right ?? null,
    delta: valid ? delta?.delta ?? null : null,
    valid,
    unit: delta?.unit || null,
    invalidReason: valid ? null : delta?.invalid_reason || 'not_comparable',
    caveats: Array.isArray(delta?.caveats) ? delta.caveats : [],
    raw: delta || {},
  }
}

export const PERFORMANCE_DELTA_DEFS = [
  { key: 'net_pnl', label: 'Net PnL', format: 'currency' },
  { key: 'total_return_pct', label: 'Return', format: 'percent' },
  { key: 'max_drawdown_pct', label: 'Max Drawdown', format: 'percent' },
  { key: 'sharpe', label: 'Sharpe', format: 'number' },
  { key: 'sortino', label: 'Sortino', format: 'number' },
  { key: 'calmar', label: 'Calmar', format: 'number' },
  { key: 'profit_factor', label: 'Profit Factor', format: 'number' },
  { key: 'expectancy', label: 'Expectancy', format: 'currency' },
  { key: 'win_rate', label: 'Win Rate', format: 'percent' },
  { key: 'trade_count', label: 'Trades', format: 'integer' },
  { key: 'fees', label: 'Fees', format: 'currency' },
  { key: 'exposure_pct', label: 'Exposure', format: 'percent' },
]

const booleanStatus = (value) => {
  if (value === true) return 'match'
  if (value === false) return 'mismatch'
  return UNKNOWN
}

export const buildRunComparisonView = (payload = {}) => {
  const trust = payload.trust_comparison || {}
  const performance = payload.performance_delta || {}
  const behavior = payload.behavior_delta || {}
  const wallet = payload.wallet_comparison || {}
  const operational = payload.operational_drift || {}
  const firstDivergence = payload.first_divergence || {}
  const goldenEvidence = payload.golden_evidence || {}

  const trustRows = [
    {
      key: 'lifecycle',
      label: 'Lifecycle',
      left: trust.lifecycle_status_left || UNKNOWN,
      right: trust.lifecycle_status_right || UNKNOWN,
      status: trust.lifecycle_status_left && trust.lifecycle_status_right ? booleanStatus(trust.lifecycle_status_left === trust.lifecycle_status_right) : UNKNOWN,
    },
    {
      key: 'readiness',
      label: 'Readiness',
      left: trust.readiness_status_left || UNKNOWN,
      right: trust.readiness_status_right || UNKNOWN,
      status: trust.readiness_status_left && trust.readiness_status_right ? booleanStatus(trust.readiness_status_left === trust.readiness_status_right) : UNKNOWN,
    },
    {
      key: 'golden',
      label: 'Golden',
      left: trust.golden_status_left || UNKNOWN,
      right: trust.golden_status_right || UNKNOWN,
      status: trust.golden_status_left && trust.golden_status_right ? booleanStatus(trust.golden_status_left === trust.golden_status_right) : UNKNOWN,
    },
    { key: 'semantic', label: 'Semantic Fingerprint', left: booleanStatus(trust.semantic_fingerprint_match), right: '', status: booleanStatus(trust.semantic_fingerprint_match) },
    { key: 'operational', label: 'Operational Fingerprint', left: booleanStatus(trust.operational_fingerprint_match), right: '', status: booleanStatus(trust.operational_fingerprint_match) },
    { key: 'data', label: 'Data Snapshot', left: booleanStatus(trust.data_snapshot_hash_match), right: '', status: booleanStatus(trust.data_snapshot_hash_match) },
    {
      key: 'runtime',
      label: 'Runtime Ordering',
      left: trust.runtime_ordering_status_left || UNKNOWN,
      right: trust.runtime_ordering_status_right || UNKNOWN,
      status: trust.runtime_ordering_status_left && trust.runtime_ordering_status_right ? booleanStatus(trust.runtime_ordering_status_left === trust.runtime_ordering_status_right) : UNKNOWN,
    },
    {
      key: 'wallet',
      label: 'Wallet Traces',
      left: trust.wallet_trace_complete_left,
      right: trust.wallet_trace_complete_right,
      status:
        trust.wallet_trace_complete_left === undefined || trust.wallet_trace_complete_right === undefined
          ? UNKNOWN
          : trust.wallet_trace_complete_left === true && trust.wallet_trace_complete_right === true
            ? 'match'
            : 'mismatch',
    },
    {
      key: 'candles',
      label: 'Candle Continuity',
      left: trust.candle_continuity_left || UNKNOWN,
      right: trust.candle_continuity_right || UNKNOWN,
      status: trust.candle_continuity_left && trust.candle_continuity_right ? booleanStatus(trust.candle_continuity_left === trust.candle_continuity_right) : UNKNOWN,
    },
    {
      key: 'observer',
      label: 'Observer Safety',
      left: trust.observer_safety_left || UNKNOWN,
      right: trust.observer_safety_right || UNKNOWN,
      status: trust.observer_safety_left && trust.observer_safety_right ? booleanStatus(trust.observer_safety_left === trust.observer_safety_right) : UNKNOWN,
    },
  ]

  return {
    contractVersion: payload.contract_version,
    supported: payload.contract_version === 'run_report_comparison_v1',
    leftRunId: payload.left_run_id || '',
    rightRunId: payload.right_run_id || '',
    comparisonStatus: payload.comparison_status || UNKNOWN,
    comparisonVerdict: payload.comparison_verdict || UNKNOWN,
    canCompare: payload.can_compare === true,
    blockedReason: payload.blocked_reason || null,
    trustRows,
    performanceMetrics: PERFORMANCE_DELTA_DEFS.map((definition) => ({
      ...definition,
      state: metricDeltaState(performance[definition.key]),
    })),
    behavior: {
      decisionCountDelta: behavior.decision_count_delta ?? null,
      acceptedDelta: behavior.accepted_delta ?? null,
      rejectedDelta: behavior.rejected_delta ?? null,
      rejectionReasonDeltas: behavior.rejection_reason_deltas || {},
      actionDistributionDeltas: behavior.action_distribution_deltas || {},
      entryCountDelta: behavior.entry_count_delta ?? null,
      exitCountDelta: behavior.exit_count_delta ?? null,
      tradeLifecycleEqual: behavior.trade_lifecycle_equal ?? null,
      tradeLifecycleSource: behavior.trade_lifecycle_source || 'not_available',
      missingDecisionIds: behavior.missing_decision_ids || [],
      extraDecisionIds: behavior.extra_decision_ids || [],
      verdictChanges: behavior.verdict_changes ?? null,
      goldenArtifactStatus: behavior.golden_artifact_status || 'not_integrated',
    },
    wallet,
    goldenEvidence: {
      available: goldenEvidence.available === true,
      status: goldenEvidence.status || 'not_available',
      artifactPath: goldenEvidence.artifact_path || null,
      generatedAt: goldenEvidence.generated_at || null,
      verdict: goldenEvidence.verdict || 'not_available',
      failReasons: goldenEvidence.fail_reasons || [],
      semanticFingerprintMatch: goldenEvidence.semantic_fingerprint_match ?? null,
      operationalFingerprintMatch: goldenEvidence.operational_fingerprint_match ?? null,
      dataSnapshotHashMatch: goldenEvidence.data_snapshot_hash_match ?? null,
      materialConfigHashMatch: goldenEvidence.material_config_hash_match ?? null,
      strategyHashMatch: goldenEvidence.strategy_hash_match ?? null,
      decisionCountLeft: goldenEvidence.decision_count_left ?? null,
      decisionCountRight: goldenEvidence.decision_count_right ?? null,
      missingDecisionCount: goldenEvidence.missing_decision_count ?? null,
      extraDecisionCount: goldenEvidence.extra_decision_count ?? null,
      missingDecisionIds: goldenEvidence.missing_decision_ids || [],
      extraDecisionIds: goldenEvidence.extra_decision_ids || [],
      decisionDiffFullListsAvailable: goldenEvidence.decision_diff_full_lists_available === true,
      verdictChangeCount: goldenEvidence.verdict_change_count ?? null,
      verdictChanges: goldenEvidence.verdict_changes || [],
      verdictChangesFullAvailable: goldenEvidence.verdict_changes_full_available === true,
      tradeLifecycleEqual: goldenEvidence.trade_lifecycle_equal ?? null,
      tradeCountLeft: goldenEvidence.trade_count_left ?? null,
      tradeCountRight: goldenEvidence.trade_count_right ?? null,
      walletTraceMissingLeft: goldenEvidence.wallet_trace_missing_left ?? null,
      walletTraceMissingRight: goldenEvidence.wallet_trace_missing_right ?? null,
      walletMarketTimeOvertakeLeft: goldenEvidence.wallet_market_time_overtake_left ?? null,
      walletMarketTimeOvertakeRight: goldenEvidence.wallet_market_time_overtake_right ?? null,
      entryDecisionOrderTimeoutLeft: goldenEvidence.entry_decision_order_timeout_left ?? null,
      entryDecisionOrderTimeoutRight: goldenEvidence.entry_decision_order_timeout_right ?? null,
      runtimeOrderingLeft: goldenEvidence.runtime_ordering_left || {},
      runtimeOrderingRight: goldenEvidence.runtime_ordering_right || {},
    },
    symbolDeltas: Array.isArray(payload.symbol_deltas) ? payload.symbol_deltas : [],
    coordinatorWaitDelta: payload.coordinator_wait_delta || {},
    operationalDrift: operational,
    firstDivergence: {
      present: firstDivergence.present === true,
      divergenceType: firstDivergence.divergence_type || 'not_computed',
      explanation: firstDivergence.explanation || null,
      source: firstDivergence.source || 'not_computed',
      fieldPath: firstDivergence.field_path || null,
      leftValue: firstDivergence.left_value ?? null,
      rightValue: firstDivergence.right_value ?? null,
      symbol: firstDivergence.symbol || null,
      timeframe: firstDivergence.timeframe || null,
      barTime: firstDivergence.bar_time || null,
      decisionId: firstDivergence.decision_id || null,
      tradeId: firstDivergence.trade_id || null,
    },
    rawRefs: payload.raw_refs || {},
    raw: payload,
  }
}

export const reportComparableForSelection = (report = {}) => {
  const lifecycle = String(report.lifecycleStatus || '').toLowerCase()
  const terminal = ['completed', 'failed', 'canceled', 'cancelled', 'stopped'].includes(lifecycle)
  const reportReady = String(report.reportStatus || '').toLowerCase() === 'ready'
  return terminal && reportReady && report.canViewReport === true
}
