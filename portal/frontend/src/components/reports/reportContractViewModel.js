export const readinessStatus = (readiness = {}) => {
  const status = readiness.results_status || readiness.dataset_status
  if (status === 'ready') return 'ready'
  if (status === 'partial') return 'partial'
  if (status === 'failed') return 'failed'
  if (readiness.results_ready) return 'ready'
  if (readiness.dataset_ready) return 'partial'
  return 'unavailable'
}

export const comparisonStatus = (readiness = {}) => {
  if (readiness.comparison_status === 'ready') return 'eligible'
  if (readiness.comparison_status === 'ready_with_caveats') return 'caution'
  return readiness.safe_to_compare ? 'eligible' : 'blocked'
}

export const durationSecondsFromWindow = (window = {}) => {
  const start = window?.start
  const end = window?.end
  if (!start || !end) return null
  const startMs = new Date(start).getTime()
  const endMs = new Date(end).getTime()
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return null
  return Math.round((endMs - startMs) / 1000)
}

export const metricValue = (metric = {}) => {
  if (!metric || metric.valid !== true) return null
  if (metric.value === null || metric.value === undefined || metric.value === '') return null
  const numeric = Number(metric.value)
  return Number.isFinite(numeric) ? numeric : metric.value
}

export const metricDisplayState = (metric = {}) => {
  const hasMetric = metric && typeof metric === 'object'
  const valid = hasMetric && metric.valid === true && metric.value !== null && metric.value !== undefined && metric.value !== ''
  return {
    value: valid ? metric.value : null,
    valid,
    unit: hasMetric ? metric.unit || null : null,
    method: hasMetric ? metric.method || null : null,
    source: hasMetric ? metric.source || null : null,
    sampleCount: hasMetric ? metric.sample_count ?? null : null,
    minimumSampleCount: hasMetric ? metric.minimum_sample_count ?? null : null,
    invalidReason: hasMetric
      ? metric.invalid_reason || metric.caveats?.[0] || (valid ? null : 'not_available')
      : 'not_available',
    caveats: hasMetric && Array.isArray(metric.caveats) ? metric.caveats : [],
    raw: hasMetric ? metric : null,
  }
}

export const runReportView = (payload = {}) => {
  const identity = payload.identity || {}
  const trust = payload.trust || {}
  const performance = payload.performance || {}
  const behavior = payload.behavior || {}
  const wallet = payload.wallet || {}
  const coordinatorWaits = payload.coordinator_waits || {}
  const operationalDiagnostics = payload.operational_diagnostics || {}

  return {
    contractVersion: payload.contract_version,
    schemaVersion: payload.schema_version,
    supported: payload.contract_version === 'run_report_v2',
    runId: payload.run_id || identity.run_id,
    identity,
    trust,
    performance,
    behavior,
    wallet,
    symbolBreakdown: Array.isArray(payload.symbol_breakdown) ? payload.symbol_breakdown : [],
    coordinatorWaits,
    operationalDiagnostics,
    sections: payload.sections || {},
    rawRefs: payload.raw_refs || {},
    raw: payload,
  }
}

export const runReportListItemView = (payload = {}, fallback = {}) => {
  const view = runReportView(payload)
  const identity = view.identity || {}
  const trust = view.trust || {}
  const performance = view.performance || {}
  const diagnostics = view.operationalDiagnostics || {}
  const wallClockWindow = identity.wall_clock_window || fallback.wallClockWindow || {}
  const simulatedWindow = identity.simulated_window || fallback.simulatedWindow || {}
  const timeframe = identity.timeframe || identity.timeframes?.[0] || fallback.timeframe

  return {
    ...fallback,
    runId: view.runId || fallback.runId,
    botId: identity.bot_id || fallback.botId,
    botName: identity.bot_name || fallback.botName || identity.bot_id || 'Bot',
    strategyId: identity.strategy_id || fallback.strategyId,
    strategyName: identity.strategy_name || fallback.strategyName || identity.strategy_id || 'Strategy',
    runType: identity.run_type || fallback.runType,
    symbols: identity.symbols || fallback.symbols || [],
    timeframe,
    timeframes: identity.timeframes || fallback.timeframes || (timeframe ? [timeframe] : []),
    executionMode: identity.execution_mode || fallback.executionMode,
    simulatedWindow,
    wallClockWindow,
    startedAt: wallClockWindow.start || fallback.startedAt,
    endedAt: wallClockWindow.end || fallback.endedAt,
    completedAt: wallClockWindow.end || fallback.completedAt,
    durationSeconds: durationSecondsFromWindow(wallClockWindow) ?? fallback.durationSeconds ?? null,
    lifecycleStatus: trust.lifecycle_status || fallback.lifecycleStatus || 'unknown',
    researchStatus: trust.research_status || fallback.researchStatus || 'unknown',
    readinessStatus: trust.readiness_status || fallback.readinessStatus || fallback.reportStatus || 'unknown',
    goldenStatus: trust.golden_status || fallback.goldenStatus || trust.golden_candidate_status || 'not_available',
    semanticFingerprint: trust.semantic_fingerprint || fallback.semanticFingerprint || null,
    operationalFingerprint: trust.operational_fingerprint || diagnostics.operational_fingerprint || fallback.operationalFingerprint || null,
    operationalDriftStatus: diagnostics.operational_drift_status || fallback.operationalDriftStatus || 'not_computed',
    runtimeOrderingStatus: trust.runtime_ordering_status || fallback.runtimeOrderingStatus || 'unknown',
    walletTraceComplete: trust.wallet_trace_complete ?? fallback.walletTraceComplete ?? null,
    candleContinuityStatus: trust.candle_continuity_status || fallback.candleContinuityStatus || 'unknown',
    netPnl: metricValue(performance.net_pnl) ?? fallback.netPnl ?? null,
    totalReturn: metricValue(performance.total_return_pct) ?? fallback.totalReturn ?? null,
    maxDrawdownPct: metricValue(performance.max_drawdown_pct) ?? fallback.maxDrawdownPct ?? null,
    sharpe: metricValue(performance.sharpe) ?? fallback.sharpe ?? null,
    sharpeMetric: performance.sharpe || fallback.sharpeMetric || null,
    trades: metricValue(performance.trade_count) ?? fallback.trades ?? 0,
    reportStatus: trust.readiness_status || fallback.reportStatus || 'unknown',
    comparisonStatus: fallback.comparisonStatus || 'not_implemented',
    reportV2: view,
  }
}

export const reportListItemView = (item = {}) => {
  const summary = item.summary || {}
  const portfolioMetrics = item.portfolio_metrics || {}
  const readiness = item.readiness || {}
  const reportMaterialization = item.report_materialization || {}
  const simulatedWindow = item.simulated_window || item.date_range || {}
  const wallClockWindow = item.wall_clock_window || {}
  const durationSeconds = item.duration_seconds ?? durationSecondsFromWindow(wallClockWindow)
  return {
    runId: item.run_id,
    botId: item.bot_id,
    botName: item.bot_name || 'Bot',
    strategyId: item.strategy_id,
    strategyName: item.strategy_name || 'Strategy',
    symbols: item.symbols || [],
    timeframe: item.timeframe,
    executionMode: item.execution_mode,
    simulatedWindow,
    wallClockWindow,
    startedAt: wallClockWindow.start,
    endedAt: wallClockWindow.end,
    completedAt: item.completed_at || wallClockWindow.end,
    durationSeconds,
    lifecycleStatus: item.status || item.lifecycle_status || 'unknown',
    readinessStatus: readiness.results_status || readiness.dataset_status || readinessStatus(readiness),
    researchStatus: readiness.research_status || readiness.results_status || readinessStatus(readiness),
    goldenStatus: readiness.golden_candidate_status || 'not_available',
    semanticFingerprint: readiness.semantic_fingerprint || readiness.material_fingerprint || null,
    operationalFingerprint: readiness.operational_fingerprint || null,
    operationalDriftStatus: readiness.operational_drift_status || 'not_computed',
    netPnl: summary.net_pnl,
    totalReturn: summary.total_return ?? summary.return_pct,
    maxDrawdownPct: summary.max_drawdown_pct,
    sharpe: summary.sharpe ?? portfolioMetrics.sharpe,
    trades: summary.total_trades ?? summary.closed_trades ?? 0,
    readiness,
    reportMaterialization,
    reportStatus: reportMaterialization.status || readinessStatus(readiness),
    canViewReport: reportMaterialization.can_view === true,
    comparisonStatus: comparisonStatus(readiness),
    dataQualityStatus: readiness.data_quality_status,
    executionQualityStatus: readiness.execution_quality_status,
    caveats: readiness.caveats || [],
    degradedSections: readiness.degraded_sections || [],
    unavailableSections: readiness.unavailable_sections || [],
  }
}

export const reportSummaryView = (payload = {}) => {
  const metadata = payload.metadata || {}
  const summary = payload.summary || {}
  const portfolioMetrics = payload.portfolio_metrics || summary.portfolio_metrics || {}
  const readiness = payload.readiness || {}
  const sections = payload.sections || {}
  return {
    runId: payload.run_id || metadata.run_id,
    metadata,
    summary,
    readiness,
    sections,
    portfolioMetrics,
    reportStatus: readinessStatus(readiness),
    comparisonStatus: comparisonStatus(readiness),
    dataQualityStatus: readiness.data_quality_status,
    executionQualityStatus: readiness.execution_quality_status,
    caveats: readiness.caveats || [],
    degradedSections: readiness.degraded_sections || [],
    unavailableSections: readiness.unavailable_sections || [],
    symbols: metadata.symbols || [],
    instrumentIds: metadata.instrument_ids || [],
    timeframes: metadata.timeframes || [],
    timeframe: metadata.timeframe,
    executionMode: metadata.execution_mode,
    netPnl: summary.net_pnl,
    grossPnl: summary.gross_pnl,
    fees: summary.fees,
    returnPct: summary.return_pct,
    equityStart: summary.equity_start,
    equityEnd: summary.equity_end,
    maxDrawdownPct: summary.max_drawdown_pct,
    sharpe: portfolioMetrics.sharpe ?? summary.sharpe,
    sortino: portfolioMetrics.sortino ?? summary.sortino,
    calmar: portfolioMetrics.calmar ?? summary.calmar,
    annualizedVolatility: portfolioMetrics.annualized_volatility ?? summary.annualized_volatility,
    exposurePct: portfolioMetrics.exposure_pct ?? summary.exposure_pct,
    winRate: summary.win_rate,
    expectancy: summary.expectancy,
    profitFactor: summary.profit_factor,
    trades: summary.closed_trades ?? summary.trades ?? 0,
    decisions: summary.total_decisions ?? 0,
    rejectedDecisions: summary.rejected_decisions ?? 0,
  }
}

export const severityTone = (severity) => {
  const normalized = String(severity || '').toLowerCase()
  if (normalized === 'critical') return 'danger'
  if (normalized === 'warning') return 'warning'
  return 'info'
}

export const sectionByName = (sections = {}) => {
  const map = new Map()
  ;(sections.items || []).forEach((section) => {
    if (section?.name) map.set(section.name, section)
  })
  return map
}
