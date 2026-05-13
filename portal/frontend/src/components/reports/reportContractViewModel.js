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

export const reportListItemView = (item = {}) => {
  const summary = item.summary || {}
  const portfolioMetrics = item.portfolio_metrics || {}
  const readiness = item.readiness || {}
  const simulatedWindow = item.simulated_window || item.date_range || {}
  const wallClockWindow = item.wall_clock_window || {}
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
    completedAt: item.completed_at || wallClockWindow.end,
    netPnl: summary.net_pnl,
    totalReturn: summary.total_return ?? summary.return_pct,
    maxDrawdownPct: summary.max_drawdown_pct,
    sharpe: summary.sharpe ?? portfolioMetrics.sharpe,
    trades: summary.total_trades ?? summary.closed_trades ?? 0,
    readiness,
    reportStatus: readinessStatus(readiness),
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
