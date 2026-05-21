import test from 'node:test'
import assert from 'node:assert/strict'

import {
  comparisonStatus,
  durationSecondsFromWindow,
  metricDisplayState,
  readinessStatus,
  reportListItemView,
  reportSummaryView,
  runReportListItemView,
  runReportView,
  sectionByName,
} from '../src/components/reports/reportContractViewModel.js'

test('report contract list item view maps canonical summary and readiness', () => {
  const view = reportListItemView({
    run_id: 'run-1',
    bot_id: 'bot-1',
    bot_name: 'Research Bot',
    strategy_name: 'Breakout',
    symbols: ['BTC'],
    timeframe: '1h',
    execution_mode: 'full',
    simulated_window: { start: '2026-01-01T00:00:00Z', end: '2026-01-31T00:00:00Z' },
    summary: {
      net_pnl: 12.5,
      total_return: 0.0125,
      max_drawdown_pct: 0.04,
      total_trades: 3,
    },
    portfolio_metrics: {
      sharpe: 1.4,
    },
    readiness: {
      results_ready: true,
      safe_to_compare: true,
    },
  })

  assert.equal(view.runId, 'run-1')
  assert.equal(view.netPnl, 12.5)
  assert.equal(view.totalReturn, 0.0125)
  assert.equal(view.sharpe, 1.4)
  assert.equal(view.reportStatus, 'ready')
  assert.equal(view.comparisonStatus, 'eligible')
})

test('report summary view preserves sectioned contract fields', () => {
  const view = reportSummaryView({
    run_id: 'run-1',
    metadata: {
      run_id: 'run-1',
      execution_mode: 'fast',
      symbols: ['ETH'],
    },
    summary: {
      net_pnl: -3,
      fees: 1,
      closed_trades: 2,
      rejected_decisions: 1,
    },
    portfolio_metrics: {
      sharpe: 1.2,
      sortino: 1.3,
      calmar: 0.4,
      annualized_volatility: 0.12,
      exposure_pct: 0.25,
    },
    readiness: {
      dataset_ready: true,
      results_ready: false,
      safe_to_compare: false,
    },
    sections: {
      items: [{ name: 'trades', available: true, row_count: 2 }],
    },
  })

  assert.equal(view.reportStatus, 'partial')
  assert.equal(view.comparisonStatus, 'blocked')
  assert.equal(view.trades, 2)
  assert.equal(view.sharpe, 1.2)
  assert.equal(view.sortino, 1.3)
  assert.equal(view.calmar, 0.4)
  assert.equal(view.annualizedVolatility, 0.12)
  assert.equal(view.exposurePct, 0.25)
  assert.equal(view.rejectedDecisions, 1)
  assert.equal(sectionByName(view.sections).get('trades').row_count, 2)
})

test('readiness helpers do not infer comparison from result readiness alone', () => {
  assert.equal(readinessStatus({ results_ready: true, safe_to_compare: false }), 'ready')
  assert.equal(comparisonStatus({ results_ready: true, safe_to_compare: false }), 'blocked')
})

test('run report v2 view preserves backend trust and sectioned DTO data', () => {
  const view = runReportView({
    contract_version: 'run_report_v2',
    schema_version: 'run_report.v2',
    run_id: 'run-v2',
    identity: {
      bot_id: 'bot-1',
      bot_name: 'Research Bot',
      strategy_id: 'strategy-1',
      strategy_name: 'Breakout',
      symbols: ['BIP', 'XPP'],
      timeframe: '1d',
      wall_clock_window: { start: '2026-01-01T00:00:00Z', end: '2026-01-01T00:10:00Z' },
    },
    trust: {
      lifecycle_status: 'completed',
      readiness_status: 'ready',
      research_status: 'research_ready',
      semantic_fingerprint: 'semantic-abc',
      data_snapshot_hash: 'data-abc',
    },
    performance: {
      net_pnl: { value: 125, valid: true, unit: 'currency', method: 'sum_closed_trade_net_pnl' },
      sharpe: { value: null, valid: false, invalid_reason: 'zero_return_stddev' },
    },
    behavior: { total_decisions: 103 },
    wallet: { wallet_projection_status: 'equal' },
    symbol_breakdown: [{ symbol: 'BIP', trade_count: 4 }],
    coordinator_waits: { status: 'not_available' },
    operational_diagnostics: { operational_drift_status: 'not_computed' },
    raw_refs: { dataset: 'run_research_dataset_v1' },
  })

  assert.equal(view.supported, true)
  assert.equal(view.runId, 'run-v2')
  assert.equal(view.trust.research_status, 'research_ready')
  assert.equal(view.trust.semantic_fingerprint, 'semantic-abc')
  assert.equal(view.performance.net_pnl.value, 125)
  assert.equal(view.performance.sharpe.valid, false)
  assert.equal(view.symbolBreakdown.length, 1)
  assert.equal(view.coordinatorWaits.status, 'not_available')
})

test('run report v2 list item adapter uses DTO facts without inventing trust', () => {
  const fallback = reportListItemView({
    run_id: 'run-v2',
    bot_name: 'Fallback Bot',
    strategy_name: 'Fallback Strategy',
    summary: { net_pnl: -1, total_trades: 1 },
    readiness: { results_ready: true, safe_to_compare: false },
  })

  const view = runReportListItemView({
    contract_version: 'run_report_v2',
    run_id: 'run-v2',
    identity: {
      bot_name: 'DTO Bot',
      strategy_name: 'DTO Strategy',
      run_type: 'backtest',
      symbols: ['BIP'],
      timeframe: '1d',
      wall_clock_window: { start: '2026-01-01T00:00:00Z', end: '2026-01-01T00:10:00Z' },
    },
    trust: {
      readiness_status: 'ready',
      research_status: 'research_ready',
      golden_status: 'pass',
      semantic_fingerprint: 'semantic-abc',
    },
    performance: {
      net_pnl: { value: 42, valid: true, unit: 'currency' },
      total_return_pct: { value: 0.012, valid: true, unit: 'ratio' },
      max_drawdown_pct: { value: 0.03, valid: true, unit: 'ratio' },
      trade_count: { value: 9, valid: true, unit: 'count' },
      sharpe: { value: null, valid: false, invalid_reason: 'zero_return_stddev' },
    },
    operational_diagnostics: { operational_drift_status: 'not_computed' },
  }, fallback)

  assert.equal(view.botName, 'DTO Bot')
  assert.equal(view.strategyName, 'DTO Strategy')
  assert.equal(view.readinessStatus, 'ready')
  assert.equal(view.researchStatus, 'research_ready')
  assert.equal(view.goldenStatus, 'pass')
  assert.equal(view.semanticFingerprint, 'semantic-abc')
  assert.equal(view.netPnl, 42)
  assert.equal(view.totalReturn, 0.012)
  assert.equal(view.maxDrawdownPct, 0.03)
  assert.equal(view.trades, 9)
  assert.equal(view.sharpe, null)
  assert.equal(view.sharpeMetric.valid, false)
  assert.equal(view.durationSeconds, 600)
})

test('metric display state exposes validity metadata for unavailable ratios', () => {
  const valid = metricDisplayState({
    value: 1.25,
    valid: true,
    unit: 'ratio',
    method: 'return_mean_over_stddev',
    source: 'RunResearchDataset.portfolio_metrics',
    sample_count: 12,
    minimum_sample_count: 2,
  })
  const invalid = metricDisplayState({
    value: null,
    valid: false,
    unit: 'ratio',
    method: 'cagr_over_drawdown',
    source: 'RunResearchDataset.portfolio_metrics',
    invalid_reason: 'max_drawdown_zero',
    caveats: ['drawdown unavailable'],
  })

  assert.equal(valid.valid, true)
  assert.equal(valid.value, 1.25)
  assert.equal(valid.method, 'return_mean_over_stddev')
  assert.equal(valid.sampleCount, 12)
  assert.equal(invalid.valid, false)
  assert.equal(invalid.value, null)
  assert.equal(invalid.invalidReason, 'max_drawdown_zero')
  assert.deepEqual(invalid.caveats, ['drawdown unavailable'])
})

test('duration helper derives wall-clock runtime when backend supplies window bounds', () => {
  assert.equal(
    durationSecondsFromWindow({ start: '2026-01-01T00:00:00Z', end: '2026-01-01T00:09:20Z' }),
    560,
  )
  assert.equal(durationSecondsFromWindow({ start: '2026-01-01T00:00:00Z' }), null)
})
