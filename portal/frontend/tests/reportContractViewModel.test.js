import test from 'node:test'
import assert from 'node:assert/strict'

import {
  comparisonStatus,
  readinessStatus,
  reportListItemView,
  reportSummaryView,
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
