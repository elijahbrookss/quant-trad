import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildCurrentRunRowsFromBots,
  buildProjectedRunsFromBots,
  buildRunRows,
  filterAndSortRunRows,
  filterResearchRows,
  formatBacktestWindow,
} from '../src/features/operations/buildOperationsViewModel.js'
import { buildRunInventoryScopeKey } from '../src/features/operations/runInventoryScope.js'

const NOW = Date.parse('2026-08-02T12:00:00Z')

test('run inventory keeps definition identity and run instance identity separate', () => {
  const rows = buildRunRows([{
    run_id: 'run-1',
    bot_id: 'definition-1',
    bot_name: 'Breakout paper',
    run_type: 'paper',
    execution_mode: 'full',
    status: 'completed',
    strategy_name: 'Breakout',
    symbols: ['ETH-USD'],
    started_at: '2026-08-02T10:00:00Z',
    ended_at: '2026-08-02T11:00:00Z',
    definition: { id: 'definition-1', name: 'Definition name' },
  }], { nowEpochMs: NOW })

  assert.equal(rows[0].id, 'run-1')
  assert.equal(rows[0].definitionId, 'definition-1')
  assert.equal(rows[0].definitionName, 'Definition name')
  assert.equal(rows[0].durationMs, 3_600_000)
})

test('historical backtests expose a stable UTC evaluation window', () => {
  const rows = buildRunRows([{
    run_id: 'run-year',
    run_type: 'backtest',
    backtest_start: '2024-01-01T00:00:00Z',
    backtest_end: '2025-01-01T00:00:00Z',
  }], { nowEpochMs: NOW })

  assert.equal(rows[0].simulatedStart, '2024-01-01T00:00:00Z')
  assert.equal(rows[0].simulatedEnd, '2025-01-01T00:00:00Z')
  assert.equal(rows[0].simulatedWindowLabel, 'Jan 1, 2024 → Jan 1, 2025')
  assert.equal(formatBacktestWindow('invalid', '2025-01-01T00:00:00Z'), null)
})

test('run filters are deterministic with run id as the secondary key', () => {
  const rows = buildRunRows([
    { run_id: 'run-b', status: 'completed', run_type: 'backtest', started_at: '2026-08-02T10:00:00Z', symbols: ['ETH-USD'] },
    { run_id: 'run-a', status: 'completed', run_type: 'backtest', started_at: '2026-08-02T10:00:00Z', symbols: ['BTC-USD'] },
    { run_id: 'run-live', status: 'running', run_type: 'live', started_at: '2026-08-02T11:00:00Z' },
  ], { nowEpochMs: NOW })

  const selected = filterAndSortRunRows(rows, { status: 'completed', runType: 'backtest', sort: 'recent' })
  assert.deepEqual(selected.map((row) => row.id), ['run-a', 'run-b'])
  assert.deepEqual(filterAndSortRunRows(rows, { query: 'eth' }).map((row) => row.id), ['run-b'])
})

test('current run rows come from live bot projections and exclude terminal definitions', () => {
  const bots = [
    {
      id: 'bot-live',
      name: 'Live backtest',
      run_type: 'backtest',
      active_run_id: 'run-live',
      status: 'running',
      lifecycle: { status: 'running', phase: 'live', telemetry: { run_id: 'run-live', seq: 4 } },
      runtime: { status: 'running', run_id: 'run-live', stats: { total_trades: 3 } },
      run: { started_at: '2026-08-02T11:00:00Z' },
      controls: { can_open_lens: true },
    },
    {
      id: 'bot-done',
      name: 'Finished backtest',
      latest_run_id: 'run-done',
      status: 'completed',
      runtime: { status: 'completed', run_id: 'run-done' },
      run: { started_at: '2026-08-02T09:00:00Z', ended_at: '2026-08-02T10:00:00Z' },
    },
  ]

  assert.deepEqual(buildCurrentRunRowsFromBots(bots, { nowEpochMs: NOW }).map((row) => row.id), ['run-live'])
  assert.deepEqual(buildProjectedRunsFromBots(bots, { nowEpochMs: NOW }).map((run) => run.run_id), ['run-live', 'run-done'])
})

test('history inventory scope is stable across equivalent live array identities', () => {
  const first = [{ id: 'bot-1', active_run_id: 'run-1', latest_run_id: 'run-0' }]
  const second = first.map((definition) => ({ ...definition }))
  assert.equal(buildRunInventoryScopeKey(first), buildRunInventoryScopeKey(second))
})

test('research evidence filtering is status-aware and newest-first', () => {
  const rows = filterResearchRows([
    { id: 'old', kind: 'research_check', status: 'tested', title: 'Old', created_at: '2026-08-01T10:00:00Z' },
    { id: 'new', kind: 'research_check', status: 'tested', title: 'New', created_at: '2026-08-02T10:00:00Z' },
    { id: 'draft', kind: 'hypothesis', status: 'draft', title: 'Draft', created_at: '2026-08-03T10:00:00Z' },
  ], { status: 'tested' })
  assert.deepEqual(rows.map((row) => row.id), ['new', 'old'])
})
