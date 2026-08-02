function toEpochMs(value) {
  const parsed = Date.parse(String(value || ''))
  return Number.isFinite(parsed) ? parsed : null
}

function durationMs(start, end, nowEpochMs) {
  const startMs = toEpochMs(start)
  if (startMs === null) return null
  const endMs = toEpochMs(end) ?? nowEpochMs
  return Math.max(0, endMs - startMs)
}

export function buildRunRows(runs = [], { nowEpochMs = Date.now() } = {}) {
  return runs.map((run) => {
    const status = String(run?.runtime_status || run?.lifecycle?.status || run?.status || 'unknown').toLowerCase()
    const definition = run?.definition || {}
    return {
      id: run.run_id,
      run,
      definition,
      definitionId: run.bot_id || definition.id || null,
      definitionName: definition.name || run.bot_name || 'Definition unavailable',
      strategy: run.strategy_name || definition.strategy_name || run.strategy_id || definition.strategy_id || 'Unavailable',
      runType: run.run_type || definition.run_type || 'unknown',
      executionMode: run.execution_mode || definition.execution_mode || 'unavailable',
      status,
      phase: run?.lifecycle?.phase || null,
      instruments: Array.isArray(run.symbols) ? run.symbols : [],
      timeframe: run.timeframe || '—',
      startedAt: run.started_at || null,
      endedAt: run.ended_at || null,
      knownAt: run.known_at || run.last_snapshot_at || run.updated_at || null,
      durationMs: durationMs(run.started_at, run.ended_at, nowEpochMs),
      simulatedStart: run.backtest_start || null,
      simulatedEnd: run.backtest_end || null,
      netPnl: run?.summary?.net_pnl,
      warningCount: Number(run?.summary?.warning_count || run?.warning_count || 0),
      botLensAvailable: Boolean(run.botlens_available),
      botLensReason: run.botlens_reason || null,
      isActive: Boolean(run.is_active),
    }
  })
}

export function filterAndSortRunRows(rows = [], {
  query = '',
  status = 'all',
  runType = 'all',
  sort = 'recent',
} = {}) {
  const needle = String(query || '').trim().toLowerCase()
  const filtered = rows.filter((row) => {
    if (status !== 'all' && row.status !== status) return false
    if (runType !== 'all' && row.runType !== runType) return false
    if (!needle) return true
    return [
      row.id,
      row.definitionName,
      row.strategy,
      row.runType,
      row.executionMode,
      row.status,
      row.phase,
      row.timeframe,
      ...row.instruments,
    ].some((value) => String(value || '').toLowerCase().includes(needle))
  })

  return [...filtered].sort((left, right) => {
    if (sort === 'oldest') {
      const delta = (toEpochMs(left.startedAt) || 0) - (toEpochMs(right.startedAt) || 0)
      return delta !== 0 ? delta : left.id.localeCompare(right.id)
    }
    if (sort === 'status') {
      const delta = left.status.localeCompare(right.status)
      return delta !== 0 ? delta : left.id.localeCompare(right.id)
    }
    if (sort === 'definition') {
      const delta = left.definitionName.localeCompare(right.definitionName)
      return delta !== 0 ? delta : left.id.localeCompare(right.id)
    }
    const delta = (toEpochMs(right.startedAt) || 0) - (toEpochMs(left.startedAt) || 0)
    return delta !== 0 ? delta : left.id.localeCompare(right.id)
  })
}

export function filterResearchRows(items = [], { query = '', status = 'all' } = {}) {
  const needle = String(query || '').trim().toLowerCase()
  return items
    .filter((item) => status === 'all' || item?.status === status)
    .filter((item) => !needle || [
      item?.id,
      item?.kind,
      item?.title,
      item?.status,
      item?.symbol,
      item?.timeframe,
      ...(item?.tags || []),
    ].some((value) => String(value || '').toLowerCase().includes(needle)))
    .sort((left, right) => {
      const delta = (toEpochMs(right?.created_at) || 0) - (toEpochMs(left?.created_at) || 0)
      return delta !== 0 ? delta : String(left?.id || '').localeCompare(String(right?.id || ''))
    })
}
