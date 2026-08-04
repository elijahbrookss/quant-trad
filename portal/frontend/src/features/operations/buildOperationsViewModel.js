import { getBotCardDisplayState } from '../bots/state/botRuntimeStatus.js'

const ACTIVE_RUN_STATUSES = new Set(['starting', 'running', 'degraded', 'telemetry_degraded', 'paused'])

const LIFECYCLE_PHASE_LABELS = Object.freeze({
  start_requested: 'Accepting start request',
  validating_configuration: 'Validating configuration',
  resolving_strategy: 'Resolving strategy',
  resolving_runtime_dependencies: 'Resolving runtime dependencies',
  preparing_run: 'Preparing run record',
  stamping_starting_state: 'Recording startup state',
  launching_container: 'Launching runtime container',
  container_launched: 'Runtime container launched',
  awaiting_container_boot: 'Waiting for runtime process',
  container_booting: 'Runtime process is booting',
  loading_bot_config: 'Loading bot configuration',
  claiming_run: 'Claiming run ownership',
  loading_strategy_snapshot: 'Loading frozen strategy snapshot',
  preparing_wallet: 'Preparing execution wallet',
  planning_series_workers: 'Planning instrument workers',
  spawning_series_workers: 'Starting instrument workers',
  waiting_for_series_bootstrap: 'Loading frozen market series',
  warming_up_runtime: 'Warming strategy state',
  runtime_subscribing: 'Connecting runtime telemetry',
  awaiting_first_snapshot: 'Waiting for first runtime snapshot',
  live: 'Evaluating market data',
  degraded: 'Running with degraded evidence',
  telemetry_degraded: 'Runtime active; telemetry delayed',
  stopping: 'Stopping runtime',
  cancel_requested: 'Accepting cancel request',
  canceling: 'Stopping runtime container',
  startup_failed: 'Startup failed',
  failed: 'Run failed',
  crashed: 'Runtime crashed',
  stopped: 'Runtime stopped',
  canceled: 'Run canceled',
  degraded_terminal: 'Run ended with degraded evidence',
  completed: 'Run completed',
})

export function describeLifecyclePhase(value) {
  const phase = String(value || '').trim().toLowerCase()
  if (!phase) return 'Lifecycle phase unavailable'
  return LIFECYCLE_PHASE_LABELS[phase]
    || phase.split('_').filter(Boolean).map((word) => word[0]?.toUpperCase() + word.slice(1)).join(' ')
}

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
    const totalTrades = run?.summary?.total_trades
    const phase = run?.lifecycle?.phase || null
    const rawProgress = run?.progress ?? run?.runtime?.progress
    const progressValue = rawProgress == null ? Number.NaN : Number(rawProgress)
    const progress = Number.isFinite(progressValue)
      ? Math.min(Math.max(progressValue, 0), 1)
      : null
    return {
      id: run.run_id,
      run,
      definition,
      definitionId: run.bot_id || definition.id || null,
      definitionName: definition.name || run.bot_name || 'Definition unavailable',
      strategy: run.strategy_name || definition.strategy_variant_name || definition.strategy_name || run.strategy_id || definition.strategy_id || 'Unavailable',
      runType: run.run_type || definition.run_type || 'unknown',
      executionMode: run.execution_mode || definition.execution_mode || 'unavailable',
      status,
      phase,
      phaseLabel: describeLifecyclePhase(phase),
      instruments: Array.isArray(run.symbols) ? run.symbols : [],
      timeframe: run.timeframe || '—',
      startedAt: run.started_at || null,
      endedAt: run.ended_at || null,
      knownAt: run.known_at || run.last_snapshot_at || run.updated_at || null,
      livenessState: String(run?.liveness?.state || 'unknown').toLowerCase(),
      livenessLabel: run?.liveness?.state === 'alive'
        ? 'Alive'
        : run?.liveness?.state === 'awaiting_telemetry'
          ? 'Awaiting telemetry'
          : 'Liveness unavailable',
      progress,
      progressPercent: progress === null ? null : Math.round(progress * 100),
      progressCurrent: Number.isFinite(Number(run?.runtime?.progress_current)) ? Number(run.runtime.progress_current) : null,
      progressTotal: Number.isFinite(Number(run?.runtime?.progress_total)) ? Number(run.runtime.progress_total) : null,
      durationMs: durationMs(run.started_at, run.ended_at, nowEpochMs),
      simulatedStart: run.backtest_start || null,
      simulatedEnd: run.backtest_end || null,
      netPnl: run?.summary?.net_pnl,
      totalTrades: Number.isFinite(Number(totalTrades)) ? Number(totalTrades) : null,
      datasetId: run?.config_snapshot?.dataset_binding?.dataset_id || run?.dataset_id || null,
      warningCount: Number(run?.summary?.warning_count || run?.warning_count || 0),
      botLensAvailable: Boolean(run.botlens_available),
      botLensReason: run.botlens_reason || null,
      isActive: Boolean(run.is_active),
    }
  })
}

export function buildProjectedRunsFromBots(bots = [], { nowEpochMs = Date.now() } = {}) {
  return bots.flatMap((bot) => {
    const display = getBotCardDisplayState(bot, { nowEpochMs })
    const runId = String(display?.runId || '').trim()
    if (!runId) return []
    const runtimeStats = bot?.runtime?.stats && typeof bot.runtime.stats === 'object'
      ? bot.runtime.stats
      : {}
    const persistedSummary = bot?.run?.summary && typeof bot.run.summary === 'object'
      ? bot.run.summary
      : {}
    return [{
      ...(bot?.run || {}),
      run_id: runId,
      bot_id: bot.id,
      bot_name: bot.name,
      run_type: bot.run_type,
      execution_mode: bot.execution_mode,
      strategy_id: bot.strategy_id,
      strategy_name: bot.strategy_name,
      symbols: bot?.run?.symbols || bot.symbols || [],
      timeframe: bot?.run?.timeframe || bot.timeframe,
      started_at: display.startedAt || bot?.run?.started_at || null,
      ended_at: display.endedAt || bot?.run?.ended_at || null,
      known_at: bot?.lifecycle?.updated_at || bot?.updated_at || null,
      runtime_status: display.statusKey,
      lifecycle: {
        ...(bot.lifecycle || {}),
        status: display.statusKey,
      },
      progress: bot?.runtime?.progress ?? null,
      summary: { ...persistedSummary, ...runtimeStats },
      botlens_available: Boolean(display?.controls?.canOpenLens),
      botlens_reason: display?.controls?.canOpenLens
        ? null
        : 'BotLens opens after this run has a projected run identity and inspectable evidence.',
      is_active: ACTIVE_RUN_STATUSES.has(display.statusKey),
      definition: bot,
    }]
  })
}

export function buildCurrentRunRowsFromBots(bots = [], { nowEpochMs = Date.now() } = {}) {
  return buildRunRows(
    buildProjectedRunsFromBots(bots, { nowEpochMs }).filter((run) => run.is_active),
    { nowEpochMs },
  )
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
