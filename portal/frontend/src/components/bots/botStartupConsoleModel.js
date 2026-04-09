const ACTIVE_STARTUP_PHASES = new Set([
  'start_requested',
  'validating_configuration',
  'resolving_strategy',
  'resolving_runtime_dependencies',
  'preparing_run',
  'stamping_starting_state',
  'launching_container',
  'container_launched',
  'awaiting_container_boot',
  'container_booting',
  'loading_bot_config',
  'claiming_run',
  'loading_strategy_snapshot',
  'preparing_wallet',
  'planning_series_workers',
  'spawning_series_workers',
  'waiting_for_series_bootstrap',
  'warming_up_runtime',
  'runtime_subscribing',
  'awaiting_first_snapshot',
])

const STABLE_PHASES = new Set(['live', 'completed', 'stopped'])
const DEGRADED_PHASES = new Set(['degraded', 'telemetry_degraded'])
const FAILURE_PHASES = new Set(['startup_failed', 'crashed'])

const PHASE_LABELS = {
  idle: 'Standby',
  start_requested: 'Start requested',
  validating_configuration: 'Validating configuration',
  resolving_strategy: 'Resolving strategy',
  resolving_runtime_dependencies: 'Resolving runtime dependencies',
  preparing_run: 'Preparing run',
  stamping_starting_state: 'Stamping starting state',
  launching_container: 'Launching container',
  container_launched: 'Container launched',
  awaiting_container_boot: 'Awaiting container boot',
  container_booting: 'Container booting',
  loading_bot_config: 'Loading bot config',
  claiming_run: 'Claiming run',
  loading_strategy_snapshot: 'Loading strategy snapshot',
  preparing_wallet: 'Preparing wallet',
  planning_series_workers: 'Planning series workers',
  spawning_series_workers: 'Spawning series workers',
  waiting_for_series_bootstrap: 'Waiting for series bootstrap',
  warming_up_runtime: 'Warming runtime',
  runtime_subscribing: 'Subscribing runtime',
  awaiting_first_snapshot: 'Awaiting first snapshot',
  live: 'Live',
  degraded: 'Degraded',
  telemetry_degraded: 'Telemetry degraded',
  startup_failed: 'Startup failed',
  crashed: 'Crashed',
  stopped: 'Stopped',
  completed: 'Completed',
}

const SERIES_STATUS_LABELS = {
  planned: 'worker plan ready',
  spawned: 'worker spawned',
  bootstrapped: 'bootstrap complete',
  warming_up: 'warming runtime',
  awaiting_first_snapshot: 'awaiting first snapshot',
  live: 'first snapshot received',
  failed: 'series degraded',
}

function normalizeText(value) {
  const normalized = String(value || '').trim()
  return normalized || ''
}

function hasLifecycleFailure(failure) {
  return Boolean(
    failure &&
    typeof failure === 'object' &&
    Object.values(failure).some((value) => normalizeText(value))
  )
}

function normalizePhase(bot) {
  const lifecyclePhase = normalizeText(bot?.lifecycle?.phase).toLowerCase()
  if (lifecyclePhase) return lifecyclePhase
  const lifecycleStatus = normalizeText(bot?.lifecycle?.status).toLowerCase()
  if (lifecycleStatus) return lifecycleStatus
  const runtimeStatus = normalizeText(bot?.runtime?.status).toLowerCase()
  if (runtimeStatus) return runtimeStatus
  return 'idle'
}

function normalizeStatus(bot) {
  return (
    normalizeText(bot?.lifecycle?.status).toLowerCase() ||
    normalizeText(bot?.runtime?.status).toLowerCase() ||
    normalizeText(bot?.status).toLowerCase() ||
    'idle'
  )
}

function normalizeRunId(bot) {
  return (
    normalizeText(bot?.active_run_id) ||
    normalizeText(bot?.runtime?.run_id) ||
    normalizeText(bot?.lifecycle?.telemetry?.run_id) ||
    null
  )
}

export function formatLifecyclePhaseLabel(phase) {
  const normalized = normalizeText(phase).toLowerCase()
  if (!normalized) return 'Standby'
  if (PHASE_LABELS[normalized]) return PHASE_LABELS[normalized]
  return normalized
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

export function classifyLifecycleTone({ phase, status, failure }) {
  const normalizedPhase = normalizeText(phase).toLowerCase()
  const normalizedStatus = normalizeText(status).toLowerCase()
  if (hasLifecycleFailure(failure) || FAILURE_PHASES.has(normalizedPhase) || ['error', 'failed', 'crashed', 'startup_failed'].includes(normalizedStatus)) {
    return 'rose'
  }
  if (DEGRADED_PHASES.has(normalizedPhase) || ['degraded', 'telemetry_degraded'].includes(normalizedStatus)) {
    return 'amber'
  }
  if (STABLE_PHASES.has(normalizedPhase) || normalizedStatus === 'completed') {
    return normalizedPhase === 'completed' || normalizedStatus === 'completed' ? 'sky' : 'emerald'
  }
  if (ACTIVE_STARTUP_PHASES.has(normalizedPhase) || normalizedStatus === 'starting') {
    return 'sky'
  }
  return 'slate'
}

function formatTimeLabel(value) {
  const parsed = Date.parse(String(value || ''))
  if (!Number.isFinite(parsed)) return 'now'
  return new Date(parsed).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

function buildSeriesProgressSummary(progress) {
  if (!progress || typeof progress !== 'object') return ''
  const totalSeries = Math.max(0, Number(progress.total_series) || 0)
  const workersPlanned = Math.max(0, Number(progress.workers_planned) || 0)
  const workersSpawned = Math.max(0, Number(progress.workers_spawned) || 0)
  const bootstrapped = Array.isArray(progress.bootstrapped_series) ? progress.bootstrapped_series.length : 0
  const awaitingFirstSnapshot = Array.isArray(progress.awaiting_first_snapshot_series) ? progress.awaiting_first_snapshot_series.length : 0
  const live = Array.isArray(progress.live_series) ? progress.live_series.length : 0
  const failed = Array.isArray(progress.failed_series) ? progress.failed_series.length : 0
  const parts = []
  if (totalSeries > 0) parts.push(`${totalSeries} series`)
  if (workersPlanned > 0) parts.push(`${workersSpawned}/${workersPlanned} workers`)
  if (bootstrapped > 0) parts.push(`${bootstrapped} bootstrapped`)
  if (awaitingFirstSnapshot > 0) parts.push(`${awaitingFirstSnapshot} awaiting snapshot`)
  if (live > 0) parts.push(`${live} live`)
  if (failed > 0) parts.push(`${failed} failed`)
  return parts.join(' • ')
}

function buildLifecycleMetadataSummary(metadata) {
  if (!metadata || typeof metadata !== 'object') return ''
  const progressSummary = buildSeriesProgressSummary(metadata.series_progress)
  if (progressSummary) return progressSummary
  const symbolCount = Math.max(0, Number(metadata.symbol_count) || 0)
  const parts = []
  if (symbolCount > 0) parts.push(`${symbolCount} symbols`)
  if (Array.isArray(metadata.symbols) && metadata.symbols.length > 0) {
    parts.push(metadata.symbols.map((symbol) => String(symbol || '').trim().toUpperCase()).filter(Boolean).join(', '))
  }
  if (metadata.strategy_id) {
    parts.push(`strategy ${String(metadata.strategy_id).slice(0, 8)}`)
  }
  return parts.join(' • ')
}

function buildPhaseEntry(bot) {
  const lifecycle = bot?.lifecycle
  if (!lifecycle || typeof lifecycle !== 'object') return null
  const runId = normalizeRunId(bot) || 'pending'
  const phase = normalizePhase(bot)
  const status = normalizeStatus(bot)
  const failure = lifecycle.failure && typeof lifecycle.failure === 'object' ? lifecycle.failure : null
  const checkpointAt = lifecycle.checkpoint_at || lifecycle.updated_at || null
  const message = normalizeText(failure?.message) || normalizeText(lifecycle.message) || `${formatLifecyclePhaseLabel(phase)}.`
  const metadataSummary = buildLifecycleMetadataSummary(lifecycle.metadata)
  const detail = failure && normalizeText(lifecycle.message) && normalizeText(lifecycle.message) !== message
    ? normalizeText(lifecycle.message)
    : metadataSummary
  const fingerprint = [
    runId,
    phase,
    status,
    checkpointAt || 'no-checkpoint',
    message,
    normalizeText(failure?.message),
  ].join('|')
  return {
    id: `phase:${fingerprint}`,
    kind: 'phase',
    runId,
    phase,
    status,
    tone: classifyLifecycleTone({ phase, status, failure }),
    timestamp: checkpointAt,
    timeLabel: formatTimeLabel(checkpointAt),
    label: formatLifecyclePhaseLabel(phase),
    message,
    meta: detail || metadataSummary,
    terminal: STABLE_PHASES.has(phase) || DEGRADED_PHASES.has(phase) || FAILURE_PHASES.has(phase),
  }
}

function buildSeriesSnapshot(progress) {
  const nextSnapshot = {}
  const seriesMap = progress && typeof progress === 'object' && progress.series && typeof progress.series === 'object'
    ? progress.series
    : {}
  const entries = []
  Object.entries(seriesMap)
    .sort(([left], [right]) => left.localeCompare(right))
    .forEach(([symbolKey, rawState]) => {
      const symbol = normalizeText(symbolKey || rawState?.symbol).toUpperCase()
      if (!symbol || !rawState || typeof rawState !== 'object') return
      const status = normalizeText(rawState.status).toLowerCase()
      if (!status) return
      const message = normalizeText(rawState.error) || normalizeText(rawState.message) || SERIES_STATUS_LABELS[status] || formatLifecyclePhaseLabel(status)
      const fingerprint = [status, message, normalizeText(rawState.worker_id), normalizeText(rawState.series_key), normalizeText(rawState.updated_at)].join('|')
      nextSnapshot[symbol] = fingerprint
      entries.push({
        symbol,
        status,
        message,
        workerId: normalizeText(rawState.worker_id),
        seriesKey: normalizeText(rawState.series_key),
        updatedAt: rawState.updated_at || null,
      })
    })
  return { nextSnapshot, entries }
}

function buildSeriesEntries(runId, progress, previousSnapshot) {
  if (!progress || typeof progress !== 'object') {
    return { entries: [], snapshot: previousSnapshot || {} }
  }
  const { nextSnapshot, entries } = buildSeriesSnapshot(progress)
  const nextEntries = []
  for (const entry of entries) {
    if (previousSnapshot?.[entry.symbol] === nextSnapshot[entry.symbol]) continue
    const tone = entry.status === 'failed'
      ? 'rose'
      : entry.status === 'live'
        ? 'emerald'
        : entry.status === 'bootstrapped'
          ? 'sky'
          : 'slate'
    const metaParts = []
    if (entry.workerId) metaParts.push(entry.workerId)
    if (entry.seriesKey) metaParts.push(entry.seriesKey)
    nextEntries.push({
      id: `series:${runId || 'pending'}:${entry.symbol}:${nextSnapshot[entry.symbol]}`,
      kind: 'series',
      runId,
      phase: entry.status,
      status: entry.status,
      symbol: entry.symbol,
      tone,
      timestamp: entry.updatedAt,
      timeLabel: formatTimeLabel(entry.updatedAt),
      label: entry.symbol,
      message: messageForSeriesEntry(entry.status, entry.message),
      meta: metaParts.join(' • '),
      terminal: entry.status === 'failed' || entry.status === 'live',
    })
  }
  return { entries: nextEntries, snapshot: nextSnapshot }
}

function messageForSeriesEntry(status, message) {
  if (!message) return SERIES_STATUS_LABELS[status] || formatLifecyclePhaseLabel(status)
  const prefix = SERIES_STATUS_LABELS[status]
  if (!prefix || message.toLowerCase() === prefix.toLowerCase()) return message
  return `${prefix} • ${message}`
}

export function buildBotStartupConsoleState(previousState, bot, { maxEntries = 28 } = {}) {
  const runId = normalizeRunId(bot)
  const resetHistory = Boolean(previousState?.runId && runId && previousState.runId !== runId)
  const previousEntries = resetHistory ? [] : Array.isArray(previousState?.entries) ? previousState.entries : []
  const previousSnapshot = resetHistory ? {} : previousState?.seriesSnapshot || {}
  const dedupe = new Set(previousEntries.map((entry) => entry.id))
  const nextEntries = [...previousEntries]

  const phaseEntry = buildPhaseEntry(bot)
  if (phaseEntry && !dedupe.has(phaseEntry.id)) {
    nextEntries.push(phaseEntry)
    dedupe.add(phaseEntry.id)
  }

  const seriesProgress = bot?.lifecycle?.metadata?.series_progress
  const { entries: seriesEntries, snapshot } = buildSeriesEntries(runId, seriesProgress, previousSnapshot)
  for (const entry of seriesEntries) {
    if (dedupe.has(entry.id)) continue
    nextEntries.push(entry)
    dedupe.add(entry.id)
  }

  const trimmedEntries = nextEntries.slice(-maxEntries)
  return {
    runId,
    entries: trimmedEntries,
    seriesSnapshot: snapshot,
    current: buildCurrentConsoleLine(bot),
  }
}

export function buildCurrentConsoleLine(bot) {
  const lifecycle = bot?.lifecycle || {}
  const phase = normalizePhase(bot)
  const status = normalizeStatus(bot)
  const failure = lifecycle.failure && typeof lifecycle.failure === 'object' ? lifecycle.failure : null
  const metadataSummary = buildLifecycleMetadataSummary(lifecycle.metadata)
  return {
    key: [normalizeRunId(bot) || 'pending', phase, lifecycle.updated_at || lifecycle.checkpoint_at || 'now'].join('|'),
    runId: normalizeRunId(bot),
    phase,
    status,
    label: formatLifecyclePhaseLabel(phase),
    message: normalizeText(failure?.message) || normalizeText(lifecycle.message) || 'No active lifecycle checkpoint.',
    meta: metadataSummary,
    tone: classifyLifecycleTone({ phase, status, failure }),
    animated: ACTIVE_STARTUP_PHASES.has(phase),
    stable: STABLE_PHASES.has(phase),
    degraded: DEGRADED_PHASES.has(phase),
    failure,
  }
}

export function isLifecycleConsoleVisible(bot) {
  const phase = normalizePhase(bot)
  const status = normalizeStatus(bot)
  return phase !== 'idle' || status !== 'idle' || Boolean(normalizeRunId(bot))
}
