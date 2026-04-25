const STARTING_PHASES = new Set([
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
const PHASE_LABELS = {
  idle: 'Idle',
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
  runtime_subscribing: 'Runtime subscribing',
  awaiting_first_snapshot: 'Waiting for live runtime facts',
  live: 'Live',
  degraded: 'Degraded',
  telemetry_degraded: 'Telemetry degraded',
  startup_failed: 'Startup failed',
  crashed: 'Crashed',
  stopped: 'Stopped',
  completed: 'Completed',
}

export function normalizeBotStatus(value, fallback = 'idle') {
  const normalized = String(value || '').trim().toLowerCase()
  return normalized || fallback
}

export function getBotStatus(bot) {
  const lifecycleStatus = normalizeBotStatus(bot?.lifecycle?.status, '')
  if (lifecycleStatus && lifecycleStatus !== 'idle') return lifecycleStatus
  const runtimeStatus = normalizeBotStatus(bot?.runtime?.status, '')
  if (runtimeStatus) return runtimeStatus
  return normalizeBotStatus(bot?.status || lifecycleStatus || 'idle')
}

export function getBotRunId(bot) {
  const value = bot?.active_run_id || bot?.lifecycle?.telemetry?.run_id || null
  const normalized = String(value || '').trim()
  return normalized || null
}

export function formatLifecyclePhaseLabel(phase) {
  const normalized = String(phase || '').trim().toLowerCase()
  if (!normalized) return 'Idle'
  if (PHASE_LABELS[normalized]) return PHASE_LABELS[normalized]
  return normalized
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

function describeReason(reason, telemetry) {
  switch (reason) {
    case 'container_start_pending':
      return {
        label: 'Container queued',
        detail: 'Docker runtime is being requested from the backend host.',
      }
    case 'runtime_booting':
      return {
        label: 'Runtime booting',
        detail: 'Container is up and the bot engine is initializing.',
      }
    case 'awaiting_first_snapshot':
      return {
        label: 'Waiting for live runtime facts',
        detail: 'Runtime bootstrap completed, but BotLens has not received the first live runtime facts yet.',
      }
    case 'live_runtime':
      return {
        label: 'Live',
        detail: telemetry?.seq
          ? `Receiving runtime snapshots live. seq ${Number(telemetry.seq)}`
          : 'Receiving runtime snapshots live.',
      }
    case 'runtime_degraded':
      return {
        label: 'Degraded',
        detail: 'The runtime is still alive, but one or more workers degraded.',
      }
    case 'runner_stale':
      return {
        label: 'Runner stale',
        detail: 'The backend lost fresh watchdog heartbeats for this bot.',
      }
    case 'container_missing':
      return {
        label: 'Container missing',
        detail: 'The bot row still points at an active lifecycle, but the container is gone.',
      }
    case 'container_exited':
      return {
        label: 'Container exited',
        detail: 'The runtime container exited before a clean terminal update reached the UI.',
      }
    case 'run_completed':
      return {
        label: 'Run completed',
        detail: 'The latest run finished and historical data is available in BotLens.',
      }
    case 'run_stopped':
      return {
        label: 'Stopped',
        detail: 'The runtime was stopped cleanly.',
      }
    case 'runtime_failed':
      return {
        label: 'Runtime failed',
        detail: 'The bot exited with an error. Open diagnostics for the lifecycle trail.',
      }
    default:
      return {
        label: 'Idle',
        detail: 'No active runtime is attached.',
      }
  }
}

export function describeBotLifecycle(bot) {
  const lifecycle = bot?.lifecycle || {}
  const runtime = bot?.runtime || {}
  const heartbeat = lifecycle?.heartbeat || {}
  const telemetry = lifecycle?.telemetry || {}
  const container = lifecycle?.container || {}
  const failure = lifecycle?.failure || {}
  const status = getBotStatus(bot)
  const lifecyclePhase = String(lifecycle?.phase || '').trim().toLowerCase()
  const runtimePhase = String(runtime?.phase || '').trim().toLowerCase()
  const phase = (
    lifecyclePhase && (lifecyclePhase !== 'idle' || !runtimePhase)
      ? lifecyclePhase
      : runtimePhase
  ) || (status === 'running' ? 'live' : 'idle')
  const reason = String(lifecycle?.reason || '').trim().toLowerCase() || status
  const description = describeReason(reason, telemetry)
  const backendLabel = formatLifecyclePhaseLabel(phase)
  const backendMessage =
    String(failure?.message || '').trim() ||
    String(lifecycle?.message || '').trim() ||
    description.detail
  let tone = 'slate'
  if (['running', 'completed'].includes(status) || phase === 'live') tone = status === 'completed' || phase === 'completed' ? 'sky' : 'emerald'
  else if (
    ['starting', 'paused'].includes(status) ||
    [
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
    ].includes(phase)
  ) tone = 'sky'
  else if (['degraded', 'telemetry_degraded'].includes(status) || ['degraded', 'telemetry_degraded'].includes(phase) || reason === 'runner_stale') tone = 'amber'
  else if (['error', 'failed', 'crashed', 'stopped', 'startup_failed'].includes(status) || ['crashed', 'startup_failed', 'stopped'].includes(phase)) tone = 'rose'

  return {
    status,
    phase,
    reason,
    tone,
    label: backendLabel || description.label,
    detail: backendMessage,
    message: String(lifecycle?.message || '').trim() || null,
    failure: failure && typeof failure === 'object' ? failure : null,
    metadata: lifecycle?.metadata && typeof lifecycle.metadata === 'object' ? lifecycle.metadata : {},
    crashSummary: String(lifecycle?.crash_summary || '').trim() || null,
    telemetry,
    heartbeat,
    container,
    live: Boolean(lifecycle?.live),
    heartbeatState: String(heartbeat?.state || 'inactive'),
    containerStatus: String(container?.status || 'missing'),
    updatedAt: lifecycle?.updated_at || lifecycle?.checkpoint_at || runtime?.last_snapshot_at || runtime?.known_at || null,
  }
}

const FAILURE_STATUSES = new Set(['error', 'failed', 'crashed', 'startup_failed'])
const RUNNING_STATUSES = new Set(['running'])
const DEGRADED_STATUSES = new Set(['degraded', 'telemetry_degraded'])
const STOPPED_STATUSES = new Set(['stopped'])
const COMPLETED_STATUSES = new Set(['completed'])
const HEALTHY_PHASES = new Set(['live', 'degraded', 'telemetry_degraded'])
const FAILURE_REASONS = new Set(['container_exited', 'container_missing', 'runner_stale', 'runtime_crashed', 'runtime_failed', 'startup_failed'])

function formatElapsedDuration(startedAt, endedAt = null, nowEpochMs = Date.now()) {
  const startMs = Date.parse(startedAt || '')
  if (!Number.isFinite(startMs)) return null
  const endMs = endedAt ? Date.parse(endedAt) : nowEpochMs
  if (!Number.isFinite(endMs) || endMs <= startMs) return null
  const elapsedSeconds = Math.max(0, Math.floor((endMs - startMs) / 1000))
  const hours = Math.floor(elapsedSeconds / 3600)
  const minutes = Math.floor((elapsedSeconds % 3600) / 60)
  const seconds = elapsedSeconds % 60

  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m`
  if (minutes > 0) return `${minutes}m ${String(seconds).padStart(2, '0')}s`
  return `${seconds}s`
}

export function formatRelativeTime(value, { nowEpochMs = Date.now() } = {}) {
  const epochMs = Date.parse(String(value || ''))
  if (!Number.isFinite(epochMs)) return null
  const deltaSeconds = Math.max(0, Math.floor((nowEpochMs - epochMs) / 1000))
  if (deltaSeconds < 60) return `${deltaSeconds}s ago`
  const deltaMinutes = Math.floor(deltaSeconds / 60)
  if (deltaMinutes < 60) return `${deltaMinutes}m ago`
  const deltaHours = Math.floor(deltaMinutes / 60)
  if (deltaHours < 24) return `${deltaHours}h ago`
  const deltaDays = Math.floor(deltaHours / 24)
  return `${deltaDays}d ago`
}

function normalizeOptionalStatus(value) {
  return normalizeBotStatus(value, '')
}

function firstNonEmpty(values) {
  for (const value of values) {
    const text = String(value || '').trim()
    if (text) return text
  }
  return ''
}

function startupSeriesProgress(metadata) {
  const lifecycleMetadata = metadata && typeof metadata === 'object' ? metadata : {}
  const seriesProgress = lifecycleMetadata.series_progress
  return seriesProgress && typeof seriesProgress === 'object' ? seriesProgress : {}
}

function startupProgressCount(seriesProgress, key) {
  const entries = seriesProgress?.[key]
  return Array.isArray(entries) ? entries.length : 0
}

function startupSeriesTotal(seriesProgress) {
  const total = Number(seriesProgress?.total_series || 0)
  return total > 0 ? total : 0
}

function normalizeFailureMessage(bot, lifecycle) {
  const artifactError = bot?.last_run_artifact?.error
  return firstNonEmpty([
    lifecycle?.failure?.message,
    typeof artifactError === 'string' ? artifactError : artifactError?.message,
  ])
}

function extractBotCardFacts(bot, lifecycle, pendingStart) {
  const run = bot?.run && typeof bot.run === 'object' ? bot.run : {}
  const rawBotStatus = normalizeOptionalStatus(bot?.status)
  const rawLifecycleStatus = normalizeOptionalStatus(bot?.lifecycle?.status)
  const rawRunStatus = normalizeOptionalStatus(run?.status)
  const rawRuntimeStatus = normalizeOptionalStatus(bot?.runtime?.status)
  const phase = String(lifecycle?.phase || '').trim().toLowerCase()
  const runtimePhase = String(bot?.runtime?.phase || '').trim().toLowerCase()
  const reason = String(lifecycle?.reason || '').trim().toLowerCase()
  const containerStatus = String(lifecycle?.containerStatus || lifecycle?.container?.status || 'missing').trim().toLowerCase()
  const heartbeatState = String(lifecycle?.heartbeatState || lifecycle?.heartbeat?.state || 'inactive').trim().toLowerCase()
  const telemetrySeq = Number(lifecycle?.telemetry?.seq || 0)
  const warningCount = Number(lifecycle?.telemetry?.warning_count || 0)
  const projectedRunId = getBotRunId(bot)
  const runtimeRunId = String(bot?.runtime?.run_id || '').trim() || null
  const failureMessage = normalizeFailureMessage(bot, lifecycle)
  const crashSummary = String(lifecycle?.crashSummary || '').trim()
  const startedAt = run?.started_at || bot?.last_run_artifact?.started_at || bot?.last_run_at || null
  const endedAt =
    run?.ended_at ||
    bot?.last_run_artifact?.ended_at ||
    lifecycle?.container?.finished_at ||
    null
  const statuses = [rawLifecycleStatus, rawRunStatus, rawBotStatus].filter(Boolean)
  const runningSignal = (
    statuses.some((status) => RUNNING_STATUSES.has(status)) ||
    RUNNING_STATUSES.has(rawRuntimeStatus) ||
    phase === 'live' ||
    runtimePhase === 'live'
  )
  const degradedSignal = (
    statuses.some((status) => DEGRADED_STATUSES.has(status)) ||
    DEGRADED_STATUSES.has(rawRuntimeStatus) ||
    DEGRADED_STATUSES.has(phase) ||
    DEGRADED_STATUSES.has(runtimePhase)
  )
  const completedSignal = statuses.some((status) => COMPLETED_STATUSES.has(status)) || phase === 'completed' || reason === 'run_completed'
  const stoppedSignal = statuses.some((status) => STOPPED_STATUSES.has(status)) || phase === 'stopped' || reason === 'run_stopped'
  const startupFailureSignal =
    statuses.includes('startup_failed') || phase === 'startup_failed' || reason === 'startup_failed'
  const crashSignal =
    !completedSignal &&
    !stoppedSignal &&
    (
      statuses.some((status) => FAILURE_STATUSES.has(status) && status !== 'startup_failed') ||
      phase === 'crashed' ||
      FAILURE_REASONS.has(reason) ||
      ['exited', 'dead'].includes(containerStatus) ||
      (heartbeatState === 'stale' && Boolean(projectedRunId || runtimeRunId))
    )
  const healthyEvidence =
    runningSignal ||
    degradedSignal ||
    HEALTHY_PHASES.has(phase) ||
    telemetrySeq > 0
  const startingContext =
    pendingStart ||
    statuses.includes('starting') ||
    STARTING_PHASES.has(phase) ||
    rawRuntimeStatus === 'starting' ||
    STARTING_PHASES.has(runtimePhase) ||
    (!healthyEvidence && Boolean(projectedRunId || runtimeRunId) && !completedSignal && !stoppedSignal && !startupFailureSignal && !crashSignal)
  const runId = projectedRunId || ((runningSignal || degradedSignal || startingContext) ? runtimeRunId : null)

  return {
    rawBotStatus,
    rawLifecycleStatus,
    rawRunStatus,
    rawRuntimeStatus,
    phase,
    runtimePhase,
    reason,
    containerStatus,
    heartbeatState,
    warningCount,
    runId,
    startedAt,
    endedAt,
    failureMessage,
    crashSummary,
    runningSignal,
    degradedSignal,
    completedSignal,
    stoppedSignal,
    startupFailureSignal,
    crashSignal,
    healthyEvidence,
    startingContext,
    pendingStart,
  }
}

function getBotCardStatusKey(facts) {
  if (facts.rawLifecycleStatus === 'paused' || facts.rawRunStatus === 'paused' || facts.rawBotStatus === 'paused') {
    return 'paused'
  }
  if (facts.pendingStart && !facts.healthyEvidence) {
    return 'starting'
  }
  if (facts.startupFailureSignal) {
    return 'failed_start'
  }
  if (facts.crashSignal) {
    return facts.startingContext && !facts.healthyEvidence ? 'failed_start' : 'crashed'
  }
  if (facts.completedSignal) return 'completed'
  if (facts.stoppedSignal) return 'stopped'
  if (facts.degradedSignal) return 'degraded'
  if (facts.runningSignal) return 'running'
  if (facts.startingContext) return 'starting'
  return 'stopped'
}

function getStartingStatusDetail(phase, lifecycle, facts) {
  if (['start_requested', 'validating_configuration', 'resolving_strategy', 'resolving_runtime_dependencies', 'preparing_run', 'stamping_starting_state'].includes(phase)) {
    return 'Preparing run'
  }
  if (['launching_container', 'container_launched'].includes(phase)) {
    return 'Initializing execution environment'
  }
  if (['awaiting_container_boot', 'container_booting'].includes(phase)) {
    return 'Waiting for runtime bootstrap'
  }
  const seriesProgress = startupSeriesProgress(lifecycle?.metadata)
  const totalSeries = startupSeriesTotal(seriesProgress)
  if (phase === 'waiting_for_series_bootstrap' && totalSeries > 0) {
    return `Waiting for series bootstrap (${startupProgressCount(seriesProgress, 'bootstrapped_series')}/${totalSeries} bootstrapped)`
  }
  if (phase === 'warming_up_runtime' && totalSeries > 0) {
    return `Warming runtime (${startupProgressCount(seriesProgress, 'warming_series')}/${totalSeries} series warming)`
  }
  if (phase === 'runtime_subscribing' && totalSeries > 0) {
    return `Subscribing workers to live facts (${startupProgressCount(seriesProgress, 'bootstrapped_series')}/${totalSeries} series bootstrapped)`
  }
  if (phase === 'awaiting_first_snapshot' && totalSeries > 0) {
    return `Bootstrap complete (${startupProgressCount(seriesProgress, 'live_series')}/${totalSeries} series live)`
  }
  if (
    [
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
    ].includes(phase)
  ) {
    if (phase === 'awaiting_first_snapshot') {
      return lifecycle?.message || 'Waiting for first live runtime facts'
    }
    return lifecycle?.message || 'Waiting for runtime bootstrap'
  }
  if (facts.runId) return 'Run requested, awaiting backend lifecycle'
  return lifecycle?.message || lifecycle?.detail || 'Preparing run'
}

function getFailureDetail(statusKey, facts) {
  if (facts.failureMessage && facts.failureMessage.length <= 120) {
    return facts.failureMessage
  }
  if (facts.crashSummary && facts.crashSummary.length <= 120) {
    return facts.crashSummary
  }
  if (facts.reason === 'runner_stale') {
    return statusKey === 'failed_start' ? 'Startup heartbeat timed out' : 'Runtime heartbeat lost'
  }
  if (facts.reason === 'container_missing') {
    return statusKey === 'failed_start'
      ? 'Runtime container never became ready'
      : 'Runtime container is no longer running'
  }
  if (facts.reason === 'container_exited' || ['exited', 'dead'].includes(facts.containerStatus)) {
    return statusKey === 'failed_start'
      ? 'Container exited during bootstrap'
      : 'Container exited unexpectedly'
  }
  return statusKey === 'failed_start' ? 'Execution bootstrap failed' : 'Runtime exited unexpectedly'
}

function getCardStatusDetail(bot, lifecycle, facts, statusKey, nowEpochMs) {
  const activeDuration = formatElapsedDuration(facts.startedAt, null, nowEpochMs)
  const completedDuration = formatElapsedDuration(
    bot?.last_run_artifact?.started_at || facts.startedAt,
    bot?.last_run_artifact?.ended_at || facts.endedAt,
    nowEpochMs,
  )

  if (statusKey === 'starting') {
    return getStartingStatusDetail(lifecycle?.phase, lifecycle, facts)
  }
  if (statusKey === 'running') {
    return activeDuration ? `Runtime live for ${activeDuration}` : 'Runtime is active'
  }
  if (statusKey === 'degraded') {
    if (facts.warningCount > 0) {
      return `${facts.warningCount} runtime ${facts.warningCount === 1 ? 'warning' : 'warnings'} active`
    }
    return 'Runtime is active with degraded health'
  }
  if (statusKey === 'paused') {
    return 'Runtime is paused'
  }
  if (statusKey === 'completed') {
    return completedDuration ? `Run completed in ${completedDuration}` : 'Run completed'
  }
  if (statusKey === 'failed_start' || statusKey === 'crashed') {
    return getFailureDetail(statusKey, facts)
  }
  return facts.reason === 'run_stopped' ? 'Stopped cleanly' : 'Ready to start'
}

function resolveCardControls(bot, facts, statusKey) {
  const rawControls = bot?.controls && typeof bot.controls === 'object' ? bot.controls : {}
  const active = ['starting', 'running', 'degraded', 'paused'].includes(statusKey)
  const lensEligible = ['running', 'degraded', 'paused'].includes(statusKey) && Boolean(facts.runId)
  const diagnosticsEligible = ['crashed', 'failed_start'].includes(statusKey) && Boolean(facts.runId)

  return {
    canOpenLens: lensEligible && rawControls.can_open_lens !== false,
    canViewReport: Boolean(facts.runId),
    canViewDiagnostics: diagnosticsEligible,
    canStop:
      statusKey === 'starting'
        ? Boolean(rawControls.can_stop)
        : statusKey === 'running' || statusKey === 'degraded' || statusKey === 'paused'
          ? rawControls.can_stop !== false
          : false,
    canStart: ['stopped', 'completed', 'crashed', 'failed_start'].includes(statusKey),
    canDelete: !active,
    startLabel:
      statusKey === 'completed'
        ? 'Rerun'
        : ['crashed', 'failed_start', 'degraded'].includes(statusKey)
          ? 'Restart'
          : 'Start',
  }
}

function buildCardActions(statusKey, controls, pendingStart) {
  const actions = []

  if (pendingStart && statusKey === 'starting') {
    actions.push({ key: 'starting', label: 'Starting…', tone: 'primary', busy: true, disabled: true })
  } else if (statusKey === 'starting') {
    if (controls.canStop) {
      actions.push({ key: 'stop', label: 'Cancel', tone: 'danger' })
    } else {
      actions.push({ key: 'starting', label: 'Starting…', tone: 'ghost', disabled: true })
    }
  } else if (statusKey === 'running' || statusKey === 'paused' || statusKey === 'degraded') {
    if (controls.canOpenLens) {
      actions.push({ key: 'open', label: 'Open Lens', tone: 'primary' })
    }
    if (controls.canStop) {
      actions.push({ key: 'stop', label: 'Stop', tone: 'ghost' })
    }
  } else if (statusKey === 'completed') {
    if (controls.canStart) {
      actions.push({ key: 'start', label: 'Rerun', tone: 'primary' })
    }
  } else if (statusKey === 'crashed' || statusKey === 'failed_start') {
    if (controls.canViewDiagnostics) {
      actions.push({ key: 'diagnostics', label: 'View Diagnostics', tone: 'danger' })
    }
    if (controls.canStart) {
      actions.push({ key: 'start', label: 'Restart', tone: 'ghost' })
    }
  } else if (controls.canStart) {
    actions.push({ key: 'start', label: controls.startLabel || 'Start', tone: 'primary' })
  }

  if (controls.canDelete) {
    actions.push({ key: 'delete', label: 'Delete', tone: 'ghost' })
  }

  actions.push({
    key: 'report',
    label: 'View Report',
    tone: 'secondary',
    disabled: !controls.canViewReport,
    title: controls.canViewReport ? undefined : 'No run id available.',
  })

  return rankBotCardActions(actions)
}

function actionPriority(action) {
  const key = String(action?.key || '').trim().toLowerCase()
  if (key === 'start' || key === 'starting') return 0
  if (key === 'open') return 1
  if (key === 'report') return 2
  if (key === 'diagnostics') return 3
  if (key === 'delete' || key === 'stop') return 4
  return 4
}

function actionVariant(action) {
  const key = String(action?.key || '').trim().toLowerCase()
  if (key === 'start' || key === 'starting') return 'primary'
  if (key === 'diagnostics') return 'diagnostic'
  if (key === 'open') return 'secondary'
  if (key === 'report') return 'secondary'
  if (key === 'delete' || key === 'stop') return 'danger'
  return 'tertiary'
}

export function rankBotCardActions(actions = []) {
  return [...actions]
    .sort((left, right) => actionPriority(left) - actionPriority(right))
    .map((action) => ({
      ...action,
      variant: actionVariant(action),
    }))
}

export function getBotCardDisplayState(bot, { nowEpochMs = Date.now(), pendingStart = false } = {}) {
  const lifecycle = describeBotLifecycle(bot)
  const facts = extractBotCardFacts(bot, lifecycle, pendingStart)
  const statusKey = getBotCardStatusKey(facts)
  const controls = resolveCardControls(bot, facts, statusKey)
  const tone =
    statusKey === 'running'
        ? 'emerald'
      : statusKey === 'degraded' || statusKey === 'paused'
        ? 'amber'
        : statusKey === 'starting'
          ? 'sky'
          : statusKey === 'completed'
            ? 'sky'
            : statusKey === 'crashed' || statusKey === 'failed_start'
              ? 'rose'
              : 'slate'

  return {
    statusKey,
    displayStatus:
      statusKey === 'failed_start'
        ? 'Startup failed'
        : statusKey === 'paused'
        ? 'Paused'
        : statusKey === 'degraded'
          ? 'Degraded'
        : statusKey.charAt(0).toUpperCase() + statusKey.slice(1),
    tone,
    detail: getCardStatusDetail(bot, lifecycle, facts, statusKey, nowEpochMs),
    warningCount: facts.warningCount,
    runId: facts.runId,
    containerStatus: facts.containerStatus,
    heartbeatState: facts.heartbeatState,
    startedAt: facts.startedAt,
    endedAt: facts.endedAt,
    isTerminal: ['failed_start', 'crashed', 'stopped', 'completed'].includes(statusKey),
    lifecycle,
    controls,
    allowedActions: buildCardActions(statusKey, controls, pendingStart),
  }
}
