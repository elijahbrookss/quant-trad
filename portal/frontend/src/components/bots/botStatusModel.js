const ACTIVE_STATUSES = new Set(['starting', 'running', 'paused', 'degraded', 'telemetry_degraded'])

export function normalizeBotStatus(value, fallback = 'idle') {
  const normalized = String(value || '').trim().toLowerCase()
  return normalized || fallback
}

export function getBotStatus(bot) {
  return normalizeBotStatus(bot?.runtime?.status || bot?.lifecycle?.status || bot?.status || 'idle')
}

export function getBotRunId(bot) {
  const value = bot?.runtime?.run_id || bot?.active_run_id || bot?.lifecycle?.telemetry?.run_id || null
  const normalized = String(value || '').trim()
  return normalized || null
}

export function getBotControls(bot) {
  const controls = bot?.controls || {}
  const status = getBotStatus(bot)
  return {
    canStart: controls.can_start ?? !ACTIVE_STATUSES.has(status),
    canStop: controls.can_stop ?? ACTIVE_STATUSES.has(status),
    canOpenLens: controls.can_open_lens ?? Boolean(getBotRunId(bot)),
    canDelete: controls.can_delete ?? !ACTIVE_STATUSES.has(status),
    startLabel:
      controls.start_label ||
      (status === 'completed'
        ? 'Rerun'
        : ['crashed', 'error', 'failed', 'stopped', 'degraded', 'telemetry_degraded'].includes(status)
          ? 'Restart'
          : status === 'starting'
            ? 'Starting'
            : 'Start'),
  }
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
        label: 'Awaiting first snapshot',
        detail: 'Container is running, but BotLens has not received the first merged runtime snapshot yet.',
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
        detail: 'The bot exited with an error. Open BotLens or the error panel for details.',
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
  const heartbeat = lifecycle?.heartbeat || {}
  const telemetry = lifecycle?.telemetry || {}
  const container = lifecycle?.container || {}
  const status = getBotStatus(bot)
  const phase = String(lifecycle?.phase || '').trim().toLowerCase() || (status === 'running' ? 'live' : 'idle')
  const reason = String(lifecycle?.reason || '').trim().toLowerCase() || status
  const description = describeReason(reason, telemetry)
  let tone = 'slate'
  if (['running', 'completed'].includes(status) || phase === 'live') tone = status === 'completed' ? 'sky' : 'emerald'
  else if (['starting', 'paused'].includes(status) || ['starting_container', 'booting_runtime', 'awaiting_snapshot'].includes(phase)) tone = 'sky'
  else if (['degraded', 'telemetry_degraded'].includes(status) || reason === 'runner_stale') tone = 'amber'
  else if (['error', 'failed', 'crashed', 'stopped'].includes(status)) tone = 'rose'

  return {
    status,
    phase,
    reason,
    tone,
    label: description.label,
    detail: description.detail,
    telemetry,
    heartbeat,
    container,
    live: Boolean(lifecycle?.live),
    heartbeatState: String(heartbeat?.state || 'inactive'),
    containerStatus: String(container?.status || 'missing'),
  }
}

export function isActiveBotStatus(status) {
  return ACTIVE_STATUSES.has(normalizeBotStatus(status))
}
