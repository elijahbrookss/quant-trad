const DEFAULT_GRACE_MULTIPLIER = 2
const DEFAULT_STALE_MULTIPLIER = 3

function toEpochMs(value) {
  if (!value) return null
  const parsed = new Date(value).getTime()
  return Number.isFinite(parsed) ? parsed : null
}

/**
 * Derives honest collector health facts — never a single "running" boolean.
 * `status` distinguishes delivery evidence from scheduler configuration.
 * Missing timestamps (no recorded successful attempt, no next-scheduled time)
 * always produce 'unknown', never silently fall through to 'healthy'.
 * 'healthy' requires both a current worker heartbeat and on-schedule delivery evidence.
 */
export function deriveCollectorHealth(definition, attempts = [], nowEpochMs = Date.now()) {
  const schedulerEnabled = Boolean(definition?.enabled)
  const workerStatus = String(definition?.worker_health?.status || 'unknown')
  const workerAlive = workerStatus === 'alive'
  const workerLivenessKnown = workerStatus !== 'unknown'
  const pollIntervalMs = Math.max(1, Number(definition?.poll_interval_seconds) || 0) * 1000

  const sortedAttempts = Array.isArray(attempts)
    ? [...attempts].sort((a, b) => (toEpochMs(b?.started_at) || 0) - (toEpochMs(a?.started_at) || 0))
    : []
  const lastAttempt = sortedAttempts[0] || null
  const lastAttemptAt = lastAttempt?.started_at || null
  const lastAttemptStatus = lastAttempt?.status || null
  const lastAttemptEpochMs = toEpochMs(lastAttemptAt)

  const lastSuccess = sortedAttempts.find((attempt) => attempt?.status === 'succeeded') || null
  const lastSuccessAt = lastSuccess?.finished_at || lastSuccess?.started_at || null
  const lastSuccessEpochMs = toEpochMs(lastSuccessAt)

  const nextExpectedAt = definition?.next_scheduled_at || null
  const nextExpectedEpochMs = toEpochMs(nextExpectedAt)

  const graceMs = pollIntervalMs * DEFAULT_GRACE_MULTIPLIER
  const staleMs = pollIntervalMs * DEFAULT_STALE_MULTIPLIER

  const overdue = nextExpectedEpochMs != null && nowEpochMs > nextExpectedEpochMs + graceMs
  const stale = lastSuccessEpochMs != null && nowEpochMs - lastSuccessEpochMs > staleMs

  let deliveryStatus
  if (!schedulerEnabled) {
    deliveryStatus = 'disabled'
  } else if (
    lastAttemptStatus === 'failed'
    && lastAttemptEpochMs !== null
    && (lastSuccessEpochMs === null || lastAttemptEpochMs >= lastSuccessEpochMs)
  ) {
    deliveryStatus = 'failed'
  } else if (lastSuccessEpochMs == null || nextExpectedEpochMs == null) {
    deliveryStatus = 'unknown'
  } else if (overdue) {
    deliveryStatus = 'overdue'
  } else if (stale) {
    deliveryStatus = 'stale'
  } else {
    deliveryStatus = 'healthy'
  }

  const activeAttemptStalled = lastAttemptStatus === 'running'
    && definition?.lease_active === true
    && definition?.lease_current === false
  let status = deliveryStatus
  if (schedulerEnabled && activeAttemptStalled) {
    status = 'stalled'
  } else if (schedulerEnabled && !workerAlive) {
    status = workerLivenessKnown ? 'offline' : 'unknown'
  }

  return {
    status,
    deliveryStatus,
    schedulerEnabled,
    workerStatus,
    workerAlive,
    workerLivenessKnown,
    activeAttemptStalled,
    lastAttemptAt,
    lastAttemptStatus,
    lastSuccessAt,
    nextExpectedAt,
    overdue,
    stale,
    processLivenessUnknown: !workerLivenessKnown,
  }
}

export const COLLECTOR_HEALTH_COPY = {
  healthy: 'On schedule — recent delivery evidence',
  failed: 'Latest collection attempt failed',
  offline: 'Collector worker heartbeat expired',
  stalled: 'Active collection attempt lost its lease',
  overdue: 'Overdue — past expected poll time',
  stale: 'Stale — no recent successful attempt',
  unknown: 'Timing or successful-attempt evidence is missing',
  disabled: 'Configured but scheduler disabled',
}
