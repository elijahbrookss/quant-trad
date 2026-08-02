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
 * Even 'healthy' means on-schedule delivery only; process liveness is unknown.
 */
export function deriveCollectorHealth(definition, attempts = [], nowEpochMs = Date.now()) {
  const schedulerEnabled = Boolean(definition?.enabled)
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

  let status
  if (!schedulerEnabled) {
    status = 'disabled'
  } else if (
    lastAttemptStatus === 'failed'
    && lastAttemptEpochMs !== null
    && (lastSuccessEpochMs === null || lastAttemptEpochMs >= lastSuccessEpochMs)
  ) {
    status = 'failed'
  } else if (lastSuccessEpochMs == null || nextExpectedEpochMs == null) {
    status = 'unknown'
  } else if (overdue) {
    status = 'overdue'
  } else if (stale) {
    status = 'stale'
  } else {
    status = 'healthy'
  }

  return {
    status,
    schedulerEnabled,
    lastAttemptAt,
    lastAttemptStatus,
    lastSuccessAt,
    nextExpectedAt,
    overdue,
    stale,
    processLivenessUnknown: true,
  }
}

export const COLLECTOR_HEALTH_COPY = {
  healthy: 'On schedule — recent delivery evidence',
  failed: 'Latest collection attempt failed',
  overdue: 'Overdue — past expected poll time',
  stale: 'Stale — no recent successful attempt',
  unknown: 'Timing or successful-attempt evidence is missing',
  disabled: 'Configured but scheduler disabled',
}
