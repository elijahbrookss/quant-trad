import { formatRelativeTime } from '../bots/state/botRuntimeStatus.js'

const STATE_TONE = {
  HEALTHY: 'emerald',
  STARTING: 'cyan',
  RECOVERING: 'cyan',
  RETRYING: 'amber',
  DEGRADED: 'amber',
  FAILED: 'rose',
  STOPPING: 'amber',
  STOPPED: 'slate',
  PAUSED: 'slate',
  DISABLED: 'slate',
}

const STATE_COPY = {
  HEALTHY: 'Worker, acquisition, persistence, and freshness evidence are coherent.',
  STARTING: 'Running is desired; accepted canonical fact evidence has not arrived yet.',
  RECOVERING: 'A bounded recovery operation is active.',
  RETRYING: 'The collector is waiting in its canonical retry policy.',
  DEGRADED: 'The backend detected stale, missing, or failing operational evidence.',
  FAILED: 'The registered collector cannot currently execute safely.',
  STOPPING: 'The desired state changed and active ownership is draining.',
  STOPPED: 'The registered collector is intentionally stopped.',
  PAUSED: 'The registered collector is intentionally paused.',
  DISABLED: 'The collector is disabled in code-owned configuration.',
}

function subjectLabel(collector) {
  return (collector?.subjects || [])
    .map((subject) => subject?.provider_product_id || subject?.symbol || subject?.instrument_id)
    .filter(Boolean)
    .join(', ') || 'Subject unavailable'
}

function schemaLabel(collector) {
  return (collector?.fact_schemas || [])
    .map((schema) => `${schema.fact_type}@${schema.schema_version}`)
    .join(', ') || 'Schema unavailable'
}

/** Render-only mapping. Lifecycle and health are supplied by the backend. */
export function buildCollectorCardViewModel(collector, { nowEpochMs = Date.now() } = {}) {
  const state = String(collector?.actual_state || 'UNKNOWN').toUpperCase()
  const acceptedAt = collector?.acquisition?.last_accepted_fact_at || null
  const heartbeatAt = collector?.worker?.heartbeat_at || null
  const freshness = collector?.acquisition?.freshness_seconds
  const id = String(collector?.collector_id || '')

  return {
    id,
    key: `${collector?.collector_kind || 'unknown'}:${id}`,
    route: `/operations/market/${encodeURIComponent(collector?.collector_kind || '')}/${encodeURIComponent(id)}`,
    displayName: subjectLabel(collector),
    providerLabel: collector?.provider || 'Provider unavailable',
    kindLabel: String(collector?.collector_kind || '').replaceAll('_', ' ') || 'Collector',
    schemaLabel: schemaLabel(collector),
    state,
    stateCopy: STATE_COPY[state] || 'The backend did not report a recognized lifecycle state.',
    tone: STATE_TONE[state] || 'slate',
    needsAttention: ['DEGRADED', 'FAILED', 'RETRYING'].includes(state),
    evidenceAt: collector?.acquisition?.last_attempt_at || acceptedAt || heartbeatAt,
    freshnessLabel: Number.isFinite(Number(freshness))
      ? `${Math.round(Number(freshness))}s lag`
      : 'Lag unavailable',
    lastAcceptedLabel: formatRelativeTime(acceptedAt, { nowEpochMs }) || 'No accepted fact',
    heartbeatLabel: collector?.worker?.alive
      ? formatRelativeTime(heartbeatAt, { nowEpochMs }) || 'Current'
      : 'Worker unavailable',
    throughputLabel: `${Number(collector?.throughput?.accepted_last_minute || 0).toLocaleString()}/min`,
  }
}

export const COLLECTOR_STATE_TONE = STATE_TONE
export const COLLECTOR_STATE_COPY = STATE_COPY
