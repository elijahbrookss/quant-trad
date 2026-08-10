import { formatRelativeTime } from '../bots/state/botRuntimeStatus.js'

const HEALTH_TONE = {
  HEALTHY: 'emerald',
  DELAYED: 'amber',
  FAILED: 'rose',
  UNKNOWN: 'slate',
  NOT_APPLICABLE: 'slate',
}

const STATE_COPY = {
  RUNNING: 'The collector is expected to acquire canonical Facts.',
  STOPPING: 'Active ownership is draining after an operator state change.',
  STOPPED: 'The collector is intentionally stopped.',
  PAUSED: 'The collector is intentionally paused.',
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

function lastDataLabel(value) {
  if (!value) return 'No accepted data'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime())
    ? 'Last data unavailable'
    : `Last data ${parsed.toLocaleDateString()}`
}

/** Render-only mapping. Lifecycle, health, and attention are backend-owned. */
export function buildCollectorCardViewModel(collector, { nowEpochMs = Date.now() } = {}) {
  const state = String(collector?.operational_state || collector?.actual_state || 'UNKNOWN').toUpperCase()
  const health = String(collector?.health_status || 'UNKNOWN').toUpperCase()
  const acceptedAt = collector?.acquisition?.last_accepted_fact_at || null
  const heartbeatAt = collector?.worker?.heartbeat_at || null
  const freshness = collector?.acquisition?.freshness_seconds
  const id = String(collector?.collector_id || '')
  const running = state === 'RUNNING'

  return {
    id,
    key: `${collector?.collector_kind || 'unknown'}:${id}`,
    route: `/operations/market/${encodeURIComponent(collector?.collector_kind || '')}/${encodeURIComponent(id)}`,
    displayName: subjectLabel(collector),
    providerLabel: collector?.provider || 'Provider unavailable',
    kindLabel: String(collector?.collector_kind || '').replaceAll('_', ' ') || 'Collector',
    schemaLabel: schemaLabel(collector),
    state,
    health,
    stateCopy: STATE_COPY[state] || 'Lifecycle explanation unavailable.',
    tone: HEALTH_TONE[health] || 'slate',
    needsAttention: Boolean(collector?.needs_attention),
    evidenceAt: collector?.acquisition?.last_attempt_at || acceptedAt || heartbeatAt,
    freshnessLabel: running && Number.isFinite(Number(freshness))
      ? `${Math.round(Number(freshness))}s`
      : running
        ? 'Freshness unknown'
        : lastDataLabel(acceptedAt),
    lastAcceptedLabel: formatRelativeTime(acceptedAt, { nowEpochMs }) || 'No accepted fact',
    heartbeatLabel: collector?.worker?.alive
      ? formatRelativeTime(heartbeatAt, { nowEpochMs }) || 'Current'
      : 'Worker unavailable',
    throughputLabel: running
      ? `${Number(collector?.throughput?.accepted_last_minute || 0).toLocaleString()}/min`
      : '—',
  }
}

export const COLLECTOR_STATE_COPY = STATE_COPY
