import { formatRelativeTime } from '../bots/state/botRuntimeStatus.js'
import { deriveCollectorHealth, COLLECTOR_HEALTH_COPY } from './collectorHealth.js'

const STATUS_TONE = {
  healthy: 'emerald',
  failed: 'rose',
  disabled: 'slate',
  overdue: 'rose',
  stale: 'amber',
  unknown: 'slate',
}

const STATUS_BADGE_LABEL = {
  healthy: 'On schedule',
  failed: 'Failed',
  disabled: 'Disabled',
  overdue: 'Overdue',
  stale: 'Stale',
  unknown: 'Unknown',
}

function formatCadence(pollIntervalSeconds) {
  const seconds = Number(pollIntervalSeconds)
  if (!Number.isFinite(seconds) || seconds <= 0) return 'unknown cadence'
  if (seconds < 60) return `every ${seconds}s`
  if (seconds < 3600) return `every ${Math.round(seconds / 60)}m`
  return `every ${Math.round(seconds / 3600)}h`
}

function statusDetailFor(health, nowEpochMs) {
  if (health.status === 'unknown') {
    return health.lastAttemptAt
      ? `no successful attempt yet · last try ${formatRelativeTime(health.lastAttemptAt, { nowEpochMs }) || 'unknown'}`
      : 'no recorded attempts yet'
  }
  if (health.status === 'disabled') {
    return health.lastSuccessAt
      ? `scheduler disabled · last success ${formatRelativeTime(health.lastSuccessAt, { nowEpochMs }) || 'unknown'}`
      : 'scheduler disabled · no successful attempt recorded'
  }
  if (health.status === 'failed') return 'latest collection attempt failed'
  if (health.status === 'overdue') {
    return `past expected poll time · next expected ${formatRelativeTime(health.nextExpectedAt, { nowEpochMs }) || 'unknown'}`
  }
  if (health.status === 'stale') {
    return `last success ${formatRelativeTime(health.lastSuccessAt, { nowEpochMs }) || 'unknown'} · connection stale`
  }
  return `last success ${formatRelativeTime(health.lastSuccessAt, { nowEpochMs }) || 'unknown'} · process liveness unobserved`
}

/**
 * Maps a collector definition + its recent attempts into the same card-view
 * shape `buildBotCardViewModel` produces (statusLabel/display.tone/metricStats)
 * so it renders through the existing .qt2-fleet-card/TONE_CLASS styling.
 * Never reports 'healthy' on missing timestamps — see deriveCollectorHealth.
 */
export function buildCollectorCardViewModel(definition, attempts = [], { nowEpochMs = Date.now() } = {}) {
  const health = deriveCollectorHealth(definition, attempts, nowEpochMs)
  const id = String(definition?.id || '').trim()
  const displayName = [definition?.provider, definition?.fact_type].filter(Boolean).join(' · ') || 'Collector'
  const instrumentLabel = String(definition?.instrument_id || '—')
  const venueLabel = String(definition?.venue || '—')
  const cadenceLabel = formatCadence(definition?.poll_interval_seconds)

  return {
    id,
    displayName,
    instrumentLabel,
    venueLabel,
    cadenceLabel,
    health,
    statusLabel: STATUS_BADGE_LABEL[health.status] || 'Unknown',
    statusCopy: COLLECTOR_HEALTH_COPY[health.status],
    statusDetail: statusDetailFor(health, nowEpochMs),
    metricStats: [
      { key: 'last-success', label: 'Last success', value: formatRelativeTime(health.lastSuccessAt, { nowEpochMs }) || '—' },
      { key: 'next-expected', label: 'Next expected', value: formatRelativeTime(health.nextExpectedAt, { nowEpochMs }) || '—' },
    ],
    display: {
      tone: STATUS_TONE[health.status] || 'slate',
      warningCount: health.status === 'healthy' ? 0 : 1,
      controls: { canOpenLens: true },
    },
  }
}
