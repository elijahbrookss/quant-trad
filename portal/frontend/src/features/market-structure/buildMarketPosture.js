import { deriveCollectorHealth } from '../collectors/collectorHealth.js'

const PAIR_LABELS = {
  bip_btc: 'BIP / BTC',
  etp_eth: 'ETP / ETH',
  slp_sol: 'SLP / SOL',
}

const TONE_RANK = {
  danger: 0,
  warning: 1,
  info: 2,
  success: 3,
  neutral: 4,
}

function toEpochMs(value) {
  const parsed = Date.parse(String(value || ''))
  return Number.isFinite(parsed) ? parsed : null
}

function latestAt(values) {
  const candidates = values
    .map((value) => ({ value, epoch: toEpochMs(value) }))
    .filter((entry) => entry.epoch !== null)
    .sort((a, b) => b.epoch - a.epoch)
  return candidates[0]?.value || null
}

function humanize(value) {
  const normalized = String(value || '').trim()
  if (!normalized) return 'Unavailable'
  return normalized
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function worstState(states, fallback = { label: 'Unavailable', tone: 'neutral' }) {
  const present = states.filter(Boolean)
  if (!present.length) return fallback
  return [...present].sort(
    (left, right) =>
      (TONE_RANK[left.tone] ?? TONE_RANK.neutral)
      - (TONE_RANK[right.tone] ?? TONE_RANK.neutral),
  )[0]
}

function coverageState(statuses) {
  const intervals = statuses.flatMap((status) =>
    Array.isArray(status?.coverage_intervals)
      ? status.coverage_intervals
      : [],
  )
  if (!intervals.length) {
    return { label: 'No coverage evidence', tone: 'neutral', value: 'unavailable' }
  }
  if (intervals.some((interval) =>
    interval?.status === 'invalid' || interval?.archive_status === 'loss')) {
    return { label: 'Invalid coverage', tone: 'danger', value: 'invalid' }
  }
  if (intervals.some((interval) => interval?.status === 'open_valid')) {
    return { label: 'Open valid interval', tone: 'info', value: 'open_valid' }
  }
  if (intervals.every((interval) => interval?.status === 'closed_valid')) {
    return { label: 'Closed valid intervals', tone: 'success', value: 'closed_valid' }
  }
  return { label: 'Coverage incomplete', tone: 'warning', value: 'incomplete' }
}

function bookState(statuses) {
  const intervals = statuses.flatMap((status) =>
    Array.isArray(status?.book_validity_intervals)
      ? status.book_validity_intervals
      : [],
  )
  if (!intervals.length) {
    return { label: 'No book evidence', tone: 'neutral', value: 'unavailable' }
  }
  if (intervals.some((interval) =>
    ['closed_invalidated', 'invalid'].includes(interval?.status))) {
    return { label: 'Invalid interval recorded', tone: 'danger', value: 'invalid' }
  }
  if (intervals.some((interval) => interval?.status === 'open_valid')) {
    return { label: 'Book valid', tone: 'success', value: 'open_valid' }
  }
  return { label: 'Closed valid evidence', tone: 'info', value: 'closed_valid' }
}

function archiveState(statuses) {
  if (!statuses.length) {
    return { label: 'Status unavailable', tone: 'neutral', value: 'unavailable' }
  }
  const manifests = statuses.reduce(
    (total, status) => total + Number(status?.manifest_count || 0),
    0,
  )
  const lag = statuses.reduce(
    (total, status) =>
      total + Number(status?.archive_mapping_lag_records || 0),
    0,
  )
  if (!manifests) {
    return { label: 'No archive objects', tone: 'neutral', value: 'none' }
  }
  if (lag > 0) {
    return {
      label: `${lag.toLocaleString()} records awaiting mapping`,
      tone: 'warning',
      value: 'mapping_lag',
    }
  }
  return {
    label: `${manifests.toLocaleString()} acknowledged object${manifests === 1 ? '' : 's'}`,
    tone: 'success',
    value: 'available',
  }
}

function collectorState(entries, nowEpochMs) {
  if (!entries.length) {
    return {
      label: 'Not configured',
      detail: 'No scheduled OI or funding collector definition.',
      tone: 'neutral',
      value: 'unavailable',
      lastSuccessAt: null,
    }
  }
  const states = entries.map(({ definition, attempts }) => ({
    factType: definition?.fact_type,
    health: deriveCollectorHealth(definition, attempts, nowEpochMs),
  }))
  const state = worstState(
    states.map(({ health }) => {
      if (health.status === 'offline') {
        return { label: 'Collector offline', tone: 'danger', value: 'offline' }
      }
      if (health.status === 'stalled') {
        return { label: 'Attempt stalled', tone: 'danger', value: 'stalled' }
      }
      if (health.status === 'failed') {
        return { label: 'Latest attempt failed', tone: 'danger', value: 'failed' }
      }
      if (health.status === 'overdue') {
        return { label: 'Overdue', tone: 'danger', value: 'overdue' }
      }
      if (health.status === 'stale') {
        return { label: 'Stale', tone: 'warning', value: 'stale' }
      }
      if (health.status === 'healthy') {
        return { label: 'Recently successful', tone: 'success', value: 'recent_success' }
      }
      if (health.status === 'disabled') {
        return { label: 'Configured, disabled', tone: 'neutral', value: 'disabled' }
      }
      return { label: 'Timing unknown', tone: 'warning', value: 'unknown' }
    }),
  )
  const workerDetail = states.every(({ health }) => health.workerAlive)
    ? 'worker heartbeat current'
    : 'worker heartbeat unavailable'
  const factLabels = states
    .map(({ factType }) => String(factType || '').split('.').pop())
    .filter(Boolean)
    .sort()
  return {
    ...state,
    detail: `${factLabels.join(' + ') || 'scheduled facts'} · ${workerDetail}`,
    lastSuccessAt: latestAt(states.map(({ health }) => health.lastSuccessAt)),
  }
}

function latestSessionForDefinitions(sessions, definitionIds) {
  return sessions
    .filter((session) => definitionIds.has(session?.definition_id))
    .sort(
      (left, right) =>
        (toEpochMs(right?.occurred_at) || 0)
        - (toEpochMs(left?.occurred_at) || 0),
    )[0] || null
}

export function buildMarketPostureRows({
  definitions = [],
  sessions = [],
  statusByDefinition = {},
  normalizationSpecs = [],
  normalizationAvailable = true,
  collectors = [],
  nowEpochMs = Date.now(),
} = {}) {
  const pairIds = new Set(
    definitions
      .map((definition) => definition?.config?.pair_id)
      .filter(Boolean),
  )
  const rows = []

  Array.from(pairIds).sort().forEach((pairId) => {
    const pairDefinitions = definitions.filter(
      (definition) => definition?.config?.pair_id === pairId,
    )
    const definitionIds = new Set(
      pairDefinitions.map((definition) => definition.id),
    )
    const statuses = pairDefinitions
      .map((definition) => statusByDefinition[definition.id])
      .filter((entry) => entry?.available && entry.value)
      .map((entry) => entry.value)
    const unavailableStatusCount = pairDefinitions.filter(
      (definition) => statusByDefinition[definition.id]?.available === false,
    ).length
    const instrumentIds = new Set(
      pairDefinitions.map((definition) => definition?.instrument_id).filter(Boolean),
    )
    const pairCollectors = collectors.filter(({ definition }) =>
      instrumentIds.has(definition?.instrument_id))
    const collection = collectorState(pairCollectors, nowEpochMs)
    const latestSession = latestSessionForDefinitions(sessions, definitionIds)
    const safetyPolicyPresent = (
      pairDefinitions.length > 0
      && pairDefinitions.every((definition) => Boolean(definition?.config?.safety_policy))
    )
    const enabledCount = pairDefinitions.filter((definition) => definition.enabled).length
    const currentLeaseCount = pairDefinitions.filter((definition) => definition.lease_current).length
    const qualityCount = statuses.reduce(
      (total, status) =>
        total + Object.values(status?.quality_counts || {})
          .reduce((subtotal, count) => subtotal + Number(count || 0), 0),
      0,
    )
    const frozenDatasetIds = new Set(
      statuses.flatMap((status) =>
        (status?.dataset_coverage || [])
          .map((entry) => entry?.dataset_id)
          .filter(Boolean),
      ),
    )
    const normalization = !normalizationAvailable
      ? {
          label: 'Normalization unavailable',
          tone: 'warning',
          value: 'unavailable',
        }
      : frozenDatasetIds.size
      ? {
          label: `${frozenDatasetIds.size} frozen dataset${frozenDatasetIds.size === 1 ? '' : 's'}`,
          tone: 'success',
          value: 'frozen',
        }
      : normalizationSpecs.length
        ? {
            label: `${normalizationSpecs.length} specs; no frozen evidence`,
            tone: 'info',
            value: 'specs_only',
          }
        : {
            label: 'No normalization evidence',
            tone: 'neutral',
            value: 'unavailable',
          }
    const evidenceTimes = [
      latestSession?.occurred_at,
      collection.lastSuccessAt,
      ...statuses.map((status) => status?.last_acknowledged_at),
      ...statuses.flatMap((status) =>
        (status?.coverage_intervals || []).map((interval) => interval?.known_at),
      ),
    ]

    rows.push({
      id: pairId,
      label: PAIR_LABELS[pairId] || humanize(pairId),
      products: [...new Set(
        pairDefinitions.map((definition) => definition.provider_product_id),
      )].sort(),
      configured: {
        label: `${pairDefinitions.length} stream definitions`,
        tone: pairDefinitions.length ? 'info' : 'neutral',
        value: pairDefinitions.length ? 'configured' : 'unavailable',
      },
      enabled: {
        label: `${enabledCount}/${pairDefinitions.length} enabled`,
        tone: enabledCount ? 'info' : 'neutral',
        value: enabledCount ? 'enabled' : 'disabled',
      },
      collection,
      coverage: coverageState(statuses),
      book: bookState(statuses),
      archive: archiveState(statuses),
      normalization,
      safety: {
        label: safetyPolicyPresent ? 'Safety policy pinned' : 'Safety policy absent',
        tone: safetyPolicyPresent ? 'success' : 'warning',
        value: safetyPolicyPresent ? 'policy_pinned' : 'policy_absent',
      },
      stream: currentLeaseCount
        ? {
            label: `${currentLeaseCount} current lease${currentLeaseCount === 1 ? '' : 's'}`,
            tone: 'info',
            value: 'lease_current',
          }
        : {
            label: 'No current lease',
            tone: 'neutral',
            value: 'not_observed',
          },
      qualityCount,
      unavailableStatusCount,
      latestEvidenceAt: latestAt(evidenceTimes),
      latestSession,
      definitionIds: [...definitionIds],
      frozenDatasetIds: [...frozenDatasetIds].sort(),
    })
  })

  return rows
}

export function buildStreamSessionRows({ definitions = [], sessions = [] } = {}) {
  const definitionById = new Map(
    definitions.map((definition) => [definition.id, definition]),
  )
  const latestBySession = new Map()
  sessions.forEach((session) => {
    const key = `${session?.definition_id || 'unknown'}:${session?.session_id || 'unknown'}`
    const current = latestBySession.get(key)
    if (!current || (toEpochMs(session?.occurred_at) || 0) > (toEpochMs(current?.occurred_at) || 0)) {
      latestBySession.set(key, session)
    }
  })
  return Array.from(latestBySession.values())
    .map((session) => {
      const definition = definitionById.get(session?.definition_id) || {}
      const eventType = String(session?.event_type || 'unknown')
      const failed = eventType.includes('failed')
      return {
        id: `${session?.definition_id || 'unknown'}:${session?.session_id || 'unknown'}`,
        definitionId: session?.definition_id,
        sessionId: session?.session_id,
        pairId: definition?.config?.pair_id || null,
        productId: definition?.provider_product_id || '—',
        channels: Array.isArray(definition?.channels) ? definition.channels : [],
        eventType,
        eventLabel: humanize(eventType),
        occurredAt: session?.occurred_at || null,
        bounded: Boolean(session?.bounded),
        leaseCurrent: Boolean(definition?.lease_current),
        tone: failed ? 'danger' : definition?.lease_current ? 'info' : 'neutral',
      }
    })
    .sort(
      (left, right) =>
        (toEpochMs(right.occurredAt) || 0) - (toEpochMs(left.occurredAt) || 0),
    )
}
