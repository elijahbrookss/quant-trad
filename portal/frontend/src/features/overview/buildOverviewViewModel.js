import { buildCollectorCardViewModel } from '../collectors/buildCollectorCardViewModel.js'

export const ATTENTION_CONTRACT = Object.freeze({
  lookbackHours: 72,
  ordering: 'severity_desc,evidence_at_desc,id_asc',
  deduplication: 'one item per canonical evidence identity; retain the more severe and newer item',
})

const ACTIVE_RUN_STATUSES = new Set(['starting', 'running', 'degraded', 'telemetry_degraded', 'paused'])
const FAILED_RUN_STATUSES = new Set(['crashed', 'failed', 'failed_start', 'startup_failed'])
const SEVERITY_RANK = { critical: 0, warning: 1, info: 2 }

function toEpochMs(value) {
  const parsed = Date.parse(String(value || ''))
  return Number.isFinite(parsed) ? parsed : null
}

function evidenceTime(record) {
  return record?.ended_at
    || record?.finished_at
    || record?.checkpoint_at
    || record?.updated_at
    || record?.started_at
    || record?.created_at
    || null
}

function withinLookback(value, nowEpochMs, lookbackHours) {
  const epoch = toEpochMs(value)
  return epoch !== null && epoch >= nowEpochMs - lookbackHours * 3_600_000
}

function addDeduplicated(target, item) {
  const current = target.get(item.id)
  if (!current) {
    target.set(item.id, item)
    return
  }
  const severityDelta = (SEVERITY_RANK[item.severity] ?? 9) - (SEVERITY_RANK[current.severity] ?? 9)
  const newer = (toEpochMs(item.evidenceAt) || 0) > (toEpochMs(current.evidenceAt) || 0)
  if (severityDelta < 0 || (severityDelta === 0 && newer)) target.set(item.id, item)
}

export function resolveGreeting(nowEpochMs = Date.now()) {
  const hour = new Date(nowEpochMs).getHours()
  if (hour < 12) return 'Good morning'
  if (hour < 18) return 'Good afternoon'
  return 'Good evening'
}

export function rankAttentionItems({
  runs = [],
  collectors = [],
  postureRows = [],
  researchItems = [],
  nowEpochMs = Date.now(),
  lookbackHours = ATTENTION_CONTRACT.lookbackHours,
} = {}) {
  const items = new Map()

  runs.forEach((run) => {
    const status = String(run?.lifecycle?.status || run?.status || 'unknown').toLowerCase()
    const at = evidenceTime(run?.lifecycle) || evidenceTime(run)
    const label = run?.definition?.name || run?.definition?.strategy_name || run?.bot_id || 'Run'
    if (FAILED_RUN_STATUSES.has(status) && withinLookback(at, nowEpochMs, lookbackHours)) {
      addDeduplicated(items, {
        id: 'run:' + run.run_id,
        severity: 'critical',
        kind: 'run',
        title: label + ' failed',
        detail: 'Run ' + run.run_id + ' · ' + status,
        evidenceAt: at,
        href: '/operations/runs/' + run.run_id,
        state: { run, definition: run.definition, from: '/overview' },
      })
    } else if (ACTIVE_RUN_STATUSES.has(status) && status.includes('degraded')) {
      addDeduplicated(items, {
        id: 'run:' + run.run_id,
        severity: 'warning',
        kind: 'run',
        title: label + ' degraded',
        detail: 'Run ' + run.run_id + ' · ' + status,
        evidenceAt: at,
        href: '/operations/runs/' + run.run_id,
        state: { run, definition: run.definition, from: '/overview' },
      })
    }
  })

  collectors.forEach((collector) => {
    if (['HEALTHY', 'DISABLED', 'STOPPED', 'PAUSED'].includes(collector?.actual_state)) return
    const vm = buildCollectorCardViewModel(collector, { nowEpochMs })
    const severity = collector.actual_state === 'FAILED' ? 'critical' : 'warning'
    addDeduplicated(items, {
      id: `collector:${collector.collector_kind}:${collector.collector_id}`,
      severity,
      kind: 'collector',
      title: `${vm.providerLabel} · ${vm.displayName}`,
      detail: `${collector.actual_state} · ${collector.error?.message || vm.stateCopy}`,
      evidenceAt: vm.evidenceAt,
      href: vm.route,
      state: { from: '/overview' },
    })
  })

  postureRows.forEach((row) => {
    const invalidCoverage = ['invalid', 'incomplete'].includes(row?.coverage?.value)
    const invalidBook = row?.book?.value === 'invalid'
    const unavailable = Number(row?.unavailableStatusCount || 0) > 0
    if (!invalidCoverage && !invalidBook && !unavailable) return
    addDeduplicated(items, {
      id: 'market:' + row.id,
      severity: invalidCoverage || invalidBook ? 'critical' : 'warning',
      kind: 'market',
      title: row.label + ' data evidence',
      detail: invalidCoverage
        ? row.coverage.label
        : invalidBook
          ? row.book.label
          : row.unavailableStatusCount + ' status projection' + (row.unavailableStatusCount === 1 ? '' : 's') + ' unavailable',
      evidenceAt: row.latestEvidenceAt,
      href: '/operations?tab=data-plane',
      state: { from: '/overview' },
    })
  })

  researchItems.forEach((item) => {
    if (item?.kind !== 'research_check' || item?.status !== 'blocked') return
    if (!withinLookback(item?.created_at, nowEpochMs, lookbackHours)) return
    addDeduplicated(items, {
      id: 'research:' + item.id,
      severity: 'critical',
      kind: 'research',
      title: item.title || 'Research check blocked',
      detail: [item.symbol, item.timeframe, 'blocked'].filter(Boolean).join(' · '),
      evidenceAt: item.created_at,
      href: '/operations/research/' + item.id,
      state: { item, from: '/overview' },
    })
  })

  return Array.from(items.values()).sort((left, right) => {
    const severity = (SEVERITY_RANK[left.severity] ?? 9) - (SEVERITY_RANK[right.severity] ?? 9)
    if (severity !== 0) return severity
    const recency = (toEpochMs(right.evidenceAt) || 0) - (toEpochMs(left.evidenceAt) || 0)
    return recency !== 0 ? recency : left.id.localeCompare(right.id)
  })
}

export function buildCurrentOperations({ runs = [], collectors = [] } = {}) {
  const runRows = runs
    .filter((run) => ACTIVE_RUN_STATUSES.has(String(run?.lifecycle?.status || run?.status || '').toLowerCase()))
    .map((run) => ({
      id: 'run:' + run.run_id,
      kind: run?.run_type === 'backtest' ? 'backtest' : 'run',
      title: run?.definition?.name || run?.definition?.strategy_name || run?.bot_id || 'Run',
      detail: [run?.run_type, run?.lifecycle?.phase, run?.status].filter(Boolean).join(' · '),
      status: run?.lifecycle?.status || run?.status || 'unknown',
      evidenceAt: evidenceTime(run?.lifecycle) || evidenceTime(run),
      href: '/operations/runs/' + run.run_id,
      state: { run, definition: run.definition, from: '/overview' },
    }))

  const collectorRows = collectors.flatMap((collector) => {
    if (!collector?.runtime?.active) return []
    const vm = buildCollectorCardViewModel(collector)
    return [{
      id: `collector:${collector.collector_kind}:${collector.collector_id}`,
      kind: 'collector',
      title: `${collector.provider} · ${vm.displayName}`,
      detail: vm.schemaLabel,
      status: collector.actual_state,
      evidenceAt: vm.evidenceAt,
      href: vm.route,
      state: { from: '/overview' },
    }]
  })

  return [...runRows, ...collectorRows].sort((left, right) => {
    const recency = (toEpochMs(right.evidenceAt) || 0) - (toEpochMs(left.evidenceAt) || 0)
    return recency !== 0 ? recency : left.id.localeCompare(right.id)
  })
}

export const selectActiveWorkloads = buildCurrentOperations
