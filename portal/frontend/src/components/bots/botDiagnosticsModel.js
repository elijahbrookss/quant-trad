import { formatLifecyclePhaseLabel, normalizeBotStatus } from './botStatusModel.js'

const STATUS_LABELS = {
  running: 'Running',
  completed: 'Completed',
  stopped: 'Stopped',
  degraded: 'Degraded',
  telemetry_degraded: 'Telemetry Degraded',
  starting: 'Starting',
  crashed: 'Crashed',
  startup_failed: 'Startup Failed',
  failed: 'Failed',
  pending: 'Pending',
  skipped: 'Skipped',
}

const OWNER_LABELS = {
  backend: 'Backend',
  container: 'Container',
  runtime: 'Runtime',
  watchdog: 'Watchdog',
}

const TERMINAL_OR_FAILURE_STATUSES = new Set([
  'crashed',
  'startup_failed',
  'failed',
  'completed',
  'stopped',
  'degraded',
  'telemetry_degraded',
])

export const DIAGNOSTICS_COPY_RESET_MS = 1600

function titleCaseWords(value) {
  return String(value || '')
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function humanizeToken(value) {
  const text = String(value || '').trim()
  if (!text) return '—'
  return titleCaseWords(text.replaceAll('_', ' '))
}

function normalizeMessage(value) {
  const text = String(value || '').trim()
  if (!text) return null
  if (text.startsWith('Bot marked crashed by watchdog: container_not_running:')) {
    return 'Watchdog observed that the runtime container was no longer running.'
  }
  return text
}

function formatBool(value) {
  return value ? 'Yes' : 'No'
}

function formatCount(value) {
  const count = Number(value)
  return Number.isFinite(count) ? String(Math.max(0, count)) : '0'
}

function formatCountLabel(value, singular, plural = `${singular}s`) {
  const count = Number(value)
  const safeCount = Number.isFinite(count) ? Math.max(0, count) : 0
  return `${safeCount} ${safeCount === 1 ? singular : plural}`
}

function normalizeSummary(payload, lifecycle) {
  const summary = payload?.summary && typeof payload.summary === 'object' ? payload.summary : {}
  const finalObservation = summary?.final_observation && typeof summary.final_observation === 'object'
    ? summary.final_observation
    : {}
  const runStatus = normalizeBotStatus(summary.run_status || payload?.run_status || lifecycle?.status, '')
  return {
    runStatus,
    currentPhase: summary.current_phase || lifecycle?.phase || null,
    rootFailurePhase: summary.root_failure_phase || null,
    rootFailureOwner: summary.root_failure_owner || null,
    rootFailureMessage: normalizeMessage(summary.root_failure_message),
    firstFailureAt: summary.first_failure_at || null,
    lastSuccessfulCheckpoint: summary.last_successful_checkpoint || null,
    containerLaunched: Boolean(summary.container_launched),
    containerBooted: Boolean(summary.container_booted),
    workersPlanned: Number(summary.workers_planned) || 0,
    workersSpawned: Number(summary.workers_spawned) || 0,
    workersLive: Number(summary.workers_live) || 0,
    workersFailed: Number(summary.workers_failed) || 0,
    failedSymbols: Array.isArray(summary.failed_symbols)
      ? summary.failed_symbols.map((value) => String(value || '').trim()).filter(Boolean)
      : [],
    firstFailedWorkerId: summary.first_failed_worker_id || null,
    firstFailedSymbol: summary.first_failed_symbol || null,
    failedWorkerCount: Number(summary.failed_worker_count) || 0,
    anyWorkerLive: Boolean(summary.any_worker_live),
    crashBeforeAnySeriesLive: Boolean(summary.crash_before_any_series_live),
    finalObservation: {
      phase: finalObservation.phase || null,
      owner: finalObservation.owner || null,
      message: normalizeMessage(finalObservation.message),
      at: finalObservation.at || null,
      status: normalizeBotStatus(finalObservation.status || '', ''),
    },
  }
}

export function formatTimestamp(value) {
  const parsed = Date.parse(String(value || ''))
  if (!Number.isFinite(parsed)) return '—'
  return new Date(parsed).toLocaleString()
}

export function formatOwnerLabel(owner) {
  const key = String(owner || '').trim().toLowerCase()
  return OWNER_LABELS[key] || humanizeToken(key)
}

export function formatStatusLabel(status) {
  const key = normalizeBotStatus(status, '')
  return STATUS_LABELS[key] || humanizeToken(key)
}

export function formatCheckpointLabel(value) {
  if (!value) return '—'
  if (typeof value === 'string') return formatLifecyclePhaseLabel(value)
  if (typeof value === 'object') {
    const phase = value.phase || value.label || value.name || null
    if (phase) return formatLifecyclePhaseLabel(phase)
  }
  return humanizeToken(value)
}

function buildSubtitle(summary) {
  if (summary.rootFailureMessage) return summary.rootFailureMessage
  if (summary.finalObservation.message) return summary.finalObservation.message
  if (TERMINAL_OR_FAILURE_STATUSES.has(summary.runStatus)) return formatStatusLabel(summary.runStatus)
  if (summary.currentPhase) return formatLifecyclePhaseLabel(summary.currentPhase)
  if (summary.runStatus) return formatStatusLabel(summary.runStatus)
  return 'Lifecycle diagnostics'
}

function buildQuickFacts(summary) {
  const facts = []
  facts.push(`${formatCount(summary.failedWorkerCount || summary.workersFailed)} workers failed`)
  facts.push(`${formatCount(summary.workersLive)} live`)
  facts.push(summary.containerBooted ? 'Container booted' : 'Container not booted')
  if (summary.lastSuccessfulCheckpoint) {
    facts.push(`Last successful: ${formatCheckpointLabel(summary.lastSuccessfulCheckpoint)}`)
  }
  return facts
}

function buildPrimaryFailure(summary) {
  const contextParts = []
  if (summary.rootFailureOwner) contextParts.push(formatOwnerLabel(summary.rootFailureOwner))
  if (summary.firstFailureAt) contextParts.push(`First detected ${formatTimestamp(summary.firstFailureAt)}`)
  const keyFacts = []
  if (summary.firstFailedWorkerId || summary.firstFailedSymbol) {
    const parts = []
    if (summary.firstFailedWorkerId) parts.push(summary.firstFailedWorkerId)
    if (summary.firstFailedSymbol) parts.push(summary.firstFailedSymbol)
    keyFacts.push({ label: 'First failure', value: parts.join(' • ') })
  }
  if (summary.lastSuccessfulCheckpoint) {
    keyFacts.push({ label: 'Last successful', value: formatCheckpointLabel(summary.lastSuccessfulCheckpoint) })
  }
  keyFacts.push({ label: 'Before any series live', value: formatBool(summary.crashBeforeAnySeriesLive) })
  return {
    title: summary.rootFailurePhase ? humanizeToken(summary.rootFailurePhase) : 'Failure Not Recorded',
    message: summary.rootFailureMessage || 'No structured root failure was recorded for this run.',
    contextLine: contextParts.join(' • '),
    keyFacts,
  }
}

function buildFinalState(summary) {
  return {
    title: 'Final State',
    facts: [
      { label: 'Run status', value: formatStatusLabel(summary.runStatus) },
      { label: 'Current phase', value: summary.currentPhase ? formatLifecyclePhaseLabel(summary.currentPhase) : '—' },
      { label: 'Container launched', value: formatBool(summary.containerLaunched) },
      { label: 'Container booted', value: formatBool(summary.containerBooted) },
      {
        label: 'Workers',
        value: `Planned ${formatCount(summary.workersPlanned)} • Spawned ${formatCount(summary.workersSpawned)} • Live ${formatCount(summary.workersLive)} • Failed ${formatCount(summary.workersFailed)}`,
      },
      { label: 'Before any series live', value: formatBool(summary.crashBeforeAnySeriesLive) },
    ],
  }
}

function buildWorkerFailureEntries(events) {
  const ordered = [...events].sort((left, right) => {
    const leftSeq = Number(left?.seq || 0)
    const rightSeq = Number(right?.seq || 0)
    if (leftSeq !== rightSeq) return leftSeq - rightSeq
    const leftAt = Date.parse(String(left?.checkpoint_at || left?.created_at || ''))
    const rightAt = Date.parse(String(right?.checkpoint_at || right?.created_at || ''))
    if (Number.isFinite(leftAt) && Number.isFinite(rightAt) && leftAt !== rightAt) {
      return leftAt - rightAt
    }
    return 0
  })
  const seen = new Set()
  const entries = []
  for (const event of ordered) {
    const failure = selectFailurePayload(event)
    if (!failure) continue
    const workerId = String(failure.worker_id || '').trim()
    const symbol = String(failure.symbol || '').trim()
    const exitCode = failure.exit_code
    const message = normalizeMessage(failure.message || event?.message)
    if (!workerId && !symbol && exitCode == null) continue
    const key = workerId || symbol || `${event?.seq || entries.length}`
    if (seen.has(key)) continue
    seen.add(key)
    const summaryParts = []
    if (workerId) summaryParts.push(workerId)
    if (symbol) summaryParts.push(symbol)
    if (exitCode != null) summaryParts.push(`exit code ${exitCode}`)
    entries.push({
      key,
      summary: summaryParts.join(' • '),
      message,
    })
  }
  return entries
}

function buildWorkerFailureSummary(summary, events) {
  const entries = buildWorkerFailureEntries(events)
  return {
    title: 'Worker Failures',
    facts: [
      { label: 'Failed workers', value: formatCount(summary.failedWorkerCount) },
      {
        label: 'First failure',
        value: [summary.firstFailedWorkerId, summary.firstFailedSymbol].filter(Boolean).join(' • ') || '—',
      },
      { label: 'Failed symbols', value: summary.failedSymbols.length > 0 ? summary.failedSymbols.join(' • ') : '—' },
    ],
    entries,
  }
}

function compactIdentifier(value) {
  const text = String(value || '').trim()
  if (!text) return 'pending'
  if (text.length <= 18) return text
  return `${text.slice(0, 8)}…${text.slice(-8)}`
}

function buildIdentifier(label, key, value) {
  const raw = String(value || '').trim()
  return {
    label,
    key,
    value: raw,
    displayValue: compactIdentifier(raw),
  }
}

function selectFailurePayload(event) {
  if (event?.failure_details && typeof event.failure_details === 'object' && Object.keys(event.failure_details).length > 0) {
    return event.failure_details
  }
  if (event?.failure && typeof event.failure === 'object' && Object.keys(event.failure).length > 0) {
    return event.failure
  }
  return null
}

function prettyJson(value) {
  if (!value || typeof value !== 'object' || Object.keys(value).length === 0) return null
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return null
  }
}

function buildLifecycleEventRows(events) {
  return [...events].sort((left, right) => {
    const leftSeq = Number(left?.seq || 0)
    const rightSeq = Number(right?.seq || 0)
    if (leftSeq !== rightSeq) return rightSeq - leftSeq
    const leftAt = Date.parse(String(left?.checkpoint_at || left?.created_at || ''))
    const rightAt = Date.parse(String(right?.checkpoint_at || right?.created_at || ''))
    if (Number.isFinite(leftAt) && Number.isFinite(rightAt) && leftAt !== rightAt) {
      return rightAt - leftAt
    }
    return 0
  }).map((event) => {
    const failurePayload = selectFailurePayload(event)
    const metadataJson = prettyJson(event?.metadata)
    const failureJson = prettyJson(failurePayload)
    const failureMessage = normalizeMessage(
      failurePayload?.message ||
      failurePayload?.stderr_tail ||
      event?.failure?.message,
    )
    const message = normalizeMessage(event?.message) || failureMessage || 'No checkpoint message.'
    const checkpointStatus = String(event?.checkpoint_status || '').trim().toLowerCase()
    const badgeStatus = checkpointStatus || normalizeBotStatus(event?.status, '') || 'pending'
    const details = []
    if (failureJson) details.push({ label: 'Failure', tone: 'failure', value: failureJson })
    if (metadataJson) details.push({ label: 'Metadata', tone: 'metadata', value: metadataJson })
    return {
      key: event?.event_id || `${event?.seq || '0'}-${event?.checkpoint_at || event?.created_at || 'event'}`,
      seq: Number(event?.seq || 0),
      owner: formatOwnerLabel(event?.owner || 'system'),
      phase: event?.phase ? formatLifecyclePhaseLabel(event.phase) : 'Unknown',
      message,
      at: formatTimestamp(event?.checkpoint_at || event?.created_at),
      badgeStatus,
      badgeLabel: formatStatusLabel(badgeStatus),
      details,
    }
  })
}

export function buildDiagnosticsViewModel({ botId, runId, lifecycle, diagnostics, loading = false }) {
  const payload = diagnostics && typeof diagnostics === 'object' ? diagnostics : {}
  const events = Array.isArray(payload?.events) ? payload.events : []
  const summary = normalizeSummary(payload, lifecycle)
  return {
    header: {
      title: 'Runtime Diagnostics',
      status: summary.runStatus || normalizeBotStatus(lifecycle?.status, ''),
      statusLabel: formatStatusLabel(summary.runStatus || lifecycle?.status),
      subtitle: buildSubtitle(summary),
      quickFacts: buildQuickFacts(summary),
      eventCountLabel: loading
        ? 'Loading lifecycle evidence…'
        : `${events.length} lifecycle ${events.length === 1 ? 'event' : 'events'}`,
      identifiers: [
        buildIdentifier('Bot ID', 'bot_id', botId),
        buildIdentifier('Run ID', 'run_id', runId),
      ],
    },
    primaryFailure: buildPrimaryFailure(summary),
    finalState: buildFinalState(summary),
    workerFailureSummary: buildWorkerFailureSummary(summary, events),
    lifecycleTrail: {
      title: 'Lifecycle Trail',
      subtitle: 'Supporting lifecycle evidence for the selected run.',
      rows: buildLifecycleEventRows(events),
    },
  }
}

export async function copyDiagnosticsIdentifier({
  copyKey,
  value,
  writeText,
  onCopiedChange,
  scheduleReset,
  resetMs = DIAGNOSTICS_COPY_RESET_MS,
}) {
  const raw = String(value || '').trim()
  if (!raw || typeof writeText !== 'function') return false
  await writeText(raw)
  if (typeof onCopiedChange === 'function') onCopiedChange(copyKey, true)
  if (typeof scheduleReset === 'function') {
    scheduleReset(() => {
      if (typeof onCopiedChange === 'function') onCopiedChange(copyKey, false)
    }, resetMs)
  }
  return true
}
