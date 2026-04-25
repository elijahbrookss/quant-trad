export const LIFECYCLE_STATES = new Set([
  'starting',
  'running',
  'completed',
  'failed',
  'stopped',
  'cancelled',
  'unknown',
])

export const HEALTH_STATES = new Set(['ok', 'warning', 'critical', 'unknown'])
export const REPORT_STATUSES = new Set(['unknown', 'not_started', 'preparing', 'ready', 'failed', 'unavailable', 'stale'])
export const COMPARISON_STATUSES = new Set(['unknown', 'eligible', 'blocked', 'not_applicable'])
export const ERROR_SEVERITIES = new Set(['info', 'warning', 'critical', 'unknown'])
export const ERROR_CATEGORIES = new Set(['startup', 'configuration', 'runtime', 'reporting', 'network', 'storage', 'unknown'])

const ERROR_COPY = {
  DUPLICATE_RUN: {
    title: 'Duplicate run',
    message: 'The backend reported that this run already exists.',
    category: 'runtime',
    severity: 'warning',
  },
  INVALID_CONFIG: {
    title: 'Invalid configuration',
    message: 'The backend rejected the run configuration.',
    category: 'configuration',
    severity: 'critical',
  },
  STARTUP_FAILED: {
    title: 'Startup failed',
    message: 'The runtime did not complete startup.',
    category: 'startup',
    severity: 'critical',
  },
  RESULTS_NOT_READY: {
    title: 'Report unavailable',
    message: 'The backend reported that report results are not ready.',
    category: 'reporting',
    severity: 'info',
  },
  RUN_NOT_FOUND: {
    title: 'Run not found',
    message: 'The backend could not find this run.',
    category: 'runtime',
    severity: 'warning',
  },
}

function normalizeToken(value) {
  return String(value || '').trim().toLowerCase().replace(/[\s-]+/g, '_')
}

function normalizeKnown(value, allowed, fallback = 'unknown') {
  const normalized = normalizeToken(value)
  return allowed.has(normalized) ? normalized : fallback
}

export function normalizeLifecycleState(value) {
  const normalized = normalizeToken(value)
  if (!normalized) return 'unknown'
  if (['queued', 'pending', 'booting', 'initializing', 'preparing', 'start_requested'].includes(normalized)) return 'starting'
  if (['live', 'active', 'degraded', 'paused'].includes(normalized)) return 'running'
  if (['done', 'finished', 'success', 'succeeded'].includes(normalized)) return 'completed'
  if (['error', 'failed_start', 'startup_failed', 'crashed', 'dead'].includes(normalized)) return 'failed'
  if (['stop', 'stopping', 'terminated'].includes(normalized)) return 'stopped'
  if (['cancelled', 'canceled'].includes(normalized)) return 'cancelled'
  return normalizeKnown(normalized, LIFECYCLE_STATES)
}

export function normalizeHealthState(value) {
  const normalized = normalizeToken(value)
  if (!normalized) return 'unknown'
  if (['healthy', 'good', 'green'].includes(normalized)) return 'ok'
  if (['warn', 'degraded', 'yellow'].includes(normalized)) return 'warning'
  if (['error', 'failed', 'red'].includes(normalized)) return 'critical'
  return normalizeKnown(normalized, HEALTH_STATES)
}

export function normalizeReportStatus(value) {
  return normalizeKnown(value, REPORT_STATUSES)
}

export function normalizeComparisonStatus(value) {
  return normalizeKnown(value, COMPARISON_STATUSES)
}

function firstString(values) {
  for (const value of values) {
    const text = String(value || '').trim()
    if (text) return text
  }
  return ''
}

function firstFiniteNumber(values) {
  for (const value of values) {
    const numeric = Number(value)
    if (Number.isFinite(numeric)) return numeric
  }
  return null
}

function readPath(source, path) {
  let cursor = source
  for (const part of path) {
    if (!cursor || typeof cursor !== 'object' || !(part in cursor)) return undefined
    cursor = cursor[part]
  }
  return cursor
}

function normalizeMode(value) {
  const normalized = normalizeToken(value)
  if (['backtest', 'paper', 'live'].includes(normalized)) return normalized
  if (['paper_trade', 'sim_trade'].includes(normalized)) return 'paper'
  return 'unknown'
}

function normalizeErrorSeverity(value) {
  const normalized = normalizeToken(value)
  if (normalized === 'error' || normalized === 'danger' || normalized === 'fatal') return 'critical'
  if (normalized === 'warn') return 'warning'
  return normalizeKnown(normalized, ERROR_SEVERITIES)
}

function normalizeErrorCategory(value) {
  return normalizeKnown(value, ERROR_CATEGORIES)
}

function extractBackendError(apiRun) {
  const candidates = [
    ['primary_error'],
    ['error'],
    ['failure'],
    ['lifecycle', 'failure'],
    ['last_run_artifact', 'error'],
  ]

  for (const path of candidates) {
    let cursor = apiRun
    let exists = true
    for (const part of path) {
      if (!cursor || typeof cursor !== 'object' || !Object.prototype.hasOwnProperty.call(cursor, part)) {
        exists = false
        break
      }
      cursor = cursor[part]
    }
    if (!exists || cursor == null || cursor === '') continue
    if (typeof cursor === 'object' && !Array.isArray(cursor) && Object.keys(cursor).length === 0) continue
    return cursor
  }

  return null
}

export function mapErrorToViewModel(rawError) {
  if (!rawError) return null

  const isObject = rawError && typeof rawError === 'object'
  const code = firstString([
    isObject ? rawError.code : '',
    isObject ? rawError.error_code : '',
    isObject ? rawError.type : '',
  ])
  const known = ERROR_COPY[code] || null
  const rawMessage = isObject ? firstString([rawError.message, rawError.detail, rawError.reason]) : String(rawError || '').trim()

  return {
    code,
    title: firstString([isObject ? rawError.title : '', known?.title]) || 'Unexpected error',
    message:
      firstString([rawMessage, known?.message]) ||
      'The backend returned an untyped error for this run.',
    severity: normalizeErrorSeverity(isObject ? rawError.severity : known?.severity) || known?.severity || 'unknown',
    category: normalizeErrorCategory(isObject ? rawError.category : known?.category) || known?.category || 'unknown',
    raw: rawError,
  }
}

export function mapRunToViewModel(apiRun = {}, options = {}) {
  const strategy = options.strategy || null
  const display = options.display || null
  const lifecycleState = normalizeLifecycleState(
    apiRun.lifecycle_state ??
      apiRun.lifecycleState ??
      apiRun.lifecycle?.status ??
      apiRun.run?.status ??
      apiRun.status ??
      display?.statusKey,
  )

  return {
    botId: firstString([apiRun.bot_id, apiRun.id]),
    runId: firstString([
      apiRun.run_id,
      apiRun.active_run_id,
      apiRun.lifecycle?.telemetry?.run_id,
      apiRun.runtime?.run_id,
      display?.runId,
    ]) || null,
    name: firstString([apiRun.name, apiRun.bot_name, apiRun.id]) || 'Bot',
    strategyName: firstString([apiRun.strategy_name, strategy?.name, apiRun.strategy_id]) || null,
    timeframe: firstString([apiRun.timeframe, strategy?.timeframe]) || null,
    mode: normalizeMode(apiRun.mode || apiRun.run_type),
    lifecycleState,
    healthState: normalizeHealthState(apiRun.health_state ?? apiRun.healthState),
    reportStatus: normalizeReportStatus(apiRun.report_status ?? apiRun.reportStatus),
    comparisonStatus: normalizeComparisonStatus(apiRun.comparison_status ?? apiRun.comparisonStatus),
    pnl: firstFiniteNumber([
      apiRun.pnl,
      apiRun.net_pnl,
      readPath(apiRun, ['runtime', 'stats', 'net_pnl']),
      readPath(apiRun, ['run', 'summary', 'net_pnl']),
      readPath(apiRun, ['last_stats', 'net_pnl']),
      readPath(apiRun, ['last_run_artifact', 'summary', 'net_pnl']),
      readPath(apiRun, ['last_run_artifact', 'stats', 'net_pnl']),
    ]),
    totalTrades: firstFiniteNumber([
      apiRun.total_trades,
      readPath(apiRun, ['runtime', 'stats', 'total_trades']),
      readPath(apiRun, ['run', 'summary', 'total_trades']),
      readPath(apiRun, ['last_stats', 'total_trades']),
      readPath(apiRun, ['last_run_artifact', 'summary', 'total_trades']),
    ]),
    openTrades: firstFiniteNumber([
      apiRun.open_trades,
      readPath(apiRun, ['lifecycle', 'telemetry', 'trade_count']),
    ]),
    warningsCount: Math.max(
      0,
      Number(apiRun.warning_count ?? apiRun.warnings_count ?? apiRun.lifecycle?.telemetry?.warning_count ?? 0) || 0,
      Number(Array.isArray(apiRun.runtime?.warnings) ? apiRun.runtime.warnings.length : 0) || 0,
    ),
    primaryError: mapErrorToViewModel(extractBackendError(apiRun)),
  }
}
