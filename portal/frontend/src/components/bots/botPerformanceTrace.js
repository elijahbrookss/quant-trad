function readPath(source, path) {
  let current = source
  for (const segment of path) {
    if (!current || typeof current !== 'object') return null
    current = current[segment]
  }
  return current ?? null
}

function firstFinite(values) {
  for (const value of values) {
    const numeric = Number(value)
    if (Number.isFinite(numeric)) return numeric
  }
  return null
}

function normalizePoint(entry, index, valueKeys = []) {
  if (Number.isFinite(Number(entry))) {
    return { x: index, y: Number(entry) }
  }
  if (!entry || typeof entry !== 'object') return null

  const y = firstFinite([
    ...valueKeys.map((key) => entry[key]),
    entry.value,
    entry.balance,
    entry.equity,
    entry.net_pnl,
    entry.pnl,
    entry.wallet,
    entry.close,
  ])
  if (!Number.isFinite(y)) return null

  const timeCandidate = entry.time ?? entry.timestamp ?? entry.at ?? entry.ts ?? entry.seq ?? entry.index ?? index
  const numericTime =
    typeof timeCandidate === 'string'
      ? Date.parse(timeCandidate)
      : Number.isFinite(Number(timeCandidate))
        ? Number(timeCandidate)
        : index

  return {
    x: Number.isFinite(numericTime) ? numericTime : index,
    y,
  }
}

function dedupeAndSample(points, maxPoints = 32) {
  if (!Array.isArray(points) || points.length === 0) return []
  const deduped = []
  for (const point of points) {
    const last = deduped[deduped.length - 1]
    if (last && last.x === point.x) {
      deduped[deduped.length - 1] = point
    } else {
      deduped.push(point)
    }
  }
  if (deduped.length <= maxPoints) return deduped

  const stride = Math.max(1, Math.ceil(deduped.length / maxPoints))
  const sampled = deduped.filter((_, index) => index % stride === 0)
  const last = deduped[deduped.length - 1]
  if (sampled[sampled.length - 1] !== last) sampled.push(last)
  return sampled.slice(-maxPoints)
}

function normalizeSeries(raw, valueKeys = []) {
  if (!Array.isArray(raw)) return []
  const points = raw
    .map((entry, index) => normalizePoint(entry, index, valueKeys))
    .filter(Boolean)
    .sort((a, b) => a.x - b.x)
  return dedupeAndSample(points)
}

function traceCandidate(bot, candidate) {
  const raw = readPath(bot, candidate.path)
  const points = normalizeSeries(raw, candidate.valueKeys)
  if (!points.length) return null

  const first = points[0]?.y ?? 0
  const last = points[points.length - 1]?.y ?? 0
  const delta = last - first
  return {
    kind: 'series',
    source: candidate.source,
    label: candidate.label,
    points,
    latestValue: last,
    trend: delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat',
  }
}

function placeholderLabel(statusKey) {
  if (statusKey === 'starting') return 'Trace pending'
  if (statusKey === 'running' || statusKey === 'degraded' || statusKey === 'paused') return 'Awaiting live trace'
  if (statusKey === 'crashed' || statusKey === 'failed_start') return 'No last trace'
  return 'No performance trace'
}

const TRACE_CANDIDATES = [
  // Fleet control only consumes a compact trace if the bot payload already carries it.
  { path: ['runtime', 'stats', 'balance_trace'], source: 'wallet', label: 'Wallet', valueKeys: ['balance'] },
  { path: ['runtime', 'balance_trace'], source: 'wallet', label: 'Wallet', valueKeys: ['balance'] },
  { path: ['runtime', 'wallet_trace'], source: 'wallet', label: 'Wallet', valueKeys: ['balance', 'wallet', 'value'] },
  { path: ['runtime', 'wallet_curve'], source: 'wallet', label: 'Wallet', valueKeys: ['value', 'balance', 'wallet'] },
  { path: ['runtime', 'equity_curve'], source: 'equity', label: 'Equity', valueKeys: ['value', 'equity'] },
  { path: ['last_run_artifact', 'charts', 'equity_curve'], source: 'equity', label: 'Equity', valueKeys: ['value', 'equity'] },
  { path: ['last_run_artifact', 'equity_curve'], source: 'equity', label: 'Equity', valueKeys: ['value', 'equity'] },
  { path: ['runtime', 'net_pnl_series'], source: 'net_pnl', label: 'Net P&L', valueKeys: ['value', 'net_pnl', 'pnl'] },
  { path: ['last_run_artifact', 'charts', 'net_pnl_curve'], source: 'net_pnl', label: 'Net P&L', valueKeys: ['value', 'net_pnl', 'pnl'] },
  { path: ['last_run_artifact', 'net_pnl_curve'], source: 'net_pnl', label: 'Net P&L', valueKeys: ['value', 'net_pnl', 'pnl'] },
]

export function getBotPerformanceTrace(bot, { statusKey = 'stopped' } = {}) {
  for (const candidate of TRACE_CANDIDATES) {
    const trace = traceCandidate(bot, candidate)
    if (trace) {
      return {
        ...trace,
        quoteCurrency:
          String(bot?.runtime?.stats?.quote_currency || bot?.last_stats?.quote_currency || '').trim() || null,
      }
    }
  }

  return {
    kind: 'placeholder',
    label: placeholderLabel(statusKey),
    source: null,
    points: [],
    latestValue: null,
    trend: 'flat',
    quoteCurrency: null,
  }
}
