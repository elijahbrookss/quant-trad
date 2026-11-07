export const DAY_MS = 86_400_000
export const PRESETS = { '6h': 6 * 3600_000, '24h': DAY_MS, '7d': 7 * DAY_MS, '30d': 30 * DAY_MS }
export const METRICS = [
  { id: 'database_allocated_bytes', label: 'Database allocated', color: '#9dbaea', kind: 'stock', basis: 'sampled_database_allocation' },
  { id: 'market_allocated_bytes', label: 'Market schema allocated', color: '#dfb877', kind: 'stock', basis: 'sampled_market_schema_allocation' },
  { id: 'archive_compressed_bytes', label: 'Archived compressed', color: '#9dbaea', kind: 'flow', basis: 'acknowledged_compressed_manifest_bytes' },
  { id: 'verified_first_compressed_bytes', label: 'First verified compressed', color: '#8ac5ac', kind: 'flow', basis: 'first_qualified_verification_compressed_bytes' },
  { id: 'reclaimed_physical_hot_bytes', label: 'Reclaimed physical hot', color: '#dfb877', kind: 'flow', basis: 'reclaimed_physical_partition_bytes' },
]
export function byteLabel(value, signed = false) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'Unavailable'
  const abs = Math.abs(value)
  const power = abs >= 1024 ** 4 ? 4 : abs >= 1024 ** 3 ? 3 : abs >= 1024 ** 2 ? 2 : abs >= 1024 ? 1 : 0
  return `${signed && value > 0 ? '+' : ''}${(value / 1024 ** power).toLocaleString(undefined, { maximumFractionDigits: 2 })} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][power]}`
}
export function exactRange(preset, custom, now = Date.now()) {
  const to = preset === 'custom' ? Date.parse(custom.to) : now
  const from = preset === 'custom' ? Date.parse(custom.from) : to - PRESETS[preset]
  if (!Number.isFinite(from) || !Number.isFinite(to) || from >= to) throw new Error('Choose a valid start before the end.')
  if (to - from > 30 * DAY_MS) throw new Error('Choose a range of 30 days or less.')
  if (to > now) throw new Error('The end must not be in the future.')
  return { from: new Date(from).toISOString(), to: new Date(to).toISOString(), resolution: 'auto' }
}
export function validateMetrics(payload) {
  const range = payload?.range
  const from = Date.parse(range?.from)
  const to = Date.parse(range?.to)
  if (payload?.schema_version !== 'qt.storage_metrics.v1' || !Number.isFinite(from) || !Number.isFinite(to)
    || from >= to || to - from > 30 * DAY_MS || !Number.isFinite(range.bucket_seconds)
    || range.bucket_seconds <= 0 || Math.ceil((to - from) / (range.bucket_seconds * 1000)) > 720
    || !Number.isFinite(payload.cadence_seconds) || payload.cadence_seconds <= 0
    || !Number.isFinite(Date.parse(payload.generated_at)) || !Array.isArray(payload.series)) {
    throw new Error('Storage trends response has an invalid range or contract.')
  }
  for (const metric of METRICS) {
    const matches = payload.series.filter((entry) => entry.metric_id === metric.id)
    const series = matches[0]
    if (matches.length > 1 || (series && (series.unit !== 'bytes' || series.basis !== metric.basis
      || !Array.isArray(series.points) || series.points.length > 720))) {
      throw new Error('Storage trends response has an invalid metric basis or point bound.')
    }
  }
  return payload
}
export function seriesState(series) {
  return ['ready', 'partial', 'unavailable', 'stale'].includes(series?.state) ? series.state : 'unavailable'
}
export function finitePoint(point) {
  return point?.value !== null && Number.isFinite(point?.value)
    && !['unavailable', 'stale', 'error'].includes(point?.state)
}
export function chartPoints(series, range, cadence) {
  const from = Date.parse(range.from) / 1000
  const to = Date.parse(range.to) / 1000
  const step = Math.max(range.bucket_seconds, cadence)
  const stock = METRICS.some((metric) => metric.id === series?.metric_id && metric.kind === 'stock')
  const data = []
  let previous = null
  for (const point of [...(series?.points || [])].sort((a, b) => Date.parse(a.time) - Date.parse(b.time))) {
    const bucket = Date.parse(point.time) / 1000
    const observed = Date.parse(point.observed_at) / 1000
    const time = stock && finitePoint(point) && Number.isFinite(observed) ? observed : bucket
    if (!Number.isFinite(bucket) || bucket < from || bucket >= to || !Number.isFinite(time)
      || time < from || time >= to || (previous !== null && time <= previous)) continue
    const breakBefore = (previous !== null && time - previous > 2 * step)
      || (stock && (point.reason === 'raw_sample_gap' || point.max_gap_seconds > 2 * cadence))
    data.push(finitePoint(point) ? { time, value: point.value, breakBefore } : { time })
    previous = time
  }
  return data
}
// Separate line series prevent a renderer from bridging missing samples.
export function lineSegments(points) {
  const segments = []
  let current = null
  for (const point of points) {
    if (point.breakBefore) current = null
    if (!Number.isFinite(point.value)) { current = null; continue }
    if (!current) { current = []; segments.push(current) }
    current.push({ time: point.time, value: point.value })
  }
  return segments
}
export function grafanaUrl(payload) {
  const link = payload?.links?.grafana
  if (link?.state !== 'available' || !link.dashboard_url) return null
  try {
    const url = new URL(link.dashboard_url)
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) return null
    const from = Date.parse(payload.range?.from)
    const to = Date.parse(payload.range?.to)
    if (!Number.isFinite(from) || !Number.isFinite(to) || from >= to) return null
    url.search = ''
    url.hash = ''
    url.searchParams.set('from', String(from))
    url.searchParams.set('to', String(to))
    url.searchParams.set('timezone', 'utc')
    return url.toString()
  } catch { return null }
}
export function responseStale(payload, live, now = Date.now()) {
  return !!payload && live && now - Date.parse(payload.generated_at) > 3 * payload.cadence_seconds * 1000
}
export function tableRows(payload) {
  const rows = new Map()
  for (const metric of METRICS) {
    const series = payload.series.find((entry) => entry.metric_id === metric.id)
    for (const point of series?.points || []) {
      const time = Date.parse(point.time)
      if (!Number.isFinite(time) || time < Date.parse(payload.range.from) || time >= Date.parse(payload.range.to)) continue
      if (!rows.has(time)) rows.set(time, { time, values: {}, observed: {} })
      rows.get(time).values[metric.id] = finitePoint(point) ? point.value : null
      rows.get(time).observed[metric.id] = point.observed_at || null
    }
  }
  return [...rows.values()].sort((a, b) => a.time - b.time)
}
