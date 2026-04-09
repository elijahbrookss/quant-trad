import { toFiniteNumber, toSec } from './chartDataUtils.js'

export function canonicalSeriesKey(instrumentId, timeframe) {
  const normalizedInstrumentId = String(instrumentId || '').trim()
  const normalizedTimeframe = String(timeframe || '').trim().toLowerCase()
  if (!normalizedInstrumentId || !normalizedTimeframe) return ''
  return `${normalizedInstrumentId}|${normalizedTimeframe}`
}

export function normalizeSeriesKey(value) {
  const text = String(value || '').trim()
  if (!text) return ''
  const [instrumentId, timeframe, ...rest] = text.split('|')
  if (rest.length || !text.includes('|')) return ''
  return canonicalSeriesKey(instrumentId, timeframe)
}

export function canonicalSeriesKeyFromEntry(entry) {
  if (!entry || typeof entry !== 'object') return ''
  const explicit = String(entry.series_key || '').trim()
  if (explicit) return normalizeSeriesKey(explicit)
  const instrumentId = String(entry.instrument_id || entry?.instrument?.id || '').trim()
  return canonicalSeriesKey(instrumentId, entry.timeframe)
}

export function normalizeCandleTime(value) {
  const epoch = toSec(value)
  return Number.isFinite(epoch) ? Math.floor(epoch) : null
}

export function normalizeCandle(candle) {
  if (!candle || typeof candle !== 'object') return null
  const time = normalizeCandleTime(candle.time)
  if (!Number.isFinite(time)) return null
  const normalized = { ...candle, time }
  for (const key of ['open', 'high', 'low', 'close']) {
    if (!(key in normalized)) continue
    const numeric = toFiniteNumber(normalized[key])
    if (!Number.isFinite(numeric)) return null
    normalized[key] = numeric
  }
  return normalized
}

export function mergeCanonicalCandles(...streams) {
  const byTime = new Map()
  streams.forEach((stream) => {
    ;(Array.isArray(stream) ? stream : []).forEach((candle) => {
      const normalized = normalizeCandle(candle)
      if (!normalized) return
      byTime.set(normalized.time, normalized)
    })
  })
  return Array.from(byTime.entries())
    .sort((left, right) => left[0] - right[0])
    .map((entry) => entry[1])
}

export function validateCanonicalCandles(candles) {
  let previous = null
  for (let index = 0; index < (Array.isArray(candles) ? candles.length : 0); index += 1) {
    const current = candles[index]
    const time = normalizeCandleTime(current?.time)
    if (!Number.isFinite(time)) {
      return { index, prev: previous, current: current?.time, reason: 'invalid_time' }
    }
    if (previous !== null && time <= previous) {
      return { index, prev: previous, current: time, reason: 'non_increasing_time' }
    }
    previous = time
  }
  return null
}

function stableOverlayRevision(value) {
  return JSON.stringify(sortValue(value))
}

function sortValue(value) {
  if (Array.isArray(value)) return value.map(sortValue)
  if (value && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce((acc, key) => {
        acc[key] = sortValue(value[key])
        return acc
      }, {})
  }
  return value
}

function overlayIdentity(overlay, index) {
  if (!overlay || typeof overlay !== 'object') return `index:${index}`
  const explicitOverlayId = String(overlay.overlay_id || '').trim()
  if (explicitOverlayId) return explicitOverlayId
  for (const key of ['id', 'name', 'key', 'slug', 'indicator_id', 'type']) {
    const value = String(overlay[key] || '').trim()
    if (value) return `${key}:${value}`
  }
  return `index:${index}`
}

export function projectOverlayState(overlays = []) {
  const projected = new Map()
  ;(Array.isArray(overlays) ? overlays : []).forEach((overlay, index) => {
    if (!overlay || typeof overlay !== 'object') return
    const overlayId = overlayIdentity(overlay, index)
    projected.set(overlayId, {
      ...overlay,
      overlay_id: overlayId,
      overlay_revision: stableOverlayRevision({ ...overlay, overlay_id: overlayId }),
    })
  })
  return Array.from(projected.values())
}

export function normalizeSeriesEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const instrumentId = String(entry.instrument_id || entry?.instrument?.id || '').trim()
  const symbol = String(entry.symbol || '').trim().toUpperCase()
  const timeframe = String(entry.timeframe || '').trim().toLowerCase()
  const seriesKey = canonicalSeriesKey(instrumentId, timeframe)
  if (!seriesKey) return null
  return {
    ...entry,
    instrument_id: instrumentId,
    symbol,
    timeframe,
    series_key: seriesKey,
    candles: mergeCanonicalCandles(entry.candles || []),
    overlays: projectOverlayState(entry.overlays || []),
    stats: entry.stats && typeof entry.stats === 'object' ? { ...entry.stats } : {},
  }
}

export function normalizeProjection(projection, { runId = null, seq = 0, seriesKey = null } = {}) {
  const source = projection && typeof projection === 'object' ? projection : {}
  const seriesByKey = new Map()
  ;(Array.isArray(source.series) ? source.series : []).forEach((entry, index) => {
    const normalized = normalizeSeriesEntry(entry, index)
    if (!normalized) return
    seriesByKey.set(normalized.series_key, normalized)
  })
  return {
    run_id: String(source.run_id || runId || '').trim() || null,
    seq: Number(source.seq ?? seq ?? 0) || 0,
    series_key: normalizeSeriesKey(source.series_key || seriesKey || '') || null,
    series: Array.from(seriesByKey.values()),
    trades: Array.isArray(source.trades) ? source.trades.filter((entry) => entry && typeof entry === 'object').map((entry) => ({ ...entry })) : [],
    logs: Array.isArray(source.logs) ? [...source.logs] : [],
    decisions: Array.isArray(source.decisions) ? [...source.decisions] : [],
    warnings: Array.isArray(source.warnings) ? [...source.warnings] : [],
    runtime: source.runtime && typeof source.runtime === 'object' ? { ...source.runtime } : {},
  }
}

export function findProjectionSeries(projection, seriesKey) {
  const target = normalizeSeriesKey(seriesKey)
  return (Array.isArray(projection?.series) ? projection.series : []).find((entry) => canonicalSeriesKeyFromEntry(entry) === target) || null
}

function replaceProjectionSeries(projection, nextSeries) {
  const target = canonicalSeriesKeyFromEntry(nextSeries)
  const existing = Array.isArray(projection?.series) ? projection.series : []
  let replaced = false
  const next = existing.map((entry) => {
    if (canonicalSeriesKeyFromEntry(entry) !== target) return entry
    replaced = true
    return nextSeries
  })
  if (!replaced) next.push(nextSeries)
  return next
}

export function buildProjectionFromWindow({ runId, seq, seriesKey, window }) {
  const sourceWindow = window && typeof window === 'object' ? window : {}
  if (sourceWindow.projection && typeof sourceWindow.projection === 'object') {
    return normalizeProjection(sourceWindow.projection, { runId, seq, seriesKey })
  }
  const selectedSeries = sourceWindow.selected_series && typeof sourceWindow.selected_series === 'object'
    ? sourceWindow.selected_series
    : {
        instrument_id: String(seriesKey || '').split('|')[0] || '',
        timeframe: String(seriesKey || '').split('|')[1] || '',
        candles: Array.isArray(sourceWindow.candles) ? sourceWindow.candles : [],
        overlays: [],
        stats: {},
      }
  return normalizeProjection(
    {
      run_id: runId,
      seq,
      series_key: seriesKey,
      series: [selectedSeries],
      trades: sourceWindow.trades || [],
      logs: sourceWindow.logs || [],
      decisions: sourceWindow.decisions || [],
      warnings: sourceWindow.warnings || [],
      runtime: sourceWindow.runtime || { status: sourceWindow.status || 'running' },
    },
    { runId, seq, seriesKey },
  )
}

export function applyHistoryPage({ projection, seriesKey, candles }) {
  const current = normalizeProjection(projection)
  const series = findProjectionSeries(current, seriesKey)
  if (!series) return current
  const nextSeries = normalizeSeriesEntry(
    {
      ...series,
      candles: mergeCanonicalCandles(candles || [], series.candles || []),
    },
    0,
  )
  const runtime = { ...(current.runtime || {}) }
  if (Array.isArray(nextSeries?.candles) && nextSeries.candles.length > 0) {
    runtime.last_bar = { ...nextSeries.candles[nextSeries.candles.length - 1] }
  }
  return {
    ...current,
    runtime,
    series: replaceProjectionSeries(current, nextSeries),
  }
}
