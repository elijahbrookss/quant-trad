import { toFiniteNumber, toSec } from './chartDataUtils.js'
import { createLogger } from '../../utils/logger.js'

const DEFAULT_DETAIL_CACHE_LIMIT = 6
const MAX_LOGS = 300
const MAX_DECISIONS = 600
const MAX_TRADES = 240
const DETAIL_DELTA_DROP_WARN_INTERVAL_MS = 10000
const logger = createLogger('botlensProjection')
const detailDeltaDropWarnings = new Map()

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

export function applyOverlayDelta(overlays = [], overlayDelta = {}) {
  const current = projectOverlayState(overlays)
  const overlayMap = new Map(current.map((overlay) => [overlay.overlay_id, overlay]))
  const ops = Array.isArray(overlayDelta?.ops) ? overlayDelta.ops : []
  ops.forEach((op) => {
    if (!op || typeof op !== 'object') return
    const opName = String(op.op || '').trim().toLowerCase()
    const key = String(op.key || '').trim()
    if (!key) return
    if (opName === 'remove') {
      overlayMap.delete(key)
      return
    }
    if (opName !== 'upsert' || !op.overlay || typeof op.overlay !== 'object') return
    overlayMap.set(key, {
      ...op.overlay,
      overlay_id: key,
      overlay_revision: stableOverlayRevision({ ...op.overlay, overlay_id: key }),
    })
  })
  return Array.from(overlayMap.values())
}

function upsertTail(entries, item, keyFields, limit) {
  const ordered = new Map()
  ;(Array.isArray(entries) ? entries : []).forEach((entry) => {
    if (!entry || typeof entry !== 'object') return
    const key = keyFields.map((field) => entry[field]).find(Boolean)
    if (!key) return
    ordered.set(String(key), entry)
  })
  const itemKey = keyFields.map((field) => item[field]).find(Boolean)
  if (itemKey) ordered.set(String(itemKey), item)
  const values = Array.from(ordered.values())
  if (values.length <= limit) return values
  return values.slice(-limit)
}

function normalizeSummary(summary) {
  if (!summary || typeof summary !== 'object') return null
  const symbolKey = normalizeSeriesKey(summary.symbol_key || '')
  if (!symbolKey) return null
  return {
    ...summary,
    symbol_key: symbolKey,
    instrument_id: String(summary.instrument_id || '').trim() || null,
    symbol: String(summary.symbol || '').trim().toUpperCase() || null,
    timeframe: String(summary.timeframe || '').trim().toLowerCase() || null,
  }
}

function normalizeTrade(trade) {
  if (!trade || typeof trade !== 'object') return null
  const tradeId = String(trade.trade_id || trade.id || '').trim()
  if (!tradeId) return null
  return {
    ...trade,
    trade_id: tradeId,
    symbol_key: normalizeSeriesKey(trade.symbol_key || ''),
  }
}

function warningSeverityRank(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (normalized === 'error' || normalized === 'critical') return 0
  if (normalized === 'warn' || normalized === 'warning') return 1
  return 2
}

function normalizeWarning(warning, index = 0) {
  if (!warning || typeof warning !== 'object') return null
  const warningType = String(warning.warning_type || warning.type || '').trim()
  if (!warningType) return null
  const indicatorId = String(warning.indicator_id || warning.context?.indicator_id || '').trim() || null
  const symbolKey = normalizeSeriesKey(warning.symbol_key || warning.context?.symbol_key || '')
  const symbol = String(warning.symbol || warning.context?.symbol || '').trim().toUpperCase() || null
  const timeframe = String(warning.timeframe || warning.context?.timeframe || '').trim().toLowerCase() || null
  const warningId = String(warning.warning_id || warning.id || '').trim()
    || [warningType, indicatorId || `idx:${index}`, symbolKey || symbol || '', timeframe || ''].filter(Boolean).join('::')
  return {
    ...warning,
    warning_id: warningId,
    id: warningId,
    warning_type: warningType,
    severity: String(warning.severity || warning.level || 'warning').trim().toLowerCase() || 'warning',
    indicator_id: indicatorId,
    symbol_key: symbolKey || null,
    symbol,
    timeframe,
    title: String(warning.title || '').trim() || null,
    message: String(warning.message || '').trim() || 'Runtime warning',
    count: Math.max(1, Number(warning.count || 1) || 1),
    first_seen_at: warning.first_seen_at || warning.timestamp || null,
    last_seen_at: warning.last_seen_at || warning.updated_at || warning.timestamp || null,
    context: warning.context && typeof warning.context === 'object' ? { ...warning.context } : {},
  }
}

function normalizeHealth(health) {
  if (!health || typeof health !== 'object') return {}
  const warnings = (Array.isArray(health.warnings) ? health.warnings : [])
    .map((warning, index) => normalizeWarning(warning, index))
    .filter(Boolean)
    .sort((left, right) => {
      const severityDelta = warningSeverityRank(left.severity) - warningSeverityRank(right.severity)
      if (severityDelta !== 0) return severityDelta
      const leftSeen = Date.parse(left.last_seen_at || left.first_seen_at || '') || 0
      const rightSeen = Date.parse(right.last_seen_at || right.first_seen_at || '') || 0
      if (leftSeen !== rightSeen) return rightSeen - leftSeen
      return Number(right.count || 0) - Number(left.count || 0)
    })
  return {
    ...health,
    warning_count: Math.max(Number(health.warning_count || 0) || 0, warnings.length),
    warnings,
  }
}

export function normalizeDetail(detail, { symbolKey = null, seq = 0 } = {}) {
  const source = detail && typeof detail === 'object' ? detail : {}
  const normalizedSymbolKey = normalizeSeriesKey(source.symbol_key || symbolKey || '')
  const [instrumentId, timeframe] = normalizedSymbolKey.split('|')
  return {
    symbol_key: normalizedSymbolKey,
    instrument_id: String(source.instrument_id || instrumentId || '').trim(),
    symbol: String(source.symbol || '').trim().toUpperCase(),
    timeframe: String(source.timeframe || timeframe || '').trim().toLowerCase(),
    display_label: String(source.display_label || '').trim() || null,
    status: String(source.status || 'waiting').trim(),
    last_event_at: source.last_event_at || null,
    continuity: source.continuity && typeof source.continuity === 'object' ? { ...source.continuity } : {},
    seq: Number(source.seq ?? seq ?? 0) || 0,
    candles: mergeCanonicalCandles(source.candles || []),
    overlays: projectOverlayState(source.overlays || []),
    recent_trades: (Array.isArray(source.recent_trades) ? source.recent_trades : [])
      .map((entry) => normalizeTrade(entry))
      .filter(Boolean),
    logs: Array.isArray(source.logs) ? source.logs.filter((entry) => entry && typeof entry === 'object').map((entry) => ({ ...entry })) : [],
    decisions: Array.isArray(source.decisions) ? source.decisions.filter((entry) => entry && typeof entry === 'object').map((entry) => ({ ...entry })) : [],
    stats: source.stats && typeof source.stats === 'object' ? { ...source.stats } : {},
    runtime: source.runtime && typeof source.runtime === 'object' ? { ...source.runtime } : {},
  }
}

function touchCacheOrder(order = [], symbolKey, limit = DEFAULT_DETAIL_CACHE_LIMIT) {
  const next = order.filter((entry) => entry !== symbolKey)
  if (symbolKey) next.unshift(symbolKey)
  return next.slice(0, Math.max(1, limit))
}

function trimDetailCache(detailCache, order, selectedSymbolKey, limit = DEFAULT_DETAIL_CACHE_LIMIT) {
  const allowed = new Set((order || []).slice(0, Math.max(1, limit)))
  if (selectedSymbolKey) allowed.add(selectedSymbolKey)
  const next = {}
  Object.entries(detailCache || {}).forEach(([key, value]) => {
    if (allowed.has(key)) next[key] = value
  })
  return next
}

function warnDroppedDetailDelta(symbolKey, message, store) {
  const now = Date.now()
  const last = Number(detailDeltaDropWarnings.get(symbolKey) || 0)
  if (now - last < DETAIL_DELTA_DROP_WARN_INTERVAL_MS) return
  detailDeltaDropWarnings.set(symbolKey, now)
  logger.warn('botlens_detail_delta_dropped_missing_base', {
    symbol_key: symbolKey,
    seq: Number(message?.seq || 0),
    detail_seq: Number(message?.payload?.detail_seq || 0),
    selected_symbol_key: normalizeSeriesKey(store?.selectedSymbolKey || ''),
    detail_cache_size: Object.keys(store?.detailCache || {}).length,
  })
}

export function createRunStore(session, { detailCacheLimit = DEFAULT_DETAIL_CACHE_LIMIT } = {}) {
  const summaries = Array.isArray(session?.symbol_summaries) ? session.symbol_summaries : []
  const symbolIndex = summaries.reduce((acc, summary) => {
    const normalized = normalizeSummary(summary)
    if (!normalized) return acc
    acc[normalized.symbol_key] = normalized
    return acc
  }, {})
  const openTrades = Array.isArray(session?.open_trades) ? session.open_trades : []
  const openTradesIndex = openTrades.reduce((acc, trade) => {
    const normalized = normalizeTrade(trade)
    if (!normalized) return acc
    acc[normalized.trade_id] = normalized
    return acc
  }, {})
  const selectedSymbolKey = normalizeSeriesKey(session?.selected_symbol_key || '')
  const detail = session?.detail && typeof session.detail === 'object'
    ? normalizeDetail(session.detail, { symbolKey: selectedSymbolKey, seq: Number(session?.seq || 0) || 0 })
    : null
  const detailCache = detail && detail.symbol_key ? { [detail.symbol_key]: detail } : {}
  const detailCacheOrder = detail ? [detail.symbol_key] : []
  return {
    schemaVersion: Number(session?.schema_version || 4) || 4,
    seq: Number(session?.seq || 0) || 0,
    runMeta: session?.run_meta && typeof session.run_meta === 'object' ? { ...session.run_meta } : null,
    lifecycle: session?.lifecycle && typeof session.lifecycle === 'object' ? { ...session.lifecycle } : {},
    health: normalizeHealth(session?.health),
    symbolIndex,
    openTradesIndex,
    detailCache,
    detailCacheOrder,
    selectedSymbolKey: selectedSymbolKey || null,
    detailCacheLimit: Math.max(1, Number(detailCacheLimit) || DEFAULT_DETAIL_CACHE_LIMIT),
  }
}

export function applySummaryDelta(store, message) {
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const nextSymbolIndex = { ...(store?.symbolIndex || {}) }
  ;(Array.isArray(payload.symbol_upserts) ? payload.symbol_upserts : []).forEach((summary) => {
    const normalized = normalizeSummary(summary)
    if (!normalized) return
    nextSymbolIndex[normalized.symbol_key] = {
      ...(nextSymbolIndex[normalized.symbol_key] || {}),
      ...normalized,
    }
  })
  ;(Array.isArray(payload.symbol_removals) ? payload.symbol_removals : []).forEach((symbolKey) => {
    delete nextSymbolIndex[normalizeSeriesKey(symbolKey)]
  })
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.seq || 0)),
    health: payload.health && typeof payload.health === 'object'
      ? normalizeHealth({ ...(store?.health || {}), ...payload.health })
      : store.health,
    lifecycle: payload.lifecycle && typeof payload.lifecycle === 'object' ? { ...store.lifecycle, ...payload.lifecycle } : store.lifecycle,
    symbolIndex: nextSymbolIndex,
  }
}

export function applyOpenTradesDelta(store, message) {
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const openTradesIndex = { ...(store?.openTradesIndex || {}) }
  ;(Array.isArray(payload.upserts) ? payload.upserts : []).forEach((trade) => {
    const normalized = normalizeTrade(trade)
    if (!normalized) return
    openTradesIndex[normalized.trade_id] = normalized
  })
  ;(Array.isArray(payload.removals) ? payload.removals : []).forEach((tradeId) => {
    delete openTradesIndex[String(tradeId)]
  })
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.seq || 0)),
    openTradesIndex,
  }
}

export function applyDetailSnapshot(store, detailPayload) {
  const detail = normalizeDetail(detailPayload?.detail || detailPayload, {
    symbolKey: detailPayload?.symbol_key || detailPayload?.detail?.symbol_key || null,
    seq: detailPayload?.seq || detailPayload?.detail?.seq || 0,
  })
  if (!detail.symbol_key) return store
  const detailCache = { ...(store?.detailCache || {}), [detail.symbol_key]: detail }
  const detailCacheOrder = touchCacheOrder(store?.detailCacheOrder, detail.symbol_key, store?.detailCacheLimit)
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(detail.seq || 0)),
    detailCache: trimDetailCache(detailCache, detailCacheOrder, store?.selectedSymbolKey, store?.detailCacheLimit),
    detailCacheOrder,
    symbolIndex: {
      ...(store?.symbolIndex || {}),
      [detail.symbol_key]: {
        ...((store?.symbolIndex || {})[detail.symbol_key] || {}),
        symbol_key: detail.symbol_key,
        instrument_id: detail.instrument_id || null,
        symbol: detail.symbol || null,
        timeframe: detail.timeframe || null,
        display_label: detail.display_label || null,
        status: detail.status || null,
        continuity_status: detail.continuity?.status || null,
        last_event_at: detail.last_event_at || null,
        candle_count: Array.isArray(detail.candles) ? detail.candles.length : 0,
        stats: detail.stats || {},
      },
    },
  }
}

export function applyDetailDelta(store, message) {
  const symbolKey = normalizeSeriesKey(message?.symbol_key || '')
  if (!symbolKey) return store
  const current = store?.detailCache?.[symbolKey]
  if (!current) {
    warnDroppedDetailDelta(symbolKey, message, store)
    return store
  }
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const next = {
    ...current,
    seq: Math.max(Number(current.seq || 0), Number(payload.detail_seq || message?.seq || 0)),
    last_event_at: payload.event_time || current.last_event_at || null,
    continuity: payload.continuity && typeof payload.continuity === 'object' ? { ...payload.continuity } : current.continuity,
    runtime: payload.runtime && typeof payload.runtime === 'object' ? { ...current.runtime, ...payload.runtime } : current.runtime,
    stats: payload.stats && typeof payload.stats === 'object' ? { ...payload.stats } : current.stats,
  }
  if (payload.candle && typeof payload.candle === 'object') {
    next.candles = mergeCanonicalCandles(current.candles || [], [payload.candle])
  }
  if (payload.overlay_delta && typeof payload.overlay_delta === 'object') {
    next.overlays = applyOverlayDelta(current.overlays || [], payload.overlay_delta)
  }
  ;(Array.isArray(payload.trade_upserts) ? payload.trade_upserts : []).forEach((trade) => {
    const normalized = normalizeTrade(trade)
    if (!normalized) return
    next.recent_trades = upsertTail(next.recent_trades, normalized, ['trade_id', 'id'], MAX_TRADES)
  })
  ;(Array.isArray(payload.trade_removals) ? payload.trade_removals : []).forEach((tradeId) => {
    next.recent_trades = (Array.isArray(next.recent_trades) ? next.recent_trades : []).filter(
      (trade) => String(trade?.trade_id || '') !== String(tradeId),
    )
  })
  ;(Array.isArray(payload.log_append) ? payload.log_append : []).forEach((entry) => {
    if (!entry || typeof entry !== 'object') return
    next.logs = upsertTail(next.logs, entry, ['id', 'event_id'], MAX_LOGS)
  })
  ;(Array.isArray(payload.decision_append) ? payload.decision_append : []).forEach((entry) => {
    if (!entry || typeof entry !== 'object') return
    next.decisions = upsertTail(next.decisions, entry, ['event_id', 'id'], MAX_DECISIONS)
  })
  const detailCache = { ...(store?.detailCache || {}), [symbolKey]: next }
  const detailCacheOrder = touchCacheOrder(store?.detailCacheOrder, symbolKey, store?.detailCacheLimit)
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.seq || 0)),
    detailCache: trimDetailCache(detailCache, detailCacheOrder, store?.selectedSymbolKey, store?.detailCacheLimit),
    detailCacheOrder,
  }
}

export function applyHistoryPage(store, { symbolKey, candles }) {
  const normalizedSymbolKey = normalizeSeriesKey(symbolKey)
  const current = store?.detailCache?.[normalizedSymbolKey]
  if (!current) return store
  const detailCache = {
    ...(store?.detailCache || {}),
    [normalizedSymbolKey]: {
      ...current,
      candles: mergeCanonicalCandles(candles || [], current.candles || []),
    },
  }
  return {
    ...store,
    detailCache,
  }
}

export function selectSymbol(store, symbolKey) {
  const normalized = normalizeSeriesKey(symbolKey)
  if (!normalized) return store
  const detailCacheOrder = touchCacheOrder(store?.detailCacheOrder, normalized, store?.detailCacheLimit)
  return {
    ...store,
    selectedSymbolKey: normalized,
    detailCacheOrder,
    detailCache: trimDetailCache(store?.detailCache || {}, detailCacheOrder, normalized, store?.detailCacheLimit),
  }
}

export function getSelectedDetail(store) {
  const symbolKey = normalizeSeriesKey(store?.selectedSymbolKey || '')
  if (!symbolKey) return null
  return store?.detailCache?.[symbolKey] || null
}
