import { toFiniteNumber, toSec } from './chartDataUtils.js'
import { createLogger } from '../../utils/logger.js'

const DEFAULT_SYMBOL_SNAPSHOT_LIMIT = 6
const MAX_LOGS = 300
const MAX_DECISIONS = 600
const MAX_TRADES = 240
const SYMBOL_DELTA_DROP_WARN_INTERVAL_MS = 10000
const logger = createLogger('botlensProjection')
const symbolDeltaDropWarnings = new Map()

export const SYMBOL_CANDLE_DELTA_TYPE = 'symbol_candle_delta'
export const SYMBOL_OVERLAY_DELTA_TYPE = 'symbol_overlay_delta'
export const SYMBOL_TRADE_DELTA_TYPE = 'symbol_trade_delta'
export const SYMBOL_LOG_DELTA_TYPE = 'symbol_log_delta'
export const SYMBOL_DECISION_DELTA_TYPE = 'symbol_decision_delta'
export const SYMBOL_RUNTIME_DELTA_TYPE = 'symbol_runtime_delta'
export const SYMBOL_SNAPSHOT_MESSAGE_TYPE = 'botlens_symbol_snapshot'

const SYMBOL_DELTA_TYPES = new Set([
  SYMBOL_CANDLE_DELTA_TYPE,
  SYMBOL_OVERLAY_DELTA_TYPE,
  SYMBOL_TRADE_DELTA_TYPE,
  SYMBOL_LOG_DELTA_TYPE,
  SYMBOL_DECISION_DELTA_TYPE,
  SYMBOL_RUNTIME_DELTA_TYPE,
])

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

export function normalizeSymbolSnapshot(snapshot, { symbolKey = null, seq = 0 } = {}) {
  const source = snapshot && typeof snapshot === 'object' ? snapshot : {}
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

function touchSymbolSnapshotOrder(order = [], symbolKey, limit = DEFAULT_SYMBOL_SNAPSHOT_LIMIT) {
  const next = order.filter((entry) => entry !== symbolKey)
  if (symbolKey) next.unshift(symbolKey)
  return next.slice(0, Math.max(1, limit))
}

function trimSymbolSnapshots(symbolSnapshots = {}, order, selectedSymbolKey, limit = DEFAULT_SYMBOL_SNAPSHOT_LIMIT) {
  const allowed = new Set((order || []).slice(0, Math.max(1, limit)))
  if (selectedSymbolKey) allowed.add(selectedSymbolKey)
  const next = {}
  Object.entries(symbolSnapshots || {}).forEach(([key, value]) => {
    if (allowed.has(key)) next[key] = value
  })
  return next
}

function warnDroppedSymbolDelta(symbolKey, message, store) {
  const now = Date.now()
  const warningKey = `${symbolKey}:${String(message?.type || '')}`
  const last = Number(symbolDeltaDropWarnings.get(warningKey) || 0)
  if (now - last < SYMBOL_DELTA_DROP_WARN_INTERVAL_MS) return
  symbolDeltaDropWarnings.set(warningKey, now)
  logger.warn('botlens_symbol_delta_dropped_missing_base', {
    symbol_key: symbolKey,
    type: String(message?.type || ''),
    seq: Number(message?.seq || 0),
    selected_symbol_key: normalizeSeriesKey(store?.selectedSymbolKey || ''),
    snapshot_cache_size: Object.keys(store?.symbolSnapshots || {}).length,
  })
}

export function isTypedSymbolDeltaMessage(message) {
  return SYMBOL_DELTA_TYPES.has(String(message?.type || ''))
}

export function isSymbolSnapshotMessage(message) {
  return String(message?.type || '') === SYMBOL_SNAPSHOT_MESSAGE_TYPE
}

export function createRunStore(session, { symbolSnapshotLimit = DEFAULT_SYMBOL_SNAPSHOT_LIMIT } = {}) {
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
  const symbolSnapshot = session?.detail && typeof session.detail === 'object'
    ? normalizeSymbolSnapshot(session.detail, { symbolKey: selectedSymbolKey, seq: Number(session?.seq || 0) || 0 })
    : null
  const symbolSnapshots = symbolSnapshot && symbolSnapshot.symbol_key ? { [symbolSnapshot.symbol_key]: symbolSnapshot } : {}
  const symbolSnapshotOrder = symbolSnapshot ? [symbolSnapshot.symbol_key] : []
  return {
    schemaVersion: Number(session?.schema_version || 4) || 4,
    live: Boolean(session?.live),
    seq: Number(session?.seq || 0) || 0,
    runMeta: session?.run_meta && typeof session.run_meta === 'object' ? { ...session.run_meta } : null,
    lifecycle: session?.lifecycle && typeof session.lifecycle === 'object' ? { ...session.lifecycle } : {},
    health: normalizeHealth(session?.health),
    symbolIndex,
    openTradesIndex,
    symbolSnapshots,
    symbolSnapshotOrder,
    selectedSymbolKey: selectedSymbolKey || null,
    symbolSnapshotLimit: Math.max(1, Number(symbolSnapshotLimit) || DEFAULT_SYMBOL_SNAPSHOT_LIMIT),
  }
}

export function applySummaryDelta(store, message) {
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const nextSymbolIndex = { ...(store?.symbolIndex || {}) }
  const nextSymbolSnapshots = { ...(store?.symbolSnapshots || {}) }
  ;(Array.isArray(payload.symbol_upserts) ? payload.symbol_upserts : []).forEach((summary) => {
    const normalized = normalizeSummary(summary)
    if (!normalized) return
    nextSymbolIndex[normalized.symbol_key] = {
      ...(nextSymbolIndex[normalized.symbol_key] || {}),
      ...normalized,
    }
    if (nextSymbolSnapshots[normalized.symbol_key]) {
      nextSymbolSnapshots[normalized.symbol_key] = {
        ...nextSymbolSnapshots[normalized.symbol_key],
        instrument_id: normalized.instrument_id || nextSymbolSnapshots[normalized.symbol_key].instrument_id,
        symbol: normalized.symbol || nextSymbolSnapshots[normalized.symbol_key].symbol,
        timeframe: normalized.timeframe || nextSymbolSnapshots[normalized.symbol_key].timeframe,
        display_label: normalized.display_label || nextSymbolSnapshots[normalized.symbol_key].display_label,
        status: normalized.status || nextSymbolSnapshots[normalized.symbol_key].status,
        last_event_at: normalized.last_event_at || nextSymbolSnapshots[normalized.symbol_key].last_event_at,
        stats: normalized.stats && typeof normalized.stats === 'object'
          ? { ...normalized.stats }
          : nextSymbolSnapshots[normalized.symbol_key].stats,
      }
    }
  })
  ;(Array.isArray(payload.symbol_removals) ? payload.symbol_removals : []).forEach((symbolKey) => {
    const normalizedSymbolKey = normalizeSeriesKey(symbolKey)
    delete nextSymbolIndex[normalizedSymbolKey]
    delete nextSymbolSnapshots[normalizedSymbolKey]
  })
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.seq || 0)),
    health: payload.health && typeof payload.health === 'object'
      ? normalizeHealth({ ...(store?.health || {}), ...payload.health })
      : store.health,
    lifecycle: payload.lifecycle && typeof payload.lifecycle === 'object' ? { ...store.lifecycle, ...payload.lifecycle } : store.lifecycle,
    symbolIndex: nextSymbolIndex,
    symbolSnapshots: trimSymbolSnapshots(
      nextSymbolSnapshots,
      store?.symbolSnapshotOrder,
      store?.selectedSymbolKey,
      store?.symbolSnapshotLimit,
    ),
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

export function applySymbolSnapshot(store, snapshotPayload) {
  const symbolSnapshot = normalizeSymbolSnapshot(snapshotPayload?.detail || snapshotPayload, {
    symbolKey: snapshotPayload?.symbol_key || snapshotPayload?.detail?.symbol_key || null,
    seq: snapshotPayload?.seq || snapshotPayload?.detail?.seq || 0,
  })
  if (!symbolSnapshot.symbol_key) return store
  const symbolSnapshots = { ...(store?.symbolSnapshots || {}), [symbolSnapshot.symbol_key]: symbolSnapshot }
  const symbolSnapshotOrder = touchSymbolSnapshotOrder(
    store?.symbolSnapshotOrder,
    symbolSnapshot.symbol_key,
    store?.symbolSnapshotLimit,
  )
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(symbolSnapshot.seq || 0)),
    symbolSnapshots: trimSymbolSnapshots(
      symbolSnapshots,
      symbolSnapshotOrder,
      store?.selectedSymbolKey,
      store?.symbolSnapshotLimit,
    ),
    symbolSnapshotOrder,
    symbolIndex: {
      ...(store?.symbolIndex || {}),
      [symbolSnapshot.symbol_key]: {
        ...((store?.symbolIndex || {})[symbolSnapshot.symbol_key] || {}),
        symbol_key: symbolSnapshot.symbol_key,
        instrument_id: symbolSnapshot.instrument_id || null,
        symbol: symbolSnapshot.symbol || null,
        timeframe: symbolSnapshot.timeframe || null,
        display_label: symbolSnapshot.display_label || null,
        status: symbolSnapshot.status || null,
        last_event_at: symbolSnapshot.last_event_at || null,
        candle_count: Array.isArray(symbolSnapshot.candles) ? symbolSnapshot.candles.length : 0,
        stats: symbolSnapshot.stats || {},
      },
    },
  }
}

function commitSymbolSnapshot(store, symbolKey, next) {
  const symbolSnapshots = { ...(store?.symbolSnapshots || {}), [symbolKey]: next }
  const symbolSnapshotOrder = touchSymbolSnapshotOrder(store?.symbolSnapshotOrder, symbolKey, store?.symbolSnapshotLimit)
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(next?.seq || 0)),
    symbolSnapshots: trimSymbolSnapshots(symbolSnapshots, symbolSnapshotOrder, store?.selectedSymbolKey, store?.symbolSnapshotLimit),
    symbolSnapshotOrder,
  }
}

function withSymbolSnapshot(store, message, applyChange) {
  const symbolKey = normalizeSeriesKey(message?.symbol_key || '')
  if (!symbolKey) return store
  const current = store?.symbolSnapshots?.[symbolKey]
  if (!current) {
    warnDroppedSymbolDelta(symbolKey, message, store)
    return store
  }
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const next = applyChange({
    ...current,
    seq: Math.max(Number(current.seq || 0), Number(message?.seq || 0)),
    last_event_at: message?.event_time || current.last_event_at || null,
  }, payload, current)
  return commitSymbolSnapshot(store, symbolKey, next)
}

export function applyCandleDelta(store, message) {
  return withSymbolSnapshot(store, message, (next, payload) => {
    if (payload.candle && typeof payload.candle === 'object') {
      next.candles = mergeCanonicalCandles(next.candles || [], [payload.candle])
    }
    return next
  })
}

export function applyOverlayDeltaMessage(store, message) {
  return withSymbolSnapshot(store, message, (next, payload) => {
    if (payload.overlay_delta && typeof payload.overlay_delta === 'object') {
      next.overlays = applyOverlayDelta(next.overlays || [], payload.overlay_delta)
    }
    return next
  })
}

export function applyTradeDelta(store, message) {
  return withSymbolSnapshot(store, message, (next, payload) => {
    ;(Array.isArray(payload.upserts) ? payload.upserts : []).forEach((trade) => {
      const normalized = normalizeTrade(trade)
      if (!normalized) return
      next.recent_trades = upsertTail(next.recent_trades, normalized, ['trade_id', 'id'], MAX_TRADES)
    })
    ;(Array.isArray(payload.removals) ? payload.removals : []).forEach((tradeId) => {
      next.recent_trades = (Array.isArray(next.recent_trades) ? next.recent_trades : []).filter(
        (trade) => String(trade?.trade_id || '') !== String(tradeId),
      )
    })
    return next
  })
}

export function applyLogDelta(store, message) {
  return withSymbolSnapshot(store, message, (next, payload) => {
    ;(Array.isArray(payload.append) ? payload.append : []).forEach((entry) => {
      if (!entry || typeof entry !== 'object') return
      next.logs = upsertTail(next.logs, entry, ['id', 'event_id'], MAX_LOGS)
    })
    return next
  })
}

export function applyDecisionDelta(store, message) {
  return withSymbolSnapshot(store, message, (next, payload) => {
    ;(Array.isArray(payload.append) ? payload.append : []).forEach((entry) => {
      if (!entry || typeof entry !== 'object') return
      next.decisions = upsertTail(next.decisions, entry, ['event_id', 'id'], MAX_DECISIONS)
    })
    return next
  })
}

export function applyRuntimeDelta(store, message) {
  return withSymbolSnapshot(store, message, (next, payload) => {
    next.runtime = payload.runtime && typeof payload.runtime === 'object'
      ? { ...next.runtime, ...payload.runtime }
      : next.runtime
    if (payload.runtime && typeof payload.runtime === 'object' && payload.runtime.status) {
      next.status = String(payload.runtime.status || next.status || 'waiting').trim()
    }
    return next
  })
}

export function applyTypedSymbolDelta(store, message) {
  switch (String(message?.type || '')) {
    case SYMBOL_CANDLE_DELTA_TYPE:
      return applyCandleDelta(store, message)
    case SYMBOL_OVERLAY_DELTA_TYPE:
      return applyOverlayDeltaMessage(store, message)
    case SYMBOL_TRADE_DELTA_TYPE:
      return applyTradeDelta(store, message)
    case SYMBOL_LOG_DELTA_TYPE:
      return applyLogDelta(store, message)
    case SYMBOL_DECISION_DELTA_TYPE:
      return applyDecisionDelta(store, message)
    case SYMBOL_RUNTIME_DELTA_TYPE:
      return applyRuntimeDelta(store, message)
    default:
      return store
  }
}

export function applyHistoryPage(store, { symbolKey, candles }) {
  const normalizedSymbolKey = normalizeSeriesKey(symbolKey)
  const current = store?.symbolSnapshots?.[normalizedSymbolKey]
  if (!current) return store
  const symbolSnapshots = {
    ...(store?.symbolSnapshots || {}),
    [normalizedSymbolKey]: {
      ...current,
      candles: mergeCanonicalCandles(candles || [], current.candles || []),
    },
  }
  return {
    ...store,
    symbolSnapshots,
  }
}

export function selectSymbol(store, symbolKey) {
  const normalized = normalizeSeriesKey(symbolKey)
  if (!normalized) return store
  const symbolSnapshotOrder = touchSymbolSnapshotOrder(store?.symbolSnapshotOrder, normalized, store?.symbolSnapshotLimit)
  return {
    ...store,
    selectedSymbolKey: normalized,
    symbolSnapshotOrder,
    symbolSnapshots: trimSymbolSnapshots(
      store?.symbolSnapshots || {},
      symbolSnapshotOrder,
      normalized,
      store?.symbolSnapshotLimit,
    ),
  }
}

export function getSelectedSymbolSnapshot(store) {
  const symbolKey = normalizeSeriesKey(store?.selectedSymbolKey || '')
  if (!symbolKey) return null
  return store?.symbolSnapshots?.[symbolKey] || null
}

export function getSelectedSymbolSlices(store) {
  const snapshot = getSelectedSymbolSnapshot(store)
  if (!snapshot) return null
  return {
    snapshot,
    metadata: {
      symbol_key: snapshot.symbol_key,
      instrument_id: snapshot.instrument_id,
      symbol: snapshot.symbol,
      timeframe: snapshot.timeframe,
      display_label: snapshot.display_label,
      status: snapshot.status,
      seq: snapshot.seq,
      last_event_at: snapshot.last_event_at,
    },
    candles: Array.isArray(snapshot.candles) ? snapshot.candles : [],
    overlays: Array.isArray(snapshot.overlays) ? snapshot.overlays : [],
    recentTrades: Array.isArray(snapshot.recent_trades) ? snapshot.recent_trades : [],
    logs: Array.isArray(snapshot.logs) ? snapshot.logs : [],
    decisions: Array.isArray(snapshot.decisions) ? snapshot.decisions : [],
    runtime: snapshot.runtime && typeof snapshot.runtime === 'object' ? snapshot.runtime : {},
    stats: snapshot.stats && typeof snapshot.stats === 'object' ? snapshot.stats : {},
  }
}
