import { toFiniteNumber, toSec } from './chartDataUtils.js'
import { createLogger } from '../../utils/logger.js'

const DEFAULT_SYMBOL_STATE_LIMIT = 6
const MAX_LOGS = 300
const MAX_DECISIONS = 600
const MAX_TRADES = 240
const SYMBOL_DELTA_DROP_WARN_INTERVAL_MS = 10000
const logger = createLogger('botlensProjection')
const symbolDeltaDropWarnings = new Map()

export const SYMBOL_SIGNAL_DELTA_TYPE = 'botlens_symbol_signal_delta'
export const SYMBOL_CANDLE_DELTA_TYPE = 'botlens_symbol_candle_delta'
export const SYMBOL_OVERLAY_DELTA_TYPE = 'botlens_symbol_overlay_delta'
export const SYMBOL_TRADE_DELTA_TYPE = 'botlens_symbol_trade_delta'
export const SYMBOL_LOG_DELTA_TYPE = 'botlens_symbol_diagnostic_delta'
export const SYMBOL_DECISION_DELTA_TYPE = 'botlens_symbol_decision_delta'
export const SYMBOL_STATS_DELTA_TYPE = 'botlens_symbol_stats_delta'
export const RUN_LIFECYCLE_DELTA_TYPE = 'botlens_run_lifecycle_delta'
export const RUN_HEALTH_DELTA_TYPE = 'botlens_run_health_delta'
export const RUN_FAULT_DELTA_TYPE = 'botlens_run_fault_delta'
export const RUN_SYMBOL_CATALOG_DELTA_TYPE = 'botlens_run_symbol_catalog_delta'
export const RUN_OPEN_TRADES_DELTA_TYPE = 'botlens_run_open_trades_delta'

const SYMBOL_DELTA_TYPES = new Set([
  SYMBOL_CANDLE_DELTA_TYPE,
  SYMBOL_OVERLAY_DELTA_TYPE,
  SYMBOL_SIGNAL_DELTA_TYPE,
  SYMBOL_TRADE_DELTA_TYPE,
  SYMBOL_LOG_DELTA_TYPE,
  SYMBOL_DECISION_DELTA_TYPE,
  SYMBOL_STATS_DELTA_TYPE,
])

const MAX_SIGNALS = 600
const MAX_RUN_FAULTS = 120

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

function normalizeSymbolReadiness(readiness, defaults = {}) {
  const source = readiness && typeof readiness === 'object' ? readiness : {}
  return {
    catalog_discovered: Boolean(
      source.catalog_discovered ?? defaults.catalog_discovered,
    ),
    snapshot_ready: Boolean(
      source.snapshot_ready ?? defaults.snapshot_ready,
    ),
    symbol_live: Boolean(
      source.symbol_live ?? defaults.symbol_live,
    ),
  }
}

function normalizeSelectedSymbolReadiness(readiness, defaults = {}) {
  const source = normalizeSymbolReadiness(readiness, defaults)
  return {
    ...source,
    run_live: Boolean(
      (readiness && typeof readiness === 'object' ? readiness.run_live : undefined)
      ?? defaults.run_live,
    ),
  }
}

function normalizeRunReadiness(readiness, defaults = {}) {
  const source = readiness && typeof readiness === 'object' ? readiness : {}
  return {
    catalog_discovered: Boolean(
      source.catalog_discovered ?? defaults.catalog_discovered,
    ),
    run_live: Boolean(
      source.run_live ?? defaults.run_live,
    ),
  }
}

function normalizeContinuity(continuity) {
  if (!continuity || typeof continuity !== 'object') return null
  const normalizedSeriesKey = normalizeSeriesKey(continuity.series_key || '')
  const normalizeOptionalNumber = (value) => {
    const numeric = Number(value)
    return Number.isFinite(numeric) ? numeric : null
  }
  return {
    candle_count: Math.max(0, Number(continuity.candle_count || 0) || 0),
    first_ts: continuity.first_ts || null,
    last_ts: continuity.last_ts || null,
    expected_interval_seconds: normalizeOptionalNumber(continuity.expected_interval_seconds),
    detected_gap_count: Math.max(0, Number(continuity.detected_gap_count || 0) || 0),
    defect_gap_count: Math.max(0, Number(continuity.defect_gap_count || 0) || 0),
    missing_candle_estimate: Math.max(0, Number(continuity.missing_candle_estimate || 0) || 0),
    largest_gap_seconds: normalizeOptionalNumber(continuity.largest_gap_seconds),
    max_gap_seconds: normalizeOptionalNumber(continuity.max_gap_seconds ?? continuity.max_gap),
    max_gap_multiple: normalizeOptionalNumber(continuity.max_gap_multiple),
    continuity_ratio: normalizeOptionalNumber(continuity.continuity_ratio),
    duplicate_count: Math.max(0, Number(continuity.duplicate_count || 0) || 0),
    out_of_order_count: Math.max(0, Number(continuity.out_of_order_count || 0) || 0),
    missing_ohlcv_count: Math.max(0, Number(continuity.missing_ohlcv_count || 0) || 0),
    gap_count_by_type: continuity.gap_count_by_type && typeof continuity.gap_count_by_type === 'object'
      ? { ...continuity.gap_count_by_type }
      : {},
    final_status: String(continuity.final_status || '').trim().toLowerCase() || null,
    boundary_name: String(continuity.boundary_name || '').trim() || null,
    series_key: normalizedSeriesKey || null,
    timeframe: String(continuity.timeframe || '').trim().toLowerCase() || null,
    source_reason: String(continuity.source_reason || '').trim().toLowerCase() || null,
  }
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
  return explicitOverlayId
}

export function projectOverlayState(overlays = []) {
  const projected = new Map()
  ;(Array.isArray(overlays) ? overlays : []).forEach((overlay, index) => {
    if (!overlay || typeof overlay !== 'object') return
    const overlayId = overlayIdentity(overlay, index)
    if (!overlayId) return
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

function normalizeLiveSymbolSummary(summary) {
  if (!summary || typeof summary !== 'object') return null
  const symbolKey = normalizeSeriesKey(summary.symbol_key || '')
  if (!symbolKey) return null
  return {
    ...summary,
    symbol_key: symbolKey,
    instrument_id: String(summary.instrument_id || '').trim() || null,
    symbol: String(summary.symbol || '').trim().toUpperCase() || null,
    timeframe: String(summary.timeframe || '').trim().toLowerCase() || null,
    readiness: normalizeSymbolReadiness(summary.readiness, {
      catalog_discovered: true,
    }),
  }
}

function normalizeRunBootstrapSymbol(entry) {
  if (!entry || typeof entry !== 'object') return null
  const symbolKey = normalizeSeriesKey(entry.symbol_key || '')
  if (!symbolKey) return null
  const identity = entry.identity && typeof entry.identity === 'object' ? entry.identity : {}
  const activity = entry.activity && typeof entry.activity === 'object' ? entry.activity : {}
  const openTrade = entry.open_trade && typeof entry.open_trade === 'object' ? entry.open_trade : {}
  return {
    symbol_key: symbolKey,
    instrument_id: String(identity.instrument_id || '').trim() || null,
    symbol: String(identity.symbol || '').trim().toUpperCase() || null,
    timeframe: String(identity.timeframe || '').trim().toLowerCase() || null,
    display_label: String(identity.display_label || '').trim() || null,
    status: String(activity.status || 'waiting').trim(),
    last_event_at: activity.last_event_at || null,
    last_bar_time: activity.last_bar_time || null,
    last_price: activity.last_price ?? null,
    candle_count: Number(activity.candle_count || 0) || 0,
    has_open_trade: Boolean(openTrade.present),
    open_trade_count: Number(openTrade.count || 0) || 0,
    last_trade_at: activity.last_trade_at || null,
    last_activity_at: activity.last_activity_at || null,
    stats: entry.stats && typeof entry.stats === 'object' ? { ...entry.stats } : {},
    readiness: normalizeSymbolReadiness(entry.readiness, {
      catalog_discovered: true,
    }),
  }
}

function normalizeTrade(trade) {
  if (!trade || typeof trade !== 'object') return null
  const tradeId = String(trade.trade_id || '').trim()
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
  const warningType = String(warning.warning_type || '').trim()
  const warningId = String(warning.warning_id || '').trim()
  if (!warningType || !warningId) return null
  const indicatorId = String(warning.indicator_id || '').trim() || null
  const symbolKey = normalizeSeriesKey(warning.symbol_key || '')
  const symbol = String(warning.symbol || '').trim().toUpperCase() || null
  const timeframe = String(warning.timeframe || '').trim().toLowerCase() || null
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

export function normalizeSelectedSymbolState(selectedSymbol, { symbolKey = null, seq = 0 } = {}) {
  const source = selectedSymbol && typeof selectedSymbol === 'object' ? selectedSymbol : {}
  const normalizedSymbolKey = normalizeSeriesKey(source.symbol_key || symbolKey || '')
  const [instrumentId, timeframe] = normalizedSymbolKey.split('|')
  const normalizedSeq = Number(source.seq ?? seq ?? 0) || 0
  return {
    symbol_key: normalizedSymbolKey,
    instrument_id: String(source.instrument_id || instrumentId || '').trim(),
    symbol: String(source.symbol || '').trim().toUpperCase(),
    timeframe: String(source.timeframe || timeframe || '').trim().toLowerCase(),
    display_label: String(source.display_label || '').trim() || null,
    status: String(source.status || 'waiting').trim(),
    last_event_at: source.last_event_at || null,
    seq: normalizedSeq,
    readiness: normalizeSelectedSymbolReadiness(source.readiness, {
      catalog_discovered: Boolean(normalizedSymbolKey),
      snapshot_ready: normalizedSeq > 0,
      symbol_live: false,
      run_live: false,
    }),
    candles: mergeCanonicalCandles(source.candles || []),
    overlays: projectOverlayState(source.overlays || []),
    signals: Array.isArray(source.signals) ? source.signals.filter((entry) => entry && typeof entry === 'object').map((entry) => ({ ...entry })) : [],
    recent_trades: (Array.isArray(source.recent_trades) ? source.recent_trades : [])
      .map((entry) => normalizeTrade(entry))
      .filter(Boolean),
    logs: Array.isArray(source.logs) ? source.logs.filter((entry) => entry && typeof entry === 'object').map((entry) => ({ ...entry })) : [],
    decisions: Array.isArray(source.decisions) ? source.decisions.filter((entry) => entry && typeof entry === 'object').map((entry) => ({ ...entry })) : [],
    stats: source.stats && typeof source.stats === 'object' ? { ...source.stats } : {},
    runtime: source.runtime && typeof source.runtime === 'object' ? { ...source.runtime } : {},
    continuity: normalizeContinuity(source.continuity),
  }
}

function touchSymbolStateOrder(order = [], symbolKey, limit = DEFAULT_SYMBOL_STATE_LIMIT) {
  const next = order.filter((entry) => entry !== symbolKey)
  if (symbolKey) next.unshift(symbolKey)
  return next.slice(0, Math.max(1, limit))
}

function trimSymbolStates(symbolStates = {}, order, selectedSymbolKey, limit = DEFAULT_SYMBOL_STATE_LIMIT) {
  const allowed = new Set((order || []).slice(0, Math.max(1, limit)))
  if (selectedSymbolKey) allowed.add(selectedSymbolKey)
  const next = {}
  Object.entries(symbolStates || {}).forEach(([key, value]) => {
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
    state_cache_size: Object.keys(store?.symbolStates || {}).length,
  })
}

export function isTypedSymbolDeltaMessage(message) {
  return SYMBOL_DELTA_TYPES.has(String(message?.type || ''))
}

export function createRunStore(runBootstrap, { symbolStateLimit = DEFAULT_SYMBOL_STATE_LIMIT } = {}) {
  const run = runBootstrap?.run && typeof runBootstrap.run === 'object' ? runBootstrap.run : {}
  const navigation = runBootstrap?.navigation && typeof runBootstrap.navigation === 'object' ? runBootstrap.navigation : {}
  const summaries = Array.isArray(navigation?.symbols) ? navigation.symbols : []
  const symbolIndex = summaries.reduce((acc, summary) => {
    const normalized = normalizeRunBootstrapSymbol(summary)
    if (!normalized) return acc
    acc[normalized.symbol_key] = normalized
    return acc
  }, {})
  const openTrades = Array.isArray(run?.open_trades) ? run.open_trades : []
  const openTradesIndex = openTrades.reduce((acc, trade) => {
    const normalized = normalizeTrade(trade)
    if (!normalized) return acc
    acc[normalized.trade_id] = normalized
    return acc
  }, {})
  const selectedSymbolKey = normalizeSeriesKey(navigation?.selected_symbol_key || '')
  const selectedSymbolPayload = runBootstrap?.selected_symbol && typeof runBootstrap.selected_symbol === 'object'
    ? runBootstrap.selected_symbol
    : null
  const selectedMetadata = selectedSymbolPayload?.metadata && typeof selectedSymbolPayload.metadata === 'object'
    ? selectedSymbolPayload.metadata
    : {}
  const selectedSnapshot = selectedSymbolPayload?.current && typeof selectedSymbolPayload.current === 'object'
    ? selectedSymbolPayload.current
    : selectedSymbolPayload?.visual && typeof selectedSymbolPayload.visual === 'object'
      ? selectedSymbolPayload.visual
    : {}
  const bootstrappedSelectedSymbol = selectedSymbolPayload
    ? normalizeSelectedSymbolState(
        { ...selectedMetadata, ...selectedSnapshot },
        {
          symbolKey: selectedSymbolKey || selectedMetadata?.symbol_key || null,
          seq: Number(selectedMetadata?.seq || 0) || 0,
        },
      )
    : null
  const symbolStates = bootstrappedSelectedSymbol?.symbol_key
    ? { [bootstrappedSelectedSymbol.symbol_key]: bootstrappedSelectedSymbol }
    : {}
  const symbolStateOrder = bootstrappedSelectedSymbol?.symbol_key
    ? [bootstrappedSelectedSymbol.symbol_key]
    : []
  const lifecycle = run?.lifecycle && typeof run.lifecycle === 'object' ? { ...run.lifecycle } : {}
  const readiness = normalizeRunReadiness(runBootstrap?.readiness || run?.readiness, {
    catalog_discovered: summaries.length > 0,
    run_live: Boolean(lifecycle.live),
  })
  return {
    schemaVersion: Number(runBootstrap?.schema_version || 4) || 4,
    state: String(runBootstrap?.state || '').trim() || null,
    contractState: String(runBootstrap?.contract_state || runBootstrap?.state || '').trim() || null,
    readiness,
    transportEligible: Boolean(runBootstrap?.live_transport?.eligible),
    seq: Number(runBootstrap?.bootstrap?.bootstrap_seq || 0) || 0,
    streamSessionId: String(runBootstrap?.live_transport?.stream_session_id || '').trim() || null,
    lastStreamSeq: Number(runBootstrap?.bootstrap?.base_seq || 0) || 0,
    runMeta: run?.meta && typeof run.meta === 'object' ? { ...run.meta } : null,
    lifecycle,
    health: normalizeHealth(run?.health),
    faults: [],
    symbolIndex,
    openTradesIndex,
    symbolStates,
    symbolStateOrder,
    selectedSymbolKey: selectedSymbolKey || null,
    symbolStateLimit: Math.max(1, Number(symbolStateLimit) || DEFAULT_SYMBOL_STATE_LIMIT),
  }
}

function advanceLiveCursor(store, message) {
  const streamSessionId = String(message?.stream_session_id || '').trim()
  const streamSeq = Number(message?.stream_seq || 0)
  if (!streamSessionId || !Number.isFinite(streamSeq) || streamSeq <= 0) {
    return { apply: false, store }
  }
  const currentSessionId = String(store?.streamSessionId || '').trim()
  const currentStreamSeq = Number(store?.lastStreamSeq || 0)
  if (currentSessionId && currentSessionId !== streamSessionId) {
    return { apply: false, store }
  }
  if (streamSeq <= currentStreamSeq) {
    return { apply: false, store }
  }
  return {
    apply: true,
    streamSessionId,
    streamSeq,
    store: {
      ...store,
      streamSessionId,
      lastStreamSeq: streamSeq,
    },
  }
}

export function applyRunSymbolCatalogDelta(store, message) {
  const gated = advanceLiveCursor(store, message)
  if (!gated.apply) return store
  store = gated.store
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const nextSymbolIndex = { ...(store?.symbolIndex || {}) }
  const nextSymbolStates = { ...(store?.symbolStates || {}) }
  ;(Array.isArray(payload.upserts) ? payload.upserts : []).forEach((summary) => {
    const normalized = normalizeLiveSymbolSummary(summary)
    if (!normalized) return
    nextSymbolIndex[normalized.symbol_key] = {
      ...(nextSymbolIndex[normalized.symbol_key] || {}),
      ...normalized,
    }
    if (nextSymbolStates[normalized.symbol_key]) {
      const currentReadiness = nextSymbolStates[normalized.symbol_key].readiness || {}
      nextSymbolStates[normalized.symbol_key] = {
        ...nextSymbolStates[normalized.symbol_key],
        instrument_id: normalized.instrument_id || nextSymbolStates[normalized.symbol_key].instrument_id,
        symbol: normalized.symbol || nextSymbolStates[normalized.symbol_key].symbol,
        timeframe: normalized.timeframe || nextSymbolStates[normalized.symbol_key].timeframe,
        display_label: normalized.display_label || nextSymbolStates[normalized.symbol_key].display_label,
        status: normalized.status || nextSymbolStates[normalized.symbol_key].status,
        last_event_at: normalized.last_event_at || nextSymbolStates[normalized.symbol_key].last_event_at,
        stats: normalized.stats && typeof normalized.stats === 'object'
          ? { ...normalized.stats }
          : nextSymbolStates[normalized.symbol_key].stats,
        readiness: normalizeSelectedSymbolReadiness(normalized.readiness, {
          catalog_discovered: true,
          snapshot_ready: currentReadiness.snapshot_ready,
          symbol_live: currentReadiness.symbol_live || normalized.readiness?.symbol_live,
          run_live: store?.readiness?.run_live || currentReadiness.run_live,
        }),
      }
    }
  })
  ;(Array.isArray(payload.removals) ? payload.removals : []).forEach((symbolKey) => {
    const normalizedSymbolKey = normalizeSeriesKey(symbolKey)
    delete nextSymbolIndex[normalizedSymbolKey]
    delete nextSymbolStates[normalizedSymbolKey]
  })
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.scope_seq || 0)),
    symbolIndex: nextSymbolIndex,
    symbolStates: trimSymbolStates(
      nextSymbolStates,
      store?.symbolStateOrder,
      store?.selectedSymbolKey,
      store?.symbolStateLimit,
    ),
  }
}

export function applyOpenTradesDelta(store, message) {
  const gated = advanceLiveCursor(store, message)
  if (!gated.apply) return store
  store = gated.store
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
    seq: Math.max(Number(store?.seq || 0), Number(message?.scope_seq || 0)),
    openTradesIndex,
  }
}

export function applyRunLifecycleDelta(store, message) {
  const gated = advanceLiveCursor(store, message)
  if (!gated.apply) return store
  store = gated.store
  const lifecycle = message?.payload?.lifecycle && typeof message.payload.lifecycle === 'object'
    ? message.payload.lifecycle
    : null
  if (!lifecycle) return store
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.scope_seq || 0)),
    lifecycle: { ...(store?.lifecycle || {}), ...lifecycle },
    readiness: normalizeRunReadiness(store?.readiness, {
      catalog_discovered: store?.readiness?.catalog_discovered || Object.keys(store?.symbolIndex || {}).length > 0,
      run_live: Boolean(lifecycle.live ?? store?.readiness?.run_live),
    }),
  }
}

export function applyRunHealthDelta(store, message) {
  const gated = advanceLiveCursor(store, message)
  if (!gated.apply) return store
  store = gated.store
  const health = message?.payload?.health && typeof message.payload.health === 'object'
    ? normalizeHealth({ ...(store?.health || {}), ...message.payload.health })
    : store.health
  const nextSymbolStates = { ...(store?.symbolStates || {}) }
  Object.keys(nextSymbolStates).forEach((symbolKey) => {
    nextSymbolStates[symbolKey] = {
      ...nextSymbolStates[symbolKey],
      status: String(health?.status || nextSymbolStates[symbolKey]?.status || 'waiting').trim(),
      runtime: health && typeof health === 'object'
        ? { ...(nextSymbolStates[symbolKey]?.runtime || {}), ...health }
        : nextSymbolStates[symbolKey]?.runtime,
    }
  })
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.scope_seq || 0)),
    health,
    symbolStates: trimSymbolStates(
      nextSymbolStates,
      store?.symbolStateOrder,
      store?.selectedSymbolKey,
      store?.symbolStateLimit,
    ),
  }
}

export function applyRunFaultDelta(store, message) {
  const gated = advanceLiveCursor(store, message)
  if (!gated.apply) return store
  store = gated.store
  const faults = Array.isArray(store?.faults) ? store.faults : []
  let nextFaults = faults
  ;(Array.isArray(message?.payload?.entries) ? message.payload.entries : []).forEach((entry) => {
    if (!entry || typeof entry !== 'object') return
    nextFaults = upsertTail(nextFaults, entry, ['event_id', 'fault_code'], MAX_RUN_FAULTS)
  })
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(message?.scope_seq || 0)),
    faults: nextFaults,
  }
}

export function applySelectedSymbolBootstrap(store, bootstrapPayload) {
  const scope = bootstrapPayload?.scope && typeof bootstrapPayload.scope === 'object' ? bootstrapPayload.scope : {}
  const selection = bootstrapPayload?.selection && typeof bootstrapPayload.selection === 'object' ? bootstrapPayload.selection : {}
  const selectedSymbol = bootstrapPayload?.selected_symbol && typeof bootstrapPayload.selected_symbol === 'object'
    ? bootstrapPayload.selected_symbol
    : {}
  const metadata = selectedSymbol?.metadata && typeof selectedSymbol.metadata === 'object' ? selectedSymbol.metadata : {}
  const snapshot = selectedSymbol?.current && typeof selectedSymbol.current === 'object'
    ? selectedSymbol.current
    : selectedSymbol?.visual && typeof selectedSymbol.visual === 'object'
      ? selectedSymbol.visual
      : {}
  const symbolState = normalizeSelectedSymbolState(
    { ...metadata, ...snapshot },
    {
      symbolKey: selection?.selected_symbol_key || scope?.symbol_key || metadata?.symbol_key || null,
      seq: Number(metadata?.seq || bootstrapPayload?.bootstrap?.bootstrap_seq || 0) || 0,
    },
  )
  if (!symbolState.symbol_key) return store
  const existingState = store?.symbolStates?.[symbolState.symbol_key] || null
  const hasSnapshotField = (field) => Object.prototype.hasOwnProperty.call(snapshot, field)
  const nextSymbolState = {
    ...symbolState,
    readiness: normalizeSelectedSymbolReadiness(symbolState.readiness, {
      catalog_discovered: true,
      snapshot_ready: true,
      symbol_live: existingState?.readiness?.symbol_live,
      run_live: store?.readiness?.run_live,
    }),
    runtime: hasSnapshotField('runtime') ? symbolState.runtime : existingState?.runtime || symbolState.runtime,
    logs: hasSnapshotField('logs') ? symbolState.logs : existingState?.logs || symbolState.logs,
    signals: hasSnapshotField('signals') ? symbolState.signals : existingState?.signals || symbolState.signals,
    decisions: hasSnapshotField('decisions') ? symbolState.decisions : existingState?.decisions || symbolState.decisions,
    recent_trades: hasSnapshotField('recent_trades') ? symbolState.recent_trades : existingState?.recent_trades || symbolState.recent_trades,
    stats: hasSnapshotField('stats') ? symbolState.stats : existingState?.stats || symbolState.stats,
    overlays: hasSnapshotField('overlays') ? symbolState.overlays : existingState?.overlays || symbolState.overlays,
    candles: hasSnapshotField('candles') ? symbolState.candles : existingState?.candles || symbolState.candles,
    continuity: hasSnapshotField('continuity') ? symbolState.continuity : existingState?.continuity || symbolState.continuity,
  }
  const symbolStates = { ...(store?.symbolStates || {}), [nextSymbolState.symbol_key]: nextSymbolState }
  const symbolStateOrder = touchSymbolStateOrder(
    store?.symbolStateOrder,
    nextSymbolState.symbol_key,
    store?.symbolStateLimit,
  )
  return {
    ...store,
    seq: Math.max(
      Number(store?.seq || 0),
      Number(bootstrapPayload?.bootstrap?.run_bootstrap_seq || 0),
      Number(nextSymbolState.seq || 0),
    ),
    streamSessionId: String(
      bootstrapPayload?.live_transport?.stream_session_id
        || store?.streamSessionId
        || '',
    ).trim() || null,
    lastStreamSeq: Math.max(
      Number(store?.lastStreamSeq || 0),
      Number(bootstrapPayload?.bootstrap?.base_seq || 0),
      0,
    ),
    readiness: normalizeRunReadiness(store?.readiness, {
      catalog_discovered: Object.keys(store?.symbolIndex || {}).length > 0,
      run_live: nextSymbolState.readiness.run_live || store?.readiness?.run_live,
    }),
    symbolStates: trimSymbolStates(
      symbolStates,
      symbolStateOrder,
      nextSymbolState.symbol_key,
      store?.symbolStateLimit,
    ),
    symbolStateOrder,
    selectedSymbolKey: nextSymbolState.symbol_key,
    symbolIndex: {
      ...(store?.symbolIndex || {}),
      [nextSymbolState.symbol_key]: {
        ...((store?.symbolIndex || {})[nextSymbolState.symbol_key] || {}),
        symbol_key: nextSymbolState.symbol_key,
        instrument_id: nextSymbolState.instrument_id || null,
        symbol: nextSymbolState.symbol || null,
        timeframe: nextSymbolState.timeframe || null,
        display_label: nextSymbolState.display_label || null,
        status: nextSymbolState.status || null,
        last_event_at: nextSymbolState.last_event_at || null,
        candle_count: Array.isArray(nextSymbolState.candles) ? nextSymbolState.candles.length : 0,
        stats: nextSymbolState.stats || {},
        readiness: normalizeSymbolReadiness(nextSymbolState.readiness, {
          catalog_discovered: true,
          snapshot_ready: true,
          symbol_live: nextSymbolState.readiness.symbol_live,
        }),
      },
    },
  }
}

function commitSymbolState(store, symbolKey, next) {
  const symbolStates = { ...(store?.symbolStates || {}), [symbolKey]: next }
  const symbolStateOrder = touchSymbolStateOrder(store?.symbolStateOrder, symbolKey, store?.symbolStateLimit)
  return {
    ...store,
    seq: Math.max(Number(store?.seq || 0), Number(next?.seq || 0)),
    symbolStates: trimSymbolStates(symbolStates, symbolStateOrder, store?.selectedSymbolKey, store?.symbolStateLimit),
    symbolStateOrder,
  }
}

function withSymbolState(store, message, applyChange) {
  const gated = advanceLiveCursor(store, message)
  if (!gated.apply) return store
  const symbolKey = normalizeSeriesKey(message?.symbol_key || '')
  if (!symbolKey) return store
  const current = store?.symbolStates?.[symbolKey]
  if (!current) {
    warnDroppedSymbolDelta(symbolKey, message, store)
    return store
  }
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const next = applyChange({
    ...current,
    seq: Math.max(Number(current.seq || 0), Number(message?.scope_seq || 0)),
    last_event_at: message?.event_time || current.last_event_at || null,
    readiness: normalizeSelectedSymbolReadiness(current.readiness, {
      catalog_discovered: true,
      snapshot_ready: current.readiness?.snapshot_ready || true,
      symbol_live: true,
      run_live: store?.readiness?.run_live || current.readiness?.run_live,
    }),
  }, payload, current)
  return {
    ...commitSymbolState(store, symbolKey, next),
    streamSessionId: gated.streamSessionId,
    lastStreamSeq: gated.streamSeq,
  }
}

export function applyCandleDelta(store, message) {
  return withSymbolState(store, message, (next, payload) => {
    if (payload.candle && typeof payload.candle === 'object') {
      next.candles = mergeCanonicalCandles(next.candles || [], [payload.candle])
    }
    return next
  })
}

export function applyOverlayDeltaMessage(store, message) {
  return withSymbolState(store, message, (next, payload) => {
    if (Array.isArray(payload.ops)) {
      next.overlays = applyOverlayDelta(next.overlays || [], { ops: payload.ops })
    }
    return next
  })
}

export function applySignalDelta(store, message) {
  return withSymbolState(store, message, (next, payload) => {
    ;(Array.isArray(payload.entries) ? payload.entries : []).forEach((entry) => {
      if (!entry || typeof entry !== 'object') return
      next.signals = upsertTail(next.signals, entry, ['event_id', 'signal_id'], MAX_SIGNALS)
    })
    return next
  })
}

export function applyTradeDelta(store, message) {
  return withSymbolState(store, message, (next, payload) => {
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
  return withSymbolState(store, message, (next, payload) => {
    ;(Array.isArray(payload.entries) ? payload.entries : []).forEach((entry) => {
      if (!entry || typeof entry !== 'object') return
      next.logs = upsertTail(next.logs, entry, ['id', 'event_id'], MAX_LOGS)
    })
    return next
  })
}

export function applyDecisionDelta(store, message) {
  return withSymbolState(store, message, (next, payload) => {
    ;(Array.isArray(payload.entries) ? payload.entries : []).forEach((entry) => {
      if (!entry || typeof entry !== 'object') return
      next.decisions = upsertTail(next.decisions, entry, ['event_id', 'id'], MAX_DECISIONS)
    })
    return next
  })
}

export function applyStatsDelta(store, message) {
  return withSymbolState(store, message, (next, payload) => {
    if (payload.stats && typeof payload.stats === 'object') {
      next.stats = { ...payload.stats }
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
    case SYMBOL_SIGNAL_DELTA_TYPE:
      return applySignalDelta(store, message)
    case SYMBOL_TRADE_DELTA_TYPE:
      return applyTradeDelta(store, message)
    case SYMBOL_LOG_DELTA_TYPE:
      return applyLogDelta(store, message)
    case SYMBOL_DECISION_DELTA_TYPE:
      return applyDecisionDelta(store, message)
    case SYMBOL_STATS_DELTA_TYPE:
      return applyStatsDelta(store, message)
    default:
      return store
  }
}

export function selectSymbol(store, symbolKey) {
  const normalized = normalizeSeriesKey(symbolKey)
  if (!normalized) return store
  const symbolStateOrder = touchSymbolStateOrder(store?.symbolStateOrder, normalized, store?.symbolStateLimit)
  return {
    ...store,
    selectedSymbolKey: normalized,
    symbolStateOrder,
    symbolStates: trimSymbolStates(
      store?.symbolStates || {},
      symbolStateOrder,
      normalized,
      store?.symbolStateLimit,
    ),
  }
}

export function getSelectedSymbolState(store) {
  const symbolKey = normalizeSeriesKey(store?.selectedSymbolKey || '')
  if (!symbolKey) return null
  return store?.symbolStates?.[symbolKey] || null
}

export function getSelectedSymbolSlices(store) {
  const symbolState = getSelectedSymbolState(store)
  if (!symbolState) return null
  return {
    symbolState,
    metadata: {
      symbol_key: symbolState.symbol_key,
      instrument_id: symbolState.instrument_id,
      symbol: symbolState.symbol,
      timeframe: symbolState.timeframe,
      display_label: symbolState.display_label,
      status: symbolState.status,
      seq: symbolState.seq,
      last_event_at: symbolState.last_event_at,
      readiness: symbolState.readiness || null,
    },
    candles: Array.isArray(symbolState.candles) ? symbolState.candles : [],
    overlays: Array.isArray(symbolState.overlays) ? symbolState.overlays : [],
    signals: Array.isArray(symbolState.signals) ? symbolState.signals : [],
    recentTrades: Array.isArray(symbolState.recent_trades) ? symbolState.recent_trades : [],
    logs: Array.isArray(symbolState.logs) ? symbolState.logs : [],
    decisions: Array.isArray(symbolState.decisions) ? symbolState.decisions : [],
    runtime: symbolState.runtime && typeof symbolState.runtime === 'object' ? symbolState.runtime : {},
    stats: symbolState.stats && typeof symbolState.stats === 'object' ? symbolState.stats : {},
    continuity: symbolState.continuity || null,
  }
}
