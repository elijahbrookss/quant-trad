import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'
import { LocateFixed, Maximize2, Minimize2, X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { OverlayToggleBar } from './OverlayToggleBar.jsx'
import { ActiveTradeChip } from './ActiveTradeChip.jsx'
import DecisionTrace from './DecisionTrace/index.jsx'
import { useOverlayControls } from './hooks/useOverlayControls.js'
import { createLogger } from '../../utils/logger.js'
import {
  fetchBotActiveRun,
  fetchBotLensSeriesCatalog,
  fetchBotLensSeriesHistory,
  fetchBotLensSeriesWindow,
  fetchBotRunLedgerEvents,
  openBotLensSeriesLiveStream,
} from '../../adapters/bot.adapter.js'
import { shouldForceResyncForSeqGap } from './botlensStreamContract.js'
import { BOTLENS_PHASES, botlensReducer, initialBotLensState } from './botlensStateMachine.js'

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function formatPercent(value) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(1)}%`
}

function formatNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return value.toFixed(digits)
}

function normalizeEventPayload(message) {
  if (!message || typeof message !== 'object') return null
  if (String(message.type || '') !== 'botlens_live_tail') return null
  return {
    runId: message?.run_id ? String(message.run_id) : null,
    seq: Number(message?.seq || 0),
    messageType: String(message?.message_type || ''),
    payload: message?.payload && typeof message.payload === 'object' ? message.payload : {},
  }
}

function buildWindowSnapshot({ seriesKey, candles, status = 'running' }) {
  const [symbolRaw, timeframeRaw] = String(seriesKey || '').split('|')
  const symbol = String(symbolRaw || '').toUpperCase()
  const timeframe = String(timeframeRaw || '').toLowerCase()
  return {
    series: [
      {
        strategy_id: 'botlens',
        symbol,
        timeframe,
        candles: Array.isArray(candles) ? candles : [],
        overlays: [],
        stats: {},
      },
    ],
    trades: [],
    logs: [],
    decisions: [],
    warnings: [],
    runtime: { status },
  }
}

function readPositiveNumber(name, fallback) {
  const raw = import.meta.env?.[name]
  if (raw === undefined || raw === null || raw === '') return fallback
  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback
  return parsed
}

function readPositiveInt(name, fallback) {
  const value = readPositiveNumber(name, fallback)
  return Math.max(1, Math.floor(value))
}

const TARGET_RENDER_LAG_MS = readPositiveNumber('VITE_BOTLENS_TARGET_RENDER_LAG_MS', 120)
const CATCHUP_RENDER_LAG_MS = readPositiveNumber('VITE_BOTLENS_CATCHUP_RENDER_LAG_MS', 1200)
const CATCHUP_SEQ_BEHIND = readPositiveInt('VITE_BOTLENS_CATCHUP_SEQ_BEHIND', 6)
const CATCHUP_QUEUE_DEPTH = readPositiveInt('VITE_BOTLENS_CATCHUP_QUEUE_DEPTH', 8)
const NORMAL_APPLY_INTERVAL_MS = readPositiveNumber('VITE_BOTLENS_NORMAL_APPLY_INTERVAL_MS', 33)
const CATCHUP_APPLY_INTERVAL_MS = readPositiveNumber('VITE_BOTLENS_CATCHUP_APPLY_INTERVAL_MS', 12)
const MAX_CATCHUP_BATCH = readPositiveInt('VITE_BOTLENS_MAX_CATCHUP_BATCH', 2)
const METRICS_PUBLISH_MS = readPositiveNumber('VITE_BOTLENS_METRICS_PUBLISH_MS', 120)
const SNAP_TO_LATEST_CANDLE_LAG = readPositiveInt('VITE_BOTLENS_SNAP_CANDLES_BEHIND', 30)
const LEDGER_POLL_INTERVAL_MS = readPositiveInt('VITE_BOTLENS_LEDGER_POLL_MS', 800)
const LEDGER_POLL_LIMIT = readPositiveInt('VITE_BOTLENS_LEDGER_POLL_LIMIT', 500)
const LEDGER_MAX_EVENTS = readPositiveInt('VITE_BOTLENS_LEDGER_MAX_EVENTS', 3000)

function primarySeries(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return null
  const series = Array.isArray(snapshot.series) ? snapshot.series : null
  if (!series || !series.length) return null
  const entry = series[0]
  return entry && typeof entry === 'object' ? entry : null
}

function primarySeriesKey(snapshot) {
  const primary = primarySeries(snapshot)
  if (!primary) return null
  const symbol = String(primary.symbol || '').trim()
  const timeframe = String(primary.timeframe || '').trim()
  if (!symbol || !timeframe) return null
  return `${symbol}|${timeframe}`
}

function primaryCandles(snapshot) {
  const primary = primarySeries(snapshot)
  const candles = primary?.candles
  return Array.isArray(candles) ? candles : null
}

function primaryCandleCount(snapshot) {
  const candles = primaryCandles(snapshot)
  return Array.isArray(candles) ? candles.length : 0
}

function overlayIdentity(overlay, index) {
  if (!overlay || typeof overlay !== 'object') return `index:${index}`
  const keys = ['id', 'overlay_id', 'name', 'key', 'slug', 'indicator_id', 'type']
  for (const key of keys) {
    const value = String(overlay?.[key] || '').trim()
    if (value) return `${key}:${value}`
  }
  return `index:${index}`
}

function seriesIdentity(series, index) {
  if (!series || typeof series !== 'object') return `series_index:${index}`
  const strategyId = String(series?.strategy_id || '').trim()
  const symbol = String(series?.symbol || '').trim()
  const timeframe = String(series?.timeframe || '').trim()
  if (strategyId || symbol || timeframe) return `${strategyId}|${symbol}|${timeframe}`
  return `series_index:${index}`
}

function mergeSeriesOverlays(baseSeriesEntry, incomingSeriesEntry) {
  const incomingOverlays = Array.isArray(incomingSeriesEntry?.overlays) ? incomingSeriesEntry.overlays : []
  const overlayDelta = incomingSeriesEntry?.overlay_delta && typeof incomingSeriesEntry.overlay_delta === 'object'
    ? incomingSeriesEntry.overlay_delta
    : null
  const mode = String(overlayDelta?.mode || 'replace').toLowerCase()
  if (mode !== 'delta') return incomingOverlays

  const previousOverlays = Array.isArray(baseSeriesEntry?.overlays) ? baseSeriesEntry.overlays : []
  const mergedById = new Map()
  const overlayOrder = []
  previousOverlays.forEach((overlay, index) => {
    const id = overlayIdentity(overlay, index)
    if (!mergedById.has(id)) overlayOrder.push(id)
    mergedById.set(id, overlay)
  })
  incomingOverlays.forEach((overlay, index) => {
    const id = overlayIdentity(overlay, index)
    if (!mergedById.has(id)) overlayOrder.push(id)
    mergedById.set(id, overlay)
  })

  const removedIds = new Set(
    Array.isArray(overlayDelta?.removed) ? overlayDelta.removed.map((value) => String(value)) : [],
  )
  const merged = []
  overlayOrder.forEach((id) => {
    if (removedIds.has(String(id))) return
    const overlay = mergedById.get(id)
    if (overlay) merged.push(overlay)
  })
  return merged
}

function mergeSnapshotFromStreamDelta(baseSnapshot, incomingSnapshot) {
  if (!incomingSnapshot || typeof incomingSnapshot !== 'object') return null
  const incomingSeries = Array.isArray(incomingSnapshot?.series) ? incomingSnapshot.series : []
  if (!incomingSeries.length || !baseSnapshot || typeof baseSnapshot !== 'object') return incomingSnapshot

  const baseSeries = Array.isArray(baseSnapshot?.series) ? baseSnapshot.series : []
  const baseById = new Map()
  baseSeries.forEach((entry, index) => {
    if (!entry || typeof entry !== 'object') return
    baseById.set(seriesIdentity(entry, index), entry)
  })

  const mergedSeries = incomingSeries.map((entry, index) => {
    if (!entry || typeof entry !== 'object') return entry
    const baseEntry = baseById.get(seriesIdentity(entry, index))
    const overlays = mergeSeriesOverlays(baseEntry, entry)
    const next = { ...entry, overlays }
    if ('overlay_delta' in next) {
      delete next.overlay_delta
    }
    return next
  })
  return { ...incomingSnapshot, series: mergedSeries }
}

function candleLag(renderedSnapshot, canonicalSnapshot) {
  if (!renderedSnapshot || !canonicalSnapshot) return 0
  if (primarySeriesKey(renderedSnapshot) !== primarySeriesKey(canonicalSnapshot)) return 0
  const renderedCount = primaryCandleCount(renderedSnapshot)
  const canonicalCount = primaryCandleCount(canonicalSnapshot)
  if (!Number.isFinite(renderedCount) || !Number.isFinite(canonicalCount)) return 0
  return Math.max(0, canonicalCount - renderedCount)
}

function snapshotWithPrimaryCandles(snapshot, candles) {
  if (!snapshot || typeof snapshot !== 'object') return snapshot
  const series = Array.isArray(snapshot.series) ? snapshot.series : []
  if (!series.length) return snapshot
  const clonedSeries = series.slice()
  const first = clonedSeries[0]
  if (!first || typeof first !== 'object') return snapshot
  clonedSeries[0] = { ...first, candles: Array.isArray(candles) ? candles : [] }
  const next = { ...snapshot, series: clonedSeries }
  if (snapshot.runtime && typeof snapshot.runtime === 'object') {
    const runtime = { ...snapshot.runtime }
    if (Array.isArray(candles) && candles.length > 0) {
      const last = candles[candles.length - 1]
      if (last && typeof last === 'object') {
        runtime.last_bar = { ...last }
      }
    }
    next.runtime = runtime
  }
  return next
}

function buildSnapshotFrames({ baseSnapshot, targetSnapshot, envelope }) {
  const finalFrame = {
    runId: envelope.runId,
    seq: envelope.seq,
    snapshot: targetSnapshot,
    receivedAt: envelope.receivedAt,
    critical: envelope.critical,
    eventType: envelope.eventType,
    staged: false,
  }
  if (!baseSnapshot || !targetSnapshot) {
    return [finalFrame]
  }
  if (envelope.critical) {
    return [finalFrame]
  }
  if (primarySeriesKey(baseSnapshot) !== primarySeriesKey(targetSnapshot)) {
    return [finalFrame]
  }
  const baseTrades = Array.isArray(baseSnapshot?.trades) ? baseSnapshot.trades : []
  const targetTrades = Array.isArray(targetSnapshot?.trades) ? targetSnapshot.trades : []
  if (baseTrades.length !== targetTrades.length) {
    return [finalFrame]
  }
  const base = primaryCandles(baseSnapshot)
  const target = primaryCandles(targetSnapshot)
  if (!Array.isArray(base) || !Array.isArray(target)) {
    return [finalFrame]
  }
  const deltaBars = target.length - base.length
  if (deltaBars <= 1) {
    return [finalFrame]
  }
  const frames = []
  for (let length = base.length + 1; length < target.length; length += 1) {
    frames.push({
      runId: envelope.runId,
      seq: envelope.seq,
      snapshot: snapshotWithPrimaryCandles(targetSnapshot, target.slice(0, length)),
      receivedAt: envelope.receivedAt,
      critical: false,
      eventType: 'staged_bar_fill',
      staged: true,
    })
  }
  frames.push(finalFrame)
  return frames
}

function nowMs() {
  if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
    return performance.now()
  }
  return Date.now()
}

function eventSeq(event) {
  const value = Number(event?.seq || 0)
  if (Number.isFinite(value) && value > 0) return value
  return 0
}

function ledgerEventKey(event) {
  if (!event || typeof event !== 'object') return ''
  const eventId = String(event.event_id || '').trim()
  if (eventId) return `id:${eventId}`
  const seq = eventSeq(event)
  if (seq > 0) return `seq:${seq}`
  const eventName = String(event.event_name || '').trim()
  const createdAt = String(event.created_at || event.event_ts || '').trim()
  const tradeId = String(event.trade_id || '').trim()
  const symbol = String(event.symbol || '').trim()
  return `${createdAt}|${eventName}|${tradeId}|${symbol}`
}

function mergeLedgerEvents(existing, incoming) {
  const merged = new Map()
  const seed = Array.isArray(existing) ? existing : []
  const delta = Array.isArray(incoming) ? incoming : []
  seed.forEach((event) => {
    const key = ledgerEventKey(event)
    if (!key) return
    merged.set(key, event)
  })
  delta.forEach((event) => {
    const key = ledgerEventKey(event)
    if (!key) return
    merged.set(key, event)
  })
  const ordered = Array.from(merged.values()).sort((left, right) => {
    const seqGap = eventSeq(left) - eventSeq(right)
    if (seqGap !== 0) return seqGap
    const leftTs = String(left?.event_ts || left?.created_at || '')
    const rightTs = String(right?.event_ts || right?.created_at || '')
    return leftTs.localeCompare(rightTs)
  })
  if (ordered.length <= LEDGER_MAX_EVENTS) return ordered
  return ordered.slice(-LEDGER_MAX_EVENTS)
}

function normalizeSymbolKey(value) {
  return String(value || '').trim().toUpperCase()
}

function selectedSeriesKeyFor(entry, index) {
  if (!entry || typeof entry !== 'object') return `series-${index}`
  const strategyId = String(entry.strategy_id || '').trim()
  const symbol = normalizeSymbolKey(entry.symbol)
  const timeframe = String(entry.timeframe || '').trim().toUpperCase()
  return `${strategyId}|${symbol}|${timeframe}|${index}`
}

function isOpenTrade(trade) {
  if (!trade || typeof trade !== 'object') return false
  if (trade.closed_at) return false
  const status = String(trade.status || '').toLowerCase()
  if (status === 'closed' || status === 'completed' || status === 'complete') return false
  const legs = Array.isArray(trade.legs) ? trade.legs : []
  if (!legs.length) return true
  return legs.some((leg) => {
    if (!leg || typeof leg !== 'object') return false
    if (!leg.exit_time) return true
    const legStatus = String(leg.status || '').toLowerCase()
    return legStatus === 'open'
  })
}

function tradeMatchesSeries(trade, seriesEntry) {
  if (!seriesEntry || typeof seriesEntry !== 'object') return true
  const targetSymbol = normalizeSymbolKey(seriesEntry.symbol)
  if (!targetSymbol) return true
  const tradeSymbol = normalizeSymbolKey(trade?.symbol)
  if (!tradeSymbol) return true
  return tradeSymbol === targetSymbol
}

function buildTradeChip(trade) {
  if (!trade || typeof trade !== 'object') return null
  const direction = String(trade.direction || '').toLowerCase() === 'short' ? 'short' : 'long'
  const quantityRaw = Number(
    trade?.entry_order?.contracts ?? trade?.entry_order?.quantity ?? trade?.qty ?? trade?.quantity ?? trade?.contracts,
  )
  const quantityLabel = Number.isFinite(quantityRaw) && quantityRaw > 0 ? String(Number(quantityRaw.toFixed(4))) : null
  return {
    symbol: String(trade.symbol || '—'),
    direction,
    directionLabel: direction.toUpperCase(),
    sizeLabel: quantityLabel || `${Math.max((trade.legs || []).length, 1)}x`,
    entry: trade.entry_price,
  }
}

export function BotLensLiveModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotLensLiveModal'), [])
  const [snapshot, setSnapshot] = useState(null)
  const [streamState, setStreamState] = useState('idle')
  const [statusMessage, setStatusMessage] = useState('')
  const [error, setError] = useState(null)
  const [cursor, setCursor] = useState({ runId: null, seq: 0 })
  const [renderCursor, setRenderCursor] = useState({ runId: null, seq: 0 })
  const [ledgerEvents, setLedgerEvents] = useState([])
  const [ledgerState, setLedgerState] = useState({
    runId: null,
    nextAfterSeq: 0,
    status: 'idle',
    error: null,
  })
  const [staleMode, setStaleMode] = useState(false)
  const [overlayPanelCollapsed, setOverlayPanelCollapsed] = useState(false)
  const [followLive, setFollowLive] = useState(true)
  const [fullScreen, setFullScreen] = useState(false)
  const [selectedSeriesKey, setSelectedSeriesKey] = useState(null)
  const [hoveredTradeId, setHoveredTradeId] = useState(null)
  const [renderMetrics, setRenderMetrics] = useState({
    mode: 'smooth',
    queueDepth: 0,
    seqBehind: 0,
    candlesBehind: 0,
    lagMs: 0,
    appliedRate: 0,
  })
  const [lensState, dispatchLens] = useReducer(botlensReducer, initialBotLensState)
  const socketRef = useRef(null)
  const cursorRef = useRef({ runId: null, seq: 0 })
  const renderCursorRef = useRef({ runId: null, seq: 0 })
  const streamStateRef = useRef('idle')
  const syncInFlightRef = useRef(false)
  const syncTokenRef = useRef(0)
  const mountedRef = useRef(false)
  const pendingFramesRef = useRef([])
  const animationFrameRef = useRef(0)
  const lastApplyAtRef = useRef(0)
  const lastMetricsPublishRef = useRef(0)
  const appliedSincePublishRef = useRef(0)
  const renderedSnapshotRef = useRef(null)
  const canonicalFrameRef = useRef({ runId: null, seq: 0, snapshot: null })
  const lastSnapCursorRef = useRef({ runId: null, seq: 0 })
  const ledgerSyncTokenRef = useRef(0)
  const lastCursorPublishRef = useRef(0)
  const activeSeriesKeyRef = useRef(null)

  useEffect(() => {
    cursorRef.current = cursor
  }, [cursor])

  useEffect(() => {
    renderCursorRef.current = renderCursor
  }, [renderCursor])

  useEffect(() => {
    streamStateRef.current = streamState
  }, [streamState])

  useEffect(() => {
    if (!open) {
      setFullScreen(false)
      setHoveredTradeId(null)
    }
  }, [open])

  useEffect(() => {
    setFollowLive(true)
  }, [bot?.id])


  useEffect(() => {
    if (!open) return undefined
    const onVisibility = () => {
      if (document.visibilityState === 'hidden') {
        pendingFramesRef.current = []
        const socket = socketRef.current
        socketRef.current = null
        if (socket) {
          try {
            socket.close()
          } catch {
            // no-op
          }
        }
      } else if (document.visibilityState === 'visible') {
        syncTokenRef.current += 1
      }
    }
    document.addEventListener('visibilitychange', onVisibility)
    return () => document.removeEventListener('visibilitychange', onVisibility)
  }, [open])

  useEffect(() => {
    if (!open) return undefined
    let cancelled = false

    const publishMetrics = (ts, mode) => {
      const queueDepth = pendingFramesRef.current.length
      const canonical = cursorRef.current
      const rendered = renderCursorRef.current
      const sameRun = canonical?.runId && rendered?.runId && canonical.runId === rendered.runId
      const seqBehind = sameRun ? Math.max(0, Number(canonical.seq || 0) - Number(rendered.seq || 0)) : queueDepth
      const candlesBehind = candleLag(renderedSnapshotRef.current, canonicalFrameRef.current?.snapshot || null)
      const oldest = queueDepth ? pendingFramesRef.current[0] : null
      const lagMs = oldest ? Math.max(0, ts - Number(oldest.receivedAt || ts)) : 0
      const elapsed = Math.max(1, ts - Number(lastMetricsPublishRef.current || ts))
      const appliedRate = (Number(appliedSincePublishRef.current || 0) * 1000) / elapsed
      appliedSincePublishRef.current = 0
      lastMetricsPublishRef.current = ts
      setRenderMetrics({
        mode,
        queueDepth,
        seqBehind,
        candlesBehind,
        lagMs,
        appliedRate,
      })
      if (ts - Number(lastCursorPublishRef.current || 0) >= Math.max(80, METRICS_PUBLISH_MS)) {
        setCursor({ runId: canonical?.runId || null, seq: Number(canonical?.seq || 0) })
        lastCursorPublishRef.current = ts
      }
    }

    const step = (ts) => {
      if (cancelled) return
      const queue = pendingFramesRef.current
      const canonical = cursorRef.current
      const rendered = renderCursorRef.current
      const sameRun = canonical?.runId && rendered?.runId && canonical.runId === rendered.runId
      const seqBehind = sameRun ? Math.max(0, Number(canonical.seq || 0) - Number(rendered.seq || 0)) : queue.length
      const canonicalFrame = canonicalFrameRef.current
      const renderedSnapshot = renderedSnapshotRef.current
      const candlesBehind = candleLag(renderedSnapshot, canonicalFrame?.snapshot || null)
      const shouldSnapToLatest =
        candlesBehind > SNAP_TO_LATEST_CANDLE_LAG &&
        canonicalFrame?.snapshot &&
        canonical?.runId &&
        canonicalFrame.runId === canonical.runId
      if (shouldSnapToLatest) {
        queue.length = 0
        setSnapshot(canonicalFrame.snapshot)
        renderedSnapshotRef.current = canonicalFrame.snapshot
        const snappedCursor = { runId: canonicalFrame.runId, seq: Number(canonicalFrame.seq || 0) }
        renderCursorRef.current = snappedCursor
        setRenderCursor(snappedCursor)
        lastApplyAtRef.current = ts
        appliedSincePublishRef.current += 1
        const lastSnap = lastSnapCursorRef.current
        if (lastSnap.runId !== snappedCursor.runId || Number(lastSnap.seq || 0) !== snappedCursor.seq) {
          logger.info('botlens_render_snap_to_latest', {
            bot_id: bot?.id || null,
            run_id: snappedCursor.runId,
            seq: snappedCursor.seq,
            candles_behind: candlesBehind,
          })
          lastSnapCursorRef.current = snappedCursor
        }
      }
      const oldest = queue.length ? queue[0] : null
      const oldestLagMs = oldest ? Math.max(0, ts - Number(oldest.receivedAt || ts)) : 0
      const catchupMode =
        queue.length >= CATCHUP_QUEUE_DEPTH ||
        seqBehind >= CATCHUP_SEQ_BEHIND ||
        oldestLagMs >= CATCHUP_RENDER_LAG_MS
      const mode = catchupMode ? 'catchup' : 'smooth'
      const minInterval = catchupMode ? CATCHUP_APPLY_INTERVAL_MS : NORMAL_APPLY_INTERVAL_MS
      const maxBatch = catchupMode ? Math.min(MAX_CATCHUP_BATCH, queue.length) : 1

      let applied = 0
      while (queue.length > 0 && applied < maxBatch) {
        if (ts - lastApplyAtRef.current < minInterval) break
        const head = queue[0]
        const headLagMs = Math.max(0, ts - Number(head.receivedAt || ts))
        const holdForSmooth = !catchupMode && headLagMs < TARGET_RENDER_LAG_MS
        if (holdForSmooth) break
        const nextFrame = queue.shift()
        setSnapshot(nextFrame.snapshot)
        renderedSnapshotRef.current = nextFrame.snapshot
        renderCursorRef.current = { runId: nextFrame.runId, seq: nextFrame.seq }
        setRenderCursor({ runId: nextFrame.runId, seq: nextFrame.seq })
        lastApplyAtRef.current = ts
        applied += 1
      }

      if (applied > 0) {
        appliedSincePublishRef.current += applied
      }
      if (ts - lastMetricsPublishRef.current >= METRICS_PUBLISH_MS) {
        publishMetrics(ts, mode)
      }
      animationFrameRef.current = window.requestAnimationFrame(step)
    }

    animationFrameRef.current = window.requestAnimationFrame(step)
    return () => {
      cancelled = true
      if (animationFrameRef.current) {
        window.cancelAnimationFrame(animationFrameRef.current)
        animationFrameRef.current = 0
      }
      pendingFramesRef.current = []
      lastApplyAtRef.current = 0
      lastMetricsPublishRef.current = 0
      appliedSincePublishRef.current = 0
      canonicalFrameRef.current = { runId: null, seq: 0, snapshot: null }
      lastSnapCursorRef.current = { runId: null, seq: 0 }
    }
  }, [bot?.id, logger, open])

  const closeSocket = useCallback(() => {
    const socket = socketRef.current
    socketRef.current = null
    if (!socket) return
    try {
      socket.onopen = null
      socket.onmessage = null
      socket.onerror = null
      socket.onclose = null
      socket.close()
    } catch {
      // no-op
    }
  }, [])

  const connectSocket = useCallback(
    ({ botId, runId, seq, token }) => {
      closeSocket()
      const socket = openBotLensSeriesLiveStream(runId, activeSeriesKeyRef.current || primarySeriesKey(renderedSnapshotRef.current) || 'UNKNOWN|1m', { afterSeq: seq })
      if (!socket) {
        setStreamState('stale')
        setStaleMode(true)
        setStatusMessage('WebSocket unavailable. Retrying bootstrap...')
        return
      }
      socketRef.current = socket
      setStreamState('connecting')

      socket.onopen = () => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        setStreamState('open')
        setStatusMessage('Live stream connected.')
        logger.info('botlens_ws_open', { bot_id: botId, run_id: runId, seq })
      }

      socket.onmessage = (event) => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        try {
          const message = JSON.parse(event.data)
          const next = normalizeEventPayload(message)
          if (!next || !next.runId || next.seq <= 0) return

          const prev = cursorRef.current
          if (prev.runId && next.runId === prev.runId) {
            if (next.seq <= prev.seq) return
            if (shouldForceResyncForSeqGap({ previousSeq: prev.seq, nextSeq: next.seq, maxAllowedGap: 1 })) {
              pendingFramesRef.current = []
              renderedSnapshotRef.current = null
              canonicalFrameRef.current = { runId: null, seq: 0, snapshot: null }
              setStaleMode(true)
              setStreamState('resyncing')
              setStatusMessage(`Stream gap detected (${prev.seq} -> ${next.seq}). Re-syncing snapshot...`)
              dispatchLens({ type: 'SEQ_GAP' })
              logger.warn('botlens_stream_seq_gap', {
                bot_id: botId,
                run_id: next.runId,
                expected_seq: prev.seq + 1,
                actual_seq: next.seq,
              })
              syncTokenRef.current += 1
              closeSocket()
              return
            }
          } else if (prev.runId && next.runId !== prev.runId) {
            pendingFramesRef.current = []
            renderedSnapshotRef.current = null
            canonicalFrameRef.current = { runId: null, seq: 0, snapshot: null }
            setStaleMode(true)
            setStreamState('resyncing')
            setStatusMessage(`Run changed (${prev.runId} -> ${next.runId}). Auto-attaching to latest run...`)
            syncTokenRef.current += 1
            closeSocket()
            return
          }

          const receivedAt = nowMs()
          const queue = pendingFramesRef.current
          const baseSnapshot = queue.length > 0
            ? queue[queue.length - 1]?.snapshot || null
            : renderedSnapshotRef.current
          const base = baseSnapshot && typeof baseSnapshot === 'object' ? baseSnapshot : buildWindowSnapshot({
            seriesKey: activeSeriesKeyRef.current || 'UNKNOWN|1m',
            candles: [],
            status: 'running',
          })
          const bars = primaryCandles(base) || []
          let nextBars = bars
          if (next.messageType === 'bar_append' && next.payload?.bar) {
            nextBars = [...bars, next.payload.bar]
          } else if (next.messageType === 'bar_update' && next.payload?.bar) {
            const bar = next.payload.bar
            const tail = bars.length ? bars[bars.length - 1] : null
            if (tail && Number(tail.time) === Number(bar.time)) {
              nextBars = [...bars.slice(0, -1), bar]
            } else {
              nextBars = [...bars, bar]
            }
          }
          const mergedSnapshot = snapshotWithPrimaryCandles(base, nextBars)
          if (next.messageType === 'status') {
            mergedSnapshot.runtime = { ...(mergedSnapshot.runtime || {}), status: String(next.payload?.status || 'running') }
          }
          const frames = [{
            runId: next.runId,
            seq: next.seq,
            snapshot: mergedSnapshot,
            receivedAt,
            critical: false,
            eventType: next.messageType || 'live_tail',
            staged: false,
          }]
          canonicalFrameRef.current = { runId: next.runId, seq: next.seq, snapshot: mergedSnapshot }
          pendingFramesRef.current.push(...frames)
          cursorRef.current = { runId: next.runId, seq: next.seq }
          setStreamState('open')
          setStaleMode(false)
          setStatusMessage(`Live update (${next.messageType || 'tail'})`)
        } catch (err) {
          logger.warn('botlens_ws_parse_failed', { bot_id: botId }, err)
        }
      }

      socket.onerror = (err) => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        setStaleMode(true)
        setStreamState('stale')
        setStatusMessage('WebSocket error. Keeping stale snapshot and retrying bootstrap...')
        logger.warn('botlens_ws_error', { bot_id: botId, run_id: runId }, err)
      }

      socket.onclose = () => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        setStaleMode(true)
        setStreamState('stale')
        setStatusMessage('WebSocket closed. Keeping stale snapshot and retrying bootstrap...')
        syncTokenRef.current += 1
      }
    },
    [closeSocket, logger],
  )

  useEffect(() => {
    if (!open || !bot?.id) return undefined
    mountedRef.current = true
    let cancelled = false
    const bootstrapAndConnect = async (reason) => {
      if (syncInFlightRef.current) return
      syncInFlightRef.current = true
      const token = ++syncTokenRef.current
      if (reason) {
        setStaleMode(true)
        setStatusMessage(reason)
      } else {
        setStreamState('bootstrapping')
        setStatusMessage('Loading latest BotLens snapshot...')
      }
      while (!cancelled && mountedRef.current && token === syncTokenRef.current) {
        try {
          const active = await fetchBotActiveRun(bot.id)
          const runId = active?.run_id ? String(active.run_id) : null
          if (!runId) {
            setStreamState('waiting')
            setStatusMessage('Runtime has not emitted snapshot events yet. Retrying...')
            await delay(1200)
            continue
          }
          pendingFramesRef.current = []
          lastApplyAtRef.current = nowMs()
          const catalog = await fetchBotLensSeriesCatalog(runId)
          const availableSeries = Array.isArray(catalog?.series) ? catalog.series : []
          const bootSeriesKey = activeSeriesKeyRef.current || availableSeries[0] || null
          if (!bootSeriesKey) {
            setStreamState('waiting')
            setStatusMessage('No series available yet. Retrying...')
            await delay(1200)
            continue
          }
          activeSeriesKeyRef.current = bootSeriesKey
          const window = await fetchBotLensSeriesWindow(runId, bootSeriesKey, { to: 'now', limit: 320 })
          const windowCandles = Array.isArray(window?.window?.candles) ? window.window.candles : []
          const seeded = buildWindowSnapshot({ seriesKey: bootSeriesKey, candles: windowCandles, status: window?.window?.status || 'running' })
          setSnapshot(seeded)
          renderedSnapshotRef.current = seeded
          const startSeq = Number(window?.seq || 0)
          canonicalFrameRef.current = { runId, seq: startSeq, snapshot: seeded }
          dispatchLens({ type: 'BOOTSTRAP_SUCCESS', runId, seriesKey: bootSeriesKey, seq: startSeq, candles: windowCandles })
          lastSnapCursorRef.current = { runId: null, seq: 0 }
          cursorRef.current = { runId, seq: startSeq }
          renderCursorRef.current = { runId, seq: startSeq }
          setCursor({ runId, seq: startSeq })
          setRenderCursor({ runId, seq: startSeq })
          setRenderMetrics({
            mode: 'smooth',
            queueDepth: 0,
            seqBehind: 0,
            candlesBehind: 0,
            lagMs: 0,
            appliedRate: 0,
          })
          setError(null)
          setStaleMode(false)
          setStatusMessage('Snapshot loaded. Connecting live stream...')
          connectSocket({ botId: bot.id, runId, seq: startSeq, token })
          syncInFlightRef.current = false
          return
        } catch (err) {
          logger.warn('botlens_bootstrap_failed', { bot_id: bot.id }, err)
          setStaleMode(true)
          setStreamState('stale')
          setError(err?.message || 'Unable to bootstrap BotLens state.')
          setStatusMessage('Bootstrap failed. Keeping stale view and retrying...')
          await delay(1500)
        }
      }
      syncInFlightRef.current = false
    }

    bootstrapAndConnect('')

    const monitor = setInterval(() => {
      if (!mountedRef.current || cancelled) return
      if (streamStateRef.current === 'open') return
      bootstrapAndConnect('Resyncing BotLens snapshot...')
    }, 2500)

    return () => {
      cancelled = true
      mountedRef.current = false
      clearInterval(monitor)
      syncInFlightRef.current = false
      closeSocket()
    }
  }, [bot?.id, connectSocket, closeSocket, logger, open])

  const activeRunId = cursor.runId || renderCursor.runId || null

  useEffect(() => {
    if (!open || !bot?.id) {
      setLedgerEvents([])
      setLedgerState({
        runId: null,
        nextAfterSeq: 0,
        status: 'idle',
        error: null,
      })
      return undefined
    }
    if (!activeRunId) {
      setLedgerEvents([])
      setLedgerState({
        runId: null,
        nextAfterSeq: 0,
        status: 'waiting',
        error: null,
      })
      return undefined
    }

    let cancelled = false
    const token = ++ledgerSyncTokenRef.current
    let nextAfterSeq = 0
    setLedgerEvents([])
    setLedgerState({
      runId: activeRunId,
      nextAfterSeq: 0,
      status: 'syncing',
      error: null,
    })

    const pollLedger = async () => {
      while (!cancelled && token === ledgerSyncTokenRef.current) {
        try {
          const response = await fetchBotRunLedgerEvents(bot.id, activeRunId, {
            afterSeq: nextAfterSeq,
            limit: LEDGER_POLL_LIMIT,
          })
          if (cancelled || token !== ledgerSyncTokenRef.current) return
          const incoming = Array.isArray(response?.events) ? response.events : []
          const cursorCandidate = Number(response?.next_after_seq || nextAfterSeq)
          if (Number.isFinite(cursorCandidate) && cursorCandidate > nextAfterSeq) {
            nextAfterSeq = cursorCandidate
          } else {
            const maxSeq = incoming.reduce((acc, event) => Math.max(acc, eventSeq(event)), nextAfterSeq)
            nextAfterSeq = maxSeq
          }
          if (incoming.length > 0) {
            setLedgerEvents((current) => mergeLedgerEvents(current, incoming))
          }
          setLedgerState({
            runId: activeRunId,
            nextAfterSeq,
            status: 'open',
            error: null,
          })
          const isCatchupBatch = incoming.length >= LEDGER_POLL_LIMIT
          await delay(isCatchupBatch ? 80 : LEDGER_POLL_INTERVAL_MS)
        } catch (err) {
          if (cancelled || token !== ledgerSyncTokenRef.current) return
          const message = err?.message || 'Ledger query failed'
          setLedgerState({
            runId: activeRunId,
            nextAfterSeq,
            status: 'error',
            error: message,
          })
          logger.warn('botlens_db_ledger_poll_failed', {
            bot_id: bot.id,
            run_id: activeRunId,
            after_seq: nextAfterSeq,
          }, err)
          await delay(Math.max(1200, LEDGER_POLL_INTERVAL_MS))
        }
      }
    }

    pollLedger()
    return () => {
      cancelled = true
    }
  }, [activeRunId, bot?.id, logger, open])

  const series = useMemo(() => (Array.isArray(snapshot?.series) ? snapshot.series : []), [snapshot])
  const seriesSelectorOptions = useMemo(
    () =>
      series.map((entry, index) => ({
        key: selectedSeriesKeyFor(entry, index),
        symbol: String(entry?.symbol || '—'),
        timeframe: String(entry?.timeframe || '—'),
      })),
    [series],
  )
  useEffect(() => {
    setSelectedSeriesKey((current) => {
      if (!seriesSelectorOptions.length) return null
      if (current && seriesSelectorOptions.some((entry) => entry.key === current)) return current
      return seriesSelectorOptions[0].key
    })
  }, [seriesSelectorOptions])
  const selectedSeries = useMemo(() => {
    if (!series.length) return {}
    const selectedIndex = series.findIndex((entry, index) => selectedSeriesKeyFor(entry, index) === selectedSeriesKey)
    return selectedIndex >= 0 ? series[selectedIndex] : series[0]
  }, [selectedSeriesKey, series])
  const candles = useMemo(() => (Array.isArray(selectedSeries?.candles) ? selectedSeries.candles : []), [selectedSeries])
  const overlays = useMemo(() => (Array.isArray(selectedSeries?.overlays) ? selectedSeries.overlays : []), [selectedSeries])
  const trades = useMemo(() => (Array.isArray(snapshot?.trades) ? snapshot.trades : []), [snapshot])
  const chartTrades = useMemo(() => trades.filter((trade) => tradeMatchesSeries(trade, selectedSeries)), [selectedSeries, trades])
  const allActiveTrades = useMemo(() => trades.filter((trade) => isOpenTrade(trade)), [trades])
  const seriesPriceContext = useMemo(() => {
    const map = new Map()
    series.forEach((entry) => {
      const symbol = normalizeSymbolKey(entry?.symbol)
      if (!symbol) return
      const seriesCandles = Array.isArray(entry?.candles) ? entry.candles : []
      const last = seriesCandles.length ? seriesCandles[seriesCandles.length - 1] : null
      const epoch = Number(last?.time)
      const isoBarTime = Number.isFinite(epoch) ? new Date(epoch * 1000).toISOString() : null
      map.set(symbol, {
        currentPrice: Number(last?.close),
        latestBarTime: isoBarTime,
      })
    })
    return map
  }, [series])
  const tradeCards = useMemo(
    () =>
      allActiveTrades
        .map((trade, index) => ({
          id: String(trade?.trade_id || `${trade?.entry_time || ''}|${trade?.symbol || ''}|${index}`),
          trade,
          chip: buildTradeChip(trade),
        }))
        .filter((entry) => entry.chip),
    [allActiveTrades],
  )
  const logs = useMemo(() => (Array.isArray(snapshot?.logs) ? snapshot.logs : []), [snapshot])
  const decisionEvents = useMemo(() => (Array.isArray(ledgerEvents) ? ledgerEvents : []), [ledgerEvents])
  const runtime = snapshot?.runtime || bot?.runtime || {}
  const stats = runtime?.stats || {}
  const status = runtime?.status || bot?.status || 'idle'
  const { overlayOptions, visibility, visibleOverlays, toggleOverlay } = useOverlayControls({ overlays })
  const seriesStats = useMemo(() => {
    return series.map((entry) => {
      const entryStats = entry?.stats || {}
      return {
        key: `${entry?.symbol || 'symbol'}|${entry?.timeframe || 'tf'}`,
        symbol: entry?.symbol || '—',
        timeframe: entry?.timeframe || '—',
        trades: Number(entryStats?.total_trades || 0),
        netPnl: Number(entryStats?.net_pnl || 0),
        winRate: Number(entryStats?.win_rate || 0),
        maxDrawdown: Number(entryStats?.max_drawdown || 0),
      }
    })
  }, [series])
  const selectedSeriesLabel = `${selectedSeries?.symbol || '—'} ${selectedSeries?.timeframe || ''}`.trim()

  useEffect(() => {
    if (!selectedSeries || !selectedSeries.symbol || !selectedSeries.timeframe) return
    const nextSeriesKey = `${String(selectedSeries.symbol || '').toUpperCase()}|${String(selectedSeries.timeframe || '').toLowerCase()}`
    if (activeSeriesKeyRef.current === nextSeriesKey) return
    activeSeriesKeyRef.current = nextSeriesKey
    if (cursorRef.current?.runId) {
      syncTokenRef.current += 1
      closeSocket()
      setStreamState('resyncing')
      setStatusMessage(`Switching series to ${nextSeriesKey}...`)
    }
  }, [closeSocket, selectedSeries])


  const loadOlderHistory = useCallback(async () => {
    const runId = cursorRef.current?.runId
    const sk = selectedSeries ? `${String(selectedSeries.symbol || '').toUpperCase()}|${String(selectedSeries.timeframe || '').toLowerCase()}` : null
    if (!runId || !sk) return
    const oldest = candles.length ? candles[0] : null
    const beforeTs = oldest?.time ? new Date(Number(oldest.time) * 1000).toISOString() : undefined
    try {
      const page = await fetchBotLensSeriesHistory(runId, sk, { beforeTs, limit: 240 })
      const pageCandles = Array.isArray(page?.history?.candles) ? page.history.candles : []
      if (!pageCandles.length) return
      dispatchLens({ type: 'HISTORY_PAGE_SUCCESS', candles: pageCandles })
      setSnapshot((current) => {
        const merged = [...pageCandles, ...(primaryCandles(current) || [])]
        return snapshotWithPrimaryCandles(current, merged)
      })
    } catch (err) {
      logger.warn('botlens_history_page_failed', { run_id: runId, series_key: sk }, err)
    }
  }, [candles, logger, selectedSeries])

  useEffect(() => {
    if (!hoveredTradeId) return
    const stillVisible = tradeCards.some((entry) => entry.id && entry.id === hoveredTradeId)
    if (!stillVisible) setHoveredTradeId(null)
  }, [hoveredTradeId, tradeCards])

  if (!open || !bot) return null

  const modalShellClassName = fullScreen
    ? 'h-screen w-full max-w-none overflow-hidden border-0 bg-slate-950 shadow-2xl'
    : 'h-[86vh] w-full max-w-6xl overflow-hidden rounded-2xl border border-slate-800 bg-slate-950 shadow-2xl'
  const modalBodyHeightClass = fullScreen ? 'h-[calc(100vh-58px)]' : 'h-[calc(86vh-58px)]'

  return (
    <div
      className={`fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/70 ${fullScreen ? 'p-0' : 'p-4'}`}
      onClick={onClose}
    >
      <div
        className={modalShellClassName}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <div>
            <p className="text-sm font-semibold text-slate-100">BotLens Live</p>
            <p className="text-xs text-slate-500">
              bot_id={bot.id} · run={renderCursor.runId || cursor.runId || '—'} · seq(render/canon)={renderCursor.seq || 0}/{cursor.seq || 0} · stream={streamState}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setFollowLive((prev) => !prev)}
              className={`inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-xs font-medium transition-colors ${
                followLive
                  ? 'border-emerald-600/70 bg-emerald-500/15 text-emerald-200'
                  : 'border-slate-700 text-slate-300 hover:border-slate-600 hover:text-slate-100'
              }`}
              aria-pressed={followLive}
              title="Keep chart pinned to the latest bar while streaming"
            >
              <LocateFixed className="size-3.5" />
              Follow Live
            </button>
            <button
              type="button"
              onClick={() => setFullScreen((prev) => !prev)}
              className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-700 text-slate-300 transition-colors hover:border-slate-600 hover:text-slate-100"
              aria-label={fullScreen ? 'Exit full screen' : 'Enter full screen'}
              title={fullScreen ? 'Exit full screen' : 'Full screen'}
            >
              {fullScreen ? <Minimize2 className="size-4" /> : <Maximize2 className="size-4" />}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-700 text-slate-400 hover:text-slate-200"
              aria-label="Close"
            >
              <X className="size-4" />
            </button>
          </div>
        </div>

        <div className={`${modalBodyHeightClass} overflow-auto p-4`}>
          {statusMessage ? (
            <div
              className={`mb-3 rounded border px-3 py-2 text-sm ${
                staleMode
                  ? 'border-amber-700/70 bg-amber-950/30 text-amber-200'
                  : 'border-slate-800 bg-slate-900/40 text-slate-300'
              }`}
            >
              {staleMode ? 'Read-only stale mode: ' : ''}
              {statusMessage}
            </div>
          ) : null}
          {error ? <div className="mb-3 rounded border border-rose-800/60 bg-rose-950/30 px-3 py-2 text-sm text-rose-200">{error}</div> : null}
          {ledgerState?.error ? (
            <div className="mb-3 rounded border border-amber-700/60 bg-amber-950/30 px-3 py-2 text-sm text-amber-200">
              DB ledger unavailable: {ledgerState.error}
            </div>
          ) : null}
          {!snapshot ? (
            <div className="rounded border border-slate-800 bg-slate-900/40 px-4 py-8 text-sm text-slate-400">
              Waiting for runtime snapshot data...
            </div>
          ) : (
            <>
              <div className="mb-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Runtime Status</p>
                  <p className="text-sm font-semibold text-slate-200">{status || '—'}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Progress</p>
                  <p className="text-sm font-semibold text-slate-200">{formatPercent(runtime?.progress)}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Trades</p>
                  <p className="text-sm font-semibold text-slate-200">{stats?.total_trades ?? trades.length}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Net P&L</p>
                  <p className="text-sm font-semibold text-slate-200">{formatNumber(stats?.net_pnl)}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Win Rate</p>
                  <p className="text-sm font-semibold text-slate-200">{formatPercent(stats?.win_rate)}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Next Bar In</p>
                  <p className="text-sm font-semibold text-slate-200">
                    {runtime?.next_bar_in_seconds ?? '—'}{runtime?.next_bar_in_seconds !== undefined ? 's' : ''}
                  </p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Series</p>
                  <p className="text-sm font-semibold text-slate-200">{series.length}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Ledger / Logs</p>
                  <p className="text-sm font-semibold text-slate-200">{decisionEvents.length} / {logs.length}</p>
                  <p className="text-[10px] text-slate-500">db={ledgerState.status || 'idle'} · after_seq={ledgerState.nextAfterSeq || 0}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Render Queue</p>
                  <p className="text-sm font-semibold text-slate-200">{renderMetrics.queueDepth}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Render Lag</p>
                  <p className="text-sm font-semibold text-slate-200">{Math.round(renderMetrics.lagMs)}ms</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Render Mode</p>
                  <p className="text-sm font-semibold text-slate-200">
                    {renderMetrics.mode} · {renderMetrics.seqBehind} seq · {renderMetrics.candlesBehind} candles · {formatNumber(renderMetrics.appliedRate, 1)}/s
                  </p>
                </div>
              </div>
              {seriesSelectorOptions.length ? (
                <div className="mb-3 rounded border border-slate-800 bg-slate-900/40 p-2.5">
                  <div className="mb-2 flex items-center justify-between">
                    <p className="text-[10px] uppercase tracking-[0.28em] text-slate-500">Symbols</p>
                    <p className="text-xs text-slate-400">Viewing: {selectedSeriesLabel || '—'}</p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {seriesSelectorOptions.map((entry) => {
                      const selected = entry.key === selectedSeriesKey
                      return (
                        <button
                          key={entry.key}
                          type="button"
                          onClick={() => setSelectedSeriesKey(entry.key)}
                          className={`rounded-md border px-2.5 py-1 text-xs font-medium uppercase tracking-wide transition-colors ${
                            selected
                              ? 'border-sky-500/60 bg-sky-500/20 text-sky-100'
                              : 'border-slate-700 bg-slate-900/40 text-slate-300 hover:border-slate-600 hover:text-slate-100'
                          }`}
                          aria-pressed={selected}
                        >
                          {entry.symbol} · {entry.timeframe}
                        </button>
                      )
                    })}
                  </div>
                </div>
              ) : null}
              <div className="mb-2 flex items-center justify-between">
                <p className="text-[11px] uppercase tracking-[0.24em] text-slate-500">{lensState.phase}</p>
                <button
                  type="button"
                  onClick={loadOlderHistory}
                  className="rounded border border-slate-700 px-2 py-1 text-xs text-slate-300 hover:border-slate-500"
                >
                  Load older
                </button>
              </div>
              <div className="mb-3">
                <OverlayToggleBar
                  overlays={overlayOptions}
                  visibility={visibility}
                  onToggle={toggleOverlay}
                  collapsed={overlayPanelCollapsed}
                  onToggleCollapse={() => setOverlayPanelCollapsed((prev) => !prev)}
                />
              </div>
              <BotLensChart
                chartId={`botlens-live-${bot.id}`}
                candles={candles}
                trades={chartTrades}
                overlays={visibleOverlays}
                mode={bot.mode}
                playbackSpeed={Number(bot.playback_speed || 0)}
                timeframe={selectedSeries?.timeframe || null}
                overlayVisibility={visibility}
                followLive={followLive}
              />
              <div className="mt-3 rounded border border-slate-800 bg-slate-900/50 p-3">
                <div className="mb-2 flex items-center justify-between">
                  <p className="text-[11px] uppercase tracking-[0.28em] text-slate-500">Live Trades</p>
                  <p className="text-xs text-slate-500">{tradeCards.length} open</p>
                </div>
                {tradeCards.length ? (
                  <div className="grid gap-2 md:grid-cols-2">
                    {tradeCards.map((entry) => {
                      const symbolKey = normalizeSymbolKey(entry.trade?.symbol)
                      const selectedSymbolKey = normalizeSymbolKey(selectedSeries?.symbol)
                      const context = symbolKey ? seriesPriceContext.get(symbolKey) : null
                      const matchingSeries = seriesSelectorOptions.find(
                        (option) => normalizeSymbolKey(option.symbol) === symbolKey,
                      )
                      return (
                        <ActiveTradeChip
                          key={entry.id || `${entry.trade?.entry_time || 'trade'}|${entry.trade?.symbol || 'symbol'}`}
                          chip={entry.chip}
                          trade={entry.trade}
                          currentPrice={context?.currentPrice}
                          latestBarTime={context?.latestBarTime}
                          visible={!hoveredTradeId || hoveredTradeId === entry.id}
                          onHover={(hovering) => setHoveredTradeId(hovering ? entry.id : null)}
                          isActiveSymbol={selectedSymbolKey ? symbolKey === selectedSymbolKey : true}
                          onClick={() => {
                            if (matchingSeries?.key) setSelectedSeriesKey(matchingSeries.key)
                          }}
                        />
                      )
                    })}
                  </div>
                ) : (
                  <div className="rounded border border-dashed border-slate-800 px-3 py-4 text-sm text-slate-500">
                    No active trades right now.
                  </div>
                )}
              </div>
              {seriesStats.length ? (
                <div className="mt-4 rounded border border-slate-800 bg-slate-900/50 p-3">
                  <div className="mb-2 flex items-center justify-between">
                    <p className="text-[11px] uppercase tracking-[0.28em] text-slate-500">Per Symbol Stats</p>
                    <p className="text-xs text-slate-500">{seriesStats.length} symbols</p>
                  </div>
                  <div className="overflow-auto">
                    <table className="min-w-full text-left text-xs text-slate-300">
                      <thead className="text-[10px] uppercase tracking-[0.25em] text-slate-500">
                        <tr>
                          <th className="px-2 py-1">Symbol</th>
                          <th className="px-2 py-1">TF</th>
                          <th className="px-2 py-1">Trades</th>
                          <th className="px-2 py-1">Win</th>
                          <th className="px-2 py-1">Net P&L</th>
                          <th className="px-2 py-1">Max DD</th>
                        </tr>
                      </thead>
                      <tbody>
                        {seriesStats.map((entry) => (
                          <tr key={entry.key} className="border-t border-slate-800/80">
                            <td className="px-2 py-1.5 font-medium text-slate-200">{entry.symbol}</td>
                            <td className="px-2 py-1.5">{entry.timeframe}</td>
                            <td className="px-2 py-1.5 tabular-nums">{entry.trades}</td>
                            <td className="px-2 py-1.5 tabular-nums">{formatPercent(entry.winRate)}</td>
                            <td className={`px-2 py-1.5 tabular-nums ${entry.netPnl >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                              {formatNumber(entry.netPnl)}
                            </td>
                            <td className="px-2 py-1.5 tabular-nums">{formatNumber(entry.maxDrawdown)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
              <div className="mt-4">
                <DecisionTrace ledgerEvents={decisionEvents} />
              </div>
              <details className="mt-4 rounded border border-slate-800 bg-slate-900/50 p-3 text-xs text-slate-300">
                <summary className="cursor-pointer text-slate-400">Raw snapshot payload</summary>
                <pre className="mt-3 max-h-64 overflow-auto">{JSON.stringify(snapshot, null, 2)}</pre>
              </details>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
