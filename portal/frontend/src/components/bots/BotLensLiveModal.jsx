import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'
import { LocateFixed, Maximize2, Minimize2, X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { OverlayToggleBar } from './OverlayToggleBar.jsx'
import { ActiveTradeChip } from './ActiveTradeChip.jsx'
import DecisionTrace from './DecisionTrace/index.jsx'
import { useOverlayControls } from './hooks/useOverlayControls.js'
import { createLogger } from '../../utils/logger.js'
import {
  fetchBotRuns,
  fetchBotLensSeriesCatalog,
  fetchBotLensSeriesHistory,
  fetchBotLensSeriesWindow,
  fetchBotRunLedgerEvents,
  openBotLensSeriesLiveStream,
} from '../../adapters/bot.adapter.js'
import { describeBotLifecycle, getBotRunId, getBotStatus, normalizeBotStatus } from './botStatusModel.js'
import { BOTLENS_PHASES, botlensReducer, initialBotLensState } from './botlensStateMachine.js'
import {
  applyHistoryPage,
  applyLiveTail,
  assessLiveContinuity,
  buildProjectionFromWindow,
  canonicalSeriesKey,
  findProjectionSeries,
  normalizeProjection,
  normalizeSeriesKey,
} from './botlensProjection.js'

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
    seriesKey: normalizeSeriesKey(message?.series_key || ''),
    seq: Number(message?.seq || 0),
    messageType: String(message?.message_type || ''),
    payload: message?.payload && typeof message.payload === 'object' ? message.payload : {},
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

function selectedProjectionSeries(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return null
  const selected = findProjectionSeries(snapshot, snapshot?.series_key || '')
  if (selected) return selected
  const series = Array.isArray(snapshot.series) ? snapshot.series : null
  if (!series || !series.length) return null
  const entry = series[0]
  return entry && typeof entry === 'object' ? entry : null
}

function selectedProjectionSeriesKey(snapshot) {
  const explicit = normalizeSeriesKey(snapshot?.series_key || '')
  if (explicit) return explicit
  const primary = selectedProjectionSeries(snapshot)
  if (!primary) return null
  return canonicalSeriesKey(primary.symbol, primary.timeframe)
}

function selectedProjectionCandles(snapshot) {
  const primary = selectedProjectionSeries(snapshot)
  const candles = primary?.candles
  return Array.isArray(candles) ? candles : null
}

function selectedProjectionCandleCount(snapshot) {
  const candles = selectedProjectionCandles(snapshot)
  return Array.isArray(candles) ? candles.length : 0
}

function candleLag(renderedSnapshot, canonicalSnapshot) {
  if (!renderedSnapshot || !canonicalSnapshot) return 0
  if (selectedProjectionSeriesKey(renderedSnapshot) !== selectedProjectionSeriesKey(canonicalSnapshot)) return 0
  const renderedCount = selectedProjectionCandleCount(renderedSnapshot)
  const canonicalCount = selectedProjectionCandleCount(canonicalSnapshot)
  if (!Number.isFinite(renderedCount) || !Number.isFinite(canonicalCount)) return 0
  return Math.max(0, canonicalCount - renderedCount)
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

function seriesOptionFromKey(key) {
  const canonical = normalizeSeriesKey(key)
  const [symbol, timeframe] = canonical.split('|')
  return {
    key: canonical,
    symbol: String(symbol || '—'),
    timeframe: String(timeframe || '—'),
  }
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

function shortRunId(runId) {
  const value = String(runId || '').trim()
  if (!value) return '—'
  return value.length <= 12 ? value : `${value.slice(0, 8)}…${value.slice(-4)}`
}

function formatRunMoment(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return String(value)
  }
}

function describeRunRow(run) {
  if (!run || typeof run !== 'object') return 'No run selected'
  const status = normalizeBotStatus(run.runtime_status || run.status || 'idle')
  const started = run.started_at || run.created_at
  const ended = run.ended_at
  if (run.is_active) {
    return `Active ${status} run · started ${formatRunMoment(started)}`
  }
  if (ended) {
    return `${status} · ended ${formatRunMoment(ended)}`
  }
  return `${status} · started ${formatRunMoment(started)}`
}

function buildLensStages({ botLifecycle, selectedRun, snapshotReady, streamState, liveTarget }) {
  const hasRun = Boolean(selectedRun?.run_id)
  const hasSnapshot = Boolean(snapshotReady)
  const liveConnected = liveTarget ? streamState === 'open' : hasSnapshot
  return [
    {
      key: 'run',
      label: liveTarget ? 'Run attached' : 'Run selected',
      status: hasRun ? 'done' : 'current',
      detail: hasRun ? shortRunId(selectedRun?.run_id) : 'Waiting for run_id',
    },
    {
      key: 'runtime',
      label: liveTarget ? 'Runtime' : 'Archive',
      status: !hasRun
        ? 'upcoming'
        : liveTarget
          ? botLifecycle.live || ['awaiting_snapshot', 'booting_runtime', 'starting_container', 'degraded'].includes(botLifecycle.phase)
            ? 'current'
            : 'done'
          : 'done',
      detail: liveTarget ? botLifecycle.label : 'Reading BotLens runtime records',
    },
    {
      key: 'snapshot',
      label: 'Baseline',
      status: hasSnapshot ? 'done' : hasRun ? 'current' : 'upcoming',
      detail: hasSnapshot ? 'Canonical baseline established' : liveTarget ? 'Waiting for first BotLens frame' : 'Loading historical baseline',
    },
    {
      key: 'bridge',
      label: liveTarget ? 'Live bridge' : 'History mode',
      status: liveConnected ? 'done' : hasSnapshot ? 'current' : 'upcoming',
      detail: liveTarget ? (streamState === 'open' ? 'Streaming deltas into chart' : 'Connecting websocket') : 'Viewing archived state only',
    },
  ]
}

export function BotLensLiveModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotLensLiveModal'), [])
  const [snapshot, setSnapshot] = useState(null)
  const [streamState, setStreamState] = useState('idle')
  const [statusMessage, setStatusMessage] = useState('')
  const [error, setError] = useState(null)
  const [runCatalog, setRunCatalog] = useState([])
  const [runCatalogState, setRunCatalogState] = useState('idle')
  const [selectedRunId, setSelectedRunId] = useState(null)
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
  const [availableSeriesKeys, setAvailableSeriesKeys] = useState([])
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
  const botLifecycle = useMemo(() => describeBotLifecycle(bot), [bot])
  const activeBotRunId = getBotRunId(bot)
  const botStatus = getBotStatus(bot)
  const selectedRun = useMemo(
    () => runCatalog.find((entry) => String(entry?.run_id || '') === String(selectedRunId || '')) || null,
    [runCatalog, selectedRunId],
  )
  const selectedRunStatus = normalizeBotStatus(selectedRun?.runtime_status || selectedRun?.status || botStatus)
  const selectedRunIsTerminal = ['completed', 'stopped', 'failed', 'crashed', 'error'].includes(selectedRunStatus)
  const selectedRunIsLiveTarget = Boolean(selectedRunId) && (
    (selectedRun?.is_active ?? false) || (activeBotRunId && selectedRunId === activeBotRunId)
  ) && !selectedRunIsTerminal
  const showStaleStatus = staleMode && selectedRunIsLiveTarget
  const lifecycleStages = useMemo(
    () => buildLensStages({
      botLifecycle,
      selectedRun,
      snapshotReady: Boolean(snapshot),
      streamState,
      liveTarget: selectedRunIsLiveTarget,
    }),
    [botLifecycle, selectedRun, snapshot, streamState, selectedRunIsLiveTarget],
  )

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
    if (!open) return
    pendingFramesRef.current = []
    renderedSnapshotRef.current = null
    canonicalFrameRef.current = { runId: null, seq: 0, snapshot: null }
    setAvailableSeriesKeys([])
    setSnapshot(null)
    setError(null)
  }, [open, selectedRunId])

  useEffect(() => {
    if (!open || !bot?.id) {
      setRunCatalog([])
      setRunCatalogState('idle')
      setSelectedRunId(null)
      return undefined
    }

    let cancelled = false
    const loadRuns = async () => {
      try {
        setRunCatalogState((current) => (current === 'open' ? current : 'loading'))
        const payload = await fetchBotRuns(bot.id, { limit: 30 })
        if (cancelled) return
        const runs = Array.isArray(payload?.runs) ? payload.runs : []
        setRunCatalog(runs)
        setRunCatalogState('open')
        setSelectedRunId((current) => {
          if (current && runs.some((entry) => String(entry?.run_id || '') === current)) return current
          const active = runs.find((entry) => entry?.is_active)
          return String(active?.run_id || runs[0]?.run_id || activeBotRunId || '') || null
        })
      } catch (err) {
        if (cancelled) return
        setRunCatalogState('error')
        logger.warn('botlens_run_catalog_failed', { bot_id: bot.id }, err)
      }
    }

    loadRuns()
    const pollId = window.setInterval(loadRuns, 4000)
    return () => {
      cancelled = true
      window.clearInterval(pollId)
    }
  }, [activeBotRunId, bot?.id, logger, open])


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
      const socket = openBotLensSeriesLiveStream(
        runId,
        activeSeriesKeyRef.current || selectedProjectionSeriesKey(renderedSnapshotRef.current) || 'UNKNOWN|1m',
        { afterSeq: seq },
      )
      if (!socket) {
        setStreamState('stale')
        setStaleMode(true)
        setStatusMessage('WebSocket unavailable. Retrying bootstrap...')
        dispatchLens({ type: 'STREAM_STALE' })
        return
      }
      socketRef.current = socket
      setStreamState('connecting')
      dispatchLens({ type: 'LIVE_CONNECTING' })

      socket.onopen = () => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        setStreamState('open')
        setStatusMessage('Live stream connected.')
        dispatchLens({ type: 'LIVE_CONNECTED' })
        logger.info('botlens_ws_open', { bot_id: botId, run_id: runId, seq })
      }

      socket.onmessage = (event) => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        try {
          const message = JSON.parse(event.data)
          const next = normalizeEventPayload(message)
          if (!next || !next.runId || next.seq <= 0) return

          const baseProjection = (
            pendingFramesRef.current.length > 0
              ? pendingFramesRef.current[pendingFramesRef.current.length - 1]?.snapshot
              : renderedSnapshotRef.current
          ) || canonicalFrameRef.current?.snapshot
          const continuity = assessLiveContinuity({
            projection: baseProjection,
            message: next,
            seriesKey: activeSeriesKeyRef.current,
            maxAllowedGap: 1,
          })
          if (continuity.action === 'ignore') return
          if (continuity.action === 'resync') {
            pendingFramesRef.current = []
            renderedSnapshotRef.current = null
            canonicalFrameRef.current = { runId: null, seq: 0, snapshot: null }
            setStaleMode(true)
            setStreamState('resyncing')
            setStatusMessage(`BotLens continuity lost (${continuity.reason}). Re-syncing canonical baseline...`)
            dispatchLens({ type: 'SEQ_GAP' })
            logger.warn('botlens_stream_resync_required', {
              bot_id: botId,
              run_id: next.runId,
              seq: next.seq,
              reason: continuity.reason,
            })
            syncTokenRef.current += 1
            closeSocket()
            return
          }

          const receivedAt = nowMs()
          const queue = pendingFramesRef.current
          const baseSnapshot = queue.length > 0
            ? queue[queue.length - 1]?.snapshot || null
            : renderedSnapshotRef.current
          const base = normalizeProjection(baseSnapshot, {
            runId: next.runId,
            seq: Number((baseSnapshot && baseSnapshot.seq) || 0),
            seriesKey: activeSeriesKeyRef.current,
          })
          const mergedSnapshot = applyLiveTail({
            projection: base,
            message: next,
            seriesKey: activeSeriesKeyRef.current,
          })
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
          if (next.messageType === 'status') {
            setStatusMessage(`Runtime status update received: ${String(next.payload?.status || 'running')}`)
          } else if (streamStateRef.current !== 'open') {
            setStatusMessage('Live stream connected. Rendering incoming updates.')
          }
          dispatchLens({ type: 'LIVE_CONNECTED' })
        } catch (err) {
          logger.warn('botlens_ws_parse_failed', { bot_id: botId }, err)
        }
      }

      socket.onerror = (err) => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        setStaleMode(true)
        setStreamState('stale')
        setStatusMessage('WebSocket error. Keeping stale snapshot and retrying bootstrap...')
        dispatchLens({ type: 'STREAM_STALE' })
        logger.warn('botlens_ws_error', { bot_id: botId, run_id: runId }, err)
      }

      socket.onclose = () => {
        if (syncTokenRef.current !== token || !mountedRef.current) return
        setStaleMode(true)
        setStreamState('stale')
        setStatusMessage('WebSocket closed. Keeping stale snapshot and retrying bootstrap...')
        dispatchLens({ type: 'STREAM_STALE' })
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
      dispatchLens({ type: 'BOOTSTRAP_START' })
      if (reason) {
        setStaleMode(true)
        setStatusMessage(reason)
      } else {
        setStreamState('bootstrapping')
        if (!selectedRunIsLiveTarget) setStaleMode(false)
        setStatusMessage(selectedRunIsLiveTarget ? 'Loading BotLens baseline…' : 'Loading historical BotLens baseline…')
      }
      while (!cancelled && mountedRef.current && token === syncTokenRef.current) {
        try {
          const runId = selectedRunId ? String(selectedRunId) : null
          if (!runId) {
            dispatchLens({ type: 'WAITING_FOR_RUN' })
            setStreamState('waiting')
            setAvailableSeriesKeys([])
            setStatusMessage(selectedRunIsLiveTarget ? botLifecycle.detail : 'Select a run to open BotLens.')
            syncInFlightRef.current = false
            return
          }
          pendingFramesRef.current = []
          lastApplyAtRef.current = nowMs()
          const catalog = await fetchBotLensSeriesCatalog(runId)
          const availableSeries = (Array.isArray(catalog?.series) ? catalog.series : []).map((value) => normalizeSeriesKey(value)).filter(Boolean)
          setAvailableSeriesKeys(availableSeries)
          const retainedSeriesKey = activeSeriesKeyRef.current
          const bootSeriesKey = retainedSeriesKey && availableSeries.includes(retainedSeriesKey) ? retainedSeriesKey : availableSeries[0] || null
          if (!bootSeriesKey) {
            dispatchLens({ type: 'WAITING_FOR_SNAPSHOT' })
            setStreamState(selectedRunIsLiveTarget ? 'waiting' : 'historical')
            if (!selectedRunIsLiveTarget) setStaleMode(false)
            setAvailableSeriesKeys([])
            activeSeriesKeyRef.current = null
            setStatusMessage(
              selectedRunIsLiveTarget
                ? 'Run is active, but BotLens has not published the first series catalog yet. Retrying…'
                : 'No BotLens series were found for this run.',
            )
            if (selectedRunIsLiveTarget) {
              await delay(1200)
              continue
            }
            syncInFlightRef.current = false
            return
          }
          activeSeriesKeyRef.current = bootSeriesKey
          const window = await fetchBotLensSeriesWindow(runId, bootSeriesKey, { to: 'now', limit: 320 })
          const startSeq = Number(window?.seq || 0)
          const seeded = buildProjectionFromWindow({
            runId,
            seq: startSeq,
            seriesKey: bootSeriesKey,
            window: window?.window,
          })
          setSnapshot(seeded)
          renderedSnapshotRef.current = seeded
          canonicalFrameRef.current = { runId, seq: startSeq, snapshot: seeded }
          dispatchLens({
            type: 'BOOTSTRAP_SUCCESS',
            runId,
            seriesKey: bootSeriesKey,
            seq: startSeq,
            live: selectedRunIsLiveTarget,
          })
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
          if (selectedRunIsLiveTarget) {
            setStatusMessage('Baseline loaded. Connecting live stream…')
            connectSocket({ botId: bot.id, runId, seq: startSeq, token })
          } else {
            setStreamState('historical')
            setStatusMessage('Historical run loaded from BotLens runtime records. Live stream is disabled for archived runs.')
          }
          syncInFlightRef.current = false
          return
        } catch (err) {
          logger.warn('botlens_bootstrap_failed', { bot_id: bot.id, run_id: selectedRunId || null }, err)
          dispatchLens({ type: 'STREAM_STALE' })
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
      if (!selectedRunId) return
      if (selectedRunIsLiveTarget && streamStateRef.current === 'open') return
      if (!selectedRunIsLiveTarget && renderedSnapshotRef.current) return
      bootstrapAndConnect('Re-syncing BotLens baseline…')
    }, 2500)

    return () => {
      cancelled = true
      mountedRef.current = false
      clearInterval(monitor)
      syncInFlightRef.current = false
      closeSocket()
    }
  }, [
    bot?.id,
    botLifecycle.detail,
    closeSocket,
    connectSocket,
    logger,
    open,
    selectedRunId,
    selectedRunIsLiveTarget,
    selectedRunStatus,
  ])

  const activeRunId = selectedRunId || cursor.runId || renderCursor.runId || null

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
    () => {
      const sourceKeys = availableSeriesKeys.length
        ? availableSeriesKeys
        : series.map((entry) => canonicalSeriesKey(entry?.symbol, entry?.timeframe)).filter(Boolean)
      return sourceKeys.map((key) => seriesOptionFromKey(key))
    },
    [availableSeriesKeys, series],
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
    return findProjectionSeries(snapshot, selectedSeriesKey) || series[0]
  }, [selectedSeriesKey, series, snapshot])
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
  const status = runtime?.status || botStatus || 'idle'
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
    const nextSeriesKey = normalizeSeriesKey(selectedSeriesKey || '')
    if (!nextSeriesKey) return
    if (activeSeriesKeyRef.current === nextSeriesKey) return
    activeSeriesKeyRef.current = nextSeriesKey
    if (cursorRef.current?.runId) {
      pendingFramesRef.current = []
      renderedSnapshotRef.current = null
      canonicalFrameRef.current = { runId: null, seq: 0, snapshot: null }
      setSnapshot(null)
      syncTokenRef.current += 1
      closeSocket()
      dispatchLens({ type: 'BOOTSTRAP_START' })
      setStreamState('resyncing')
      setStatusMessage(`Switching series to ${nextSeriesKey}...`)
    }
  }, [closeSocket, selectedSeriesKey])


  const loadOlderHistory = useCallback(async () => {
    const runId = cursorRef.current?.runId
    const sk = normalizeSeriesKey(selectedSeriesKey || activeSeriesKeyRef.current || '')
    if (!runId || !sk) return
    const oldest = candles.length ? candles[0] : null
    const beforeTs = oldest?.time ? new Date(Number(oldest.time) * 1000).toISOString() : undefined
    try {
      dispatchLens({ type: 'HISTORY_PAGE_START' })
      const page = await fetchBotLensSeriesHistory(runId, sk, { beforeTs, limit: 240 })
      const pageCandles = Array.isArray(page?.history?.candles) ? page.history.candles : []
      if (!pageCandles.length) return
      dispatchLens({ type: 'HISTORY_PAGE_SUCCESS', candles: pageCandles })
      setSnapshot((current) => {
        const nextProjection = applyHistoryPage({
          projection: current,
          seriesKey: sk,
          candles: pageCandles,
        })
        renderedSnapshotRef.current = nextProjection
        if (canonicalFrameRef.current?.snapshot) {
          canonicalFrameRef.current = {
            ...canonicalFrameRef.current,
            snapshot: applyHistoryPage({
              projection: canonicalFrameRef.current.snapshot,
              seriesKey: sk,
              candles: pageCandles,
            }),
          }
        }
        pendingFramesRef.current = pendingFramesRef.current.map((frame) => ({
          ...frame,
          snapshot: applyHistoryPage({
            projection: frame.snapshot,
            seriesKey: sk,
            candles: pageCandles,
          }),
        }))
        return nextProjection
      })
    } catch (err) {
      logger.warn('botlens_history_page_failed', { run_id: runId, series_key: sk }, err)
    }
  }, [candles, logger, selectedSeriesKey])

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
              bot_id={bot.id} · run={shortRunId(selectedRunId || renderCursor.runId || cursor.runId || '—')} · seq(render/canon)={renderCursor.seq || 0}/{cursor.seq || 0} · stream={streamState}
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
          <div className="mb-4 overflow-hidden rounded-3xl border border-slate-800 bg-[radial-gradient(circle_at_top_left,rgba(14,165,233,0.16),transparent_45%),radial-gradient(circle_at_bottom_right,rgba(16,185,129,0.12),transparent_42%),rgba(2,6,23,0.84)]">
            <div className="border-b border-slate-800/80 px-4 py-3">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-slate-400">Lifecycle</p>
                  <p className="mt-1 text-lg font-semibold text-slate-100">{botLifecycle.label}</p>
                  <p className="mt-1 text-sm text-slate-300">{statusMessage || botLifecycle.detail}</p>
                </div>
                <div className="flex min-w-[18rem] flex-col gap-1">
                  <label className="text-[10px] font-semibold uppercase tracking-[0.24em] text-slate-500">Run</label>
                  <select
                    value={selectedRunId || ''}
                    onChange={(event) => setSelectedRunId(event.target.value || null)}
                    className="rounded-xl border border-slate-700 bg-slate-950/80 px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-sky-500/60"
                  >
                    <option value="" disabled>Select a run</option>
                    {runCatalog.map((run) => (
                      <option key={run.run_id} value={run.run_id}>
                        {`${run.is_active ? 'LIVE' : 'RUN '} · ${shortRunId(run.run_id)} · ${normalizeBotStatus(run.runtime_status || run.status || 'idle')}`}
                      </option>
                    ))}
                  </select>
                  <p className="text-xs text-slate-400">{describeRunRow(selectedRun)}</p>
                </div>
              </div>
            </div>
            <div className="grid gap-2 px-4 py-4 md:grid-cols-4">
              {lifecycleStages.map((stage, index) => {
                const stageClass =
                  stage.status === 'done'
                    ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-100'
                    : stage.status === 'current'
                      ? 'border-sky-500/40 bg-sky-500/10 text-sky-100'
                      : 'border-slate-800 bg-slate-950/40 text-slate-500'
                return (
                  <div key={stage.key} className={`rounded-2xl border p-3 ${stageClass}`}>
                    <div className="flex items-center gap-2">
                      <div className={`relative flex h-6 w-6 items-center justify-center rounded-full border text-[11px] font-semibold ${
                        stage.status === 'done'
                          ? 'border-emerald-400/70 bg-emerald-400/15 text-emerald-200'
                          : stage.status === 'current'
                            ? 'border-sky-400/70 bg-sky-400/15 text-sky-200'
                            : 'border-slate-700 bg-slate-900/80 text-slate-500'
                      }`}>
                        {stage.status === 'current' ? <span className="absolute inset-0 rounded-full animate-pulse bg-sky-400/10" /> : null}
                        <span className="relative">{index + 1}</span>
                      </div>
                      <div>
                        <p className="text-[10px] font-semibold uppercase tracking-[0.24em]">{stage.label}</p>
                        <p className="mt-1 text-xs opacity-80">{stage.detail}</p>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
          {statusMessage ? (
            <div
              className={`mb-3 rounded border px-3 py-2 text-sm ${
                showStaleStatus
                  ? 'border-amber-700/70 bg-amber-950/30 text-amber-200'
                  : 'border-slate-800 bg-slate-900/40 text-slate-300'
              }`}
            >
              {showStaleStatus ? 'Read-only stale mode: ' : ''}
              {statusMessage}
            </div>
          ) : null}
          {runCatalogState === 'error' ? (
            <div className="mb-3 rounded border border-amber-700/60 bg-amber-950/30 px-3 py-2 text-sm text-amber-200">
              Run catalog unavailable right now. BotLens will keep the current selection until the backend responds again.
            </div>
          ) : null}
          {error ? <div className="mb-3 rounded border border-rose-800/60 bg-rose-950/30 px-3 py-2 text-sm text-rose-200">{error}</div> : null}
          {ledgerState?.error ? (
            <div className="mb-3 rounded border border-amber-700/60 bg-amber-950/30 px-3 py-2 text-sm text-amber-200">
              DB ledger unavailable: {ledgerState.error}
            </div>
          ) : null}
          {!snapshot ? (
            <div className="relative overflow-hidden rounded-3xl border border-slate-800 bg-slate-900/50 px-6 py-8">
              <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(59,130,246,0.12),transparent_40%),radial-gradient(circle_at_bottom_right,rgba(16,185,129,0.10),transparent_35%)]" />
              <div className="relative flex flex-col gap-5 md:flex-row md:items-center md:justify-between">
                <div className="flex items-center gap-4">
                  <div className="relative flex h-16 w-16 items-center justify-center rounded-2xl border border-sky-500/30 bg-sky-500/10">
                    <span className="absolute inset-2 rounded-xl border border-sky-400/20 animate-pulse" />
                    <span className="absolute h-3 w-3 rounded-full bg-sky-300 shadow-[0_0_24px_rgba(125,211,252,0.75)]" />
                    <span className="absolute h-7 w-7 rounded-full border border-sky-300/40 animate-ping" />
                  </div>
                  <div>
                    <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-sky-300/80">
                      {selectedRunIsLiveTarget ? 'Joining live runtime' : 'Loading archived run'}
                    </p>
                    <p className="mt-1 text-xl font-semibold text-slate-100">
                      {selectedRunIsLiveTarget ? botLifecycle.label : selectedRun ? 'Historical snapshot bootstrap' : 'Waiting for run selection'}
                    </p>
                    <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-300">
                      {statusMessage || botLifecycle.detail}
                    </p>
                  </div>
                </div>
                <div className="grid gap-2 text-xs text-slate-300 sm:grid-cols-2">
                  <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3">
                    <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Selected Run</p>
                    <p className="mt-1 font-semibold text-slate-100">{shortRunId(selectedRunId)}</p>
                  </div>
                  <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-3">
                    <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Lifecycle Reason</p>
                    <p className="mt-1 font-semibold text-slate-100">{botLifecycle.reason.replaceAll('_', ' ')}</p>
                  </div>
                </div>
              </div>
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
                <p className="text-[11px] uppercase tracking-[0.24em] text-slate-500">{String(lensState.phase || '').replaceAll('_', ' ')}</p>
                <button
                  type="button"
                  onClick={loadOlderHistory}
                  className="rounded border border-slate-700 px-2 py-1 text-xs text-slate-300 hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-50"
                  disabled={!selectedRunId}
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
