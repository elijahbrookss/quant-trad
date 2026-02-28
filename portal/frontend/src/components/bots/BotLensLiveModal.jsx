import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { OverlayToggleBar } from './OverlayToggleBar.jsx'
import DecisionTrace from './DecisionTrace/index.jsx'
import { useOverlayControls } from './hooks/useOverlayControls.js'
import { createLogger } from '../../utils/logger.js'
import { fetchBotLensBootstrap, openBotLensStream } from '../../adapters/bot.adapter.js'

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
  const payload = message?.payload && typeof message.payload === 'object' ? message.payload : {}
  const snapshot = payload?.snapshot && typeof payload.snapshot === 'object' ? payload.snapshot : null
  return {
    runId: message?.run_id ? String(message.run_id) : null,
    seq: Number(message?.seq || 0),
    eventType: String(message?.event_type || 'state_delta'),
    critical: Boolean(message?.critical),
    snapshot,
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

const TARGET_RENDER_LAG_MS = readPositiveNumber('VITE_BOTLENS_TARGET_RENDER_LAG_MS', 420)
const CATCHUP_RENDER_LAG_MS = readPositiveNumber('VITE_BOTLENS_CATCHUP_RENDER_LAG_MS', 1200)
const CATCHUP_SEQ_BEHIND = readPositiveInt('VITE_BOTLENS_CATCHUP_SEQ_BEHIND', 6)
const CATCHUP_QUEUE_DEPTH = readPositiveInt('VITE_BOTLENS_CATCHUP_QUEUE_DEPTH', 8)
const NORMAL_APPLY_INTERVAL_MS = readPositiveNumber('VITE_BOTLENS_NORMAL_APPLY_INTERVAL_MS', 100)
const CATCHUP_APPLY_INTERVAL_MS = readPositiveNumber('VITE_BOTLENS_CATCHUP_APPLY_INTERVAL_MS', 12)
const MAX_CATCHUP_BATCH = readPositiveInt('VITE_BOTLENS_MAX_CATCHUP_BATCH', 2)
const METRICS_PUBLISH_MS = readPositiveNumber('VITE_BOTLENS_METRICS_PUBLISH_MS', 120)

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
  if (primarySeriesKey(baseSnapshot) !== primarySeriesKey(targetSnapshot)) {
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

export function BotLensLiveModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotLensLiveModal'), [])
  const [snapshot, setSnapshot] = useState(null)
  const [streamState, setStreamState] = useState('idle')
  const [statusMessage, setStatusMessage] = useState('')
  const [error, setError] = useState(null)
  const [cursor, setCursor] = useState({ runId: null, seq: 0 })
  const [renderCursor, setRenderCursor] = useState({ runId: null, seq: 0 })
  const [staleMode, setStaleMode] = useState(false)
  const [overlayPanelCollapsed, setOverlayPanelCollapsed] = useState(false)
  const [renderMetrics, setRenderMetrics] = useState({
    mode: 'smooth',
    queueDepth: 0,
    seqBehind: 0,
    lagMs: 0,
    appliedRate: 0,
  })
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
    if (!open) return undefined
    let cancelled = false

    const publishMetrics = (ts, mode) => {
      const queueDepth = pendingFramesRef.current.length
      const canonical = cursorRef.current
      const rendered = renderCursorRef.current
      const sameRun = canonical?.runId && rendered?.runId && canonical.runId === rendered.runId
      const seqBehind = sameRun ? Math.max(0, Number(canonical.seq || 0) - Number(rendered.seq || 0)) : queueDepth
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
        lagMs,
        appliedRate,
      })
    }

    const step = (ts) => {
      if (cancelled) return
      const queue = pendingFramesRef.current
      const canonical = cursorRef.current
      const rendered = renderCursorRef.current
      const sameRun = canonical?.runId && rendered?.runId && canonical.runId === rendered.runId
      const seqBehind = sameRun ? Math.max(0, Number(canonical.seq || 0) - Number(rendered.seq || 0)) : queue.length
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
    }
  }, [open])

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
      const socket = openBotLensStream(botId, { runId, sinceSeq: seq })
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
          if (message?.type !== 'bot_runtime_event') return
          const next = normalizeEventPayload(message)
          if (!next.runId || next.seq <= 0 || !next.snapshot) return

          const prev = cursorRef.current
          if (prev.runId && next.runId === prev.runId) {
            if (next.seq <= prev.seq) return
            if (next.seq !== prev.seq + 1) {
              pendingFramesRef.current = []
              renderedSnapshotRef.current = null
              setStaleMode(true)
              setStreamState('resyncing')
              setStatusMessage(
                `Gap detected (expected ${prev.seq + 1}, got ${next.seq}). Keeping stale view and resyncing...`,
              )
              syncTokenRef.current += 1
              closeSocket()
              return
            }
          } else if (prev.runId && next.runId !== prev.runId) {
            pendingFramesRef.current = []
            renderedSnapshotRef.current = null
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
          const frames = buildSnapshotFrames({
            baseSnapshot,
            targetSnapshot: next.snapshot,
            envelope: {
              runId: next.runId,
              seq: next.seq,
              critical: next.critical,
              eventType: next.eventType,
              receivedAt,
            },
          })
          pendingFramesRef.current.push(...frames)
          cursorRef.current = { runId: next.runId, seq: next.seq }
          setCursor({ runId: next.runId, seq: next.seq })
          setStreamState('open')
          setStaleMode(false)
          if (next.critical) {
            setStatusMessage(`Live update (${next.eventType})`)
          }
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
          const boot = await fetchBotLensBootstrap(bot.id)
          const runId = boot?.run_id ? String(boot.run_id) : null
          const seq = Number(boot?.seq || 0)
          const snap = boot?.snapshot && typeof boot.snapshot === 'object' ? boot.snapshot : null
          if (!runId || seq <= 0 || !snap) {
            setStreamState('waiting')
            setStatusMessage('Runtime has not emitted snapshot events yet. Retrying...')
            await delay(1200)
            continue
          }
          pendingFramesRef.current = []
          lastApplyAtRef.current = nowMs()
          setSnapshot(snap)
          renderedSnapshotRef.current = snap
          cursorRef.current = { runId, seq }
          renderCursorRef.current = { runId, seq }
          setCursor({ runId, seq })
          setRenderCursor({ runId, seq })
          setRenderMetrics({
            mode: 'smooth',
            queueDepth: 0,
            seqBehind: 0,
            lagMs: 0,
            appliedRate: 0,
          })
          setError(null)
          setStaleMode(false)
          setStatusMessage('Snapshot loaded. Connecting live stream...')
          connectSocket({ botId: bot.id, runId, seq, token })
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

  const series = useMemo(() => (Array.isArray(snapshot?.series) ? snapshot.series : []), [snapshot])
  const primary = useMemo(() => series[0] || {}, [series])
  const candles = useMemo(() => (Array.isArray(primary?.candles) ? primary.candles : []), [primary])
  const overlays = useMemo(() => (Array.isArray(primary?.overlays) ? primary.overlays : []), [primary])
  const trades = useMemo(() => (Array.isArray(snapshot?.trades) ? snapshot.trades : []), [snapshot])
  const logs = useMemo(() => (Array.isArray(snapshot?.logs) ? snapshot.logs : []), [snapshot])
  const decisionEvents = useMemo(
    () => (Array.isArray(snapshot?.decisions) ? snapshot.decisions : []),
    [snapshot],
  )
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

  if (!open || !bot) return null

  return (
    <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/70 p-4" onClick={onClose}>
      <div
        className="h-[86vh] w-full max-w-6xl overflow-hidden rounded-2xl border border-slate-800 bg-slate-950 shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <div>
            <p className="text-sm font-semibold text-slate-100">BotLens Live</p>
            <p className="text-xs text-slate-500">
              bot_id={bot.id} · run={renderCursor.runId || cursor.runId || '—'} · seq(render/canon)={renderCursor.seq || 0}/{cursor.seq || 0} · stream={streamState}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-700 text-slate-400 hover:text-slate-200"
            aria-label="Close"
          >
            <X className="size-4" />
          </button>
        </div>

        <div className="h-[calc(86vh-58px)] overflow-auto p-4">
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
                    {renderMetrics.mode} · {renderMetrics.seqBehind} behind · {formatNumber(renderMetrics.appliedRate, 1)}/s
                  </p>
                </div>
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
                trades={trades}
                overlays={visibleOverlays}
                mode={bot.mode}
                playbackSpeed={Number(bot.playback_speed || 0)}
                timeframe={primary?.timeframe || null}
                overlayVisibility={visibility}
              />
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
