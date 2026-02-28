import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
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

export function BotLensLiveModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotLensLiveModal'), [])
  const [snapshot, setSnapshot] = useState(null)
  const [streamState, setStreamState] = useState('idle')
  const [statusMessage, setStatusMessage] = useState('')
  const [error, setError] = useState(null)
  const [cursor, setCursor] = useState({ runId: null, seq: 0 })
  const [staleMode, setStaleMode] = useState(false)
  const socketRef = useRef(null)
  const cursorRef = useRef({ runId: null, seq: 0 })
  const streamStateRef = useRef('idle')
  const syncInFlightRef = useRef(false)
  const syncTokenRef = useRef(0)
  const mountedRef = useRef(false)

  useEffect(() => {
    cursorRef.current = cursor
  }, [cursor])

  useEffect(() => {
    streamStateRef.current = streamState
  }, [streamState])

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
            setStaleMode(true)
            setStreamState('resyncing')
            setStatusMessage(`Run changed (${prev.runId} -> ${next.runId}). Auto-attaching to latest run...`)
            syncTokenRef.current += 1
            closeSocket()
            return
          }

          setSnapshot(next.snapshot)
          setCursor({ runId: next.runId, seq: next.seq })
          setStaleMode(false)
          setStreamState('open')
          setStatusMessage(next.critical ? `Live update (${next.eventType})` : 'Live update')
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
          setSnapshot(snap)
          setCursor({ runId, seq })
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

  if (!open || !bot) return null

  const series = Array.isArray(snapshot?.series) ? snapshot.series : []
  const primary = series[0] || {}
  const candles = Array.isArray(primary?.candles) ? primary.candles : []
  const overlays = Array.isArray(primary?.overlays) ? primary.overlays : []
  const trades = Array.isArray(snapshot?.trades) ? snapshot.trades : []
  const runtime = snapshot?.runtime || bot?.runtime || {}
  const stats = runtime?.stats || {}
  const status = runtime?.status || bot?.status || 'idle'

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
              bot_id={bot.id} · run={cursor.runId || '—'} · seq={cursor.seq || 0} · stream={streamState}
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
                  <p className="text-[10px] uppercase text-slate-500">Candles</p>
                  <p className="text-sm font-semibold text-slate-200">{candles.length}</p>
                </div>
                <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
                  <p className="text-[10px] uppercase text-slate-500">Overlays / Trades</p>
                  <p className="text-sm font-semibold text-slate-200">{overlays.length} / {trades.length}</p>
                </div>
              </div>
              <BotLensChart
                chartId={`botlens-live-${bot.id}`}
                candles={candles}
                trades={trades}
                overlays={overlays}
                mode={bot.mode}
                playbackSpeed={Number(bot.playback_speed || 0)}
                timeframe={primary?.timeframe || null}
              />
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
