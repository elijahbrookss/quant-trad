import { useEffect, useMemo, useRef, useState } from 'react'
import { X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { createLogger } from '../../utils/logger.js'
import { openWebSocket } from '../../adapters/realtime.adapter.js'

export function BotLensLiveModal({ bot, open, onClose }) {
  const logger = useMemo(() => createLogger('BotLensLiveModal'), [])
  const [snapshot, setSnapshot] = useState(null)
  const [status, setStatus] = useState('connecting')
  const [error, setError] = useState(null)
  const socketRef = useRef(null)

  useEffect(() => {
    if (!open || !bot?.id) return undefined

    const path = `/api/bots/ws/${encodeURIComponent(bot.id)}`
    const socket = openWebSocket(path)
    if (!socket) {
      setStatus('error')
      setError('Live BotLens stream failed to initialize.')
      logger.warn('botlens_ws_init_failed', { bot_id: bot.id, path })
      return undefined
    }
    socketRef.current = socket
    setStatus('connecting')
    setError(null)

    socket.onopen = () => {
      setStatus('open')
      logger.info('botlens_ws_connected', { bot_id: bot.id, path })
    }

    socket.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data)
        const nextSnapshot = payload?.snapshot || null
        if (nextSnapshot) {
          setSnapshot(nextSnapshot)
        }
      } catch (err) {
        logger.warn('botlens_ws_parse_failed', { bot_id: bot.id }, err)
      }
    }

    socket.onerror = (err) => {
      setStatus('error')
      setError('Live BotLens stream failed.')
      logger.warn('botlens_ws_error', { bot_id: bot.id }, err)
    }

    socket.onclose = () => {
      setStatus('closed')
      logger.info('botlens_ws_closed', { bot_id: bot.id })
    }

    return () => {
      try {
        socket.close()
      } catch {
        // no-op
      }
      socketRef.current = null
    }
  }, [bot?.id, logger, open])

  if (!open || !bot) return null

  const series = Array.isArray(snapshot?.series) ? snapshot.series : []
  const primary = series[0] || {}
  const candles = Array.isArray(primary?.candles) ? primary.candles : []
  const overlays = Array.isArray(primary?.overlays) ? primary.overlays : []
  const trades = Array.isArray(snapshot?.trades) ? snapshot.trades : []

  return (
    <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/70 p-4" onClick={onClose}>
      <div
        className="h-[86vh] w-full max-w-6xl overflow-hidden rounded-2xl border border-slate-800 bg-slate-950 shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <div>
            <p className="text-sm font-semibold text-slate-100">BotLens Live</p>
            <p className="text-xs text-slate-500">bot_id={bot.id} · stream={status}</p>
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
          {error ? <div className="mb-3 rounded border border-rose-800/60 bg-rose-950/30 px-3 py-2 text-sm text-rose-200">{error}</div> : null}
          {!snapshot ? (
            <div className="rounded border border-slate-800 bg-slate-900/40 px-4 py-8 text-sm text-slate-400">
              Waiting for live telemetry from container runtime…
            </div>
          ) : (
            <>
              <BotLensChart
                chartId={`botlens-live-${bot.id}`}
                candles={candles}
                trades={trades}
                overlays={overlays}
                mode={bot.mode}
                playbackSpeed={Number(bot.playback_speed || 0)}
                timeframe={primary?.timeframe || null}
              />
              <pre className="mt-4 max-h-64 overflow-auto rounded border border-slate-800 bg-slate-900/50 p-3 text-xs text-slate-300">
                {JSON.stringify(snapshot, null, 2)}
              </pre>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
