import { useEffect, useState, useCallback } from 'react'
import { X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { fetchBotPerformance } from '../../adapters/bot.adapter.js'
import LoadingOverlay from '../LoadingOverlay.jsx'

export function BotPerformanceModal({ bot, open, onClose }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [payload, setPayload] = useState(null)

  const loadPerformance = useCallback(async () => {
    if (!bot?.id) return
    setLoading(true)
    setError(null)
    try {
      const data = await fetchBotPerformance(bot.id)
      setPayload(data)
    } catch (err) {
      setError(err?.message || 'Unable to fetch performance')
    } finally {
      setLoading(false)
    }
  }, [bot?.id])

  useEffect(() => {
    if (open) {
      loadPerformance()
    }
  }, [open, loadPerformance])

  useEffect(() => {
    const handler = (event) => {
      if (event.key === 'Escape') {
        onClose?.()
      }
    }
    if (open) {
      window.addEventListener('keydown', handler)
    }
    return () => window.removeEventListener('keydown', handler)
  }, [open, onClose])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div className="relative flex h-full max-h-[90vh] w-full max-w-6xl flex-col gap-4 overflow-hidden rounded-3xl border border-white/10 bg-[#0e1016] p-6 shadow-2xl">
        <header className="flex items-center justify-between gap-4 border-b border-white/5 pb-4">
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Bot lens</p>
            <h3 className="text-2xl font-semibold text-white">{bot?.name}</h3>
            <p className="text-sm text-slate-400">Mode: {bot?.mode} • Timeframe: {bot?.timeframe}</p>
          </div>
          <button
            type="button"
            className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/10 bg-white/5 text-slate-200 hover:border-white/30 hover:text-white"
            onClick={onClose}
          >
            <X className="size-5" />
          </button>
        </header>

        <div className="flex flex-1 flex-col gap-6 overflow-auto">
          <div className="relative">
            {loading ? <LoadingOverlay label="Loading bot performance…" /> : null}
            {error ? (
              <div className="rounded-2xl border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-200">{error}</div>
            ) : null}
            {!error ? (
              <BotLensChart
                chartId={`bot-${bot?.id}`}
                candles={payload?.candles || []}
                trades={payload?.trades || []}
              />
            ) : null}
          </div>

          <div className="grid gap-4 rounded-3xl border border-white/5 bg-white/5 p-4 text-sm text-slate-200 sm:grid-cols-3">
            {Object.entries(payload?.stats || {}).map(([key, value]) => (
              <div key={key} className="rounded-2xl border border-white/10 bg-black/20 p-3">
                <p className="text-xs uppercase tracking-[0.35em] text-slate-400">{key.replace(/_/g, ' ')}</p>
                <p className="text-2xl font-semibold text-white">{value ?? '—'}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
