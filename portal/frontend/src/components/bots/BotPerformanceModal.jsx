import { useEffect, useState, useCallback, useMemo } from 'react'
import { X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { fetchBotPerformance } from '../../adapters/bot.adapter.js'
import LoadingOverlay from '../LoadingOverlay.jsx'

export function BotPerformanceModal({ bot, open, onClose }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [payload, setPayload] = useState(null)

  const strategies = payload?.meta?.strategies || []
  const botMeta = payload?.meta?.bot || {}

  const headerDetails = useMemo(() => {
    const parts = []
    const symbols = []
    for (const strategy of strategies) {
      for (const symbol of strategy?.symbols || []) {
        if (symbol && !symbols.includes(symbol)) symbols.push(symbol)
      }
    }
    if (symbols.length) {
      parts.push(`Symbols: ${symbols.join(', ')}`)
    }
    if (botMeta.datasource) {
      parts.push(`Datasource: ${botMeta.datasource}`)
    }
    if (botMeta.exchange) {
      parts.push(`Exchange: ${botMeta.exchange}`)
    }
    parts.push(`Mode: ${bot?.mode}`)
    parts.push(`Timeframe: ${bot?.timeframe}`)
    return parts.filter(Boolean).join(' • ')
  }, [strategies, botMeta.datasource, botMeta.exchange, bot?.mode, bot?.timeframe])

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
            <p className="text-sm text-slate-400">{headerDetails}</p>
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

          {strategies.length ? (
            <div className="space-y-3 rounded-3xl border border-white/5 bg-black/30 p-4">
              <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Strategy wiring</p>
              <div className="grid gap-4 md:grid-cols-2">
                {strategies.map((strategy) => (
                  <article
                    key={strategy.id}
                    className="flex flex-col gap-3 rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200"
                  >
                    <div>
                      <p className="text-xs uppercase tracking-[0.3em] text-slate-400">{strategy.id}</p>
                      <h4 className="text-lg font-semibold text-white">{strategy.name || 'Unnamed strategy'}</h4>
                    </div>
                    <dl className="grid gap-2 text-xs text-slate-300 sm:grid-cols-2">
                      <div>
                        <dt className="uppercase tracking-[0.3em] text-slate-500">Symbols</dt>
                        <dd className="text-sm text-slate-100">{strategy.symbols?.join(', ') || '—'}</dd>
                      </div>
                      <div>
                        <dt className="uppercase tracking-[0.3em] text-slate-500">Timeframe</dt>
                        <dd className="text-sm text-slate-100">{strategy.timeframe || '—'}</dd>
                      </div>
                      <div>
                        <dt className="uppercase tracking-[0.3em] text-slate-500">Datasource</dt>
                        <dd className="text-sm text-slate-100">{strategy.datasource || botMeta.datasource || '—'}</dd>
                      </div>
                      <div>
                        <dt className="uppercase tracking-[0.3em] text-slate-500">Exchange</dt>
                        <dd className="text-sm text-slate-100">{strategy.exchange || botMeta.exchange || '—'}</dd>
                      </div>
                    </dl>
                    <div>
                      <p className="text-[11px] uppercase tracking-[0.35em] text-slate-400">Indicator overlays</p>
                      {strategy.indicators?.length ? (
                        <ul className="divide-y divide-white/5 rounded-xl border border-white/10 bg-black/30">
                          {strategy.indicators.map((indicator, idx) => (
                            <li key={`${indicator.id || idx}-${idx}`} className="flex items-center justify-between gap-3 px-3 py-2">
                              <div className="flex items-center gap-2">
                                <span
                                  className="h-2 w-2 rounded-full"
                                  style={{ backgroundColor: indicator.color || '#a5b4fc' }}
                                />
                                <span className="text-sm text-white">{indicator.name || indicator.id}</span>
                              </div>
                              <span className="text-[10px] uppercase tracking-[0.35em] text-slate-400">{indicator.type || 'custom'}</span>
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <div className="rounded-xl border border-white/5 bg-white/5 px-3 py-2 text-xs text-slate-400">
                          No indicator overlays attached
                        </div>
                      )}
                    </div>
                  </article>
                ))}
              </div>
            </div>
          ) : null}

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
