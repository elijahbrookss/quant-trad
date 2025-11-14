import { useEffect, useState, useCallback, useMemo } from 'react'
import { X, Pause, RotateCw } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { fetchBotPerformance, pauseBot, resumeBot } from '../../adapters/bot.adapter.js'
import LoadingOverlay from '../LoadingOverlay.jsx'

export function BotPerformanceModal({ bot, open, onClose, onRefresh }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [payload, setPayload] = useState(null)
  const [action, setAction] = useState(null)

  const strategies = payload?.meta?.strategies || []
  const botMeta = payload?.meta?.bot || {}
  const runtime = payload?.runtime || {}
  const logs = payload?.logs || []

  const formatTimestamp = useCallback((value) => {
    if (!value) return '—'
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return value
    return date.toLocaleTimeString([], { hour12: false })
  }, [])

  const describeLog = useCallback((entry) => {
    if (!entry) return '—'
    if (entry.message) return entry.message
    const parts = []
    if (entry.symbol) parts.push(entry.symbol)
    if (entry.direction) parts.push(entry.direction.toUpperCase())
    if (entry.leg) parts.push(entry.leg)
    if (entry.price !== undefined && entry.price !== null) {
      const price = Number(entry.price)
      parts.push(Number.isFinite(price) ? `@ ${price.toFixed(4)}` : `@ ${entry.price}`)
    }
    if (entry.targets && Array.isArray(entry.targets)) {
      parts.push(`targets: ${entry.targets.map((t) => t.name).join(', ')}`)
    }
    return parts.length ? parts.join(' • ') : '—'
  }, [])

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

  const loadPerformance = useCallback(async (withLoader = true) => {
    if (!bot?.id) return
    if (withLoader) setLoading(true)
    setError(null)
    try {
      const data = await fetchBotPerformance(bot.id)
      setPayload(data)
    } catch (err) {
      setError(err?.message || 'Unable to fetch performance')
    } finally {
      if (withLoader) setLoading(false)
    }
  }, [bot?.id])

  useEffect(() => {
    if (open) {
      loadPerformance(true)
    }
  }, [open, loadPerformance])

  useEffect(() => {
    if (!open) return undefined
    const id = setInterval(() => {
      loadPerformance(false)
    }, 4000)
    return () => clearInterval(id)
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

  const handlePause = async () => {
    if (!bot?.id) return
    setAction('pause')
    setError(null)
    try {
      await pauseBot(bot.id)
      await loadPerformance(false)
      onRefresh?.()
    } catch (err) {
      setError(err?.message || 'Unable to pause bot')
    } finally {
      setAction(null)
    }
  }

  const handleResume = async () => {
    if (!bot?.id) return
    setAction('resume')
    setError(null)
    try {
      await resumeBot(bot.id)
      await loadPerformance(false)
      onRefresh?.()
    } catch (err) {
      setError(err?.message || 'Unable to resume bot')
    } finally {
      setAction(null)
    }
  }

  const runtimeStatus = (runtime?.status || bot?.status || 'idle').toLowerCase()
  const progressDisplay =
    typeof runtime?.progress === 'number' ? `${Math.round(runtime.progress * 1000) / 10}%` : '—'
  const timerDisplay =
    typeof runtime?.next_bar_in_seconds === 'number'
      ? `${Math.max(0, Math.round(runtime.next_bar_in_seconds))}s`
      : '—'
  const canPause = runtimeStatus === 'running' && (bot?.mode || '').toLowerCase() === 'walk-forward'
  const canResume = runtimeStatus === 'paused'

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
          <div className="grid gap-3 rounded-3xl border border-white/5 bg-black/30 p-4 text-[13px] text-slate-300 sm:grid-cols-3">
            <div>
              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Status</p>
              <p className="text-lg font-semibold text-white">{runtimeStatus}</p>
            </div>
            <div>
              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Progress</p>
              <p className="text-lg font-semibold text-white">{progressDisplay}</p>
            </div>
            <div>
              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Next bar</p>
              <p className="text-lg font-semibold text-white">{timerDisplay}</p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {canPause ? (
              <button
                type="button"
                onClick={handlePause}
                disabled={action === 'pause'}
                className="inline-flex items-center gap-2 rounded-full border border-amber-500/30 px-4 py-2 text-sm text-amber-200 hover:bg-amber-500/10 disabled:opacity-40"
              >
                <Pause className="size-4" /> Pause walk-forward
              </button>
            ) : null}
            {canResume ? (
              <button
                type="button"
                onClick={handleResume}
                disabled={action === 'resume'}
                className="inline-flex items-center gap-2 rounded-full border border-emerald-500/30 px-4 py-2 text-sm text-emerald-200 hover:bg-emerald-500/10 disabled:opacity-40"
              >
                <RotateCw className="size-4" /> Resume
              </button>
            ) : null}
          </div>
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
                overlays={payload?.overlays || []}
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

          <div className="space-y-3 rounded-3xl border border-white/5 bg-black/40 p-4">
            <div className="flex items-center justify-between">
              <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Runtime log</p>
              <span className="text-xs text-slate-400">Showing last {logs.length} events</span>
            </div>
            <div className="max-h-64 space-y-2 overflow-y-auto pr-1">
              {logs.length ? (
                logs
                  .slice()
                  .reverse()
                  .map((entry, idx) => (
                    <article
                      key={entry.id || `${entry.timestamp || 'log'}-${idx}`}
                      className="rounded-2xl border border-white/10 bg-white/5 p-3 text-sm text-white"
                    >
                      <div className="flex items-center justify-between text-xs uppercase tracking-[0.3em] text-slate-400">
                        <span>{entry.event || 'event'}</span>
                        <span>{formatTimestamp(entry.event_time || entry.bar_time || entry.timestamp)}</span>
                      </div>
                      <p className="mt-1 text-base font-semibold text-white">{describeLog(entry)}</p>
                      <div className="mt-1 flex flex-wrap gap-3 text-[11px] uppercase tracking-[0.3em] text-slate-500">
                        {entry.trade_id ? <span>Trade {entry.trade_id.slice(0, 8)}</span> : null}
                        {entry.bar_time ? <span>Bar {formatTimestamp(entry.bar_time)}</span> : null}
                        {entry.symbol ? <span>{entry.symbol}</span> : null}
                      </div>
                    </article>
                  ))
              ) : (
                <div className="rounded-2xl border border-dashed border-white/10 p-6 text-center text-sm text-slate-400">
                  No runtime events yet
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
