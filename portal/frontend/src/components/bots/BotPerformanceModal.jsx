import { useEffect, useState, useCallback, useMemo } from 'react'
import { X } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { toSec } from './chartDataUtils.js'
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx'
import LoadingOverlay from '../LoadingOverlay.jsx'
import { ActiveTradeChip } from './ActiveTradeChip.jsx'
import { useBotPerformance } from './hooks/useBotPerformance.js'
import DecisionTrace from './DecisionTrace'

const BOOTLINE_POOL = {
  runtime: ['Spinning up bot runtime', 'Teaching the bot patience'],
  strategy: ['Warming up indicators', 'Wiring strategy overlays'],
  datasource: ['Syncing datasource with exchange', 'Counting R multiples'],
  generic: [
    'Teaching the bot patience',
    'Counting R multiples',
    'Syncing datasource with exchange',
    'Warming up indicators',
    'Wiring strategy overlays',
  ],
}

export function BotPerformanceModal({ bot, open, onClose, onRefresh }) {
  const [bootLine, setBootLine] = useState(BOOTLINE_POOL.generic[0])
  const [bootDots, setBootDots] = useState(1)
  const [activeSymbol, setActiveSymbol] = useState(null)
  const { getChart } = useChartState()

  const {
    action,
    error,
    handlePause,
    handlePlaybackInput,
    handleFocusSymbolChange,
    handleResume,
    loading,
    payload,
    playbackDraft,
    playbackLabel,
    runtimeStatus,
    speedSaving,
    streamEligible,
    streamStatus,
  } = useBotPerformance({ bot, open, onRefresh })

  const logs = payload?.logs || []
  const strategies = payload?.meta?.strategies || []
  const runtime = payload?.runtime || {}
  const seriesList = Array.isArray(payload?.series) ? payload.series : []
  const seriesBySymbol = useMemo(() => {
    const map = new Map()
    for (const series of seriesList) {
      if (series?.symbol) {
        map.set(series.symbol, series)
      }
    }
    return map
  }, [seriesList])
  const seriesSymbols = useMemo(() => seriesList.map((series) => series?.symbol).filter(Boolean), [seriesList])

  useEffect(() => {
    if (!seriesSymbols.length) {
      setActiveSymbol(null)
      return
    }
    setActiveSymbol((prev) => (prev && seriesSymbols.includes(prev) ? prev : seriesSymbols[0]))
  }, [seriesSymbols])

  useEffect(() => {
    if (open) {
      handleFocusSymbolChange?.(activeSymbol)
    }
  }, [activeSymbol, handleFocusSymbolChange, open])

  const activeSeries = activeSymbol ? seriesBySymbol.get(activeSymbol) : null
  const activeSymbolTrades = Array.isArray(activeSeries?.trades) ? activeSeries.trades : []
  const activeChartId = activeSymbol ? `bot-${bot?.id}-${activeSymbol}` : ''
  const chartHandle = useChartValue(activeChartId)

  const chartHasData = Array.isArray(activeSeries?.candles) && activeSeries.candles.length > 0
  const isBootingStatus = ['initialising', 'starting', 'booting'].includes(runtimeStatus)
  const isBooting = isBootingStatus || loading || streamStatus === 'connecting'
  const showInactiveState = !isBooting && (Boolean(payload?.inactive) || (!streamEligible && !chartHasData))
  const idleMessage = payload?.message || 'Start this bot to stream performance data.'
  const strategiesReady = strategies.length > 0
  const atmReady = strategies.some((entry) => Boolean(entry?.atm_template))
  const runtimeInitialising = runtimeStatus === 'initialising'

  const bootStage = useMemo(() => {
    if (runtimeInitialising || isBootingStatus) return 'runtime'
    if (streamStatus === 'connecting') return 'datasource'
    if (!strategiesReady) return 'strategy'
    if (!atmReady) return 'strategy'
    return 'generic'
  }, [runtimeInitialising, isBootingStatus, strategiesReady, atmReady, streamStatus])

  const lastCandle = useMemo(() => {
    if (!Array.isArray(activeSeries?.candles) || activeSeries.candles.length === 0) return null
    return activeSeries.candles[activeSeries.candles.length - 1]
  }, [activeSeries?.candles])

  const simTimeLabel = useMemo(() => {
    const epoch = toSec(lastCandle?.time)
    if (!Number.isFinite(epoch)) return null
    const date = new Date(epoch * 1000)
    const dateLabel = date.toLocaleDateString('en-US', {
      timeZone: 'UTC',
      month: 'short',
      day: '2-digit',
      year: 'numeric',
    })
    const timeLabel = date.toLocaleTimeString([], {
      timeZone: 'UTC',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
    })
    return `Sim Time: ${dateLabel} — ${timeLabel} UTC`
  }, [lastCandle?.time])

  const sumContracts = useCallback((legs = []) => {
    return legs.reduce((sum, leg) => sum + (Number(leg?.contracts) || 0), 0)
  }, [])

  const openTrades = useMemo(() => {
    const open = []
    for (const series of seriesList) {
      const trades = Array.isArray(series?.trades) ? series.trades : []
      for (const trade of trades) {
        const hasOpenLeg = (trade.legs || []).some((leg) => leg.status === 'open' || !leg.exit_time)
        const isOpen = hasOpenLeg || !trade?.closed_at
        if (isOpen) {
          open.push({ ...trade, symbol: trade.symbol || series.symbol })
        }
      }
    }
    return open
  }, [seriesList])

  const buildTradeChip = useCallback(
    (trade) => {
      if (!trade) return null
      const directionLabel = (trade.direction || 'long').toLowerCase() === 'short' ? 'Short' : 'Long'
      const contractsTotal = sumContracts(trade.legs)
      const openContracts = sumContracts((trade.legs || []).filter((leg) => leg.status === 'open' || !leg.exit_time))
      const entryPrice = Number(trade.entry_price)
      const stopPrice = Number(trade.stop_price)
      const targets = (trade.legs || [])
        .map((leg) => Number(leg?.target_price))
        .filter((value) => Number.isFinite(value))
      const tpPrice = targets.length
        ? (directionLabel === 'Short' ? Math.min(...targets) : Math.max(...targets))
        : null
      const openContractsCount = openContracts || contractsTotal || 0
      const fmtPrice = (value) => (Number.isFinite(value) ? Number(value).toFixed(2) : '—')
      return {
        directionLabel,
        sizeLabel: `${Math.max(1, openContractsCount || contractsTotal || 1)}x`,
        entry: fmtPrice(entryPrice),
        stop: fmtPrice(stopPrice),
        target: fmtPrice(tpPrice),
        direction: directionLabel.toLowerCase(),
        symbol: trade.symbol || '—',
        tradeId: trade.trade_id,
      }
    },
    [sumContracts],
  )

  const handleTradeHover = useCallback(
    (trade, hovering) => {
      const handles = chartHandle?.handles || chartHandle
      if (!handles) return
      if (hovering && trade && trade.symbol === activeSymbol) {
        handles.pulseTrade?.(trade)
      } else {
        handles.clearPulse?.()
      }
    },
    [activeSymbol, chartHandle],
  )

  const loadingLabel = runtimeInitialising ? 'Spinning up runtime…' : 'Loading bot performance…'
  const statusDisplay = isBooting ? 'booting' : runtimeStatus
  const bootOverlayVisible = isBooting && !error && !showInactiveState

  useEffect(() => {
    if (!isBooting) {
      setBootDots(1)
      return undefined
    }
    const stagePool = BOOTLINE_POOL[bootStage] || []
    const genericPool = BOOTLINE_POOL.generic
    const choosePhrase = () => {
      const options = [...stagePool, ...genericPool].filter(Boolean)
      if (!options.length) return
      const candidate = options[Math.floor(Math.random() * options.length)]
      setBootLine((previous) => {
        if (options.length > 1 && candidate === previous) {
          const alternate = options.find((option) => option !== previous) || candidate
          return alternate
        }
        return candidate
      })
    }
    choosePhrase()
    const phraseTimer = setInterval(choosePhrase, 3200)
    const dotsTimer = setInterval(() => {
      setBootDots((value) => (value % 3) + 1)
    }, 480)
    return () => {
      clearInterval(phraseTimer)
      clearInterval(dotsTimer)
    }
  }, [bootStage, isBooting])

  const bootLineDisplay = useMemo(() => {
    return `${bootLine}${'.'.repeat(Math.max(1, bootDots))}`
  }, [bootDots, bootLine])

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

  const progressDisplay =
    typeof runtime?.progress === 'number' ? `${Math.round(runtime.progress * 1000) / 10}%` : '—'
  const playbackDisabled = isBooting
  const isWalkForward = (bot?.mode || '').toLowerCase() === 'walk-forward'
  const isCompleted = runtimeStatus === 'completed' || runtimeStatus === 'stopped'
  const canPause = runtimeStatus === 'running' && isWalkForward
  const canResume = runtimeStatus === 'paused'
  const canRestart = isCompleted && isWalkForward
  const bootLineVisible = streamStatus === 'connecting' && streamEligible

  const focusChartAt = useCallback(
    (timeValue, price, symbol) => {
      if (!timeValue) return
      const time = toSec(timeValue)
      let handles = chartHandle?.handles || chartHandle
      if (symbol && seriesBySymbol.has(symbol)) {
        const targetId = `bot-${bot?.id}-${symbol}`
        const target = getChart(targetId)
        if (target) {
          handles = target?.handles || target
        }
        setActiveSymbol(symbol)
      }
      if (!handles?.focusAtTime) return
      handles.focusAtTime(time, price)
    },
    [bot?.id, chartHandle, getChart, seriesBySymbol],
  )

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div className="relative flex h-full max-h-[90vh] w-full max-w-6xl flex-col gap-4 overflow-hidden rounded-3xl border border-white/10 bg-[#0e1016] p-6 shadow-2xl">
        <header className="flex items-center justify-between gap-4 border-b border-white/5 pb-4">
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Bot lens</p>
            <h3 className="text-2xl font-semibold text-white">{bot?.name}</h3>
            <p className="text-sm text-slate-400">Decision trace and execution context for this bot run.</p>
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
          <div className="rounded-2xl border border-white/10 bg-black/30 px-4 py-3">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex flex-wrap items-center gap-3 text-xs text-slate-300">
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Status</span>
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-sm text-white">
                  {statusDisplay || '—'}
                </span>
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Progress</span>
                <span className="text-slate-200">{progressDisplay}</span>
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Feed</span>
                <span className="text-slate-200">{streamStatus}</span>
                <span className="text-[11px] uppercase tracking-[0.2em] text-slate-400">Sim Time</span>
                <span className="text-slate-300">{simTimeLabel || '—'}</span>
              </div>
              {(canPause || canResume || canRestart) && (
                <div className="flex flex-wrap items-center gap-2">
                  {canResume && (
                    <button
                      type="button"
                      onClick={handleResume}
                      disabled={action === 'resuming'}
                      className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-sm text-emerald-200 hover:border-emerald-500/50 hover:bg-emerald-500/20 disabled:opacity-50"
                    >
                      {action === 'resuming' ? 'Resuming...' : 'Resume'}
                    </button>
                  )}
                  {canPause && (
                    <button
                      type="button"
                      onClick={handlePause}
                      disabled={action === 'pausing'}
                      className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-sm text-amber-200 hover:border-amber-500/50 hover:bg-amber-500/20 disabled:opacity-50"
                    >
                      {action === 'pausing' ? 'Pausing...' : 'Pause'}
                    </button>
                  )}
                  {canRestart && (
                    <button
                      type="button"
                      onClick={onRefresh}
                      className="rounded-lg border border-blue-500/30 bg-blue-500/10 px-3 py-1 text-sm text-blue-200 hover:border-blue-500/50 hover:bg-blue-500/20"
                    >
                      Restart
                    </button>
                  )}
                </div>
              )}
            </div>
          </div>

          <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
            <p className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Active trades</p>
            {openTrades.length ? (
              <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                {openTrades.map((trade) => {
                  const chip = buildTradeChip(trade)
                  if (!chip) return null
                  return (
                    <ActiveTradeChip
                      key={trade.trade_id || `${trade.symbol}-${trade.entry_time}`}
                      chip={chip}
                      isActiveSymbol={trade.symbol === activeSymbol}
                      visible
                      onHover={(hovering) => handleTradeHover(trade, hovering)}
                    />
                  )
                })}
              </div>
            ) : (
              <p className="mt-2 text-xs text-slate-400">No active trades right now.</p>
            )}
          </div>

          <section className="space-y-3">
            <div className="flex items-center justify-between">
              <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
                Context Chart
              </p>
              {activeSymbol ? <span className="text-xs text-slate-400">{activeSymbol}</span> : null}
            </div>
            {seriesSymbols.length ? (
              <div className="flex flex-wrap items-center gap-2">
                {seriesSymbols.map((symbol) => (
                  <button
                    key={`bot-series-${symbol}`}
                    type="button"
                    onClick={() => setActiveSymbol(symbol)}
                    className={`rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.3em] ${
                      symbol === activeSymbol
                        ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-white'
                        : 'border-white/10 bg-black/20 text-slate-300 hover:border-white/30 hover:text-white'
                    }`}
                  >
                    {symbol}
                  </button>
                ))}
              </div>
            ) : null}

            <div className="relative min-h-[360px] rounded-2xl border border-white/10 bg-black/30 p-4">
              <div
                className={`absolute inset-0 z-10 flex items-center justify-center transition-opacity duration-300 ${
                  bootOverlayVisible ? 'opacity-100' : 'pointer-events-none opacity-0'
                }`}
              >
                <div className="rounded-full border border-white/10 bg-white/5 px-4 py-3 text-base font-semibold text-slate-100 shadow-sm animate-pulse">
                  {bootLineDisplay}
                </div>
              </div>
              <div
                className={`transition-opacity duration-300 ${
                  bootOverlayVisible ? 'pointer-events-none opacity-0' : 'opacity-100'
                }`}
              >
                {!bootOverlayVisible && loading ? <LoadingOverlay label={loadingLabel} /> : null}
                {error ? (
                  <div className="rounded-2xl border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-200">
                    {error}
                  </div>
                ) : showInactiveState ? (
                  <div className="flex h-[360px] items-center justify-center rounded-2xl border border-dashed border-white/10 bg-black/30 p-6 text-center text-sm text-slate-400">
                    {idleMessage}
                  </div>
                ) : chartHasData ? (
                  <BotLensChart
                    chartId={activeChartId}
                    candles={activeSeries?.candles || []}
                    trades={activeSymbolTrades}
                    overlays={activeSeries?.overlays || []}
                    playbackSpeed={playbackDraft}
                  />
                ) : (
                  <div className="flex h-[360px] items-center justify-center rounded-2xl border border-dashed border-white/10 bg-black/30 p-6 text-center text-sm text-slate-400">
                    Awaiting the first candle…
                  </div>
                )}
              </div>
              {bootLineVisible ? (
                <div className="pointer-events-none absolute right-4 top-4 rounded-full border border-white/20 bg-black/60 px-3 py-1 text-xs text-slate-200">
                  Establishing live feed…
                </div>
              ) : null}
            </div>
          </section>

          <div className="rounded-3xl border border-white/5 bg-black/30 p-4">
            <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
              Decision Trace
            </p>
            <p className="mt-1 text-sm text-slate-400">
              Strategy signals, decisions, and execution events in chronological order.
            </p>
          </div>
          <DecisionTrace
            decisions={payload?.decisions || []}
            logs={logs}
            onEventClick={(timeValue, price, symbol) => focusChartAt(timeValue, price, symbol)}
          />
        </div>
      </div>
    </div>
  )
}
