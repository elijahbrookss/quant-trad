import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { TriangleAlert, X, Check } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { toSec } from './chartDataUtils.js'
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx'
import LoadingOverlay from '../LoadingOverlay.jsx'
import { ActiveTradeChip } from './ActiveTradeChip.jsx'
import { useBotPerformance } from './hooks/useBotPerformance.js'
import DecisionTrace from './DecisionTrace'
import { OverlayToggleBar } from './OverlayToggleBar.jsx'
import { useOverlayControls } from './hooks/useOverlayControls.js'

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

const PHASE_LABELS = {
  prepare: 'Preparing bot runtime',
  prepare_series: 'Building strategy series',
  prepared: 'Series ready',
  prepare_runtime: 'Initializing runtime context',
  start_threads: 'Starting series threads',
  running: 'Running bot',
}

const STATUS_LABELS = {
  initialising: 'Preparing bot runtime',
  starting: 'Starting series threads',
  running: 'Running bot',
  crashed: 'Bot crashed',
}

export function BotPerformanceModal({ bot, open, onClose, onRefresh }) {
  const [bootLine, setBootLine] = useState(BOOTLINE_POOL.generic[0])
  const [bootDots, setBootDots] = useState(1)
  const [activeSymbol, setActiveSymbol] = useState(null)
  const [statsTab, setStatsTab] = useState('overview')
  const [showAllWarnings, setShowAllWarnings] = useState(false)
  const [warningsCollapsed, setWarningsCollapsed] = useState(true)
  const { getChart } = useChartState()

  const {
    action,
    error,
    handlePause,
    handleFocusSymbolChange,
    handleResume,
    loading,
    payload,
    runtimeStatus,
    streamEligible,
    streamStatus,
  } = useBotPerformance({ bot, open, onRefresh })

  const logs = payload?.logs || []
  const runtimeWarnings = Array.isArray(bot?.runtime?.warnings) ? bot.runtime.warnings : []
  const warnings = useMemo(() => {
    const fromPayload = Array.isArray(payload?.warnings) ? payload.warnings : []
    if (!fromPayload.length && !runtimeWarnings.length) return []
    if (!fromPayload.length) return runtimeWarnings
    if (!runtimeWarnings.length) return fromPayload
    const seen = new Set()
    const merged = []
    for (const entry of [...fromPayload, ...runtimeWarnings]) {
      const key = entry?.id || entry?.timestamp || entry?.message || JSON.stringify(entry)
      if (seen.has(key)) continue
      seen.add(key)
      merged.push(entry)
    }
    return merged
  }, [payload?.warnings, runtimeWarnings])
  const strategies = payload?.meta?.strategies || []
  const runtime = payload?.runtime || {}
  const seriesList = Array.isArray(payload?.series) ? payload.series : []
  const isBacktestRun = (bot?.run_type || '').toLowerCase() === 'backtest'
  const visibleWarnings = showAllWarnings ? warnings : warnings.slice(0, 5)
  const chartSectionRef = useRef(null)
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
  const isSeriesCompleted = useCallback(
    (series) => {
      if (!isBacktestRun || !series) return false

      // If the overall backtest is done, treat all series as done.
      const overallStatus = (runtimeStatus || '').toLowerCase()
      if (['completed', 'stopped'].includes(overallStatus)) return true

      const status = (series.status || series.runtime_status || '').toLowerCase()
      if (status === 'completed' || status === 'stopped') return true
      if (series.completed === true) return true

      const progress = Number(
        series.progress
        ?? series.stats?.progress
        ?? series.stats?.completion
        ?? series.stats?.progress_fraction,
      )
      if (Number.isFinite(progress) && progress >= 1) return true

      // Fallback: compare last bar time to known backtest end
      const endCandidates = [
        series.backtest_end,
        series.stats?.backtest_end,
        runtime?.backtest_end,
        runtime?.backtest?.end,
        payload?.backtest_end,
        payload?.meta?.backtest_end,
        bot?.backtest_end,
        bot?.last_run_artifact?.backtest_end,
      ]
      const endAt = endCandidates.map(toSec).find((v) => Number.isFinite(v))
      const candles = Array.isArray(series.candles) ? series.candles : []
      const lastCandleSec = candles.length ? toSec(candles[candles.length - 1]?.time) : null
      if (Number.isFinite(endAt) && Number.isFinite(lastCandleSec)) {
        // allow generous tolerance (one bar of 1h plus buffer)
        const tolerance = 3600 * 2 // seconds
        if (lastCandleSec + tolerance >= endAt) return true
      }

      return false
    },
    [
      isBacktestRun,
      runtimeStatus,
      runtime?.backtest_end,
      runtime?.backtest?.end,
      bot?.backtest_end,
      bot?.last_run_artifact?.backtest_end,
      payload?.backtest_end,
      payload?.meta?.backtest_end,
    ],
  )
  const scrollChartIntoView = useCallback(() => {
    const node = chartSectionRef.current
    if (!node) return
    node.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }, [])

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

  useEffect(() => {
    if (!open) {
      setShowAllWarnings(false)
      setWarningsCollapsed(true)
      return
    }
    if (warnings.length <= 5 && showAllWarnings) {
      setShowAllWarnings(false)
    }
  }, [open, warnings.length, showAllWarnings])

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
  const chartMode = bot?.run_type === 'backtest' ? payload?.runtime?.mode : undefined
  const chartPlaybackSpeed = 1

  const phaseLabel = PHASE_LABELS[runtime?.phase] || null
  const statusLabel = STATUS_LABELS[runtimeStatus] || null
  const bootStage = useMemo(() => {
    if (phaseLabel || statusLabel) return 'runtime'
    if (runtimeInitialising || isBootingStatus) return 'runtime'
    if (streamStatus === 'connecting') return 'datasource'
    if (!strategiesReady) return 'strategy'
    if (!atmReady) return 'strategy'
    return 'generic'
  }, [phaseLabel, statusLabel, runtimeInitialising, isBootingStatus, strategiesReady, atmReady, streamStatus])

  const lastCandle = useMemo(() => {
    if (!Array.isArray(activeSeries?.candles) || activeSeries.candles.length === 0) return null
    return activeSeries.candles[activeSeries.candles.length - 1]
  }, [activeSeries?.candles])

  const baseOverlays = useMemo(() => {
    return Array.isArray(activeSeries?.overlays) ? activeSeries.overlays : []
  }, [activeSeries?.overlays])

  const { overlayOptions, visibility: overlayVisibility, visibleOverlays, toggleOverlay } = useOverlayControls({
    overlays: baseOverlays,
  })

  // Build a map of latest prices and bar times per symbol for active trade chips
  const latestDataBySymbol = useMemo(() => {
    const dataMap = new Map()
    for (const series of seriesList) {
      if (!series?.symbol) continue
      const candles = series.candles
      if (!Array.isArray(candles) || candles.length === 0) continue
      const lastCandle = candles[candles.length - 1]
      // Use close price, or if intrabar data exists, use the most recent price
      const price = lastCandle?.close ?? lastCandle?.price ?? lastCandle?.c
      // Get the bar time - convert from epoch seconds if needed
      let barTime = lastCandle?.time ?? lastCandle?.t ?? lastCandle?.timestamp
      if (typeof barTime === 'number' && barTime < 1e12) {
        // Likely epoch seconds, convert to ms
        barTime = barTime * 1000
      }
      dataMap.set(series.symbol, {
        price: Number.isFinite(Number(price)) ? Number(price) : null,
        barTime: barTime ?? null,
      })
    }
    return dataMap
  }, [seriesList])

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
      const fmtSize = (value) => {
        if (!Number.isFinite(value) || value <= 0) return '—'
        if (value >= 100) return value.toFixed(0)
        if (value >= 10) return value.toFixed(1)
        if (value >= 1) return value.toFixed(2)
        return value.toFixed(4)
      }
      return {
        directionLabel,
        sizeLabel: `${fmtSize(openContractsCount)}x`,
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

  const toggleWarningScope = useCallback(
    (event) => {
      const modifierActivated = event?.altKey || event?.metaKey || event?.ctrlKey || event?.shiftKey || event?.detail > 1
      if (modifierActivated) {
        event?.preventDefault?.()
        setWarningsCollapsed(false)
        setShowAllWarnings((prev) => !prev)
      } else {
        setWarningsCollapsed((prev) => !prev)
      }
    },
    [],
  )

  const loadingLabel = runtimeInitialising ? 'Spinning up runtime…' : 'Loading bot performance…'
  const statusDisplay = isBooting ? 'booting' : runtimeStatus
  const bootOverlayVisible = isBooting && !error && !showInactiveState

  useEffect(() => {
    if (!isBooting) {
      setBootDots(1)
      return undefined
    }
    if (phaseLabel || statusLabel) {
      setBootLine(phaseLabel || statusLabel || '')
      const dotsTimer = setInterval(() => {
        setBootDots((value) => (value % 3) + 1)
      }, 480)
      return () => {
        clearInterval(dotsTimer)
      }
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
  }, [bootStage, isBooting, phaseLabel, statusLabel])

  const bootLineDisplay = useMemo(() => {
    const label = phaseLabel || statusLabel || bootLine
    return `${label}${'.'.repeat(Math.max(1, bootDots))}`
  }, [bootDots, bootLine, phaseLabel, statusLabel])

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
  const isWalkForward = (bot?.mode || '').toLowerCase() === 'walk-forward'
  const isCompleted = ['completed', 'stopped', 'crashed'].includes(runtimeStatus)
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

  const handleTradeClick = useCallback(
    (trade) => {
      if (!trade) return
      if (trade.symbol) {
        setActiveSymbol(trade.symbol)
      }
      scrollChartIntoView()
      focusChartAt(trade.entry_time || trade?.legs?.[0]?.entry_time || trade.closed_at, trade.entry_price, trade.symbol)
    },
    [focusChartAt, scrollChartIntoView],
  )

  // Aggregate stats across all symbols
  const aggregateStats = useMemo(() => {
    const stats = runtime?.stats || payload?.stats || {}
    return {
      net_pnl: stats.net_pnl,
      total_trades: stats.total_trades,
      wins: stats.wins,
      losses: stats.losses,
      win_rate: stats.win_rate,
      avg_win: stats.avg_win,
      avg_loss: stats.avg_loss,
      largest_win: stats.largest_win,
      largest_loss: stats.largest_loss,
      total_fees: stats.total_fees,
      max_drawdown: stats.max_drawdown,
    }
  }, [runtime?.stats, payload?.stats])

  // Per-symbol stats
  const symbolStats = useMemo(() => {
    return seriesList.map((series) => ({
      symbol: series.symbol,
      net_pnl: series.stats?.net_pnl,
      total_trades: series.stats?.total_trades,
      wins: series.stats?.wins,
      losses: series.stats?.losses,
      win_rate: series.stats?.win_rate,
      total_fees: series.stats?.total_fees,
    })).filter((s) => s.symbol)
  }, [seriesList])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4 backdrop-blur-sm">
      <div className="relative flex h-full max-h-[90vh] w-full max-w-[1400px] flex-col gap-5 overflow-hidden rounded-xl border border-slate-800 bg-slate-950 p-6 shadow-2xl">
        <header className="flex items-center justify-between gap-4 border-b border-slate-800 pb-4">
          <div className="min-w-0 flex-1">
            <h3 className="truncate text-xl font-medium text-slate-50">{bot?.name}</h3>
            <p className="mt-0.5 text-sm text-slate-400">Decision trace and execution context</p>
          </div>
          <button
            type="button"
            className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-slate-800 bg-slate-900/50 text-slate-400 transition-colors hover:border-slate-700 hover:bg-slate-900 hover:text-slate-300"
            onClick={onClose}
          >
            <X className="size-4" />
          </button>
        </header>

        <div className="flex flex-1 flex-col gap-5 overflow-auto">
          <div className="rounded-lg border border-slate-800 bg-slate-900/40 px-4 py-3">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs">
                <div className="flex items-center gap-1.5">
                  <span className="font-medium text-slate-500">Status:</span>
                  <span className="font-medium text-slate-300">{statusDisplay || '—'}</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="font-medium text-slate-500">Progress:</span>
                  <span className="tabular-nums font-medium text-slate-300">{progressDisplay}</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="font-medium text-slate-500">Feed:</span>
                  <span className="font-medium text-slate-300">{streamStatus}</span>
                </div>
                {simTimeLabel ? (
                  <div className="flex items-center gap-1.5">
                    <span className="font-medium text-slate-500">Sim Time:</span>
                    <span className="tabular-nums font-medium text-slate-300">{simTimeLabel}</span>
                  </div>
                ) : null}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                {(canPause || canResume || canRestart) && (
                  <>
                    {canResume && (
                      <button
                        type="button"
                        onClick={handleResume}
                        disabled={action === 'resuming'}
                        className="rounded-md border border-emerald-900/50 bg-emerald-950/30 px-3 py-1.5 text-xs font-medium text-emerald-300 transition-colors hover:border-emerald-800/60 hover:bg-emerald-950/50 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {action === 'resuming' ? 'Resuming…' : 'Resume'}
                      </button>
                    )}
                    {canPause && (
                      <button
                        type="button"
                        onClick={handlePause}
                        disabled={action === 'pausing'}
                        className="rounded-md border border-amber-900/50 bg-amber-950/30 px-3 py-1.5 text-xs font-medium text-amber-300 transition-colors hover:border-amber-800/60 hover:bg-amber-950/50 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {action === 'pausing' ? 'Pausing…' : 'Pause'}
                      </button>
                    )}
                    {canRestart && (
                      <button
                        type="button"
                        onClick={onRefresh}
                        className="rounded-md border border-sky-900/50 bg-sky-950/30 px-3 py-1.5 text-xs font-medium text-sky-300 transition-colors hover:border-sky-800/60 hover:bg-sky-950/50"
                      >
                        Restart
                      </button>
                    )}
                  </>
                )}
              </div>
            </div>
          </div>

          {warnings.length > 0 && (
            <div className="rounded-lg border border-amber-700/40 bg-amber-950/30 px-4 py-3 text-xs text-amber-100">
              <div
                className="flex items-center justify-between gap-2"
                title="Click to expand/collapse. Alt/Option or double-click to show all."
                onClick={toggleWarningScope}
              >
                <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-wider text-amber-200">
                  <TriangleAlert className="size-4 text-amber-300" />
                  <span>Warnings</span>
                </div>
                <div className="flex items-center gap-3 text-[10px] uppercase tracking-wider text-amber-200/70">
                  <span>{warnings.length} active</span>
                  {warnings.length > 5 ? (
                    <span className="cursor-default text-amber-300/80">
                      {showAllWarnings ? 'Showing all (⌥ click to collapse)' : '⌥ click or double-click to view all'}
                    </span>
                  ) : null}
                  <span className="text-amber-300/70">{warningsCollapsed ? 'Collapsed' : 'Expanded'}</span>
                </div>
              </div>
              {!warningsCollapsed && (
                <div className="mt-3 space-y-3">
                  {visibleWarnings.map((warning) => (
                    <div
                      key={warning.id || warning.timestamp || warning.message}
                      className="rounded-xl border border-amber-700/60 bg-amber-950/60 px-3 py-2 shadow-inner"
                    >
                      <p className="text-sm font-medium text-amber-100">{warning.message || 'Warning issued'}</p>
                      <WarningContext context={warning.context} />
                    </div>
                  ))}
                  {warnings.length > 5 && !showAllWarnings && (
                    <p className="text-[10px] text-amber-200/70">Showing first 5 warnings. Alt/Option or double-click the header to expand fully.</p>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Performance Stats Tabs */}
          <div className="rounded-lg border border-slate-800 bg-slate-900/40">
            {/* Tab Headers */}
            <div className="flex items-center gap-1 border-b border-slate-800 px-3 py-2 bg-slate-950/50">
              <button
                type="button"
                onClick={() => setStatsTab('overview')}
                className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                  statsTab === 'overview'
                    ? 'bg-slate-800/80 text-slate-200'
                    : 'text-slate-500 hover:bg-slate-900/50 hover:text-slate-400'
                }`}
              >
                Overview
              </button>
              <button
                type="button"
                onClick={() => setStatsTab('symbols')}
                className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                  statsTab === 'symbols'
                    ? 'bg-slate-800/80 text-slate-200'
                    : 'text-slate-500 hover:bg-slate-900/50 hover:text-slate-400'
                }`}
              >
                By Symbol
              </button>
              <button
                type="button"
                onClick={() => setStatsTab('risk')}
                className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                  statsTab === 'risk'
                    ? 'bg-slate-800/80 text-slate-200'
                    : 'text-slate-500 hover:bg-slate-900/50 hover:text-slate-400'
                }`}
              >
                Risk & Fees
              </button>
            </div>

            {/* Tab Content */}
            <div className="p-4">
              {statsTab === 'overview' && (
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-6">
                  <StatItem label="Net PNL" value={aggregateStats.net_pnl} format="currency" tone="pnl" />
                  <StatItem label="Total Trades" value={aggregateStats.total_trades} format="number" />
                  <StatItem label="Win Rate" value={aggregateStats.win_rate} format="percent" />
                  <StatItem label="Wins" value={aggregateStats.wins} format="number" />
                  <StatItem label="Losses" value={aggregateStats.losses} format="number" />
                  <StatItem label="Avg Win" value={aggregateStats.avg_win} format="currency" tone="positive" />
                  <StatItem label="Avg Loss" value={aggregateStats.avg_loss} format="currency" tone="negative" />
                  <StatItem label="Best Trade" value={aggregateStats.largest_win} format="currency" tone="positive" />
                  <StatItem label="Worst Trade" value={aggregateStats.largest_loss} format="currency" tone="negative" />
                  <StatItem label="Total Fees" value={aggregateStats.total_fees} format="currency" />
                  <StatItem label="Max Drawdown" value={aggregateStats.max_drawdown} format="currency" tone="negative" />
                </div>
              )}

              {statsTab === 'symbols' && (
                symbolStats.length > 0 ? (
                  <div className="space-y-3">
                    {symbolStats.map((stat) => (
                      <div key={stat.symbol} className="rounded-md border border-slate-800 bg-slate-950/50 p-3">
                        <div className="mb-2 flex items-center justify-between">
                          <span className="text-sm font-medium text-slate-300">{stat.symbol}</span>
                          <span className={`text-sm font-medium tabular-nums ${
                            Number(stat.net_pnl) > 0
                              ? 'text-emerald-400'
                              : Number(stat.net_pnl) < 0
                                ? 'text-rose-400'
                                : 'text-slate-400'
                          }`}>
                            {formatCurrency(stat.net_pnl)}
                          </span>
                        </div>
                        <div className="grid grid-cols-3 gap-2 text-xs sm:grid-cols-5">
                          <div>
                            <p className="text-slate-600">Trades</p>
                            <p className="font-medium tabular-nums text-slate-300">{formatNumber(stat.total_trades)}</p>
                          </div>
                          <div>
                            <p className="text-slate-600">Win Rate</p>
                            <p className="font-medium tabular-nums text-slate-300">{formatPercent(stat.win_rate)}</p>
                          </div>
                          <div>
                            <p className="text-slate-600">Wins</p>
                            <p className="font-medium tabular-nums text-slate-300">{formatNumber(stat.wins)}</p>
                          </div>
                          <div>
                            <p className="text-slate-600">Losses</p>
                            <p className="font-medium tabular-nums text-slate-300">{formatNumber(stat.losses)}</p>
                          </div>
                          <div>
                            <p className="text-slate-600">Fees</p>
                            <p className="font-medium tabular-nums text-slate-300">{formatCurrency(stat.total_fees)}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-slate-500">No per-symbol stats available</p>
                )
              )}

              {statsTab === 'risk' && (
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
                  <StatItem label="Total Fees Paid" value={aggregateStats.total_fees} format="currency" />
                  <StatItem label="Max Drawdown" value={aggregateStats.max_drawdown} format="currency" tone="negative" />
                  <StatItem label="Avg Win" value={aggregateStats.avg_win} format="currency" tone="positive" />
                  <StatItem label="Avg Loss" value={aggregateStats.avg_loss} format="currency" tone="negative" />
                  <StatItem label="Best Trade" value={aggregateStats.largest_win} format="currency" tone="positive" />
                  <StatItem label="Worst Trade" value={aggregateStats.largest_loss} format="currency" tone="negative" />
                </div>
              )}
            </div>
          </div>

          <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-4">
            <p className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Active Trades</p>
            {openTrades.length ? (
              <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                {openTrades.map((trade) => {
                  const chip = buildTradeChip(trade)
                  if (!chip) return null
                  const latestData = latestDataBySymbol.get(trade.symbol)
                  return (
                    <ActiveTradeChip
                      key={trade.trade_id || `${trade.symbol}-${trade.entry_time}`}
                      chip={chip}
                      trade={trade}
                      currentPrice={latestData?.price}
                      latestBarTime={latestData?.barTime}
                      isActiveSymbol={trade.symbol === activeSymbol}
                      visible
                      onHover={(hovering) => handleTradeHover(trade, hovering)}
                      onClick={() => handleTradeClick(trade)}
                    />
                  )
                })}
              </div>
            ) : (
              <p className="mt-2 text-sm text-slate-500">No active trades</p>
            )}
          </div>

          <section className="space-y-3" ref={chartSectionRef}>
            <div className="flex items-center justify-between">
              <p className="text-xs font-medium text-slate-400">Context Chart</p>
              {activeSymbol ? <span className="text-xs font-medium text-slate-500">{activeSymbol}</span> : null}
            </div>
            {seriesSymbols.length > 1 ? (
              <div className="flex flex-wrap items-center gap-2">
                {seriesSymbols.map((symbol) => (
                  <button
                    key={`bot-series-${symbol}`}
                    type="button"
                    onClick={() => setActiveSymbol(symbol)}
                    className={`rounded-md border px-2.5 py-1 text-xs font-medium uppercase tracking-wider transition-colors ${
                      symbol === activeSymbol
                        ? 'border-slate-700 bg-slate-800/80 text-slate-200'
                        : 'border-slate-800 bg-slate-950/50 text-slate-500 hover:border-slate-700 hover:bg-slate-950 hover:text-slate-400'
                    }`}
                  >
                    <span className="inline-flex items-center gap-1">
                      {symbol}
                      {isSeriesCompleted(seriesBySymbol.get(symbol)) ? (
                        <Check className="size-3 text-emerald-400" />
                      ) : null}
                    </span>
                  </button>
                ))}
              </div>
            ) : null}

            <OverlayToggleBar
              overlays={overlayOptions}
              visibility={overlayVisibility}
              onToggle={toggleOverlay}
            />

            <div className="relative min-h-[360px] rounded-lg border border-slate-800 bg-slate-900/40 p-4">
              <div
                className={`absolute inset-0 z-10 flex items-center justify-center transition-opacity duration-300 ${
                  bootOverlayVisible ? 'opacity-100' : 'pointer-events-none opacity-0'
                }`}
              >
                <div className="animate-pulse rounded-lg border border-slate-700 bg-slate-800/80 px-4 py-2.5 text-sm font-medium text-slate-200 shadow-sm">
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
                  <div className="rounded-lg border border-rose-900/50 bg-rose-950/20 p-4 text-sm text-rose-300">
                    {error}
                  </div>
                ) : showInactiveState ? (
                  <div className="flex h-[360px] items-center justify-center rounded-lg border border-dashed border-slate-800 bg-slate-950/50 p-6 text-center text-sm text-slate-500">
                    {idleMessage}
                  </div>
                ) : chartHasData ? (
                  <BotLensChart
                    chartId={activeChartId}
                    candles={activeSeries?.candles || []}
                    trades={activeSymbolTrades}
                    overlays={visibleOverlays}
                    playbackSpeed={chartPlaybackSpeed}
                    mode={chartMode}
                  />
                ) : (
                  <div className="flex h-[360px] items-center justify-center rounded-lg border border-dashed border-slate-800 bg-slate-950/50 p-6 text-center text-sm text-slate-500">
                    Awaiting the first candle…
                  </div>
                )}
              </div>
              {bootLineVisible ? (
                <div className="pointer-events-none absolute right-4 top-4 rounded-md border border-slate-700 bg-slate-900/90 px-3 py-1.5 text-xs font-medium text-slate-300 backdrop-blur-sm">
                  Establishing live feed…
                </div>
              ) : null}
            </div>
          </section>

          <div className="space-y-1 border-t border-slate-800 pt-5">
            <p className="text-xs font-medium text-slate-400">Decision Ledger</p>
            <p className="text-sm text-slate-500">Signal → Decision → Execution → Outcome with explainable context</p>
          </div>
          <DecisionTrace
            ledgerEvents={payload?.decisions || []}
            onEventClick={(timeValue, price, symbol) => focusChartAt(timeValue, price, symbol)}
          />
        </div>
      </div>
    </div>
  )
}

// Helper component for stat items
function StatItem({ label, value, format = 'number', tone = 'neutral' }) {
  let formattedValue = '—'
  if (value !== undefined && value !== null) {
    const num = Number(value)
    if (Number.isFinite(num)) {
      if (format === 'currency') {
        formattedValue = formatCurrency(num)
      } else if (format === 'percent') {
        formattedValue = formatPercent(num)
      } else {
        formattedValue = formatNumber(num)
      }
    }
  }

  let toneClass = 'text-slate-200'
  if (tone === 'pnl') {
    const num = Number(value)
    if (num > 0) toneClass = 'text-emerald-400'
    else if (num < 0) toneClass = 'text-rose-400'
    else toneClass = 'text-slate-300'
  } else if (tone === 'positive') {
    toneClass = 'text-emerald-400'
  } else if (tone === 'negative') {
    toneClass = 'text-rose-400'
  }

  return (
    <div className="space-y-1">
      <p className="text-[10px] font-medium uppercase tracking-wider text-slate-600">{label}</p>
      <p className={`truncate text-sm font-medium tabular-nums ${toneClass}`}>{formattedValue}</p>
    </div>
  )
}

// Formatting helpers
function formatCurrency(value) {
  if (value === undefined || value === null) return '—'
  const num = Number(value)
  if (!Number.isFinite(num)) return '—'
  return num.toFixed(2)
}

function formatPercent(value) {
  if (value === undefined || value === null) return '—'
  const num = Number(value)
  if (!Number.isFinite(num)) return '—'
  return `${(num * 100).toFixed(1)}%`
}

function formatNumber(value) {
  if (value === undefined || value === null) return '—'
  const num = Number(value)
  if (!Number.isFinite(num)) return '—'
  return num.toString()
}

function WarningContext({ context }) {
  if (!context || typeof context !== 'object') return null
  const entries = Object.entries(context).filter(
    ([, value]) => value !== undefined && value !== null && value !== '',
  )
  if (!entries.length) return null
  return (
    <div className="mt-2 flex flex-wrap gap-2 text-[10px] text-amber-100/80">
      {entries.map(([key, value]) => (
        <span key={key} className="rounded-full border border-amber-500/40 px-2 py-0.5">
          {formatWarningLabel(key)}: {String(value)}
        </span>
      ))}
    </div>
  )
}

function formatWarningLabel(label) {
  if (!label) return ''
  return label
    .toString()
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}
