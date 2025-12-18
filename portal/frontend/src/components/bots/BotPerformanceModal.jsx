import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { X, ChevronDown } from 'lucide-react'
import { BotLensChart } from './BotLensChart.jsx'
import { toSec } from './chartDataUtils.js'
import { useChartValue } from '../../contexts/ChartStateContext.jsx'
import ATMTemplateSummary from '../atm/ATMTemplateSummary.jsx'
import LoadingOverlay from '../LoadingOverlay.jsx'
import { BotStatusChips } from './BotStatusChips.jsx'
import { PlaybackControls } from './PlaybackControls.jsx'
import { PerformanceStats } from './PerformanceStats.jsx'
import { TradeLogList } from './TradeLogList.jsx'
import { ActiveTradeChip } from './ActiveTradeChip.jsx'
import { describeLog, formatStatValue } from './botPerformanceFormatters.js'
import { useBotPerformance } from './hooks/useBotPerformance.js'

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
  const [expandedStrategies, setExpandedStrategies] = useState(() => new Set())
  const [logTab, setLogTab] = useState('trade')
  const chipHideTimeoutRef = useRef(null)
  const [chipVisible, setChipVisible] = useState(false)
  const [renderedChip, setRenderedChip] = useState(null)
  const [bootLine, setBootLine] = useState(BOOTLINE_POOL.generic[0])
  const [bootDots, setBootDots] = useState(1)
  const chartHandle = useChartValue(`bot-${bot?.id}`)

  const {
    action,
    error,
    handlePause,
    handlePlaybackInput,
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

  useEffect(() => {
    setExpandedStrategies(new Set())
  }, [bot?.id])

  const logs = payload?.logs || []
  const quoteCurrency = payload?.stats?.quote_currency || payload?.trades?.[0]?.currency
  const strategies = payload?.meta?.strategies || []
  const botMeta = payload?.meta?.bot || {}
  const runtime = payload?.runtime || {}

  const chartHasData = Array.isArray(payload?.candles) && payload.candles.length > 0
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
    if (!Array.isArray(payload?.candles) || payload.candles.length === 0) return null
    return payload.candles[payload.candles.length - 1]
  }, [payload?.candles])

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

  const formatStatValueWithCurrency = useCallback(
    (key, value) => formatStatValue(key, value, quoteCurrency),
    [quoteCurrency],
  )

  const activeTrade = useMemo(() => {
    const trades = Array.isArray(payload?.trades) ? payload.trades : []
    return (
      trades.find((trade) => {
        const hasOpenLeg = (trade.legs || []).some((leg) => leg.status === 'open' || !leg.exit_time)
        return hasOpenLeg || !trade?.closed_at
      }) || null
    )
  }, [payload?.trades])

  const sumContracts = useCallback((legs = []) => {
    return legs.reduce((sum, leg) => sum + (Number(leg?.contracts) || 0), 0)
  }, [])

  const activeTradeChip = useMemo(() => {
    if (!activeTrade) return null
    const directionLabel = (activeTrade.direction || 'long').toLowerCase() === 'short' ? 'Short' : 'Long'
    const contractsTotal = sumContracts(activeTrade.legs)
    const openContracts = sumContracts((activeTrade.legs || []).filter((leg) => leg.status === 'open' || !leg.exit_time))
    const entryPrice = Number(activeTrade.entry_price)
    const stopPrice = Number(activeTrade.stop_price)
    const tickSize = Number(activeTrade.tick_size)
    const tickValue = Number(activeTrade.tick_value)
    const contractSize = Number(activeTrade.contract_size) || 1
    const targets = (activeTrade.legs || [])
      .map((leg) => Number(leg?.target_price))
      .filter((value) => Number.isFinite(value))
    const tpPrice = targets.length
      ? (directionLabel === 'Short' ? Math.min(...targets) : Math.max(...targets))
      : null
    const currentPrice = Number(lastCandle?.close ?? lastCandle?.price)
    const directionSign = directionLabel === 'Short' ? -1 : 1
    const riskPerUnit = Number.isFinite(entryPrice) && Number.isFinite(stopPrice) ? Math.abs(entryPrice - stopPrice) : null
    const rMultiple =
      riskPerUnit && Number.isFinite(currentPrice)
        ? ((currentPrice - entryPrice) * directionSign) / riskPerUnit
        : null
    const openContractsCount = openContracts || contractsTotal || 0
    const unrealized =
      Number.isFinite(entryPrice) &&
      Number.isFinite(currentPrice) &&
      Number.isFinite(tickSize) &&
      Number.isFinite(tickValue) &&
      tickSize !== 0
        ? ((currentPrice - entryPrice) / tickSize) * directionSign * tickValue * contractSize * openContractsCount
        : null
    const realized = Number.isFinite(activeTrade.net_pnl) ? activeTrade.net_pnl : 0
    const currentPnl = Number.isFinite(unrealized) ? realized + unrealized : realized
    const fmtPrice = (value) => (Number.isFinite(value) ? Number(value).toFixed(2) : '—')
    const fmtPnl = Number.isFinite(currentPnl)
      ? `${currentPnl >= 0 ? '+' : ''}${currentPnl.toFixed(2)}${quoteCurrency ? ` ${quoteCurrency}` : ''}`
      : '—'
    const fmtR = Number.isFinite(rMultiple) ? `${rMultiple >= 0 ? '+' : ''}${rMultiple.toFixed(2)} R` : '—'
    return {
      headline: `${directionLabel} ${Math.max(1, openContractsCount || contractsTotal || 1)}x @ ${fmtPrice(entryPrice)}`,
      r: fmtR,
      pnl: fmtPnl,
      sl: fmtPrice(stopPrice),
      tp: fmtPrice(tpPrice),
      direction: directionLabel.toLowerCase(),
    }
  }, [activeTrade, lastCandle?.close, lastCandle?.price, quoteCurrency, sumContracts])

  const handleChipHover = useCallback(
    (hovering) => {
      const handles = chartHandle?.handles || chartHandle
      if (!handles) return
      if (hovering && activeTrade) {
        handles.pulseTrade?.(activeTrade)
      } else {
        handles.clearPulse?.()
      }
    },
    [activeTrade, chartHandle],
  )

  useEffect(() => {
    if (chipHideTimeoutRef.current) {
      clearTimeout(chipHideTimeoutRef.current)
      chipHideTimeoutRef.current = null
    }
    if (activeTradeChip) {
      setRenderedChip(activeTradeChip)
      requestAnimationFrame(() => setChipVisible(true))
    } else if (renderedChip) {
      setChipVisible(false)
      chipHideTimeoutRef.current = setTimeout(() => {
        setRenderedChip(null)
      }, 200)
    }
  }, [activeTradeChip, renderedChip])

  const tradeMetrics = useMemo(() => {
    const trades = Array.isArray(payload?.trades) ? payload.trades : []
    const toContracts = (legs = []) => legs.reduce((sum, leg) => sum + (Number(leg?.contracts) || 0), 0)
    const sortedTrades = trades.slice().sort((a, b) => (toSec(a?.entry_time) || 0) - (toSec(b?.entry_time) || 0))
    let totalR = 0
    let running = 0
    let peak = 0
    let maxDrawdown = 0
    const rValues = []
    const pnlValues = []
    const winPnls = []
    const lossPnls = []
    for (const trade of sortedTrades) {
      const net = Number(trade?.net_pnl)
      const entryPrice = Number(trade?.entry_price)
      const stopPrice = Number(trade?.stop_price)
      const tickSize = Number(trade?.tick_size)
      const tickValue = Number(trade?.tick_value)
      const contracts = toContracts(trade?.legs)
      const riskValue =
        Number.isFinite(entryPrice) &&
        Number.isFinite(stopPrice) &&
        Number.isFinite(tickSize) &&
        Number.isFinite(tickValue) &&
        tickSize !== 0 &&
        contracts > 0
          ? (Math.abs(entryPrice - stopPrice) / tickSize) * tickValue * contracts
          : null
      const r = riskValue && Number.isFinite(net) && riskValue !== 0 ? net / riskValue : null
      if (Number.isFinite(r)) {
        rValues.push(r)
        totalR += r
      }
      if (Number.isFinite(net)) {
        pnlValues.push(net)
        running += net
        if (running > peak) peak = running
        const dd = peak - running
        if (dd > maxDrawdown) maxDrawdown = dd
        if (net > 0) winPnls.push(net)
        else if (net < 0) lossPnls.push(net)
      }
    }
    const expectancyR = rValues.length ? rValues.reduce((a, b) => a + b, 0) / rValues.length : null
    const expectancyPnl = pnlValues.length ? pnlValues.reduce((a, b) => a + b, 0) / pnlValues.length : null
    const avgWin = winPnls.length ? winPnls.reduce((a, b) => a + b, 0) / winPnls.length : null
    const avgLoss = lossPnls.length ? lossPnls.reduce((a, b) => a + b, 0) / lossPnls.length : null
    return { totalR, expectancyR, expectancyPnl, maxDrawdown, avgWin, avgLoss }
  }, [payload?.trades])

  const statEntries = useMemo(() => {
    const hidden = new Set(['quote_currency', 'legs_closed', 'breakeven_trades', 'completed_trades'])
    const entries = Object.entries(payload?.stats || {}).filter(([key]) => !hidden.has(key))
    if (Number.isFinite(tradeMetrics.maxDrawdown)) entries.push(['max_drawdown', tradeMetrics.maxDrawdown])
    if (Number.isFinite(tradeMetrics.expectancyR)) entries.push(['expectancy_r', tradeMetrics.expectancyR])
    if (Number.isFinite(tradeMetrics.expectancyPnl)) entries.push(['expectancy_value', tradeMetrics.expectancyPnl])
    if (Number.isFinite(tradeMetrics.avgWin)) entries.push(['avg_win', tradeMetrics.avgWin])
    if (Number.isFinite(tradeMetrics.avgLoss)) entries.push(['avg_loss', tradeMetrics.avgLoss])
    if (Number.isFinite(tradeMetrics.totalR)) entries.push(['total_r', tradeMetrics.totalR])
    return entries
  }, [payload?.stats, tradeMetrics])

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

  const formatRiskReward = useCallback((metrics) => {
    if (!metrics || metrics.reward_to_risk === null || metrics.reward_to_risk === undefined) {
      return '—'
    }
    const numeric = Number(metrics.reward_to_risk)
    if (!Number.isFinite(numeric)) {
      return '—'
    }
    return `${numeric.toFixed(2)} R`
  }, [])

  const toggleStrategyDetails = useCallback((strategyId) => {
    if (!strategyId) return
    setExpandedStrategies((prev) => {
      const next = new Set(prev)
      if (next.has(strategyId)) {
        next.delete(strategyId)
      } else {
        next.add(strategyId)
      }
      return next
    })
  }, [])

  const headerDetails = useMemo(() => {
    const parts = []
    const collectUnique = (iterable) => {
      const set = new Set()
      for (const value of iterable || []) {
        if (value) set.add(value)
      }
      return Array.from(set)
    }
    const strategySymbols = collectUnique(strategies.flatMap((s) => s?.symbols || []))
    if (strategySymbols.length) {
      parts.push(`Symbols: ${strategySymbols.join(', ')}`)
    }
    const strategyTimeframes = collectUnique(strategies.map((s) => s?.timeframe))
    const timeframes = strategyTimeframes.length
      ? strategyTimeframes
      : botMeta.timeframe
        ? [botMeta.timeframe]
        : []
    if (timeframes.length) {
      parts.push(`Timeframe: ${timeframes.join(', ')}`)
    }
    const datasources = collectUnique(strategies.map((s) => s?.datasource || botMeta.datasource))
    if (datasources.length) {
      parts.push(`Datasource: ${datasources.join(', ')}`)
    }
    const exchanges = collectUnique(strategies.map((s) => s?.exchange || botMeta.exchange))
    if (exchanges.length) {
      parts.push(`Exchange: ${exchanges.join(', ')}`)
    }
    parts.push(`Mode: ${bot?.mode}`)
    parts.push(`Run: ${(bot?.run_type || 'backtest').replace('_', ' ')}`)
    return parts.filter(Boolean).join(' • ')
  }, [strategies, botMeta.timeframe, botMeta.datasource, botMeta.exchange, bot?.mode, bot?.run_type])

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
  const canPause = runtimeStatus === 'running' && (bot?.mode || '').toLowerCase() === 'walk-forward'
  const canResume = runtimeStatus === 'paused'
  const bootLineVisible = streamStatus === 'connecting' && streamEligible

  const logsForDisplay = useMemo(() => logs.filter((entry) => describeLog(entry) !== '—'), [logs])

  const focusChartOnLog = useCallback(
    (entry) => {
      const handles = chartHandle?.handles || chartHandle
      if (!handles?.focusAtTime) return
      const time = entry?.bar_time || entry?.event_time || entry?.timestamp || entry?.time
      const price = entry?.price
      handles.focusAtTime(time, price)
    },
    [chartHandle],
  )

  const handleZoomIn = useCallback(() => {
    const handles = chartHandle?.handles || chartHandle
    handles?.zoomIn?.()
  }, [chartHandle])

  const handleZoomOut = useCallback(() => {
    const handles = chartHandle?.handles || chartHandle
    handles?.zoomOut?.()
  }, [chartHandle])

  const handleCenterView = useCallback(() => {
    const handles = chartHandle?.handles || chartHandle
    handles?.centerView?.()
  }, [chartHandle])

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
          <BotStatusChips statusDisplay={statusDisplay} progressDisplay={progressDisplay} streamStatus={streamStatus} />

          <PlaybackControls
            canPause={canPause}
            canResume={canResume}
            onPause={handlePause}
            onResume={handleResume}
            action={action}
            playbackDisabled={playbackDisabled}
            simTimeLabel={simTimeLabel}
            onZoomIn={handleZoomIn}
            onZoomOut={handleZoomOut}
            onCenter={handleCenterView}
            playbackDraft={playbackDraft}
            playbackLabel={playbackLabel}
            onPlaybackChange={handlePlaybackInput}
            speedSaving={speedSaving}
          />

        <div className="flex flex-wrap items-center justify-center mb-2">
          <ActiveTradeChip chip={renderedChip} visible={chipVisible} onHover={handleChipHover} />
        </div>

        <div className="relative min-h-[360px]">
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
                <div className="rounded-2xl border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-200">{error}</div>
              ) : showInactiveState ? (
                <div className="flex h-[360px] items-center justify-center rounded-2xl border border-dashed border-white/10 bg-black/30 p-6 text-center text-sm text-slate-400">
                  {idleMessage}
                </div>
              ) : chartHasData ? (
                <BotLensChart
                  chartId={`bot-${bot?.id}`}
                  candles={payload?.candles || []}
                  trades={payload?.trades || []}
                  overlays={payload?.overlays || []}
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

          {strategies.length ? (
            <div className="space-y-4 rounded-3xl border border-white/5 bg-black/30 p-4">
              <div className="flex items-center justify-between">
                <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Strategy wiring</p>
                <span className="text-xs text-slate-400">{strategies.length} linked</span>
              </div>
              <div className="space-y-3">
                {strategies.map((strategy) => {
                  const summarySymbols = strategy.symbols?.join(', ') || strategy.symbol || '—'
                  const timeframeLabel = strategy.timeframe || botMeta.timeframe || '—'
                  const datasourceLabel = strategy.datasource || botMeta.datasource || '—'
                  const exchangeLabel = strategy.exchange || botMeta.exchange || '—'
                  const primaryInstrument = strategy.instruments?.[0] || strategy.instrument
                  const contractSize = primaryInstrument?.contract_size ?? strategy.atm_template?.contract_size ?? '—'
                  const rrDisplay = formatRiskReward(strategy.atm_metrics)
                  const isExpanded = expandedStrategies.has(strategy.id)
                  return (
                    <article key={strategy.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div>
                          <h4 className="text-lg font-semibold text-white">{strategy.name || 'Unnamed strategy'}</h4>
                          <p className="font-mono text-[11px] uppercase tracking-[0.3em] text-slate-500">{strategy.id}</p>
                        </div>
                        <button
                          type="button"
                          onClick={() => toggleStrategyDetails(strategy.id)}
                          className="inline-flex items-center gap-2 rounded-full border border-white/20 px-3 py-1 text-xs text-slate-100 hover:border-white/40"
                        >
                          <span>{isExpanded ? 'Hide details' : 'Details'}</span>
                          <ChevronDown className={`size-4 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                        </button>
                      </div>
                      <dl className="mt-3 grid gap-3 text-xs text-slate-400 sm:grid-cols-4">
                        <div>
                          <dt className="uppercase tracking-[0.3em]">Symbols</dt>
                          <dd className="text-sm text-white">{summarySymbols}</dd>
                        </div>
                        <div>
                          <dt className="uppercase tracking-[0.3em]">Timeframe</dt>
                          <dd className="text-sm text-white">{timeframeLabel}</dd>
                        </div>
                        <div>
                          <dt className="uppercase tracking-[0.3em]">Datasource / Exch.</dt>
                          <dd className="text-sm text-white">{datasourceLabel} / {exchangeLabel}</dd>
                        </div>
                        <div>
                          <dt className="uppercase tracking-[0.3em]">Contract &amp; R:R</dt>
                          <dd className="text-sm text-white">
                            {contractSize} / {rrDisplay}
                          </dd>
                        </div>
                      </dl>
                      {isExpanded ? (
                        <div className="mt-4 space-y-4 border-t border-white/10 pt-4 text-sm text-slate-200">
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.35em] text-slate-400">Indicator overlays</p>
                            {strategy.indicators?.length ? (
                              <ul className="mt-2 divide-y divide-white/5 rounded-xl border border-white/10 bg-black/30">
                                {strategy.indicators.map((indicator, idx) => (
                                  <li key={`${indicator.id || idx}-${idx}`} className="flex items-center justify-between gap-3 px-3 py-2">
                                    <div className="flex flex-wrap items-center gap-2 text-sm text-white">
                                      <span
                                        className="h-2 w-2 rounded-full"
                                        style={{ backgroundColor: indicator.color || '#a5b4fc' }}
                                      />
                                      <span>{indicator.name || indicator.id || 'Indicator'}</span>
                                      {indicator.id ? (
                                        <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-slate-500">
                                          {indicator.id}
                                        </span>
                                      ) : null}
                                    </div>
                                    <span className="text-[10px] uppercase tracking-[0.35em] text-slate-400">
                                      {indicator.type || 'custom'}
                                    </span>
                                  </li>
                                ))}
                              </ul>
                            ) : (
                              <div className="mt-2 rounded-xl border border-white/5 bg-white/5 px-3 py-2 text-xs text-slate-400">
                                No indicator overlays attached
                              </div>
                            )}
                          </div>
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.35em] text-slate-400">Instruments</p>
                            {strategy.instruments?.length ? (
                              <ul className="mt-2 divide-y divide-white/5 rounded-xl border border-white/10 bg-black/30">
                                {strategy.instruments.map((instrument, idx) => (
                                  <li key={`${instrument.symbol || idx}-${idx}`} className="flex flex-col gap-1 px-3 py-2">
                                    <div className="flex items-center justify-between text-sm text-white">
                                      <span>{instrument.symbol || 'Instrument'}</span>
                                      <span className="text-[11px] uppercase tracking-[0.3em] text-slate-500">
                                        {instrument.quote_currency || '—'}
                                      </span>
                                    </div>
                                    <div className="text-xs text-slate-300">
                                      <div className="flex flex-wrap gap-3">
                                        <span>Tick: {instrument.tick_size ?? '—'}</span>
                                        <span>
                                          Tick Value: {instrument.tick_value ?? '—'}
                                          {instrument.quote_currency ? ` ${instrument.quote_currency}` : ''}
                                        </span>
                                      </div>
                                      <div className="flex flex-wrap gap-3">
                                        <span>Contract: {instrument.contract_size ?? '—'}</span>
                                        <span>
                                          Fees{' '}
                                          {instrument.maker_fee_rate != null
                                            ? `${(Number(instrument.maker_fee_rate) * 100).toFixed(2)}%`
                                            : '—'}{' '}
                                          /{' '}
                                          {instrument.taker_fee_rate != null
                                            ? `${(Number(instrument.taker_fee_rate) * 100).toFixed(2)}%`
                                            : '—'}
                                        </span>
                                      </div>
                                    </div>
                                  </li>
                                ))}
                              </ul>
                            ) : (
                              <div className="mt-2 rounded-xl border border-white/5 bg-white/5 px-3 py-2 text-xs text-slate-400">
                                No instrument metadata attached
                              </div>
                            )}
                          </div>
                          <div>
                            <div className="mb-2 flex items-center justify-between text-[11px] uppercase tracking-[0.35em] text-slate-400">
                              <span>ATM template</span>
                              <span className="text-xs text-slate-200">R:R {rrDisplay}</span>
                            </div>
                            <ATMTemplateSummary template={strategy.atm_template} />
                          </div>
                        </div>
                      ) : null}
                    </article>
                  )
                })}
              </div>
            </div>
          ) : null}

          <PerformanceStats statEntries={statEntries} formatStatValue={formatStatValueWithCurrency} />

          <TradeLogList logs={logsForDisplay} logTab={logTab} onTabChange={setLogTab} onFocusLog={focusChartOnLog} />
        </div>
      </div>
    </div>
  )
}
