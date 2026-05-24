import { useEffect, useMemo, useState } from 'react'
import { LocateFixed, Maximize2, Minimize2, RefreshCcw } from 'lucide-react'

import { BotLensChart } from '../../../../components/bots/BotLensChart.jsx'
import { OverlayToggleBar } from '../../../../components/bots/OverlayToggleBar.jsx'
import { useChartState } from '../../../../contexts/ChartStateContext.jsx'
import { SymbolSelectorPanel } from './SymbolSelectorPanel.jsx'

const RUNTIME_CHART_ID = 'botlens-runtime-chart'

function parseTimeframeToSeconds(rawTimeframe) {
  const text = String(rawTimeframe || '').trim().toLowerCase()
  if (!text) return null
  const match = text.match(/^(\d+)\s*([a-z]+)$/)
  if (!match) return null
  const amount = Number(match[1])
  const unit = match[2]
  if (!Number.isFinite(amount) || amount <= 0) return null
  if (['s', 'sec', 'secs', 'second', 'seconds'].includes(unit)) return amount
  if (['m', 'min', 'mins', 'minute', 'minutes'].includes(unit)) return amount * 60
  if (['h', 'hr', 'hrs', 'hour', 'hours'].includes(unit)) return amount * 3600
  if (['d', 'day', 'days'].includes(unit)) return amount * 86400
  if (['w', 'wk', 'wks', 'week', 'weeks'].includes(unit)) return amount * 7 * 86400
  if (['mo', 'mon', 'month', 'months'].includes(unit)) return amount * 30 * 86400
  if (['y', 'yr', 'yrs', 'year', 'years'].includes(unit)) return amount * 365 * 86400
  return null
}

function candleEpochSeconds(candle) {
  const raw = candle?.time
  if (typeof raw === 'number' && Number.isFinite(raw)) return raw > 1e12 ? Math.floor(raw / 1000) : raw
  const parsed = Date.parse(raw)
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : null
}

function formatDuration(totalSeconds) {
  const safeSeconds = Math.max(0, Math.floor(Number(totalSeconds || 0)))
  const hours = Math.floor(safeSeconds / 3600)
  const minutes = Math.floor((safeSeconds % 3600) / 60)
  const seconds = safeSeconds % 60
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m`
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`
}

function BarTimer({ candles, mode, timeframe }) {
  const [nowMs, setNowMs] = useState(() => Date.now())
  const timeframeSeconds = useMemo(() => parseTimeframeToSeconds(timeframe), [timeframe])
  const lastCandleTime = useMemo(() => {
    const rows = Array.isArray(candles) ? candles : []
    return candleEpochSeconds(rows[rows.length - 1])
  }, [candles])
  const liveMode = ['live', 'paper', 'paper_trading', 'observe_only'].includes(String(mode || '').trim().toLowerCase())

  useEffect(() => {
    if (!liveMode || !Number.isFinite(timeframeSeconds) || !Number.isFinite(lastCandleTime)) return undefined
    const timerId = window.setInterval(() => setNowMs(Date.now()), 1000)
    return () => window.clearInterval(timerId)
  }, [lastCandleTime, liveMode, timeframeSeconds])

  if (!liveMode) return null

  if (!Number.isFinite(timeframeSeconds) || !Number.isFinite(lastCandleTime)) {
    return <span>Next bar unavailable</span>
  }

  const nextCloseMs = (lastCandleTime + timeframeSeconds) * 1000
  const remainingSeconds = Math.ceil((nextCloseMs - nowMs) / 1000)
  if (remainingSeconds >= 0) {
    return <span>Next bar in {formatDuration(remainingSeconds)}</span>
  }
  return <span>Waiting for next bar +{formatDuration(Math.abs(remainingSeconds))}</span>
}

function directionClass(direction) {
  if (direction === 'up') return 'text-emerald-300'
  if (direction === 'down') return 'text-rose-300'
  return 'text-slate-300'
}

function runModeClass(tone) {
  return {
    amber: 'border-amber-300/50 bg-amber-300/12 text-amber-100',
    rose: 'border-rose-300/55 bg-rose-300/12 text-rose-100',
    sky: 'border-sky-300/45 bg-sky-300/10 text-sky-100',
    slate: 'border-white/10 bg-white/5 text-slate-200',
  }[tone] || 'border-white/10 bg-white/5 text-slate-200'
}

function ChartHud({ context, candles, mode }) {
  const symbol = context?.symbol || '—'
  const timeframe = context?.timeframe || '—'
  const openTradeCount = Number(context?.openTradeCount || 0)
  const direction = context?.priceDirection || 'unknown'

  return (
    <div className="pointer-events-none absolute left-3 top-3 z-10 max-w-[72%] text-slate-100 drop-shadow-[0_12px_28px_rgba(0,0,0,0.7)]">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm font-semibold text-white">{symbol}</span>
        <span className="rounded-[3px] border border-white/10 bg-black/40 px-1.5 py-0.5 text-[10px] font-semibold text-slate-200">
          {timeframe}
        </span>
        {context?.runMode ? (
          <span className={`rounded-[3px] border px-1.5 py-0.5 text-[10px] font-semibold ${runModeClass(context.runMode.tone)}`}>
            {context.runMode.label}
          </span>
        ) : null}
        {openTradeCount > 0 ? (
          <span className="rounded-[3px] border border-emerald-400/30 bg-emerald-400/10 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-200">
            Open {openTradeCount}
          </span>
        ) : null}
      </div>
      <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-slate-400">
        <span>Last {context?.lastPrice || '—'}</span>
        <span className={directionClass(direction)}>
          {context?.priceChange || '—'} {context?.priceChangePct || ''}
        </span>
        <span>Net {context?.netPnl || '—'}</span>
        <BarTimer candles={candles} mode={mode} timeframe={timeframe} />
      </div>
    </div>
  )
}

function tradeTone(entry) {
  const direction = String(entry?.chip?.direction || '').toLowerCase()
  const current = Number(entry?.currentPrice)
  const entryPrice = Number(entry?.chip?.entry)
  if (!Number.isFinite(current) || !Number.isFinite(entryPrice)) return 'neutral'
  const delta = direction === 'short' ? entryPrice - current : current - entryPrice
  if (delta > 0) return 'gain'
  if (delta < 0) return 'loss'
  return 'neutral'
}

function liveTradeClass(tone, isActiveSymbol) {
  const base = isActiveSymbol ? 'ring-1 ring-white/10' : ''
  if (tone === 'gain') return `${base} border-emerald-400/35 bg-emerald-400/10 text-emerald-100`
  if (tone === 'loss') return `${base} border-rose-400/35 bg-rose-400/10 text-rose-100`
  return `${base} border-white/10 bg-black/20 text-slate-200`
}

function LiveTradeStrip({ entries, onSelectSymbol }) {
  const liveEntries = Array.isArray(entries) ? entries : []
  if (!liveEntries.length) return null

  return (
    <div className="flex items-center gap-2 overflow-x-auto rounded-[3px] border border-white/8 bg-black/15 px-2 py-2">
      <span className="shrink-0 text-xs font-semibold text-slate-300">Live trades</span>
      <div className="flex min-w-0 flex-1 gap-1.5 overflow-x-auto">
        {liveEntries.map((entry) => {
          const tone = tradeTone(entry)
          const symbolKey = entry?.trade?.symbol_key
          return (
            <button
              key={entry.id}
              type="button"
              onClick={() => {
                if (symbolKey) onSelectSymbol(symbolKey)
              }}
              className={`inline-flex shrink-0 items-center gap-2 rounded-[3px] border px-2 py-1 text-[11px] font-semibold transition hover:border-white/20 ${liveTradeClass(tone, entry.isActiveSymbol)}`}
            >
              <span className="size-1.5 rounded-full bg-current opacity-80" />
              <span>{entry.chip?.symbol || entry.trade?.symbol || '—'}</span>
              <span>{entry.chip?.directionLabel || '—'}</span>
              {Number.isFinite(Number(entry.currentPrice)) ? <span>{Number(entry.currentPrice).toFixed(2)}</span> : null}
            </button>
          )
        })}
      </div>
    </div>
  )
}

function ChartViewport({
  canRefocus,
  centerView,
  isFullscreen,
  model,
  onLoadOlderHistory,
  onToggleFullscreen,
  overlayVisibility,
  viewportResetKey,
}) {
  const chartHeightClass = isFullscreen
    ? 'h-[calc(100vh-2.25rem)]'
    : 'h-[min(62vh,620px)] min-h-[430px]'
  const shellClassName = isFullscreen
    ? 'fixed inset-3 z-[100] bg-[#070a0f] p-0 shadow-[0_30px_90px_rgba(0,0,0,0.8)]'
    : 'relative'

  return (
    <div className={shellClassName}>
      <div className="absolute right-3 top-3 z-20 flex items-center gap-1.5">
        <button
          type="button"
          onClick={() => centerView?.()}
          disabled={!canRefocus}
          className="inline-flex h-8 items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/45 px-2.5 text-xs font-semibold text-slate-200 transition hover:border-white/20 hover:bg-black/60 hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
          aria-label="Reset chart view to latest window"
          title="Reset chart view"
        >
          <LocateFixed className="size-3.5" />
          Reset
        </button>
        <button
          type="button"
          onClick={onLoadOlderHistory}
          disabled={!model.candles.length}
          className="inline-flex h-8 items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/45 px-2.5 text-xs font-semibold text-slate-200 transition hover:border-white/20 hover:bg-black/60 hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
          title="Load older chart history"
        >
          <RefreshCcw className="size-3.5" />
          Older
        </button>
        <button
          type="button"
          onClick={onToggleFullscreen}
          className="inline-flex h-8 w-8 items-center justify-center rounded-[3px] border border-white/10 bg-black/45 text-slate-200 transition hover:border-white/20 hover:bg-black/60 hover:text-white"
          title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen chart'}
          aria-pressed={isFullscreen}
        >
          {isFullscreen ? <Minimize2 className="size-3.5" /> : <Maximize2 className="size-3.5" />}
        </button>
      </div>

      {model.status === 'ready' ? (
        <>
          <ChartHud context={model.chartContext} candles={model.candles} mode={model.timerMode} />
          <BotLensChart
            key={model.chartKey}
            chartId={RUNTIME_CHART_ID}
            candles={model.candles}
            trades={model.trades}
            overlays={model.overlays}
            mode={model.mode}
            playbackSpeed={model.playbackSpeed}
            timeframe={model.timeframe}
            overlayVisibility={overlayVisibility}
            viewportResetKey={viewportResetKey}
            heightClass={chartHeightClass}
          />
        </>
      ) : (
        <div className={`flex items-center justify-center rounded-[3px] border border-dashed border-white/10 bg-black/20 text-sm text-slate-400 ${chartHeightClass}`}>
          {model.emptyMessage}
        </div>
      )}
    </div>
  )
}

export function ChartPanel({
  model,
  symbolSelector,
  overlayOptions,
  overlayVisibility,
  onLoadOlderHistory,
  onSelectSymbol,
  onToggleOverlay,
  onToggleOverlayCollapse,
  overlayPanelCollapsed,
  viewportResetKey,
}) {
  const [isFullscreen, setIsFullscreen] = useState(false)
  const { getChart } = useChartState()
  const centerView = getChart(RUNTIME_CHART_ID)?.handles?.centerView
  const canRefocus = model.candles.length > 0 && typeof centerView === 'function'
  const barCount = Array.isArray(model.candles) ? model.candles.length : 0

  useEffect(() => {
    if (!isFullscreen) return undefined
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setIsFullscreen(false)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [isFullscreen])

  return (
    <section className="space-y-3">
      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(18rem,30rem)]">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <p className="text-sm font-semibold text-slate-100">Chart</p>
            {barCount > 0 ? (
              <span className="text-xs text-slate-500">
                {barCount} {barCount === 1 ? 'bar' : 'bars'} loaded
              </span>
            ) : null}
            {model.historyStatus === 'loading' ? (
              <span className="text-xs text-slate-500">Loading older bars</span>
            ) : null}
          </div>
          <p className="mt-1 truncate text-xs text-slate-500">
            {model.selectedSymbol?.label || model.selectedLabel || 'No symbol selected'}
          </p>
        </div>
        <SymbolSelectorPanel model={symbolSelector} onSelectSymbol={onSelectSymbol} />
      </div>

      <OverlayToggleBar
        overlays={overlayOptions}
        visibility={overlayVisibility}
        onToggle={onToggleOverlay}
        collapsed={overlayPanelCollapsed}
        onToggleCollapse={onToggleOverlayCollapse}
      />

      <LiveTradeStrip entries={model.liveTrades} onSelectSymbol={onSelectSymbol} />

      <ChartViewport
        canRefocus={canRefocus}
        centerView={centerView}
        isFullscreen={isFullscreen}
        model={model}
        onLoadOlderHistory={onLoadOlderHistory}
        onToggleFullscreen={() => setIsFullscreen((value) => !value)}
        overlayVisibility={overlayVisibility}
        viewportResetKey={viewportResetKey}
      />
    </section>
  )
}
