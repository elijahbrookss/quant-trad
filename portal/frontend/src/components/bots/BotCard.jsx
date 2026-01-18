import { Play, Square, Eye, Trash2, Pause, RotateCw } from 'lucide-react'
import { memo, useMemo } from 'react'
import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols.js'

const computeStatus = (bot) => (bot?.runtime?.status || bot?.status || 'idle').toLowerCase()

/**
 * Get status color for the card stripe
 */
const getStatusColor = (status) => {
  const colors = {
    running: 'bg-emerald-500',
    paused: 'bg-amber-500',
    stopped: 'bg-rose-500',
    crashed: 'bg-rose-500',
    error: 'bg-rose-500',
    completed: 'bg-sky-500',
    starting: 'bg-slate-500',
    initialising: 'bg-slate-500',
    booting: 'bg-slate-500',
    idle: 'bg-slate-600',
  }
  return colors[status] || colors.idle
}

/**
 * Format date range concisely
 */
const formatDateShort = (dateStr) => {
  if (!dateStr) return '—'
  try {
    const date = new Date(dateStr)
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  } catch {
    return '—'
  }
}

/**
 * Get base currency from instrument or symbol
 */
const getBaseCurrency = (symbol, strategyLookup, strategyIds) => {
  if (!symbol) return symbol

  // Try to find instrument metadata from strategies
  for (const strategyId of strategyIds || []) {
    const strategy = strategyLookup?.get(strategyId)
    const instruments = strategy?.instruments || []
    for (const inst of instruments) {
      if (inst?.symbol?.toUpperCase() === symbol.toUpperCase()) {
        const baseCurrency = inst?.metadata?.instrument_fields?.base_currency || inst?.base_currency
        if (baseCurrency) return baseCurrency
      }
    }
  }

  // Fallback: if short enough, return as-is
  if (symbol.length <= 6) return symbol
  return symbol.slice(0, 4) + '…'
}

export const BotCard = memo(function BotCard({
  bot,
  strategyLookup,
  describeRange,
  statusBadge,
  nowEpochMs,
  onStart,
  onStop,
  onPause,
  onResume,
  onDelete,
  onOpen,
  pendingStart,
  pendingDelete,
}) {
  const assignedNames = useMemo(
    () =>
      (bot.strategy_ids || [])
        .map((id) => strategyLookup.get(id)?.name || id)
        .filter(Boolean),
    [bot.strategy_ids, strategyLookup],
  )

  const runtimeStatus = computeStatus(bot)
  const statusColor = getStatusColor(runtimeStatus)
  const progressValue =
    typeof bot.runtime?.progress === 'number'
      ? bot.runtime.progress
      : runtimeStatus === 'completed'
        ? 1
        : 0
  const progressPct = Math.min(100, Math.max(0, progressValue * 100))
  const showProgress = progressPct > 0 || ['running', 'starting', 'paused'].includes(runtimeStatus)
  const showPause = runtimeStatus === 'running' && bot.mode === 'walk-forward'
  const showResume = runtimeStatus === 'paused'
  const timeframeLabel = describeBotMeta(bot, strategyLookup, 'timeframe')
  const rawSymbols = describeBotMeta(bot, strategyLookup, 'symbol')
  const canStart = ['idle', 'stopped', 'completed', 'error', 'crashed'].includes(runtimeStatus)
  const canStop = ['running', 'paused', 'starting'].includes(runtimeStatus)
  const startLabel =
    runtimeStatus === 'completed'
      ? 'Rerun'
      : runtimeStatus === 'stopped' || runtimeStatus === 'crashed'
        ? 'Restart'
        : 'Start'
  const runDurationLabel = buildRunDuration(bot, runtimeStatus, nowEpochMs)
  const runType = (bot.run_type || 'backtest').toLowerCase()
  const runTypePill = runType === 'backtest' ? 'BT' : runType === 'paper' || runType === 'paper_trade' ? 'SIM' : 'LIVE'
  const modeLabel =
    runType === 'backtest'
      ? (bot.mode || '').toLowerCase() === 'instant'
        ? 'Fast'
        : (bot.mode || '').toLowerCase() === 'walk-forward'
          ? 'Full'
          : null
      : null

  // Get P&L data - this is the hero metric
  const stats = bot?.runtime?.stats || bot?.last_stats || {}
  const netPnl = stats.net_pnl
  const hasStats = netPnl !== undefined && netPnl !== null
  const pnlValue = Number(netPnl) || 0
  const pnlTone = pnlValue > 0 ? 'positive' : pnlValue < 0 ? 'negative' : 'neutral'
  const pnlDisplay = hasStats ? (pnlValue >= 0 ? '+' : '') + pnlValue.toFixed(2) : '—'

  // Compact stats for the card
  const totalTrades = stats.total_trades ?? 0
  const winRate = stats.win_rate
  const winRateDisplay = winRate !== undefined ? `${(winRate * 100).toFixed(0)}%` : '—'
  const maxDrawdown = stats.max_drawdown
  const ddDisplay = maxDrawdown !== undefined ? `${Math.abs(Number(maxDrawdown)).toFixed(1)}%` : null

  // Format symbols using base_currency
  const symbolsDisplay = useMemo(() => {
    if (!rawSymbols) return '—'
    const symbolList = rawSymbols.split(', ').slice(0, 3)
    const formatted = symbolList.map(s => getBaseCurrency(s, strategyLookup, bot.strategy_ids))
    const extra = rawSymbols.split(', ').length - 3
    return formatted.join(', ') + (extra > 0 ? ` +${extra}` : '')
  }, [rawSymbols, strategyLookup, bot.strategy_ids])

  // Compact date range
  const dateRangeShort = useMemo(() => {
    if (runType !== 'backtest') return 'Live'
    return `${formatDateShort(bot.backtest_start)} → ${formatDateShort(bot.backtest_end)}`
  }, [runType, bot.backtest_start, bot.backtest_end])

  return (
    <article className="group relative flex overflow-hidden rounded-xl border border-slate-800 bg-slate-900/40 transition-all duration-200 hover:border-slate-700 hover:bg-slate-900/60">
      {/* Status color stripe */}
      <div className={`w-1 flex-shrink-0 ${statusColor}`} />

      <div className="flex flex-1 flex-col gap-3 p-4">
        {/* Header: Name + P&L Hero */}
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <h4 className="truncate text-sm font-medium text-slate-100">{bot.name}</h4>
              <span className="shrink-0 rounded bg-slate-800/80 px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wider text-slate-500">
                {runTypePill}
              </span>
              {modeLabel ? (
                <span className="shrink-0 rounded bg-slate-900/80 px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wider text-slate-400">
                  {modeLabel}
                </span>
              ) : null}
            </div>
            <p className="mt-0.5 truncate text-xs text-slate-500">
              {assignedNames.length === 1 ? assignedNames[0] : `${assignedNames.length} strategies`}
            </p>
          </div>

          {/* P&L Hero Metric */}
          <div className="flex flex-col items-end">
            <span className={`text-lg font-semibold tabular-nums ${
              pnlTone === 'positive' ? 'text-emerald-400' :
              pnlTone === 'negative' ? 'text-rose-400' : 'text-slate-400'
            }`}>
              {pnlDisplay}
            </span>
            <span className="text-[10px] uppercase tracking-wider text-slate-600">P&L</span>
          </div>
        </div>

        {/* Compact info row */}
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
          <span title={rawSymbols}>{symbolsDisplay}</span>
          <span className="text-slate-700">·</span>
          <span>{timeframeLabel || '—'}</span>
          <span className="text-slate-700">·</span>
          <span className="tabular-nums">{dateRangeShort}</span>
          {runDurationLabel && (
            <>
              <span className="text-slate-700">·</span>
              <span className="tabular-nums text-slate-400">{runDurationLabel}</span>
            </>
          )}
        </div>

        {/* Progress bar - only show when relevant */}
        {showProgress && (
          <div className="flex items-center gap-2">
            <div className="h-1 flex-1 overflow-hidden rounded-full bg-slate-800">
              <div
                className={`h-full transition-all duration-500 ${
                  runtimeStatus === 'running' ? 'bg-emerald-500' :
                  runtimeStatus === 'paused' ? 'bg-amber-500' : 'bg-slate-600'
                }`}
                style={{ width: `${progressPct}%` }}
              />
            </div>
            <span className="shrink-0 text-[10px] tabular-nums text-slate-600">
              {Math.round(progressPct)}%
            </span>
          </div>
        )}

        {/* Compact stats row */}
        <div className="flex items-center justify-between border-t border-slate-800/50 pt-3">
          <div className="flex items-center gap-4 text-xs">
            <div className="flex items-center gap-1.5">
              <span className="text-slate-600">Trades</span>
              <span className="font-medium tabular-nums text-slate-300">{totalTrades}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-600">Win</span>
              <span className="font-medium tabular-nums text-slate-300">{winRateDisplay}</span>
            </div>
            {ddDisplay && (
              <div className="flex items-center gap-1.5">
                <span className="text-slate-600">DD</span>
                <span className="font-medium tabular-nums text-rose-400/80">{ddDisplay}</span>
              </div>
            )}
          </div>

          {/* Status badge - compact */}
          <span className={`text-[10px] font-medium uppercase tracking-wider ${
            runtimeStatus === 'running' ? 'text-emerald-400' :
            runtimeStatus === 'paused' ? 'text-amber-400' :
            runtimeStatus === 'error' || runtimeStatus === 'crashed' ? 'text-rose-400' :
            runtimeStatus === 'completed' ? 'text-sky-400' : 'text-slate-500'
          }`}>
            {runtimeStatus}
          </span>
        </div>

        {/* Action Buttons - compact */}
        <div className="flex items-center gap-1.5 pt-1">
          <ActionButton onClick={() => onOpen(bot)} icon={<Eye className="size-3" />} label="Details" size="sm" />
          {showPause && (
            <ActionButton onClick={() => onPause(bot.id)} icon={<Pause className="size-3" />} label="Pause" size="sm" />
          )}
          {showResume && (
            <ActionButton onClick={() => onResume(bot.id)} icon={<Play className="size-3" />} label="Resume" size="sm" variant="success" />
          )}
          {canStop && (
            <ActionButton onClick={() => onStop(bot.id)} icon={<Square className="size-3" />} label="Stop" size="sm" />
          )}
          {canStart && (
            <ActionButton
              onClick={() => onStart(bot.id)}
              icon={runtimeStatus === 'completed' ? <RotateCw className="size-3" /> : <Play className="size-3" />}
              label={pendingStart === bot.id ? '…' : startLabel}
              busy={pendingStart === bot.id}
              variant="primary"
              size="sm"
            />
          )}
          <div className="ml-auto">
            <ActionButton
              onClick={() => onDelete(bot.id)}
              icon={<Trash2 className="size-3" />}
              busy={pendingDelete === bot.id}
              variant="danger"
              size="sm"
              iconOnly
            />
          </div>
        </div>
      </div>
    </article>
  )
})

function buildRunDuration(bot, status, nowEpochMs = Date.now()) {
  if (!bot?.runtime?.started_at) return null
  if (!['running', 'paused', 'starting'].includes(status)) return null
  const startMs = Date.parse(bot.runtime.started_at)
  if (!Number.isFinite(startMs)) return null
  const elapsedSeconds = Math.max(0, Math.floor((nowEpochMs - startMs) / 1000))
  const hours = Math.floor(elapsedSeconds / 3600)
  const minutes = Math.floor((elapsedSeconds % 3600) / 60)
  const seconds = elapsedSeconds % 60

  if (hours > 0) {
    return `${hours}h ${String(minutes).padStart(2, '0')}m`
  }
  if (minutes > 0) {
    return `${minutes}m ${String(seconds).padStart(2, '0')}s`
  }
  return `${seconds}s`
}

function describeBotMeta(bot, strategyLookup, key) {
  if (!bot) return null

  const fromStrategies = new Set()
  for (const strategyId of bot.strategy_ids || []) {
    const strategy = strategyLookup.get(strategyId)
    if (!strategy) continue
    if (key === 'symbol') {
      symbolsFromInstrumentSlots(strategy.instrument_slots).forEach((sym) => fromStrategies.add(sym))
      continue
    }
    const value = strategy[key]
    if (value) {
      if (Array.isArray(value)) {
        value.forEach((val) => fromStrategies.add(val))
      } else {
        fromStrategies.add(value)
      }
    }
  }

  if (fromStrategies.size) {
    const label = Array.from(fromStrategies).join(', ')
    return key === 'timeframe' ? label.toUpperCase() : label
  }
  return null
}

function ActionButton({ onClick, icon, label, busy, variant = 'ghost', size = 'md', iconOnly = false }) {
  const variantClass = {
    primary:
      'border-slate-700 bg-slate-800/80 text-slate-200 hover:border-slate-600 hover:bg-slate-800 hover:text-slate-50',
    success:
      'border-emerald-900/50 bg-emerald-950/30 text-emerald-300 hover:border-emerald-800/60 hover:bg-emerald-950/50 hover:text-emerald-200',
    danger:
      'border-rose-900/50 bg-rose-950/30 text-rose-400 hover:border-rose-800/60 hover:bg-rose-950/50 hover:text-rose-300',
    ghost: 'border-slate-800 bg-slate-950/50 text-slate-400 hover:border-slate-700 hover:bg-slate-950 hover:text-slate-300',
  }[variant]

  const sizeClass = size === 'sm'
    ? iconOnly ? 'px-1.5 py-1.5' : 'px-2 py-1 text-[11px]'
    : 'px-2.5 py-1.5 text-xs'

  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-1 rounded-md border font-medium transition-colors ${variantClass} ${sizeClass} disabled:cursor-not-allowed disabled:opacity-50`}
      disabled={busy}
      title={iconOnly ? label : undefined}
    >
      {icon}
      {!iconOnly && <span>{label}</span>}
    </button>
  )
}

function buildStats(bot) {
  const source = bot?.runtime?.stats || bot?.last_stats || {}
  const entries = [
    { key: 'net_pnl', label: 'NET PNL', value: source.net_pnl },
    { key: 'total_trades', label: 'TOTAL TRADES', value: source.total_trades },
    { key: 'wins', label: 'WINS', value: source.wins },
    { key: 'losses', label: 'LOSSES', value: source.losses },
    { key: 'win_rate', label: 'WIN RATE', value: source.win_rate },
  ]

  return entries
    .map((entry) => {
      if (entry.value === undefined || entry.value === null) return null
      if (entry.key === 'net_pnl') {
        const numeric = Number(entry.value)
        const tone = Number.isFinite(numeric)
          ? numeric > 0
            ? 'positive'
            : numeric < 0
              ? 'negative'
              : 'neutral'
          : 'neutral'
        return {
          ...entry,
          tone,
          value: Number.isFinite(numeric) ? numeric.toFixed(2) : entry.value,
        }
      }

      return { ...entry, value: entry.value }
    })
    .filter(Boolean)
}

function buildWalletEntries(bot) {
  const balances = bot?.wallet_config?.balances
  if (!balances || typeof balances !== 'object') return []
  return Object.entries(balances)
    .map(([currency, amount]) => {
      const numeric = Number(amount)
      const value = Number.isFinite(numeric)
        ? numeric.toLocaleString(undefined, { maximumFractionDigits: 8 })
        : amount
      const label = currency ? currency.toUpperCase() : 'BAL'
      return { label, value }
    })
    .filter((entry) => entry.value !== undefined && entry.value !== null)
}

export function sortBots(bots) {
  return [...bots].sort((a, b) => {
    const aTime = Date.parse(a?.created_at || '') || 0
    const bTime = Date.parse(b?.created_at || '') || 0
    if (aTime !== bTime) return bTime - aTime
    return (a.name || '').localeCompare(b.name || '')
  })
}
