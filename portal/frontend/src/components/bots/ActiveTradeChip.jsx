import { TrendingUp, TrendingDown, Clock } from 'lucide-react'

/**
 * Format duration with appropriate unit based on length
 */
const formatDuration = (diffMs) => {
  if (diffMs < 0) return '0s'

  const secs = Math.floor(diffMs / 1000)
  const mins = Math.floor(secs / 60)
  const hours = Math.floor(mins / 60)
  const days = Math.floor(hours / 24)
  const weeks = Math.floor(days / 7)

  if (weeks > 0) {
    const remainingDays = days % 7
    return remainingDays > 0 ? `${weeks}w ${remainingDays}d` : `${weeks}w`
  }
  if (days > 0) {
    const remainingHours = hours % 24
    return remainingHours > 0 ? `${days}d ${remainingHours}h` : `${days}d`
  }
  if (hours > 0) {
    const remainingMins = mins % 60
    return remainingMins > 0 ? `${hours}h ${remainingMins}m` : `${hours}h`
  }
  if (mins > 0) {
    const remainingSecs = secs % 60
    return remainingSecs > 0 ? `${mins}m ${remainingSecs}s` : `${mins}m`
  }
  return `${secs}s`
}

/**
 * Truncate symbol for display
 */
const truncateSymbol = (symbol) => {
  if (!symbol) return '—'
  if (symbol.length <= 10) return symbol
  return symbol.slice(0, 8) + '…'
}

/**
 * Calculate unrealized P&L percentage
 */
const calcUnrealizedPct = (entry, current, direction) => {
  if (!entry || !current) return null
  const entryNum = Number(entry)
  const currentNum = Number(current)
  if (!Number.isFinite(entryNum) || !Number.isFinite(currentNum) || entryNum === 0) return null

  const diff = direction === 'short'
    ? entryNum - currentNum
    : currentNum - entryNum

  return (diff / entryNum) * 100
}

export function ActiveTradeChip({ chip, trade, currentPrice, latestBarTime, visible, onHover, isActiveSymbol, onClick }) {
  if (!chip) return null

  const isShort = chip.direction === 'short'
  const entryNum = Number(chip.entry)
  const currentNum = Number(currentPrice) || entryNum

  // Calculate P&L
  const unrealizedPct = calcUnrealizedPct(chip.entry, currentNum, chip.direction)

  // Calculate duration from entry to latest bar time (simulation time)
  const entryTime = trade?.entry_time ? new Date(trade.entry_time).getTime() : null
  const barTime = latestBarTime ? new Date(latestBarTime).getTime() : null
  const durationMs = entryTime && barTime && !Number.isNaN(entryTime) && !Number.isNaN(barTime)
    ? barTime - entryTime
    : null
  const duration = durationMs !== null && durationMs >= 0 ? formatDuration(durationMs) : null

  // Color based on P&L, not direction
  const isProfit = unrealizedPct !== null && unrealizedPct > 0
  const isLoss = unrealizedPct !== null && unrealizedPct < 0
  const isNeutral = unrealizedPct === null || unrealizedPct === 0

  // Dynamic colors based on profit/loss state
  const stateColors = isProfit
    ? {
        bg: 'bg-emerald-500/10',
        border: 'border-emerald-500/40',
        text: 'text-emerald-400',
        accent: 'bg-emerald-500',
        pnl: 'text-emerald-400'
      }
    : isLoss
    ? {
        bg: 'bg-rose-500/10',
        border: 'border-rose-500/40',
        text: 'text-rose-400',
        accent: 'bg-rose-500',
        pnl: 'text-rose-400'
      }
    : {
        bg: 'bg-slate-500/10',
        border: 'border-slate-600/40',
        text: 'text-slate-400',
        accent: 'bg-slate-500',
        pnl: 'text-slate-400'
      }

  return (
    <div
      className={`group relative overflow-hidden rounded-[3px] border transition-all duration-300 ${stateColors.border} ${stateColors.bg} ${
        visible ? 'opacity-100' : 'opacity-50'
      } ${isActiveSymbol ? 'ring-1 ring-white/20' : ''}`}
      onMouseEnter={() => onHover?.(true)}
      onMouseLeave={() => onHover?.(false)}
      onClick={onClick}
      role="button"
      tabIndex={0}
    >
      {/* P&L color stripe - transitions with profit/loss */}
      <div className={`absolute left-0 top-0 h-full w-1 transition-colors duration-300 ${stateColors.accent}`} />

      <div className="px-3 py-2.5 pl-4">
        {/* Main row: Symbol, Direction badge, Size, P&L */}
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2 min-w-0">
            <span className="text-sm font-medium text-slate-100 truncate" title={chip.symbol}>
              {truncateSymbol(chip.symbol)}
            </span>
            <span className="qt-mono flex shrink-0 items-center gap-0.5 rounded-[3px] border border-white/10 bg-slate-900/80 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-300">
              {isShort ? <TrendingDown className="size-3" /> : <TrendingUp className="size-3" />}
              {chip.directionLabel}
            </span>
            <span className="qt-mono shrink-0 rounded-[3px] border border-white/10 bg-slate-950/70 px-1.5 py-0.5 text-[10px] font-medium tabular-nums text-slate-400">
              {chip.sizeLabel}
            </span>
          </div>

          {/* Unrealized P&L - hero metric */}
          <span className={`shrink-0 text-base font-semibold tabular-nums transition-colors duration-300 ${stateColors.pnl}`}>
            {unrealizedPct !== null ? `${unrealizedPct >= 0 ? '+' : ''}${unrealizedPct.toFixed(2)}%` : '—'}
          </span>
        </div>

        {/* Secondary row: Duration in simulation time */}
        {duration && (
          <div className="mt-1.5 flex items-center gap-1 text-[11px] text-slate-500">
            <Clock className="size-3" />
            <span className="tabular-nums">{duration}</span>
          </div>
        )}
      </div>

      {/* Pulsing indicator for active trade */}
      {visible && (
        <div className="absolute right-2 top-2">
          <span className="relative flex size-2">
            <span className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 transition-colors duration-300 ${stateColors.accent}`} />
            <span className={`relative inline-flex size-2 rounded-full transition-colors duration-300 ${stateColors.accent}`} />
          </span>
        </div>
      )}
    </div>
  )
}
