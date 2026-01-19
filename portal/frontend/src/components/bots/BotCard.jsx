import { Play, Square, Eye, Trash2, Pause, RotateCw, TriangleAlert } from 'lucide-react'
import { memo, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
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
  const navigate = useNavigate()
  const [errorOpen, setErrorOpen] = useState(false)
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
  const isCompleted = runtimeStatus === 'completed'
  const isStopped = runtimeStatus === 'stopped'
  const isCrashed = runtimeStatus === 'crashed' || runtimeStatus === 'error'
  const isIdle = runtimeStatus === 'idle'
  const showDetails = !isCompleted && !isStopped && !isCrashed && !isIdle
  const showViewReport = isCompleted
  const showViewError = isCrashed
  const startLabel =
    runtimeStatus === 'completed'
      ? 'Rerun'
      : runtimeStatus === 'stopped' || runtimeStatus === 'crashed' || runtimeStatus === 'error'
        ? 'Restart'
        : 'Start'
  const runDurationLabel = buildRunDuration(bot, runtimeStatus, nowEpochMs)
  const completedDuration = buildCompletedDuration(bot)
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

  const reportRunId = bot?.last_run_artifact?.run_id || bot?.runtime?.run_id || null
  const errorPayload = bot?.runtime?.error || bot?.last_run_artifact?.error || null
  const errorMessage = typeof errorPayload === 'string' ? errorPayload : errorPayload?.message || 'Bot crashed'
  const errorMeta = typeof errorPayload === 'object' && errorPayload ? errorPayload : {}

  return (
    <article className="group relative overflow-hidden rounded-2xl border border-slate-800 bg-gradient-to-br from-slate-950/80 via-slate-950/60 to-slate-900/80 shadow-[0_12px_40px_rgba(2,6,23,0.35)] transition-all duration-200 hover:border-slate-700">
      <div className={`absolute left-0 top-0 h-full w-1 ${statusColor}`} />
      <div className="relative flex flex-col gap-4 p-5">
        {/* Header */}
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <h4 className="truncate text-base font-semibold tracking-tight text-slate-100">{bot.name}</h4>
              <span className="shrink-0 rounded-full bg-slate-800/80 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                {runTypePill}
              </span>
              {modeLabel ? (
                <span className="shrink-0 rounded-full bg-slate-900/80 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                  {modeLabel}
                </span>
              ) : null}
            </div>
            <p className="mt-1 truncate text-xs text-slate-500">
              {assignedNames.length === 1 ? assignedNames[0] : `${assignedNames.length} strategies`}
            </p>
          </div>

          <div className="text-right">
            <span className={`text-2xl font-semibold tabular-nums ${
              pnlTone === 'positive' ? 'text-emerald-400' :
              pnlTone === 'negative' ? 'text-rose-400' : 'text-slate-400'
            }`}>
              {pnlDisplay}
            </span>
            <div className="text-[10px] uppercase tracking-widest text-slate-600">Net P&L</div>
          </div>
        </div>

        {/* Trading context */}
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-slate-400">
          <span className="rounded-full border border-slate-800/80 bg-slate-900/60 px-2 py-0.5 font-medium text-slate-300" title={rawSymbols}>
            {symbolsDisplay}
          </span>
          <span className="text-slate-700">•</span>
          <span className="uppercase tracking-wider">{timeframeLabel || '—'}</span>
          <span className="text-slate-700">•</span>
          <span className="tabular-nums">{dateRangeShort}</span>
          {runDurationLabel && (
            <>
              <span className="text-slate-700">•</span>
              <span className="tabular-nums text-slate-400">{runDurationLabel}</span>
            </>
          )}
        </div>

        {/* Progress bar */}
        {showProgress && (
          <div className="flex items-center gap-3">
            <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-slate-800/80">
              <div
                className={`h-full transition-all duration-500 ${
                  runtimeStatus === 'running' ? 'bg-emerald-500' :
                  runtimeStatus === 'paused' ? 'bg-amber-500' : 'bg-slate-600'
                }`}
                style={{ width: `${progressPct}%` }}
              />
            </div>
            <span className="shrink-0 text-[10px] tabular-nums text-slate-500">{Math.round(progressPct)}%</span>
          </div>
        )}

        {/* Performance strip */}
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-slate-800/70 bg-slate-950/40 px-4 py-3">
          <div className="flex flex-wrap items-center gap-4 text-xs text-slate-400">
            <div className="flex items-center gap-1.5">
              <span className="text-slate-500">Trades</span>
              <span className="font-semibold tabular-nums text-slate-200">{totalTrades}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-500">Win</span>
              <span className="font-semibold tabular-nums text-slate-200">{winRateDisplay}</span>
            </div>
            {runtimeStatus === 'completed' ? (
              <div className="flex items-center gap-1.5">
                <span className="text-slate-500">Run</span>
                <span className="font-semibold tabular-nums text-slate-200">{completedDuration || '—'}</span>
              </div>
            ) : ddDisplay && (
              <div className="flex items-center gap-1.5">
                <span className="text-slate-500">DD</span>
                <span className="font-semibold tabular-nums text-rose-400/80">{ddDisplay}</span>
              </div>
            )}
          </div>

          <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-widest ${
            runtimeStatus === 'running' ? 'border-emerald-500/40 text-emerald-400' :
            runtimeStatus === 'paused' ? 'border-amber-500/40 text-amber-400' :
            runtimeStatus === 'error' || runtimeStatus === 'crashed' ? 'border-rose-500/40 text-rose-400' :
            runtimeStatus === 'completed' ? 'border-sky-500/40 text-sky-400' :
            'border-slate-600/40 text-slate-500'
          }`}>
            {runtimeStatus}
          </span>
        </div>

        {/* Action Buttons */}
        <div className="flex flex-wrap items-center gap-2">
          {showDetails && (
            <ActionButton onClick={() => onOpen(bot)} icon={<Eye className="size-3" />} label="Details" size="sm" />
          )}
          {showViewReport && (
            <ActionButton
              onClick={() => {
                const path = reportRunId ? `/reports?runId=${reportRunId}` : '/reports'
                navigate(path)
              }}
              icon={<Eye className="size-3" />}
              label="View Report"
              size="sm"
            />
          )}
          {showViewError && (
            <ActionButton
              onClick={() => setErrorOpen(true)}
              icon={<TriangleAlert className="size-3" />}
              label="View Error"
              size="sm"
              variant="danger"
            />
          )}
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
      {showViewError && errorOpen ? (
        <div
          className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/70 p-4"
          onClick={() => setErrorOpen(false)}
        >
          <div
            className="w-full max-w-md rounded-2xl border border-rose-900/50 bg-slate-950 p-5 shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-center gap-3">
                <div className="flex size-10 items-center justify-center rounded-full border border-rose-800/60 bg-rose-950/40 text-rose-400">
                  <TriangleAlert className="size-5" />
                </div>
                <div>
                  <p className="text-xs font-semibold uppercase tracking-widest text-rose-400">Bot Crashed</p>
                  <p className="text-sm text-slate-300">Runtime error details</p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => setErrorOpen(false)}
                className="rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1 text-xs text-slate-400 hover:border-slate-700 hover:text-slate-200"
              >
                Close
              </button>
            </div>
            <div className="mt-4 space-y-3 text-xs text-slate-300">
              <div>
                <p className="text-[10px] uppercase tracking-wider text-slate-500">Message</p>
                <p className="mt-1 text-sm text-rose-200">{errorMessage}</p>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-slate-500">Strategy</p>
                  <p className="mt-1">{errorMeta.strategy_id || '—'}</p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-slate-500">Symbol</p>
                  <p className="mt-1">{errorMeta.symbol || '—'}</p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-slate-500">Timeframe</p>
                  <p className="mt-1">{errorMeta.timeframe || '—'}</p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-slate-500">Status</p>
                  <p className="mt-1">{runtimeStatus}</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}
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

function buildCompletedDuration(bot) {
  const startedAt = bot?.last_run_artifact?.started_at || bot?.runtime?.started_at
  const endedAt = bot?.last_run_artifact?.ended_at || bot?.runtime?.ended_at
  if (!startedAt || !endedAt) return null
  const startMs = Date.parse(startedAt)
  const endMs = Date.parse(endedAt)
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return null
  const elapsedSeconds = Math.floor((endMs - startMs) / 1000)
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
