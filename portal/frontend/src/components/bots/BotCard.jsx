import { Play, Square, Eye, Trash2, Pause, RotateCw } from 'lucide-react'
import { memo, useMemo } from 'react'

const STATUS_ORDER = {
  running: 0,
  starting: 1,
  paused: 2,
  completed: 3,
  idle: 4,
  stopped: 5,
  error: 6,
}

const computeStatus = (bot) => (bot?.runtime?.status || bot?.status || 'idle').toLowerCase()

export const BotCard = memo(function BotCard({
  bot,
  strategyLookup,
  describeRange,
  statusBadge,
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
  const progressValue =
    typeof bot.runtime?.progress === 'number'
      ? bot.runtime.progress
      : runtimeStatus === 'completed'
        ? 1
        : 0
  const progressWidth = `${Math.min(100, Math.max(0, progressValue * 100))}%`
  const showPause = runtimeStatus === 'running' && bot.mode === 'walk-forward'
  const showResume = runtimeStatus === 'paused'
  const timeframeLabel = describeBotMeta(bot, strategyLookup, 'timeframe')
  const symbolLabel = describeBotMeta(bot, strategyLookup, 'symbol')
  const canStart = ['idle', 'stopped', 'completed', 'error'].includes(runtimeStatus)
  const canStop = ['running', 'paused', 'starting'].includes(runtimeStatus)
  const startLabel = runtimeStatus === 'completed' ? 'Rerun' : runtimeStatus === 'stopped' ? 'Restart' : 'Start'
  const statsEntries = buildStats(bot)
  const runTypeLabel = (bot.run_type || 'backtest').replace('_', ' ')
  const runTypePill = runTypeLabel.toUpperCase()

  return (
    <article className="flex flex-col gap-4 rounded-3xl border border-white/10 bg-gradient-to-br from-slate-950/90 via-slate-950/40 to-slate-900/30 p-5 shadow-lg shadow-black/30">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.32em] text-slate-300">
            <span className="inline-flex items-center gap-2 rounded-full border border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] px-3 py-1 text-[10px] font-semibold text-white">
              {runTypePill}
            </span>
          </div>
          <p className="text-sm uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
            {assignedNames.length ? assignedNames.join(', ') : 'No strategies assigned'}
          </p>
          <h4 className="text-xl font-semibold text-white">{bot.name}</h4>
          <p className="text-xs text-slate-400">{describeRange(bot)}</p>
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-300">
            {timeframeLabel ? <MetaPill label="Timeframe" value={timeframeLabel} /> : null}
            {symbolLabel ? <MetaPill label="Symbol" value={symbolLabel} /> : null}
          </div>
        </div>

        <div className="flex items-center gap-2 self-start text-xs text-slate-300">{statusBadge(runtimeStatus)}</div>
      </div>

      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="flex flex-1 items-center gap-3">
          <div className="h-2 flex-1 overflow-hidden rounded-full bg-white/5">
            <div className="h-full bg-[color:var(--accent-alpha-40)] transition-[width] duration-500" style={{ width: progressWidth }} />
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <ActionButton onClick={() => onOpen(bot)} icon={<Eye className="size-4" />} label="Open" />
          {showPause ? (
            <ActionButton onClick={() => onPause(bot.id)} icon={<Pause className="size-4" />} label="Pause" />
          ) : null}
          {showResume ? (
            <ActionButton onClick={() => onResume(bot.id)} icon={<Play className="size-4" />} label="Resume" />
          ) : null}
          {canStop ? (
            <ActionButton onClick={() => onStop(bot.id)} icon={<Square className="size-4" />} label="Stop" />
          ) : null}
          {canStart ? (
            <ActionButton
              onClick={() => onStart(bot.id)}
              icon={runtimeStatus === 'completed' ? <RotateCw className="size-4" /> : <Play className="size-4" />}
              label={pendingStart === bot.id ? 'Starting…' : startLabel}
              busy={pendingStart === bot.id}
              variant="accent"
            />
          ) : null}
          <ActionButton
            onClick={() => onDelete(bot.id)}
            icon={<Trash2 className="size-4" />}
            label={pendingDelete === bot.id ? 'Deleting…' : 'Delete'}
            busy={pendingDelete === bot.id}
            variant="danger"
          />
        </div>
      </div>

      {statsEntries.length ? (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-5">
          {statsEntries.map((entry) => (
            <div
              key={entry.key}
              className={`rounded-xl border border-white/10 px-3 py-2 ${
                entry.key === 'net_pnl' ? 'bg-white/5' : 'bg-white/[0.04]'
              }`}
            >
              <p className="text-[10px] uppercase tracking-[0.3em] text-slate-500">{entry.label}</p>
              <p
                className={`text-sm font-semibold ${
                  entry.key === 'net_pnl'
                    ? entry.tone === 'positive'
                      ? 'text-emerald-300'
                      : entry.tone === 'negative'
                        ? 'text-rose-300'
                        : 'text-slate-200'
                    : 'text-white'
                }`}
              >
                {entry.value}
              </p>
            </div>
          ))}
        </div>
      ) : null}
    </article>
  )
})

function describeBotMeta(bot, strategyLookup, key) {
  if (!bot) return null

  const fromStrategies = new Set()
  for (const strategyId of bot.strategy_ids || []) {
    const strategy = strategyLookup.get(strategyId)
    if (!strategy) continue
    const value = strategy[key]
    if (!value && Array.isArray(strategy?.symbols) && key === 'symbol') {
      strategy.symbols.forEach((sym) => fromStrategies.add(sym))
    }
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

  const raw = bot?.[key] || bot?.config?.[key] || bot?.runtime?.[key]
  if (!raw) return null
  if (key === 'timeframe') return String(raw).toUpperCase()
  if (Array.isArray(raw)) return raw.join(', ')
  return raw
}

function MetaPill({ label, value }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300">
      <span className="text-slate-500">{label}</span>
      <span className="text-white">{value}</span>
    </span>
  )
}

function ActionButton({ onClick, icon, label, busy, variant = 'ghost' }) {
  const variantClass = {
    accent:
      'border-[color:var(--accent-alpha-50)] bg-transparent text-white hover:border-[color:var(--accent-alpha-70)] hover:bg-[color:var(--accent-alpha-10)]',
    danger:
      'border-rose-400/50 bg-transparent text-rose-100 hover:border-rose-300 hover:bg-rose-500/10',
    ghost: 'border-white/15 bg-transparent text-white hover:border-white/35 hover:bg-white/5',
  }[variant]

  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-semibold transition ${variantClass} disabled:cursor-not-allowed disabled:opacity-60`}
      disabled={busy}
    >
      {icon}
      <span>{label}</span>
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

export function sortBots(bots) {
  return [...bots].sort((a, b) => {
    const sa = STATUS_ORDER[computeStatus(a)] ?? 10
    const sb = STATUS_ORDER[computeStatus(b)] ?? 10
    if (sa !== sb) return sa - sb
    return (a.name || '').localeCompare(b.name || '')
  })
}

