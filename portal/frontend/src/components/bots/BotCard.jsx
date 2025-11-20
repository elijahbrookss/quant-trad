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
  playbackLabelFor,
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
  const progressPct = `${Math.round(progressValue * 1000) / 10}%`
  const progressWidth = `${Math.min(100, Math.max(0, progressValue * 100))}%`
  const showPause = runtimeStatus === 'running' && bot.mode === 'walk-forward'
  const showResume = runtimeStatus === 'paused'
  const timeframeLabel = describeBotMeta(bot, 'timeframe')
  const datasourceLabel = describeBotMeta(bot, 'datasource')
  const exchangeLabel = describeBotMeta(bot, 'exchange')
  const canStart = ['idle', 'stopped', 'completed', 'error'].includes(runtimeStatus)
  const canStop = ['running', 'paused', 'starting'].includes(runtimeStatus)
  const startLabel = runtimeStatus === 'completed' ? 'Rerun' : runtimeStatus === 'stopped' ? 'Restart' : 'Start'
  const keyStats = ['total_trades', 'wins', 'losses', 'win_rate']
  const statsEntries = keyStats
    .map((key) => ({ key, value: bot.last_stats?.[key] ?? bot.runtime?.stats?.[key] }))
    .filter((entry) => entry.value !== undefined && entry.value !== null)
  const runTypeLabel = (bot.run_type || 'backtest').replace('_', ' ')
  const runTypePill = runTypeLabel.toUpperCase()

  return (
    <article className="flex flex-col gap-4 rounded-3xl border border-white/10 bg-gradient-to-br from-slate-950/90 via-slate-950/40 to-slate-900/30 p-5 shadow-lg shadow-black/30">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.32em] text-slate-300">
            <span className="inline-flex items-center gap-2 rounded-full border border-white/30 bg-white/10 px-3 py-1 text-[10px] font-semibold text-white">
              {runTypePill}
            </span>
            {timeframeLabel ? (
              <span className="rounded-full border border-white/10 bg-white/5 px-2 py-1 text-[10px] text-slate-200">TF {timeframeLabel}</span>
            ) : null}
            <span className="rounded-full border border-white/10 bg-white/5 px-2 py-1 text-[10px] text-slate-200">{playbackLabelFor(bot)}</span>
          </div>
          <p className="text-sm uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
            {assignedNames.length ? assignedNames.join(', ') : 'No strategies assigned'}
          </p>
          <h4 className="text-xl font-semibold text-white">{bot.name}</h4>
          <p className="text-xs text-slate-400">{describeRange(bot)}</p>
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-300">
            {timeframeLabel ? <MetaPill label="Timeframe" value={timeframeLabel} /> : null}
            {datasourceLabel ? <MetaPill label="Datasource" value={datasourceLabel} /> : null}
            {exchangeLabel ? <MetaPill label="Exchange" value={exchangeLabel} /> : null}
          </div>
        </div>

        <div className="flex items-center gap-2 self-start text-xs text-slate-300">
          {statusBadge(runtimeStatus)}
          <span className="text-[11px] uppercase tracking-[0.24em] text-slate-400">{progressPct}</span>
        </div>
      </div>

      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="flex flex-1 items-center gap-3">
          <div className="h-2 flex-1 overflow-hidden rounded-full bg-white/5">
            <div className="h-full bg-[color:var(--accent-alpha-40)] transition-[width] duration-500" style={{ width: progressWidth }} />
          </div>
          <span className="w-14 text-right text-xs text-slate-300">{progressPct}</span>
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
            />
          ) : null}
          <ActionButton
            onClick={() => onDelete(bot.id)}
            icon={<Trash2 className="size-4" />}
            label={pendingDelete === bot.id ? 'Deleting…' : 'Delete'}
            busy={pendingDelete === bot.id}
          />
        </div>
      </div>

      {statsEntries.length ? (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          {statsEntries.map((entry) => (
            <div key={entry.key} className="rounded-xl border border-white/10 bg-white/5 px-3 py-2">
              <p className="text-[10px] uppercase tracking-[0.3em] text-slate-500">{entry.key.replace('_', ' ')}</p>
              <p className="text-sm text-white">{entry.value}</p>
            </div>
          ))}
        </div>
      ) : null}
    </article>
  )
})

function describeBotMeta(bot, key) {
  const raw = bot?.[key] || bot?.config?.[key] || bot?.runtime?.[key]
  if (!raw) return null
  if (key === 'timeframe') return String(raw).toUpperCase()
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

function ActionButton({ onClick, icon, label, busy }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="inline-flex items-center gap-2 rounded-full border border-white/20 bg-white/5 px-3 py-1 text-xs font-semibold text-white transition hover:border-white/40"
      disabled={busy}
    >
      {icon}
      <span>{label}</span>
    </button>
  )
}

export function sortBots(bots) {
  return [...bots].sort((a, b) => {
    const sa = STATUS_ORDER[computeStatus(a)] ?? 10
    const sb = STATUS_ORDER[computeStatus(b)] ?? 10
    if (sa !== sb) return sa - sb
    return (a.name || '').localeCompare(b.name || '')
  })
}

