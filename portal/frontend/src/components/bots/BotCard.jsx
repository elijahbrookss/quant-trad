import { AlertTriangle, Eye, Play, RotateCw, Square, Trash2 } from 'lucide-react'
import { memo, useMemo } from 'react'
import { buildBotCardViewModel } from './botControlSurfaceModel.js'

export const BotCard = memo(function BotCard({
  bot,
  strategyLookup,
  nowEpochMs,
  onStart,
  onStop,
  onDelete,
  onOpenLens,
  onOpenDiagnostics,
  pendingStart,
  pendingDelete,
}) {
  const view = useMemo(
    () => buildBotCardViewModel(bot, { strategyLookup, nowEpochMs, pendingStart: pendingStart === bot.id }),
    [bot, strategyLookup, nowEpochMs, pendingStart],
  )
  const display = view.display
  const primaryAction = display.allowedActions[0] || null
  const secondaryActions = display.allowedActions.slice(1)

  return (
    <article className="group relative overflow-hidden rounded-lg border border-white/[0.06] bg-black/35 transition-colors duration-150 hover:border-white/[0.1]">
      <div className="relative p-4">
        <div className="flex flex-col gap-4 xl:grid xl:grid-cols-[minmax(0,1.35fr)_auto] xl:items-start xl:gap-5">
          <div className="min-w-0 space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge tone={display.tone} label={view.statusLabel} statusKey={display.statusKey} />
              <h4 className="truncate text-[16px] font-semibold tracking-[0.01em] text-slate-50">{bot.name}</h4>
              <InlineTag>{view.metaItems.find((item) => item.key === 'execution')?.value || 'Execution'}</InlineTag>
            </div>

            <div className="grid gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1.05fr)] sm:gap-x-6">
              <IdentityBlock label="Strategy" value={view.strategyLabel} />
              <IdentityBlock label="Symbols" value={view.symbolsLabel} title={view.symbolsTitle} mono />
            </div>

            {view.statusDetail ? (
              <p className="max-w-3xl text-[13px] leading-relaxed text-slate-300">
                {view.statusDetail}
                {display.statusKey === 'starting' ? <span aria-hidden="true" className="qt-bot-ellipsis" /> : null}
              </p>
            ) : null}

            <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-white/[0.06] pt-3">
              {view.metaItems
                .filter((item) => item.key !== 'execution')
                .map((item) => (
                  <MetaItem key={item.key} label={item.label} value={item.value} mono={item.mono} title={item.title} />
                ))}
            </div>
          </div>

          <div className="flex min-w-[16rem] flex-col gap-3 xl:items-end">
            <div className="flex flex-wrap items-center gap-2 xl:justify-end">
              {primaryAction ? (
                <ActionButton
                  onClick={() => handleAction(primaryAction.key, { bot, onOpenLens, onOpenDiagnostics, onStart, onStop, onDelete })}
                  icon={actionIcon(primaryAction.key, display.statusKey)}
                  label={primaryAction.label}
                  busy={primaryAction.busy || (primaryAction.key === 'delete' && pendingDelete === bot.id)}
                  disabled={Boolean(primaryAction.disabled)}
                  variant={primaryAction.tone === 'danger' ? 'danger' : primaryAction.tone === 'primary' ? 'primary' : 'ghost'}
                />
              ) : null}
              {secondaryActions.map((action) => (
                <ActionButton
                  key={action.key}
                  onClick={() => handleAction(action.key, { bot, onOpenLens, onOpenDiagnostics, onStart, onStop, onDelete })}
                  icon={actionIcon(action.key, display.statusKey)}
                  label={action.label}
                  busy={action.busy || (action.key === 'delete' && pendingDelete === bot.id)}
                  disabled={Boolean(action.disabled)}
                  variant={action.tone === 'danger' ? 'danger' : action.tone === 'primary' ? 'primary' : 'ghost'}
                  subdued
                />
              ))}
            </div>

            <div className="rounded-md border border-white/[0.06] bg-black/30 px-3 py-2.5 xl:min-w-[16rem]">
              {view.contextItems.map((item, index) => (
                <ContextRow
                  key={item.key}
                  label={item.label}
                  value={item.value}
                  title={item.title}
                  mono={item.mono}
                  bordered={index < view.contextItems.length - 1}
                  intent={item.key === 'next' && (display.statusKey === 'failed_start' || display.statusKey === 'crashed') ? 'attention' : 'default'}
                />
              ))}
            </div>
          </div>
        </div>
      </div>
    </article>
  )
})

function actionIcon(actionKey, statusKey) {
  if (actionKey === 'open') return <Eye className="size-3.5" />
  if (actionKey === 'diagnostics') return <AlertTriangle className="size-3.5" />
  if (actionKey === 'stop') return <Square className="size-3.5" />
  if (actionKey === 'delete') return <Trash2 className="size-3.5" />
  if (actionKey === 'start') {
    return statusKey === 'completed' || statusKey === 'crashed' || statusKey === 'failed_start'
      ? <RotateCw className="size-3.5" />
      : <Play className="size-3.5" />
  }
  return <RotateCw className="size-3.5" />
}

function handleAction(actionKey, { bot, onOpenLens, onOpenDiagnostics, onStart, onStop, onDelete }) {
  if (actionKey === 'open') {
    onOpenLens?.(bot)
    return
  }
  if (actionKey === 'diagnostics') {
    onOpenDiagnostics?.(bot)
    return
  }
  if (actionKey === 'start') {
    onStart?.(bot.id)
    return
  }
  if (actionKey === 'stop') {
    onStop?.(bot.id)
    return
  }
  if (actionKey === 'delete') {
    onDelete?.(bot.id)
  }
}

function InlineTag({ children }) {
  return (
    <span className="qt-mono inline-flex items-center rounded-[4px] border border-white/[0.06] bg-black/30 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
      {children}
    </span>
  )
}

function IdentityBlock({ label, value, title, mono = false }) {
  return (
    <div className="min-w-0" title={title}>
      <p className="qt-mono text-[10px] font-semibold uppercase tracking-[0.22em] text-slate-500">{label}</p>
      <p className={`mt-1 truncate text-[14px] ${mono ? 'qt-mono text-slate-200' : 'text-slate-200'}`}>{value}</p>
    </div>
  )
}

function MetaItem({ label, value, mono = false, title }) {
  return (
    <div className="min-w-[5.25rem]" title={title}>
      <p className="qt-mono text-[10px] font-semibold uppercase tracking-[0.24em] text-slate-500">{label}</p>
      <p className={`mt-1 text-[13px] ${mono ? 'qt-mono text-slate-300' : 'text-slate-300'}`}>{value}</p>
    </div>
  )
}

function ContextRow({ label, value, title, mono = false, bordered = false, intent = 'default' }) {
  const valueClass = intent === 'attention' ? 'text-rose-200' : mono ? 'qt-mono text-slate-200' : 'text-slate-200'
  return (
    <div className={bordered ? 'border-b border-white/[0.06] pb-2.5' : ''} title={title}>
      <div className="flex items-center justify-between gap-3 pt-0.5">
        <span className="qt-mono text-[10px] font-semibold uppercase tracking-[0.22em] text-slate-500">{label}</span>
        <span className={`text-right text-[12px] ${valueClass}`}>{value}</span>
      </div>
    </div>
  )
}

function StatusBadge({ tone, label, statusKey }) {
  const toneClass = {
    emerald: 'border-emerald-500/50 bg-emerald-500/10 text-emerald-200 shadow-[0_0_0_1px_rgba(16,185,129,0.04)]',
    amber: 'border-amber-500/50 bg-amber-500/10 text-amber-200 shadow-[0_0_0_1px_rgba(245,158,11,0.04)]',
    rose: 'border-rose-500/55 bg-rose-500/12 text-rose-200 shadow-[0_0_0_1px_rgba(244,63,94,0.06)]',
    sky: 'border-sky-500/50 bg-sky-500/10 text-sky-200 shadow-[0_0_0_1px_rgba(56,189,248,0.05)]',
    slate: 'border-slate-700/80 bg-slate-950/90 text-slate-200',
  }[tone] || 'border-slate-700/80 bg-slate-950/90 text-slate-200'

  const isStarting = statusKey === 'starting'
  const isRunning = statusKey === 'running'
  const badgeMotionClass = isStarting ? 'qt-bot-status-badge-starting' : ''
  const dotMotionClass = isStarting
    ? 'qt-bot-status-dot-starting'
    : isRunning
      ? 'qt-bot-status-dot-running'
      : ''

  return (
    <span
      className={`qt-mono inline-flex items-center gap-1.5 rounded-[3px] border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.24em] ${toneClass} ${badgeMotionClass}`}
    >
      {(isStarting || isRunning) ? <span aria-hidden="true" className={`qt-bot-status-dot ${dotMotionClass}`} /> : null}
      {label}
    </span>
  )
}

function ActionButton({ onClick, icon, label, busy, disabled = false, variant = 'ghost', subdued = false }) {
  const variantClass = {
    primary:
      'border-slate-600 bg-slate-800/80 text-slate-100 hover:border-slate-500 hover:bg-slate-700',
    danger:
      'border-rose-800/75 bg-rose-950/40 text-rose-200 hover:border-rose-700/80 hover:bg-rose-950/55',
    ghost:
      'border-white/[0.06] bg-black/30 text-slate-300 hover:border-white/[0.1] hover:bg-black/45 hover:text-slate-200',
  }[variant]

  return (
    <button
      type="button"
      onClick={onClick}
      className={`qt-mono inline-flex items-center gap-1.5 rounded-[3px] border px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.18em] transition-colors ${variantClass} ${subdued ? 'opacity-90' : ''} disabled:cursor-not-allowed disabled:opacity-50`}
      disabled={busy || disabled}
      aria-label={label}
    >
      {icon}
      <span>{label}</span>
    </button>
  )
}

export function sortBots(bots) {
  return [...bots].sort((a, b) => {
    const aTime = Date.parse(a?.created_at || '') || 0
    const bTime = Date.parse(b?.created_at || '') || 0
    if (aTime !== bTime) return bTime - aTime
    return (a.name || '').localeCompare(b.name || '')
  })
}
