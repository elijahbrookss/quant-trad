import { AlertTriangle, Check, Copy, Eye, FileText, LoaderCircle, Play, RotateCw, Square, Trash2 } from 'lucide-react'
import { memo, useEffect, useMemo, useState } from 'react'
import { buildBotCardViewModel } from '../buildBotFleetViewModel.js'
import { ErrorCard } from '../../../../components/ui/ErrorCard.jsx'
import { SemanticStatusBadge } from '../../../../components/ui/StatusBadge.jsx'

export const BotFleetCard = memo(function BotFleetCard({
  bot,
  strategyLookup,
  nowEpochMs,
  onStart,
  onStop,
  onDelete,
  onOpenLens,
  onOpenDiagnostics,
  onViewReport,
  pendingStart,
  pendingStop,
  pendingDelete,
}) {
  const view = useMemo(
    () => buildBotCardViewModel(bot, { strategyLookup, nowEpochMs, pendingStart: pendingStart === bot.id }),
    [bot, strategyLookup, nowEpochMs, pendingStart],
  )
  const display = view.display
  const mainActions = display.allowedActions.filter((action) => action.variant !== 'danger')
  const dangerActions = display.allowedActions.filter((action) => action.variant === 'danger')
  const metricStats = view.metricStats || []
  const stateFacts = view.stateFacts || []
  const runView = view.runView || {}

  return (
    <article className="qt-ops-panel group relative overflow-hidden transition-[border-color,background-color] duration-150 hover:border-white/14">
      <div className="qt-ops-grid pointer-events-none absolute inset-0 opacity-40" aria-hidden="true" />
      <div className="relative px-3.5 py-3">
        <div className="space-y-3">
          <header className="pb-0.5">
            <div className="min-w-0">
              <div className="flex min-w-0 items-center gap-2">
                <StatusBadge tone={display.tone} label={view.statusLabel} statusKey={display.statusKey} />
                {runView.healthState && runView.healthState !== 'unknown' ? (
                  <SemanticStatusBadge kind="health" value={runView.healthState} />
                ) : null}
                <h4 className="min-w-0 flex-1 truncate text-[14px] font-semibold tracking-[0.01em] text-slate-50">
                  {bot.name}
                </h4>
              </div>
              <p title={view.headerMetaText} className="mt-1.5 truncate text-[11px] leading-4 text-slate-400">
                {view.headerMetaText}
              </p>
            </div>

            {view.statusDetail ? (
              <p title={view.statusDetail} className="mt-2 max-w-4xl truncate text-[11px] leading-4 text-slate-500">
                {view.statusDetail}
                {display.statusKey === 'starting' ? '…' : ''}
              </p>
            ) : null}
          </header>

          <section className="border-y border-white/6 py-2">
            <div className="grid gap-x-3 gap-y-2 sm:grid-cols-2 xl:grid-cols-4">
              {view.metadataItems.map((item) => (
                <MetadataField key={item.key} {...item} />
              ))}
            </div>
            {runView.reportStatus && runView.reportStatus !== 'unknown' ? (
              <div className="mt-2 flex flex-wrap gap-1.5">
                <SemanticStatusBadge kind="report" value={runView.reportStatus} />
              </div>
            ) : null}
          </section>

          <div className="grid gap-3 xl:grid-cols-[minmax(0,1.75fr)_minmax(13rem,0.7fr)]">
            <div className="min-w-0 space-y-2">
              <section className="space-y-1.5">
                <dl className="grid gap-x-4 gap-y-1.5 sm:grid-cols-2">
                  {metricStats.map((item) => (
                    <MetricRow key={item.key} {...item} />
                  ))}
                </dl>

                {stateFacts.length ? (
                  <dl className="grid gap-1 pt-0.5">
                    {stateFacts.map((item) => (
                      <MetricRow key={item.key} {...item} />
                    ))}
                  </dl>
                ) : null}
              </section>

              <section className="border-t border-white/6 pt-1.5">
                <SymbolsRow symbols={view.symbols} />
              </section>

              {runView.primaryError ? (
                <section className="border-t border-white/6 pt-2">
                  <ErrorCard error={runView.primaryError} compact showCode={false} />
                </section>
              ) : null}
            </div>

            <aside className="min-w-0 border-t border-white/8 pt-2.5 xl:border-t-0 xl:border-l xl:border-white/8 xl:pl-3 xl:pt-0">
              <div className="space-y-1.5">
                <p className="qt-ops-kicker text-slate-500">Operational</p>
                <div className="divide-y divide-white/6 border-y border-white/6">
                  {view.operationalRows.map((row) => (
                    <OperationalRow key={row.key} {...row} />
                  ))}
                </div>
                <div className="space-y-0.5">
                  <p className="qt-ops-kicker">Operator Hint</p>
                  <p title={view.actionHint} className="text-[11px] leading-4 text-slate-400">
                    {view.actionHint}
                  </p>
                </div>
              </div>
            </aside>
          </div>

          <footer className="flex flex-col gap-1.5 border-t border-white/8 pt-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex flex-wrap items-center gap-1.5">
              {mainActions.map((action) => (
                <ActionButton
                  key={action.key}
                  onClick={() => handleAction(action.key, { bot, onOpenLens, onOpenDiagnostics, onViewReport, onStart, onStop, onDelete })}
                  title={action.title}
                  icon={actionIcon(action.key, display.statusKey)}
                  label={action.label}
                  busy={actionBusy(action, bot.id, { pendingStart, pendingStop, pendingDelete })}
                  disabled={Boolean(action.disabled)}
                  variant={action.variant}
                />
              ))}
            </div>
            {dangerActions.length ? (
              <div className="flex flex-wrap items-center gap-1.5 sm:justify-end">
                {dangerActions.map((action) => (
                  <ActionButton
                    key={action.key}
                    onClick={() => handleAction(action.key, { bot, onOpenLens, onOpenDiagnostics, onViewReport, onStart, onStop, onDelete })}
                    title={action.title}
                    icon={actionIcon(action.key, display.statusKey)}
                    label={action.label}
                    busy={actionBusy(action, bot.id, { pendingStart, pendingStop, pendingDelete })}
                    disabled={Boolean(action.disabled)}
                    variant={action.variant}
                  />
                ))}
              </div>
            ) : null}
          </footer>
        </div>
      </div>
    </article>
  )
})

async function copyText(value) {
  const normalized = String(value || '').trim()
  if (!normalized) return false
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(normalized)
      return true
    } catch {
      return false
    }
  }
  if (typeof document !== 'undefined') {
    const textarea = document.createElement('textarea')
    textarea.value = normalized
    textarea.setAttribute('readonly', 'true')
    textarea.style.position = 'absolute'
    textarea.style.left = '-9999px'
    document.body.appendChild(textarea)
    textarea.select()
    const copied = document.execCommand('copy')
    document.body.removeChild(textarea)
    return copied
  }
  return false
}

function actionBusy(action, botId, { pendingStart, pendingStop, pendingDelete }) {
  return (
    action.busy ||
    (action.key === 'start' && pendingStart === botId) ||
    (action.key === 'stop' && pendingStop === botId) ||
    (action.key === 'delete' && pendingDelete === botId)
  )
}

function actionIcon(actionKey, statusKey) {
  if (actionKey === 'open') return <Eye className="size-3.5" />
  if (actionKey === 'report') return <FileText className="size-3.5" />
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

function handleAction(actionKey, { bot, onOpenLens, onOpenDiagnostics, onViewReport, onStart, onStop, onDelete }) {
  if (actionKey === 'open') {
    onOpenLens?.(bot)
    return
  }
  if (actionKey === 'report') {
    onViewReport?.(bot)
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

function MetadataField({ label, value, rawValue, title, mono = false, copyable = false, missing = false }) {
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!copied) return undefined
    const timeoutId = setTimeout(() => setCopied(false), 1400)
    return () => clearTimeout(timeoutId)
  }, [copied])

  const valueClass = missing
    ? 'text-rose-200'
    : mono
      ? 'qt-mono text-[12px] text-slate-300'
      : 'text-[12px] text-slate-300'

  async function handleCopy() {
    if (!copyable || !rawValue) return
    const didCopy = await copyText(rawValue)
    if (didCopy) setCopied(true)
  }

  return (
    <div className="min-w-0">
      <p className="qt-ops-kicker">{label}</p>
      <div className="mt-0.5 flex items-center gap-1">
        <span title={title || rawValue || value} className={`min-w-0 truncate ${valueClass}`}>
          {value}
        </span>
        {copyable ? (
          <button
            type="button"
            onClick={handleCopy}
            title={`Copy full ${label}`}
            aria-label={`Copy full ${label}`}
            className="inline-flex shrink-0 items-center justify-center rounded-[3px] border border-white/8 bg-white/[0.03] p-[3px] text-slate-500 transition hover:border-white/14 hover:bg-white/[0.06] hover:text-slate-100"
          >
            {copied ? <Check className="size-3 text-emerald-300" /> : <Copy className="size-3" />}
          </button>
        ) : null}
      </div>
    </div>
  )
}

function MetricRow({ label, value, title, mono = false, tone = 'default' }) {
  const toneClass = tone === 'attention'
    ? 'text-amber-200'
    : tone === 'positive'
      ? 'text-emerald-200'
      : tone === 'danger'
        ? 'text-rose-200'
        : 'text-slate-100'

  return (
    <div title={title} className="min-w-0">
      <div className="flex min-w-0 items-baseline gap-1.5">
        <dt className="shrink-0 text-[9px] font-medium uppercase tracking-[0.16em] text-slate-600">{label}</dt>
        <dd className={`min-w-0 truncate text-[13px] font-semibold ${mono ? 'qt-mono tabular-nums' : ''} ${toneClass}`}>
          {value}
        </dd>
      </div>
    </div>
  )
}

function SymbolsRow({ symbols }) {
  return (
    <div className="space-y-0.5">
      <div className="flex min-w-0 items-baseline gap-1.5">
        <p className="shrink-0 text-[9px] font-medium uppercase tracking-[0.16em] text-slate-600">Symbols</p>
        <p className="min-w-0 truncate text-[13px] font-semibold text-slate-100">{symbols.summaryLabel || symbols.trackedLabel}</p>
      </div>
      <p title={symbols.title} className="truncate text-[11px] leading-4 text-slate-500">
        {symbols.preview}
      </p>
    </div>
  )
}

function OperationalRow({ label, value, mono = false }) {
  return (
    <div className="flex items-center justify-between gap-3 py-[0.3125rem]">
      <span className="text-[9px] font-medium uppercase tracking-[0.16em] text-slate-600">{label}</span>
      <span className={`min-w-0 truncate text-right text-[11px] ${mono ? 'qt-mono tabular-nums text-slate-300' : 'text-slate-400'}`}>{value}</span>
    </div>
  )
}

function StatusBadge({ tone, label, statusKey }) {
  const toneClass = {
    emerald: 'border-emerald-500/45 bg-emerald-500/10 text-emerald-200',
    amber: 'border-amber-500/45 bg-amber-500/10 text-amber-200',
    rose: 'border-rose-500/50 bg-rose-500/12 text-rose-200',
    sky: 'border-sky-500/45 bg-sky-500/10 text-sky-200',
    slate: 'border-slate-700 bg-slate-950/90 text-slate-200',
  }[tone] || 'border-slate-700 bg-slate-950/90 text-slate-200'

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
      className={`qt-mono inline-flex items-center gap-1.5 rounded-[3px] border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] ${toneClass} ${badgeMotionClass}`}
    >
      {(isStarting || isRunning) ? <span aria-hidden="true" className={`qt-bot-status-dot ${dotMotionClass}`} /> : null}
      {label}
    </span>
  )
}

function ActionButton({ onClick, icon, label, busy, disabled = false, variant = 'tertiary', title }) {
  const variantClass = {
    primary:
      'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] text-[color:var(--accent-text-strong)] hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)]',
    diagnostic:
      'border-amber-500/22 bg-amber-500/[0.08] text-amber-100 hover:border-amber-400/30 hover:bg-amber-500/[0.12]',
    secondary:
      'border-white/[0.10] bg-white/[0.04] text-slate-200 hover:border-white/[0.16] hover:bg-white/[0.07] hover:text-slate-50',
    danger:
      'border-rose-800/70 bg-rose-950/35 text-rose-200 hover:border-rose-700/80 hover:bg-rose-950/50',
    tertiary:
      'border-white/[0.08] bg-black/20 text-slate-300 hover:border-white/[0.14] hover:bg-black/35 hover:text-slate-100',
  }[variant]

  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      className={`qt-mono inline-flex items-center gap-1.5 rounded-[3px] border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] transition-colors ${variantClass} disabled:cursor-not-allowed disabled:opacity-50`}
      disabled={busy || disabled}
      aria-label={label}
      aria-busy={busy ? 'true' : 'false'}
    >
      {busy ? <LoaderCircle className="size-3.5 animate-spin" /> : icon}
      <span>{label}</span>
    </button>
  )
}
