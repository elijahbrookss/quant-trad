import { ExternalLink } from 'lucide-react'
import { SemanticStatusBadge } from './StatusBadge.jsx'

const severityClass = {
  info: 'border-sky-500/20 bg-sky-500/[0.06]',
  warning: 'border-amber-500/22 bg-amber-500/[0.07]',
  critical: 'border-rose-500/25 bg-rose-500/[0.08]',
  unknown: 'border-white/10 bg-white/[0.04]',
}

export function ErrorCard({ error, compact = false, actions = [], showCode = true }) {
  const model = error && typeof error === 'object'
    ? error
    : {
        code: 'UNTYPED_BACKEND_ERROR',
        title: 'Unexpected error',
        message: 'The backend returned an untyped error for this run.',
        severity: 'unknown',
        category: 'unknown',
        raw: error,
      }

  const severity = model.severity || 'unknown'
  const category = model.category || 'unknown'
  const className = severityClass[severity] || severityClass.unknown

  return (
    <div className={`rounded-[6px] border p-3 ${className}`}>
      <div className="flex flex-wrap items-center gap-2">
        <SemanticStatusBadge kind="severity" value={severity} />
        <span className="rounded-[4px] border border-white/10 bg-black/20 px-2 py-0.5 text-[10px] text-slate-400">
          {category}
        </span>
        {showCode && model.code ? <span className="qt-mono text-[10px] text-slate-500">{model.code}</span> : null}
      </div>
      <div className={compact ? 'mt-2' : 'mt-3'}>
        <p className="text-sm font-semibold text-slate-100">{model.title || 'Unexpected error'}</p>
        {!compact ? (
          <p className="mt-1 text-xs leading-5 text-slate-400">
            {model.message || 'The backend returned an untyped error for this run.'}
          </p>
        ) : null}
      </div>
      {actions.length ? (
        <div className="mt-3 flex flex-wrap gap-2">
          {actions.map((action) => (
            <button
              key={action.key || action.label}
              type="button"
              onClick={action.onClick}
              className="inline-flex items-center gap-1.5 rounded-[4px] border border-white/10 bg-black/20 px-2.5 py-1 text-[10px] font-medium text-slate-300 transition hover:border-white/16 hover:bg-black/30 hover:text-slate-100"
            >
              {action.icon || <ExternalLink className="size-3" />}
              {action.label}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  )
}
