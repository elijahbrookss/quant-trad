import React from 'react'

export const RuleCard = ({
  rule,
  summary,
  conditionCount,
  filterCount,
  expanded,
  onToggleExpand,
  onEdit,
  onDelete,
  onDuplicate,
  children,
}) => {
  return (
    <div className="rounded-xl border border-white/10 bg-white/5 p-3">
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <span
            className={`inline-flex items-center rounded-full px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] ${
              rule.action === 'buy'
                ? 'bg-emerald-500/20 text-emerald-100 border border-emerald-500/30'
                : 'bg-rose-500/20 text-rose-100 border border-rose-500/30'
            }`}
          >
            {rule.action?.toUpperCase() || 'ACTION'}
          </span>
          <span
            className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
              rule.enabled
                ? 'bg-emerald-600/30 text-emerald-100'
                : 'bg-slate-700/60 text-slate-400'
            }`}
          >
            {rule.enabled ? 'Enabled' : 'Disabled'}
          </span>
        </div>

        <div className="min-w-0 flex-1">
          <button
            type="button"
            onClick={onToggleExpand}
            className="w-full text-left focus:outline-none"
          >
            <div className="flex items-center justify-between gap-2">
              <span className="truncate text-sm font-semibold text-white">{rule.name}</span>
              <span className="text-xs text-slate-500">{expanded ? '▾' : '▸'}</span>
            </div>
            <p className="mt-1 truncate text-[11px] text-slate-400">{summary}</p>
            <div className="mt-2 flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-[0.18em] text-slate-500">
              <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5">
                {conditionCount} condition{conditionCount === 1 ? '' : 's'}
              </span>
              <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5">
                {filterCount} gate{filterCount === 1 ? '' : 's'}
              </span>
            </div>
          </button>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            className="rounded border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-200 hover:border-white/20"
            onClick={onEdit}
          >
            Edit
          </button>
          <button
            type="button"
            className="rounded border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-200 hover:border-white/20"
            onClick={onDuplicate}
          >
            Duplicate
          </button>
          <button
            type="button"
            className="rounded border border-rose-500/40 bg-rose-500/10 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-rose-200 hover:border-rose-400/70"
            onClick={onDelete}
          >
            Delete
          </button>
        </div>
      </div>

      {expanded && children ? (
        <div className="mt-4 space-y-4 border-t border-white/10 pt-4">
          {children}
        </div>
      ) : null}
    </div>
  )
}
