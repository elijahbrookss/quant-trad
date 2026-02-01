import React from 'react'
import { buildFilterSummary } from './filterUtils.js'

export const GateCard = ({
  filter,
  onToggle,
  onEdit,
  onDelete,
  actionsLabel = 'Actions',
}) => {
  const summary = buildFilterSummary(filter)
  const name = filter.name || summary

  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-white/10 bg-black/30 px-4 py-3">
      <button
        type="button"
        onClick={() => onToggle?.(filter)}
        className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] ${
          filter.enabled
            ? 'bg-emerald-500/20 text-emerald-100 border border-emerald-500/30'
            : 'bg-slate-700/60 text-slate-400 border border-white/5'
        }`}
      >
        {filter.enabled ? 'Enabled' : 'Disabled'}
      </button>
      <div className="min-w-[200px] flex-1">
        <p className="text-sm font-semibold text-white">{name}</p>
        <p className="mt-1 text-[11px] text-slate-400">{summary}</p>
      </div>
      <div className="flex items-center gap-2" aria-label={actionsLabel}>
        <button
          type="button"
          className="rounded border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-200 hover:border-white/20"
          onClick={() => onEdit?.(filter)}
        >
          Edit
        </button>
        <button
          type="button"
          className="rounded border border-rose-500/40 bg-rose-500/10 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-rose-200 hover:border-rose-400/70"
          onClick={() => onDelete?.(filter)}
        >
          Delete
        </button>
      </div>
    </div>
  )
}
