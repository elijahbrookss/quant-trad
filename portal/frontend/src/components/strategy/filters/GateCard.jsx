import React from 'react'
import { Pencil, Trash2 } from 'lucide-react'
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
  const showSummary = filter.name && summary !== filter.name

  return (
    <div
      className={`
        group relative flex items-center gap-3 rounded-lg border px-4 py-3 transition-all
        ${filter.enabled
          ? 'border-white/10 bg-white/[0.02] hover:border-white/15 hover:bg-white/[0.04]'
          : 'border-white/5 bg-white/[0.01] opacity-60 hover:opacity-80'
        }
      `}
    >
      {/* Enable/Disable toggle */}
      <button
        type="button"
        onClick={() => onToggle?.(filter)}
        className={`
          relative h-5 w-9 shrink-0 rounded-full transition-colors
          ${filter.enabled ? 'bg-emerald-500/30' : 'bg-slate-700/50'}
        `}
        aria-label={filter.enabled ? 'Disable gate' : 'Enable gate'}
        title={filter.enabled ? 'Click to disable' : 'Click to enable'}
      >
        <span
          className={`
            absolute top-0.5 h-4 w-4 rounded-full transition-all
            ${filter.enabled
              ? 'left-[18px] bg-emerald-400'
              : 'left-0.5 bg-slate-500'
            }
          `}
        />
      </button>

      {/* Content */}
      <div className="min-w-0 flex-1">
        <p className={`text-sm font-medium ${filter.enabled ? 'text-white' : 'text-slate-400'}`}>
          {name}
        </p>
        {showSummary && (
          <p className="mt-0.5 truncate text-xs text-slate-500">{summary}</p>
        )}
      </div>

      {/* Actions - visible on hover */}
      <div
        className="flex shrink-0 items-center gap-1 opacity-0 transition-opacity group-hover:opacity-100"
        aria-label={actionsLabel}
      >
        <button
          type="button"
          className="flex h-7 w-7 items-center justify-center rounded text-slate-400 transition hover:bg-white/5 hover:text-white"
          onClick={() => onEdit?.(filter)}
          title="Edit gate"
        >
          <Pencil className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          className="flex h-7 w-7 items-center justify-center rounded text-slate-400 transition hover:bg-rose-500/10 hover:text-rose-400"
          onClick={() => onDelete?.(filter)}
          title="Delete gate"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  )
}
