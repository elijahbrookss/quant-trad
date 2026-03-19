import React, { Fragment } from 'react'
import { Popover, PopoverButton, PopoverPanel, Transition } from '@headlessui/react'
import { MoreVertical, Pencil, CopyPlus, Trash2, ChevronDown, ChevronRight } from 'lucide-react'

export const RuleCard = ({
  rule,
  summary,
  triggerCount,
  guardCount,
  expanded,
  onToggleExpand,
  onEdit,
  onDelete,
  onDuplicate,
  children,
}) => {
  const isBuy = rule.action === 'buy'
  const accentColor = isBuy ? 'emerald' : 'rose'

  return (
    <div
      className={`
        group relative overflow-hidden rounded-xl border transition-all
        ${rule.enabled ? 'border-white/10 bg-white/[0.03]' : 'border-white/5 bg-white/[0.01] opacity-60'}
        hover:border-white/15 hover:bg-white/[0.04]
      `}
    >
      {/* Left accent bar */}
      <div
        className={`absolute left-0 top-0 h-full w-1 ${isBuy ? 'bg-emerald-500' : 'bg-rose-500'}`}
        aria-hidden="true"
      />

      {/* Main content */}
      <div className="flex items-start gap-3 py-3 pl-4 pr-3">
        {/* Action badge */}
        <span
          className={`mt-0.5 inline-flex shrink-0 items-center rounded px-2 py-0.5 text-[10px] font-bold uppercase tracking-[0.15em] ${
            isBuy
              ? 'bg-emerald-500/15 text-emerald-300'
              : 'bg-rose-500/15 text-rose-300'
          }`}
        >
          {rule.action?.toUpperCase() || 'ACTION'}
        </span>

        {/* Content area - clickable to expand */}
        <button
          type="button"
          onClick={onToggleExpand}
          className="min-w-0 flex-1 text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent-ring)] focus-visible:ring-offset-2 focus-visible:ring-offset-[#0d1117] rounded"
        >
          <div className="flex items-start gap-2">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className="truncate text-sm font-semibold text-white">
                  {rule.name}
                </span>
                {!rule.enabled && (
                  <span className="shrink-0 rounded bg-slate-700/50 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-slate-400">
                    Off
                  </span>
                )}
              </div>
              <p className="mt-0.5 truncate text-xs text-slate-400">{summary}</p>
            </div>
            <span className="mt-1 shrink-0 text-slate-500">
              {expanded ? (
                <ChevronDown className="h-4 w-4" />
              ) : (
                <ChevronRight className="h-4 w-4" />
              )}
            </span>
          </div>

          {/* Metadata badges */}
          <div className="mt-2 flex flex-wrap items-center gap-1.5">
            <span className="inline-flex items-center rounded bg-white/5 px-2 py-0.5 text-[10px] text-slate-400">
              {triggerCount} trigger{triggerCount === 1 ? '' : 's'}
            </span>
            <span className="inline-flex items-center rounded bg-white/5 px-2 py-0.5 text-[10px] text-slate-400">
              {guardCount} guard{guardCount === 1 ? '' : 's'}
            </span>
          </div>
        </button>

        {/* Kebab menu */}
        <Popover className="relative shrink-0">
          {({ close }) => (
            <>
              <PopoverButton
                className="flex h-8 w-8 items-center justify-center rounded-md border border-transparent text-slate-400 transition hover:border-white/10 hover:bg-white/5 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent-ring)]"
                title="More actions"
              >
                <MoreVertical className="h-4 w-4" />
              </PopoverButton>
              <Transition
                as={Fragment}
                enter="transition ease-out duration-100"
                enterFrom="opacity-0 scale-95"
                enterTo="opacity-100 scale-100"
                leave="transition ease-in duration-75"
                leaveFrom="opacity-100 scale-100"
                leaveTo="opacity-0 scale-95"
              >
                <PopoverPanel className="absolute right-0 top-full z-50 mt-1 w-44 origin-top-right rounded-lg border border-white/10 bg-[#131a2b] p-1.5 shadow-xl">
                  <button
                    type="button"
                    onClick={() => {
                      onEdit?.()
                      close()
                    }}
                    className="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/5"
                  >
                    <Pencil className="h-3.5 w-3.5" />
                    Edit
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      onDuplicate?.()
                      close()
                    }}
                    className="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/5"
                  >
                    <CopyPlus className="h-3.5 w-3.5" />
                    Duplicate
                  </button>
                  <div className="my-1 h-px bg-white/10" />
                  <button
                    type="button"
                    onClick={() => {
                      onDelete?.()
                      close()
                    }}
                    className="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-left text-sm text-rose-300 transition hover:bg-rose-500/10"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                    Delete
                  </button>
                </PopoverPanel>
              </Transition>
            </>
          )}
        </Popover>
      </div>

      {/* Expanded content */}
      {expanded && children ? (
        <div className="border-t border-white/5 bg-black/20 px-4 py-4">
          {children}
        </div>
      ) : null}
    </div>
  )
}
