import React from 'react'
import { formatNumber } from '../../utils/formatters'
import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols'

/**
 * Grid display of strategy cards.
 */
export const StrategyGrid = ({ strategies, selectedId, onSelect, onEdit, onDelete, layout = 'grid' }) => {
  if (!strategies.length) {
    return (
      <div className="rounded-2xl border border-dashed border-white/10 bg-black/20 p-12 text-center">
        <svg
          className="mx-auto h-12 w-12 text-slate-600"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={1.5}
            d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"
          />
        </svg>
        <p className="mt-4 text-sm font-medium text-slate-400">No strategies yet</p>
        <p className="mt-1 text-xs text-slate-500">Create your first strategy to get started</p>
      </div>
    )
  }

  const gridClasses =
    layout === 'stacked'
      ? 'grid gap-3'
      : 'grid gap-4 sm:grid-cols-2 lg:grid-cols-3'

  return (
    <div className={gridClasses}>
      {strategies.map((strategy) => {
        const isActive = strategy.id === selectedId
        const ruleCount = Array.isArray(strategy.rules) ? strategy.rules.length : 0
        const indicatorCount = Array.isArray(strategy.indicator_ids) ? strategy.indicator_ids.length : 0
        const atmTargets = strategy.atm_template?.take_profit_orders?.length || 0
        const stopR = strategy.atm_template?.initial_stop?.atr_multiplier || strategy.atm_template?.stop_r_multiple || null
        const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots)
        const symbolPreview = symbols.slice(0, 3).join(', ')
        const symbolSuffix = symbols.length > 3 ? ` +${symbols.length - 3}` : ''

        return (
          <div
            key={strategy.id}
            className={`group relative rounded-2xl border p-5 transition-all ${
              isActive
                ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] shadow-lg shadow-[color:var(--accent-shadow-soft)]'
                : 'border-white/10 bg-black/30 hover:border-white/20 hover:bg-black/40'
            }`}
          >
            <button
              onClick={() => onSelect(strategy.id)}
              className="absolute inset-0 z-0 rounded-2xl"
              aria-label={`Select ${strategy.name}`}
            />

            <div className="relative z-10 space-y-4">
              {/* Header */}
              <div>
                <h3 className="text-base font-semibold text-white">{strategy.name}</h3>
                <p className="mt-1 text-xs text-slate-400">
                  {strategy.timeframe} • {symbolPreview}{symbolSuffix}
                </p>
              </div>

              {/* Stats */}
              <div className="flex flex-wrap gap-2">
                <span className="inline-flex items-center gap-1.5 rounded-lg bg-black/40 px-2 py-1 text-xs text-slate-300">
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
                  </svg>
                  {ruleCount} {ruleCount === 1 ? 'rule' : 'rules'}
                </span>
                <span className="inline-flex items-center gap-1.5 rounded-lg bg-black/40 px-2 py-1 text-xs text-slate-300">
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
                  </svg>
                  {indicatorCount} {indicatorCount === 1 ? 'indicator' : 'indicators'}
                </span>
                {atmTargets > 0 && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg bg-black/40 px-2 py-1 text-xs text-slate-300">
                    <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                    </svg>
                    {atmTargets} {atmTargets === 1 ? 'target' : 'targets'}
                  </span>
                )}
                {stopR && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg bg-black/40 px-2 py-1 text-xs text-slate-300">
                    {formatNumber(Math.abs(stopR), 1)}R stop
                  </span>
                )}
              </div>

              {/* Actions */}
              <div className="flex items-center gap-2 opacity-0 transition-opacity group-hover:opacity-100">
                <button
                  onClick={(e) => { e.stopPropagation(); onEdit(strategy); }}
                  className="relative z-20 rounded-lg border border-white/10 bg-black/40 px-3 py-1.5 text-xs font-medium text-slate-300 transition hover:border-white/20 hover:bg-black/50 hover:text-white"
                >
                  Edit
                </button>
                <button
                  onClick={(e) => { e.stopPropagation(); onDelete(strategy); }}
                  className="relative z-20 rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-1.5 text-xs font-medium text-rose-300 transition hover:border-rose-500/30 hover:bg-rose-500/20 hover:text-rose-200"
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
