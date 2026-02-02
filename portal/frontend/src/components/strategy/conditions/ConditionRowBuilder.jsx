import React from 'react'
import { addConditionRow, removeConditionRow, updateConditionRow } from './conditionRowUtils.js'

export { addConditionRow, removeConditionRow, updateConditionRow }

export const ConditionRowBuilder = ({
  rows,
  columns,
  onAddRow,
  addLabel = 'Add condition',
  emptyState,
  gridClassName,
}) => {
  const hasRows = Array.isArray(rows) && rows.length > 0
  const gridClasses = gridClassName || 'md:grid-cols-[160px_minmax(0,1fr)_120px_140px_160px_140px_40px]'

  return (
    <div className="space-y-3">
      <div className={`hidden items-center gap-2 text-[10px] uppercase tracking-[0.16em] text-slate-500 md:grid ${gridClasses}`}>
        {columns.map((column) => (
          <div key={column.key} className={column.headerClassName}>
            {column.label}
          </div>
        ))}
      </div>

      {hasRows ? (
        rows.map((row, index) => (
          <div
            key={`condition-row-${index}`}
            className={`grid items-center gap-2 rounded-lg border border-white/10 bg-black/35 px-3 py-2 md:grid ${gridClasses}`}
          >
            {columns.map((column) => (
              <div key={`${column.key}-${index}`} className={column.cellClassName}>
                {column.render(row, index)}
              </div>
            ))}
          </div>
        ))
      ) : (
        <div className="rounded-lg border border-dashed border-white/10 bg-black/30 p-3 text-xs text-slate-500">
          {emptyState || 'No conditions configured yet.'}
        </div>
      )}

      <button
        type="button"
        className="inline-flex items-center gap-2 rounded border border-white/10 bg-white/5 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-200 hover:border-white/20"
        onClick={onAddRow}
      >
        + {addLabel}
      </button>
    </div>
  )
}
