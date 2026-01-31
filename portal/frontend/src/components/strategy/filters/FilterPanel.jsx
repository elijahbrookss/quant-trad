import React, { useState } from 'react'
import { Button } from '../../ui'
import { FilterModal } from './FilterModal.jsx'
import { buildFilterSummary } from './filterUtils.js'

export const FilterPanel = ({
  title,
  description,
  filters = [],
  onCreate,
  onUpdate,
  onDelete,
  ActionButton,
  emptyState,
}) => {
  const [modalState, setModalState] = useState({ open: false, filter: null })

  const handleAdd = () => setModalState({ open: true, filter: null })
  const handleEdit = (filter) => setModalState({ open: true, filter })
  const handleClose = () => setModalState({ open: false, filter: null })

  const handleSave = async (payload) => {
    if (modalState.filter?.id) {
      await onUpdate(modalState.filter.id, payload)
    } else {
      await onCreate(payload)
    }
    handleClose()
  }

  const handleToggle = async (filter) => {
    if (!filter?.id) return
    await onUpdate(filter.id, {
      ...filter,
      enabled: !filter.enabled,
      dsl: filter.dsl,
    })
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h4 className="text-sm font-semibold text-white">{title}</h4>
          {description && <p className="mt-1 text-xs text-slate-400">{description}</p>}
        </div>
        <Button onClick={handleAdd}>Add Filter</Button>
      </div>

      {filters.length === 0 ? (
        <div className="rounded-xl border border-dashed border-white/10 bg-black/30 p-4 text-sm text-slate-400">
          {emptyState || 'No filters configured yet.'}
        </div>
      ) : (
        <div className="divide-y divide-white/5 rounded-xl border border-white/10 bg-white/5">
          {filters.map((filter) => (
            <div key={filter.id} className="flex flex-wrap items-center gap-3 px-4 py-3">
              <button
                type="button"
                onClick={() => handleToggle(filter)}
                className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] ${
                  filter.enabled
                    ? 'bg-emerald-500/20 text-emerald-100 border border-emerald-500/30'
                    : 'bg-slate-700/60 text-slate-400 border border-white/5'
                }`}
              >
                {filter.enabled ? 'Enabled' : 'Disabled'}
              </button>
              <div className="min-w-[200px] flex-1">
                <p className="text-sm font-semibold text-white">
                  {filter.name || buildFilterSummary(filter)}
                </p>
                <p className="mt-1 text-[11px] text-slate-400">
                  {buildFilterSummary(filter)}
                </p>
              </div>
              <div className="flex items-center gap-2">
                <ActionButton variant="ghost" onClick={() => handleEdit(filter)}>
                  Edit
                </ActionButton>
                <ActionButton variant="danger" onClick={() => onDelete(filter.id)}>
                  Delete
                </ActionButton>
              </div>
            </div>
          ))}
        </div>
      )}

      <FilterModal
        open={modalState.open}
        initialFilter={modalState.filter}
        onClose={handleClose}
        onSave={handleSave}
        title={modalState.filter ? 'Edit filter' : 'Add filter'}
      />
    </div>
  )
}
