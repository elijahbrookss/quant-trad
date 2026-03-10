import React, { useState } from 'react'
import { Filter, Plus } from 'lucide-react'
import { Button } from '../../ui'
import { FilterDrawer } from './FilterDrawer.jsx'
import { GateCard } from './GateCard.jsx'

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
  const [drawerState, setDrawerState] = useState({ open: false, filter: null })

  const handleAdd = () => setDrawerState({ open: true, filter: null })
  const handleEdit = (filter) => setDrawerState({ open: true, filter })
  const handleClose = () => setDrawerState({ open: false, filter: null })

  const handleSave = async (payload) => {
    if (drawerState.filter?.id) {
      await onUpdate(drawerState.filter.id, payload)
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
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold text-white">{title}</h3>
          {filters.length > 0 && (
            <span className="rounded-full bg-white/5 px-2 py-0.5 text-[11px] text-slate-400">
              {filters.length} gate{filters.length === 1 ? '' : 's'}
            </span>
          )}
        </div>
        <Button onClick={handleAdd} className="gap-1.5">
          <Plus className="h-4 w-4" />
          Add gate
        </Button>
      </div>
      {description && <p className="text-xs text-slate-500">{description}</p>}

      {filters.length === 0 ? (
        <div className="flex items-center gap-4 rounded-lg border border-dashed border-white/10 bg-black/20 px-5 py-4">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-white/5">
            <Filter className="h-5 w-5 text-slate-500" />
          </div>
          <div className="min-w-0 flex-1">
            <p className="text-sm text-slate-400">
              {emptyState || 'No gates configured yet.'}
            </p>
          </div>
          <button
            type="button"
            onClick={handleAdd}
            className="shrink-0 text-xs font-medium text-[color:var(--accent-text-soft)] transition hover:text-[color:var(--accent-text-strong)]"
          >
            Add gate
          </button>
        </div>
      ) : (
        <div className="space-y-3">
          {filters.map((filter) => (
            <GateCard
              key={filter.id}
              filter={filter}
              onToggle={handleToggle}
              onEdit={handleEdit}
              onDelete={(entry) => onDelete(entry.id)}
            />
          ))}
        </div>
      )}

      <FilterDrawer
        open={drawerState.open}
        initialFilter={drawerState.filter}
        onClose={handleClose}
        onSave={handleSave}
        title={drawerState.filter ? 'Edit gate' : 'Add gate'}
      />
    </div>
  )
}
