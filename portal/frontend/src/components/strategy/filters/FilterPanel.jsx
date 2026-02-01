import React, { useState } from 'react'
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
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h4 className="text-sm font-semibold text-white">{title}</h4>
          {description && <p className="mt-1 text-xs text-slate-400">{description}</p>}
        </div>
        <Button onClick={handleAdd}>Add gate</Button>
      </div>

      {filters.length === 0 ? (
        <div className="rounded-xl border border-dashed border-white/10 bg-black/30 p-6 text-sm text-slate-400">
          <p>{emptyState || 'No gates configured yet.'}</p>
          <div className="mt-3">
            <ActionButton variant="ghost" onClick={handleAdd}>
              Add gate
            </ActionButton>
          </div>
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
