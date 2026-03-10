import React, { useState } from 'react'
import { FilterDrawer } from '../filters/FilterDrawer.jsx'
import { GateCard } from '../filters/GateCard.jsx'

export const RuleGateSection = ({
  ruleId,
  filters,
  onCreateFilter,
  onUpdateFilter,
  onDeleteFilter,
  ActionButton,
}) => {
  const [drawerState, setDrawerState] = useState({ open: false, filter: null })

  const handleAdd = () => setDrawerState({ open: true, filter: null })
  const handleEdit = (filter) => setDrawerState({ open: true, filter })
  const handleClose = () => setDrawerState({ open: false, filter: null })

  const handleSave = async (payload) => {
    if (drawerState.filter?.id) {
      await onUpdateFilter?.(ruleId, drawerState.filter.id, payload)
    } else {
      await onCreateFilter?.(ruleId, payload)
    }
    handleClose()
  }

  const handleToggle = async (filter) => {
    if (!filter?.id) return
    await onUpdateFilter?.(ruleId, filter.id, {
      enabled: !filter.enabled,
    })
  }

  return (
    <div>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Rule gates</p>
        <ActionButton variant="ghost" onClick={handleAdd}>
          Add gate
        </ActionButton>
      </div>
      {filters?.length ? (
        <div className="mt-3 space-y-2">
          {filters.map((filter) => (
            <GateCard
              key={filter.id}
              filter={filter}
              onToggle={(entry) => handleToggle(entry)}
              onEdit={() => handleEdit(filter)}
              onDelete={() => onDeleteFilter?.(ruleId, filter.id)}
              actionsLabel={`Gate actions for ${filter.name || 'filter'}`}
            />
          ))}
        </div>
      ) : (
        <p className="mt-2 text-[11px] text-slate-400">No gates attached to this rule.</p>
      )}

      <FilterDrawer
        open={drawerState.open}
        initialFilter={drawerState.filter}
        onClose={handleClose}
        onSave={handleSave}
        title={drawerState.filter ? 'Edit rule gate' : 'Add rule gate'}
      />
    </div>
  )
}
