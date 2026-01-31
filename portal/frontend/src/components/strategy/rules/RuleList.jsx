import React, { Fragment, useState } from 'react'
import { FilterModal } from '../filters/FilterModal.jsx'
import { buildFilterSummary } from '../filters/filterUtils.js'

/**
 * Badge component displaying a single rule condition with indicator, signal type, and direction.
 */
const ConditionBadge = ({ label, signalType, direction, ruleId, broken }) => {
  const normalizedDirection = typeof direction === 'string' ? direction.toLowerCase() : ''
  const ruleLabel = typeof ruleId === 'string' && ruleId.trim().length
    ? ruleId.replace(/_/g, ' ').toUpperCase()
    : ''

  const directionConfig = {
    label: 'Any bias',
    icon: '•',
    classes: 'border-white/12 bg-white/5 text-slate-200',
  }

  if (normalizedDirection === 'long') {
    directionConfig.label = 'Long bias'
    directionConfig.icon = '↗'
    directionConfig.classes = 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200'
  } else if (normalizedDirection === 'short') {
    directionConfig.label = 'Short bias'
    directionConfig.icon = '↘'
    directionConfig.classes = 'border-rose-500/40 bg-rose-500/15 text-rose-200'
  }

  const brokenClasses = broken
    ? 'border-amber-500/60 bg-amber-500/10 text-amber-100'
    : 'border-white/12 bg-black/25 text-slate-200'

  return (
    <div className={`flex min-w-[220px] items-stretch gap-3 rounded-2xl border px-3 py-2 ${brokenClasses}`}>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="truncate text-xs font-semibold text-white">{label}</span>
          {ruleLabel ? (
            <span className="rounded-md border border-white/10 bg-white/5 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.25em] text-slate-400">
              {ruleLabel}
            </span>
          ) : null}
          {broken && (
            <span className="rounded-md border border-amber-400/60 bg-amber-500/20 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.25em] text-amber-100">
              Detached
            </span>
          )}
        </div>
        <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] text-slate-300">
          <span className="inline-flex items-center rounded-md border border-white/10 bg-white/5 px-2 py-0.5 uppercase tracking-[0.25em]">
            {signalType ? signalType.toUpperCase() : 'SIGNAL'}
          </span>
          <span className={`inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10px] font-semibold ${directionConfig.classes}`}>
            <span>{directionConfig.icon}</span>
            {directionConfig.label}
          </span>
        </div>
      </div>
    </div>
  )
}

/**
 * Component displaying a list of strategy rules with conditions.
 *
 * Note: This component expects ActionButton which is passed in from the parent.
 */
export const RuleList = ({
  rules,
  onEdit,
  onDelete,
  onCreateFilter,
  onUpdateFilter,
  onDeleteFilter,
  indicatorLookup,
  ActionButton,
  brokenIndicatorIds,
}) => {
  const [expanded, setExpanded] = useState(null)
  const [filterModal, setFilterModal] = useState({ open: false, ruleId: null, filter: null })

  const toggleExpanded = (id) => {
    setExpanded((prev) => (prev === id ? null : id))
  }

  const buildSentence = (rule) => {
    const conditions = Array.isArray(rule?.conditions) ? rule.conditions : []
    if (!conditions.length) return 'No conditions set yet.'
    const connector = rule.match === 'any' ? ' OR ' : ' AND '
    const parts = conditions.slice(0, 2).map((condition) => {
      const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
      const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id || 'indicator'
      const signal = condition.signal_type || 'signal'
      const bias = condition.direction ? ` (${String(condition.direction).toUpperCase()})` : ''
      return `${label}: ${signal}${bias}`
    })
    const tail = conditions.length > 2 ? ` +${conditions.length - 2} more` : ''
    return `${parts.join(connector)}${tail}`
  }

  const handleFilterAdd = (ruleId) => {
    setFilterModal({ open: true, ruleId, filter: null })
  }

  const handleFilterEdit = (ruleId, filter) => {
    setFilterModal({ open: true, ruleId, filter })
  }

  const handleFilterClose = () => {
    setFilterModal({ open: false, ruleId: null, filter: null })
  }

  const handleFilterSave = async (payload) => {
    if (!filterModal.ruleId) return
    if (filterModal.filter?.id) {
      await onUpdateFilter?.(filterModal.ruleId, filterModal.filter.id, payload)
    } else {
      await onCreateFilter?.(filterModal.ruleId, payload)
    }
    handleFilterClose()
  }

  const handleFilterToggle = async (ruleId, filter) => {
    if (!filter?.id) return
    await onUpdateFilter?.(ruleId, filter.id, {
      enabled: !filter.enabled,
    })
  }

  if (!rules.length) {
    return (
      <p className="rounded-xl border border-white/10 bg-white/5 p-4 text-sm text-slate-400">
        No rules yet. Create at least one BUY or SELL rule to generate signals.
      </p>
    )
  }

  return (
    <>
      <div className="divide-y divide-white/5 rounded-xl border border-white/10 bg-white/5">
        {rules.map((rule) => {
          const conditionCount = Array.isArray(rule.conditions) ? rule.conditions.length : 0
          const filterCount = Array.isArray(rule.filters) ? rule.filters.length : 0
          return (
            <div key={rule.id} className="p-3">
              <div className="flex flex-wrap items-center gap-3">
                <div className="flex items-center gap-2">
                  <span className={`inline-flex items-center rounded-full px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.24em] ${rule.action === 'buy' ? 'bg-emerald-500/20 text-emerald-100 border border-emerald-500/30' : 'bg-rose-500/20 text-rose-100 border border-rose-500/30'}`}>
                    {rule.action?.toUpperCase() || 'ACTION'}
                  </span>
                  <span className="rounded-md border border-white/10 bg-white/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.24em] text-slate-300">
                    {rule.match === 'any' ? 'OR' : 'AND'}
                  </span>
                  <span className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] ${rule.enabled ? 'bg-emerald-600/30 text-emerald-100' : 'bg-slate-700/60 text-slate-400'}`}>
                    {rule.enabled ? 'Enabled' : 'Disabled'}
                  </span>
                </div>

                <div className="min-w-0 flex-1 text-sm text-white">
                  <button
                    type="button"
                    onClick={() => toggleExpanded(rule.id)}
                    className="w-full text-left hover:text-white/90 focus:outline-none"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate font-semibold">{rule.name}</span>
                      <span className="text-xs text-slate-500">{expanded === rule.id ? '▾' : '▸'}</span>
                    </div>
                    <p className="mt-1 truncate text-[11px] text-slate-400">
                      {buildSentence(rule)}
                    </p>
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-[0.24em] text-slate-500">
                      <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5">
                        {conditionCount} condition{conditionCount === 1 ? '' : 's'}
                      </span>
                      <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5">
                        {filterCount} filter{filterCount === 1 ? '' : 's'}
                      </span>
                    </div>
                  </button>
                </div>

                <div className="flex items-center gap-1">
                  <ActionButton variant="ghost" onClick={() => onEdit(rule)}>
                    Edit logic
                  </ActionButton>
                  <ActionButton variant="danger" onClick={() => onDelete(rule)}>
                    Delete
                  </ActionButton>
                </div>
              </div>

              {expanded === rule.id && (
                <div className="mt-3 space-y-4 rounded-lg border border-white/10 bg-black/30 p-3 text-xs text-slate-300">
                  {rule.description && (
                    <p className="text-[11px] text-slate-400">{rule.description}</p>
                  )}
                  <div>
                    <div className="flex items-center justify-between">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400">Conditions</p>
                      <button
                        type="button"
                        className="text-[10px] font-semibold uppercase tracking-[0.24em] text-slate-400 hover:text-slate-200"
                        onClick={() => onEdit(rule)}
                      >
                        Edit
                      </button>
                    </div>
                    {Array.isArray(rule.conditions) && rule.conditions.length ? (
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        {rule.conditions.map((condition, index) => {
                          const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
                          const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id
                          const isBroken = brokenIndicatorIds?.has?.(condition.indicator_id)
                          const connectorLabel = rule.match === 'any' ? 'OR' : 'AND'
                          return (
                            <Fragment key={`${rule.id}-condition-${index}`}>
                              <ConditionBadge
                                label={label}
                                signalType={condition.signal_type}
                                direction={condition.direction}
                                ruleId={condition.rule_id || condition.signal_type}
                                broken={isBroken}
                              />
                              {index < rule.conditions.length - 1 && (
                                <span className="rounded-md border border-white/10 bg-[#111622] px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.3em] text-slate-400">
                                  {connectorLabel}
                                </span>
                              )}
                            </Fragment>
                          )
                        })}
                      </div>
                    ) : (
                      <p className="mt-2 text-[11px] text-slate-400">No conditions configured.</p>
                    )}
                  </div>

                  <div>
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400">Filters</p>
                      <ActionButton variant="ghost" onClick={() => handleFilterAdd(rule.id)}>
                        Add filter
                      </ActionButton>
                    </div>
                    {filterCount ? (
                      <div className="mt-2 space-y-2">
                        {rule.filters.map((filter) => (
                          <div key={filter.id} className="flex flex-wrap items-center gap-3 rounded-lg border border-white/10 bg-black/40 px-3 py-2">
                            <button
                              type="button"
                              onClick={() => handleFilterToggle(rule.id, filter)}
                              className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] ${
                                filter.enabled
                                  ? 'bg-emerald-500/20 text-emerald-100 border border-emerald-500/30'
                                  : 'bg-slate-700/60 text-slate-400 border border-white/5'
                              }`}
                            >
                              {filter.enabled ? 'Enabled' : 'Disabled'}
                            </button>
                            <div className="min-w-[200px] flex-1">
                              <p className="text-[11px] font-semibold text-white">
                                {filter.name || buildFilterSummary(filter)}
                              </p>
                              <p className="text-[10px] text-slate-400">
                                {buildFilterSummary(filter)}
                              </p>
                            </div>
                            <div className="flex items-center gap-2">
                              <ActionButton variant="ghost" onClick={() => handleFilterEdit(rule.id, filter)}>
                                Edit
                              </ActionButton>
                              <ActionButton variant="danger" onClick={() => onDeleteFilter?.(rule.id, filter.id)}>
                                Delete
                              </ActionButton>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-2 text-[11px] text-slate-400">No filters attached to this rule.</p>
                    )}
                  </div>
                </div>
              )}
            </div>
          )
        })}
      </div>
      <FilterModal
        open={filterModal.open}
        initialFilter={filterModal.filter}
        onClose={handleFilterClose}
        onSave={handleFilterSave}
        title={filterModal.filter ? 'Edit rule filter' : 'Add rule filter'}
      />
    </>
  )
}
