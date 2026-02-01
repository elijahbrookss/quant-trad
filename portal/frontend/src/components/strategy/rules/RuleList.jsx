import React, { useState } from 'react'
import { buildRuleConditionSummary } from './ruleUtils.js'
import { RuleCard } from './RuleCard.jsx'
import { RuleGateSection } from './RuleGateSection.jsx'

/**
 * Component displaying a list of strategy rules with conditions.
 *
 * Note: This component expects ActionButton which is passed in from the parent.
 */
export const RuleList = ({
  rules,
  onEdit,
  onDelete,
  onDuplicate,
  onCreateFilter,
  onUpdateFilter,
  onDeleteFilter,
  indicatorLookup,
  ActionButton,
  brokenIndicatorIds,
  onAddRule,
}) => {
  const [expanded, setExpanded] = useState(null)
  const toggleExpanded = (id) => {
    setExpanded((prev) => (prev === id ? null : id))
  }

  if (!rules.length) {
    return (
      <div className="rounded-xl border border-dashed border-white/10 bg-black/30 p-6 text-sm text-slate-400">
        <p>No rules yet. Create at least one BUY or SELL rule to generate signals.</p>
        <div className="mt-3">
          <button
            type="button"
            className="rounded border border-white/10 bg-white/5 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-200 hover:border-white/20"
            onClick={onAddRule}
          >
            Create rule
          </button>
        </div>
      </div>
    )
  }

  return (
    <>
      <div className="space-y-3">
        {rules.map((rule) => {
          const conditionCount = Array.isArray(rule.conditions) ? rule.conditions.length : 0
          const filterCount = Array.isArray(rule.filters) ? rule.filters.length : 0
          const summary = buildRuleConditionSummary({
            conditions: rule.conditions,
            match: rule.match,
            indicatorLookup,
          })
          const expandedState = expanded === rule.id

          return (
            <RuleCard
              key={rule.id}
              rule={rule}
              summary={summary}
              conditionCount={conditionCount}
              filterCount={filterCount}
              expanded={expandedState}
              onToggleExpand={() => toggleExpanded(rule.id)}
              onEdit={() => onEdit(rule)}
              onDelete={() => onDelete(rule)}
              onDuplicate={() => onDuplicate?.(rule)}
            >
              <div>
                <div className="flex items-center justify-between">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Conditions</p>
                  <ActionButton variant="ghost" onClick={() => onEdit(rule)}>
                    Edit logic
                  </ActionButton>
                </div>
                {conditionCount ? (
                  <div className="mt-3 space-y-2">
                    <div className="hidden grid-cols-[1.1fr_1fr_120px] gap-2 text-[10px] uppercase tracking-[0.16em] text-slate-500 md:grid">
                      <div>Source</div>
                      <div>Signal</div>
                      <div>Direction</div>
                    </div>
                    {rule.conditions.map((condition, index) => {
                      const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
                      const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id
                      const isBroken = brokenIndicatorIds?.has?.(condition.indicator_id)
                      return (
                        <div
                          key={`${rule.id}-condition-${index}`}
                          className={`grid gap-2 rounded-lg border px-3 py-2 text-xs text-slate-200 md:grid-cols-[1.1fr_1fr_120px] ${
                            isBroken ? 'border-amber-500/50 bg-amber-500/10 text-amber-100' : 'border-white/10 bg-black/40'
                          }`}
                        >
                          <div className="flex items-center gap-2">
                            <span className="truncate font-semibold text-white">{label}</span>
                            {isBroken && (
                              <span className="rounded-full border border-amber-400/60 bg-amber-500/20 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.2em] text-amber-100">
                                Detached
                              </span>
                            )}
                          </div>
                          <div>{condition.signal_type || 'Signal'}</div>
                          <div className="text-slate-300">
                            {condition.direction ? condition.direction.toUpperCase() : 'Any bias'}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                ) : (
                  <p className="mt-2 text-[11px] text-slate-400">No conditions configured.</p>
                )}
              </div>

              <RuleGateSection
                ruleId={rule.id}
                filters={rule.filters || []}
                onCreateFilter={onCreateFilter}
                onUpdateFilter={onUpdateFilter}
                onDeleteFilter={onDeleteFilter}
                ActionButton={ActionButton}
              />
            </RuleCard>
          )
        })}
      </div>
    </>
  )
}
