import React, { useState } from 'react'
import { GitBranch, Plus } from 'lucide-react'
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
      <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-white/10 bg-black/20 px-6 py-10 text-center">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-white/5">
          <GitBranch className="h-6 w-6 text-slate-500" />
        </div>
        <h4 className="mt-4 text-sm font-medium text-white">No trading rules yet</h4>
        <p className="mt-1.5 max-w-sm text-xs text-slate-500">
          Rules define when to buy or sell based on your indicator signals. Create a rule to start building your strategy.
        </p>
        <button
          type="button"
          className="mt-5 inline-flex items-center gap-1.5 rounded-lg border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] px-4 py-2 text-sm font-medium text-[color:var(--accent-text-soft)] transition hover:bg-[color:var(--accent-alpha-20)]"
          onClick={onAddRule}
        >
          <Plus className="h-4 w-4" />
          Create first rule
        </button>
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
