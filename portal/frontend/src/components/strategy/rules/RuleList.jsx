import React, { useState } from 'react'
import { ArrowRight, GitBranch, Plus } from 'lucide-react'

import { buildRuleConditionSummary, extractRuleFlow } from './ruleUtils.js'
import { RuleCard } from './RuleCard.jsx'

const resolveIndicatorLabel = (indicatorLookup, indicatorId) => {
  const meta = indicatorLookup?.get?.(indicatorId) || indicatorLookup?.[indicatorId]
  return meta?.name || meta?.type || indicatorId || 'Indicator'
}

const guardSummary = (guard) => {
  if (guard?.type === 'context_match') {
    return `${guard.output_name} = ${guard.state_key}`
  }
  if (guard?.type === 'metric_match') {
    return `${guard.output_name}.${guard.field} ${guard.operator} ${guard.value}`
  }
  return 'Guard'
}

export const RuleList = ({
  rules,
  onEdit,
  onDelete,
  onDuplicate,
  indicatorLookup,
  brokenIndicatorIds,
  ActionButton,
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
        <h4 className="mt-4 text-sm font-medium text-white">No trading flows yet</h4>
        <p className="mt-1.5 max-w-sm text-xs text-slate-500">
          Each rule starts with one signal trigger, then optional context and metric guards.
        </p>
        <button
          type="button"
          className="mt-5 inline-flex items-center gap-1.5 rounded-lg border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] px-4 py-2 text-sm font-medium text-[color:var(--accent-text-soft)] transition hover:bg-[color:var(--accent-alpha-20)]"
          onClick={onAddRule}
        >
          <Plus className="h-4 w-4" />
          Create first flow
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {rules.map((rule) => {
        const { trigger, guards } = extractRuleFlow(rule)
        const triggerCount = trigger?.indicator_id ? 1 : 0
        const guardCount = Array.isArray(guards) ? guards.length : 0
        const summary = buildRuleConditionSummary({ rule, indicatorLookup })
        const expandedState = expanded === rule.id
        const triggerLabel = resolveIndicatorLabel(indicatorLookup, trigger?.indicator_id)
        const brokenTrigger = brokenIndicatorIds?.has?.(trigger?.indicator_id)

        return (
          <RuleCard
            key={rule.id}
            rule={rule}
            summary={summary}
            triggerCount={triggerCount}
            guardCount={guardCount}
            expanded={expandedState}
            onToggleExpand={() => toggleExpanded(rule.id)}
            onEdit={() => onEdit(rule)}
            onDelete={() => onDelete(rule)}
            onDuplicate={() => onDuplicate?.(rule)}
          >
            <div className="grid gap-4 xl:grid-cols-[1.1fr_44px_1.1fr_44px_0.8fr]">
              <div className={`rounded-xl border p-3 ${brokenTrigger ? 'border-amber-500/40 bg-amber-500/10' : 'border-emerald-500/25 bg-emerald-500/10'}`}>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-200/80">Trigger</p>
                {triggerCount ? (
                  <div className="mt-3 space-y-1.5">
                    <div className="text-sm font-semibold text-white">{triggerLabel}</div>
                    <div className="text-xs text-slate-300">{trigger?.output_name}</div>
                    <div className="inline-flex rounded-full border border-emerald-300/20 bg-black/20 px-2 py-0.5 text-[10px] uppercase tracking-[0.18em] text-emerald-100">
                      {trigger?.event_key || 'event'}
                    </div>
                  </div>
                ) : (
                  <p className="mt-3 text-xs text-slate-400">No signal trigger configured.</p>
                )}
              </div>

              <div className="flex items-center justify-center text-slate-500">
                <ArrowRight className="h-4 w-4" />
              </div>

              <div className="rounded-xl border border-amber-500/25 bg-amber-500/10 p-3">
                <div className="flex items-center justify-between">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-amber-200/80">Guards</p>
                  <ActionButton variant="ghost" onClick={() => onEdit(rule)}>
                    Edit flow
                  </ActionButton>
                </div>
                {guardCount ? (
                  <div className="mt-3 space-y-2">
                    {guards.map((guard, index) => {
                      const label = resolveIndicatorLabel(indicatorLookup, guard?.indicator_id)
                      const broken = brokenIndicatorIds?.has?.(guard?.indicator_id)
                      return (
                        <div
                          key={`${rule.id}-guard-${index}`}
                          className={`rounded-lg border px-3 py-2 text-xs ${
                            broken ? 'border-amber-500/40 bg-amber-500/10 text-amber-100' : 'border-white/10 bg-black/20 text-slate-200'
                          }`}
                        >
                          <div className="font-semibold text-white">{label}</div>
                          <div className="mt-1 text-slate-300">{guardSummary(guard)}</div>
                        </div>
                      )
                    })}
                  </div>
                ) : (
                  <p className="mt-3 text-xs text-slate-400">No guards. This rule fires on the trigger alone.</p>
                )}
              </div>

              <div className="flex items-center justify-center text-slate-500">
                <ArrowRight className="h-4 w-4" />
              </div>

              <div className={`rounded-xl border p-3 ${rule.action === 'buy' ? 'border-emerald-500/25 bg-emerald-500/10' : 'border-rose-500/25 bg-rose-500/10'}`}>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-300">Action</p>
                <div className="mt-3">
                  <span className={`inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] ${
                    rule.action === 'buy'
                      ? 'border-emerald-400/30 bg-emerald-400/15 text-emerald-100'
                      : 'border-rose-400/30 bg-rose-400/15 text-rose-100'
                  }`}>
                    {rule.action || 'action'}
                  </span>
                </div>
              </div>
            </div>
          </RuleCard>
        )
      })}
    </div>
  )
}
