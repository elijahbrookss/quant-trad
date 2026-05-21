import React, { useState } from 'react'
import { GitBranch, Plus } from 'lucide-react'

import { buildRuleConditionSummary, extractRuleFlow } from './ruleUtils.js'
import { RuleCard } from './RuleCard.jsx'

const resolveIndicatorLabel = (indicatorLookup, indicatorId) => {
  const meta = indicatorLookup?.get?.(indicatorId) || indicatorLookup?.[indicatorId]
  return meta?.name || meta?.type || indicatorId || 'Indicator'
}

const guardSummary = (guard) => {
  if (guard?.type === 'context_match') {
    if (Array.isArray(guard?.value)) {
      return `${guard.output_name}.${guard.field || 'state'} ∈ [${guard.value.join(', ')}]`
    }
    return `${guard.output_name}.${guard.field || 'state'} = ${guard.value}`
  }
  if (guard?.type === 'metric_match') {
    return `${guard.output_name}.${guard.field} ${guard.operator} ${guard.value}`
  }
  if (guard?.type === 'holds_for_bars') {
    return `${guard.base?.output_name || 'signal'} held for ${guard.bars} bars`
  }
  if (guard?.type === 'signal_seen_within_bars') {
    return `${guard.output_name}.${guard.event_key} seen within ${guard.lookback_bars} bars`
  }
  if (guard?.type === 'signal_absent_within_bars') {
    return `${guard.output_name}.${guard.event_key} absent within ${guard.lookback_bars} bars`
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
        <h4 className="mt-4 text-sm font-medium text-white">No strategy rules yet</h4>
        <p className="mt-1.5 max-w-sm text-xs text-slate-500">
          Each rule starts with one signal trigger, then optional context and metric guards.
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
    <div className="space-y-3">
      {rules.map((rule) => {
        const { trigger, guards } = extractRuleFlow(rule)
        const isLong = rule.intent !== 'enter_short'
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
            <div className="space-y-3">
              <div className="space-y-1.5">
                <p className={`text-[10px] uppercase tracking-[0.2em] ${brokenTrigger ? 'text-amber-400/80' : 'text-slate-500'}`}>Trigger</p>
                {triggerCount ? (
                  <>
                    <p className="text-sm text-white">{triggerLabel}</p>
                    <p className="text-xs text-slate-300">{trigger?.output_name}</p>
                    <p className="text-xs text-slate-400">{trigger?.event_key || 'event'}</p>
                  </>
                ) : (
                  <p className="text-xs text-slate-400">No signal trigger configured.</p>
                )}
              </div>

              <div className="space-y-1.5">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Conditions</p>
                  <ActionButton variant="ghost" onClick={() => onEdit(rule)}>
                    Edit rule
                  </ActionButton>
                </div>
                {guardCount ? (
                  <div className="space-y-1">
                    {guards.map((guard, index) => {
                      const label = resolveIndicatorLabel(indicatorLookup, guard?.indicator_id)
                      const broken = brokenIndicatorIds?.has?.(guard?.indicator_id)
                      return (
                        <p
                          key={`${rule.id}-guard-${index}`}
                          className={`text-xs ${broken ? 'text-amber-200' : 'text-slate-300'}`}
                        >
                          <span className="text-white">{label}</span> {guardSummary(guard)}
                        </p>
                      )
                    })}
                  </div>
                ) : (
                  <p className="text-xs text-slate-400">No guards. This rule fires on the trigger alone.</p>
                )}
              </div>

              <div className="space-y-1.5">
                <p className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Intent</p>
                <div>
                  <span className={`inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] ${
                    isLong
                      ? 'border-emerald-400/30 bg-emerald-400/15 text-emerald-100'
                      : 'border-rose-400/30 bg-rose-400/15 text-rose-100'
                  }`}>
                    {isLong ? 'LONG' : 'SHORT'}
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
