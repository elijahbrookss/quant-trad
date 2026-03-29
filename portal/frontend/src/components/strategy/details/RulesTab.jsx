import React, { useMemo } from 'react'
import { Plus } from 'lucide-react'

import { Button } from '../../ui'
import { AttachedIndicators } from '../indicators'
import { RuleList } from '../rules'
import { findBrokenRuleIndicators } from '../utils/indicatorUsage.js'

export const RulesTab = ({
  strategy,
  attachedIndicators,
  availableIndicators,
  onAttachIndicator,
  onDetachIndicator,
  onAddRule,
  onEditRule,
  onDeleteRule,
  onDuplicateRule,
  indicatorLookup,
  DropdownSelect,
  ActionButton,
}) => {
  const attachedIndicatorEntries = Array.isArray(attachedIndicators) ? attachedIndicators : []
  const brokenIndicatorIds = useMemo(
    () => findBrokenRuleIndicators(attachedIndicatorEntries.map((ind) => ind.id), strategy?.rules),
    [attachedIndicatorEntries, strategy?.rules],
  )

  const rules = Array.isArray(strategy?.rules) ? strategy.rules : []

  if (!strategy) {
    return (
      <div className="flex h-32 items-center justify-center rounded-xl border border-dashed border-white/10 bg-black/20">
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-slate-500 border-t-transparent" />
          Loading strategy rules...
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-sm font-semibold text-white">Indicator Inputs</h3>
            <p className="mt-0.5 text-xs text-slate-500">
              Attach indicators, inspect their published outputs, then use those outputs in rule flows.
            </p>
          </div>
        </div>
        <div className="mt-4">
          <AttachedIndicators
            strategy={strategy}
            attached={attachedIndicatorEntries}
            availableIndicators={availableIndicators}
            onAttach={onAttachIndicator}
            onDetach={onDetachIndicator}
            DropdownSelect={DropdownSelect}
            ActionButton={ActionButton}
          />
        </div>
      </section>

      <section className="rounded-xl border border-white/10 bg-black/20 p-5">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <h3 className="text-base font-semibold text-white">Rule Flows</h3>
            {rules.length > 0 && (
              <span className="rounded-full bg-white/5 px-2 py-0.5 text-[11px] text-slate-400">
                {rules.length} rule{rules.length === 1 ? '' : 's'}
              </span>
            )}
          </div>
          <Button onClick={onAddRule} className="gap-1.5">
            <Plus className="h-4 w-4" />
            New rule
          </Button>
        </div>
        <p className="mt-1 text-xs text-slate-500">
          Compose one required signal trigger with up to two optional context or metric guards.
        </p>
        <div className="mt-5">
          <RuleList
            rules={rules}
            onEdit={onEditRule}
            onDelete={onDeleteRule}
            onDuplicate={onDuplicateRule}
            indicatorLookup={indicatorLookup}
            brokenIndicatorIds={brokenIndicatorIds}
            ActionButton={ActionButton}
            onAddRule={onAddRule}
          />
        </div>
      </section>
    </div>
  )
}
