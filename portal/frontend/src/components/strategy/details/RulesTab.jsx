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
      <div className="flex h-32 items-center justify-center rounded-sm border border-dashed border-white/10 bg-[#0a0d13]">
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-slate-500 border-t-transparent" />
          Loading strategy rules...
        </div>
      </div>
    )
  }

  return (
    <div className="flex min-h-0 divide-x divide-white/[0.06]">
      <div className="w-[260px] shrink-0 overflow-y-auto p-4 space-y-3">
        <span className="text-[10px] uppercase tracking-[0.2em] text-slate-500">
          Indicator Inputs
        </span>
        <p className="text-xs text-slate-600">
          Attach indicators; use their outputs in rules.
        </p>
        <div>
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
      </div>

      <div className="min-w-0 flex-1 overflow-y-auto p-4 space-y-3">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <span className="text-[10px] uppercase tracking-[0.2em] text-slate-500">
              Rule Flows
            </span>
            {rules.length > 0 && (
              <span className="rounded bg-white/5 px-1.5 py-0.5 text-[10px] text-slate-400">
                <span className="qt-mono">{rules.length}</span> rule{rules.length === 1 ? '' : 's'}
              </span>
            )}
          </div>
          <Button onClick={onAddRule} className="h-7 gap-1.5 px-2.5 text-xs">
            <Plus className="h-3 w-3" />
            New rule
          </Button>
        </div>
        <p className="text-xs text-slate-600">
          Signal trigger + up to two optional context or metric guards.
        </p>
        <div>
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
      </div>
    </div>
  )
}
