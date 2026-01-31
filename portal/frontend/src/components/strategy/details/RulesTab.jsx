import React, { useMemo } from 'react'
import { Button } from '../../ui'
import { AttachedIndicators } from '../indicators'
import { RuleList } from '../rules'
import { findBrokenRuleIndicators } from '../utils/indicatorUsage.js'

/**
 * Signal Sources & Rules tab combining indicator attachment and rule management.
 */
export const RulesTab = ({
  strategy,
  attachedIndicators,
  availableIndicators,
  onAttachIndicator,
  onDetachIndicator,
  onAddRule,
  onEditRule,
  onDeleteRule,
  indicatorLookup,
  // These components need to be passed in
  DropdownSelect,
  ActionButton
}) => {
  const brokenIndicatorIds = useMemo(
    () => findBrokenRuleIndicators(attachedIndicators.map((ind) => ind.id), strategy?.rules),
    [attachedIndicators, strategy?.rules],
  )

  return (
    <div className="space-y-6">
      {/* Signal sources (attach only) */}
      <div className="space-y-3">
        <h4 className="text-sm font-semibold text-white">Signal Sources</h4>
        <p className="text-xs text-slate-400">
          Attach indicators to use their signals in rules. Editing belongs in QuantLab.
        </p>
        <AttachedIndicators
          strategy={strategy}
          attached={attachedIndicators}
          availableIndicators={availableIndicators}
          onAttach={onAttachIndicator}
          onDetach={onDetachIndicator}
          DropdownSelect={DropdownSelect}
          ActionButton={ActionButton}
        />
      </div>

      {/* Divider */}
      <div className="h-px bg-white/10" />

      {/* Rules Section */}
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <h4 className="text-sm font-semibold text-white">Rules</h4>
            <p className="mt-1 text-xs text-slate-400">
              Define entry and exit conditions based on indicator signals.
            </p>
          </div>
          <Button onClick={onAddRule}>New rule</Button>
        </div>
        <RuleList
          rules={Array.isArray(strategy.rules) ? strategy.rules : []}
          onEdit={onEditRule}
          onDelete={onDeleteRule}
          indicatorLookup={indicatorLookup}
          brokenIndicatorIds={brokenIndicatorIds}
          ActionButton={ActionButton}
        />
      </div>
    </div>
  )
}
