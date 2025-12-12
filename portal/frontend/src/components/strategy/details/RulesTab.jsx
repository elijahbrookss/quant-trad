import React from 'react'
import { Button } from '../../ui'
import { AttachedIndicators } from '../indicators'
import { RuleList } from '../rules'

/**
 * Rules & Indicators tab combining indicator attachment and rule management.
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
  return (
    <div className="space-y-6">
      {/* Indicators Section */}
      <div className="space-y-3">
        <h4 className="text-sm font-semibold text-white">Indicators</h4>
        <p className="text-xs text-slate-400">
          Attach indicators to this strategy to enable signal generation and rule evaluation.
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
          ActionButton={ActionButton}
        />
      </div>
    </div>
  )
}
