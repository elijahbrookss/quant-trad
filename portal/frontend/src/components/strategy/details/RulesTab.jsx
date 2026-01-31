import React, { useMemo } from 'react'
import { Button } from '../../ui'
import { AttachedIndicators } from '../indicators'
import { FilterPanel } from '../filters/FilterPanel.jsx'
import { RuleList } from '../rules'
import { findBrokenRuleIndicators } from '../utils/indicatorUsage.js'

/**
 * Decision Logic tab combining indicator attachment, rule management, and filter gates.
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
  onCreateGlobalFilter,
  onUpdateGlobalFilter,
  onDeleteGlobalFilter,
  onCreateRuleFilter,
  onUpdateRuleFilter,
  onDeleteRuleFilter,
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
      <section className="space-y-3">
        <div>
          <h4 className="text-sm font-semibold text-white">Signal Sources</h4>
          <p className="text-xs text-slate-400">
            Attach indicators to use their signals in rules. Editing belongs in QuantLab.
          </p>
        </div>
        <AttachedIndicators
          strategy={strategy}
          attached={attachedIndicators}
          availableIndicators={availableIndicators}
          onAttach={onAttachIndicator}
          onDetach={onDetachIndicator}
          DropdownSelect={DropdownSelect}
          ActionButton={ActionButton}
        />
      </section>

      <div className="h-px bg-white/10" />

      <section className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h4 className="text-sm font-semibold text-white">Rule Logic</h4>
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
          onCreateFilter={onCreateRuleFilter}
          onUpdateFilter={onUpdateRuleFilter}
          onDeleteFilter={onDeleteRuleFilter}
          indicatorLookup={indicatorLookup}
          brokenIndicatorIds={brokenIndicatorIds}
          ActionButton={ActionButton}
        />
      </section>

      <div className="h-px bg-white/10" />

      <section className="space-y-3">
        <FilterPanel
          title="Filter Logic"
          description="Apply global filters that gate every matched rule using candle and regime stats."
          filters={Array.isArray(strategy.global_filters) ? strategy.global_filters : []}
          onCreate={onCreateGlobalFilter}
          onUpdate={onUpdateGlobalFilter}
          onDelete={onDeleteGlobalFilter}
          ActionButton={ActionButton}
          emptyState="No global filters yet. Add candle or regime gates to stop weak setups."
        />
      </section>
    </div>
  )
}
