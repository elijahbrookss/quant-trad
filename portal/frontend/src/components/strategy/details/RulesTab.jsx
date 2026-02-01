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
  onDuplicateRule,
  onCreateGlobalFilter,
  onUpdateGlobalFilter,
  onDeleteGlobalFilter,
  onCreateRuleFilter,
  onUpdateRuleFilter,
  onDeleteRuleFilter,
  indicatorLookup,
  // These components need to be passed in
  DropdownSelect,
  ActionButton,
}) => {
  const brokenIndicatorIds = useMemo(
    () => findBrokenRuleIndicators(attachedIndicators.map((ind) => ind.id), strategy?.rules),
    [attachedIndicators, strategy?.rules],
  )

  if (!strategy) {
    return (
      <div className="rounded-xl border border-dashed border-white/10 bg-black/30 p-6 text-sm text-slate-400">
        Loading decision logic…
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <section className="rounded-xl border border-white/10 bg-black/30 p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Section A</p>
            <h4 className="text-sm font-semibold text-white">Signal Sources</h4>
            <p className="mt-1 text-xs text-slate-400">
              Attach indicators to use their signals in rules. Editing belongs in QuantLab.
            </p>
          </div>
        </div>
        <div className="mt-4">
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
      </section>

      <section className="rounded-xl border border-white/10 bg-black/30 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Section B</p>
            <h4 className="text-sm font-semibold text-white">Rules</h4>
            <p className="mt-1 text-xs text-slate-400">
              Define entry and exit rules from indicator signals.
            </p>
          </div>
          <Button onClick={onAddRule}>New rule</Button>
        </div>
        <div className="mt-4">
          <RuleList
            rules={Array.isArray(strategy.rules) ? strategy.rules : []}
            onEdit={onEditRule}
            onDelete={onDeleteRule}
            onDuplicate={onDuplicateRule}
            onCreateFilter={onCreateRuleFilter}
            onUpdateFilter={onUpdateRuleFilter}
            onDeleteFilter={onDeleteRuleFilter}
            indicatorLookup={indicatorLookup}
            brokenIndicatorIds={brokenIndicatorIds}
            ActionButton={ActionButton}
            onAddRule={onAddRule}
          />
        </div>
      </section>

      <section className="rounded-xl border border-white/10 bg-black/30 p-4">
        <FilterPanel
          title="Global Gates"
          description="Apply gates after rule matches to avoid weak setups."
          filters={Array.isArray(strategy.global_filters) ? strategy.global_filters : []}
          onCreate={onCreateGlobalFilter}
          onUpdate={onUpdateGlobalFilter}
          onDelete={onDeleteGlobalFilter}
          ActionButton={ActionButton}
          emptyState="No global gates yet. Add candle or regime gates to stop weak setups."
        />
      </section>
    </div>
  )
}
