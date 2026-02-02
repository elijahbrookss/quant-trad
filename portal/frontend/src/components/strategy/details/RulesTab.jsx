import React, { useMemo } from 'react'
import { Plus } from 'lucide-react'
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

  const rules = Array.isArray(strategy?.rules) ? strategy.rules : []
  const globalFilters = Array.isArray(strategy?.global_filters) ? strategy.global_filters : []

  if (!strategy) {
    return (
      <div className="flex h-32 items-center justify-center rounded-xl border border-dashed border-white/10 bg-black/20">
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-slate-500 border-t-transparent" />
          Loading decision logic...
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      {/* Signal Sources - compact section */}
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-sm font-semibold text-white">Signal Sources</h3>
            <p className="mt-0.5 text-xs text-slate-500">
              Attach indicators to use their signals in rules
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

      {/* Rules - primary section */}
      <section className="rounded-xl border border-white/10 bg-black/20 p-5">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <h3 className="text-base font-semibold text-white">Trading Rules</h3>
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
          Define when to buy or sell based on indicator signals
        </p>
        <div className="mt-5">
          <RuleList
            rules={rules}
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

      {/* Global Gates */}
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <FilterPanel
          title="Global Gates"
          description="Filter out weak setups after rules match"
          filters={globalFilters}
          onCreate={onCreateGlobalFilter}
          onUpdate={onUpdateGlobalFilter}
          onDelete={onDeleteGlobalFilter}
          ActionButton={ActionButton}
          emptyState="No global gates yet. Add gates to filter by market regime, candle patterns, or other conditions."
        />
      </section>
    </div>
  )
}
