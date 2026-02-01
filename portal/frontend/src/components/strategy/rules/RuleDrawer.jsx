import React, { useMemo, useRef } from 'react'
import { Dialog } from '@headlessui/react'
import { Trash2, X } from 'lucide-react'
import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import { Button } from '../../ui'
import useRuleForm from '../../../hooks/strategy/useRuleForm.js'
import { ConditionRowBuilder } from '../conditions/ConditionRowBuilder.jsx'
import { buildRuleConditionSummary, buildRuleDefaultName } from './ruleUtils.js'

export const RuleDrawer = ({
  open,
  indicators = [],
  ensureIndicatorMeta,
  initialValues,
  onSubmit,
  onCancel,
  submitting,
}) => {
  const initialFocusRef = useRef(null)
  const {
    form,
    indicatorMap,
    canSubmit,
    handleSubmit,
    handleFieldChange,
    addCondition,
    removeCondition,
    handleConditionIndicatorChange,
    handleConditionRuleChange,
    handleConditionDirectionChange,
  } = useRuleForm({
    open,
    indicators,
    ensureIndicatorMeta,
    initialValues,
    onSubmit,
    getDefaultName: (draft, map) => buildRuleDefaultName({
      action: draft.action,
      conditions: draft.conditions,
      match: draft.match,
      indicatorLookup: map,
    }),
  })

  const conditionSummary = useMemo(
    () => buildRuleConditionSummary({
      conditions: form.conditions,
      match: form.match,
      indicatorLookup: indicatorMap,
      limit: 3,
    }),
    [form.conditions, form.match, indicatorMap],
  )

  if (!open) return null

  const defaultName = buildRuleDefaultName({
    action: form.action,
    conditions: form.conditions,
    match: form.match,
    indicatorLookup: indicatorMap,
  })

  return (
    <Dialog open={open} onClose={onCancel} className="relative z-50" initialFocus={initialFocusRef}>
      <Dialog.Backdrop className="fixed inset-0 bg-black/40" />
      <div className="fixed inset-0 flex justify-end">
        <Dialog.Panel className="flex h-full w-full max-w-4xl flex-col border-l border-white/10 bg-[#111622] text-slate-100 shadow-2xl">
          <header className="flex items-start justify-between border-b border-white/10 px-5 py-4">
            <div>
              <Dialog.Title className="text-base font-semibold text-white">
                {initialValues ? 'Edit rule' : 'Create rule'}
              </Dialog.Title>
              <p className="mt-1 text-xs text-slate-400">
                Define how indicator signals convert into entries or exits.
              </p>
            </div>
            <button
              type="button"
              ref={initialFocusRef}
              onClick={onCancel}
              className="rounded p-1 text-slate-400 hover:text-white"
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </header>

          <form className="flex-1 space-y-5 overflow-y-auto px-5 py-4" onSubmit={handleSubmit}>
            <div className="rounded-lg border border-white/10 bg-black/30 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Condition summary</p>
              <p className="mt-1 text-sm text-slate-200">{conditionSummary}</p>
            </div>

            <div className="grid gap-4 lg:grid-cols-[1.2fr_1fr]">
              <div className="space-y-3">
                <details className="rounded-lg border border-white/10 bg-black/30 px-4 py-3">
                  <summary className="cursor-pointer text-xs font-semibold uppercase tracking-[0.18em] text-slate-300">
                    Optional details
                  </summary>
                  <div className="mt-3 grid gap-3">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Name</label>
                      <input
                        className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
                        value={form.name}
                        onChange={handleFieldChange('name')}
                        placeholder={defaultName}
                      />
                    </div>
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Description</label>
                      <textarea
                        className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
                        rows={3}
                        value={form.description}
                        onChange={handleFieldChange('description')}
                        placeholder="Optional"
                      />
                    </div>
                  </div>
                </details>
              </div>

              <div className="space-y-3 rounded-lg border border-white/10 bg-black/30 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Rule controls</p>
                <div className="grid gap-3">
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Action</label>
                    <select
                      className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
                      value={form.action}
                      onChange={handleFieldChange('action')}
                    >
                      <option value="buy">Buy</option>
                      <option value="sell">Sell</option>
                    </select>
                  </div>
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Match</label>
                    <select
                      className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
                      value={form.match}
                      onChange={handleFieldChange('match')}
                    >
                      <option value="all">All conditions</option>
                      <option value="any">Any condition</option>
                    </select>
                  </div>
                </div>
                <label className="inline-flex items-center gap-2 text-xs text-slate-300">
                  <input
                    type="checkbox"
                    className="h-4 w-4 rounded border-white/20 bg-black/40"
                    checked={form.enabled}
                    onChange={handleFieldChange('enabled')}
                  />
                  Enabled
                </label>
              </div>
            </div>

            <div className="space-y-3 rounded-lg border border-white/10 bg-black/30 px-4 py-3">
              <div className="flex items-center justify-between">
                <h4 className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Conditions
                </h4>
              </div>
              <ConditionRowBuilder
                rows={form.conditions}
                onAddRow={addCondition}
                addLabel="Add condition"
                gridClassName="md:grid-cols-[200px_minmax(0,1fr)_160px_40px]"
                columns={[
                  {
                    key: 'indicator',
                    label: 'Source',
                    render: (condition, index) => (
                      <DropdownSelect
                        value={condition.indicator_id}
                        onChange={handleConditionIndicatorChange(index)}
                        placeholder="Indicator"
                        options={indicators.map((indicator) => {
                          const hasSignals = Array.isArray(indicator.signal_rules)
                            && indicator.signal_rules.length > 0
                          const label = indicator.name || indicator.type
                          return {
                            value: indicator.id,
                            label: hasSignals ? label : `${label} (no signals)`,
                            disabled: !hasSignals,
                          }
                        })}
                        className="gap-0"
                      />
                    ),
                  },
                  {
                    key: 'signal',
                    label: 'Signal',
                    render: (condition, index) => {
                      const indicatorMeta = indicatorMap.get(condition.indicator_id)
                      const ruleOptions = Array.isArray(indicatorMeta?.signal_rules)
                        ? indicatorMeta.signal_rules
                        : []
                      return (
                        <DropdownSelect
                          value={condition.rule_id || condition.signal_type}
                          onChange={handleConditionRuleChange(index)}
                          placeholder="Signal"
                          options={ruleOptions.map((rule) => ({
                            value: rule.id,
                            label: rule.label || rule.signal_type,
                          }))}
                          className="gap-0"
                        />
                      )
                    },
                  },
                  {
                    key: 'direction',
                    label: 'Direction',
                    render: (condition, index) => {
                      const indicatorMeta = indicatorMap.get(condition.indicator_id)
                      const ruleOptions = Array.isArray(indicatorMeta?.signal_rules)
                        ? indicatorMeta.signal_rules
                        : []
                      const selectedRule = ruleOptions.find((rule) => rule.id === condition.rule_id)
                      const directionOptions = Array.isArray(selectedRule?.directions)
                        ? selectedRule.directions
                        : []
                      return directionOptions.length ? (
                        <DropdownSelect
                          value={condition.direction}
                          onChange={handleConditionDirectionChange(index)}
                          placeholder="Bias"
                          options={directionOptions.map((direction) => ({
                            value: direction.id,
                            label: direction.label || direction.id,
                          }))}
                          className="gap-0"
                        />
                      ) : (
                        <span className="text-xs text-slate-500">Any bias</span>
                      )
                    },
                  },
                  {
                    key: 'delete',
                    label: '',
                    render: (_, index) => (
                      <button
                        type="button"
                        className="inline-flex items-center justify-center rounded p-1 text-slate-500 hover:text-rose-400"
                        onClick={() => removeCondition(index)}
                        aria-label="Remove condition"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    ),
                  },
                ]}
              />
            </div>
          </form>

          <footer className="flex items-center justify-end gap-2 border-t border-white/10 px-5 py-3">
            <Button variant="ghost" onClick={onCancel}>Cancel</Button>
            <Button type="submit" disabled={submitting || !canSubmit}>
              {submitting ? 'Saving…' : 'Save rule'}
            </Button>
          </footer>
        </Dialog.Panel>
      </div>
    </Dialog>
  )
}
