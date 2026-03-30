import React, { useMemo, useRef } from 'react'
import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from '@headlessui/react'
import { CopyPlus, Plus, Trash2, X } from 'lucide-react'

import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import { Button } from '../../ui'
import useRuleForm from '../../../hooks/strategy/useRuleForm.js'
import { getAuthorableOutputsByType, getIndicatorOutputsByType } from '../../../utils/indicatorOutputs.js'
import { buildRuleConditionSummary, buildRuleDefaultName } from './ruleUtils.js'

const outputOptionsForType = (indicator, outputType, options = {}) => (
  getAuthorableOutputsByType(indicator, outputType, options).map((entry) => ({
    value: entry.name,
    label: entry.label || entry.name,
    description: entry?.enabled === false ? 'Disabled in indicator settings; kept for existing rules.' : '',
    badge: entry?.enabled === false ? 'Disabled' : undefined,
    meta: entry,
  }))
)

const eventOptions = (indicator, outputName) => {
  const outputs = getIndicatorOutputsByType(indicator, 'signal')
  const output = outputs.find((entry) => entry?.name === outputName)
  return Array.isArray(output?.event_keys)
    ? output.event_keys.map((entry) => ({ value: entry, label: entry }))
    : []
}

const contextFieldOptions = (indicator, outputName) => {
  const outputs = getIndicatorOutputsByType(indicator, 'context')
  const output = outputs.find((entry) => entry?.name === outputName)
  const dynamicFields = Array.isArray(output?.fields)
    ? output.fields
      .filter((entry) => entry && entry !== 'state')
      .map((entry) => ({ value: entry, label: entry }))
    : []
  return [{ value: 'state', label: 'State' }, ...dynamicFields]
}

const contextValueOptions = (indicator, outputName, field) => {
  if (field !== 'state') return []
  const outputs = getIndicatorOutputsByType(indicator, 'context')
  const output = outputs.find((entry) => entry?.name === outputName)
  return Array.isArray(output?.state_keys)
    ? output.state_keys.map((entry) => ({ value: entry, label: entry }))
    : []
}

const metricFieldOptions = (indicator, outputName) => {
  const outputs = getIndicatorOutputsByType(indicator, 'metric')
  const output = outputs.find((entry) => entry?.name === outputName)
  return Array.isArray(output?.fields)
    ? output.fields.map((entry) => ({ value: entry, label: entry }))
    : []
}

const indicatorOptionsForGuards = (indicators = [], outputType) => (
  indicators
    .filter((indicator) => outputOptionsForType(indicator, outputType).length > 0)
    .map((indicator) => ({
      value: indicator.id,
      label: indicator.name || indicator.type || indicator.id,
    }))
)

const RULE_DRAWER_FORM_ID = 'strategy-rule-drawer-form'

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
    signalIndicators,
    guardFieldFilters,
    canSubmit,
    addGuard,
    duplicateGuard,
    removeGuard,
    buildPayload,
    handleFieldChange,
    handleTriggerIndicatorChange,
    handleTriggerOutputChange,
    handleTriggerEventChange,
    handleGuardTypeChange,
    handleGuardIndicatorChange,
    handleGuardOutputChange,
    handleGuardFieldChange,
    handleGuardFieldFilterChange,
    clearGuardFieldFilter,
  } = useRuleForm({
    open,
    indicators,
    ensureIndicatorMeta,
    initialValues,
    getDefaultName: ({ intent, trigger, guards, indicatorLookup }) => buildRuleDefaultName({
      intent,
      trigger,
      guards,
      indicatorLookup,
    }),
  })

  const triggerIndicator = indicatorMap.get(form.trigger.indicator_id)
  const triggerOutputOptions = outputOptionsForType(triggerIndicator, 'signal', {
    selectedOutputName: form.trigger.output_name,
  })
  const triggerEventOptions = eventOptions(triggerIndicator, form.trigger.output_name)
  const conditionSummary = useMemo(
    () => buildRuleConditionSummary({
      rule: {
        trigger: {
          type: 'signal_match',
          indicator_id: form.trigger.indicator_id,
          output_name: form.trigger.output_name,
          event_key: form.trigger.event_key,
        },
        guards: (form.guards || []).filter(Boolean).filter((entry) => entry?.indicator_id),
      },
      indicatorLookup: indicatorMap,
    }),
    [form.trigger, form.guards, indicatorMap],
  )

  if (!open) return null

  const handleSave = async (event) => {
    event.preventDefault()
    const payload = buildPayload()
    if (!payload) return
    await onSubmit(payload)
  }

  return (
    <Dialog open={open} onClose={onCancel} className="relative z-50" initialFocus={initialFocusRef}>
      <DialogBackdrop className="fixed inset-0 bg-black/60 backdrop-blur-sm" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="flex max-h-[94vh] w-full max-w-6xl flex-col overflow-hidden rounded border border-[#172033] bg-[linear-gradient(180deg,#101827_0%,#0a1220_100%)] text-slate-100 shadow-2xl">
          <header className="flex items-start justify-between border-b border-white/10 px-6 py-5">
            <div>
              <DialogTitle className="text-lg font-semibold text-white">
                {initialValues ? 'Edit strategy rule' : 'Create strategy rule'}
              </DialogTitle>
              <p className="mt-1 text-sm text-slate-400">
                One signal trigger is required. Add optional context or metric guards.
              </p>
            </div>
            <button
              type="button"
              ref={initialFocusRef}
              onClick={onCancel}
              className="rounded-full border border-white/10 p-2 text-slate-400 transition hover:border-white/20 hover:text-white"
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </header>

          <form
            id={RULE_DRAWER_FORM_ID}
            className="flex-1 overflow-y-auto px-6 py-5"
            onSubmit={handleSave}
          >
            <div className="grid gap-5 xl:grid-cols-[1.6fr_0.7fr]">
              <section className="space-y-6">
                <div>
                  <p className="mb-3 text-[10px] uppercase tracking-[0.22em] text-slate-500">Trigger</p>
                  <div className="space-y-2">
                    <DropdownSelect
                      value={form.trigger.indicator_id}
                      onChange={handleTriggerIndicatorChange}
                      placeholder="Select indicator"
                      options={signalIndicators.map((ind) => ({
                        value: ind.id,
                        label: ind.name || ind.type || ind.id,
                      }))}
                    />
                    <DropdownSelect
                      value={form.trigger.output_name}
                      onChange={handleTriggerOutputChange}
                      placeholder="Select signal output"
                      options={triggerOutputOptions.map((entry) => ({ value: entry.value, label: entry.label }))}
                      disabled={!form.trigger.indicator_id}
                    />
                    <DropdownSelect
                      value={form.trigger.event_key}
                      onChange={handleTriggerEventChange}
                      placeholder="Select event"
                      options={triggerEventOptions}
                      disabled={!form.trigger.output_name}
                    />
                  </div>
                </div>

                <div className="border-t border-white/[0.06]" />

                <div>
                  <div className="mb-3 flex items-center justify-between">
                    <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">
                      Conditions
                      <span className="ml-2 normal-case tracking-normal text-slate-600">optional</span>
                    </p>
                    <button
                      type="button"
                      onClick={addGuard}
                      className="flex items-center gap-1 text-xs text-slate-400 transition hover:text-white"
                    >
                      <Plus className="h-3 w-3" /> Add
                    </button>
                  </div>
                  <div className="max-h-72 space-y-2 overflow-y-auto">
                    {(form.guards || []).length === 0 ? (
                      <p className="text-xs italic text-slate-600">No conditions — rule fires on trigger alone.</p>
                    ) : (
                      form.guards.map((guard, index) => {
                        const indicator = indicatorMap.get(guard.indicator_id)
                        const outputType = guard.type === 'context_match' ? 'context' : 'metric'
                        const indicatorOptions = indicatorOptionsForGuards(indicators, outputType)
                        const outputOptions = outputOptionsForType(indicator, outputType)
                        const contextOptions = contextFieldOptions(indicator, guard.output_name)
                        const metricOptions = metricFieldOptions(indicator, guard.output_name)
                        const valueOptions = contextValueOptions(indicator, guard.output_name, guard.field || 'state')
                        const selectedValues = Array.isArray(guard.value_text)
                          ? guard.value_text.filter(Boolean)
                          : [guard.value_text].filter(Boolean)
                        const availableValueOptions = valueOptions.filter(
                          (option) => !selectedValues.includes(option.value),
                        )
                        const fieldFilter = guardFieldFilters[index] || ''
                        const normalizedFieldFilter = fieldFilter.trim().toLowerCase()
                        const filteredContextOptions = contextOptions.filter((option) => (
                          !normalizedFieldFilter
                          || String(option.label || option.value || '').toLowerCase().includes(normalizedFieldFilter)
                        ))
                        const selectedContextField = guard.field || 'state'
                        const selectedContextOption = contextOptions.find(
                          (option) => option.value === selectedContextField,
                        )
                        return (
                          <div
                            key={`guard-${index}`}
                            className="flex flex-wrap items-start gap-2 rounded border border-white/[0.06] bg-white/[0.02] px-3 py-2"
                          >
                            <select
                              value={guard.type}
                              onChange={(e) => handleGuardTypeChange(index, e.target.value)}
                              className="shrink-0 rounded border border-white/10 bg-black/40 px-2 py-1.5 text-xs text-slate-300 focus:outline-none"
                            >
                              <option value="context_match">ctx</option>
                              <option value="metric_match">metric</option>
                            </select>
                            <div className="w-36 shrink-0">
                              <DropdownSelect
                                value={guard.indicator_id}
                                onChange={(v) => handleGuardIndicatorChange(index, v)}
                                placeholder="Indicator"
                                options={indicatorOptions}
                              />
                            </div>
                            <div className="w-40 shrink-0">
                              <DropdownSelect
                                value={guard.output_name}
                                onChange={(v) => handleGuardOutputChange(index, v)}
                                placeholder="Output"
                                options={outputOptions.map((e) => ({ value: e.value, label: e.label }))}
                                disabled={!guard.indicator_id}
                              />
                            </div>
                            {guard.type === 'context_match' && (
                              <>
                                <div className="min-w-[14rem] flex-1">
                                  <input
                                    className="w-full rounded border border-white/10 bg-black/40 px-2 py-1.5 text-xs text-slate-200 focus:outline-none"
                                    value={fieldFilter}
                                    onChange={(e) => handleGuardFieldFilterChange(index, e.target.value)}
                                    placeholder="Filter fields..."
                                    disabled={!guard.output_name}
                                  />
                                  {selectedContextOption ? (
                                    <div className="mt-2 flex items-center gap-2">
                                      <span className="rounded border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-200">
                                        {selectedContextOption.label}
                                      </span>
                                      <button
                                        type="button"
                                        className="text-[11px] text-slate-500 transition hover:text-white"
                                        onClick={() => {
                                          handleGuardFieldChange(index, 'field', '')
                                          clearGuardFieldFilter(index)
                                        }}
                                      >
                                        Clear
                                      </button>
                                    </div>
                                  ) : null}
                                  <div className="mt-2 max-h-40 overflow-y-auto rounded border border-white/[0.06] bg-black/20">
                                    {filteredContextOptions.length > 0 ? filteredContextOptions.map((option) => (
                                      <button
                                        key={`${guard.indicator_id || 'guard'}-${option.value}`}
                                        type="button"
                                        className={`flex w-full items-center justify-between px-2 py-1.5 text-left text-xs transition ${
                                          option.value === selectedContextField
                                            ? 'bg-white/[0.06] text-white'
                                            : 'text-slate-300 hover:bg-white/[0.03] hover:text-white'
                                        }`}
                                        onClick={() => {
                                          handleGuardFieldChange(index, 'field', option.value)
                                          clearGuardFieldFilter(index)
                                        }}
                                      >
                                        <span>{option.label}</span>
                                      </button>
                                    )) : (
                                      <div className="px-2 py-2 text-xs text-slate-500">No matching fields</div>
                                    )}
                                  </div>
                                </div>
                                <span className="self-center text-xs text-slate-500">=</span>
                                <div className="min-w-[14rem] flex-1">
                                  {valueOptions.length > 0 ? (
                                    <div className="space-y-2">
                                      <div className="flex flex-wrap gap-1.5">
                                        {selectedValues.map((selectedValue) => (
                                          <span
                                            key={`${guard.output_name || 'value'}-${selectedValue}`}
                                            className="inline-flex items-center gap-1 rounded border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-200"
                                          >
                                            {selectedValue}
                                            <button
                                              type="button"
                                              className="text-slate-500 transition hover:text-rose-400"
                                              onClick={() => handleGuardFieldChange(
                                                index,
                                                'value_text',
                                                selectedValues.filter((entry) => entry !== selectedValue),
                                              )}
                                            >
                                              ×
                                            </button>
                                          </span>
                                        ))}
                                      </div>
                                      <select
                                        defaultValue=""
                                        onChange={(e) => {
                                          const next = e.target.value
                                          if (!next) return
                                          handleGuardFieldChange(index, 'value_text', [...selectedValues, next])
                                          e.target.value = ''
                                        }}
                                        className="w-full rounded border border-white/10 bg-black/40 px-2 py-1.5 text-xs text-slate-200 focus:outline-none"
                                        disabled={!guard.output_name || availableValueOptions.length === 0}
                                      >
                                        <option value="">
                                          {availableValueOptions.length === 0 ? 'All values selected' : 'Add value'}
                                        </option>
                                        {availableValueOptions.map((option) => (
                                          <option key={`${guard.output_name || 'value-option'}-${option.value}`} value={option.value}>
                                            {option.label}
                                          </option>
                                        ))}
                                      </select>
                                    </div>
                                  ) : (
                                    <input
                                      className="w-full rounded border border-white/10 bg-black/40 px-2 py-1.5 text-xs text-slate-200 focus:outline-none"
                                      value={Array.isArray(guard.value_text) ? guard.value_text[0] || '' : guard.value_text || ''}
                                      onChange={(e) => handleGuardFieldChange(index, 'value_text', e.target.value)}
                                      placeholder="Value"
                                    />
                                  )}
                                </div>
                              </>
                            )}
                            {guard.type === 'metric_match' && (
                              <>
                                <div className="w-32 shrink-0">
                                  <DropdownSelect
                                    value={guard.field}
                                    onChange={(v) => handleGuardFieldChange(index, 'field', v)}
                                    placeholder="Field"
                                    options={metricOptions}
                                    disabled={!guard.output_name}
                                  />
                                </div>
                                <select
                                  value={guard.operator}
                                  onChange={(e) => handleGuardFieldChange(index, 'operator', e.target.value)}
                                  className="shrink-0 rounded border border-white/10 bg-black/40 px-2 py-1.5 text-xs text-slate-300 focus:outline-none"
                                >
                                  <option value=">">&gt;</option>
                                  <option value=">=">&gt;=</option>
                                  <option value="<">&lt;</option>
                                  <option value="<=">&lt;=</option>
                                  <option value="==">==</option>
                                  <option value="!=">!=</option>
                                </select>
                                <input
                                  className="w-24 shrink-0 rounded border border-white/10 bg-black/40 px-2 py-1.5 text-xs text-slate-200 focus:outline-none"
                                  value={guard.value}
                                  onChange={(e) => handleGuardFieldChange(index, 'value', e.target.value)}
                                  placeholder="0.0"
                                  inputMode="decimal"
                                />
                              </>
                            )}
                            <button
                              type="button"
                              onClick={() => duplicateGuard(index)}
                              className="self-center rounded p-1 text-slate-600 transition hover:text-white"
                              aria-label="Duplicate condition"
                            >
                              <CopyPlus className="h-3.5 w-3.5" />
                            </button>
                            <button
                              type="button"
                              onClick={() => removeGuard(index)}
                              className="self-center rounded p-1 text-slate-600 transition hover:text-rose-400"
                              aria-label="Remove condition"
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        )
                      })
                    )}
                  </div>
                </div>

                <div className="border-t border-white/[0.06]" />

                <div>
                  <p className="mb-3 text-[10px] uppercase tracking-[0.22em] text-slate-500">Intent</p>
                  <div className="flex flex-wrap items-center gap-3">
                    <select
                      className="rounded border border-white/10 bg-black/40 px-3 py-1.5 text-sm text-slate-200 focus:outline-none"
                      value={form.intent}
                      onChange={handleFieldChange('intent')}
                    >
                      <option value="enter_long">Long entry</option>
                      <option value="enter_short">Short entry</option>
                    </select>
                  </div>
                </div>

                <div className="border-t border-white/[0.06]" />

                <div>
                  <p className="mb-2 text-[10px] uppercase tracking-[0.22em] text-slate-500">Flow summary</p>
                  <p className="text-sm text-slate-300">{conditionSummary || 'No trigger selected'}</p>
                </div>
              </section>

              <aside className="space-y-4">
                <div className="rounded border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Rule details</p>
                  <div className="mt-3 space-y-3">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Name</label>
                      <input
                        className="mt-2 w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
                        value={form.name}
                        onChange={handleFieldChange('name')}
                        placeholder={buildRuleDefaultName({
                          intent: form.intent,
                          trigger: {
                            type: 'signal_match',
                            indicator_id: form.trigger.indicator_id,
                            output_name: form.trigger.output_name,
                            event_key: form.trigger.event_key,
                          },
                          guards: form.guards,
                          indicatorLookup: indicatorMap,
                        })}
                      />
                    </div>
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Description</label>
                      <textarea
                        className="mt-2 w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
                        rows={5}
                        value={form.description}
                        onChange={handleFieldChange('description')}
                        placeholder="Optional note about the setup"
                      />
                    </div>
                  </div>
                </div>

                <div className="rounded border border-white/10 bg-black/20 p-4 text-xs text-slate-400">
                  <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Rule law</p>
                  <ul className="mt-3 space-y-2">
                    <li>One signal trigger is required.</li>
                    <li>Context and metric guards are optional.</li>
                    <li>Indicator signal toggles only affect authoring availability and previews. Runtime follows the signals referenced by your strategy rules.</li>
                  </ul>
                </div>
              </aside>
            </div>
          </form>

          <footer className="flex items-center justify-between border-t border-white/10 px-6 py-4">
            <p className="text-xs text-slate-500">
              Signal required. Guards optional.
            </p>
            <div className="flex items-center gap-3">
              <Button type="button" variant="ghost" onClick={onCancel}>
                Cancel
              </Button>
              <Button form={RULE_DRAWER_FORM_ID} type="submit" disabled={!canSubmit || submitting}>
                {submitting ? 'Saving...' : initialValues ? 'Save rule' : 'Create rule'}
              </Button>
            </div>
          </footer>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
