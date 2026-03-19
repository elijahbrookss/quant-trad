import React, { useMemo, useRef } from 'react'
import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from '@headlessui/react'
import { ArrowRight, Plus, Trash2, X } from 'lucide-react'

import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import { Button } from '../../ui'
import useRuleForm from '../../../hooks/strategy/useRuleForm.js'
import { buildRuleConditionSummary, buildRuleDefaultName } from './ruleUtils.js'

const outputOptionsForType = (indicator, outputType) => {
  const outputs = Array.isArray(indicator?.typed_outputs) ? indicator.typed_outputs : []
  return outputs
    .filter((entry) => entry?.type === outputType)
    .map((entry) => ({
      value: entry.name,
      label: entry.label || entry.name,
      meta: entry,
    }))
}

const eventOptions = (indicator, outputName) => {
  const outputs = Array.isArray(indicator?.typed_outputs) ? indicator.typed_outputs : []
  const output = outputs.find((entry) => entry?.name === outputName)
  return Array.isArray(output?.event_keys)
    ? output.event_keys.map((entry) => ({ value: entry, label: entry }))
    : []
}

const contextStateOptions = (indicator, outputName) => {
  const outputs = Array.isArray(indicator?.typed_outputs) ? indicator.typed_outputs : []
  const output = outputs.find((entry) => entry?.name === outputName)
  return Array.isArray(output?.state_keys)
    ? output.state_keys.map((entry) => ({ value: entry, label: entry }))
    : []
}

const metricFieldOptions = (indicator, outputName) => {
  const outputs = Array.isArray(indicator?.typed_outputs) ? indicator.typed_outputs : []
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
    canSubmit,
    addGuard,
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
  } = useRuleForm({
    open,
    indicators,
    ensureIndicatorMeta,
    initialValues,
    getDefaultName: ({ action, trigger, guards, indicatorLookup }) => buildRuleDefaultName({
      action,
      trigger,
      guards,
      indicatorLookup,
    }),
  })

  const triggerIndicator = indicatorMap.get(form.trigger.indicator_id)
  const triggerOutputOptions = outputOptionsForType(triggerIndicator, 'signal')
  const triggerEventOptions = eventOptions(triggerIndicator, form.trigger.output_name)
  const conditionSummary = useMemo(
    () => buildRuleConditionSummary({
      rule: {
        when: {
          type: 'all',
          conditions: [
            {
              type: 'signal_match',
              indicator_id: form.trigger.indicator_id,
              output_name: form.trigger.output_name,
              event_key: form.trigger.event_key,
            },
            ...(form.guards || []).filter(Boolean),
          ].filter((entry) => entry?.indicator_id),
        },
      },
      indicatorLookup: indicatorMap,
      limit: 2,
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
        <DialogPanel className="flex max-h-[94vh] w-full max-w-6xl flex-col overflow-hidden rounded-[28px] border border-[#172033] bg-[linear-gradient(180deg,#101827_0%,#0a1220_100%)] text-slate-100 shadow-2xl">
          <header className="flex items-start justify-between border-b border-white/10 px-6 py-5">
            <div>
              <DialogTitle className="text-lg font-semibold text-white">
                {initialValues ? 'Edit strategy flow' : 'Create strategy flow'}
              </DialogTitle>
              <p className="mt-1 text-sm text-slate-400">
                One signal trigger is required. Add up to two optional context or metric guards.
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
              <section className="space-y-5">
                <div className="rounded-2xl border border-[#20304d] bg-[#0e1727] p-4">
                  <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-300/70">Flow summary</p>
                  <p className="mt-2 text-sm text-slate-200">{conditionSummary}</p>
                </div>

                <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_48px_minmax(0,1fr)_48px_minmax(0,1fr)]">
                  <div className="rounded-2xl border border-emerald-500/30 bg-emerald-500/10 p-4">
                    <p className="text-[11px] uppercase tracking-[0.22em] text-emerald-200/80">Signal Trigger</p>
                    <div className="mt-4 space-y-3">
                      <DropdownSelect
                        value={form.trigger.indicator_id}
                        onChange={handleTriggerIndicatorChange}
                        placeholder="Select signal indicator"
                        options={signalIndicators.map((indicator) => ({
                          value: indicator.id,
                          label: indicator.name || indicator.type || indicator.id,
                        }))}
                        className="gap-0"
                      />
                      <DropdownSelect
                        value={form.trigger.output_name}
                        onChange={handleTriggerOutputChange}
                        placeholder="Select signal output"
                        options={triggerOutputOptions.map((entry) => ({
                          value: entry.value,
                          label: entry.label,
                        }))}
                        className="gap-0"
                        disabled={!form.trigger.indicator_id}
                      />
                      <DropdownSelect
                        value={form.trigger.event_key}
                        onChange={handleTriggerEventChange}
                        placeholder="Select event"
                        options={triggerEventOptions}
                        className="gap-0"
                        disabled={!form.trigger.output_name}
                      />
                    </div>
                  </div>

                  <div className="flex items-center justify-center text-slate-500">
                    <ArrowRight className="h-5 w-5" />
                  </div>

                  <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 p-4">
                    <div className="flex items-center justify-between">
                      <p className="text-[11px] uppercase tracking-[0.22em] text-amber-200/80">Guards</p>
                      <button
                        type="button"
                        onClick={addGuard}
                        disabled={(form.guards || []).length >= 2}
                        className="inline-flex items-center gap-1 rounded-full border border-amber-300/20 px-2 py-1 text-[11px] text-amber-100 transition hover:border-amber-300/40 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        <Plus className="h-3 w-3" />
                        Add
                      </button>
                    </div>
                    <div className="mt-4 space-y-3">
                      {(form.guards || []).length === 0 ? (
                        <div className="rounded-xl border border-dashed border-amber-200/20 px-3 py-4 text-xs text-amber-100/80">
                          Optional. Add up to two context or metric checks.
                        </div>
                      ) : (
                        form.guards.map((guard, index) => {
                          const indicator = indicatorMap.get(guard.indicator_id)
                          const outputType = guard.type === 'context_match' ? 'context' : 'metric'
                          const indicatorOptions = indicatorOptionsForGuards(indicators, outputType)
                          const outputOptions = outputOptionsForType(indicator, outputType)
                          const contextOptions = contextStateOptions(indicator, guard.output_name)
                          const metricOptions = metricFieldOptions(indicator, guard.output_name)
                          return (
                            <div key={`guard-${index}`} className="rounded-xl border border-amber-200/20 bg-black/20 p-3">
                              <div className="flex items-center justify-between">
                                <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-amber-100/80">
                                  Guard {index + 1}
                                </span>
                                <button
                                  type="button"
                                  className="rounded p-1 text-amber-100/70 transition hover:bg-white/5 hover:text-white"
                                  onClick={() => removeGuard(index)}
                                  aria-label="Remove guard"
                                >
                                  <Trash2 className="h-4 w-4" />
                                </button>
                              </div>
                              <div className="mt-3 space-y-3">
                                <DropdownSelect
                                  value={guard.type}
                                  onChange={(value) => handleGuardTypeChange(index, value)}
                                  placeholder="Guard type"
                                  options={[
                                    { value: 'context_match', label: 'Context' },
                                    { value: 'metric_match', label: 'Metric' },
                                  ]}
                                  className="gap-0"
                                />
                                <DropdownSelect
                                  value={guard.indicator_id}
                                  onChange={(value) => handleGuardIndicatorChange(index, value)}
                                  placeholder={`Select ${outputType} indicator`}
                                  options={indicatorOptions}
                                  className="gap-0"
                                />
                                <DropdownSelect
                                  value={guard.output_name}
                                  onChange={(value) => handleGuardOutputChange(index, value)}
                                  placeholder={`Select ${outputType} output`}
                                  options={outputOptions.map((entry) => ({ value: entry.value, label: entry.label }))}
                                  className="gap-0"
                                  disabled={!guard.indicator_id}
                                />
                                {guard.type === 'context_match' ? (
                                  <DropdownSelect
                                    value={guard.state_key}
                                    onChange={(value) => handleGuardFieldChange(index, 'state_key', value)}
                                    placeholder="Select state"
                                    options={contextOptions}
                                    className="gap-0"
                                    disabled={!guard.output_name}
                                  />
                                ) : (
                                  <div className="grid gap-2 md:grid-cols-[1.2fr_100px_120px]">
                                    <DropdownSelect
                                      value={guard.field}
                                      onChange={(value) => handleGuardFieldChange(index, 'field', value)}
                                      placeholder="Metric field"
                                      options={metricOptions}
                                      className="gap-0"
                                      disabled={!guard.output_name}
                                    />
                                    <select
                                      className="rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
                                      value={guard.operator}
                                      onChange={(event) => handleGuardFieldChange(index, 'operator', event.target.value)}
                                    >
                                      <option value=">">&gt;</option>
                                      <option value=">=">&gt;=</option>
                                      <option value="<">&lt;</option>
                                      <option value="<=">&lt;=</option>
                                      <option value="==">==</option>
                                      <option value="!=">!=</option>
                                    </select>
                                    <input
                                      className="rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
                                      value={guard.value}
                                      onChange={(event) => handleGuardFieldChange(index, 'value', event.target.value)}
                                      placeholder="Value"
                                      inputMode="decimal"
                                    />
                                  </div>
                                )}
                              </div>
                            </div>
                          )
                        })
                      )}
                    </div>
                  </div>

                  <div className="flex items-center justify-center text-slate-500">
                    <ArrowRight className="h-5 w-5" />
                  </div>

                  <div className="rounded-2xl border border-fuchsia-500/30 bg-fuchsia-500/10 p-4">
                    <p className="text-[11px] uppercase tracking-[0.22em] text-fuchsia-200/80">Action</p>
                    <div className="mt-4 space-y-3">
                      <select
                        className="w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
                        value={form.action}
                        onChange={handleFieldChange('action')}
                      >
                        <option value="buy">Buy</option>
                        <option value="sell">Sell</option>
                      </select>
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
                </div>
              </section>

              <aside className="space-y-4">
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <p className="text-[11px] uppercase tracking-[0.22em] text-slate-500">Rule details</p>
                  <div className="mt-3 space-y-3">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Name</label>
                      <input
                        className="mt-2 w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
                        value={form.name}
                        onChange={handleFieldChange('name')}
                        placeholder={buildRuleDefaultName({
                          action: form.action,
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

                <div className="rounded-2xl border border-white/10 bg-black/20 p-4 text-xs text-slate-400">
                  <p className="font-semibold uppercase tracking-[0.18em] text-slate-300">Rule law</p>
                  <ul className="mt-3 space-y-2">
                    <li>One signal trigger is required.</li>
                    <li>Context and metric guards are optional.</li>
                    <li>At most two guards are supported in v1.</li>
                    <li>Strategies evaluate typed outputs only.</li>
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
                {submitting ? 'Saving…' : initialValues ? 'Save flow' : 'Create flow'}
              </Button>
            </div>
          </footer>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
