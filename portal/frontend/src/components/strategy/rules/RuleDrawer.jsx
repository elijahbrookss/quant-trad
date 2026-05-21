import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from '@headlessui/react'
import { ChevronLeft, ChevronRight, CopyPlus, Plus, Trash2, X } from 'lucide-react'

import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import ActionButton from '../ui/ActionButton.jsx'
import useRuleForm from '../../../hooks/strategy/useRuleForm.js'
import { getAuthorableOutputsByType, getIndicatorOutputsByType } from '../../../utils/indicatorOutputs.js'
import { buildRuleConditionSummary, buildRuleDefaultName } from './ruleUtils.js'

const STEP_DEFS = [
  {
    id: 'setup',
    title: 'Setup',
    description: 'Choose the trigger and core rule settings.',
  },
  {
    id: 'conditions',
    title: 'Conditions',
    description: 'Add optional context, metric, or signal guards.',
  },
  {
    id: 'review',
    title: 'Review',
    description: 'Confirm the final rule before saving.',
  },
]

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

const panelClassName = 'rounded-2xl border border-white/10 bg-black/30 px-4 py-4'
const fieldLabelClassName = 'text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-400'
const textInputClassName = 'mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none'

const describeGuard = (guard, indicatorMap) => {
  if (!guard) return 'Incomplete condition'
  const indicator = indicatorMap.get(guard.indicator_id)
  const indicatorLabel = indicator?.name || indicator?.type || guard.indicator_id || 'Indicator'
  const outputLabel = guard.output_name || 'output'

  if (guard.type === 'context_match') {
    const values = Array.isArray(guard.value) ? guard.value.join(', ') : String(guard.value || '')
    return `${indicatorLabel} ${outputLabel}.${guard.field || 'state'} = ${values}`
  }
  if (guard.type === 'metric_match') {
    return `${indicatorLabel} ${outputLabel}.${guard.field || 'field'} ${guard.operator || ''} ${guard.value ?? ''}`.trim()
  }
  if (guard.type === 'holds_for_bars') {
    return `${describeGuard(guard.guard, indicatorMap)} for ${guard.bars || 0} bars`
  }
  if (guard.type === 'signal_seen_within_bars' || guard.type === 'signal_absent_within_bars') {
    const stateLabel = guard.type === 'signal_seen_within_bars' ? 'seen' : 'absent'
    return `${indicatorLabel} ${outputLabel}.${guard.event_key || 'event'} ${stateLabel} within ${guard.lookback_bars || 0} bars`
  }
  return 'Incomplete condition'
}

const SectionHeader = ({ eyebrow, title, description }) => (
  <div className="space-y-1">
    <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-slate-500">{eyebrow}</p>
    <h4 className="text-base font-semibold text-white">{title}</h4>
    {description ? <p className="text-sm text-slate-400">{description}</p> : null}
  </div>
)

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
  const [currentStep, setCurrentStep] = useState(0)
  const {
    form,
    indicatorMap,
    signalIndicators,
    guardFieldFilters,
    canSubmit,
    incompleteGuardIndexes,
    addGuard,
    duplicateGuard,
    removeGuard,
    buildPayload,
    handleFieldChange,
    handleTriggerIndicatorChange,
    handleTriggerOutputChange,
    handleTriggerEventChange,
    handleGuardTypeChange,
    handleGuardVariantChange,
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

  useEffect(() => {
    if (!open) return
    setCurrentStep(0)
  }, [open, initialValues])

  const triggerIndicator = indicatorMap.get(form.trigger.indicator_id)
  const triggerOutputOptions = outputOptionsForType(triggerIndicator, 'signal', {
    selectedOutputName: form.trigger.output_name,
  })
  const triggerEventOptions = eventOptions(triggerIndicator, form.trigger.output_name)

  const previewGuards = useMemo(() => (
    (form.guards || []).map((guard) => {
      const contextValues = Array.isArray(guard.value_text) ? guard.value_text.filter(Boolean) : [guard.value_text].filter(Boolean)
      const numericValue = guard.value === '' ? null : Number(guard.value)
      const bars = Number(guard.bars)

      if (guard.type === 'ctx' && guard.variant === 'match' && guard.indicator_id && guard.output_name) {
        return {
          type: 'context_match',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          field: guard.field || 'state',
          value: contextValues,
        }
      }
      if (guard.type === 'ctx' && guard.variant === 'held' && guard.indicator_id && guard.output_name) {
        return {
          type: 'holds_for_bars',
          bars: Number.isFinite(bars) && bars > 0 ? bars : 0,
          guard: {
            type: 'context_match',
            indicator_id: guard.indicator_id,
            output_name: guard.output_name,
            field: guard.field || 'state',
            value: contextValues,
          },
        }
      }
      if (guard.type === 'metric' && guard.variant === 'match' && guard.indicator_id && guard.output_name) {
        return {
          type: 'metric_match',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          field: guard.field,
          operator: guard.operator,
          value: numericValue,
        }
      }
      if (guard.type === 'metric' && guard.variant === 'held' && guard.indicator_id && guard.output_name) {
        return {
          type: 'holds_for_bars',
          bars: Number.isFinite(bars) && bars > 0 ? bars : 0,
          guard: {
            type: 'metric_match',
            indicator_id: guard.indicator_id,
            output_name: guard.output_name,
            field: guard.field,
            operator: guard.operator,
            value: numericValue,
          },
        }
      }
      if (guard.type === 'signal' && guard.variant === 'seen' && guard.indicator_id && guard.output_name) {
        return {
          type: 'signal_seen_within_bars',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          event_key: guard.event_key,
          lookback_bars: Number.isFinite(bars) && bars > 0 ? bars : 0,
        }
      }
      if (guard.type === 'signal' && guard.variant === 'absent' && guard.indicator_id && guard.output_name) {
        return {
          type: 'signal_absent_within_bars',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          event_key: guard.event_key,
          lookback_bars: Number.isFinite(bars) && bars > 0 ? bars : 0,
        }
      }
      return null
    }).filter(Boolean)
  ), [form.guards])

  const conditionSummary = useMemo(
    () => buildRuleConditionSummary({
      rule: {
        trigger: {
          type: 'signal_match',
          indicator_id: form.trigger.indicator_id,
          output_name: form.trigger.output_name,
          event_key: form.trigger.event_key,
        },
        guards: previewGuards,
      },
      indicatorLookup: indicatorMap,
    }),
    [form.trigger, previewGuards, indicatorMap],
  )

  const resolvedRuleName = useMemo(
    () => form.name.trim() || buildRuleDefaultName({
      intent: form.intent,
      trigger: {
        indicator_id: form.trigger.indicator_id,
        output_name: form.trigger.output_name,
        event_key: form.trigger.event_key,
      },
      guards: previewGuards,
      indicatorLookup: indicatorMap,
    }),
    [form.name, form.intent, form.trigger, previewGuards, indicatorMap],
  )

  const incompleteGuardCount = incompleteGuardIndexes.length
  const canAdvanceFromSetup = canSubmit
  const canAdvanceFromConditions = incompleteGuardCount === 0
  const canSave = canSubmit && incompleteGuardCount === 0

  if (!open) return null

  const goNext = () => {
    if (currentStep === 0 && !canAdvanceFromSetup) return
    if (currentStep === 1 && !canAdvanceFromConditions) return
    setCurrentStep((step) => Math.min(step + 1, STEP_DEFS.length - 1))
  }

  const goBack = () => {
    setCurrentStep((step) => Math.max(step - 1, 0))
  }

  const handleSave = async (event) => {
    event.preventDefault()
    const payload = buildPayload()
    if (!payload || !canSave) return
    await onSubmit(payload)
  }

  return (
    <Dialog open={open} onClose={onCancel} className="relative z-50" initialFocus={initialFocusRef}>
      <DialogBackdrop className="fixed inset-0 bg-black/75 backdrop-blur-sm" />
      <div className="fixed inset-0 z-50 flex items-center justify-center px-4 py-8">
        <DialogPanel className="flex max-h-[calc(100vh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#14171f] text-slate-100 shadow-2xl">
          <header className="border-b border-white/5 px-6 py-5">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                  {initialValues ? 'Edit rule' : 'Create rule'}
                </p>
                <DialogTitle className="text-lg font-semibold text-white">
                  {STEP_DEFS[currentStep].title}
                </DialogTitle>
                <p className="text-sm text-slate-400">{STEP_DEFS[currentStep].description}</p>
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
            </div>
            <div className="mt-4 flex flex-wrap items-center gap-2 text-xs uppercase tracking-[0.2em] text-slate-400">
              {STEP_DEFS.map((step, index) => {
                const active = currentStep === index
                const complete = currentStep > index
                return (
                  <div
                    key={step.id}
                    className={`flex items-center gap-2 rounded-full px-3 py-1 ${
                      active
                        ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                        : complete
                          ? 'bg-white/8 text-slate-200'
                          : 'bg-white/5 text-slate-500'
                    }`}
                  >
                    <span
                      className={`flex h-6 w-6 items-center justify-center rounded-full text-[11px] font-bold ${
                        active
                          ? 'bg-[color:var(--accent-alpha-40)] text-white'
                          : complete
                            ? 'bg-white/10 text-slate-100'
                            : 'bg-white/10 text-slate-400'
                      }`}
                    >
                      {index + 1}
                    </span>
                    <span>{step.title}</span>
                  </div>
                )
              })}
            </div>
          </header>

          <form
            id={RULE_DRAWER_FORM_ID}
            className="flex-1 overflow-y-auto px-6 py-5"
            onSubmit={handleSave}
          >
            {currentStep === 0 ? (
              <div className="grid gap-5 lg:grid-cols-[minmax(0,1.5fr)_minmax(18rem,0.9fr)]">
                <section className={`${panelClassName} space-y-5`}>
                  <SectionHeader
                    eyebrow="Trigger"
                    title="Choose the rule trigger"
                    description="Every rule starts from a single signal event. Name and description can stay blank and be derived for you."
                  />
                  <div className="grid gap-4 md:grid-cols-3">
                    <div>
                      <label className={fieldLabelClassName}>Indicator</label>
                      <DropdownSelect
                        value={form.trigger.indicator_id}
                        onChange={handleTriggerIndicatorChange}
                        placeholder="Select indicator"
                        options={signalIndicators.map((ind) => ({
                          value: ind.id,
                          label: ind.name || ind.type || ind.id,
                        }))}
                        className="mt-2"
                      />
                    </div>
                    <div>
                      <label className={fieldLabelClassName}>Signal Output</label>
                      <DropdownSelect
                        value={form.trigger.output_name}
                        onChange={handleTriggerOutputChange}
                        placeholder="Select signal output"
                        options={triggerOutputOptions.map((entry) => ({ value: entry.value, label: entry.label }))}
                        disabled={!form.trigger.indicator_id}
                        className="mt-2"
                      />
                    </div>
                    <div>
                      <label className={fieldLabelClassName}>Event</label>
                      <DropdownSelect
                        value={form.trigger.event_key}
                        onChange={handleTriggerEventChange}
                        placeholder="Select event"
                        options={triggerEventOptions}
                        disabled={!form.trigger.output_name}
                        className="mt-2"
                      />
                    </div>
                  </div>
                  {!canAdvanceFromSetup ? (
                    <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-3 py-2">
                      <p className="text-sm text-amber-100">
                        Select an indicator, signal output, and event before moving on.
                      </p>
                    </div>
                  ) : null}
                  <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Live Summary</p>
                    <p className="mt-2 text-sm text-slate-300">{conditionSummary || 'No trigger selected'}</p>
                  </div>
                </section>

                <aside className={`${panelClassName} space-y-4`}>
                  <SectionHeader
                    eyebrow="Rule"
                    title="Core settings"
                    description="These control how the rule appears and how conflicts resolve."
                  />
                  <div>
                    <label className={fieldLabelClassName}>Name</label>
                    <input
                      className={textInputClassName}
                      value={form.name}
                      onChange={handleFieldChange('name')}
                      placeholder="Optional. Leave blank to auto-name from the flow."
                    />
                  </div>
                  <div>
                    <label className={fieldLabelClassName}>Description</label>
                    <textarea
                      className={`${textInputClassName} min-h-24 resize-none`}
                      value={form.description}
                      onChange={handleFieldChange('description')}
                      placeholder="Optional notes for future review."
                    />
                  </div>
                  <div className="grid gap-4 sm:grid-cols-2">
                    <div>
                      <label className={fieldLabelClassName}>Intent</label>
                      <select
                        className={textInputClassName}
                        value={form.intent}
                        onChange={handleFieldChange('intent')}
                      >
                        <option value="enter_long">Long entry</option>
                        <option value="enter_short">Short entry</option>
                      </select>
                    </div>
                    <div>
                      <label className={fieldLabelClassName}>Priority</label>
                      <input
                        className={textInputClassName}
                        type="number"
                        step="1"
                        value={form.priority}
                        onChange={handleFieldChange('priority')}
                        placeholder="0"
                      />
                    </div>
                  </div>
                  <label className="flex items-center gap-3 rounded-xl border border-white/10 bg-black/20 px-3 py-3 text-sm text-slate-300">
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border border-white/20 bg-black/60"
                      checked={Boolean(form.enabled)}
                      onChange={handleFieldChange('enabled')}
                    />
                    Rule is enabled
                  </label>
                  <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Resolved name</p>
                    <p className="mt-2 text-sm font-medium text-white">{resolvedRuleName}</p>
                  </div>
                </aside>
              </div>
            ) : null}

            {currentStep === 1 ? (
              <div className="space-y-5">
                <section className={`${panelClassName} space-y-4`}>
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <SectionHeader
                      eyebrow="Conditions"
                      title="Refine when the rule can fire"
                      description="Guards are optional. If you add one, complete it before continuing so it is not silently dropped."
                    />
                    <ActionButton type="button" variant="ghost" onClick={addGuard}>
                      <Plus className="mr-1 h-3.5 w-3.5" />
                      Add condition
                    </ActionButton>
                  </div>

                  {incompleteGuardCount > 0 ? (
                    <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-3 py-2">
                      <p className="text-sm text-amber-100">
                        {incompleteGuardCount} condition{incompleteGuardCount === 1 ? '' : 's'} still need fields before review.
                      </p>
                    </div>
                  ) : null}

                  <div className="space-y-3">
                    {(form.guards || []).length === 0 ? (
                      <div className="rounded-xl border border-dashed border-white/10 bg-black/20 px-4 py-8 text-center">
                        <p className="text-sm text-slate-300">No conditions yet.</p>
                        <p className="mt-1 text-sm text-slate-500">The rule will fire on the trigger alone.</p>
                      </div>
                    ) : (
                      form.guards.map((guard, index) => {
                        const indicator = indicatorMap.get(guard.indicator_id)
                        const outputType = guard.type === 'ctx' ? 'context' : guard.type === 'metric' ? 'metric' : 'signal'
                        const indicatorOptions = indicatorOptionsForGuards(indicators, outputType)
                        const outputOptions = outputOptionsForType(indicator, outputType, {
                          selectedOutputName: guard.output_name,
                        })
                        const contextOptions = contextFieldOptions(indicator, guard.output_name)
                        const metricOptions = metricFieldOptions(indicator, guard.output_name)
                        const valueOptions = contextValueOptions(indicator, guard.output_name, guard.field || 'state')
                        const signalEventOptions = eventOptions(indicator, guard.output_name)
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
                        const guardComplete = !incompleteGuardIndexes.includes(index)

                        return (
                          <div
                            key={`guard-${index}`}
                            className="rounded-2xl border border-white/10 bg-black/20 px-4 py-4"
                          >
                            <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
                              <div>
                                <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">
                                  Condition {index + 1}
                                </p>
                                <p className={`mt-1 text-sm ${guardComplete ? 'text-slate-300' : 'text-amber-200'}`}>
                                  {guardComplete ? 'Ready for review.' : 'Still incomplete.'}
                                </p>
                              </div>
                              <div className="flex items-center gap-2">
                                <ActionButton
                                  type="button"
                                  variant="subtle"
                                  onClick={() => duplicateGuard(index)}
                                  className="inline-flex items-center"
                                >
                                  <CopyPlus className="mr-1 h-3.5 w-3.5" />
                                  Duplicate
                                </ActionButton>
                                <ActionButton
                                  type="button"
                                  variant="subtle"
                                  onClick={() => removeGuard(index)}
                                  className="inline-flex items-center text-rose-400 hover:text-rose-300"
                                >
                                  <Trash2 className="mr-1 h-3.5 w-3.5" />
                                  Remove
                                </ActionButton>
                              </div>
                            </div>

                            <div className="grid gap-4 xl:grid-cols-[auto_auto_minmax(0,1fr)_minmax(0,1fr)]">
                              <div>
                                <label className={fieldLabelClassName}>Type</label>
                                <select
                                  value={guard.type}
                                  onChange={(e) => handleGuardTypeChange(index, e.target.value)}
                                  className={textInputClassName}
                                >
                                  <option value="ctx">Context</option>
                                  <option value="metric">Metric</option>
                                  <option value="signal">Signal</option>
                                </select>
                              </div>
                              <div>
                                <label className={fieldLabelClassName}>Variant</label>
                                <select
                                  value={guard.variant}
                                  onChange={(e) => handleGuardVariantChange(index, e.target.value)}
                                  className={textInputClassName}
                                >
                                  {guard.type === 'ctx' ? (
                                    <>
                                      <option value="match">Match</option>
                                      <option value="held">Held</option>
                                    </>
                                  ) : null}
                                  {guard.type === 'metric' ? (
                                    <>
                                      <option value="match">Match</option>
                                      <option value="held">Held</option>
                                    </>
                                  ) : null}
                                  {guard.type === 'signal' ? (
                                    <>
                                      <option value="seen">Seen</option>
                                      <option value="absent">Absent</option>
                                    </>
                                  ) : null}
                                </select>
                              </div>
                              <div>
                                <label className={fieldLabelClassName}>Indicator</label>
                                <DropdownSelect
                                  value={guard.indicator_id}
                                  onChange={(v) => handleGuardIndicatorChange(index, v)}
                                  placeholder="Indicator"
                                  options={indicatorOptions}
                                  className="mt-2"
                                />
                              </div>
                              <div>
                                <label className={fieldLabelClassName}>Output</label>
                                <DropdownSelect
                                  value={guard.output_name}
                                  onChange={(v) => handleGuardOutputChange(index, v)}
                                  placeholder="Output"
                                  options={outputOptions.map((entry) => ({ value: entry.value, label: entry.label }))}
                                  disabled={!guard.indicator_id}
                                  className="mt-2"
                                />
                              </div>
                            </div>

                            {guard.type === 'ctx' ? (
                              <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
                                <div>
                                  <label className={fieldLabelClassName}>Field</label>
                                  <input
                                    className={textInputClassName}
                                    value={fieldFilter}
                                    onChange={(e) => handleGuardFieldFilterChange(index, e.target.value)}
                                    placeholder="Filter fields..."
                                    disabled={!guard.output_name}
                                  />
                                  {selectedContextOption ? (
                                    <div className="mt-2 flex items-center gap-2">
                                      <span className="rounded-lg border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-200">
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
                                  <div className="mt-2 max-h-40 overflow-y-auto rounded-xl border border-white/10 bg-black/20">
                                    {filteredContextOptions.length > 0 ? filteredContextOptions.map((option) => (
                                      <button
                                        key={`${guard.indicator_id || 'guard'}-${option.value}`}
                                        type="button"
                                        className={`flex w-full items-center justify-between px-3 py-2 text-left text-sm transition ${
                                          option.value === selectedContextField
                                            ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                                            : 'text-slate-300 hover:bg-white/[0.04] hover:text-white'
                                        }`}
                                        onClick={() => {
                                          handleGuardFieldChange(index, 'field', option.value)
                                          clearGuardFieldFilter(index)
                                        }}
                                      >
                                        <span>{option.label}</span>
                                      </button>
                                    )) : (
                                      <div className="px-3 py-3 text-sm text-slate-500">No matching fields</div>
                                    )}
                                  </div>
                                </div>

                                <div>
                                  <label className={fieldLabelClassName}>Value</label>
                                  {valueOptions.length > 0 ? (
                                    <div className="mt-2 space-y-2">
                                      <div className="flex min-h-11 flex-wrap gap-1.5 rounded-xl border border-white/10 bg-black/20 px-2 py-2">
                                        {selectedValues.length > 0 ? selectedValues.map((selectedValue) => (
                                          <span
                                            key={`${guard.output_name || 'value'}-${selectedValue}`}
                                            className="inline-flex items-center gap-1 rounded-lg border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-200"
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
                                        )) : (
                                          <span className="px-1 py-1 text-sm text-slate-500">No values selected yet.</span>
                                        )}
                                      </div>
                                      <select
                                        defaultValue=""
                                        onChange={(e) => {
                                          const next = e.target.value
                                          if (!next) return
                                          handleGuardFieldChange(index, 'value_text', [...selectedValues, next])
                                          e.target.value = ''
                                        }}
                                        className="w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
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
                                      className={textInputClassName}
                                      value={Array.isArray(guard.value_text) ? guard.value_text[0] || '' : guard.value_text || ''}
                                      onChange={(e) => handleGuardFieldChange(index, 'value_text', e.target.value)}
                                      placeholder="Value"
                                    />
                                  )}
                                </div>

                                {guard.variant === 'held' ? (
                                  <div>
                                    <label className={fieldLabelClassName}>Bars</label>
                                    <input
                                      className={textInputClassName}
                                      type="number"
                                      min="1"
                                      value={guard.bars}
                                      onChange={(e) => handleGuardFieldChange(index, 'bars', e.target.value)}
                                      placeholder="Bars"
                                    />
                                  </div>
                                ) : null}
                              </div>
                            ) : null}

                            {guard.type === 'metric' ? (
                              <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto_auto_auto]">
                                <div>
                                  <label className={fieldLabelClassName}>Field</label>
                                  <DropdownSelect
                                    value={guard.field}
                                    onChange={(v) => handleGuardFieldChange(index, 'field', v)}
                                    placeholder="Field"
                                    options={metricOptions}
                                    disabled={!guard.output_name}
                                    className="mt-2"
                                  />
                                </div>
                                <div>
                                  <label className={fieldLabelClassName}>Operator</label>
                                  <select
                                    value={guard.operator}
                                    onChange={(e) => handleGuardFieldChange(index, 'operator', e.target.value)}
                                    className={textInputClassName}
                                  >
                                    <option value=">">&gt;</option>
                                    <option value=">=">&gt;=</option>
                                    <option value="<">&lt;</option>
                                    <option value="<=">&lt;=</option>
                                    <option value="==">==</option>
                                    <option value="!=">!=</option>
                                  </select>
                                </div>
                                <div>
                                  <label className={fieldLabelClassName}>Value</label>
                                  <input
                                    className={textInputClassName}
                                    value={guard.value}
                                    onChange={(e) => handleGuardFieldChange(index, 'value', e.target.value)}
                                    placeholder="0.0"
                                    inputMode="decimal"
                                  />
                                </div>
                                {guard.variant === 'held' ? (
                                  <div>
                                    <label className={fieldLabelClassName}>Bars</label>
                                    <input
                                      className={textInputClassName}
                                      type="number"
                                      min="1"
                                      value={guard.bars}
                                      onChange={(e) => handleGuardFieldChange(index, 'bars', e.target.value)}
                                      placeholder="Bars"
                                    />
                                  </div>
                                ) : null}
                              </div>
                            ) : null}

                            {guard.type === 'signal' ? (
                              <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto]">
                                <div>
                                  <label className={fieldLabelClassName}>Event</label>
                                  <DropdownSelect
                                    value={guard.event_key}
                                    onChange={(v) => handleGuardFieldChange(index, 'event_key', v)}
                                    placeholder="Event"
                                    options={signalEventOptions}
                                    disabled={!guard.output_name}
                                    className="mt-2"
                                  />
                                </div>
                                <div>
                                  <label className={fieldLabelClassName}>Lookback Bars</label>
                                  <input
                                    className={textInputClassName}
                                    type="number"
                                    min="1"
                                    value={guard.bars}
                                    onChange={(e) => handleGuardFieldChange(index, 'bars', e.target.value)}
                                    placeholder="Bars"
                                  />
                                </div>
                              </div>
                            ) : null}
                          </div>
                        )
                      })
                    )}
                  </div>
                </section>

                <section className={`${panelClassName} space-y-2`}>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Live Summary</p>
                  <p className="text-sm text-slate-300">{conditionSummary || 'No trigger selected'}</p>
                </section>
              </div>
            ) : null}

            {currentStep === 2 ? (
              <div className="grid gap-5 lg:grid-cols-[minmax(0,1.35fr)_minmax(18rem,0.85fr)]">
                <section className={`${panelClassName} space-y-4`}>
                  <SectionHeader
                    eyebrow="Review"
                    title={resolvedRuleName}
                    description="This is the rule that will be stored."
                  />

                  <div className="grid gap-3 sm:grid-cols-3">
                    <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Intent</p>
                      <p className="mt-2 text-sm text-white">{form.intent === 'enter_short' ? 'Short entry' : 'Long entry'}</p>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Priority</p>
                      <p className="mt-2 text-sm text-white">{Number.isFinite(Number(form.priority)) ? Number(form.priority) : 0}</p>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Status</p>
                      <p className="mt-2 text-sm text-white">{form.enabled ? 'Enabled' : 'Disabled'}</p>
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Trigger and flow</p>
                    <p className="mt-2 text-sm text-slate-300">{conditionSummary || 'No trigger selected'}</p>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Conditions</p>
                    {(previewGuards || []).length > 0 ? (
                      <div className="mt-3 space-y-2">
                        {previewGuards.map((guard, index) => (
                          <div
                            key={`review-guard-${index}`}
                            className="rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-300"
                          >
                            {describeGuard(guard, indicatorMap)}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-2 text-sm text-slate-500">No additional conditions. Trigger alone controls the rule.</p>
                    )}
                  </div>
                </section>

                <aside className={`${panelClassName} space-y-4`}>
                  <SectionHeader
                    eyebrow="Notes"
                    title="What will be saved"
                    description="Auto-generated names are only used if you leave the name field blank."
                  />
                  <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">Description</p>
                    <p className="mt-2 text-sm text-slate-300">
                      {form.description?.trim() || 'No description provided.'}
                    </p>
                  </div>
                  {!canSave ? (
                    <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3">
                      <p className="text-sm text-amber-100">
                        Finish the trigger and any added conditions before saving.
                      </p>
                    </div>
                  ) : null}
                </aside>
              </div>
            ) : null}
          </form>

          <footer className="border-t border-white/5 px-6 py-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="text-sm text-slate-500">
                Step {currentStep + 1} of {STEP_DEFS.length}
              </p>
              <div className="flex items-center gap-3">
                <ActionButton type="button" variant="ghost" onClick={onCancel}>
                  Cancel
                </ActionButton>
                {currentStep > 0 ? (
                  <ActionButton type="button" variant="ghost" onClick={goBack}>
                    <ChevronLeft className="mr-1 h-3.5 w-3.5" />
                    Back
                  </ActionButton>
                ) : null}
                {currentStep < STEP_DEFS.length - 1 ? (
                  <ActionButton
                    type="button"
                    onClick={goNext}
                    disabled={(currentStep === 0 && !canAdvanceFromSetup) || (currentStep === 1 && !canAdvanceFromConditions)}
                  >
                    Next
                    <ChevronRight className="ml-1 h-3.5 w-3.5" />
                  </ActionButton>
                ) : (
                  <ActionButton type="submit" form={RULE_DRAWER_FORM_ID} disabled={submitting || !canSave}>
                    {submitting ? 'Saving…' : (initialValues ? 'Save changes' : 'Create rule')}
                  </ActionButton>
                )}
              </div>
            </div>
          </footer>
        </DialogPanel>
      </div>
    </Dialog>
  )
}

export default RuleDrawer
