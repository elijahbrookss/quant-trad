import { Fragment, useCallback, useEffect, useMemo, useState } from 'react'

import {
  attachStrategyIndicator,
  createStrategy,
  createStrategyRule,
  deleteStrategy,
  deleteStrategyRule,
  detachStrategyIndicator,
  fetchStrategies,
  generateStrategySignals,
  updateStrategy,
  updateStrategyRule,
} from '../adapters/strategy.adapter.js'
import { fetchIndicators, fetchIndicator, fetchIndicatorStrategies } from '../adapters/indicator.adapter.js'
import { createInstrument } from '../adapters/instrument.adapter.js'
import ATMConfigForm, { DEFAULT_ATM_TEMPLATE, cloneATMTemplate } from './atm/ATMConfigForm.jsx'
import ATMTemplateSummary from './atm/ATMTemplateSummary.jsx'
import { useChartState } from '../contexts/ChartStateContext.jsx'
import { createLogger } from '../utils/logger.js'
import { DateRangePickerComponent } from './ChartComponent/DateTimePickerComponent.jsx'
import DropdownSelect from './ChartComponent/DropdownSelect.jsx'
import { TimeframeSelect } from './ChartComponent/TimeframeSelectComponent.jsx'
import {
  DATASOURCE_OPTIONS,
  DEFAULT_DATASOURCE,
  IB_EXCHANGES,
  CRYPTO_EXCHANGES,
} from '../constants/datasources.js'

const STRATEGY_FORM_DEFAULT = {
  name: '',
  description: '',
  timeframe: '15m',
  datasource: '',
  exchange: '',
  symbols: '',
  atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
}

const RULE_FORM_DEFAULT = {
  name: '',
  description: '',
  action: 'buy',
  match: 'all',
  conditions: [
    {
      indicator_id: '',
      rule_id: '',
      signal_type: '',
      direction: '',
    },
  ],
  enabled: true,
}

const INSTRUMENT_FORM_DEFAULT = {
  symbol: '',
  datasource: '',
  exchange: '',
  tick_size: '',
  tick_value: '',
  contract_size: '',
  min_order_size: '',
  quote_currency: '',
  maker_fee_rate: '',
  taker_fee_rate: '',
}

const EMPTY_LIST = Object.freeze([])


const ActionButton = ({ variant = 'default', className = '', ...props }) => {
  const base =
    'rounded-lg px-3 py-1.5 text-sm font-medium transition focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-[#10121a]'

  const styles = {
    default: `${base} bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)] hover:bg-[color:var(--accent-alpha-40)]`,
    ghost: `${base} bg-white/5 text-slate-200 hover:bg-white/10`,
    danger: `${base} bg-rose-500/80 text-white hover:bg-rose-500`,
    subtle: `${base} bg-transparent text-slate-400 hover:text-slate-100`,
  }

  const classes = [styles[variant] || styles.default, className].filter(Boolean).join(' ')
  return <button className={classes} {...props} />
}

function StrategyFormModal({
  open,
  initialValues,
  onSubmit,
  onCancel,
  submitting,
  availableATMTemplates = [],
}) {
  const [form, setForm] = useState(() => ({
    ...STRATEGY_FORM_DEFAULT,
    atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
  }))
  const [currentStep, setCurrentStep] = useState(0)
  const [atmMode, setAtmMode] = useState('new')
  const [selectedATMTemplateId, setSelectedATMTemplateId] = useState('')

  const templateKey = useCallback((template) => {
    try {
      return JSON.stringify(template || {})
    } catch (err) {
      console.error('Failed to stringify ATM template', err)
      return ''
    }
  }, [])

  const templateOptions = useMemo(
    () =>
      (availableATMTemplates || []).map((item, index) => ({
        value: item.id || `atm-${index + 1}`,
        label: item.label || `ATM template ${index + 1}`,
        template: cloneATMTemplate(item.template || DEFAULT_ATM_TEMPLATE),
        key: templateKey(item.template),
      })),
    [availableATMTemplates, templateKey],
  )

  const selectedTemplate = useMemo(
    () => templateOptions.find((option) => option.value === selectedATMTemplateId),
    [selectedATMTemplateId, templateOptions],
  )

  useEffect(() => {
    if (!open) {
      setForm({
        ...STRATEGY_FORM_DEFAULT,
        atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
      })
      setCurrentStep(0)
      setAtmMode('new')
      setSelectedATMTemplateId('')
      return
    }

    if (initialValues) {
      const fallbackTemplate = cloneATMTemplate(initialValues.atm_template || DEFAULT_ATM_TEMPLATE)
      const match = templateOptions.find(
        (option) => option.key && option.key === templateKey(initialValues.atm_template),
      )

      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        timeframe: initialValues.timeframe || '15m',
        datasource: (initialValues.datasource || '').toUpperCase(),
        exchange: initialValues.exchange || '',
        symbols: Array.isArray(initialValues.symbols)
          ? initialValues.symbols.join(', ')
          : initialValues.symbols || '',
        atm_template: match ? cloneATMTemplate(match.template) : fallbackTemplate,
      })
      setAtmMode(match ? 'existing' : 'new')
      setSelectedATMTemplateId(match?.value || '')
      setCurrentStep(0)
    } else {
      setForm({
        ...STRATEGY_FORM_DEFAULT,
        atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
      })
      setCurrentStep(0)
      setAtmMode('new')
      setSelectedATMTemplateId('')
    }
  }, [open, initialValues, templateOptions, templateKey])

  const handleChange = (field) => (input) => {
    let value = input
    if (input && typeof input === 'object' && 'target' in input) {
      value = input.target?.value ?? ''
    }

    if (field === 'datasource' && typeof value === 'string') {
      value = value.toUpperCase()
    }

    setForm((prev) => ({ ...prev, [field]: value ?? '' }))
  }

  const handleATMTemplateChange = useCallback((template) => {
    setForm((prev) => ({
      ...prev,
      atm_template: cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE),
    }))
    setAtmMode('new')
  }, [])

  const handleATMTemplateSelect = (value) => {
    const option = templateOptions.find((candidate) => candidate.value === value)
    setSelectedATMTemplateId(value || '')
    if (option) {
      handleATMTemplateChange(option.template)
      setAtmMode('existing')
    }
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const fallbackName = `Strategy ${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`
    const name = form.name.trim() || fallbackName
    const payload = {
      name,
      description: form.description.trim() || null,
      timeframe: form.timeframe.trim() || '15m',
      datasource: form.datasource.trim() || null,
      exchange: form.exchange.trim() || null,
      symbols: form.symbols
        .split(/[\s,;]+/)
        .map((token) => token.trim())
        .filter(Boolean),
      atm_template: cloneATMTemplate(form.atm_template),
    }
    if (Array.isArray(payload.symbols) && payload.symbols.length > 1) {
      payload.symbols = payload.symbols.slice(0, 1)
    }
    await onSubmit(payload)
  }

  const datasource = (form.datasource || DEFAULT_DATASOURCE).toUpperCase()
  const exchangeOptions = useMemo(() => {
    if (datasource === 'CCXT') {
      return [{ value: '', label: 'Auto (per symbol)' }, ...CRYPTO_EXCHANGES]
    }
    if (datasource === 'IBKR') {
      return [{ value: '', label: 'Auto (per symbol)' }, ...IB_EXCHANGES]
    }
    return [{ value: '', label: 'Use datasource defaults' }, ...CRYPTO_EXCHANGES, ...IB_EXCHANGES]
  }, [datasource])

  const handleStepAdvance = (event) => {
    event.preventDefault()
    setCurrentStep(1)
  }

  const handleStepBack = (event) => {
    event.preventDefault()
    setCurrentStep(0)
  }

  if (!open) return null

  const steps = [
    { id: 0, title: 'Details', description: 'Describe your strategy and trading venue.' },
    { id: 1, title: 'ATM template', description: 'Choose or build risk/target settings.' },
  ]

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-4xl space-y-6 overflow-hidden rounded-2xl border border-white/10 bg-[#1b1e28] text-slate-100 shadow-xl">
        <header className="border-b border-white/5 px-6 py-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                {initialValues ? 'Edit strategy' : 'Create strategy'}
              </p>
              <h3 className="text-lg font-semibold text-white">{steps[currentStep].title}</h3>
              <p className="text-sm text-slate-400">{steps[currentStep].description}</p>
            </div>
            <div className="flex items-center gap-3 text-xs uppercase tracking-[0.2em] text-slate-400">
              {steps.map((step) => (
                <div
                  key={step.id}
                  className={`flex items-center gap-2 rounded-full px-3 py-1 ${
                    currentStep === step.id
                      ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                      : 'bg-white/5'
                  }`}
                >
                  <span
                    className={`flex h-6 w-6 items-center justify-center rounded-full text-xs font-bold ${
                      currentStep === step.id ? 'bg-[color:var(--accent-alpha-40)] text-white' : 'bg-white/10 text-slate-200'
                    }`}
                  >
                    {step.id + 1}
                  </span>
                  <span>{step.title}</span>
                </div>
              ))}
            </div>
          </div>
        </header>

        <form className="max-h-[70vh] space-y-6 overflow-y-auto px-6 py-4" onSubmit={handleSubmit}>
          {currentStep === 0 && (
            <div className="space-y-4">
              <div className="grid gap-4 md:grid-cols-2">
                <div>
                  <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Name</label>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={form.name}
                    onChange={handleChange('name')}
                    placeholder="Optional: auto-generated if blank"
                  />
                </div>
                <div>
                  <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Description</label>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={form.description}
                    onChange={handleChange('description')}
                    placeholder="Optional context for your notes"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                <TimeframeSelect
                  selected={form.timeframe || '15m'}
                  onChange={(value) => handleChange('timeframe')(value)}
                  className="md:col-span-1"
                />
                <div className="md:col-span-1">
                  <DropdownSelect
                    label="Datasource"
                    value={form.datasource || ''}
                    onChange={handleChange('datasource')}
                    options={[{ value: '', label: 'Use chart defaults' }, ...DATASOURCE_OPTIONS]}
                    className="mt-1 w-full"
                  />
                </div>
                <div className="md:col-span-1">
                  <DropdownSelect
                    label="Exchange"
                    value={form.exchange || ''}
                    onChange={handleChange('exchange')}
                    options={exchangeOptions}
                    className="mt-1 w-full"
                  />
                </div>
              </div>

              <div>
                <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Symbol</label>
                <input
                  className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  value={form.symbols}
                  onChange={handleChange('symbols')}
                  placeholder="e.g. BTCUSD"
                />
                <p className="mt-1 text-[11px] text-slate-500">Provide one symbol to start; you can add more later.</p>
              </div>
            </div>
          )}

          {currentStep === 1 && (
            <div className="space-y-4">
              {templateOptions.length > 0 && (
                <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
                  <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Reuse ATM template</p>
                      <p className="text-xs text-slate-500">
                        Start from an existing configuration or switch to a fresh layout below.
                      </p>
                    </div>
                    <div className="flex flex-wrap gap-3 text-xs font-semibold uppercase tracking-[0.2em] text-slate-300">
                      <label className="flex items-center gap-2">
                        <input
                          type="radio"
                          className="h-4 w-4 accent-[color:var(--accent-text-strong)]"
                          checked={atmMode === 'existing'}
                          onChange={() => setAtmMode('existing')}
                        />
                        Use existing
                      </label>
                      <label className="flex items-center gap-2">
                        <input
                          type="radio"
                          className="h-4 w-4 accent-[color:var(--accent-text-strong)]"
                          checked={atmMode === 'new'}
                          onChange={() => setAtmMode('new')}
                        />
                        Create new
                      </label>
                    </div>
                  </div>

                  {atmMode === 'existing' && (
                    <div className="mt-3 space-y-3">
                      <DropdownSelect
                        label="Existing templates"
                        value={selectedATMTemplateId}
                        onChange={handleATMTemplateSelect}
                        options={templateOptions.map((option) => ({ value: option.value, label: option.label }))}
                        className="mt-1 w-full"
                      />
                      {selectedTemplate && <ATMTemplateSummary template={selectedTemplate.template} />}
                    </div>
                  )}
                </div>
              )}

              <div className="space-y-3 rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">ATM builder</p>
                    <p className="text-xs text-slate-500">Contracts, stops, targets, breakeven, and trailing.</p>
                  </div>
                  <ActionButton type="button" variant="subtle" onClick={() => handleATMTemplateChange(DEFAULT_ATM_TEMPLATE)}>
                    Reset
                  </ActionButton>
                </div>
                <ATMTemplateSummary template={form.atm_template} />
                {atmMode !== 'existing' && (
                  <ATMConfigForm value={form.atm_template} onChange={handleATMTemplateChange} />
                )}
                {atmMode === 'existing' && !selectedTemplate && (
                  <p className="text-xs text-amber-200/80">
                    Select an existing template above or switch to "Create new" to edit the fields directly.
                  </p>
                )}
              </div>
            </div>
          )}

          <footer className="flex items-center justify-between border-t border-white/5 pt-4">
            <div className="text-xs text-slate-500">
              Step {currentStep + 1} of {steps.length}
            </div>
            <div className="flex items-center gap-2">
              <ActionButton type="button" variant="ghost" onClick={onCancel}>
                Cancel
              </ActionButton>
              {currentStep === 1 && (
                <ActionButton type="button" variant="subtle" onClick={handleStepBack}>
                  Back
                </ActionButton>
              )}
              {currentStep === 0 && (
                <ActionButton type="button" onClick={handleStepAdvance}>
                  Next
                </ActionButton>
              )}
              {currentStep === 1 && (
                <ActionButton type="submit" disabled={submitting}>
                  {submitting ? 'Saving…' : 'Save strategy'}
                </ActionButton>
              )}
            </div>
          </footer>
        </form>
      </div>
    </div>
  )
}

function RuleFormModal({
  open,
  indicators,
  ensureIndicatorMeta,
  initialValues,
  onSubmit,
  onCancel,
  submitting,
}) {
  const [form, setForm] = useState(RULE_FORM_DEFAULT)

  const indicatorMap = useMemo(() => {
    const map = new Map()
    for (const indicator of indicators || []) {
      map.set(indicator.id, indicator)
    }
    return map
  }, [indicators])

  const makeEmptyCondition = useCallback(
    () => ({ indicator_id: '', rule_id: '', signal_type: '', direction: '' }),
    [],
  )

  useEffect(() => {
    if (!open) {
      setForm(RULE_FORM_DEFAULT)
      return
    }

    if (initialValues) {
      const mappedConditions = Array.isArray(initialValues.conditions)
        ? initialValues.conditions.map((condition) => ({
            indicator_id: condition.indicator_id || '',
            rule_id: condition.rule_id || '',
            signal_type: condition.signal_type || '',
            direction: condition.direction || '',
          }))
        : []

      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        action: initialValues.action || 'buy',
        match: initialValues.match || 'all',
        conditions: mappedConditions.length ? mappedConditions : [makeEmptyCondition()],
        enabled: Boolean(initialValues.enabled),
      })
    } else {
      setForm({ ...RULE_FORM_DEFAULT, conditions: [makeEmptyCondition()] })
    }
  }, [open, initialValues, makeEmptyCondition])

  const trackedIndicatorIds = useMemo(
    () =>
      Array.from(
        new Set(
          (form.conditions || [])
            .map((condition) => condition.indicator_id)
            .filter((indicatorId) => typeof indicatorId === 'string' && indicatorId.trim().length > 0),
        ),
      ),
    [form.conditions],
  )

  useEffect(() => {
    if (!open || typeof ensureIndicatorMeta !== 'function' || !trackedIndicatorIds.length) {
      return
    }
    trackedIndicatorIds.forEach((indicatorId) => {
      ensureIndicatorMeta(indicatorId)
    })
  }, [open, trackedIndicatorIds, ensureIndicatorMeta])

  if (!open) return null

  const canSubmit = form.conditions.some(
    (condition) => condition.indicator_id && condition.signal_type,
  )

  const updateCondition = (index, updates) => {
    setForm((prev) => {
      const nextConditions = prev.conditions.map((condition, idx) =>
        idx === index ? { ...condition, ...updates } : condition,
      )
      return { ...prev, conditions: nextConditions }
    })
  }

  const handleConditionIndicatorChange = (index) => (indicatorId) => {
    updateCondition(index, {
      indicator_id: indicatorId || '',
      rule_id: '',
      signal_type: '',
      direction: '',
    })
    if (indicatorId && typeof ensureIndicatorMeta === 'function') {
      ensureIndicatorMeta(indicatorId)
    }
  }

  const handleConditionRuleChange = (index) => (ruleId) => {
    setForm((prev) => {
      const nextConditions = [...prev.conditions]
      const current = nextConditions[index]
      const indicatorMeta = indicatorMap.get(current.indicator_id)
      const rules = Array.isArray(indicatorMeta?.signal_rules) ? indicatorMeta.signal_rules : []
      const selectedRule = rules.find((rule) => rule.id === ruleId)
      const defaultDirection = Array.isArray(selectedRule?.directions) && selectedRule.directions.length === 1
        ? selectedRule.directions[0].id
        : ''
      nextConditions[index] = {
        ...current,
        rule_id: ruleId || '',
        signal_type: selectedRule?.signal_type || '',
        direction: defaultDirection || '',
      }
      return { ...prev, conditions: nextConditions }
    })
  }

  const handleConditionDirectionChange = (index) => (direction) => {
    updateCondition(index, { direction: direction || '' })
  }

  const addCondition = () => {
    setForm((prev) => ({
      ...prev,
      conditions: [...prev.conditions, makeEmptyCondition()],
    }))
  }

  const removeCondition = (index) => {
    setForm((prev) => {
      const nextConditions = prev.conditions.filter((_, idx) => idx !== index)
      return {
        ...prev,
        conditions: nextConditions.length ? nextConditions : [makeEmptyCondition()],
      }
    })
  }

  const handleFieldChange = (field) => (input) => {
    let value = input
    if (input && typeof input === 'object' && 'target' in input) {
      const target = input.target
      if (target.type === 'checkbox') {
        value = target.checked
      } else {
        value = target.value
      }
    }
    setForm((prev) => ({ ...prev, [field]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const conditions = form.conditions
      .map((condition) => ({
        indicator_id: condition.indicator_id,
        signal_type: condition.signal_type,
        rule_id: condition.rule_id || null,
        direction: condition.direction || null,
      }))
      .filter((condition) => condition.indicator_id && condition.signal_type)

    if (!conditions.length) {
      return
    }

    const payload = {
      name: form.name.trim(),
      description: form.description.trim() || null,
      action: form.action,
      match: form.match,
      conditions,
      enabled: Boolean(form.enabled),
    }
    await onSubmit(payload)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-2xl space-y-6 rounded-2xl border border-white/10 bg-[#1b1e28] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">
            {initialValues ? 'Edit rule' : 'Create rule'}
          </h3>
          <p className="text-sm text-slate-400">
            Combine indicator signals into modular buy or sell logic for this strategy.
          </p>
        </header>

        <form className="space-y-5" onSubmit={handleSubmit}>
          <div>
            <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
              Name
            </label>
            <input
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              value={form.name}
              onChange={handleFieldChange('name')}
              required
            />
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
              Description
            </label>
            <textarea
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              rows={3}
              value={form.description}
              onChange={handleFieldChange('description')}
            />
          </div>

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h4 className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Conditions
              </h4>
              <ActionButton type="button" variant="ghost" onClick={addCondition}>
                Add condition
              </ActionButton>
            </div>

            {form.conditions.map((condition, index) => {
              const indicatorMeta = indicatorMap.get(condition.indicator_id)
              const ruleOptions = Array.isArray(indicatorMeta?.signal_rules)
                ? indicatorMeta.signal_rules
                : []
              const selectedRule = ruleOptions.find((rule) => rule.id === condition.rule_id)
              const directionOptions = Array.isArray(selectedRule?.directions)
                ? selectedRule.directions
                : []

              return (
                <div
                  key={`condition-${index}`}
                  className="space-y-3 rounded-xl border border-white/10 bg-black/30 p-4"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex-1 space-y-3">
                      <DropdownSelect
                        label="Indicator"
                        value={condition.indicator_id}
                        onChange={handleConditionIndicatorChange(index)}
                        placeholder="Select indicator"
                        options={indicators.map((indicator) => {
                          const hasSignals = Array.isArray(indicator.signal_rules)
                            && indicator.signal_rules.length > 0
                          const label = indicator.name || indicator.type
                          return {
                            value: indicator.id,
                            label,
                            description: hasSignals ? undefined : 'No signals registered',
                            disabled: !hasSignals && indicator.id !== condition.indicator_id,
                          }
                        })}
                        disabled={!indicators.length}
                        className="mt-1 w-full"
                      />

                      <div className="grid gap-3 md:grid-cols-2">
                        <div>
                          <DropdownSelect
                            label="Signal type"
                            value={condition.rule_id}
                            onChange={handleConditionRuleChange(index)}
                            placeholder="Select signal"
                            disabled={!condition.indicator_id}
                            options={ruleOptions.map((rule) => {
                              const parts = []
                              if (rule.signal_type) {
                                parts.push(rule.signal_type.toUpperCase())
                              }
                              if (rule.label && rule.label.toLowerCase() !== (rule.signal_type || '').toLowerCase()) {
                                parts.push(rule.label)
                              }
                              return {
                                value: rule.id,
                                label: parts.length ? parts.join(' – ') : rule.id,
                                description: rule.description,
                              }
                            })}
                            className="mt-1 w-full"
                          />
                          {condition.indicator_id && !ruleOptions.length && (
                            <p className="mt-1 text-[11px] text-amber-300/80">
                              This indicator has no registered signals yet. Configure signal rules on the Indicators tab first.
                            </p>
                          )}
                          {selectedRule?.description && (
                            <p className="mt-1 text-[11px] text-slate-400">{selectedRule.description}</p>
                          )}
                          {condition.signal_type && (
                            <p className="mt-1 text-[11px] text-slate-400">
                              Selected signal:&nbsp;
                              <span className="font-semibold text-white">{condition.signal_type.toUpperCase()}</span>
                            </p>
                          )}
                        </div>

                        <div>
                          <DropdownSelect
                            label="Direction filter"
                            value={condition.direction || ''}
                            onChange={handleConditionDirectionChange(index)}
                            disabled={!directionOptions.length}
                            options={[
                              { value: '', label: 'Any direction', description: 'Match all biases' },
                              ...directionOptions.map((direction) => ({
                                value: direction.id,
                                label: direction.label || direction.id,
                                description: direction.description,
                              })),
                            ]}
                            className="mt-1 w-full"
                          />
                          {directionOptions.length > 0 && (
                            <ul className="mt-1 space-y-1 text-[11px] text-slate-400">
                              {directionOptions.map((direction) => (
                                <li key={`${direction.id}-hint`}>
                                  <span className="font-semibold text-slate-300">{direction.label || direction.id}:</span>{' '}
                                  {direction.description}
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      </div>
                    </div>

                    {form.conditions.length > 1 && (
                      <button
                        type="button"
                        className="mt-2 rounded-full border border-white/20 px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-slate-300 hover:border-rose-400/70 hover:text-rose-200"
                        onClick={() => removeCondition(index)}
                      >
                        Remove
                      </button>
                    )}
                  </div>
                </div>
              )
            })}
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <DropdownSelect
              label="Action"
              value={form.action}
              onChange={handleFieldChange('action')}
              options={[
                { value: 'buy', label: 'Buy' },
                { value: 'sell', label: 'Sell' },
              ]}
              className="w-full"
            />

            <DropdownSelect
              label="Confluence logic"
              value={form.match}
              onChange={handleFieldChange('match')}
              options={[
                { value: 'all', label: 'All conditions must match' },
                { value: 'any', label: 'Any condition can trigger' },
              ]}
              className="w-full"
            />
          </div>

          <label className="mt-1 flex items-center gap-2 text-sm text-slate-300">
            <input
              type="checkbox"
              className="h-4 w-4 rounded border border-white/20 bg-black/60"
              checked={form.enabled}
              onChange={handleFieldChange('enabled')}
            />
            Enabled
          </label>

          {!indicators.length && (
            <p className="text-[11px] text-amber-300/80">
              Attach at least one indicator to this strategy to build rule conditions.
            </p>
          )}

          <footer className="flex items-center justify-end gap-2">
            <ActionButton type="button" variant="ghost" onClick={onCancel}>
              Cancel
            </ActionButton>
            <ActionButton type="submit" disabled={submitting || !canSubmit}>
              {submitting ? 'Saving…' : 'Save rule'}
            </ActionButton>
          </footer>
        </form>
      </div>
    </div>
  )
}

function InstrumentFormModal({ open, initialValues, onSubmit, onCancel, submitting, error }) {
  const [form, setForm] = useState(INSTRUMENT_FORM_DEFAULT)
  const [localError, setLocalError] = useState(null)

  useEffect(() => {
    if (!open) {
      setForm(INSTRUMENT_FORM_DEFAULT)
      setLocalError(null)
      return
    }
    setForm({
      ...INSTRUMENT_FORM_DEFAULT,
      ...(initialValues || {}),
    })
    setLocalError(null)
  }, [open, initialValues])

  if (!open) return null

  const handleChange = (field) => (event) => {
    const value = event?.target ? event.target.value : event
    setForm((prev) => ({ ...prev, [field]: value ?? '' }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const payload = {
      symbol: (form.symbol || '').trim().toUpperCase(),
      datasource: (form.datasource || '').trim().toUpperCase() || null,
      exchange: (form.exchange || '').trim() || null,
      quote_currency: (form.quote_currency || '').trim().toUpperCase() || null,
    }
    const numericFields = [
      'tick_size',
      'tick_value',
      'contract_size',
      'min_order_size',
      'maker_fee_rate',
      'taker_fee_rate',
    ]
    numericFields.forEach((field) => {
      const parsed = parseFloat(form[field])
      payload[field] = Number.isFinite(parsed) ? parsed : null
    })

    if (!payload.symbol) {
      setLocalError('Symbol is required for instrument metadata')
      return
    }
    if (payload.tick_size == null && payload.tick_value == null) {
      setLocalError('Provide at least a tick size or tick value')
      return
    }
    setLocalError(null)
    await onSubmit?.(payload)
  }

  const errorMessage = localError || error

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-xl space-y-6 rounded-2xl border border-white/10 bg-[#1b1e28] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">Add instrument metadata</h3>
          <p className="text-sm text-slate-400">
            Define tick sizes, contract multipliers, and fee assumptions for this symbol.
          </p>
        </header>

        <form className="space-y-4" onSubmit={handleSubmit}>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Symbol</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.25em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.symbol}
                onChange={handleChange('symbol')}
                required
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Datasource</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.3em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.datasource}
                onChange={handleChange('datasource')}
                placeholder="e.g. CCXT"
              />
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Exchange</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.exchange}
                onChange={handleChange('exchange')}
                placeholder="e.g. binanceus"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Quote currency</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.2em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.quote_currency}
                onChange={handleChange('quote_currency')}
                placeholder="e.g. USDT"
              />
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Tick size</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.tick_size}
                onChange={handleChange('tick_size')}
                placeholder="0.0001"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Tick value</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.tick_value}
                onChange={handleChange('tick_value')}
                placeholder="e.g. 0.01"
              />
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Contract size</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.contract_size}
                onChange={handleChange('contract_size')}
                placeholder="1"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Min order size</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.min_order_size}
                onChange={handleChange('min_order_size')}
                placeholder="0.01"
              />
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Maker fee %</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.maker_fee_rate}
                onChange={handleChange('maker_fee_rate')}
                placeholder="0.001"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Taker fee %</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.taker_fee_rate}
                onChange={handleChange('taker_fee_rate')}
                placeholder="0.001"
              />
            </div>
          </div>

          {errorMessage && <p className="text-xs text-rose-300">{errorMessage}</p>}

          <footer className="flex items-center justify-end gap-2">
            <ActionButton type="button" variant="ghost" onClick={onCancel} disabled={submitting}>
              Cancel
            </ActionButton>
            <ActionButton type="submit" disabled={submitting}>
              {submitting ? 'Saving…' : 'Save metadata'}
            </ActionButton>
          </footer>
        </form>
      </div>
    </div>
  )
}

const StrategyList = ({ strategies, selectedId, onSelect }) => {
  if (!strategies.length) {
    return (
      <div className="rounded-2xl border border-white/10 bg-white/5 p-6 text-center text-sm text-slate-400">
        No strategies yet. Create your first blueprint to combine indicators into rules.
      </div>
    )
  }

  return (
    <ul className="space-y-2">
      {strategies.map((strategy) => {
        const isActive = strategy.id === selectedId
        return (
          <li key={strategy.id}>
            <button
              onClick={() => onSelect(strategy.id)}
              className={`w-full rounded-2xl border px-4 py-3 text-left transition ${
                isActive
                  ? 'border-white/30 bg-white/10 text-white'
                  : 'border-white/10 bg-[#111726] text-slate-200 hover:border-white/20 hover:bg-[#1a2133]'
              }`}
            >
              <div className="flex items-center justify-between">
                <div>
                  <h4 className="text-sm font-semibold">{strategy.name}</h4>
                  <p className="text-xs text-slate-400">
                    {strategy.timeframe} • {strategy.symbols.join(', ')}
                  </p>
                </div>
                <span className="rounded-full bg-black/40 px-2 py-1 text-[11px] uppercase tracking-[0.2em] text-slate-400">
                  {Array.isArray(strategy.rules) ? strategy.rules.length : 0} rules
                </span>
              </div>
            </button>
          </li>
        )
      })}
    </ul>
  )
}

function AttachedIndicators({ strategy, attached, availableIndicators, onAttach, onDetach }) {
  const [selected, setSelected] = useState('')

  useEffect(() => {
    setSelected('')
  }, [strategy?.id])

  const handleAttach = async (event) => {
    event.preventDefault()
    if (!selected) return
    await onAttach(selected)
    setSelected('')
  }

  const entries = Array.isArray(attached) ? attached : []

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <form onSubmit={handleAttach} className="flex flex-1 items-center gap-2">
          <div className="flex-1">
            <DropdownSelect
              label="Indicator"
              value={selected}
              onChange={setSelected}
              placeholder="Attach indicator…"
              options={availableIndicators.map((indicator) => ({
                value: indicator.id,
                label: indicator.name || indicator.type,
              }))}
              disabled={!availableIndicators.length}
              className="w-full"
            />
          </div>
          <ActionButton type="submit" disabled={!selected}>
            Attach
          </ActionButton>
        </form>
      </div>

      {entries.length === 0 ? (
        <div className="rounded-xl border border-dashed border-white/10 bg-black/30 p-4 text-sm text-slate-400">
          No indicators linked yet.
        </div>
      ) : (
        <div className="space-y-3">
          {entries.map((entry) => {
            const isMissing = entry.status !== 'active'
            const params = entry.params || entry.snapshot?.params || {}
            const signals = Array.isArray(entry.signal_rules)
              ? entry.signal_rules
              : Array.isArray(entry.meta?.signal_rules)
                ? entry.meta.signal_rules
                : []
            const related = Array.isArray(entry.strategies) ? entry.strategies : []
            const otherStrategies = related.filter((s) => s.id && s.id !== strategy.id)

            const highlightTokens = []
            const pivotConfirm = params.pivot_breakout_confirmation_bars
            const marketProfileConfirm = params.market_profile_breakout_confirmation_bars
            const retestTolerance = params.market_profile_retest_tolerance_pct
            const binSize = params.bin_size
            const merged = params.market_profile_use_merged_value_areas

            if (pivotConfirm != null && pivotConfirm !== '') {
              highlightTokens.push(`Pivot confirm: ${pivotConfirm} bar${Number(pivotConfirm) === 1 ? '' : 's'}`)
            }
            if (marketProfileConfirm != null && marketProfileConfirm !== '') {
              highlightTokens.push(
                `MP confirm: ${marketProfileConfirm} bar${Number(marketProfileConfirm) === 1 ? '' : 's'}`,
              )
            }
            if (retestTolerance != null && retestTolerance !== '') {
              const numeric = Number(retestTolerance)
              const pctLabel = Number.isFinite(numeric) ? `${(numeric * 100).toFixed(2)}%` : String(retestTolerance)
              highlightTokens.push(`Retest tolerance: ${pctLabel}`)
            }
            if (binSize != null && binSize !== '') {
              highlightTokens.push(`Bin size: ${binSize}`)
            }
            if (merged != null && merged !== '') {
              const mergedLabel = merged === true || String(merged).toLowerCase() === 'true'
                ? 'Merged value areas'
                : 'Session value areas'
              highlightTokens.push(mergedLabel)
            }

            const renderSignalBadge = (rule) => {
              const baseLabel = rule?.label || rule?.id || 'Signal'
              const signalType = rule?.signal_type ? rule.signal_type.toUpperCase() : null
              const directionHint = Array.isArray(rule?.directions) && rule.directions.length === 1
                ? String(rule.directions[0].id || '').toLowerCase()
                : null
              const directionIcon = directionHint === 'long' ? '↗' : directionHint === 'short' ? '↘' : null
              const directionText = directionHint === 'long'
                ? 'Long'
                : directionHint === 'short'
                  ? 'Short'
                  : null
              return (
                <span
                  key={`${entry.id}-${rule?.id || baseLabel}`}
                  className="inline-flex items-center gap-1 rounded-lg border border-white/12 bg-white/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.15em] text-slate-200"
                >
                  {signalType || baseLabel}
                  {directionText && (
                    <span className={directionHint === 'long' ? 'text-emerald-300' : 'text-rose-300'}>
                      {directionIcon} {directionText}
                    </span>
                  )}
                </span>
              )
            }

            return (
              <div
                key={entry.id}
                className={`rounded-2xl border p-4 transition ${
                  isMissing
                    ? 'border-rose-500/40 bg-rose-500/10 text-rose-100'
                    : 'border-white/10 bg-white/5 text-slate-100'
                }`}
              >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div className="min-w-0">
                    <h5 className="text-sm font-semibold text-white truncate">
                      {entry.name || entry.type || entry.id}
                    </h5>
                    <p className="text-xs text-slate-300">
                      {entry.type || entry.snapshot?.meta?.type || 'Custom'} • {signals.length}{' '}
                      signal{signals.length === 1 ? '' : 's'} available
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span
                      className={`rounded-full px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${
                        isMissing
                          ? 'border border-rose-400/60 bg-rose-500/20 text-rose-100'
                          : 'border border-white/15 bg-black/40 text-slate-200'
                      }`}
                    >
                      {isMissing ? 'Missing' : 'Active'}
                    </span>
                    <button
                      className="rounded-full border border-white/20 px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-slate-200 hover:border-rose-400/70 hover:text-rose-200"
                      type="button"
                      onClick={() => onDetach(entry.id)}
                    >
                      Remove
                    </button>
                  </div>
                </div>

                <dl className="mt-3 grid gap-3 text-[11px] text-slate-300 md:grid-cols-3">
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Signals</dt>
                    <dd className="mt-1 flex flex-wrap gap-1">
                      {signals.length ? (
                        signals.map(renderSignalBadge)
                      ) : (
                        <span className="text-slate-500">No signals registered</span>
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Configuration</dt>
                    <dd className="mt-1 flex flex-wrap gap-1">
                      {highlightTokens.length ? (
                        highlightTokens.map((token) => (
                          <span
                            key={`${entry.id}-${token}`}
                            className="rounded-lg border border-white/12 bg-white/5 px-2 py-0.5 font-semibold text-[10px] uppercase tracking-[0.15em] text-slate-200"
                          >
                            {token}
                          </span>
                        ))
                      ) : (
                        <span className="text-slate-500">Default parameters</span>
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Usage</dt>
                    <dd className="mt-1 font-semibold text-slate-100">
                      {otherStrategies.length
                        ? `${otherStrategies.length} other strateg${otherStrategies.length === 1 ? 'y' : 'ies'}`
                        : 'Only used here'}
                    </dd>
                  </div>
                </dl>

                {otherStrategies.length > 0 && (
                  <div className="mt-3 rounded-xl border border-white/10 bg-black/30 p-3 text-xs text-slate-300">
                    <p className="font-semibold text-slate-200">Also used in:</p>
                    <ul className="mt-1 space-y-1">
                      {otherStrategies.map((item) => (
                        <li key={`${entry.id}-strategy-${item.id}`} className="flex items-center justify-between">
                          <span className="truncate text-[11px] text-slate-300">{item.name || item.id}</span>
                          <span className="text-[10px] uppercase tracking-[0.2em] text-slate-500">
                            {Array.isArray(item.rules) ? item.rules.length : 0} rules
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

const ConditionBadge = ({ label, signalType, direction, ruleId }) => {
  const normalizedDirection = typeof direction === 'string' ? direction.toLowerCase() : ''
  const ruleLabel = typeof ruleId === 'string' && ruleId.trim().length
    ? ruleId.replace(/_/g, ' ').toUpperCase()
    : ''

  const directionConfig = {
    label: 'Any bias',
    icon: '•',
    classes: 'border-white/12 bg-white/5 text-slate-200',
  }

  if (normalizedDirection === 'long') {
    directionConfig.label = 'Long bias'
    directionConfig.icon = '↗'
    directionConfig.classes = 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200'
  } else if (normalizedDirection === 'short') {
    directionConfig.label = 'Short bias'
    directionConfig.icon = '↘'
    directionConfig.classes = 'border-rose-500/40 bg-rose-500/15 text-rose-200'
  }

  return (
    <div className="flex min-w-[220px] items-stretch gap-3 rounded-2xl border border-white/12 bg-[#141a26] px-3 py-2">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="truncate text-xs font-semibold text-white">{label}</span>
          {ruleLabel ? (
            <span className="rounded-md border border-white/10 bg-white/5 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.25em] text-slate-400">
              {ruleLabel}
            </span>
          ) : null}
        </div>
        <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] text-slate-300">
          <span className="inline-flex items-center rounded-md border border-white/10 bg-white/5 px-2 py-0.5 uppercase tracking-[0.25em]">
            {signalType ? signalType.toUpperCase() : 'SIGNAL'}
          </span>
          <span className={`inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10px] font-semibold ${directionConfig.classes}`}>
            <span>{directionConfig.icon}</span>
            {directionConfig.label}
          </span>
        </div>
      </div>
    </div>
  )
}

function RuleList({ rules, onEdit, onDelete, indicatorLookup }) {
  if (!rules.length) {
    return (
      <p className="rounded-xl border border-white/10 bg-white/5 p-4 text-sm text-slate-400">
        No rules yet. Create at least one BUY or SELL rule to generate signals.
      </p>
    )
  }

  return (
    <div className="space-y-3">
      {rules.map((rule) => (
        <div
          key={rule.id}
          className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200"
        >
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <p className="text-sm font-semibold text-white">{rule.name}</p>
              <p className="text-xs text-slate-400">
                {rule.action?.toUpperCase()} • {rule.match === 'any' ? 'Any condition triggers' : 'All conditions required'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <span
                className={`rounded-full px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${
                  rule.enabled
                    ? 'bg-emerald-500/20 text-emerald-200'
                    : 'bg-slate-700/60 text-slate-400'
                }`}
              >
                {rule.enabled ? 'Enabled' : 'Disabled'}
              </span>
              <ActionButton variant="ghost" onClick={() => onEdit(rule)}>
                Edit
              </ActionButton>
              <ActionButton variant="danger" onClick={() => onDelete(rule)}>
                Delete
              </ActionButton>
            </div>
          </div>
          {rule.description && (
            <p className="mt-3 text-xs text-slate-400">{rule.description}</p>
          )}

          <div className="mt-3 space-y-2 text-xs text-slate-300">
            {Array.isArray(rule.conditions) && rule.conditions.length ? (
              <div className="flex flex-wrap items-center gap-2">
                {rule.conditions.map((condition, index) => {
                  const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
                  const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id
                  const connectorLabel = rule.match === 'any' ? 'OR' : 'AND'
                  return (
                    <Fragment key={`${rule.id}-condition-${index}`}>
                      <ConditionBadge
                        label={label}
                        signalType={condition.signal_type}
                        direction={condition.direction}
                        ruleId={condition.rule_id || condition.signal_type}
                      />
                      {index < rule.conditions.length - 1 && (
                        <span className="rounded-md border border-white/10 bg-[#111622] px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.3em] text-slate-400">
                          {connectorLabel}
                        </span>
                      )}
                    </Fragment>
                  )
                })}
              </div>
            ) : (
              <p className="text-[11px] text-slate-400">No conditions configured.</p>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}

function SignalSummary({ result }) {
  if (!result) return null

  const {
    window,
    buy_signals: buys = [],
    sell_signals: sells = [],
    rule_results: rules = [],
    status,
    missing_indicators: missingIndicatorsRaw = [],
  } = result

  const matchedRules = rules.filter((entry) => entry?.matched).length
  const totalRules = rules.length
  const missingIndicators = Array.isArray(missingIndicatorsRaw)
    ? missingIndicatorsRaw.filter(Boolean)
    : []
  const buySignalCount = buys.reduce((total, entry) => {
    if (!entry) return total
    const signalCount = Array.isArray(entry.signals) && entry.signals.length
      ? entry.signals.length
      : entry.matched
        ? 1
        : 0
    return total + signalCount
  }, 0)
  const sellSignalCount = sells.reduce((total, entry) => {
    if (!entry) return total
    const signalCount = Array.isArray(entry.signals) && entry.signals.length
      ? entry.signals.length
      : entry.matched
        ? 1
        : 0
    return total + signalCount
  }, 0)
  const buyRuleMatches = buys.length
  const sellRuleMatches = sells.length
  const statusLabel = status === 'missing_indicators' ? 'Missing indicators' : 'Complete'
  const statusClasses =
    status === 'missing_indicators'
      ? 'border-amber-500/40 bg-amber-500/10 text-amber-200'
      : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200'

  return (
    <div className="space-y-4 rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] p-4 text-sm text-slate-200">
      <div>
        <h4 className="text-sm font-semibold text-white">Evaluation window</h4>
        <p className="text-xs text-slate-400">
          {window?.start || 'start ?'} → {window?.end || 'end ?'} • {window?.interval || 'interval ?'} •{' '}
          {window?.symbol || 'symbol ?'}
          {window?.datasource ? ` • ${window.datasource}` : ''}
          {window?.exchange ? ` (${window.exchange})` : ''}
        </p>
        <span className={`mt-2 inline-flex rounded-full border px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${statusClasses}`}>
          {statusLabel}
        </span>
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-3 text-emerald-100">
          <p className="text-xs uppercase tracking-[0.3em] text-emerald-200/80">Buy</p>
          <p className="text-lg font-semibold">{buySignalCount}</p>
          <p className="text-[11px] text-emerald-200/70">
            signals · {buyRuleMatches || 0} rule{buyRuleMatches === 1 ? '' : 's'} matched
          </p>
        </div>
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-rose-100">
          <p className="text-xs uppercase tracking-[0.3em] text-rose-200/80">Sell</p>
          <p className="text-lg font-semibold">{sellSignalCount}</p>
          <p className="text-[11px] text-rose-200/70">
            signals · {sellRuleMatches || 0} rule{sellRuleMatches === 1 ? '' : 's'} matched
          </p>
        </div>
        <div className="rounded-xl border border-indigo-500/30 bg-indigo-500/10 p-3 text-indigo-100">
          <p className="text-xs uppercase tracking-[0.3em] text-indigo-200/80">Rules</p>
          <p className="text-lg font-semibold">
            {matchedRules}
            <span className="text-sm text-indigo-200/80">/{totalRules || 0}</span>
          </p>
        </div>
      </div>

      {missingIndicators.length > 0 && (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-100">
          <p className="font-semibold text-amber-200">Indicators unavailable</p>
          <p className="mt-1 text-amber-100/90">
            Recreate or reattach the following indicators before running live checks:
          </p>
          <ul className="mt-1 list-disc space-y-1 pl-4">
            {missingIndicators.map((identifier) => (
              <li key={`missing-${identifier}`} className="text-amber-100">
                {identifier}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

const StrategyDetails = ({
  strategy,
  attachedIndicators,
  availableIndicators,
  indicatorLookup,
  onEdit,
  onDelete,
  onAttachIndicator,
  onDetachIndicator,
  onAddRule,
  onEditRule,
  onDeleteRule,
  onRunSignals,
  signalWindow,
  setSignalWindow,
  signalResult,
  signalsLoading,
  onAddInstrument = () => {},
}) => {
  const hasStrategy = Boolean(strategy)
  const strategyInstruments = Array.isArray(strategy?.instruments) ? strategy.instruments : EMPTY_LIST
  const strategyInstrumentMessages = Array.isArray(strategy?.instrument_messages)
    ? strategy.instrument_messages
    : EMPTY_LIST
  const strategyDatasource = strategy?.datasource || ''
  const strategyExchange = strategy?.exchange || ''

  const handleDateRangeChange = (range) => {
    setSignalWindow((prev) => ({ ...prev, dateRange: range }))
  }

  const handleWindowChange = (field) => (input) => {
    let value = input && typeof input === 'object' && 'target' in input
      ? input.target.value
      : input

    if (field === 'datasource' && typeof value === 'string') {
      value = value.toUpperCase()
    }

    setSignalWindow((prev) => ({ ...prev, [field]: value }))
  }

  const instrumentMap = useMemo(() => {
    const map = new Map()
    for (const entry of strategyInstruments) {
      const key = (entry.symbol || '').toUpperCase()
      if (key) {
        map.set(key, entry)
      }
    }
    return map
  }, [strategyInstruments])

  const instrumentMessages = strategyInstrumentMessages

  const formatInstrumentNumber = useCallback((value) => {
    if (value === null || value === undefined || value === '') {
      return '—'
    }
    const numeric = Number(value)
    if (Number.isFinite(numeric)) {
      if (Math.abs(numeric) >= 1) {
        return numeric.toLocaleString(undefined, { maximumFractionDigits: 4 })
      }
      return numeric.toPrecision(4)
    }
    return value
  }, [])

  const handleAddInstrument = useCallback(
    (symbol) => {
      if (!symbol) return
      onAddInstrument({
        symbol,
        datasource: strategyDatasource,
        exchange: strategyExchange,
      })
    },
    [onAddInstrument, strategyDatasource, strategyExchange],
  )

  const handleSubmit = async (event) => {
    event.preventDefault()
    await onRunSignals(signalWindow)
  }

  if (!hasStrategy) {
    return (
      <div className="rounded-2xl border border-dashed border-white/10 bg-[#121520] p-6 text-center text-sm text-slate-400">
        Select a strategy to manage indicators, rules, and signal evaluations.
      </div>
    )
  }

  return (
    <div className="space-y-8">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-lg font-semibold text-white">{strategy.name}</h3>
          <p className="text-sm text-slate-400">
            {strategy.timeframe} • {strategy.symbols.join(', ')}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <ActionButton variant="ghost" onClick={onEdit}>
            Edit
          </ActionButton>
          <ActionButton variant="danger" onClick={onDelete}>
            Delete
          </ActionButton>
        </div>
      </header>

      {Array.isArray(strategy.missing_indicators) && strategy.missing_indicators.length > 0 && (
        <div className="rounded-xl border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-100">
          <p className="font-semibold text-amber-200">{strategy.missing_indicators.length} indicator(s) unavailable</p>
          <p className="mt-1 text-amber-100/90">
            Recreate these indicators or detach them from the strategy to restore evaluations:
          </p>
          <ul className="mt-1 list-disc space-y-1 pl-4">
            {strategy.missing_indicators.map((identifier) => (
              <li key={`strategy-missing-${identifier}`} className="text-amber-100">
                {identifier}
              </li>
            ))}
          </ul>
        </div>
      )}

      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-white">Instrument metadata</h4>
        </div>
        {instrumentMessages.length > 0 && (
          <div className="rounded-xl border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-100">
            <p className="font-semibold text-amber-200">Metadata notes</p>
            <ul className="mt-1 space-y-1">
              {instrumentMessages.map((entry, idx) => (
                <li key={`${entry.symbol || 'instrument'}-${idx}`}>
                  <span className="font-semibold">{entry.symbol || 'Symbol'}:</span>{' '}
                  {entry.message || 'No metadata stored'}
                </li>
              ))}
            </ul>
          </div>
        )}
        <div className="space-y-3">
          {(strategy.symbols || []).map((symbol) => {
            const key = (symbol || '').toUpperCase()
            const record = key ? instrumentMap.get(key) : null
            const hasMetadata = record && (record.tick_size != null || record.tick_value != null || record.contract_size != null)
            return (
              <div key={key || symbol} className="rounded-2xl border border-white/10 bg-[#111726] p-4 text-sm text-slate-200">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Symbol</p>
                    <p className="text-lg font-semibold text-white">{symbol || '—'}</p>
                  </div>
                  <ActionButton variant="ghost" onClick={() => handleAddInstrument(symbol)}>
                    {hasMetadata ? 'Update metadata' : 'Add metadata'}
                  </ActionButton>
                </div>
                {hasMetadata ? (
                  <dl className="mt-3 grid gap-3 text-xs text-slate-300 md:grid-cols-2">
                    <div>
                      <dt className="uppercase tracking-[0.3em] text-slate-500">Tick size</dt>
                      <dd className="text-base text-white">{formatInstrumentNumber(record.tick_size)}</dd>
                    </div>
                    <div>
                      <dt className="uppercase tracking-[0.3em] text-slate-500">Tick value</dt>
                      <dd className="text-base text-white">
                        {formatInstrumentNumber(record.tick_value)}{' '}
                        {record.quote_currency || ''}
                      </dd>
                    </div>
                    <div>
                      <dt className="uppercase tracking-[0.3em] text-slate-500">Contract size</dt>
                      <dd className="text-base text-white">{formatInstrumentNumber(record.contract_size)}</dd>
                    </div>
                    <div>
                      <dt className="uppercase tracking-[0.3em] text-slate-500">Maker / Taker fees</dt>
                      <dd className="text-base text-white">
                        {record.maker_fee_rate != null
                          ? `${(Number(record.maker_fee_rate) * 100).toFixed(2)}%`
                          : '—'}{' '}
                        /{' '}
                        {record.taker_fee_rate != null
                          ? `${(Number(record.taker_fee_rate) * 100).toFixed(2)}%`
                          : '—'}
                      </dd>
                    </div>
                    <div>
                      <dt className="uppercase tracking-[0.3em] text-slate-500">Min order size</dt>
                      <dd className="text-base text-white">{formatInstrumentNumber(record.min_order_size)}</dd>
                    </div>
                  </dl>
                ) : (
                  <p className="mt-3 text-sm text-slate-400">No tick or fee metadata stored yet.</p>
                )}
              </div>
            )
          })}
        </div>
      </section>

      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-white">ATM template</h4>
          <p className="text-xs text-slate-400">Distribution of targets, contracts, and trailing rules.</p>
        </div>
        <ATMTemplateSummary template={strategy.atm_template} />
      </section>

      <section className="space-y-4">
        <h4 className="text-sm font-semibold text-white">Indicators</h4>
        <AttachedIndicators
          strategy={strategy}
          attached={attachedIndicators}
          availableIndicators={availableIndicators}
          onAttach={onAttachIndicator}
          onDetach={onDetachIndicator}
        />
      </section>

      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-white">Rules</h4>
          <ActionButton onClick={onAddRule}>New rule</ActionButton>
        </div>
        <RuleList
          rules={Array.isArray(strategy.rules) ? strategy.rules : []}
          onEdit={onEditRule}
          onDelete={onDeleteRule}
          indicatorLookup={indicatorLookup}
        />
      </section>

      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-white">Signal check</h4>
        </div>
        <form onSubmit={handleSubmit} className="space-y-4 rounded-2xl border border-white/10 bg-white/5 p-4 text-sm">
          <DateRangePickerComponent
            dateRange={signalWindow.dateRange}
            setDateRange={handleDateRangeChange}
          />

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Interval
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={signalWindow.interval}
                onChange={handleWindowChange('interval')}
                placeholder={strategy.timeframe || '15m'}
              />
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Symbol
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-300 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={strategy.symbols?.[0] || signalWindow.symbol}
                readOnly
              />
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <DropdownSelect
                label="Data source (market data)"
                value={signalWindow.datasource || strategy.datasource || ''}
                onChange={handleWindowChange('datasource')}
                options={[
                  {
                    value: '',
                    label: `Use strategy data source (${strategy.datasource || DEFAULT_DATASOURCE})`,
                    description: 'Follow the strategy default',
                  },
                  { value: 'ALPACA', label: 'Market data • ALPACA' },
                  { value: 'IBKR', label: 'Interactive Brokers • IBKR' },
                  { value: 'CCXT', label: 'Crypto data • CCXT' },
                ]}
                className="mt-1 w-full"
              />
              <p className="mt-1 text-[11px] text-slate-500">
                Choose the provider used to load candles when checking these rules.
              </p>
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Broker / Exchange
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={signalWindow.exchange || strategy.exchange || ''}
                onChange={handleWindowChange('exchange')}
                placeholder="e.g. ALPACA, BINANCE"
              />
              <p className="mt-1 text-[11px] text-slate-500">
                Specify where trades would be routed in the future.
              </p>
            </div>
          </div>

          <div className="flex items-end justify-end">
            <ActionButton type="submit" disabled={signalsLoading} className="w-full justify-center md:w-auto">
              {signalsLoading ? 'Running…' : 'Generate signals'}
            </ActionButton>
          </div>
        </form>

        {signalResult && <SignalSummary result={signalResult} />}
      </section>
    </div>
  )
}

const StrategyTab = ({ chartId }) => {
  const { getChart, updateChart } = useChartState()
  const chartSnapshot = getChart(chartId)
  const logger = useMemo(() => createLogger('StrategyTab', { chartId }), [chartId])
  const { info, warn, error } = logger

  const [strategies, setStrategies] = useState([])
  const [selectedId, setSelectedId] = useState(null)
  const [indicators, setIndicators] = useState([])
  const [loading, setLoading] = useState(false)
  const [errorMessage, setErrorMessage] = useState(null)
  const [strategyModal, setStrategyModal] = useState({ open: false, strategy: null })
  const [ruleModal, setRuleModal] = useState({ open: false, rule: null })
  const [savingStrategy, setSavingStrategy] = useState(false)
  const [savingRule, setSavingRule] = useState(false)
  const [signalsLoading, setSignalsLoading] = useState(false)
  const [signalResult, setSignalResult] = useState(null)
  const [instrumentModal, setInstrumentModal] = useState({ open: false, defaults: null })
  const [savingInstrument, setSavingInstrument] = useState(false)
  const [instrumentError, setInstrumentError] = useState(null)
  const [signalWindow, setSignalWindow] = useState(() => {
    const end = new Date()
    const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000)
    return {
      dateRange: [start, end],
      interval: '15m',
      symbol: '',
      datasource: '',
      exchange: '',
    }
  })

  const selectedStrategy = useMemo(
    () => strategies.find((strategy) => strategy.id === selectedId) || null,
    [strategies, selectedId],
  )

  const atmTemplateKey = useCallback((template) => {
    try {
      return JSON.stringify(template || {})
    } catch (err) {
      console.error('Failed to stringify ATM template', err)
      return ''
    }
  }, [])

  const availableATMTemplates = useMemo(() => {
    const seen = new Set()
    const uniqueTemplates = []

    const pushTemplate = (id, label, template) => {
      const key = atmTemplateKey(template)
      if (!key || seen.has(key)) return
      seen.add(key)
      uniqueTemplates.push({ id, label, template })
    }

    pushTemplate('default-atm', 'Default ATM template', DEFAULT_ATM_TEMPLATE)
    strategies.forEach((strategy, index) => {
      if (!strategy?.atm_template) return
      const label = strategy.name ? `${strategy.name} ATM` : `Strategy ATM ${index + 1}`
      pushTemplate(`strategy-${strategy.id || index}`, label, strategy.atm_template)
    })

    return uniqueTemplates
  }, [atmTemplateKey, strategies])

  const openInstrumentModal = useCallback(
    (defaults = {}) => {
      const fallbackSymbol = defaults.symbol || selectedStrategy?.symbols?.[0] || ''
      setInstrumentError(null)
      setInstrumentModal({
        open: true,
        defaults: {
          symbol: fallbackSymbol,
          datasource: defaults.datasource || selectedStrategy?.datasource || '',
          exchange: defaults.exchange || selectedStrategy?.exchange || '',
        },
      })
    },
    [selectedStrategy],
  )

  const closeInstrumentModal = useCallback(() => {
    setInstrumentModal({ open: false, defaults: null })
  }, [])

  const indicatorLookup = useMemo(() => {
    const map = new Map()
    for (const indicator of indicators) {
      map.set(indicator.id, indicator)
    }
    return map
  }, [indicators])

  const ensureIndicatorDetails = useCallback(
    async (indicatorId) => {
      if (typeof indicatorId !== 'string') {
        return null
      }
      const trimmed = indicatorId.trim()
      if (!trimmed.length) {
        return null
      }
      const existing = indicatorLookup.get(trimmed)
      if (existing?.signal_rules && existing.signal_rules.length > 0) {
        return existing
      }
      try {
        const [payload, relatedStrategies] = await Promise.all([
          fetchIndicator(trimmed),
          fetchIndicatorStrategies(trimmed).catch(() => []),
        ])
        if (!payload) {
          return existing || null
        }
        const enriched = {
          ...payload,
          strategies: Array.isArray(relatedStrategies) ? relatedStrategies : [],
        }
        setIndicators((prev) => {
          const map = new Map(prev.map((indicator) => [indicator.id, indicator]))
          const merged = { ...(map.get(enriched.id) || {}), ...enriched }
          map.set(enriched.id, merged)
          return Array.from(map.values())
        })
        return enriched
      } catch (err) {
        warn('indicator_detail_fetch_failed', { indicatorId: trimmed }, err)
        return existing || null
      }
    },
    [indicatorLookup, setIndicators, warn],
  )

  const attachedIndicators = useMemo(() => {
    if (!selectedStrategy) {
      return []
    }
    const entries = Array.isArray(selectedStrategy.indicators)
      ? selectedStrategy.indicators
      : []
    return entries.map((entry) => {
      const lookupMeta = indicatorLookup.get(entry.id) || {}
      const mergedMeta = {
        ...entry.snapshot,
        ...entry.meta,
        ...lookupMeta,
      }
      return {
        ...mergedMeta,
        id: entry.id,
        status: entry.status || 'active',
        snapshot: entry.snapshot || {},
        strategies: lookupMeta.strategies || entry.meta?.strategies || [],
      }
    })
  }, [selectedStrategy, indicatorLookup])

  const indicatorsForRuleModal = useMemo(() => {
    if (!ruleModal?.rule) {
      return attachedIndicators
    }
    const existing = new Map(attachedIndicators.map((indicator) => [indicator.id, indicator]))
    const extras = []
    for (const condition of ruleModal.rule.conditions || []) {
      const indicatorId = condition.indicator_id
      if (!indicatorId || existing.has(indicatorId)) continue
      const meta = indicatorLookup.get(indicatorId)
      if (meta) {
        existing.set(indicatorId, meta)
        extras.push(meta)
      }
    }
    return [...existing.values()]
  }, [attachedIndicators, ruleModal?.rule, indicatorLookup])

  useEffect(() => {
    const nextSymbol = selectedStrategy?.symbols?.[0] || chartSnapshot?.symbol || ''
    const nextInterval = selectedStrategy?.timeframe || chartSnapshot?.interval || '15m'
    const nextDatasource = selectedStrategy?.datasource || chartSnapshot?.datasource || ''
    const nextExchange = selectedStrategy?.exchange || chartSnapshot?.exchange || ''
    const chartRange = Array.isArray(chartSnapshot?.dateRange) ? chartSnapshot.dateRange : null

    setSignalWindow((prev) => {
      const updates = { ...prev }
      let changed = false

      if (prev.symbol !== nextSymbol) {
        updates.symbol = nextSymbol
        changed = true
      }
      if (prev.interval !== nextInterval) {
        updates.interval = nextInterval
        changed = true
      }
      if ((prev.datasource || '') !== nextDatasource) {
        updates.datasource = nextDatasource
        changed = true
      }
      if ((prev.exchange || '') !== nextExchange) {
        updates.exchange = nextExchange
        changed = true
      }

      const hasValidRange = Array.isArray(prev.dateRange)
        && prev.dateRange[0] instanceof Date
        && !Number.isNaN(prev.dateRange[0]?.valueOf())
        && prev.dateRange[1] instanceof Date
        && !Number.isNaN(prev.dateRange[1]?.valueOf())

      if (!hasValidRange && Array.isArray(chartRange) && chartRange[0] instanceof Date && chartRange[1] instanceof Date) {
        updates.dateRange = chartRange
        changed = true
      }

      return changed ? updates : prev
    })
  }, [selectedStrategy?.id, selectedStrategy?.symbols, selectedStrategy?.timeframe, selectedStrategy?.datasource, selectedStrategy?.exchange, chartSnapshot?.symbol, chartSnapshot?.interval, chartSnapshot?.datasource, chartSnapshot?.exchange, chartSnapshot?.dateRange])

  const refreshStrategies = useCallback(async () => {
    setLoading(true)
    setErrorMessage(null)
    try {
      const payload = await fetchStrategies()
      const list = Array.isArray(payload) ? payload : []
      setStrategies(list)

      if (!list.length) {
        setSelectedId(null)
        return
      }

      if (!list.some((strategy) => strategy.id === selectedId)) {
        setSelectedId(list[0].id)
      }
    } catch (err) {
      const message = err?.message || 'Unable to load strategies'
      setErrorMessage(message)
      error('strategy_load_failed', err)
    } finally {
      setLoading(false)
    }
  }, [selectedId, error])

  const loadIndicators = useCallback(async () => {
    try {
      const payload = await fetchIndicators()
      setIndicators(Array.isArray(payload) ? payload : [])
    } catch (err) {
      warn('indicator_fetch_failed', err)
    }
  }, [warn])

  useEffect(() => {
    refreshStrategies()
  }, [refreshStrategies])

  useEffect(() => {
    loadIndicators()
  }, [loadIndicators])

  const openCreateStrategy = () => setStrategyModal({ open: true, strategy: null })
  const openEditStrategy = (strategy) => setStrategyModal({ open: true, strategy })
  const closeStrategyModal = () => setStrategyModal({ open: false, strategy: null })

  const openRuleModal = (rule = null) => setRuleModal({ open: true, rule })
  const closeRuleModal = () => setRuleModal({ open: false, rule: null })

  const handleStrategySubmit = async (payload) => {
    setSavingStrategy(true)
    setErrorMessage(null)
    try {
      if (strategyModal.strategy) {
        await updateStrategy(strategyModal.strategy.id, payload)
        info('strategy_updated', { strategyId: strategyModal.strategy.id })
      } else {
        await createStrategy(payload)
        info('strategy_created', { name: payload.name })
      }
      await refreshStrategies()
      closeStrategyModal()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to save strategy')
      error('strategy_save_failed', err)
    } finally {
      setSavingStrategy(false)
    }
  }

  const handleInstrumentSubmit = async (payload) => {
    setSavingInstrument(true)
    setInstrumentError(null)
    try {
      await createInstrument(payload)
      info('instrument_saved', { symbol: payload.symbol })
      await refreshStrategies()
      closeInstrumentModal()
    } catch (err) {
      setInstrumentError(err?.message || 'Failed to save instrument metadata')
      error('instrument_save_failed', err)
    } finally {
      setSavingInstrument(false)
    }
  }

  const handleDeleteStrategy = async (strategy) => {
    if (!strategy) return
    setErrorMessage(null)
    try {
      await deleteStrategy(strategy.id)
      info('strategy_deleted', { strategyId: strategy.id })
      if (selectedId === strategy.id) {
        setSelectedId(null)
      }
      await refreshStrategies()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to delete strategy')
      error('strategy_delete_failed', err)
    }
  }

  const handleAttachIndicator = async (indicatorId) => {
    if (!selectedStrategy) return
    setErrorMessage(null)
    try {
      await attachStrategyIndicator(selectedStrategy.id, indicatorId)
      info('strategy_indicator_attached', { strategyId: selectedStrategy.id, indicatorId })
      await refreshStrategies()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to attach indicator')
      error('strategy_indicator_attach_failed', err)
    }
  }

  const handleDetachIndicator = async (indicatorId) => {
    if (!selectedStrategy) return
    setErrorMessage(null)
    try {
      await detachStrategyIndicator(selectedStrategy.id, indicatorId)
      info('strategy_indicator_detached', { strategyId: selectedStrategy.id, indicatorId })
      await refreshStrategies()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to detach indicator')
      error('strategy_indicator_detach_failed', err)
    }
  }

  const handleRuleSubmit = async (payload) => {
    if (!selectedStrategy) return
    setSavingRule(true)
    setErrorMessage(null)
    try {
      if (ruleModal.rule) {
        await updateStrategyRule(selectedStrategy.id, ruleModal.rule.id, payload)
        info('strategy_rule_updated', { strategyId: selectedStrategy.id, ruleId: ruleModal.rule.id })
      } else {
        await createStrategyRule(selectedStrategy.id, payload)
        info('strategy_rule_created', { strategyId: selectedStrategy.id })
      }
      await refreshStrategies()
      closeRuleModal()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to save rule')
      error('strategy_rule_save_failed', err)
    } finally {
      setSavingRule(false)
    }
  }

  const handleDeleteRule = async (rule) => {
    if (!selectedStrategy) return
    setErrorMessage(null)
    try {
      await deleteStrategyRule(selectedStrategy.id, rule.id)
      info('strategy_rule_deleted', { strategyId: selectedStrategy.id, ruleId: rule.id })
      await refreshStrategies()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to delete rule')
      error('strategy_rule_delete_failed', err)
    }
  }

  const runSignals = async (window) => {
    if (!selectedStrategy) return
    const [startDate, endDate] = window.dateRange || []
    if (!(startDate instanceof Date) || Number.isNaN(startDate.valueOf()) || !(endDate instanceof Date) || Number.isNaN(endDate.valueOf())) {
      setErrorMessage('A valid start and end date are required to generate signals.')
      return
    }
    setSignalsLoading(true)
    setSignalResult(null)
    setErrorMessage(null)
    try {
      const symbol = selectedStrategy.symbols?.[0] || window.symbol || chartSnapshot?.symbol
      const interval = window.interval || selectedStrategy.timeframe || chartSnapshot?.interval || '15m'
      const datasource = window.datasource || selectedStrategy.datasource || chartSnapshot?.datasource || ''
      const exchange = window.exchange || selectedStrategy.exchange || chartSnapshot?.exchange || ''

      const result = await generateStrategySignals(selectedStrategy.id, {
        start: startDate.toISOString(),
        end: endDate.toISOString(),
        interval,
        symbol,
        datasource: datasource || undefined,
        exchange: exchange || undefined,
      })
      setSignalResult(result)
      info('strategy_signals_generated', { strategyId: selectedStrategy.id })

      const appliedInputs = result?.applied_inputs || {}
      const resolvedSymbol = appliedInputs.symbol || symbol
      const resolvedInterval = appliedInputs.timeframe || interval
      const resolvedDatasource = appliedInputs.datasource || datasource
      const resolvedExchange = appliedInputs.exchange || exchange

      const buyMarkers = Array.isArray(result?.chart_markers?.buy) ? result.chart_markers.buy : []
      const sellMarkers = Array.isArray(result?.chart_markers?.sell) ? result.chart_markers.sell : []
      const combinedMarkers = [...buyMarkers, ...sellMarkers]

      const existing = (getChart(chartId)?.overlays || []).filter(Boolean)
      const overlays = existing
        .filter((overlay) => !(overlay && overlay.source === 'strategy'))
        .filter(Boolean)

      if (combinedMarkers.length) {
        overlays.push({
          id: `strategy-${selectedStrategy.id}-signals`,
          source: 'strategy',
          strategyId: selectedStrategy.id,
          type: 'strategy',
          payload: { markers: combinedMarkers },
        })
      }

      const appliedDateRange = Array.isArray(window.dateRange)
        && window.dateRange[0] instanceof Date
        && window.dateRange[1] instanceof Date
          ? window.dateRange
          : undefined

      updateChart(chartId, {
        overlays,
        symbol: resolvedSymbol,
        interval: resolvedInterval,
        datasource: resolvedDatasource || null,
        exchange: resolvedExchange || null,
        dateRange: appliedDateRange,
      })
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to generate signals')
      error('strategy_signals_failed', err)
    } finally {
      setSignalsLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start gap-6">
        <div className="w-full max-w-sm space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase tracking-[0.3em] text-slate-400">
              Strategies
            </h2>
            <ActionButton onClick={openCreateStrategy}>New</ActionButton>
          </div>
          {loading ? (
            <div className="rounded-2xl border border-white/10 bg-white/5 p-6 text-center text-sm text-slate-400">
              Loading strategies…
            </div>
          ) : (
            <StrategyList strategies={strategies} selectedId={selectedId} onSelect={setSelectedId} />
          )}
          {errorMessage && (
            <p className="text-xs text-rose-300">{errorMessage}</p>
          )}
        </div>

        <div className="flex-1">
          <StrategyDetails
            strategy={selectedStrategy}
            attachedIndicators={attachedIndicators}
            availableIndicators={indicators}
            indicatorLookup={indicatorLookup}
            onEdit={() => openEditStrategy(selectedStrategy)}
            onDelete={() => handleDeleteStrategy(selectedStrategy)}
            onAttachIndicator={handleAttachIndicator}
            onDetachIndicator={handleDetachIndicator}
            onAddRule={() => openRuleModal(null)}
            onEditRule={(rule) => openRuleModal(rule)}
            onDeleteRule={handleDeleteRule}
            onRunSignals={runSignals}
            signalWindow={signalWindow}
            setSignalWindow={setSignalWindow}
            signalResult={signalResult}
            signalsLoading={signalsLoading}
            onAddInstrument={(defaults) => openInstrumentModal(defaults)}
          />
        </div>
      </div>

      <StrategyFormModal
        open={strategyModal.open}
        initialValues={strategyModal.strategy}
        onSubmit={handleStrategySubmit}
        onCancel={closeStrategyModal}
        submitting={savingStrategy}
        availableATMTemplates={availableATMTemplates}
      />

      <RuleFormModal
        open={ruleModal.open}
        initialValues={ruleModal.rule}
        indicators={indicatorsForRuleModal}
        ensureIndicatorMeta={ensureIndicatorDetails}
        onSubmit={handleRuleSubmit}
        onCancel={closeRuleModal}
        submitting={savingRule}
      />

      <InstrumentFormModal
        open={instrumentModal.open}
        initialValues={instrumentModal.defaults}
        onSubmit={handleInstrumentSubmit}
        onCancel={closeInstrumentModal}
        submitting={savingInstrument}
        error={instrumentError}
      />
    </div>
  )
}

export default StrategyTab

