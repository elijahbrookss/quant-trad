import { useCallback, useEffect, useMemo, useState } from 'react'

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
import { fetchIndicators, fetchIndicator } from '../adapters/indicator.adapter.js'
import { useChartState } from '../contexts/ChartStateContext.jsx'
import { createLogger } from '../utils/logger.js'
import { DateRangePickerComponent } from './ChartComponent/DateTimePickerComponent.jsx'

const STRATEGY_FORM_DEFAULT = {
  name: '',
  description: '',
  timeframe: '15m',
  datasource: '',
  exchange: '',
  symbols: '',
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

function StrategyFormModal({ open, initialValues, onSubmit, onCancel, submitting }) {
  const [form, setForm] = useState(STRATEGY_FORM_DEFAULT)

  useEffect(() => {
    if (!open) {
      setForm(STRATEGY_FORM_DEFAULT)
      return
    }

    if (initialValues) {
      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        timeframe: initialValues.timeframe || '15m',
        datasource: initialValues.datasource || '',
        exchange: initialValues.exchange || '',
        symbols: Array.isArray(initialValues.symbols)
          ? initialValues.symbols.join(', ')
          : initialValues.symbols || '',
      })
    } else {
      setForm(STRATEGY_FORM_DEFAULT)
    }
  }, [open, initialValues])

  if (!open) return null

  const handleChange = (field) => (event) => {
    const value = event.target.value
    setForm((prev) => ({ ...prev, [field]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const payload = {
      name: form.name.trim(),
      description: form.description.trim() || null,
      timeframe: form.timeframe.trim() || '15m',
      datasource: form.datasource.trim() || null,
      exchange: form.exchange.trim() || null,
      symbols: form.symbols
        .split(/[\s,;]+/)
        .map((token) => token.trim())
        .filter(Boolean),
    }
    if (Array.isArray(payload.symbols) && payload.symbols.length > 1) {
      payload.symbols = payload.symbols.slice(0, 1)
    }
    await onSubmit(payload)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-xl space-y-6 rounded-2xl border border-white/10 bg-[#1b1e28] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">
            {initialValues ? 'Edit strategy' : 'Create strategy'}
          </h3>
          <p className="text-sm text-slate-400">
            Define the baseline symbol universe and metadata for this strategy blueprint.
          </p>
        </header>

        <form className="space-y-4" onSubmit={handleSubmit}>
          <div>
            <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
              Name
            </label>
            <input
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              value={form.name}
              onChange={handleChange('name')}
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
              onChange={handleChange('description')}
            />
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Timeframe
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.timeframe}
                onChange={handleChange('timeframe')}
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Datasource
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.datasource}
                onChange={handleChange('datasource')}
                placeholder="optional"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Exchange
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.exchange}
                onChange={handleChange('exchange')}
                placeholder="optional"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Symbols
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.symbols}
                onChange={handleChange('symbols')}
                placeholder="e.g. BTCUSD, ETHUSD"
              />
            </div>
          </div>

          <footer className="flex items-center justify-end gap-2">
            <ActionButton type="button" variant="ghost" onClick={onCancel}>
              Cancel
            </ActionButton>
            <ActionButton type="submit" disabled={submitting}>
              {submitting ? 'Saving…' : 'Save strategy'}
            </ActionButton>
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

  const handleConditionIndicatorChange = (index) => (event) => {
    const indicatorId = event.target.value
    updateCondition(index, {
      indicator_id: indicatorId,
      rule_id: '',
      signal_type: '',
      direction: '',
    })
    if (indicatorId && typeof ensureIndicatorMeta === 'function') {
      ensureIndicatorMeta(indicatorId)
    }
  }

  const handleConditionRuleChange = (index) => (event) => {
    const ruleId = event.target.value
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
        rule_id: ruleId,
        signal_type: selectedRule?.signal_type || '',
        direction: defaultDirection || '',
      }
      return { ...prev, conditions: nextConditions }
    })
  }

  const handleConditionDirectionChange = (index) => (event) => {
    const direction = event.target.value
    updateCondition(index, { direction })
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

  const handleFieldChange = (field) => (event) => {
    const value = field === 'enabled' ? event.target.checked : event.target.value
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
                      <div>
                        <label className="block text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400">
                          Indicator
                        </label>
                        <select
                          className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                          value={condition.indicator_id}
                          onChange={handleConditionIndicatorChange(index)}
                          required
                        >
                          <option value="">Select indicator</option>
                          {indicators.map((indicator) => (
                            <option key={indicator.id} value={indicator.id}>
                              {indicator.name || indicator.type}
                            </option>
                          ))}
                        </select>
                      </div>

                      <div className="grid gap-3 md:grid-cols-2">
                        <div>
                          <label className="block text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400">
                            Signal type
                          </label>
                          <select
                            className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                            value={condition.rule_id}
                            onChange={handleConditionRuleChange(index)}
                            required
                            disabled={!condition.indicator_id}
                          >
                            <option value="">Select signal</option>
                            {ruleOptions.map((rule) => {
                              const parts = []
                              if (rule.signal_type) {
                                parts.push(rule.signal_type.toUpperCase())
                              }
                              if (rule.label && rule.label.toLowerCase() !== (rule.signal_type || '').toLowerCase()) {
                                parts.push(rule.label)
                              }
                              const optionLabel = parts.length ? parts.join(' – ') : rule.id
                              return (
                                <option key={rule.id} value={rule.id}>
                                  {optionLabel}
                                </option>
                              )
                            })}
                          </select>
                          {condition.indicator_id && !ruleOptions.length && (
                            <p className="mt-1 text-[11px] text-amber-300/80">
                              No registered signals were found for this indicator.
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
                          <label className="block text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400">
                            Direction filter
                          </label>
                          <select
                            className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                            value={condition.direction || ''}
                            onChange={handleConditionDirectionChange(index)}
                            disabled={!directionOptions.length}
                          >
                            <option value="">Any direction</option>
                            {directionOptions.map((direction) => (
                              <option key={direction.id} value={direction.id}>
                                {direction.label || direction.id}
                              </option>
                            ))}
                          </select>
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
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Action
              </label>
              <select
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.action}
                onChange={handleFieldChange('action')}
              >
                <option value="buy">Buy</option>
                <option value="sell">Sell</option>
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Confluence logic
              </label>
              <select
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.match}
                onChange={handleFieldChange('match')}
              >
                <option value="all">All conditions must match</option>
                <option value="any">Any condition can trigger</option>
              </select>
            </div>
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
                  ? 'border-[color:var(--accent-alpha-50)] bg-[color:var(--accent-alpha-20)] text-white shadow-[0_18px_40px_-20px_var(--accent-shadow-strong)]'
                  : 'border-white/10 bg-white/5 text-slate-200 hover:border-[color:var(--accent-alpha-30)] hover:bg-[color:var(--accent-alpha-10)]'
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

function AttachedIndicators({ strategy, indicators, onAttach, onDetach }) {
  const [selected, setSelected] = useState('')

  useEffect(() => {
    setSelected('')
  }, [strategy?.id])

  const indicatorById = useMemo(() => {
    const map = new Map()
    for (const indicator of indicators) {
      map.set(indicator.id, indicator)
    }
    return map
  }, [indicators])

  const handleAttach = async (event) => {
    event.preventDefault()
    if (!selected) return
    await onAttach(selected)
    setSelected('')
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <form onSubmit={handleAttach} className="flex flex-1 items-center gap-2">
          <select
            className="flex-1 rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            value={selected}
            onChange={(event) => setSelected(event.target.value)}
          >
            <option value="">Attach indicator…</option>
            {indicators.map((indicator) => (
              <option key={indicator.id} value={indicator.id}>
                {indicator.name || indicator.type}
              </option>
            ))}
          </select>
          <ActionButton type="submit" disabled={!selected}>
            Attach
          </ActionButton>
        </form>
      </div>

      <div className="flex flex-wrap gap-2">
        {(!Array.isArray(strategy.indicator_ids) || strategy.indicator_ids.length === 0) && (
          <span className="text-sm text-slate-400">No indicators linked yet.</span>
        )}
        {(Array.isArray(strategy.indicator_ids) ? strategy.indicator_ids : []).map((indicatorId) => {
          const meta = indicatorById.get(indicatorId)
          const label = meta?.name || meta?.type || indicatorId
          return (
            <span
              key={indicatorId}
              className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-200"
            >
              {label}
              <button
                className="rounded-full border border-white/20 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-slate-300 hover:border-rose-400/70 hover:text-rose-200"
                onClick={() => onDetach(indicatorId)}
                type="button"
              >
                Remove
              </button>
            </span>
          )
        })}
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
              rule.conditions.map((condition, index) => {
                const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
                const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id
                return (
                  <div
                    key={`${rule.id}-condition-${index}`}
                    className="rounded-lg border border-white/10 bg-black/30 px-3 py-2"
                  >
                    <p className="font-semibold text-white">{label}</p>
                    <p className="text-[11px] text-slate-400">
                      {condition.rule_id || condition.signal_type} •{' '}
                      {condition.signal_type ? condition.signal_type.toUpperCase() : 'Signal'}
                      {condition.direction ? ` • ${condition.direction.toUpperCase()}` : ''}
                    </p>
                  </div>
                )
              })
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
  } = result

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
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-3 text-emerald-100">
          <p className="text-xs uppercase tracking-[0.3em] text-emerald-200/80">Buy</p>
          <p className="text-lg font-semibold">{buys.length} matches</p>
        </div>
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-rose-100">
          <p className="text-xs uppercase tracking-[0.3em] text-rose-200/80">Sell</p>
          <p className="text-lg font-semibold">{sells.length} matches</p>
        </div>
      </div>

      <div className="space-y-2">
        <h5 className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
          Rule breakdown
        </h5>
        <ul className="space-y-2">
          {rules.map((entry) => (
            <li
              key={entry.rule_id}
              className="rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-xs text-slate-200"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="font-medium text-white">{entry.rule_name}</span>
                <span
                  className={`rounded-full px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${
                    entry.matched
                      ? 'bg-[color:var(--accent-alpha-40)] text-[color:var(--accent-text-strong)]'
                      : 'bg-slate-700/70 text-slate-300'
                  }`}
                >
                  {entry.matched ? 'Matched' : 'Skipped'}
                </span>
              </div>
              <div className="mt-2 space-y-2">
                {Array.isArray(entry.conditions) && entry.conditions.length > 0 && (
                  <div>
                    <p className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Conditions</p>
                    <ul className="mt-1 space-y-1">
                      {entry.conditions.map((condition, idx) => (
                        <li key={`${entry.rule_id}-condition-${idx}`} className="rounded-lg border border-white/10 bg-black/50 px-3 py-2">
                          <div className="flex items-center justify-between">
                            <span className="font-semibold text-white">{condition.indicator_id}</span>
                            <span className={`text-[10px] uppercase tracking-[0.2em] ${condition.matched ? 'text-emerald-200' : 'text-slate-400'}`}>
                              {condition.matched ? 'Matched' : 'Missed'}
                            </span>
                          </div>
                          <p className="mt-1 text-[11px] text-slate-400">
                            {condition.signal_type?.toUpperCase()}
                            {condition.direction ? ` • ${condition.direction.toUpperCase()}` : ''}
                            {condition.direction_detected ? ` → ${condition.direction_detected.toUpperCase()}` : ''}
                          </p>
                          {!condition.matched && condition.reason && (
                            <p className="text-[11px] text-slate-500">{condition.reason}</p>
                          )}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {Array.isArray(entry.signals) && entry.signals.length > 0 && (
                  <div>
                    <p className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Signals</p>
                    <ul className="mt-1 space-y-1">
                      {entry.signals.map((signal, idx) => {
                        const rawPrice = signal.metadata?.retest_close
                          ?? signal.metadata?.price
                          ?? signal.metadata?.level_price
                          ?? signal.metadata?.POC
                        const price = Number(rawPrice)
                        return (
                          <li key={`${entry.rule_id}-signal-${idx}`} className="rounded-lg border border-white/10 bg-black/60 px-3 py-2">
                            <p className="text-[11px] text-slate-300">
                              {signal.type?.toUpperCase()} • {signal.time}
                            </p>
                            {Number.isFinite(price) && (
                              <p className="text-[11px] text-slate-400">Price {price.toFixed(2)}</p>
                            )}
                            {signal.metadata?.direction && (
                              <p className="text-[11px] text-slate-400">Direction {signal.metadata.direction}</p>
                            )}
                          </li>
                        )
                      })}
                    </ul>
                  </div>
                )}
              </div>
              {!entry.matched && entry.reason && (
                <p className="mt-1 text-[11px] text-slate-400">{entry.reason}</p>
              )}
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

const StrategyDetails = ({
  strategy,
  indicators,
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
}) => {
  if (!strategy) {
    return (
      <div className="rounded-2xl border border-dashed border-white/10 bg-[#121520] p-6 text-center text-sm text-slate-400">
        Select a strategy to manage indicators, rules, and signal evaluations.
      </div>
    )
  }

  const handleDateRangeChange = (range) => {
    setSignalWindow((prev) => ({ ...prev, dateRange: range }))
  }

  const handleWindowChange = (field) => (event) => {
    const value = event.target.value
    setSignalWindow((prev) => ({ ...prev, [field]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    await onRunSignals(signalWindow)
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

      <section className="space-y-4">
        <h4 className="text-sm font-semibold text-white">Indicators</h4>
        <AttachedIndicators
          strategy={strategy}
          indicators={indicators}
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
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Data source (market data)
              </label>
              <select
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={signalWindow.datasource || strategy.datasource || ''}
                onChange={handleWindowChange('datasource')}
              >
                <option value="">
                  Use strategy data source ({strategy.datasource || 'ALPACA'})
                </option>
                <option value="ALPACA">Market data • ALPACA</option>
                <option value="CCXT">Crypto data • CCXT</option>
              </select>
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
        const payload = await fetchIndicator(trimmed)
        if (!payload) {
          return existing || null
        }
        setIndicators((prev) => {
          const map = new Map(prev.map((indicator) => [indicator.id, indicator]))
          const merged = { ...(map.get(payload.id) || {}), ...payload }
          map.set(payload.id, merged)
          return Array.from(map.values())
        })
        return payload
      } catch (err) {
        warn('indicator_detail_fetch_failed', { indicatorId: trimmed }, err)
        return existing || null
      }
    },
    [indicatorLookup, setIndicators, warn],
  )

  const selectedStrategy = useMemo(
    () => strategies.find((strategy) => strategy.id === selectedId) || null,
    [strategies, selectedId],
  )

  const attachedIndicators = useMemo(() => {
    if (!selectedStrategy || !Array.isArray(selectedStrategy.indicator_ids)) {
      return []
    }
    const allowed = new Set(selectedStrategy.indicator_ids)
    return indicators.filter((indicator) => allowed.has(indicator.id))
  }, [selectedStrategy, indicators])

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
      const remaining = existing.filter((overlay) => !(overlay?.source === 'strategy' && overlay?.strategyId === selectedStrategy.id))
      const overlays = combinedMarkers.length
        ? [
            ...remaining,
            {
              id: `strategy-${selectedStrategy.id}-signals`,
              source: 'strategy',
              strategyId: selectedStrategy.id,
              type: 'strategy',
              payload: { markers: combinedMarkers },
            },
          ]
        : remaining

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
            indicators={indicators}
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
          />
        </div>
      </div>

      <StrategyFormModal
        open={strategyModal.open}
        initialValues={strategyModal.strategy}
        onSubmit={handleStrategySubmit}
        onCancel={closeStrategyModal}
        submitting={savingStrategy}
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
    </div>
  )
}

export default StrategyTab

