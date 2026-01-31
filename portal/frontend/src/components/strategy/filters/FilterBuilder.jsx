import React from 'react'
import { listFilterTypes, operatorOptionsFor } from './registry.js'
import { buildPredicateDefaults } from './filterUtils.js'

const FieldSelect = ({ predicate, onChange }) => {
  const type = listFilterTypes().find((entry) => entry.key === predicate.source)
  const fields = Array.isArray(type?.fields) ? type.fields : []
  const value = predicate.path || ''

  return (
    <select
      className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      {fields.map((field) => (
        <option key={field.path} value={field.path}>
          {field.label}
        </option>
      ))}
      <option value="__advanced__">Advanced JSON Path…</option>
    </select>
  )
}

const AdvancedPathInput = ({ value, onChange }) => (
  <input
    className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
    placeholder="$.path.to.field"
    value={value}
    onChange={(event) => onChange(event.target.value)}
  />
)

const OperatorSelect = ({ predicate, onChange }) => {
  const options = operatorOptionsFor(predicate.source, predicate.path)
  return (
    <select
      className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
      value={predicate.operator}
      onChange={(event) => onChange(event.target.value)}
    >
      {options.map((op) => (
        <option key={op.value} value={op.value}>
          {op.label}
        </option>
      ))}
    </select>
  )
}

const ValueInput = ({ predicate, onChange }) => {
  if (predicate.operator === 'exists' || predicate.operator === 'missing') {
    return <span className="text-xs text-slate-500">No value required</span>
  }
  const placeholder = predicate.operator === 'between' ? 'min, max' : predicate.operator?.includes('in')
    ? 'value1, value2'
    : 'value'
  return (
    <input
      className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
      placeholder={placeholder}
      value={predicate.value ?? ''}
      onChange={(event) => onChange(event.target.value)}
    />
  )
}

export const FilterBuilder = ({ draft, onChange }) => {
  const filterTypes = listFilterTypes()

  const updatePredicate = (index, updates) => {
    const next = draft.predicates.map((predicate, idx) =>
      idx === index ? { ...predicate, ...updates } : predicate,
    )
    onChange({ ...draft, predicates: next })
  }

  const handleSourceChange = (index) => (value) => {
    const nextDefaults = buildPredicateDefaults(value)
    updatePredicate(index, {
      source: value,
      path: nextDefaults.path,
      operator: nextDefaults.operator,
      value: '',
    })
  }

  const handleFieldChange = (index) => (value) => {
    if (value === '__advanced__') {
      updatePredicate(index, { path: '', fieldMode: 'advanced' })
      return
    }
    updatePredicate(index, { path: value, fieldMode: 'preset' })
  }

  const handleOperatorChange = (index) => (value) => {
    updatePredicate(index, { operator: value })
  }

  const handleValueChange = (index) => (value) => {
    updatePredicate(index, { value })
  }

  const handleMissingPolicyChange = (index) => (value) => {
    updatePredicate(index, { missing_data_policy: value })
  }

  const handleVersionChange = (index, key) => (value) => {
    updatePredicate(index, { [key]: value })
  }

  const addPredicate = () => {
    onChange({ ...draft, predicates: [...draft.predicates, buildPredicateDefaults(draft.predicates[0]?.source)] })
  }

  const removePredicate = (index) => {
    const next = draft.predicates.filter((_, idx) => idx !== index)
    onChange({ ...draft, predicates: next.length ? next : [buildPredicateDefaults()] })
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">
          Match
        </label>
        <select
          className="rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
          value={draft.groupMode || 'all'}
          onChange={(event) => onChange({ ...draft, groupMode: event.target.value })}
        >
          <option value="all">All conditions</option>
          <option value="any">Any condition</option>
        </select>
      </div>

      {draft.predicates.map((predicate, index) => (
        <div
          key={`predicate-${index}`}
          className="space-y-3 rounded-lg border border-white/10 bg-black/30 p-3"
        >
          <div className="flex flex-wrap items-center gap-2">
            <select
              className="rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
              value={predicate.source}
              onChange={(event) => handleSourceChange(index)(event.target.value)}
            >
              {filterTypes.map((type) => (
                <option key={type.key} value={type.key}>
                  {type.label}
                </option>
              ))}
            </select>
            <div className="flex-1">
              {predicate.fieldMode === 'advanced' ? (
                <AdvancedPathInput
                  value={predicate.path || ''}
                  onChange={(value) => updatePredicate(index, { path: value })}
                />
              ) : (
                <FieldSelect predicate={predicate} onChange={handleFieldChange(index)} />
              )}
            </div>
            <OperatorSelect predicate={predicate} onChange={handleOperatorChange(index)} />
            <ValueInput predicate={predicate} onChange={handleValueChange(index)} />
          </div>

          <div className="grid gap-2 md:grid-cols-3">
            <div>
              <label className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Missing Data</label>
              <select
                className="mt-1 w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                value={predicate.missing_data_policy || 'fail'}
                onChange={(event) => handleMissingPolicyChange(index)(event.target.value)}
              >
                <option value="fail">Fail</option>
                <option value="pass">Pass</option>
                <option value="ignore">Ignore</option>
              </select>
            </div>
            {predicate.source === 'candle_stats' && (
              <div>
                <label className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Stats Version</label>
                <input
                  className="mt-1 w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                  placeholder="v1"
                  value={predicate.stats_version || ''}
                  onChange={(event) => handleVersionChange(index, 'stats_version')(event.target.value)}
                />
              </div>
            )}
            {predicate.source === 'regime_stats' && (
              <div>
                <label className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Regime Version</label>
                <input
                  className="mt-1 w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                  placeholder="v1"
                  value={predicate.regime_version || ''}
                  onChange={(event) => handleVersionChange(index, 'regime_version')(event.target.value)}
                />
              </div>
            )}
          </div>

          <div className="flex justify-end">
            <button
              type="button"
              className="text-[11px] font-semibold uppercase tracking-[0.2em] text-rose-200 hover:text-rose-100"
              onClick={() => removePredicate(index)}
            >
              Remove condition
            </button>
          </div>
        </div>
      ))}

      <button
        type="button"
        className="rounded border border-white/10 bg-white/5 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-200 hover:border-white/20"
        onClick={addPredicate}
      >
        Add condition
      </button>
    </div>
  )
}
