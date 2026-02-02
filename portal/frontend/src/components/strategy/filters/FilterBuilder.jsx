import React, { useState } from 'react'
import { Trash2, ChevronDown, ChevronRight, Settings2 } from 'lucide-react'
import { listFilterTypes, operatorOptionsFor } from './registry.js'
import { buildPredicateDefaults } from './filterUtils.js'
import { addConditionRow, removeConditionRow, updateConditionRow } from '../conditions/ConditionRowBuilder.jsx'

// Friendly operator labels
const OPERATOR_LABELS = {
  eq: 'equals',
  ne: 'not equals',
  gt: 'greater than',
  gte: 'at least',
  lt: 'less than',
  lte: 'at most',
  in: 'is any of',
  not_in: 'is none of',
  between: 'between',
  exists: 'exists',
  missing: 'is missing',
}

const FieldSelect = ({ predicate, onChange }) => {
  const type = listFilterTypes().find((entry) => entry.key === predicate.source)
  const fields = Array.isArray(type?.fields) ? type.fields : []
  const value = predicate.path || ''

  return (
    <select
      className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      {fields.map((field) => (
        <option key={field.path} value={field.path}>
          {field.label}
        </option>
      ))}
      <option value="__advanced__">Custom path...</option>
    </select>
  )
}

const AdvancedPathInput = ({ value, onChange }) => (
  <input
    className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
    placeholder="$.path.to.field"
    value={value}
    onChange={(event) => onChange(event.target.value)}
  />
)

const OperatorSelect = ({ predicate, onChange }) => {
  const options = operatorOptionsFor(predicate.source, predicate.path)
  return (
    <select
      className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
      value={predicate.operator}
      onChange={(event) => onChange(event.target.value)}
    >
      {options.map((op) => (
        <option key={op.value} value={op.value}>
          {OPERATOR_LABELS[op.value] || op.label}
        </option>
      ))}
    </select>
  )
}

const ValueInput = ({ predicate, onChange }) => {
  if (predicate.operator === 'exists' || predicate.operator === 'missing') {
    return (
      <span className="flex h-[38px] items-center text-sm text-slate-500 italic">
        No value needed
      </span>
    )
  }
  const placeholder = predicate.operator === 'between' ? 'min, max' : predicate.operator?.includes('in')
    ? 'value1, value2'
    : 'Enter value'
  return (
    <input
      className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
      placeholder={placeholder}
      value={predicate.value ?? ''}
      onChange={(event) => onChange(event.target.value)}
    />
  )
}

export const FilterBuilder = ({ draft, onChange }) => {
  const [showAdvanced, setShowAdvanced] = useState(false)
  const filterTypes = listFilterTypes()

  const updatePredicate = (index, updates) => {
    const next = updateConditionRow(draft.predicates, index, updates)
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
    onChange({
      ...draft,
      predicates: addConditionRow(draft.predicates, () => buildPredicateDefaults(draft.predicates[0]?.source)),
    })
  }

  const removePredicate = (index) => {
    onChange({
      ...draft,
      predicates: removeConditionRow(draft.predicates, index, () => buildPredicateDefaults()),
    })
  }

  // Check if any predicate needs version field
  const hasVersionableSource = draft.predicates.some(
    (p) => p.source === 'candle_stats' || p.source === 'regime_stats'
  )

  return (
    <div className="space-y-4">
      {/* Condition rows */}
      <div className="space-y-3">
        {draft.predicates.map((predicate, index) => (
          <div key={index} className="group">
            {/* Row connector */}
            {index > 0 && (
              <div className="flex items-center gap-3 py-2">
                <div className="h-px flex-1 bg-white/10" />
                <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                  {draft.matchMode === 'any' ? 'or' : 'and'}
                </span>
                <div className="h-px flex-1 bg-white/10" />
              </div>
            )}

            {/* Main condition row */}
            <div className="flex flex-wrap items-start gap-2">
              {/* Source */}
              <div className="w-[140px] shrink-0">
                {index === 0 && (
                  <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                    Source
                  </label>
                )}
                <select
                  className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  value={predicate.source}
                  onChange={(event) => handleSourceChange(index)(event.target.value)}
                >
                  {filterTypes.map((type) => (
                    <option key={type.key} value={type.key}>
                      {type.label}
                    </option>
                  ))}
                </select>
              </div>

              {/* Field */}
              <div className="min-w-[160px] flex-1">
                {index === 0 && (
                  <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                    Field
                  </label>
                )}
                {predicate.fieldMode === 'advanced' ? (
                  <AdvancedPathInput
                    value={predicate.path || ''}
                    onChange={(value) => updatePredicate(index, { path: value })}
                  />
                ) : (
                  <FieldSelect predicate={predicate} onChange={handleFieldChange(index)} />
                )}
              </div>

              {/* Operator */}
              <div className="w-[130px] shrink-0">
                {index === 0 && (
                  <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                    Operator
                  </label>
                )}
                <OperatorSelect predicate={predicate} onChange={handleOperatorChange(index)} />
              </div>

              {/* Value */}
              <div className="min-w-[120px] flex-1">
                {index === 0 && (
                  <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                    Value
                  </label>
                )}
                <ValueInput predicate={predicate} onChange={handleValueChange(index)} />
              </div>

              {/* Delete button */}
              <div className="shrink-0 pt-[26px]">
                {index === 0 && <div className="h-[18px]" />}
                <button
                  type="button"
                  className="flex h-[38px] w-[38px] items-center justify-center rounded-md text-slate-500 transition hover:bg-rose-500/10 hover:text-rose-400"
                  onClick={() => removePredicate(index)}
                  aria-label="Remove condition"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </div>
            </div>

            {/* Advanced options for this row (shown when toggle is on) */}
            {showAdvanced && (
              <div className="ml-[140px] mt-2 flex flex-wrap items-center gap-3 pl-2 border-l-2 border-white/5">
                <div className="flex items-center gap-2">
                  <label className="text-[10px] font-medium text-slate-500">Missing data:</label>
                  <select
                    className="rounded border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-300 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={predicate.missing_data_policy || 'fail'}
                    onChange={(event) => handleMissingPolicyChange(index)(event.target.value)}
                  >
                    <option value="fail">Fail gate</option>
                    <option value="pass">Pass gate</option>
                    <option value="ignore">Skip check</option>
                  </select>
                </div>

                {(predicate.source === 'candle_stats' || predicate.source === 'regime_stats') && (
                  <div className="flex items-center gap-2">
                    <label className="text-[10px] font-medium text-slate-500">Version:</label>
                    <input
                      className="w-16 rounded border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-300 transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                      placeholder="v1"
                      value={
                        predicate.source === 'candle_stats'
                          ? predicate.stats_version || ''
                          : predicate.regime_version || ''
                      }
                      onChange={(event) =>
                        handleVersionChange(
                          index,
                          predicate.source === 'candle_stats' ? 'stats_version' : 'regime_version'
                        )(event.target.value)
                      }
                    />
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Footer: Add condition + Advanced toggle */}
      <div className="flex flex-wrap items-center justify-between gap-3 pt-2">
        <button
          type="button"
          onClick={addPredicate}
          className="inline-flex items-center gap-1.5 rounded-md border border-dashed border-white/20 px-3 py-1.5 text-xs font-medium text-slate-300 transition hover:border-white/30 hover:bg-white/5 hover:text-white"
        >
          <span className="text-base leading-none">+</span>
          Add condition
        </button>

        <button
          type="button"
          onClick={() => setShowAdvanced((prev) => !prev)}
          className="inline-flex items-center gap-1.5 text-xs text-slate-400 transition hover:text-slate-200"
        >
          <Settings2 className="h-3.5 w-3.5" />
          Advanced options
          {showAdvanced ? (
            <ChevronDown className="h-3 w-3" />
          ) : (
            <ChevronRight className="h-3 w-3" />
          )}
        </button>
      </div>
    </div>
  )
}
