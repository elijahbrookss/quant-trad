import React from 'react'
import { Trash2 } from 'lucide-react'
import { listFilterTypes, operatorOptionsFor } from './registry.js'
import { buildPredicateDefaults } from './filterUtils.js'
import { ConditionRowBuilder, addConditionRow, removeConditionRow, updateConditionRow } from '../conditions/ConditionRowBuilder.jsx'

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

  return (
    <div className="space-y-4">
      <ConditionRowBuilder
        rows={draft.predicates}
        onAddRow={addPredicate}
        addLabel="Add condition"
        gridClassName="md:grid-cols-[150px_minmax(0,1fr)_120px_140px_150px_140px_40px]"
        columns={[
          {
            key: 'source',
            label: 'Source',
            render: (predicate, index) => (
              <select
                className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                value={predicate.source}
                onChange={(event) => handleSourceChange(index)(event.target.value)}
              >
                {filterTypes.map((type) => (
                  <option key={type.key} value={type.key}>
                    {type.label}
                  </option>
                ))}
              </select>
            ),
          },
          {
            key: 'field',
            label: 'Field',
            render: (predicate, index) => (
              predicate.fieldMode === 'advanced' ? (
                <AdvancedPathInput
                  value={predicate.path || ''}
                  onChange={(value) => updatePredicate(index, { path: value })}
                />
              ) : (
                <FieldSelect predicate={predicate} onChange={handleFieldChange(index)} />
              )
            ),
          },
          {
            key: 'operator',
            label: 'Operator',
            render: (predicate, index) => (
              <OperatorSelect predicate={predicate} onChange={handleOperatorChange(index)} />
            ),
          },
          {
            key: 'value',
            label: 'Value',
            render: (predicate, index) => (
              <ValueInput predicate={predicate} onChange={handleValueChange(index)} />
            ),
          },
          {
            key: 'missing',
            label: 'Missing Data',
            render: (predicate, index) => (
              <select
                className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                value={predicate.missing_data_policy || 'fail'}
                onChange={(event) => handleMissingPolicyChange(index)(event.target.value)}
              >
                <option value="fail">Fail</option>
                <option value="pass">Pass</option>
                <option value="ignore">Ignore</option>
              </select>
            ),
          },
          {
            key: 'version',
            label: 'Version',
            render: (predicate, index) => {
              if (predicate.source === 'candle_stats') {
                return (
                  <input
                    className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                    placeholder="v1"
                    value={predicate.stats_version || ''}
                    onChange={(event) => handleVersionChange(index, 'stats_version')(event.target.value)}
                  />
                )
              }
              if (predicate.source === 'regime_stats') {
                return (
                  <input
                    className="w-full rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                    placeholder="v1"
                    value={predicate.regime_version || ''}
                    onChange={(event) => handleVersionChange(index, 'regime_version')(event.target.value)}
                  />
                )
              }
              return <span className="text-xs text-slate-500">—</span>
            },
          },
          {
            key: 'delete',
            label: '',
            render: (_, index) => (
              <button
                type="button"
                className="inline-flex items-center justify-center rounded p-1 text-slate-500 hover:text-rose-400"
                onClick={() => removePredicate(index)}
                aria-label="Remove condition"
              >
                <Trash2 className="h-4 w-4" />
              </button>
            ),
          },
        ]}
      />
    </div>
  )
}
