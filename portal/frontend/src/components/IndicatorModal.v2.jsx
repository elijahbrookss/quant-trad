import { Dialog, DialogPanel, DialogTitle, Switch } from '@headlessui/react'
import { useEffect, useMemo, useState } from 'react'

import { fetchIndicatorTypes, fetchIndicatorType } from '../adapters/indicator.adapter.js'
import DropdownSelect from './ChartComponent/DropdownSelect.jsx'

const EMPTY_META = {
  required_params: [],
  default_params: {},
  field_types: {},
  ui_descriptions: {},
  ui_order: [],
  ui_enums: {},
  signal_rules: [],
}

const NUMBER_FIELDS = new Set(['int', 'float', 'number'])

const toInt = (value) => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.trunc(value) : null
  }
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed.length) return null
    const parsed = Number(trimmed)
    return Number.isFinite(parsed) ? Math.trunc(parsed) : null
  }
  return null
}

const toFloat = (value) => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null
  }
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed.length) return null
    const parsed = Number(trimmed)
    return Number.isFinite(parsed) ? parsed : null
  }
  return null
}

const toIntList = (value) => {
  if (Array.isArray(value)) {
    return value.map(toInt).filter((item) => item !== null)
  }
  if (typeof value === 'string') {
    return value
      .split(/[\s,;]+/)
      .filter(Boolean)
      .map(toInt)
      .filter((item) => item !== null)
  }
  if (value == null) {
    return []
  }
  const single = toInt(value)
  return single === null ? [] : [single]
}

const listToString = (value) => {
  if (Array.isArray(value)) {
    return value.join(', ')
  }
  if (value == null) {
    return ''
  }
  return String(value)
}

const normaliseString = (value) => {
  if (value == null) return ''
  return String(value)
}

const buildFieldOrder = (meta, params) => {
  const required = Array.isArray(meta.required_params) ? meta.required_params : []
  const defaults = Object.keys(meta.default_params || {})
  const current = Object.keys(params || {})
  const explicit = Array.isArray(meta.ui_order) ? meta.ui_order : []

  const ordered = [...explicit, ...required, ...defaults, ...current]
  return Array.from(new Set(ordered))
}

const deriveIntListKeys = (meta) => {
  const keys = new Set()
  const fieldTypes = meta.field_types || {}
  Object.entries(fieldTypes).forEach(([key, value]) => {
    const lower = String(value || '').toLowerCase()
    if (lower.includes('list') && lower.includes('int')) {
      keys.add(key)
    }
  })
  Object.entries(meta.default_params || {}).forEach(([key, value]) => {
    if (Array.isArray(value) && value.every((entry) => Number.isFinite(entry))) {
      keys.add(key)
    }
  })
  return keys
}

const prepareInitialParams = (meta, initialParams) => {
  const intListKeys = deriveIntListKeys(meta)
  const merged = { ...meta.default_params, ...(initialParams || {}) }
  const output = {}

  for (const key of buildFieldOrder(meta, merged)) {
    if (intListKeys.has(key)) {
      output[key] = listToString(merged[key])
      continue
    }

    const fieldType = String(meta.field_types?.[key] || '').toLowerCase()
    if (fieldType === 'bool') {
      output[key] = Boolean(merged[key])
    } else if (NUMBER_FIELDS.has(fieldType)) {
      output[key] = normaliseString(merged[key] ?? '')
    } else {
      output[key] = normaliseString(merged[key] ?? '')
    }
  }

  for (const key of meta.required_params || []) {
    if (!(key in output)) {
      output[key] = ''
    }
  }

  return output
}

const convertParamsForSave = (meta, params) => {
  const intListKeys = deriveIntListKeys(meta)
  const prepared = {}

  for (const [key, raw] of Object.entries(params || {})) {
    const fieldType = String(meta.field_types?.[key] || '').toLowerCase()

    if (intListKeys.has(key)) {
      const values = toIntList(raw)
      if (values.length) {
        prepared[key] = values
      }
      continue
    }

    if (fieldType === 'bool') {
      prepared[key] = Boolean(raw)
      continue
    }

    if (NUMBER_FIELDS.has(fieldType)) {
      const parsed = fieldType === 'int' ? toInt(raw) : toFloat(raw)
      if (parsed !== null) {
        prepared[key] = parsed
      }
      continue
    }

    const text = typeof raw === 'string' ? raw.trim() : raw
    if (text !== '' && text != null) {
      prepared[key] = text
    }
  }

  return prepared
}

export default function IndicatorModalV2({ isOpen, initial, error, onClose, onSave }) {
  const [types, setTypes] = useState([])
  const [typeId, setTypeId] = useState(initial?.type || '')
  const [name, setName] = useState(initial?.name || '')
  const [params, setParams] = useState({})
  const [meta, setMeta] = useState(EMPTY_META)
  const [metaError, setMetaError] = useState(null)
  const [availableSignalRules, setAvailableSignalRules] = useState([])
  const [selectedSignalRules, setSelectedSignalRules] = useState([])
  const [showAdvanced, setShowAdvanced] = useState(false)

  useEffect(() => {
    if (!isOpen) return
    fetchIndicatorTypes()
      .then((payload) => setTypes(Array.isArray(payload) ? payload : []))
      .catch((err) => setMetaError(err?.message || 'Failed to load indicator types'))
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) return
    setTypeId(initial?.type || '')
    setName(initial?.name || '')
    setMeta(EMPTY_META)
    setMetaError(null)
    setParams(initial?.params || {})
    setShowAdvanced(false)
    const existingRules = Array.isArray(initial?.signalRules) ? [...initial.signalRules] : []
    setSelectedSignalRules(existingRules)
    setAvailableSignalRules([])
  }, [initial, isOpen])

  useEffect(() => {
    if (!isOpen || !typeId) return
    let cancelled = false

    fetchIndicatorType(typeId)
      .then((payload) => {
        if (cancelled) return
        const nextMeta = { ...EMPTY_META, ...(payload || {}) }
        setMeta(nextMeta)
        const preparedParams = prepareInitialParams(nextMeta, initial?.params)
        setParams(preparedParams)
        setShowAdvanced(false)

        const rules = Array.isArray(nextMeta.signal_rules) ? nextMeta.signal_rules : []
        setAvailableSignalRules(rules)

        if (initial?.signalRules?.length) {
          const filtered = initial.signalRules.filter((ruleId) => rules.some((rule) => rule.id === ruleId))
          setSelectedSignalRules(filtered)
        } else if (rules.length) {
          setSelectedSignalRules(rules.map((rule) => rule.id))
        } else {
          setSelectedSignalRules([])
        }
      })
      .catch((err) => {
        if (cancelled) return
        setMetaError(err?.message || 'Failed to load indicator metadata')
      })

    return () => {
      cancelled = true
    }
  }, [initial?.params, initial?.signalRules, isOpen, typeId])

  const fieldOrder = useMemo(() => buildFieldOrder(meta, params), [meta, params])
  const intListKeys = useMemo(() => deriveIntListKeys(meta), [meta])

  const { primaryKeys, advancedKeys } = useMemo(() => {
    if (!fieldOrder.length) return { primaryKeys: [], advancedKeys: [] }

    const required = Array.isArray(meta.required_params) ? meta.required_params : []
    const preferred = Array.isArray(meta.ui_basic_keys) ? meta.ui_basic_keys : []
    // Exclude chart-context fields - these are runtime parameters, not indicator config
    const chartContextKeys = new Set([
      'symbol',
      'interval',
      'start',
      'end',
      'timeframe',
      'datasource',
      'exchange',
      'provider_id',
      'venue_id',
      'instrument_id',
    ])
    const filteredOrder = fieldOrder.filter((key) => !chartContextKeys.has(key))

    const essential = new Set([...required, ...preferred])

    for (const key of filteredOrder) {
      if (essential.size >= Math.max(required.length, Math.min(filteredOrder.length, 6))) break
      essential.add(key)
    }

    const primary = filteredOrder.filter((key) => essential.has(key))
    const advanced = filteredOrder.filter((key) => !essential.has(key))

    return { primaryKeys: primary, advancedKeys: advanced }
  }, [fieldOrder, meta])

  const handleParamChange = (key) => (input) => {
    let value = input
    if (input && typeof input === 'object' && 'target' in input) {
      value = input.target?.value ?? ''
    }
    setParams((prev) => ({ ...prev, [key]: value ?? '' }))
  }

  const handleBooleanChange = (key) => (value) => {
    setParams((prev) => ({ ...prev, [key]: Boolean(value) }))
  }

  const toggleSignalRule = (ruleId) => {
    setSelectedSignalRules((prev) => {
      const next = new Set(prev)
      if (next.has(ruleId)) {
        next.delete(ruleId)
      } else {
        next.add(ruleId)
      }
      return Array.from(next)
    })
  }

  const allRulesSelected = availableSignalRules.length > 0 && selectedSignalRules.length === availableSignalRules.length

  const renderField = (key) => {
    const fieldType = String(meta.field_types?.[key] || '').toLowerCase()
    const isRequired = Array.isArray(meta.required_params) && meta.required_params.includes(key)
    const description = meta.ui_descriptions?.[key]
    const value = params[key] ?? (fieldType === 'bool' ? false : '')
    const enumValues = Array.isArray(meta.ui_enums?.[key]) ? meta.ui_enums[key] : null

    return (
      <div key={key} className="space-y-2 rounded-xl border border-white/10 bg-white/5 p-4">
        <div>
          <label className="text-sm font-semibold text-white">
            {key}
            {isRequired && <span className="ml-1 text-rose-300">*</span>}
          </label>
          {description && <p className="text-xs text-slate-300/80">{description}</p>}
        </div>

        {fieldType === 'bool' ? (
          <div className="flex items-center gap-3">
            <Switch
              checked={Boolean(value)}
              onChange={handleBooleanChange(key)}
              className={`${value ? 'bg-emerald-500/70' : 'bg-slate-600/60'} relative inline-flex h-6 w-11 items-center rounded-full transition`}
            >
              <span className={`${value ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
            </Switch>
            <span className="text-sm text-slate-200">{value ? 'Enabled' : 'Disabled'}</span>
          </div>
        ) : enumValues ? (
          <DropdownSelect
            value={value}
            onChange={handleParamChange(key)}
            options={enumValues.map((entry) => ({ value: entry, label: String(entry) }))}
            className="w-full"
          />
        ) : intListKeys.has(key) ? (
          <input
            className="w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            value={value}
            onChange={handleParamChange(key)}
            placeholder="e.g. 5, 10, 20"
          />
        ) : NUMBER_FIELDS.has(fieldType) ? (
          <input
            className="w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            value={value}
            onChange={handleParamChange(key)}
            inputMode="decimal"
            placeholder="Enter a number"
          />
        ) : (
          <input
            className="w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            value={value}
            onChange={handleParamChange(key)}
          />
        )}
      </div>
    )
  }

  const handleSubmit = () => {
    if (!typeId) {
      setMetaError('Please select an indicator type.')
      return
    }
    if (!name.trim()) {
      setMetaError('Please provide an indicator name.')
      return
    }
    if (availableSignalRules.length > 0 && selectedSignalRules.length === 0) {
      setMetaError('Enable at least one signal rule to generate signals.')
      return
    }

    const preparedParams = convertParamsForSave(meta, params)
    onSave({ id: initial?.id, type: typeId, name: name.trim(), params: preparedParams, signalRules: selectedSignalRules })
  }

  if (!isOpen) return null

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/50" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-3xl overflow-hidden rounded-2xl border border-white/10 bg-[#121520] text-slate-100 shadow-2xl">
          <header className="border-b border-white/10 bg-white/5 px-6 py-4">
            <DialogTitle className="text-lg font-semibold text-white">
              {initial?.id ? 'Edit indicator' : 'Create indicator'}
            </DialogTitle>
            <p className="mt-1 text-sm text-slate-400">
              Configure the core parameters and choose which signal rules should run for this indicator.
            </p>
          </header>

          <div className="space-y-6 px-6 py-6">
            {(metaError || error) && (
              <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-4 py-2 text-sm text-rose-200">
                {metaError || error}
              </div>
            )}

            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-1">
                <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Name</label>
                <input
                  className="w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Indicator type</label>
                {initial?.id ? (
                  <div className="rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200">
                    {typeId || '—'}
                  </div>
                ) : (
                  <DropdownSelect
                    value={typeId}
                    onChange={setTypeId}
                    placeholder="Select type…"
                    options={types.map((entry) => ({ value: entry, label: entry }))}
                    className="mt-1 w-full"
                  />
                )}
              </div>
            </div>

            {typeId ? (
              <div className="space-y-4">
                {fieldOrder.length ? (
                  <div className="space-y-4">
                    {primaryKeys.length > 0 && (
                      <div className="grid gap-3 md:grid-cols-2">{primaryKeys.map(renderField)}</div>
                    )}

                    {advancedKeys.length > 0 && (
                      <div className="space-y-3 rounded-xl border border-dashed border-white/10 bg-white/5 p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <h4 className="text-sm font-semibold text-white">Advanced parameters</h4>
                            <p className="text-xs text-slate-400">
                              {advancedKeys.length} optional setting{advancedKeys.length > 1 ? 's' : ''} hidden by default.
                            </p>
                          </div>
                          <button
                            type="button"
                            onClick={() => setShowAdvanced((prev) => !prev)}
                            className="rounded-full border border-white/15 px-3 py-1 text-xs text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-white"
                          >
                            {showAdvanced ? 'Hide advanced' : 'Show advanced'}
                          </button>
                        </div>

                        {showAdvanced && (
                          <div className="grid gap-3 md:grid-cols-2">{advancedKeys.map(renderField)}</div>
                        )}
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-sm text-slate-400">No editable parameters for this indicator.</p>
                )}

                {availableSignalRules.length > 0 && (
                  <div className="space-y-3 rounded-xl border border-white/10 bg-white/5 p-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <h4 className="text-sm font-semibold text-white">Signal rules</h4>
                        <p className="text-xs text-slate-400">Choose which rule detections should run when generating signals.</p>
                      </div>
                      <button
                        type="button"
                        className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-white"
                        onClick={() => setSelectedSignalRules(allRulesSelected ? [] : availableSignalRules.map((rule) => rule.id))}
                      >
                        {allRulesSelected ? 'Clear' : 'Enable all'}
                      </button>
                    </div>

                    <div className="space-y-3">
                      {availableSignalRules.map((rule) => {
                        const checked = selectedSignalRules.includes(rule.id)
                        return (
                          <label
                            key={rule.id}
                            className={`flex items-start justify-between gap-3 rounded-lg border px-3 py-2 text-sm transition ${
                              checked
                                ? 'border-[color:var(--accent-alpha-50)] bg-[color:var(--accent-alpha-10)] text-white'
                                : 'border-white/10 bg-black/30 text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-white'
                            }`}
                          >
                            <div className="space-y-1">
                              <span className="font-medium">{rule.label || rule.id}</span>
                              {rule.description && <p className="text-xs text-slate-300/80">{rule.description}</p>}
                            </div>
                            <Switch
                              checked={checked}
                              onChange={() => toggleSignalRule(rule.id)}
                              className={`${checked ? 'bg-[color:var(--accent-alpha-80)]' : 'bg-slate-600/60'} relative inline-flex h-6 w-11 items-center rounded-full transition`}
                            >
                              <span className={`${checked ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
                            </Switch>
                          </label>
                        )
                      })}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <p className="rounded-xl border border-dashed border-white/10 bg-white/5 p-4 text-sm text-slate-400">
                Select an indicator type to configure its parameters and signal rules.
              </p>
            )}

            <footer className="flex items-center justify-end gap-3">
              <button
                type="button"
                className="rounded-lg border border-white/10 bg-transparent px-3 py-2 text-sm text-slate-200 hover:bg-white/10"
                onClick={onClose}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-lg bg-[color:var(--accent-alpha-40)] px-4 py-2 text-sm font-semibold text-[color:var(--accent-text-strong)] hover:bg-[color:var(--accent-alpha-60)]"
                onClick={handleSubmit}
              >
                Save indicator
              </button>
            </footer>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
