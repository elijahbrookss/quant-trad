import { Dialog, DialogPanel, DialogTitle, Switch } from '@headlessui/react'
import { useEffect, useMemo, useState } from 'react'

import { fetchIndicatorTypes, fetchIndicatorType, fetchIndicators } from '../adapters/indicator.adapter.js'
import { buildSignalOutputEnabledMap, buildSignalOutputPrefs, getIndicatorOutputsByType } from '../utils/indicatorOutputs.js'
import DropdownSelect from './ChartComponent/DropdownSelect.jsx'

const EMPTY_META = {
  type: '',
  version: '',
  label: '',
  description: '',
  params: [],
  outputs: [],
  overlays: [],
  dependencies: [],
}

const NUMBER_FIELDS = new Set(['int', 'float'])
const LIST_FIELDS = new Set(['int_list', 'float_list', 'string_list'])

const toOptionEntry = (entry) => {
  if (entry && typeof entry === 'object' && 'value' in entry) {
    return {
      value: entry.value,
      label: entry.label ?? String(entry.value),
      description: entry.description ?? '',
      badge: entry.badge ?? undefined,
      disabled: Boolean(entry.disabled),
    }
  }
  return {
    value: entry,
    label: String(entry),
    description: '',
    badge: undefined,
    disabled: false,
  }
}

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

const toFloatList = (value) => {
  if (Array.isArray(value)) {
    return value.map(toFloat).filter((item) => item !== null)
  }
  if (typeof value === 'string') {
    return value
      .split(/[\s,;]+/)
      .filter(Boolean)
      .map(toFloat)
      .filter((item) => item !== null)
  }
  if (value == null) {
    return []
  }
  const single = toFloat(value)
  return single === null ? [] : [single]
}

const toStringList = (value) => {
  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean)
  }
  if (typeof value === 'string') {
    return value.split(/[\n,;]+/).map((item) => item.trim()).filter(Boolean)
  }
  if (value == null) {
    return []
  }
  return [String(value).trim()].filter(Boolean)
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

const editableParams = (meta) => (
  Array.isArray(meta.params) ? meta.params.filter((param) => param?.editable !== false) : []
)

const normaliseString = (value) => {
  if (value == null) return ''
  return String(value)
}

const formatIndicatorType = (type) => {
  if (!type) return 'Custom'
  return String(type)
    .split(/[_-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ')
}

const compatibleIndicatorsForDependency = (dependency, indicators, currentIndicatorId) => (
  Array.isArray(indicators)
    ? indicators.filter((indicator) => (
        indicator?.id &&
        indicator.id !== currentIndicatorId &&
        String(indicator?.type || '').trim() === String(dependency?.indicator_type || '').trim()
      ))
    : []
)

const prepareInitialDependencies = (meta, initialDependencies, availableIndicators, currentIndicatorId) => {
  const definitions = Array.isArray(meta?.dependencies) ? meta.dependencies : []
  const existingBindings = Array.isArray(initialDependencies) ? initialDependencies : []
  const prepared = {}

  for (const dependency of definitions) {
    const outputName = String(dependency?.output_name || '').trim()
    if (!outputName) continue
    const existing = existingBindings.find((item) => (
      String(item?.output_name || '').trim() === outputName &&
      String(item?.indicator_id || '').trim()
    ))
    if (existing) {
      prepared[outputName] = String(existing.indicator_id).trim()
      continue
    }
    const candidates = compatibleIndicatorsForDependency(
      dependency,
      availableIndicators,
      currentIndicatorId,
    )
    if (candidates.length === 1) {
      prepared[outputName] = String(candidates[0].id)
    }
  }

  return prepared
}

const prepareInitialParams = (meta, initialParams) => {
  const definitions = editableParams(meta)
  const output = {}

  for (const param of definitions) {
    const key = param.key
    const fieldType = String(param.type || '').toLowerCase()
    const hasDefault = Boolean(param.has_default)
    const rawValue = initialParams?.[key] ?? (hasDefault ? param.default : undefined)

    if (LIST_FIELDS.has(fieldType)) {
      output[key] = listToString(rawValue)
      continue
    }

    if (fieldType === 'bool') {
      output[key] = Boolean(rawValue)
    } else if (NUMBER_FIELDS.has(fieldType)) {
      output[key] = normaliseString(rawValue ?? '')
    } else {
      output[key] = normaliseString(rawValue ?? '')
    }
  }

  return output
}

const convertParamsForSave = (meta, params) => {
  const definitions = editableParams(meta)
  const prepared = {}

  for (const param of definitions) {
    const key = param.key
    const raw = params?.[key]
    const fieldType = String(param.type || '').toLowerCase()

    if (fieldType === 'bool') {
      prepared[key] = Boolean(raw)
      continue
    }

    if (fieldType === 'int_list') {
      const values = toIntList(raw)
      if (values.length) prepared[key] = values
      continue
    }

    if (fieldType === 'float_list') {
      const values = toFloatList(raw)
      if (values.length) prepared[key] = values
      continue
    }

    if (fieldType === 'string_list') {
      const values = toStringList(raw)
      if (values.length) prepared[key] = values
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
  const [availableIndicators, setAvailableIndicators] = useState([])
  const [typeId, setTypeId] = useState(initial?.type || '')
  const [name, setName] = useState(initial?.name || '')
  const [params, setParams] = useState({})
  const [dependencyBindings, setDependencyBindings] = useState({})
  const [signalOutputEnabled, setSignalOutputEnabled] = useState({})
  const [meta, setMeta] = useState(EMPTY_META)
  const [metaError, setMetaError] = useState(null)
  const [showAdvanced, setShowAdvanced] = useState(false)

  useEffect(() => {
    if (!isOpen) return
    Promise.all([fetchIndicatorTypes(), fetchIndicators()])
      .then(([typePayload, indicatorPayload]) => {
        setTypes(Array.isArray(typePayload) ? typePayload : [])
        setAvailableIndicators(Array.isArray(indicatorPayload) ? indicatorPayload : [])
      })
      .catch((err) => setMetaError(err?.message || 'Failed to load indicator types'))
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) return
    setTypeId(initial?.type || '')
    setName(initial?.name || '')
    setMeta(EMPTY_META)
    setMetaError(null)
    setParams(initial?.params || {})
    setDependencyBindings({})
    setSignalOutputEnabled({})
    setShowAdvanced(false)
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
        setDependencyBindings(
          prepareInitialDependencies(
            nextMeta,
            initial?.dependencies,
            availableIndicators,
            initial?.id,
          ),
        )
        setSignalOutputEnabled(
          buildSignalOutputEnabledMap({
            outputs: nextMeta.outputs,
            typed_outputs: initial?.type === typeId ? initial?.typed_outputs : undefined,
            output_prefs: initial?.type === typeId ? initial?.output_prefs : undefined,
          }),
        )
        setShowAdvanced(false)

      })
      .catch((err) => {
        if (cancelled) return
        setMetaError(err?.message || 'Failed to load indicator metadata')
      })

    return () => {
      cancelled = true
    }
  }, [
    availableIndicators,
    initial?.dependencies,
    initial?.id,
    initial?.output_prefs,
    initial?.params,
    initial?.typed_outputs,
    initial?.type,
    isOpen,
    typeId,
  ])

  const fields = useMemo(() => editableParams(meta), [meta])
  const { coreFields, optionalFields, requiredKeys } = useMemo(() => {
    if (!fields.length) return { coreFields: [], optionalFields: [], requiredKeys: [] }
    const requiredOnly = fields.filter((field) => field?.required)
    const core = fields.filter((field) => !field?.advanced)
    const optional = fields.filter((field) => field?.advanced)
    return {
      coreFields: core,
      optionalFields: optional,
      requiredKeys: requiredOnly.map((field) => field.key),
    }
  }, [fields])

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

  const renderField = (param) => {
    const key = param.key
    const fieldType = String(param.type || '').toLowerCase()
    const isRequired = Boolean(param.required)
    const description = param.description
    const value = params[key] ?? (fieldType === 'bool' ? false : '')
    const enumValues = Array.isArray(param.options) && param.options.length
      ? param.options.map(toOptionEntry)
      : null

    return (
      <div key={key} className="space-y-2 rounded-lg border border-white/10 bg-slate-800/60 px-4 py-3 shadow-sm">
        <div className="flex items-start justify-between gap-3">
          <label className="text-sm font-semibold text-white">
            {param.label || key}
            {isRequired && <span className="ml-1 text-rose-300">*</span>}
          </label>
        </div>
        {description && <p className="text-xs text-slate-300/80">{description}</p>}

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
            options={enumValues}
            className="w-full"
          />
        ) : LIST_FIELDS.has(fieldType) ? (
          <input
            className="w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            value={value}
            onChange={handleParamChange(key)}
            placeholder={fieldType === 'string_list' ? 'e.g. one, two, three' : 'e.g. 5, 10, 20'}
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

  const dependencyFields = useMemo(
    () => (Array.isArray(meta.dependencies) ? meta.dependencies : []),
    [meta.dependencies],
  )
  const signalOutputs = useMemo(() => getIndicatorOutputsByType(meta, 'signal'), [meta])

  const handleDependencyChange = (outputName) => (indicatorId) => {
    setDependencyBindings((prev) => ({ ...prev, [outputName]: indicatorId }))
  }

  const handleSignalOutputToggle = (outputName) => (enabled) => {
    setSignalOutputEnabled((prev) => ({
      ...prev,
      [outputName]: Boolean(enabled),
    }))
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
    const preparedDependencies = dependencyFields.map((dependency) => {
      const outputName = String(dependency?.output_name || '').trim()
      return {
        indicator_id: String(dependencyBindings?.[outputName] || '').trim(),
        indicator_type: String(dependency?.indicator_type || '').trim(),
        output_name: outputName,
      }
    })
    const missingDependency = preparedDependencies.find((dependency) => !dependency.indicator_id)
    if (missingDependency) {
      setMetaError(
        `Please select a ${missingDependency.indicator_type} dependency for ${missingDependency.output_name}.`,
      )
      return
    }
    const preparedParams = convertParamsForSave(meta, params)
    onSave({
      id: initial?.id,
      type: typeId,
      name: name.trim(),
      params: preparedParams,
      dependencies: preparedDependencies,
      output_prefs: buildSignalOutputPrefs(meta, signalOutputEnabled),
    })
  }

  if (!isOpen) return null

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-6">
        <DialogPanel className="flex w-full max-w-4xl max-h-[92vh] flex-col overflow-hidden rounded-xl border border-white/10 bg-[#0c111d] text-slate-100 shadow-2xl">
          <header className="border-b border-white/10 bg-white/5 px-6 py-4">
            <DialogTitle className="text-lg font-semibold text-white">
              {initial?.id ? 'Edit indicator' : 'Create indicator'}
            </DialogTitle>
            <p className="mt-1 text-sm text-slate-400">
              Configure indicator parameters. Required fields are marked with *.
            </p>
          </header>

          <div className="flex-1 space-y-6 overflow-y-auto px-6 py-6">
            {(metaError || error) && (
              <div className="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-200 shadow-sm">
                {metaError || error}
              </div>
            )}

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-1">
                <label className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-400">Name</label>
                <input
                  className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-400">Indicator type</label>
                {initial?.id ? (
                  <div className="rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200">
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
              <div className="space-y-5">
                {fields.length ? (
                  <div className="space-y-5">
                    <div className="space-y-3 rounded-lg border border-white/12 bg-slate-900/60 p-4">
                      <div className="flex items-start justify-between">
                        <div>
                          <h4 className="text-sm font-semibold text-white">Core parameters</h4>
                          <p className="text-xs text-slate-400">
                            {requiredKeys.length
                              ? `${requiredKeys.length} required field${requiredKeys.length > 1 ? 's' : ''} for this indicator`
                              : 'Primary settings for this indicator'}
                          </p>
                        </div>
                        {requiredKeys.length > 0 && (
                          <span className="rounded-full border border-white/10 px-3 py-1 text-[11px] font-medium uppercase tracking-wide text-slate-300">
                            Required
                          </span>
                        )}
                      </div>

                      {coreFields.length > 0 ? (
                        <div className="grid gap-3 md:grid-cols-2">{coreFields.map(renderField)}</div>
                      ) : (
                        <p className="text-sm text-slate-400">No configurable parameters for this indicator.</p>
                      )}
                    </div>

                    {optionalFields.length > 0 && (
                      <div className="space-y-3 rounded-lg border border-dashed border-white/12 bg-slate-900/40 p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <h4 className="text-sm font-semibold text-white">Additional parameters</h4>
                            <p className="text-xs text-slate-400">
                              {optionalFields.length} optional setting{optionalFields.length > 1 ? 's' : ''} kept separate for clarity.
                            </p>
                          </div>
                          <button
                            type="button"
                            onClick={() => setShowAdvanced((prev) => !prev)}
                            className="rounded-md border border-white/15 px-3 py-1 text-xs font-medium text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:text-white"
                          >
                            {showAdvanced ? 'Hide optional' : 'Show optional'}
                          </button>
                        </div>

                        {showAdvanced && (
                          <div className="grid gap-3 md:grid-cols-2">{optionalFields.map(renderField)}</div>
                        )}
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="rounded-lg border border-dashed border-white/10 bg-slate-900/50 p-4 text-sm text-slate-400">
                    No editable parameters for this indicator.
                  </p>
                )}

                {dependencyFields.length > 0 && (
                  <div className="space-y-3 rounded-lg border border-white/12 bg-slate-900/60 p-4">
                    <div>
                      <h4 className="text-sm font-semibold text-white">Indicator dependencies</h4>
                      <p className="text-xs text-slate-400">
                        Dependent indicators must bind to a specific upstream indicator instance.
                      </p>
                    </div>
                    <div className="grid gap-3 md:grid-cols-2">
                      {dependencyFields.map((dependency) => {
                        const outputName = String(dependency?.output_name || '').trim()
                        const candidates = compatibleIndicatorsForDependency(
                          dependency,
                          availableIndicators,
                          initial?.id,
                        )
                        const value = dependencyBindings?.[outputName] || ''
                        return (
                          <div
                            key={`${dependency.indicator_type}.${outputName}`}
                            className="space-y-2 rounded-lg border border-white/10 bg-slate-800/60 px-4 py-3 shadow-sm"
                          >
                            <div className="flex items-start justify-between gap-3">
                              <label className="text-sm font-semibold text-white">
                                {dependency.label || `${formatIndicatorType(dependency.indicator_type)} Dependency`}
                                <span className="ml-1 text-rose-300">*</span>
                              </label>
                            </div>
                            {dependency.description && (
                              <p className="text-xs text-slate-300/80">{dependency.description}</p>
                            )}
                            {candidates.length ? (
                              <DropdownSelect
                                value={value}
                                onChange={handleDependencyChange(outputName)}
                                placeholder={`Select ${formatIndicatorType(dependency.indicator_type)}…`}
                                options={candidates.map((indicator) => ({
                                  value: indicator.id,
                                  label: indicator.name || indicator.id,
                                  description: `${formatIndicatorType(indicator.type)} · ${indicator.id}`,
                                }))}
                                className="w-full"
                              />
                            ) : (
                              <p className="text-sm text-amber-200">
                                No compatible {formatIndicatorType(dependency.indicator_type)} indicators are available.
                              </p>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                {signalOutputs.length > 0 && (
                  <div className="space-y-3 rounded-lg border border-white/12 bg-slate-900/60 p-4">
                    <div>
                      <h4 className="text-sm font-semibold text-white">Signal outputs</h4>
                      <p className="text-xs text-slate-400">
                        Signal outputs auto-discover from the indicator manifest. Disable them here to hide them from authoring and preview surfaces only. Bot runtime still evaluates whatever signals your strategy rules reference.
                      </p>
                    </div>
                    <div className="grid gap-3 md:grid-cols-2">
                      {signalOutputs.map((output) => {
                        const outputName = String(output?.name || '').trim()
                        const enabled = signalOutputEnabled[outputName] !== false
                        return (
                          <div
                            key={outputName}
                            className="space-y-2 rounded-lg border border-white/10 bg-slate-800/60 px-4 py-3 shadow-sm"
                          >
                            <div className="flex items-start justify-between gap-3">
                              <div>
                                <label className="text-sm font-semibold text-white">
                                  {output.label || outputName}
                                </label>
                                <p className="text-xs text-slate-400">{outputName}</p>
                              </div>
                              <Switch
                                checked={enabled}
                                onChange={handleSignalOutputToggle(outputName)}
                                className={`${enabled ? 'bg-emerald-500/70' : 'bg-slate-600/60'} relative inline-flex h-6 w-11 items-center rounded-full transition`}
                              >
                                <span className={`${enabled ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
                              </Switch>
                            </div>
                            <p className="text-xs text-slate-300/80">
                              {enabled
                                ? 'Available in indicator signal previews and rule creation.'
                                : 'Hidden from indicator signal previews and new rule creation.'}
                            </p>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

              </div>
            ) : (
              <p className="rounded-lg border border-dashed border-white/10 bg-slate-900/50 p-4 text-sm text-slate-400">
                Select an indicator type to configure its parameters.
              </p>
            )}
          </div>

          <footer className="flex items-center justify-end gap-3 border-t border-white/10 bg-white/5 px-6 py-4">
            <button
              type="button"
              className="rounded-md border border-white/10 bg-transparent px-3 py-2 text-sm text-slate-200 transition hover:bg-white/10"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              type="button"
              className="rounded-md bg-[color:var(--accent-alpha-40)] px-4 py-2 text-sm font-semibold text-[color:var(--accent-text-strong)] transition hover:bg-[color:var(--accent-alpha-60)]"
              onClick={handleSubmit}
            >
              Save indicator
            </button>
          </footer>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
