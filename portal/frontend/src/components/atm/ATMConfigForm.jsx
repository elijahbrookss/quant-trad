import { useMemo, useState } from 'react'

export const DEFAULT_ATM_TEMPLATE = {
  schema_version: 2,
  name: 'New ATM template',
  initial_stop: {
    mode: 'atr',
    atr_period: 14,
    atr_multiplier: 1.0,
  },
  risk: {
    global_risk_multiplier: 1.0,
    base_risk_per_trade: null,
  },
  take_profit_orders: [],
  stop_adjustments: [],
  _meta: { instrument_overrides: false },
}

export function cloneATMTemplate(template = DEFAULT_ATM_TEMPLATE) {
  let cloned
  try {
    cloned = JSON.parse(JSON.stringify(template || DEFAULT_ATM_TEMPLATE))
  } catch {
    cloned = JSON.parse(JSON.stringify(DEFAULT_ATM_TEMPLATE))
  }

  // Normalize v2 template
  if (cloned.name === undefined || cloned.name === null || cloned.name === '') {
    cloned.name = DEFAULT_ATM_TEMPLATE.name
  }

  if (!cloned.initial_stop || typeof cloned.initial_stop !== 'object') {
    cloned.initial_stop = { ...DEFAULT_ATM_TEMPLATE.initial_stop }
  } else {
    if (!cloned.initial_stop.mode) cloned.initial_stop.mode = 'atr'
    if (cloned.initial_stop.atr_period === undefined) cloned.initial_stop.atr_period = 14
    if (cloned.initial_stop.atr_multiplier === undefined) cloned.initial_stop.atr_multiplier = 1.0
  }

  if (!cloned.risk || typeof cloned.risk !== 'object') {
    cloned.risk = { ...DEFAULT_ATM_TEMPLATE.risk }
  } else {
    if (cloned.risk.global_risk_multiplier === undefined) cloned.risk.global_risk_multiplier = 1.0
    if (cloned.risk.base_risk_per_trade === undefined) cloned.risk.base_risk_per_trade = null
  }

  if (!Array.isArray(cloned.stop_adjustments)) {
    cloned.stop_adjustments = []
  }

  if (!Array.isArray(cloned.take_profit_orders)) {
    cloned.take_profit_orders = []
  }

  if (!cloned._meta || typeof cloned._meta !== 'object') {
    cloned._meta = {}
  }

  if (!cloned.schema_version) {
    cloned.schema_version = 2
  }

  return cloned
}

const fieldButtonClasses =
  'rounded-lg border border-white/10 bg-white/5 px-2 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-slate-300 transition hover:border-white/30 hover:text-white'

const inputClasses =
  'mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none'

function formatNumber(value) {
  if (value === null || value === undefined || value === '') return '—'
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return value
  if (Math.abs(numeric) >= 1) return numeric.toLocaleString(undefined, { maximumFractionDigits: 4 })
  return numeric.toPrecision(4)
}

function autoLabel(value, overrideFlag, fallback, suffix = '') {
  if (overrideFlag) return formatNumber(value)
  const resolved = value ?? fallback
  if (resolved === null || resolved === undefined) return 'Auto'
  return `Auto (${formatNumber(resolved)}${suffix})`
}

function parseSizePercent(target, totalContracts) {
  // v2 schema: size_fraction (0-1 range)
  const sizeFraction = target.size_fraction
  if (sizeFraction !== undefined && sizeFraction !== null) {
    const numeric = Number(sizeFraction)
    if (Number.isFinite(numeric)) {
      return numeric * 100 // Convert 0-1 to 0-100 for UI
    }
  }

  const contracts = Number(target.contracts)
  if (Number.isFinite(contracts) && totalContracts > 0) {
    return Math.max(0, (contracts / totalContracts) * 100)
  }

  return null
}

function normalizeTargets(template) {
  const entries = Array.isArray(template?.take_profit_orders) ? template.take_profit_orders : []
  const contractTotal = entries.reduce((sum, entry) => {
    const numeric = Number(entry?.contracts)
    return Number.isFinite(numeric) ? sum + Math.max(0, numeric) : sum
  }, 0)

  const normalised = entries.map((target, index) => {
    const next = { ...target }
    if (!next.id) {
      next.id = `tp-${index + 1}`
    }
    if (next.r_multiple === undefined || next.r_multiple === null) {
      next.r_multiple = index + 1
    }
    if (!next.label) {
      next.label = `TP ${index + 1}`
    }
    next.ticks = null
    next.price = null
    const parsedSize = parseSizePercent(next, contractTotal)
    next.size_percent = parsedSize === null || parsedSize === undefined ? parsedSize : Math.round(parsedSize)

    // Store as size_fraction for v2 (0-1 range)
    next.size_fraction = next.size_percent !== null && next.size_percent !== undefined ? next.size_percent / 100 : null
    return next
  })

  if (normalised.length === 1) {
    normalised[0] = { ...normalised[0], size_percent: 100, size_fraction: 1.0 }
  }

  return normalised
}

export default function ATMConfigForm({
  value,
  onChange,
  hidePositionSizing = false,
  hideRiskSettings = false,
  collapsible = false,
  errors = {},
}) {
  const template = useMemo(() => cloneATMTemplate(value), [value])
  const targets = useMemo(() => normalizeTargets(template), [template])
  const targetSizeTotal = useMemo(
    () =>
      targets.reduce((sum, target) => {
        const numeric = Number(target.size_percent)
        return Number.isFinite(numeric) ? sum + numeric : sum
      }, 0),
    [targets],
  )

  const stopMode = 'r'

  const validationErrors = errors || {}

  const update = (patch = {}) => {
    const next = { ...template, ...patch }
    if (!next._meta || typeof next._meta !== 'object') {
      next._meta = { ...template._meta }
    }
    if (!Array.isArray(next.take_profit_orders) || !next.take_profit_orders.length) {
      next.take_profit_orders = normalizeTargets(next)
    }
    onChange?.(next)
  }

  const applyOverrideField = (field, rawValue) => {
    const next = cloneATMTemplate(template)
    const meta = { ...(next._meta || {}) }
    if (rawValue === '' || rawValue === null || rawValue === undefined) {
      delete next[field]
      meta[`${field}_override`] = false
    } else {
      const numeric = Number(rawValue)
      next[field] = Number.isFinite(numeric) ? numeric : rawValue
      meta[`${field}_override`] = true
    }
    next._meta = meta
    update(next)
  }

  const distributeEvenPercents = (count) => {
    if (!count) return []
    const base = Math.floor(100 / count)
    const remainder = Math.max(0, 100 - base * count)
    return Array.from({ length: count }, (_, index) => base + (index < remainder ? 1 : 0))
  }

  const handleTargetChange = (index, field, rawValue) => {
    const nextTargets = targets.map((target, idx) => {
      if (idx !== index) return target

      const updated = { ...target }

      if (field === 'size_percent') {
        const numeric = rawValue === '' ? null : Number(rawValue)
        const roundedPercent = Number.isFinite(numeric) ? Math.round(numeric) : null
        updated.size_percent = roundedPercent
        // Also update size_fraction for v2 schema
        updated.size_fraction = roundedPercent !== null ? roundedPercent / 100 : null
      } else if (field === 'r_multiple') {
        const numeric = rawValue === '' ? null : Number(rawValue)
        updated.r_multiple = Number.isFinite(numeric) ? numeric : null
      } else if (field === 'label' && typeof rawValue === 'string') {
        updated.label = rawValue
      } else {
        updated[field] = rawValue
      }

      return updated
    })
    // Don't auto-distribute when manually editing - just apply the changes
    applyTargets(nextTargets, { autoDistribute: false })
  }

  const applyTargets = (nextTargets, { autoDistribute = false } = {}) => {
    let normalised = normalizeTargets({ ...template, take_profit_orders: nextTargets })

    // Always auto-distribute when requested (add/remove target)
    if (autoDistribute) {
      if (normalised.length === 1) {
        normalised = [{ ...normalised[0], size_percent: 100, size_fraction: 1.0 }]
      } else if (normalised.length > 1) {
        const evenPercents = distributeEvenPercents(normalised.length)
        normalised = normalised.map((target, idx) => ({
          ...target,
          size_percent: evenPercents[idx],
          size_fraction: evenPercents[idx] / 100,
        }))
      }
    }

    update({ take_profit_orders: normalised })
  }

  const addTarget = () => {
    const nextTargets = [
      ...targets,
      {
        id: `tp-${targets.length + 1}`,
        label: `TP ${targets.length + 1}`,
        r_multiple: targets.length + 1,
        size_percent: null,
      },
    ]
    applyTargets(nextTargets, { autoDistribute: true })
  }

  const removeTarget = (index) => {
    const nextTargets = targets.filter((_, idx) => idx !== index)
    applyTargets(nextTargets, { autoDistribute: true })
  }

  const stopAdjustments = useMemo(
    () =>
      (Array.isArray(template.stop_adjustments) ? template.stop_adjustments : []).map((rule, index) => {
        // v2 schema: nested trigger/action format
        if (!rule?.trigger || !rule?.action) return null

        const triggerType = rule.trigger.type === 'target_hit' ? 'target_hit' : 'r_multiple'
        const triggerValue = rule.trigger.value ?? 1

        // Check action type - could be move_to_breakeven, move_to_r, or trail_atr
        let actionType = 'move_to_breakeven'
        let actionValue = null
        let atrPeriod = null
        let atrMultiplier = null

        if (rule.action.type === 'move_to_r') {
          actionType = 'move_to_r'
          actionValue = rule.action.value ?? 0
        } else if (rule.action.type === 'trail_atr') {
          actionType = 'trail_atr'
          atrPeriod = rule.action.atr_period ?? 14
          atrMultiplier = rule.action.atr_multiplier ?? 1.0
        }

        return {
          id: rule.id || `sa-${index + 1}`,
          trigger_type: triggerType,
          trigger_value: triggerValue,
          action_type: actionType,
          action_value: actionValue,
          atr_period: atrPeriod,
          atr_multiplier: atrMultiplier,
        }
      }).filter(Boolean),
    [template.stop_adjustments],
  )

  const trailing = template.trailing || {}

  const trailingEnabled = trailing.enabled === true
  const trailingActivationType = trailing.activation_type === 'target_hit' ? 'target_hit' : 'r_multiple'
  const trailingTargetOptions = useMemo(
    () => targets.map((target) => ({ label: target.label || target.id, value: target.id || target.label })),
    [targets],
  )
  const trailingActivationTarget = useMemo(() => {
    const desired = trailing.target_id ?? trailing.target_index ?? trailing.targetId
    return trailingTargetOptions.find((option) => String(option.value) === String(desired)) || null
  }, [trailing.target_id, trailing.target_index, trailing.targetId, trailingTargetOptions])

  const maxTargetRMultiple = useMemo(
    () =>
      targets.reduce((maxR, target) => {
        const numeric = Number(target.r_multiple)
        if (!Number.isFinite(numeric)) return maxR
        return Math.max(maxR, numeric)
      }, 0),
    [targets],
  )
  const trailingActivationWarning = useMemo(() => {
    const activation = (() => {
      if (!trailingEnabled) return null
      if (trailingActivationType === 'target_hit') {
        const match = targets.find((target) => String(target.id || target.label) === String(trailingActivationTarget?.value))
        const value = Number(match?.r_multiple)
        return Number.isFinite(value) ? value : null
      }
      const value = Number(trailing?.r_multiple)
      return Number.isFinite(value) ? value : null
    })()

    return activation !== null && activation > 0 && maxTargetRMultiple > 0 && activation > maxTargetRMultiple
  }, [trailing?.r_multiple, trailingActivationType, trailingActivationTarget?.value, trailingEnabled, maxTargetRMultiple, targets])
  const [stopOpen, setStopOpen] = useState(true)
  const [targetsOpen, setTargetsOpen] = useState(true)
  const [positionOpen, setPositionOpen] = useState(true)
  const [riskUnitOpen, setRiskUnitOpen] = useState(true)

  const updateStopAdjustments = (next) => {
    // Convert flat format (used in UI) to v2 nested format for storage
    const v2StopAdjustments = next.map((rule) => ({
      id: rule.id,
      trigger: {
        type: rule.trigger_type === 'target_hit' ? 'target_hit' : 'r_multiple_reached',
        value: rule.trigger_value,
      },
      action: {
        type: rule.action_type === 'trail_atr'
          ? 'trail_atr'
          : rule.action_type === 'move_to_r'
            ? 'move_to_r'
            : 'move_to_breakeven',
        ...(rule.action_type === 'move_to_r' && rule.action_value !== null && rule.action_value !== undefined
          ? { value: rule.action_value }
          : {}),
        ...(rule.action_type === 'trail_atr'
          ? {
              atr_period: rule.atr_period ?? template.initial_stop?.atr_period ?? 14,
              atr_multiplier: rule.atr_multiplier ?? template.initial_stop?.atr_multiplier ?? 1.0,
            }
          : {}),
      },
    }))
    update({ stop_adjustments: v2StopAdjustments })
  }

  const addStopAdjustment = () => {
    const nextRule = {
      id: `sa-${stopAdjustments.length + 1}`,
      trigger_type: targets.length ? 'target_hit' : 'r_multiple',
      trigger_value: targets.length ? targets[0]?.id ?? null : 1,
      action_type: 'move_to_breakeven',
      action_value: null,
    }
    updateStopAdjustments([...stopAdjustments, nextRule])
  }

  const handleStopAdjustmentChange = (index, patch) => {
    const next = stopAdjustments.map((rule, idx) => {
      if (idx !== index) return rule
      const updated = { ...rule, ...patch }

      if (updated.trigger_type === 'r_multiple') {
        const numeric = Number(updated.trigger_value)
        updated.trigger_value = Number.isFinite(numeric) && numeric > 0 ? numeric : ''
      }

      if (updated.trigger_type === 'target_hit' && !targets.length) {
        updated.trigger_type = 'r_multiple'
        updated.trigger_value = 1
      }

      if (updated.action_type === 'move_to_r') {
        const numeric = Number(updated.action_value)
        updated.action_value = Number.isFinite(numeric) && numeric > 0 ? numeric : ''
      } else {
        updated.action_value = null
      }

      return updated
    })
    updateStopAdjustments(next)
  }

  const removeStopAdjustment = (index) => {
    const next = stopAdjustments.filter((_, idx) => idx !== index)
    updateStopAdjustments(next)
  }

  const updateTrailing = (patch = {}) => {
    update({ trailing: { ...DEFAULT_ATM_TEMPLATE.trailing, ...trailing, ...patch } })
  }

  const handleTrailingActivationTypeChange = (nextType) => {
    const type = nextType === 'target_hit' && targets.length ? 'target_hit' : 'r_multiple'
    if (type === 'target_hit') {
      const selected = trailingActivationTarget || trailingTargetOptions[0] || null
      const nextIndex = selected
        ? targets.findIndex((target) => String(target.id || target.label) === String(selected.value))
        : null
      updateTrailing({
        activation_type: 'target_hit',
        target_id: selected?.value ?? null,
        target_index: nextIndex >= 0 ? nextIndex : null,
        r_multiple: null,
        enabled: true,
      })
      return
    }

    updateTrailing({ activation_type: 'r_multiple', target_id: null, target_index: null, r_multiple: trailing.r_multiple ?? 1, enabled: true })
  }

  const handleTrailingActivationValueChange = (value) => {
    const numeric = value === '' ? null : Number(value)
    updateTrailing({ activation_type: 'r_multiple', target_id: null, target_index: null, r_multiple: numeric, enabled: true })
  }

  const handleTrailingTargetChange = (targetValue) => {
    const index = targets.findIndex((target) => String(target.id || target.label) === String(targetValue))
    updateTrailing({
      activation_type: 'target_hit',
      target_id: targetValue,
      target_index: index >= 0 ? index : null,
      r_multiple: null,
      enabled: true,
    })
  }

  const resolvedContractSize = template.contract_size ?? template._meta?.contract_size ?? 1

  const latestAtrValue =
    template._meta?.latest_atr ?? template._meta?.atr_preview ?? template._meta?.atr ?? template._meta?.atr_at_entry ?? null

  const stopDistance = template.stop_r_multiple
  const stopDistanceInvalid =
    stopDistance !== null && stopDistance !== undefined && Number.isFinite(Number(stopDistance)) && Number(stopDistance) <= 0
  const stopDistanceError =
    validationErrors.stop_r_multiple || (stopDistanceInvalid ? 'Stop distance must be positive.' : null)

  return (
    <div className="space-y-4 rounded-2xl border border-white/10 bg-black/30 p-4 text-sm">
      <div>
        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Template name</label>
        <input
          className={inputClasses}
          value={template.name || ''}
          onChange={(event) => update({ name: event.target.value })}
          placeholder="Name this ATM template"
        />
        <p className="mt-1 text-[11px] text-slate-500">Required for saving and reusing this template.</p>
        {validationErrors.name ? <p className="mt-1 text-[11px] text-rose-400">{validationErrors.name}</p> : null}
      </div>
      <div className="grid gap-4 lg:grid-cols-[1.05fr,0.95fr]">
        <div className="space-y-4">
          {!hidePositionSizing && (
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Position setup</p>
                  <p className="text-[11px] text-slate-500">Contracts and market sizing inputs.</p>
                </div>
                {collapsible && (
                  <button
                    type="button"
                    className="text-xs text-slate-300"
                    onClick={() => setPositionOpen((open) => !open)}
                  >
                    {positionOpen ? 'Collapse' : 'Expand'}
                  </button>
                )}
              </div>
              {(positionOpen || !collapsible) && (
                <div className="mt-3 space-y-4">
                  <div className="grid gap-3 md:grid-cols-2">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Contracts</label>
                      <input
                        className={inputClasses}
                        type="number"
                        min={1}
                        value={template.contracts ?? ''}
                        onChange={(event) =>
                          update({
                            contracts: Math.max(1, Number(event.target.value) || DEFAULT_ATM_TEMPLATE.contracts),
                          })
                        }
                      />
                      <p className="mt-1 text-[11px] text-slate-500">How many contracts to open per position.</p>
                    </div>
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Contract size</label>
                      <input
                        className={inputClasses}
                        type="number"
                        step="any"
                        placeholder="Auto"
                        value={template._meta?.contract_size_override ? template.contract_size ?? '' : ''}
                        onChange={(event) => applyOverrideField('contract_size', event.target.value)}
                      />
                      <p className="mt-1 text-[11px] text-slate-500">{autoLabel(template.contract_size, template._meta?.contract_size_override, resolvedContractSize, ' contracts')}</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {!hideRiskSettings && (
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Risk unit (R) settings</p>
                  <p className="text-[11px] text-slate-500">1R uses ATR-based sizing from your risk step.</p>
                </div>
                {collapsible && (
                  <button type="button" className="text-xs text-slate-300" onClick={() => setRiskUnitOpen((open) => !open)}>
                    {riskUnitOpen ? 'Collapse' : 'Expand'}
                  </button>
                )}
              </div>
              {(riskUnitOpen || !collapsible) && (
                <div className="mt-3 space-y-3">
                  <div className="grid gap-3 md:grid-cols-2">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</label>
                      <input
                        className={inputClasses}
                        type="number"
                        min={1}
                        value={template.initial_stop?.atr_period ?? DEFAULT_ATM_TEMPLATE.initial_stop.atr_period}
                        onChange={(event) =>
                          update({
                            initial_stop: {
                              ...template.initial_stop,
                              atr_period: Math.max(1, Number(event.target.value) || DEFAULT_ATM_TEMPLATE.initial_stop.atr_period),
                            },
                          })
                        }
                      />
                    </div>
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR multiplier (1R)</label>
                      <input
                        className={inputClasses}
                        type="number"
                        step="0.1"
                        value={template.initial_stop?.atr_multiplier ?? DEFAULT_ATM_TEMPLATE.initial_stop.atr_multiplier}
                        onChange={(event) =>
                          update({
                            initial_stop: {
                              ...template.initial_stop,
                              atr_multiplier: Number(event.target.value) || DEFAULT_ATM_TEMPLATE.initial_stop.atr_multiplier,
                            },
                          })
                        }
                      />
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/40 p-3 text-[12px] text-slate-300">
                    {Number.isFinite(Number(latestAtrValue))
                      ? `1R ≈ ${formatNumber((template.initial_stop?.atr_multiplier ?? DEFAULT_ATM_TEMPLATE.initial_stop.atr_multiplier) * Number(latestAtrValue))} (based on latest ATR)`
                      : '1R will resolve from ATR when data is available.'}
                  </div>
                </div>
              )}
            </div>
          )}

          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Initial stop</p>
                <p className="text-[11px] text-slate-500">Where the first protective stop begins.</p>
              </div>
              {collapsible && (
                <button type="button" className="text-xs text-slate-300" onClick={() => setStopOpen((open) => !open)}>
                  {stopOpen ? 'Collapse' : 'Expand'}
                </button>
              )}
            </div>
            {(stopOpen || !collapsible) && (
              <div className="mt-3 space-y-2 md:w-2/3">
                <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Stop distance (R)</label>
                <input
                  className={inputClasses}
                  type="number"
                  step="0.1"
                  value={stopDistance ?? ''}
                  onChange={(event) =>
                    update({
                      stop_ticks: null,
                      stop_r_multiple: event.target.value === '' ? null : Number(event.target.value),
                      stop_price: null,
                    })
                  }
                />
                <p className="text-[11px] text-slate-500">Distance in R (must be positive).</p>
                {stopDistanceError ? <p className="text-[11px] text-rose-400">{stopDistanceError}</p> : null}
              </div>
            )}
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Take-profit targets</p>
                <p className="text-[11px] text-slate-500">All targets use R multiples from your risk settings.</p>
              </div>
              <div className="flex items-center gap-2">
                {collapsible && (
                  <button type="button" className="text-xs text-slate-300" onClick={() => setTargetsOpen((open) => !open)}>
                    {targetsOpen ? 'Collapse' : 'Expand'}
                  </button>
                )}
                <button type="button" className={fieldButtonClasses} onClick={addTarget}>
                  Add target
                </button>
              </div>
            </div>
            {(targetsOpen || !collapsible) && (
              <div className="mt-3 space-y-3">
                {targets.length > 0 && Math.abs(targetSizeTotal - 100) > 0.001 && (
                  <div className="rounded-xl border border-rose-500/30 bg-rose-500/5 p-3 text-xs text-rose-200">
                    <p className="font-semibold">Allocation across targets must sum to 100%.</p>
                    <p className="mt-1 text-rose-100/80">Total: {formatNumber(targetSizeTotal)}% (must be 100%).</p>
                  </div>
                )}
                {targets.length === 0 && <p className="text-sm text-slate-400">No targets yet. Add one to get started.</p>}
                {targets.map((target, index) => (
                  <div key={target.id || index} className="rounded-xl border border-white/10 bg-black/40 p-3">
                    <div className="grid gap-3 md:grid-cols-[1.1fr,1fr,0.6fr] md:items-end">
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Label</label>
                        <input
                          className={inputClasses}
                          value={target.label || ''}
                          onChange={(event) => handleTargetChange(index, 'label', event.target.value)}
                        />
                      </div>
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Target (R multiple)</label>
                        <input
                          className={inputClasses}
                          type="number"
                          step="0.1"
                          value={target.r_multiple ?? ''}
                          onChange={(event) => handleTargetChange(index, 'r_multiple', event.target.value)}
                        />
                      </div>
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Size (%)</label>
                        <input
                          className={inputClasses}
                          type="number"
                          min={0}
                          max={100}
                          step="1"
                          value={target.size_percent ?? ''}
                          readOnly={targets.length === 1}
                          onChange={(event) => handleTargetChange(index, 'size_percent', event.target.value)}
                        />
                        <p className="mt-1 text-[11px] text-slate-500">Percent of position closed at this target.</p>
                      </div>
                    </div>
                    <div className="mt-2 flex justify-end">
                      <button type="button" className="text-[11px] text-slate-400 hover:text-slate-200" onClick={() => removeTarget(index)}>
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="space-y-4">
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Stop adjustment</p>
                <p className="text-[11px] text-slate-500">Modify or trail the protective stop after predefined progress.</p>
                <p className="text-[11px] text-slate-500">
                  Available actions: move to breakeven, move to a specific R level, or trail with ATR.
                </p>
              </div>
              {stopAdjustments.length > 0 && (
                <button type="button" className={fieldButtonClasses} onClick={addStopAdjustment}>
                  Add rule
                </button>
              )}
            </div>
            {stopAdjustments.length === 0 && (
              <div className="mt-3 flex items-start justify-between rounded-xl border border-white/10 bg-black/40 p-3">
                <div>
                  <p className="text-sm font-medium text-slate-100">Add stop adjustment</p>
                  <p className="text-[11px] text-slate-500">Modify the protective stop after predefined progress.</p>
                </div>
                <button type="button" className={fieldButtonClasses} onClick={addStopAdjustment}>
                  Add stop adjustment
                </button>
              </div>
            )}
            {stopAdjustments.length > 0 && (
              <div className="mt-3 space-y-3">
                {stopAdjustments.map((rule, index) => {
                  const triggerIsTarget = rule.trigger_type === 'target_hit'
                  const actionIsMoveToR = rule.action_type === 'move_to_r'
                  const targetOptions = targets.map((target) => ({ label: target.label || target.id, value: target.id || target.label }))

                  return (
                    <div key={rule.id || index} className="space-y-3 rounded-xl border border-white/10 bg-black/40 p-3">
                      <div className="grid gap-3 md:grid-cols-2">
                        <div className="space-y-2">
                          <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Trigger</p>
                          <div>
                            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Trigger type</label>
                            <select
                              className={inputClasses}
                              value={rule.trigger_type}
                              onChange={(event) => {
                                const nextType = event.target.value === 'target_hit' && targets.length === 0 ? 'r_multiple' : event.target.value
                                const nextValue =
                                  nextType === 'target_hit'
                                    ? targets[0]?.id ?? null
                                    : rule.trigger_type === 'target_hit'
                                      ? 1
                                      : rule.trigger_value ?? 1
                                handleStopAdjustmentChange(index, { trigger_type: nextType, trigger_value: nextValue })
                              }}
                            >
                              <option value="r_multiple">R multiple</option>
                              <option value="target_hit" disabled={targets.length === 0}>
                                Target hit
                              </option>
                            </select>
                          </div>
                          <div>
                            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Trigger value</label>
                            {triggerIsTarget ? (
                              <select
                                className={inputClasses}
                                value={rule.trigger_value ?? ''}
                                disabled={targets.length === 0}
                                onChange={(event) => handleStopAdjustmentChange(index, { trigger_value: event.target.value })}
                              >
                                {targets.length === 0 && <option>No targets available</option>}
                                {targetOptions.map((option) => (
                                  <option key={option.value} value={option.value}>
                                    {option.label}
                                  </option>
                                ))}
                              </select>
                            ) : (
                              <input
                                className={inputClasses}
                                type="number"
                                step="0.1"
                                min={0.01}
                                value={rule.trigger_value ?? ''}
                                onChange={(event) =>
                                  handleStopAdjustmentChange(index, {
                                    trigger_value: event.target.value === '' ? '' : Number(event.target.value),
                                  })
                                }
                              />
                            )}
                          </div>
                        </div>

                        <div className="space-y-2">
                          <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Action</p>
                          <div>
                            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Action type</label>
                            <select
                              className={inputClasses}
                              value={rule.action_type}
                              onChange={(event) => {
                                const nextType = event.target.value
                                handleStopAdjustmentChange(index, {
                                  action_type: nextType,
                                  action_value: nextType === 'move_to_r' ? rule.action_value ?? 0 : null,
                                  atr_period: nextType === 'trail_atr' ? rule.atr_period ?? template.initial_stop?.atr_period ?? 14 : null,
                                  atr_multiplier: nextType === 'trail_atr' ? rule.atr_multiplier ?? template.initial_stop?.atr_multiplier ?? 1.0 : null,
                                })
                              }}
                            >
                              <option value="move_to_breakeven">Move to breakeven (0R)</option>
                              <option value="move_to_r">Move to X R</option>
                              <option value="trail_atr">Trail with ATR</option>
                            </select>
                          </div>
                            {actionIsMoveToR && (
                              <div>
                                <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Action value (R)</label>
                                <input
                                  className={inputClasses}
                                  type="number"
                                  step="0.1"
                                  min={0.01}
                                  value={rule.action_value ?? ''}
                                  onChange={(event) =>
                                    handleStopAdjustmentChange(index, {
                                      action_value: event.target.value === '' ? '' : Number(event.target.value),
                                    })
                                  }
                                />
                              </div>
                            )}
                            {rule.action_type === 'trail_atr' && (
                              <>
                                <div>
                                  <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</label>
                                  <input
                                    className={inputClasses}
                                    type="number"
                                    min={1}
                                    value={rule.atr_period ?? template.initial_stop?.atr_period ?? 14}
                                    onChange={(event) =>
                                      handleStopAdjustmentChange(index, {
                                        atr_period: Math.max(1, Number(event.target.value) || 14),
                                      })
                                    }
                                  />
                                </div>
                                <div>
                                  <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR multiplier</label>
                                  <input
                                    className={inputClasses}
                                    type="number"
                                    step="0.1"
                                    min={0.01}
                                    value={rule.atr_multiplier ?? template.initial_stop?.atr_multiplier ?? 1.0}
                                    onChange={(event) =>
                                      handleStopAdjustmentChange(index, {
                                        atr_multiplier: Number(event.target.value) || 1.0,
                                      })
                                    }
                                  />
                                </div>
                              </>
                            )}
                        </div>
                      </div>

                      <div className="flex justify-end">
                        <button
                          type="button"
                          className="text-[11px] text-slate-400 hover:text-slate-200"
                          onClick={() => removeStopAdjustment(index)}
                        >
                          Remove
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
