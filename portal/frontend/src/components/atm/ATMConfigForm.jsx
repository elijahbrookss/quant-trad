import { useMemo, useState } from 'react'

export const DEFAULT_ATM_TEMPLATE = {
  contracts: 1,
  stop_ticks: null,
  stop_r_multiple: null,
  stop_price: null,
  take_profit_orders: [],
  breakeven: { enabled: false },
  trailing: { enabled: false },
  tick_size: null,
  tick_value: null,
  contract_size: null,
  risk_unit_mode: 'atr',
  ticks_stop: null,
  global_risk_multiplier: 1,
  rMode: 'atr',
  rAtrPeriod: 14,
  rAtrMultiplier: 1,
  rRiskTicks: null,
  base_risk_per_trade: null,
}

export function cloneATMTemplate(template = DEFAULT_ATM_TEMPLATE) {
  let cloned
  try {
    cloned = JSON.parse(JSON.stringify(template || DEFAULT_ATM_TEMPLATE))
  } catch {
    cloned = JSON.parse(JSON.stringify(DEFAULT_ATM_TEMPLATE))
  }
  if (cloned.rMode !== 'atr' && cloned.rMode !== 'ticks' && cloned.rMode !== 'explicit') {
    cloned.rMode = 'atr'
  }
  if (cloned.risk_unit_mode !== 'atr' && cloned.risk_unit_mode !== 'ticks') {
    cloned.risk_unit_mode = cloned.rMode
  }
  if (cloned.rMode !== cloned.risk_unit_mode) {
    cloned.rMode = cloned.risk_unit_mode
  }
  if (cloned.rAtrPeriod === undefined || cloned.rAtrPeriod === null) cloned.rAtrPeriod = DEFAULT_ATM_TEMPLATE.rAtrPeriod
  if (cloned.rAtrMultiplier === undefined || cloned.rAtrMultiplier === null)
    cloned.rAtrMultiplier = DEFAULT_ATM_TEMPLATE.rAtrMultiplier
  if (cloned.rRiskTicks === undefined) cloned.rRiskTicks = DEFAULT_ATM_TEMPLATE.rRiskTicks
  if (cloned.ticks_stop === undefined) cloned.ticks_stop = DEFAULT_ATM_TEMPLATE.ticks_stop
  if (cloned.global_risk_multiplier === undefined) cloned.global_risk_multiplier = DEFAULT_ATM_TEMPLATE.global_risk_multiplier
  if (cloned.base_risk_per_trade === undefined) cloned.base_risk_per_trade = DEFAULT_ATM_TEMPLATE.base_risk_per_trade
  if (!cloned._meta || typeof cloned._meta !== 'object') {
    cloned._meta = {}
  }
  return cloned
}

const fieldButtonClasses =
  'rounded-lg border border-white/10 bg-white/5 px-2 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-slate-300 transition hover:border-white/30 hover:text-white'

const inputClasses =
  'mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none'

const UNIT_OPTIONS = [
  { value: 'ticks', label: 'Ticks' },
  { value: 'r', label: 'R multiple' },
]

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

function normalizeTargets(template) {
  const entries = Array.isArray(template?.take_profit_orders) ? template.take_profit_orders : []
  return entries
}

export default function ATMConfigForm({ value, onChange, hidePositionSizing = false, hideRiskSettings = false, collapsible = false }) {
  const template = useMemo(() => cloneATMTemplate(value), [value])
  const targets = useMemo(() => normalizeTargets(template), [template])

  const stopMode = useMemo(() => {
    if (template.stop_r_multiple !== null && template.stop_r_multiple !== undefined) return 'r'
    return 'ticks'
  }, [template.stop_r_multiple])

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

  const handleTargetChange = (index, field, rawValue) => {
    const nextTargets = targets.map((target, idx) => {
      if (idx !== index) return target
      let valueToApply = rawValue
      if (['ticks', 'contracts'].includes(field)) {
        const numeric = Number(rawValue)
        valueToApply = Number.isFinite(numeric) ? numeric : target[field]
      }
      if (field === 'r_multiple' || field === 'price') {
        const numeric = rawValue === '' ? null : Number(rawValue)
        valueToApply = Number.isFinite(numeric) ? numeric : null
      }
      if (field === 'label' && typeof rawValue === 'string') {
        valueToApply = rawValue
      }
      return { ...target, [field]: valueToApply }
    })
    update({ take_profit_orders: nextTargets })
  }

  const handleTargetModeChange = (index, mode) => {
    const nextTargets = targets.map((target, idx) => {
      if (idx !== index) return target
      const base = { ...target, ticks: null, r_multiple: null, price: null }
      if (mode === 'ticks') {
        base.ticks = target.ticks ?? (idx + 1) * 20
      }
      if (mode === 'r') {
        base.r_multiple = target.r_multiple ?? 1
      }
      if (mode === 'price') {
        base.price = target.price ?? null
      }
      return base
    })
    update({ take_profit_orders: nextTargets })
  }

  const addTarget = () => {
    const nextTargets = [
      ...targets,
      {
        id: `tp-${targets.length + 1}`,
        label: `TP +${(targets.length + 1) * 20}`,
        ticks: (targets.length + 1) * 20,
        contracts: 1,
      },
    ]
    update({ take_profit_orders: nextTargets })
  }

  const removeTarget = (index) => {
    const nextTargets = targets.filter((_, idx) => idx !== index)
    update({ take_profit_orders: nextTargets })
  }

  const breakeven = template.breakeven || {}
  const trailing = template.trailing || {}

  const breakevenEnabled = breakeven.enabled === true
  const trailingEnabled = trailing.enabled === true
  const [stopOpen, setStopOpen] = useState(true)
  const [targetsOpen, setTargetsOpen] = useState(true)
  const [positionOpen, setPositionOpen] = useState(true)
  const [riskUnitOpen, setRiskUnitOpen] = useState(true)

  const breakevenActivation = useMemo(() => {
    if (breakeven.r_multiple !== null && breakeven.r_multiple !== undefined) return 'r'
    if (breakeven.ticks !== null && breakeven.ticks !== undefined) return 'ticks'
    return 'r'
  }, [breakeven])

  const trailingActivation = useMemo(() => {
    if (trailing.r_multiple !== null && trailing.r_multiple !== undefined) return 'r'
    if (trailing.ticks !== null && trailing.ticks !== undefined) return 'ticks'
    return 'r'
  }, [trailing])

  const trailingMode = useMemo(() => {
    if (trailing.atr_multiplier !== null && trailing.atr_multiplier !== undefined) return 'atr'
    return 'ticks'
  }, [trailing])

  const handleBreakevenActivation = (mode, value) => {
    const next = { ...breakeven, enabled: true, target_index: null }
    if (mode === 'ticks') {
      next.ticks = value ?? null
      next.r_multiple = null
    } else {
      next.r_multiple = value ?? 1
      next.ticks = null
    }
    update({ breakeven: next })
  }

  const handleTrailingActivation = (mode, value) => {
    const next = { ...trailing, enabled: true, target_index: null }
    if (mode === 'ticks') {
      next.ticks = value ?? null
      next.r_multiple = null
    } else {
      next.r_multiple = value ?? 1
      next.ticks = trailingMode === 'ticks' ? trailing.ticks ?? null : null
    }
    update({ trailing: next })
  }

  const handleTrailingMode = (mode) => {
    const next = { ...trailing }
    if (mode === 'atr') {
      next.atr_multiplier = trailing.atr_multiplier ?? 1.0
      next.atr_period = trailing.atr_period ?? 14
    } else {
      next.atr_multiplier = null
    }
    update({ trailing: next })
  }

  const resolvedTickSize = template.tick_size ?? template._meta?.tick_size ?? null
  const resolvedContractSize = template.contract_size ?? template._meta?.contract_size ?? 1
  const resolvedTickValue =
    template.tick_value ??
    template._meta?.tick_value ??
    (resolvedTickSize && resolvedContractSize ? resolvedTickSize * resolvedContractSize : null)

  const latestAtrValue =
    template._meta?.latest_atr ?? template._meta?.atr_preview ?? template._meta?.atr ?? template._meta?.atr_at_entry ?? null

  const oneR = useMemo(() => {
    if (template.rMode === 'ticks') {
      const ticks = template.rRiskTicks ?? DEFAULT_ATM_TEMPLATE.rRiskTicks
      const price = ticks && resolvedTickSize ? ticks * resolvedTickSize : null
      return { mode: 'ticks', price, ticks }
    }
    const atr = Number(latestAtrValue)
    if (!Number.isFinite(atr)) {
      return { mode: 'atr', price: null, ticks: null }
    }
    const priceMove = Number(template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier) * atr
    const ticks = resolvedTickSize ? priceMove / resolvedTickSize : null
    return { mode: 'atr', price: priceMove, ticks }
  }, [latestAtrValue, resolvedTickSize, template.rAtrMultiplier, template.rMode, template.rRiskTicks])

  const describeRApprox = (multiple = 1) => {
    if (!multiple || !oneR) return ''
    const numericMultiple = Number(multiple)
    if (!Number.isFinite(numericMultiple)) return ''
    const price = Number.isFinite(oneR.price) ? numericMultiple * Number(oneR.price) : null
    const ticks = Number.isFinite(oneR.ticks) ? numericMultiple * Number(oneR.ticks) : null
    const parts = []
    if (price !== null) parts.push(`${formatNumber(price)} pts`)
    if (ticks !== null) parts.push(`${formatNumber(ticks)} ticks`)
    if (!parts.length) return ''
    return `≈ ${parts.join(' / ')}`
  }

  return (
    <div className="space-y-4 rounded-2xl border border-white/10 bg-black/30 p-4 text-sm">
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
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Tick size</label>
                      <input
                        className={inputClasses}
                        type="number"
                        step="any"
                        placeholder="Auto"
                        value={template._meta?.tick_size_override ? template.tick_size ?? '' : ''}
                        onChange={(event) => applyOverrideField('tick_size', event.target.value)}
                      />
                      <p className="mt-1 text-[11px] text-slate-500">{autoLabel(template.tick_size, template._meta?.tick_size_override, resolvedTickSize)}</p>
                    </div>
                  </div>

                  <div className="grid gap-3 md:grid-cols-2">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Tick value</label>
                      <input
                        className={inputClasses}
                        type="number"
                        step="any"
                        placeholder="Auto"
                        value={template._meta?.tick_value_override ? template.tick_value ?? '' : ''}
                        onChange={(event) => applyOverrideField('tick_value', event.target.value)}
                      />
                      <p className="mt-1 text-[11px] text-slate-500">{autoLabel(template.tick_value, template._meta?.tick_value_override, resolvedTickValue)}</p>
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
                  <p className="text-[11px] text-slate-500">1R is your unit of risk; R inputs use this definition.</p>
                </div>
                {collapsible && (
                  <button type="button" className="text-xs text-slate-300" onClick={() => setRiskUnitOpen((open) => !open)}>
                    {riskUnitOpen ? 'Collapse' : 'Expand'}
                  </button>
                )}
              </div>
              {(riskUnitOpen || !collapsible) && (
                <div className="mt-3 space-y-3">
                  <div className="grid gap-3 md:grid-cols-2 md:items-end">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Mode</label>
                      <select
                        className={inputClasses}
                        value={template.rMode || 'atr'}
                        onChange={(event) => update({ rMode: event.target.value })}
                      >
                        <option value="atr">ATR-based</option>
                        <option value="ticks">Tick-based</option>
                        <option value="explicit">Explicit</option>
                      </select>
                    </div>
                    <div className="text-[11px] text-slate-500">
                      {template.rMode === 'ticks'
                        ? 'Use fixed ticks to define 1R for stops and targets.'
                        : 'Use ATR at entry with the multiplier below to size 1R.'}
                    </div>
                  </div>

                  {template.rMode !== 'ticks' && (
                    <div className="grid gap-3 md:grid-cols-2">
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</label>
                        <input
                          className={inputClasses}
                          type="number"
                          min={1}
                          value={template.rAtrPeriod ?? DEFAULT_ATM_TEMPLATE.rAtrPeriod}
                          onChange={(event) =>
                            update({ rAtrPeriod: Math.max(1, Number(event.target.value) || DEFAULT_ATM_TEMPLATE.rAtrPeriod) })
                          }
                        />
                      </div>
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR multiplier</label>
                        <input
                          className={inputClasses}
                          type="number"
                          step="0.1"
                          value={template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier}
                          onChange={(event) => update({ rAtrMultiplier: Number(event.target.value) || DEFAULT_ATM_TEMPLATE.rAtrMultiplier })}
                        />
                      </div>
                    </div>
                  )}

                  {template.rMode === 'ticks' && (
                    <div className="grid gap-3 md:grid-cols-2">
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Risk ticks</label>
                        <input
                          className={inputClasses}
                          type="number"
                          min={1}
                          value={template.rRiskTicks ?? ''}
                          onChange={(event) =>
                            update({ rRiskTicks: event.target.value === '' ? null : Math.max(1, Number(event.target.value) || 1) })
                          }
                        />
                      </div>
                      <div className="text-[11px] text-slate-500">Define how many ticks equal 1R.</div>
                    </div>
                  )}

                  <div className="rounded-xl border border-white/10 bg-black/40 p-3 text-[12px] text-slate-300">
                    {template.rMode === 'ticks' && template.rRiskTicks
                      ? `1R = ${formatNumber(template.rRiskTicks)} ticks${describeRApprox(1) ? ` (${describeRApprox(1)})` : ''}`
                      : template.rMode === 'atr'
                        ? Number.isFinite(Number(latestAtrValue))
                          ? `1R ≈ ${formatNumber((template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier) * Number(latestAtrValue))} (based on latest ATR)`
                          : '1R will resolve from ATR when data is available.'
                        : '1R definition pending inputs.'}
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
              <div className="mt-3 grid gap-3 md:grid-cols-[1.3fr,0.7fr]">
                <div>
                  {stopMode === 'ticks' && (
                    <input
                      className={inputClasses}
                      type="number"
                      min={1}
                      value={template.stop_ticks ?? ''}
                      onChange={(event) =>
                        update({
                          stop_ticks: event.target.value === '' ? null : Math.max(1, Number(event.target.value) || 1),
                          stop_r_multiple: null,
                          stop_price: null,
                        })
                      }
                    />
                  )}
                  {stopMode === 'r' && (
                    <div>
                      <input
                        className={inputClasses}
                        type="number"
                        step="0.1"
                        value={template.stop_r_multiple ?? ''}
                        onChange={(event) =>
                          update({ stop_ticks: null, stop_r_multiple: event.target.value === '' ? null : Number(event.target.value), stop_price: null })
                        }
                      />
                      {describeRApprox(template.stop_r_multiple) && (
                        <p className="mt-1 text-[11px] text-slate-500">{describeRApprox(template.stop_r_multiple)}</p>
                      )}
                    </div>
                  )}
                </div>
                <div>
                  <select
                    className={`${inputClasses} bg-black/50 text-xs`}
                    value={stopMode}
                    onChange={(event) => {
                      const mode = event.target.value
                      if (mode === stopMode) return
                      if (mode === 'ticks') {
                        update({ stop_ticks: template.stop_ticks ?? null, stop_r_multiple: null, stop_price: null })
                        return
                      }
                      update({
                        stop_ticks: null,
                        stop_r_multiple: template.stop_r_multiple ?? 1,
                        stop_price: null,
                      })
                    }}
                  >
                    {UNIT_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                  <p className="mt-1 text-[11px] text-slate-500">Choose ticks or R multiple.</p>
                </div>
              </div>
            )}
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Take-profit targets</p>
                <p className="text-[11px] text-slate-500">Add targets in ticks, price, or R.</p>
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
                {targets.length === 0 && <p className="text-sm text-slate-400">No targets yet. Add one to get started.</p>}
                {targets.map((target, index) => (
                  <div key={target.id || index} className="rounded-xl border border-white/10 bg-black/40 p-3">
                    <div className="grid gap-3 md:grid-cols-[1.1fr,1.2fr,0.4fr] md:items-end">
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Label</label>
                        <input
                          className={inputClasses}
                          value={target.label || ''}
                          onChange={(event) => handleTargetChange(index, 'label', event.target.value)}
                        />
                      </div>
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Target</label>
                        <div className="mt-1 space-y-1">
                          <div className="grid grid-cols-[1.1fr,0.9fr] gap-2">
                            {(target.r_multiple === null || target.r_multiple === undefined) && target.price == null && (
                              <input
                                className={inputClasses}
                                type="number"
                                value={target.ticks ?? ''}
                                onChange={(event) => handleTargetChange(index, 'ticks', event.target.value)}
                              />
                            )}
                            {target.r_multiple !== null && target.r_multiple !== undefined && (
                              <input
                                className={inputClasses}
                                type="number"
                                step="0.1"
                                value={target.r_multiple ?? ''}
                                onChange={(event) => handleTargetChange(index, 'r_multiple', event.target.value)}
                              />
                            )}
                            {target.price !== null && target.price !== undefined && (
                              <input
                                className={inputClasses}
                                type="number"
                                step="any"
                                value={target.price ?? ''}
                                onChange={(event) => handleTargetChange(index, 'price', event.target.value)}
                              />
                            )}
                            <select
                              className={`${inputClasses} bg-black/50 text-xs`}
                              value={
                                target.r_multiple !== null && target.r_multiple !== undefined
                                  ? 'r'
                                  : target.price !== null && target.price !== undefined
                                    ? 'price'
                                    : 'ticks'
                              }
                              onChange={(event) => handleTargetModeChange(index, event.target.value)}
                            >
                              {UNIT_OPTIONS.map((option) => (
                                <option key={option.value} value={option.value}>
                                  {option.label}
                                </option>
                              ))}
                            </select>
                          </div>
                          {target.r_multiple !== null && target.r_multiple !== undefined && describeRApprox(target.r_multiple) && (
                            <p className="text-[11px] text-slate-500">{describeRApprox(target.r_multiple)}</p>
                          )}
                        </div>
                      </div>
                      {!hidePositionSizing && (
                        <div>
                          <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Contracts</label>
                          <input
                            className={inputClasses}
                            type="number"
                            min={1}
                            value={target.contracts ?? 1}
                            onChange={(event) => handleTargetChange(index, 'contracts', event.target.value)}
                          />
                        </div>
                      )}
                      {hidePositionSizing && (
                        <div className="text-[11px] text-slate-500">Contracts use the global sizing.</div>
                      )}
                      <div className="flex items-center justify-end">
                        <button
                          type="button"
                          className="text-xs text-rose-300 transition hover:text-rose-100"
                          onClick={() => removeTarget(index)}
                        >
                          Remove
                        </button>
                      </div>
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
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Breakeven</p>
                <p className="text-[11px] text-slate-500">Move stop to entry after predefined progress.</p>
              </div>
              {breakevenEnabled && (
                <button
                  type="button"
                  className="text-[11px] text-slate-400 hover:text-slate-200"
                  onClick={() => update({ breakeven: { enabled: false } })}
                >
                  Remove
                </button>
              )}
            </div>
            {!breakevenEnabled && (
              <div className="mt-3 flex items-start justify-between rounded-xl border border-white/10 bg-black/40 p-3">
                <div>
                  <p className="text-sm font-medium text-slate-100">Add breakeven</p>
                  <p className="text-[11px] text-slate-500">Move stop to entry after predefined progress.</p>
                </div>
                <button
                  type="button"
                  className={fieldButtonClasses}
                  onClick={() =>
                    update({
                      breakeven: {
                        ...breakeven,
                        enabled: true,
                        target_index: null,
                        r_multiple: breakeven.r_multiple ?? 1,
                        ticks: breakeven.ticks ?? null,
                      },
                    })
                  }
                >
                  Add breakeven
                </button>
              </div>
            )}
            {breakevenEnabled && (
              <div className="mt-3 space-y-3">
                <div className="grid gap-3 md:grid-cols-[1.2fr,0.8fr] md:items-end">
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Move stop after</label>
                    <select
                      className={inputClasses}
                      value={breakevenActivation}
                      onChange={(event) => handleBreakevenActivation(event.target.value)}
                    >
                      <option value="r">After R multiple</option>
                      <option value="ticks">After ticks</option>
                    </select>
                  </div>
                  <div className="flex justify-end">
                    <button
                      type="button"
                      className="text-[11px] text-slate-400 hover:text-slate-200"
                      onClick={() => update({ breakeven: { enabled: false } })}
                    >
                      Remove
                    </button>
                  </div>
                </div>

                {breakevenActivation === 'ticks' && (
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500" title="Fallback ticks to trigger breakeven.">
                      Activate after ticks
                    </label>
                    <input
                      className={inputClasses}
                      type="number"
                      value={breakeven.ticks ?? ''}
                      onChange={(event) => handleBreakevenActivation('ticks', event.target.value === '' ? null : Number(event.target.value))}
                    />
                  </div>
                )}
                {breakevenActivation === 'r' && (
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">R multiple</label>
                    <input
                      className={inputClasses}
                      type="number"
                      step="0.1"
                      value={breakeven.r_multiple ?? ''}
                      onChange={(event) => handleBreakevenActivation('r', event.target.value === '' ? null : Number(event.target.value))}
                    />
                    {describeRApprox(breakeven.r_multiple) && (
                      <p className="mt-1 text-[11px] text-slate-500">{describeRApprox(breakeven.r_multiple)}</p>
                    )}
                  </div>
                )}
                <p className="text-[11px] text-slate-500" title="Breakeven moves the stop to entry once your trigger is reached.">
                  Breakeven moves the stop to entry using the first trigger that fires.
                </p>
              </div>
            )}
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Trailing stop</p>
                <p className="text-[11px] text-slate-500">Tighten the stop as the trade moves in your favor.</p>
              </div>
              {trailingEnabled && (
                <button
                  type="button"
                  className="text-[11px] text-slate-400 hover:text-slate-200"
                  onClick={() => update({ trailing: { enabled: false } })}
                >
                  Remove
                </button>
              )}
            </div>
            {!trailingEnabled && (
              <div className="mt-3 flex items-start justify-between rounded-xl border border-white/10 bg-black/40 p-3">
                <div>
                  <p className="text-sm font-medium text-slate-100">Add trailing stop</p>
                  <p className="text-[11px] text-slate-500">Tighten the stop as the trade moves in your favor.</p>
                </div>
                <button
                  type="button"
                  className={fieldButtonClasses}
                  onClick={() =>
                    update({
                      trailing: {
                        ...trailing,
                        enabled: true,
                        target_index: null,
                        r_multiple: trailing.r_multiple ?? 1,
                        ticks: trailing.ticks ?? null,
                      },
                    })
                  }
                >
                  Add trailing stop
                </button>
              </div>
            )}
            {trailingEnabled && (
              <div className="mt-3 space-y-3">
                <div className="grid gap-3 md:grid-cols-[1.2fr,0.8fr] md:items-end">
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Activation</label>
                    <select
                      className={inputClasses}
                      value={trailingActivation}
                      onChange={(event) => handleTrailingActivation(event.target.value)}
                    >
                      <option value="r">After R multiple</option>
                      <option value="ticks">After ticks</option>
                    </select>
                  </div>
                  <div className="flex justify-end">
                    <button
                      type="button"
                      className="text-[11px] text-slate-400 hover:text-slate-200"
                      onClick={() => update({ trailing: { enabled: false } })}
                    >
                      Remove
                    </button>
                  </div>
                </div>

                {trailingActivation === 'ticks' && (
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500" title="Start trailing once price has moved this many ticks in your favor.">
                      Activate after ticks
                    </label>
                    <input
                      className={inputClasses}
                      type="number"
                      value={trailing.ticks ?? ''}
                      onChange={(event) => handleTrailingActivation('ticks', event.target.value === '' ? null : Number(event.target.value))}
                    />
                  </div>
                )}
                {trailingActivation === 'r' && (
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Activate after R multiple</label>
                    <input
                      className={inputClasses}
                      type="number"
                      step="0.1"
                      value={trailing.r_multiple ?? ''}
                      onChange={(event) => handleTrailingActivation('r', event.target.value === '' ? null : Number(event.target.value))}
                    />
                    {describeRApprox(trailing.r_multiple) && (
                      <p className="mt-1 text-[11px] text-slate-500">{describeRApprox(trailing.r_multiple)}</p>
                    )}
                  </div>
                )}

                <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Trailing mode</label>
                <select
                  className={inputClasses}
                  value={trailingMode}
                  onChange={(event) => handleTrailingMode(event.target.value)}
                >
                  <option value="atr">ATR-based</option>
                  <option value="ticks">Fixed ticks</option>
                </select>

                {trailingMode === 'atr' && (
                  <div className="grid gap-3 md:grid-cols-2">
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</label>
                      <input
                        className={inputClasses}
                        type="number"
                        min={1}
                        value={trailing.atr_period ?? 14}
                        onChange={(event) =>
                          update({
                            trailing: {
                              ...trailing,
                              atr_period: Math.max(1, Number(event.target.value) || 14),
                              atr_multiplier: trailing.atr_multiplier ?? 1,
                            },
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
                        value={trailing.atr_multiplier ?? 1}
                        onChange={(event) =>
                          update({
                            trailing: { ...trailing, atr_multiplier: Number(event.target.value) || 1 },
                          })
                        }
                      />
                    </div>
                  </div>
                )}

                {trailingMode === 'ticks' && (
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500" title="Distance in ticks the stop will trail from the best price. Also used if ATR is unavailable.">
                      Trail distance (ticks)
                    </label>
                    <input
                      className={inputClasses}
                      type="number"
                      value={trailing.ticks ?? ''}
                      onChange={(event) => update({ trailing: { ...trailing, ticks: event.target.value === '' ? null : Number(event.target.value) } })}
                    />
                  </div>
                )}

                <p className="text-[11px] text-slate-500" title="Trailing only tightens the stop; it never loosens after activation.">
                  Trailing tightens toward price after activation; it never loosens.
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
