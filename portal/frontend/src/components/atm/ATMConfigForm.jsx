import { useMemo, useState } from 'react'

export const DEFAULT_ATM_TEMPLATE = {
  contracts: 1,
  stop_ticks: null,
  stop_r_multiple: 1,
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
  cloned.rMode = 'atr'
  cloned.risk_unit_mode = 'atr'
  if (cloned.rAtrPeriod === undefined || cloned.rAtrPeriod === null) cloned.rAtrPeriod = DEFAULT_ATM_TEMPLATE.rAtrPeriod
  if (cloned.rAtrMultiplier === undefined || cloned.rAtrMultiplier === null)
    cloned.rAtrMultiplier = DEFAULT_ATM_TEMPLATE.rAtrMultiplier
  cloned.rRiskTicks = null
  cloned.ticks_stop = null
  if (cloned.stop_r_multiple === undefined || cloned.stop_r_multiple === null) {
    cloned.stop_r_multiple = DEFAULT_ATM_TEMPLATE.stop_r_multiple
  }
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
  return entries.map((target, index) => {
    const next = { ...target }
    if (next.r_multiple === undefined || next.r_multiple === null) {
      next.r_multiple = index + 1
    }
    next.ticks = null
    next.price = null
    return next
  })
}

export default function ATMConfigForm({ value, onChange, hidePositionSizing = false, hideRiskSettings = false, collapsible = false }) {
  const template = useMemo(() => cloneATMTemplate(value), [value])
  const targets = useMemo(() => normalizeTargets(template), [template])

  const stopMode = 'r'

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
      if (field === 'contracts') {
        const numeric = Number(rawValue)
        valueToApply = Number.isFinite(numeric) ? numeric : target[field]
      }
      if (field === 'r_multiple') {
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

  const addTarget = () => {
    const nextTargets = [
      ...targets,
      {
        id: `tp-${targets.length + 1}`,
        label: `TP +${(targets.length + 1) * 20}`,
        r_multiple: (targets.length + 1) * 1,
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

  const breakevenActivation = 'r'
  const trailingActivation = 'r'

  const handleBreakevenActivation = (mode, value) => {
    const next = { ...breakeven, enabled: true, target_index: null }
    next.r_multiple = value ?? 1
    next.ticks = null
    update({ breakeven: next })
  }

  const handleTrailingActivation = (mode, value) => {
    const next = { ...trailing, enabled: true, target_index: null }
    next.r_multiple = value ?? 1
    next.ticks = null
    update({ trailing: next })
  }

  const resolvedContractSize = template.contract_size ?? template._meta?.contract_size ?? 1

  const latestAtrValue =
    template._meta?.latest_atr ?? template._meta?.atr_preview ?? template._meta?.atr ?? template._meta?.atr_at_entry ?? null

  const oneR = useMemo(() => {
    const atr = Number(latestAtrValue)
    if (!Number.isFinite(atr)) {
      return { price: null }
    }
    const priceMove = Number(template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier) * atr
    return { price: priceMove }
  }, [latestAtrValue, template.rAtrMultiplier])

  const describeRApprox = (multiple = 1) => {
    if (!multiple || !oneR) return ''
    const numericMultiple = Number(multiple)
    if (!Number.isFinite(numericMultiple)) return ''
    const price = Number.isFinite(oneR.price) ? numericMultiple * Number(oneR.price) : null
    const parts = []
    if (price !== null) parts.push(`${formatNumber(price)} price move`)
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
                        value={template.rAtrPeriod ?? DEFAULT_ATM_TEMPLATE.rAtrPeriod}
                        onChange={(event) =>
                          update({ rAtrPeriod: Math.max(1, Number(event.target.value) || DEFAULT_ATM_TEMPLATE.rAtrPeriod) })
                        }
                      />
                    </div>
                    <div>
                      <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR multiplier (1R)</label>
                      <input
                        className={inputClasses}
                        type="number"
                        step="0.1"
                        value={template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier}
                        onChange={(event) => update({ rAtrMultiplier: Number(event.target.value) || DEFAULT_ATM_TEMPLATE.rAtrMultiplier })}
                      />
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/40 p-3 text-[12px] text-slate-300">
                    {Number.isFinite(Number(latestAtrValue))
                      ? `1R ≈ ${formatNumber((template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier) * Number(latestAtrValue))} (based on latest ATR)`
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
                  min={0}
                  value={template.stop_r_multiple ?? ''}
                  onChange={(event) =>
                    update({
                      stop_ticks: null,
                      stop_r_multiple: event.target.value === '' ? null : Number(event.target.value),
                      stop_price: null,
                    })
                  }
                />
                {describeRApprox(template.stop_r_multiple) && (
                  <p className="text-[11px] text-slate-500">{describeRApprox(template.stop_r_multiple)}</p>
                )}
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
                        {describeRApprox(target.r_multiple) && (
                          <p className="mt-1 text-[11px] text-slate-500">{describeRApprox(target.r_multiple)}</p>
                        )}
                      </div>
                      <div>
                        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Size</label>
                        <input
                          className={inputClasses}
                          type="number"
                          min={0}
                          value={target.contracts ?? ''}
                          onChange={(event) => handleTargetChange(index, 'contracts', event.target.value)}
                        />
                        <p className="mt-1 text-[11px] text-slate-500">Contracts at this target.</p>
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
                        ticks: null,
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
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Move stop after R multiple</label>
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
                        ticks: null,
                        atr_multiplier:
                          trailing.atr_multiplier ?? (template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier),
                        atr_period: template.rAtrPeriod ?? DEFAULT_ATM_TEMPLATE.rAtrPeriod,
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

                <div className="grid gap-3 md:grid-cols-2">
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Trail distance (R)</label>
                    <input
                      className={inputClasses}
                      type="number"
                      step="0.1"
                      min={0}
                      value={(() => {
                        const base = template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier
                        const distance = trailing.atr_multiplier ?? base
                        return base ? distance / base : distance
                      })() ?? ''}
                      onChange={(event) => {
                        const base = template.rAtrMultiplier ?? DEFAULT_ATM_TEMPLATE.rAtrMultiplier ?? 1
                        const desiredR = event.target.value === '' ? null : Number(event.target.value)
                        update({
                          trailing: {
                            ...trailing,
                            atr_multiplier: desiredR === null ? null : desiredR * base,
                            atr_period: template.rAtrPeriod ?? DEFAULT_ATM_TEMPLATE.rAtrPeriod,
                          },
                        })
                      }}
                    />
                  </div>
                  <div className="text-[11px] text-slate-500">Trail distance is expressed in R multiples from entry.</div>
                </div>

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
