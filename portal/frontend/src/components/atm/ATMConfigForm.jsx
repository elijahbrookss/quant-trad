import { useMemo } from 'react'

export const DEFAULT_ATM_TEMPLATE = {
  contracts: 3,
  stop_ticks: 35,
  take_profit_orders: [
    { id: 'tp-1', label: 'TP +20', ticks: 20, contracts: 1 },
    { id: 'tp-2', label: 'TP +40', ticks: 40, contracts: 1 },
    { id: 'tp-3', label: 'TP +60', ticks: 60, contracts: 1 },
  ],
  breakeven: { target_index: 0, ticks: 20 },
  trailing: { enabled: true, target_index: 1, atr_multiplier: 1.0, atr_period: 14 },
}

export function cloneATMTemplate(template = DEFAULT_ATM_TEMPLATE) {
  let cloned
  try {
    cloned = JSON.parse(JSON.stringify(template || DEFAULT_ATM_TEMPLATE))
  } catch {
    cloned = JSON.parse(JSON.stringify(DEFAULT_ATM_TEMPLATE))
  }
  if (!cloned._meta || typeof cloned._meta !== 'object') {
    cloned._meta = {}
  }
  return cloned
}

const fieldButtonClasses =
  'rounded-lg border border-white/10 bg-white/5 px-2 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-slate-300 transition hover:border-white/30 hover:text-white'

const inputClasses =
  'mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none'

function normalizeTargets(template) {
  const entries = Array.isArray(template?.take_profit_orders) ? template.take_profit_orders : []
  if (entries.length) return entries
  return cloneATMTemplate(DEFAULT_ATM_TEMPLATE).take_profit_orders
}

export default function ATMConfigForm({ value, onChange }) {
  const template = useMemo(() => cloneATMTemplate(value), [value])
  const targets = useMemo(() => normalizeTargets(template), [template])

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
      if (field === 'ticks' || field === 'contracts') {
        const numeric = Number(rawValue)
        valueToApply = Number.isFinite(numeric) ? numeric : target[field]
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
        ticks: (targets.length + 1) * 20,
        contracts: 1,
      },
    ]
    update({ take_profit_orders: nextTargets })
  }

  const removeTarget = (index) => {
    if (targets.length <= 1) return
    const nextTargets = targets.filter((_, idx) => idx !== index)
    update({ take_profit_orders: nextTargets })
  }

  const breakeven = template.breakeven || {}
  const trailing = template.trailing || {}

  const targetOptions = targets.map((target, index) => ({
    label: target.label || `Target ${index + 1}`,
    value: index,
  }))

  return (
    <div className="space-y-4 rounded-2xl border border-white/10 bg-black/30 p-4 text-sm">
      <div className="grid gap-4 md:grid-cols-3">
        <div>
          <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Contracts</label>
          <input
            className={inputClasses}
            type="number"
            min={1}
            value={template.contracts ?? DEFAULT_ATM_TEMPLATE.contracts}
            onChange={(event) => update({ contracts: Math.max(1, Number(event.target.value) || DEFAULT_ATM_TEMPLATE.contracts) })}
          />
        </div>
        <div>
          <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Stop (ticks)</label>
          <input
            className={inputClasses}
            type="number"
            min={1}
            value={template.stop_ticks ?? DEFAULT_ATM_TEMPLATE.stop_ticks}
            onChange={(event) => update({ stop_ticks: Math.max(1, Number(event.target.value) || DEFAULT_ATM_TEMPLATE.stop_ticks) })}
          />
        </div>
        <div className="flex items-end justify-end">
          <button type="button" className={fieldButtonClasses} onClick={addTarget}>
            Add target
          </button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <div>
          <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Tick size</label>
          <input
            className={inputClasses}
            type="number"
            step="any"
            placeholder="Auto"
            value={template._meta?.tick_size_override ? template.tick_size ?? '' : ''}
            onChange={(event) => applyOverrideField('tick_size', event.target.value)}
          />
        </div>
        <div>
          <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Tick value</label>
          <input
            className={inputClasses}
            type="number"
            step="any"
            placeholder="Auto"
            value={template._meta?.tick_value_override ? template.tick_value ?? '' : ''}
            onChange={(event) => applyOverrideField('tick_value', event.target.value)}
          />
        </div>
        <div>
          <label className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Contract size</label>
          <input
            className={inputClasses}
            type="number"
            step="any"
            placeholder="Auto"
            value={template._meta?.contract_size_override ? template.contract_size ?? '' : ''}
            onChange={(event) => applyOverrideField('contract_size', event.target.value)}
          />
        </div>
      </div>

      <div className="space-y-3">
        {targets.map((target, index) => (
          <div key={target.id || index} className="rounded-xl border border-white/10 bg-white/5 p-3">
            <div className="flex items-center justify-between">
              <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Target {index + 1}</p>
              {targets.length > 1 && (
                <button type="button" className="text-xs text-rose-300 hover:text-rose-200" onClick={() => removeTarget(index)}>
                  Remove
                </button>
              )}
            </div>
            <div className="mt-3 grid gap-3 md:grid-cols-3">
              <div>
                <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Label</label>
                <input
                  className={inputClasses}
                  value={target.label || ''}
                  onChange={(event) => handleTargetChange(index, 'label', event.target.value)}
                />
              </div>
              <div>
                <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Ticks</label>
                <input
                  className={inputClasses}
                  type="number"
                  value={target.ticks ?? 0}
                  onChange={(event) => handleTargetChange(index, 'ticks', event.target.value)}
                />
              </div>
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
            </div>
          </div>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <div className="rounded-xl border border-white/10 bg-white/5 p-3">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Breakeven</p>
          <div className="mt-3 space-y-2">
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Move stop after</label>
            <select
              className={inputClasses}
              value={breakeven.target_index ?? ''}
              onChange={(event) => {
                const next = { ...breakeven, target_index: event.target.value === '' ? null : Number(event.target.value) }
                update({ breakeven: next })
              }}
            >
              <option value="">Manual</option>
              {targetOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Fallback ticks</label>
            <input
              className={inputClasses}
              type="number"
              value={breakeven.ticks ?? ''}
              onChange={(event) => {
                const numeric = event.target.value === '' ? null : Number(event.target.value)
                update({ breakeven: { ...breakeven, ticks: numeric } })
              }}
            />
          </div>
        </div>

        <div className="rounded-xl border border-white/10 bg-white/5 p-3">
          <div className="flex items-center justify-between">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Trailing stop</p>
            <label className="flex items-center gap-2 text-xs text-slate-300">
              <input
                type="checkbox"
                checked={Boolean(trailing.enabled)}
                onChange={(event) => update({ trailing: { ...trailing, enabled: event.target.checked } })}
              />
              Enable
            </label>
          </div>
          <div className="mt-3 space-y-2">
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Activate after</label>
            <select
              className={inputClasses}
              value={trailing.target_index ?? ''}
              onChange={(event) => {
                const nextIndex = event.target.value === '' ? null : Number(event.target.value)
                update({ trailing: { ...trailing, target_index: nextIndex } })
              }}
              disabled={!trailing.enabled}
            >
              <option value="">Manual</option>
              {targetOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR multiplier</label>
            <input
              className={inputClasses}
              type="number"
              step="0.1"
              value={trailing.atr_multiplier ?? 1}
              onChange={(event) => update({ trailing: { ...trailing, atr_multiplier: Number(event.target.value) || 1 } })}
              disabled={!trailing.enabled}
            />
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</label>
            <input
              className={inputClasses}
              type="number"
              min={1}
              value={trailing.atr_period ?? 14}
              onChange={(event) => update({ trailing: { ...trailing, atr_period: Math.max(1, Number(event.target.value) || 14) } })}
              disabled={!trailing.enabled}
            />
          </div>
        </div>
      </div>
    </div>
  )
}
