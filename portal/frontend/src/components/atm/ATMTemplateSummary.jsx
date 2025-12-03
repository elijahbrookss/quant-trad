import { cloneATMTemplate, DEFAULT_ATM_TEMPLATE } from './ATMConfigForm.jsx'

function formatNumber(value) {
  if (value === null || value === undefined || value === '') {
    return '—'
  }
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return value
  }
  if (Math.abs(numeric) >= 1) {
    return numeric.toLocaleString(undefined, { maximumFractionDigits: 4 })
  }
  return numeric.toPrecision(4)
}

function renderTargets(targets) {
  if (!targets.length) {
    return (
      <p className="text-sm text-slate-400">No take-profit targets defined yet.</p>
    )
  }
  return (
    <ul className="space-y-2">
      {targets.map((target, index) => (
        <li
          key={target.id || index}
          className="flex items-center justify-between rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200"
        >
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">
              Target {index + 1}
            </p>
            <p className="text-base text-white">{target.label || `TP +${target.ticks}`}</p>
          </div>
          <div className="text-right text-xs text-slate-400">
            {target.r_multiple !== null && target.r_multiple !== undefined ? (
              <p>{formatNumber(target.r_multiple)} R</p>
            ) : target.price !== null && target.price !== undefined ? (
              <p>@ {formatNumber(target.price)}</p>
            ) : (
              <p>{formatNumber(target.ticks)} ticks</p>
            )}
            <p>{formatNumber(target.contracts)} contracts</p>
          </div>
        </li>
      ))}
    </ul>
  )
}

export default function ATMTemplateSummary({ template }) {
  const config = cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE)
  const targets = Array.isArray(config.take_profit_orders) ? config.take_profit_orders : []
  const breakeven = config.breakeven || {}
  const trailing = config.trailing || {}
  const meta = config._meta || {}

  const describeField = (value, flag) => {
    if (flag) {
      return formatNumber(value)
    }
    return 'Auto'
  }

  return (
    <div className="space-y-4 rounded-2xl border border-white/10 bg-[#101524] p-4 text-sm text-slate-200">
      <div className="grid gap-4 md:grid-cols-3">
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Contracts</p>
          <p className="text-lg font-semibold text-white">{formatNumber(config.contracts)}</p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Initial stop</p>
          <p className="text-lg font-semibold text-white">
            {config.stop_r_multiple !== null && config.stop_r_multiple !== undefined
              ? `${formatNumber(config.stop_r_multiple)} R`
              : config.stop_price !== null && config.stop_price !== undefined
                ? `@ ${formatNumber(config.stop_price)}`
                : `${formatNumber(config.stop_ticks)} ticks`}
          </p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Breakeven</p>
          <p className="text-sm text-white">
            {breakeven?.enabled === false
              ? 'Disabled'
              : breakeven?.target_index !== undefined && breakeven?.target_index !== null
                ? `After target ${Number(breakeven.target_index) + 1}`
                : breakeven?.r_multiple !== null && breakeven?.r_multiple !== undefined
                  ? `${formatNumber(breakeven.r_multiple)} R`
                  : breakeven?.ticks
                    ? `${formatNumber(breakeven.ticks)} ticks`
                    : 'Manual'}
          </p>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Tick size</p>
          <p className="text-base text-white">{describeField(config.tick_size, meta.tick_size_override)}</p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Tick value</p>
          <p className="text-base text-white">{describeField(config.tick_value, meta.tick_value_override)}</p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Contract size</p>
          <p className="text-base text-white">{describeField(config.contract_size, meta.contract_size_override)}</p>
        </div>
      </div>

      <div className="rounded-xl border border-white/10 bg-black/30 p-3">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Take-profit targets</p>
        <div className="mt-3">{renderTargets(targets)}</div>
      </div>

      <div className="rounded-xl border border-white/10 bg-black/30 p-3">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Trailing stop</p>
        {trailing?.enabled ? (
          <dl className="mt-3 grid gap-3 text-xs text-slate-300 md:grid-cols-2">
            <div>
              <dt className="uppercase tracking-[0.3em] text-slate-500">Activate after</dt>
              <dd className="text-base text-white">
                {trailing.target_index !== undefined && trailing.target_index !== null
                  ? `Target ${Number(trailing.target_index) + 1}`
                  : trailing.r_multiple !== null && trailing.r_multiple !== undefined
                    ? `${formatNumber(trailing.r_multiple)} R`
                    : trailing.ticks
                      ? `${formatNumber(trailing.ticks)} ticks`
                      : 'Manual'}
              </dd>
            </div>
            <div>
              <dt className="uppercase tracking-[0.3em] text-slate-500">ATR multiplier</dt>
              <dd className="text-base text-white">{formatNumber(trailing.atr_multiplier ?? 1)}</dd>
            </div>
            <div>
              <dt className="uppercase tracking-[0.3em] text-slate-500">ATR period</dt>
              <dd className="text-base text-white">{formatNumber(trailing.atr_period ?? 14)}</dd>
            </div>
          </dl>
        ) : (
          <p className="mt-3 text-sm text-slate-400">Trailing stop disabled.</p>
        )}
      </div>
    </div>
  )
}
