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

function describeStopAdjustment(rule, targetLookup) {
  if (!rule) return ''
  const trigger =
    rule.trigger_type === 'target_hit'
      ? `After ${targetLookup[rule.trigger_value] || 'target'}`
      : `After ${formatNumber(rule.trigger_value)} R`
  const action =
    rule.action_type === 'move_to_r'
      ? `Move stop to ${formatNumber(rule.action_value ?? 0)} R`
      : 'Move stop to breakeven (0R)'
  return `${trigger} → ${action}`
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

export default function ATMTemplateSummary({ template, compact = false }) {
  const config = cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE)
  const targets = Array.isArray(config.take_profit_orders) ? config.take_profit_orders : []
  const stopAdjustments = Array.isArray(config.stop_adjustments) ? config.stop_adjustments : []
  const trailing = config.trailing || {}
  const meta = config._meta || {}
  const templateName = config.name?.trim() || 'Untitled template'

  const targetLabels = targets.reduce((acc, target, index) => {
    const label = target.label || `TP ${index + 1}`
    const key = target?.id || label
    acc[key] = label
    return acc
  }, {})

  if (compact) {
    return (
      <div className="space-y-3 rounded-xl border border-white/10 bg-[#101524] p-4 text-sm text-slate-200">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Template</p>
            <p className="text-lg font-semibold text-white">{templateName}</p>
          </div>
          <p className="text-sm text-slate-200">{formatNumber(config.stop_r_multiple)} R stop</p>
        </div>

        <div className="space-y-1">
          <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Take-profit targets</p>
          {targets.length ? (
            <ul className="space-y-1">
              {targets.map((target, index) => (
                <li key={target.id || index} className="flex items-center justify-between rounded-lg bg-white/5 px-3 py-2">
                  <span className="font-semibold text-white">{target.label || `TP ${index + 1}`}</span>
                  <span className="text-xs text-slate-300">
                    {formatNumber(target.r_multiple)} R • {formatNumber(target.size_percent ?? target.size_pct, 0)}%
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-slate-400">No take-profit targets defined yet.</p>
          )}
        </div>

        <div className="space-y-1">
          <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Stop adjustments</p>
          {stopAdjustments.length ? (
            <ul className="space-y-1 text-xs text-slate-200">
              {stopAdjustments.map((rule, index) => (
                <li key={rule.id || index} className="rounded-lg bg-white/5 px-3 py-2">
                  {describeStopAdjustment(rule, targetLabels)}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-slate-400">None</p>
          )}
        </div>

        <div className="space-y-1">
          <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Trailing stop</p>
          <p className="text-sm text-slate-300">
            {trailing?.enabled
              ? trailing.activation_type === 'target_hit'
                ? `Activates after ${targetLabels[trailing.target_id] || 'target'}; trails ${formatNumber(
                    trailing.atr_multiplier ?? 1,
                  )}R`
                : `Activates after ${formatNumber(trailing.r_multiple ?? 1)} R; trails ${formatNumber(
                    trailing.atr_multiplier ?? 1,
                  )}R`
              : 'Trailing stop disabled.'}
          </p>
        </div>
      </div>
    )
  }

  const resolvedTickSize = config.tick_size ?? meta.tick_size ?? null
  const latestAtrValue = meta.latest_atr ?? meta.atr_preview ?? meta.atr ?? null
  const rMode = config.rMode || 'atr'
  const riskTicks = config.rRiskTicks
  const rAtrMultiplier = config.rAtrMultiplier ?? 1

  const oneRPrice =
    rMode === 'ticks'
      ? riskTicks && resolvedTickSize
        ? riskTicks * resolvedTickSize
        : null
      : latestAtrValue
        ? rAtrMultiplier * Number(latestAtrValue)
        : null
  const oneRTicks = resolvedTickSize && oneRPrice ? oneRPrice / resolvedTickSize : riskTicks ?? null

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
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Risk unit</p>
          <p className="text-sm text-white">
            {rMode === 'ticks'
              ? `${formatNumber(riskTicks)} ticks${oneRPrice ? ` (${formatNumber(oneRPrice)} pts)` : ''}`
              : oneRPrice
                ? `${formatNumber(oneRPrice)} per R`
                : `${formatNumber(rAtrMultiplier)} x ATR`}
          </p>
          <p className="text-[11px] text-slate-500">1R definition for R-based stops and targets.</p>
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
          <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Stop adjustments</p>
          {stopAdjustments.length ? (
            <ul className="mt-1 space-y-1 text-sm text-white">
              {stopAdjustments.map((rule, index) => (
                <li key={rule.id || index}>
                  <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Rule {index + 1}</p>
                  <p>{describeStopAdjustment(rule, targetLabels)}</p>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-slate-400">None</p>
          )}
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
