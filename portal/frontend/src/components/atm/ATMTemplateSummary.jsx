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
  if (!rule?.trigger || !rule?.action) return ''

  // v2 schema: nested trigger/action format
  const triggerType = rule.trigger.type === 'target_hit' ? 'target_hit' : 'r_multiple'
  const triggerValue = rule.trigger.value
  const actionType = rule.action.type
  const actionValue = rule.action.value

  const trigger =
    triggerType === 'target_hit'
      ? `After ${targetLookup[triggerValue] || 'target'}`
      : `After ${formatNumber(triggerValue)} R`

  let action
  if (actionType === 'move_to_r') {
    action = `Move stop to ${formatNumber(actionValue ?? 0)} R`
  } else if (actionType === 'trail_atr') {
    const atrMultiplier = rule.action.atr_multiplier ?? 1.0
    action = `Trail stop (ATR × ${formatNumber(atrMultiplier)})`
  } else {
    action = 'Move stop to breakeven (0R)'
  }

  return `${trigger} → ${action}`
}

export default function ATMTemplateSummary({
  template,
  templateOptions = [],
  currentTemplateId,
  onTemplateChange,
  compact = false,
}) {
  const config = cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE)
  const targets = Array.isArray(config.take_profit_orders) ? config.take_profit_orders : []
  const stopAdjustments = Array.isArray(config.stop_adjustments) ? config.stop_adjustments : []
  const trailing = config.trailing || {}
  const templateName = config.name?.trim() || 'Untitled template'

  const targetLabels = targets.reduce((acc, target, index) => {
    const label = target.label || `TP ${index + 1}`
    const key = target?.id || label
    acc[key] = label
    return acc
  }, {})

  const stopLabel =
    config.stop_r_multiple !== null && config.stop_r_multiple !== undefined
      ? `${formatNumber(config.stop_r_multiple)}R stop`
      : 'Stop not set'
  const targetsStory = targets.length
    ? targets
        .map((target, index) => {
          // Backend stores size_fraction as 0-1 range, convert to percentage for display
          const sizePercent = (target.size_fraction ?? 0) * 100
          return `${target.label || `TP ${index + 1}`} ${formatNumber(target.r_multiple)}R (${formatNumber(sizePercent)}%)`
        })
        .join(', ')
    : 'No take-profit targets defined.'
  const adjustmentsStory = stopAdjustments.length
    ? stopAdjustments.map((rule) => describeStopAdjustment(rule, targetLabels)).join(' • ')
    : 'None'
  const trailingStory = trailing?.enabled
    ? trailing.activation_type === 'target_hit'
      ? `After ${targetLabels[trailing.target_id] || 'target'} → trail by ${formatNumber(
          trailing.atr_multiplier ?? trailing.distance_r ?? trailing.distance ?? 1,
        )}R`
      : `After ${formatNumber(trailing.r_multiple ?? 1)}R → trail by ${formatNumber(
          trailing.atr_multiplier ?? trailing.distance_r ?? trailing.distance ?? 1,
        )}R`
    : 'Trailing stop disabled.'

  const atrPeriod = config.initial_stop?.atr_period
  const atrMultiplier = config.initial_stop?.atr_multiplier
  const hasRiskDefinition = atrPeriod != null && atrMultiplier != null

  if (compact) {
    return (
      <div className="space-y-3 rounded-xl border border-white/10 bg-black/20 p-4 text-sm text-slate-200">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-slate-500">Template</p>
          <p className="text-lg font-semibold text-white">{templateName}</p>
        </div>
        <div className="space-y-1 text-sm text-slate-200">
          {hasRiskDefinition ? (
            <p>1R = ATR({atrPeriod}) × {formatNumber(atrMultiplier)}</p>
          ) : (
            <p className="text-amber-400">R not configured</p>
          )}
          <p>Stop: {stopLabel}</p>
          <p>Targets: {targetsStory}</p>
          <p>Adjustments: {adjustmentsStory}</p>
          <p>Trailing: {trailingStory}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 px-6 pb-6">
      {/* Template Selector */}
      {templateOptions.length > 0 && onTemplateChange && (
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-semibold text-white">ATM Template</h3>
            <p className="mt-0.5 text-xs text-slate-500">Risk management and order settings</p>
          </div>
          <select
            value={currentTemplateId || ''}
            onChange={onTemplateChange}
            className="rounded-lg border border-white/10 bg-black/30 px-4 py-2 text-sm text-white transition hover:border-white/20 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
          >
            {templateOptions.map((opt) => (
              <option key={opt.value} value={opt.value} className="bg-slate-900">
                {opt.label}
              </option>
            ))}
          </select>
        </div>
      )}

      {/* Risk Unit Definition */}
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-semibold text-white">Risk Unit (R)</h3>
            <p className="mt-0.5 text-xs text-slate-500">ATR-based position risk</p>
          </div>
          <div className="text-right">
            {hasRiskDefinition ? (
              <p className="text-2xl font-semibold text-white">
                ATR({atrPeriod}) × {formatNumber(atrMultiplier)}
              </p>
            ) : (
              <p className="text-sm text-amber-400">Not configured</p>
            )}
          </div>
        </div>
        {!hasRiskDefinition && (
          <div className="mt-3 rounded-lg border border-amber-500/20 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
            ATR period and multiplier must be set to define R. Edit the strategy to configure risk settings.
          </div>
        )}
        {hasRiskDefinition && (
          <details className="mt-3 group">
            <summary className="cursor-pointer text-[11px] text-slate-500 hover:text-slate-400 transition-colors">
              What is R?
            </summary>
            <p className="mt-2 text-[11px] text-slate-500 leading-relaxed">
              R is your risk unit based on Average True Range. All targets and stops are R-multiples,
              so a 2R target means 2× your ATR-defined risk distance.
            </p>
          </details>
        )}
      </section>

      {/* Initial Stop */}
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <h3 className="text-sm font-semibold text-white">Initial Stop</h3>
        <p className="mt-0.5 text-xs text-slate-500">Risk-based stop loss for all entries</p>
        <div className="mt-4">
          {config.stop_r_multiple !== null && config.stop_r_multiple !== undefined ? (
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-semibold text-white">{formatNumber(config.stop_r_multiple)}R</span>
              <span className="text-sm text-slate-400">stop loss</span>
            </div>
          ) : (
            <p className="text-sm text-slate-400">No stop configured</p>
          )}
        </div>
      </section>

      {/* Take Profit Targets */}
      <section className="rounded-xl border border-white/10 bg-black/20 p-5">
        <h3 className="text-sm font-semibold text-white">Take Profit Targets</h3>
        <p className="mt-0.5 text-xs text-slate-500">Scale out at multiple price levels</p>
        {targets.length > 0 ? (
          <div className="mt-4 space-y-2">
            {targets.map((target, index) => {
              const sizePercent = (target.size_fraction ?? 0) * 100
              return (
                <div
                  key={target.id || index}
                  className="flex items-center justify-between rounded-lg border border-white/5 bg-white/[0.02] px-4 py-3"
                >
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
                      {target.label || `TP ${index + 1}`}
                    </span>
                    <span className="text-xl font-semibold text-white">{formatNumber(target.r_multiple)}R</span>
                  </div>
                  <div className="text-right">
                    <span className="text-base font-medium text-white">{sizePercent.toFixed(1)}%</span>
                    <span className="ml-1.5 text-xs text-slate-500">of position</span>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          <div className="mt-4 rounded-lg border border-dashed border-white/10 bg-black/20 px-4 py-6 text-center">
            <p className="text-sm text-slate-400">No take-profit targets defined</p>
          </div>
        )}
      </section>

      {/* Stop Adjustments */}
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <h3 className="text-sm font-semibold text-white">Stop Adjustments</h3>
        <p className="mt-0.5 text-xs text-slate-500">Move stop based on price action</p>
        {stopAdjustments.length > 0 ? (
          <div className="mt-4 space-y-2">
            {stopAdjustments.map((rule, idx) => (
              <div
                key={idx}
                className="flex items-center gap-2 rounded-lg border border-white/5 bg-white/[0.02] px-4 py-3 text-sm text-slate-200"
              >
                <span className="text-slate-500">•</span>
                <span>{describeStopAdjustment(rule, targetLabels)}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="mt-4 rounded-lg border border-dashed border-white/10 bg-black/20 px-4 py-6 text-center">
            <p className="text-sm text-slate-400">No stop adjustments configured</p>
          </div>
        )}
      </section>

      {/* Trailing Stop */}
      <section className="rounded-xl border border-white/8 bg-black/20 p-5">
        <h3 className="text-sm font-semibold text-white">Trailing Stop</h3>
        <p className="mt-0.5 text-xs text-slate-500">Lock in profits as price moves</p>
        <div className="mt-4">
          {trailing?.enabled ? (
            <div className="rounded-lg border border-white/5 bg-white/[0.02] px-4 py-3 text-sm text-slate-200">
              {trailingStory}
            </div>
          ) : (
            <div className="rounded-lg border border-dashed border-white/10 bg-black/20 px-4 py-6 text-center">
              <p className="text-sm text-slate-400">Trailing stop disabled</p>
            </div>
          )}
        </div>
      </section>
    </div>
  )
}
