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
  const atmTargets = Array.isArray(template?.take_profit_orders) ? template.take_profit_orders : targets

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
  const currentTemplateName = templateName || ''

  if (compact) {
    return (
      <div className="space-y-3 rounded-sm border border-white/10 bg-[#0a0d13] p-4 text-sm text-slate-200">
        <div>
          <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Template</p>
          <p className="text-base font-semibold text-white">{templateName}</p>
        </div>
        <div className="space-y-1 text-sm text-slate-200">
          {hasRiskDefinition ? (
            <p className="qt-mono text-white">1R = ATR({atrPeriod}) × {formatNumber(atrMultiplier)}</p>
          ) : (
            <p className="text-amber-400">R not configured</p>
          )}
          <p><span className="text-slate-500">Stop:</span> <span className="text-white">{stopLabel}</span></p>
          <p><span className="text-slate-500">Targets:</span> <span className="text-white">{targetsStory}</span></p>
          <p><span className="text-slate-500">Adjustments:</span> <span className="text-white">{adjustmentsStory}</span></p>
          <p><span className="text-slate-500">Trailing:</span> <span className="text-white">{trailingStory}</span></p>
        </div>
      </div>
    )
  }

  return (
    <div className="overflow-hidden rounded-sm border border-white/10 bg-[#0a0d13]">
      <div className="flex items-center justify-between border-b border-white/[0.06] px-5 py-3">
        <div className="flex items-center gap-2">
          <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">ATM Template</span>
          {currentTemplateName && (
            <span className="text-xs text-slate-300">{currentTemplateName}</span>
          )}
        </div>
        {templateOptions.length > 0 && onTemplateChange ? (
          <select
            value={currentTemplateId || ''}
            onChange={onTemplateChange}
            className="rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200 focus:border-white/20 focus:outline-none"
          >
            {templateOptions.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        ) : null}
      </div>

      <div className="space-y-6 px-6 py-5">
        <section className="rounded-sm border border-white/8 bg-[#0a0d13] p-5">
        <div className="flex items-center justify-between">
          <div>
            <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Risk Unit (R)</span>
            <p className="mt-0.5 text-xs text-slate-500">ATR-based position risk</p>
          </div>
          <div className="text-right">
            {hasRiskDefinition ? (
              <p className="qt-mono text-2xl font-semibold text-white">
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
            <summary className="cursor-pointer text-[11px] text-slate-500 transition-colors hover:text-slate-400">
              What is R?
            </summary>
            <p className="mt-2 text-[11px] text-slate-500 leading-relaxed">
              R is your risk unit based on Average True Range. All targets and stops are R-multiples,
              so a 2R target means 2× your ATR-defined risk distance.
            </p>
          </details>
        )}

        <div className="my-4 px-1">
          <div className="relative flex h-8 items-center">
            <div className="absolute inset-x-0 h-px bg-white/10" />
            <div className="absolute left-0 flex flex-col items-center gap-1">
              <div className="h-3 w-px bg-rose-500" />
              <span className="qt-mono text-[9px] text-rose-400">SL</span>
            </div>
            <div className="absolute flex flex-col items-center gap-1" style={{ left: '30%' }}>
              <div className="h-4 w-0.5 bg-white/50" />
              <span className="qt-mono text-[9px] text-slate-400">Entry</span>
            </div>
            {atmTargets.map((tp, i) => {
              const r = tp.r_multiple || (i + 1)
              const pct = Math.min(30 + r * 20, 97)
              return (
                <div
                  key={tp.id || i}
                  className="absolute flex flex-col items-center gap-1"
                  style={{ left: `${pct}%` }}
                >
                  <div className="h-3 w-px bg-emerald-500" />
                  <span className="qt-mono whitespace-nowrap text-[9px] text-emerald-400">
                    TP{i + 1} {r}R
                  </span>
                </div>
              )
            })}
          </div>
        </div>
        </section>

        <section className="rounded-sm border border-white/8 bg-[#0a0d13] p-5">
        <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Initial Stop</span>
        <p className="mt-0.5 text-xs text-slate-500">Risk-based stop loss for all entries</p>
        <div className="mt-4">
          {config.stop_r_multiple !== null && config.stop_r_multiple !== undefined ? (
            <div className="flex items-baseline gap-2">
              <span className="qt-mono text-3xl font-semibold text-white">{formatNumber(config.stop_r_multiple)}R</span>
              <span className="text-sm text-slate-400">stop loss</span>
            </div>
          ) : (
            <p className="text-sm text-slate-400">No stop configured</p>
          )}
        </div>
        </section>

        <section className="rounded-sm border border-white/10 bg-[#0a0d13] p-5">
        <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Take Profit Targets</span>
        <p className="mt-0.5 text-xs text-slate-500">Scale out at multiple price levels</p>
        {targets.length > 0 ? (
          <div className="mt-4 space-y-2">
            {targets.map((target, index) => {
              const sizePercent = (target.size_fraction ?? 0) * 100
              return (
                <div
                  key={target.id || index}
                  className="flex items-center justify-between rounded-sm border border-white/5 bg-white/[0.02] px-4 py-3"
                >
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
                      {target.label || `TP ${index + 1}`}
                    </span>
                    <span className="qt-mono text-xl font-semibold text-white">{formatNumber(target.r_multiple)}R</span>
                  </div>
                  <div className="text-right">
                    <span className="qt-mono text-base font-medium text-white">{sizePercent.toFixed(1)}%</span>
                    <span className="ml-1.5 text-xs text-slate-500">of position</span>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          <div className="mt-4 rounded-sm border border-dashed border-white/10 bg-[#0a0d13] px-4 py-6 text-center">
            <p className="text-sm text-slate-400">No take-profit targets defined</p>
          </div>
        )}
        </section>

        <section className="rounded-sm border border-white/8 bg-[#0a0d13] p-5">
        <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Stop Adjustments</span>
        <p className="mt-0.5 text-xs text-slate-500">Move stop based on price action</p>
        {stopAdjustments.length > 0 ? (
          <div className="mt-4 space-y-2">
            {stopAdjustments.map((rule, idx) => (
              <div
                key={idx}
                className="flex items-center gap-2 rounded-sm border border-white/5 bg-white/[0.02] px-4 py-3 text-sm text-slate-200"
              >
                <span className="text-slate-500">•</span>
                <span>{describeStopAdjustment(rule, targetLabels)}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="mt-4 rounded-sm border border-dashed border-white/10 bg-[#0a0d13] px-4 py-6 text-center">
            <p className="text-sm text-slate-400">No stop adjustments configured</p>
          </div>
        )}
        </section>

        <section className="rounded-sm border border-white/8 bg-[#0a0d13] p-5">
        <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Trailing Stop</span>
        <p className="mt-0.5 text-xs text-slate-500">Lock in profits as price moves</p>
        <div className="mt-4">
          {trailing?.enabled ? (
            <div className="rounded-sm border border-white/5 bg-white/[0.02] px-4 py-3 text-sm text-slate-200">
              <span className="text-white">{trailingStory}</span>
            </div>
          ) : (
            <div className="rounded-sm border border-dashed border-white/10 bg-[#0a0d13] px-4 py-6 text-center">
              <span className="text-xs text-slate-600">— disabled</span>
            </div>
          )}
        </div>
        </section>
      </div>
    </div>
  )
}
