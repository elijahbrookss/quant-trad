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

export default function ATMTemplateSummary({ template, compact = false }) {
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

  const contractsLabel =
    config.contracts !== null && config.contracts !== undefined
      ? `${formatNumber(config.contracts)} contracts`
      : 'Contracts not set'
  const stopLabel =
    config.stop_r_multiple !== null && config.stop_r_multiple !== undefined
      ? `${formatNumber(config.stop_r_multiple)}R stop`
      : 'Stop not set'
  const targetsStory = targets.length
    ? targets
        .map((target, index) => {
          // v2 schema: size_fraction (0-1 range)
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

  if (compact) {
    return (
      <div className="space-y-3 rounded-xl border border-white/10 bg-[#101524] p-4 text-sm text-slate-200">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Template</p>
            <p className="text-lg font-semibold text-white">{templateName}</p>
          </div>
          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-100">{contractsLabel}</span>
        </div>
        <div className="space-y-1 text-sm text-slate-200">
          <p>Stop: {stopLabel}</p>
          <p>Targets: {targetsStory}</p>
          <p>Adjustments: {adjustmentsStory}</p>
          <p>Trailing: {trailingStory}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Template Header */}
      <div className="rounded-xl border border-white/10 bg-[#101524] p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Template Name</p>
            <p className="text-lg font-semibold text-white">{templateName}</p>
          </div>
          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-100">{contractsLabel}</span>
        </div>
      </div>

      {/* Initial Stop */}
      <div className="rounded-xl border border-white/10 bg-[#101524] p-4">
        <h4 className="mb-3 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Initial Stop</h4>
        <div className="text-sm text-white">
          {config.stop_r_multiple !== null && config.stop_r_multiple !== undefined ? (
            <div className="flex items-center gap-2">
              <span className="text-2xl font-semibold text-white">{formatNumber(config.stop_r_multiple)}R</span>
              <span className="text-slate-400">stop loss</span>
            </div>
          ) : (
            <p className="text-slate-400">No stop configured</p>
          )}
        </div>
      </div>

      {/* Take Profit Targets */}
      <div className="rounded-xl border border-white/10 bg-[#101524] p-4">
        <h4 className="mb-3 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Take Profit Targets</h4>
        {targets.length > 0 ? (
          <div className="space-y-3">
            {targets.map((target, index) => {
              const sizePercent = (target.size_fraction ?? 0) * 100
              return (
                <div key={target.id || index} className="flex items-center justify-between rounded-lg border border-white/5 bg-white/5 p-3">
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
                      {target.label || `TP ${index + 1}`}
                    </span>
                    <span className="text-lg font-semibold text-white">{formatNumber(target.r_multiple)}R</span>
                  </div>
                  <div className="text-right">
                    <span className="text-sm font-medium text-slate-300">{sizePercent.toFixed(1)}%</span>
                    <span className="ml-1 text-xs text-slate-500">of position</span>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          <p className="text-sm text-slate-400">No take-profit targets defined</p>
        )}
      </div>

      {/* Stop Adjustments */}
      {stopAdjustments.length > 0 && (
        <div className="rounded-xl border border-white/10 bg-[#101524] p-4">
          <h4 className="mb-3 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Stop Adjustments</h4>
          <div className="space-y-2 text-sm text-slate-200">
            {stopAdjustments.map((rule, idx) => (
              <div key={idx} className="flex items-start gap-2">
                <span className="text-slate-500">•</span>
                <span>{describeStopAdjustment(rule, targetLabels)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Trailing Stop */}
      {trailing?.enabled && (
        <div className="rounded-xl border border-white/10 bg-[#101524] p-4">
          <h4 className="mb-3 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Trailing Stop</h4>
          <p className="text-sm text-slate-200">{trailingStory}</p>
        </div>
      )}
    </div>
  )
}
