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
          const size = target.size_percent ?? target.size_pct ?? target.size
          return `${target.label || `TP ${index + 1}`} ${formatNumber(target.r_multiple)}R (${formatNumber(size, 0)}%)`
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
    <div className="space-y-3 rounded-2xl border border-white/10 bg-[#101524] p-4 text-sm text-slate-200">
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
