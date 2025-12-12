import React, { Fragment } from 'react'

/**
 * Badge component displaying a single rule condition with indicator, signal type, and direction.
 */
const ConditionBadge = ({ label, signalType, direction, ruleId }) => {
  const normalizedDirection = typeof direction === 'string' ? direction.toLowerCase() : ''
  const ruleLabel = typeof ruleId === 'string' && ruleId.trim().length
    ? ruleId.replace(/_/g, ' ').toUpperCase()
    : ''

  const directionConfig = {
    label: 'Any bias',
    icon: '•',
    classes: 'border-white/12 bg-white/5 text-slate-200',
  }

  if (normalizedDirection === 'long') {
    directionConfig.label = 'Long bias'
    directionConfig.icon = '↗'
    directionConfig.classes = 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200'
  } else if (normalizedDirection === 'short') {
    directionConfig.label = 'Short bias'
    directionConfig.icon = '↘'
    directionConfig.classes = 'border-rose-500/40 bg-rose-500/15 text-rose-200'
  }

  return (
    <div className="flex min-w-[220px] items-stretch gap-3 rounded-2xl border border-white/12 bg-black/25 px-3 py-2">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="truncate text-xs font-semibold text-white">{label}</span>
          {ruleLabel ? (
            <span className="rounded-md border border-white/10 bg-white/5 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.25em] text-slate-400">
              {ruleLabel}
            </span>
          ) : null}
        </div>
        <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] text-slate-300">
          <span className="inline-flex items-center rounded-md border border-white/10 bg-white/5 px-2 py-0.5 uppercase tracking-[0.25em]">
            {signalType ? signalType.toUpperCase() : 'SIGNAL'}
          </span>
          <span className={`inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10px] font-semibold ${directionConfig.classes}`}>
            <span>{directionConfig.icon}</span>
            {directionConfig.label}
          </span>
        </div>
      </div>
    </div>
  )
}

/**
 * Component displaying a list of strategy rules with conditions.
 *
 * Note: This component expects ActionButton which is passed in from the parent.
 */
export const RuleList = ({ rules, onEdit, onDelete, indicatorLookup, ActionButton }) => {
  if (!rules.length) {
    return (
      <p className="rounded-xl border border-white/10 bg-white/5 p-4 text-sm text-slate-400">
        No rules yet. Create at least one BUY or SELL rule to generate signals.
      </p>
    )
  }

  return (
    <div className="space-y-3">
      {rules.map((rule) => (
        <div
          key={rule.id}
          className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200"
        >
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <p className="text-sm font-semibold text-white">{rule.name}</p>
              <p className="text-xs text-slate-400">
                {rule.action?.toUpperCase()} • {rule.match === 'any' ? 'Any condition triggers' : 'All conditions required'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <span
                className={`rounded-full px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${
                  rule.enabled
                    ? 'bg-emerald-500/20 text-emerald-200'
                    : 'bg-slate-700/60 text-slate-400'
                }`}
              >
                {rule.enabled ? 'Enabled' : 'Disabled'}
              </span>
              <ActionButton variant="ghost" onClick={() => onEdit(rule)}>
                Edit
              </ActionButton>
              <ActionButton variant="danger" onClick={() => onDelete(rule)}>
                Delete
              </ActionButton>
            </div>
          </div>
          {rule.description && (
            <p className="mt-3 text-xs text-slate-400">{rule.description}</p>
          )}

          <div className="mt-3 space-y-2 text-xs text-slate-300">
            {Array.isArray(rule.conditions) && rule.conditions.length ? (
              <div className="flex flex-wrap items-center gap-2">
                {rule.conditions.map((condition, index) => {
                  const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
                  const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id
                  const connectorLabel = rule.match === 'any' ? 'OR' : 'AND'
                  return (
                    <Fragment key={`${rule.id}-condition-${index}`}>
                      <ConditionBadge
                        label={label}
                        signalType={condition.signal_type}
                        direction={condition.direction}
                        ruleId={condition.rule_id || condition.signal_type}
                      />
                      {index < rule.conditions.length - 1 && (
                        <span className="rounded-md border border-white/10 bg-[#111622] px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.3em] text-slate-400">
                          {connectorLabel}
                        </span>
                      )}
                    </Fragment>
                  )
                })}
              </div>
            ) : (
              <p className="text-[11px] text-slate-400">No conditions configured.</p>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}
