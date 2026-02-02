export const buildRuleConditionSummary = ({ conditions, match, indicatorLookup, limit = 2 }) => {
  const list = Array.isArray(conditions) ? conditions : []
  if (!list.length) return 'No conditions'
  const connector = match === 'any' ? ' OR ' : ' AND '
  const parts = list.slice(0, limit).map((condition) => {
    const indicatorMeta = indicatorLookup?.get?.(condition.indicator_id) || indicatorLookup?.[condition.indicator_id]
    const label = indicatorMeta?.name || indicatorMeta?.type || condition.indicator_id || 'Indicator'
    const signal = condition.signal_type || 'signal'
    const bias = condition.direction ? ` (${String(condition.direction).toUpperCase()})` : ''
    return `${label}: ${signal}${bias}`
  })
  const tail = list.length > limit ? ` +${list.length - limit} more` : ''
  return `${parts.join(connector)}${tail}`
}

export const buildRuleDefaultName = ({ action, conditions, match, indicatorLookup }) => {
  const summary = buildRuleConditionSummary({ conditions, match, indicatorLookup })
  const actionLabel = action ? action.toUpperCase() : 'Rule'
  return `${actionLabel} • ${summary}`
}
