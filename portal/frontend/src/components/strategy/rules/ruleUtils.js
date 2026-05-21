export const extractRuleFlow = (rule) => {
  if (rule?.trigger) {
    return {
      trigger: rule.trigger || {},
      guards: Array.isArray(rule.guards) ? rule.guards : [],
    }
  }
  if (rule?.flow?.trigger) {
    return {
      trigger: rule.flow.trigger || {},
      guards: Array.isArray(rule.flow.guards) ? rule.flow.guards : [],
    }
  }
  const when = rule?.when
  if (!when || typeof when !== 'object') {
    return { trigger: {}, guards: [] }
  }
  const clauses = when.type === 'all' && Array.isArray(when.conditions)
    ? when.conditions
    : [when]
  const trigger = clauses.find((clause) => clause?.type === 'signal_match') || {}
  const guards = clauses.filter((clause) => clause?.type === 'context_match' || clause?.type === 'metric_match')
  return { trigger, guards }
}

const outputLabel = (indicatorMeta, outputName) => {
  const typedOutputs = Array.isArray(indicatorMeta?.typed_outputs) ? indicatorMeta.typed_outputs : []
  const output = typedOutputs.find((entry) => entry?.name === outputName)
  return output?.label || outputName || 'output'
}

export const buildRuleConditionSummary = ({ rule, indicatorLookup, limit = 2 }) => {
  const { trigger, guards } = extractRuleFlow(rule)
  if (!trigger?.indicator_id || !trigger?.output_name) return 'No trigger'
  const triggerMeta = indicatorLookup?.get?.(trigger.indicator_id) || indicatorLookup?.[trigger.indicator_id]
  const triggerLabel = triggerMeta?.name || triggerMeta?.type || trigger.indicator_id || 'Indicator'
  const triggerOutput = outputLabel(triggerMeta, trigger.output_name)
  const parts = [`${triggerLabel} → ${triggerOutput} → ${trigger.event_key || 'event'}`]
  guards.slice(0, limit).forEach((guard) => {
    const guardMeta = indicatorLookup?.get?.(guard.indicator_id) || indicatorLookup?.[guard.indicator_id]
    const guardLabel = guardMeta?.name || guardMeta?.type || guard.indicator_id || 'Indicator'
    const guardOutput = outputLabel(guardMeta, guard.output_name)
    if (guard.type === 'context_match') {
      parts.push(`${guardLabel} ${guardOutput}.${guard.field || 'state'} = ${guard.value}`)
      return
    }
    if (guard.type === 'holds_for_bars') {
      parts.push(`Hold ${guard.bars} bars`)
      return
    }
    if (guard.type === 'signal_seen_within_bars' || guard.type === 'signal_absent_within_bars') {
      parts.push(`${guard.event_key} ${guard.type === 'signal_seen_within_bars' ? 'seen' : 'absent'} in ${guard.lookback_bars}`)
      return
    }
    parts.push(`${guardLabel} ${guardOutput}.${guard.field} ${guard.operator} ${guard.value}`)
  })
  const tail = guards.length > limit ? ` +${guards.length - limit} more guard${guards.length - limit === 1 ? '' : 's'}` : ''
  return `${parts.join(' • ')}${tail}`
}

export const buildRuleDefaultName = ({ intent, trigger, guards, indicatorLookup }) => {
  const summary = buildRuleConditionSummary({
    rule: {
      trigger,
      guards,
    },
    indicatorLookup,
    limit: 2,
  })
  const actionLabel = intent === 'enter_short' ? 'SHORT' : 'LONG'
  return `${actionLabel} • ${summary}`
}
