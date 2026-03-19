const ruleRefs = (rule) => {
  const flow = rule?.flow && typeof rule.flow === 'object' ? rule.flow : null
  if (flow) {
    return [
      flow.trigger,
      ...(Array.isArray(flow.guards) ? flow.guards : []),
    ].filter((entry) => entry && typeof entry === 'object')
  }
  const when = rule?.when
  if (when && typeof when === 'object') {
    const clauses = when.type === 'all' && Array.isArray(when.conditions)
      ? when.conditions
      : [when]
    return clauses.filter((entry) => entry && typeof entry === 'object')
  }
  const conditions = Array.isArray(rule?.conditions) ? rule.conditions : []
  return conditions.filter((entry) => entry && typeof entry === 'object')
}

/**
 * Build a map of indicator_id -> number of rule conditions that reference it.
 */
export function countIndicatorRuleUsage(rules = []) {
  const usage = new Map()
  if (!Array.isArray(rules)) return usage
  rules.forEach((rule) => {
    ruleRefs(rule).forEach((cond) => {
      const id = cond?.indicator_id
      if (!id) return
      usage.set(id, (usage.get(id) || 0) + 1)
    })
  })
  return usage
}

/**
 * Determine whether detaching an indicator requires confirmation.
 */
export function requiresDetachConfirm(indicatorId, rules = []) {
  if (!indicatorId) return false
  const usage = countIndicatorRuleUsage(rules)
  return (usage.get(indicatorId) || 0) > 0
}

/**
 * Return IDs of indicators that are referenced by rules but not attached.
 */
export function findBrokenRuleIndicators(attachedIds = [], rules = []) {
  const attachedSet = new Set(attachedIds)
  const missing = new Set()
  if (!Array.isArray(rules)) return missing
  rules.forEach((rule) => {
    ruleRefs(rule).forEach((cond) => {
      const id = cond?.indicator_id
      if (!id) return
      if (!attachedSet.has(id)) {
        missing.add(id)
      }
    })
  })
  return missing
}
