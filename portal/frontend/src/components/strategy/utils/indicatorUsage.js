/**
 * Build a map of indicator_id -> number of rule conditions that reference it.
 * Expects rules with shape { conditions: [{ indicator_id }] }.
 */
export function countIndicatorRuleUsage(rules = []) {
  const usage = new Map()
  if (!Array.isArray(rules)) return usage
  rules.forEach((rule) => {
    if (!Array.isArray(rule?.conditions)) return
    rule.conditions.forEach((cond) => {
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
    if (!Array.isArray(rule?.conditions)) return
    rule.conditions.forEach((cond) => {
      const id = cond?.indicator_id
      if (!id) return
      if (!attachedSet.has(id)) {
        missing.add(id)
      }
    })
  })
  return missing
}
