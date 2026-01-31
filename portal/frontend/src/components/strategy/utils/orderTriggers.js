/**
 * Build display-ready trigger rows from a signal preview instrument payload.
 */
export function buildTriggerRows({ instrumentResult, rules = [], symbol = '' } = {}) {
  if (!instrumentResult) return []
  const ruleLookup = new Map((Array.isArray(rules) ? rules : []).map((rule) => [rule.id, rule]))

  const toRows = (entries = [], direction = 'BUY') => {
    return entries.flatMap((entry, index) => {
      if (!entry) return []
      const signals = Array.isArray(entry.signals) ? entry.signals : []
      if (signals.length) {
        return signals.map((signal, sigIndex) => ({
          id: `${direction}-${index}-${sigIndex}-${signal.time || signal.timestamp || ''}`,
          direction,
          ruleId: entry.rule_id || signal.rule_id,
          triggerType: entry.trigger_type || signal.trigger_type || entry.action || signal.action,
          timestamp: signal.time || signal.timestamp || signal.ts || entry.timestamp,
          reasons: signal.reasons || entry.reasons || entry.conditions_met,
        }))
      }
      return [{
        id: `${direction}-${index}`,
        direction,
        ruleId: entry.rule_id,
        triggerType: entry.trigger_type || entry.action,
        timestamp: entry.timestamp,
        reasons: entry.reasons || entry.conditions_met,
        matched: entry.matched,
      }]
    })
  }

  const buyRows = toRows(instrumentResult.buy_signals || [], 'BUY')
  const sellRows = toRows(instrumentResult.sell_signals || [], 'SELL')

  return [...buyRows, ...sellRows].map((row) => {
    const rule = row.ruleId ? ruleLookup.get(row.ruleId) : null
    return {
      ...row,
      ruleName: rule?.name || row.ruleId || 'Rule',
      instrument: symbol || instrumentResult?.window?.symbol || '—',
    }
  })
}
