export function buildTriggerRows({ instrumentResult, rules = [], symbol = '' } = {}) {
  if (!instrumentResult) return []

  const ruleLookup = new Map((Array.isArray(rules) ? rules : []).map((rule) => [rule.id, rule]))
  const rows = Array.isArray(instrumentResult.trigger_rows) ? instrumentResult.trigger_rows : []

  return rows
    .map((row, index) => {
      const rule = row?.strategy_rule_id ? ruleLookup.get(row.strategy_rule_id) : null
      const timestamp = row?.timestamp || null
      const epoch = Number.isFinite(Number(row?.epoch)) ? Number(row.epoch) : 0
      const side = String(row?.side || '').toUpperCase()
      const outputName = String(row?.trigger_output_name || '').trim()
      const indicatorId = String(row?.trigger_indicator_id || '').trim()
      const guards = Array.isArray(row?.guards) ? row.guards : []
      return {
        id: row?.row_id || `${row?.strategy_rule_id || 'rule'}|${epoch || 0}|${index}`,
        rowKey: row?.row_id || `${row?.strategy_rule_id || 'rule'}|${epoch || 0}|${index}`,
        direction: side || (String(row?.action || '').toLowerCase() === 'buy' ? 'BUY' : 'SELL'),
        ruleId: row?.strategy_rule_id || null,
        ruleName: row?.rule_name || rule?.name || row?.strategy_rule_id || 'Rule',
        triggerType: row?.event_key || 'event',
        triggerLabel: outputName || 'signal',
        timestamp,
        epoch,
        matched: true,
        indicatorId,
        outputName,
        guards,
        guardCount: guards.length,
        instrument: symbol || instrumentResult?.window?.symbol || '—',
        instrumentId: instrumentResult?.window?.instrument_id || null,
        ruleRef: String(row?.strategy_rule_id || rule?.id || '').trim(),
        indicatorRef: indicatorId,
        outputRef: outputName,
      }
    })
    .sort((a, b) => (b.epoch || 0) - (a.epoch || 0))
}
