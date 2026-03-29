export function buildTriggerRows({ instrumentResult, rules = [], symbol = '' } = {}) {
  if (!instrumentResult) return []

  const ruleLookup = new Map((Array.isArray(rules) ? rules : []).map((rule) => [rule.id, rule]))
  const artifacts = Array.isArray(instrumentResult.decision_artifacts) ? instrumentResult.decision_artifacts : []

  return artifacts
    .filter((artifact) => String(artifact?.evaluation_result || '') === 'matched_selected')
    .map((artifact, index) => {
      const rule = artifact?.rule_id ? ruleLookup.get(artifact.rule_id) : null
      const timestamp = artifact?.bar_time || null
      const epoch = Number.isFinite(Number(artifact?.bar_epoch)) ? Number(artifact.bar_epoch) : 0
      const outputRef = artifact?.trigger?.output_ref
      const [indicatorId = '', outputName = ''] = String(outputRef || '').split('.', 2)
      const guards = Array.isArray(artifact?.guard_results) ? artifact.guard_results : []
      const direction = String(artifact?.emitted_intent || '') === 'enter_short' ? 'SELL' : 'BUY'
      return {
        id: artifact?.decision_id || `${artifact?.rule_id || 'rule'}|${epoch || 0}|${index}`,
        rowKey: artifact?.decision_id || `${artifact?.rule_id || 'rule'}|${epoch || 0}|${index}`,
        decisionId: artifact?.decision_id || null,
        direction,
        ruleId: artifact?.rule_id || null,
        ruleName: artifact?.rule_name || rule?.name || artifact?.rule_id || 'Rule',
        triggerType: artifact?.trigger?.event_key || 'event',
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
        ruleRef: String(artifact?.rule_id || rule?.id || '').trim(),
        indicatorRef: indicatorId,
        outputRef: outputName,
      }
    })
    .sort((a, b) => (b.epoch || 0) - (a.epoch || 0))
}
