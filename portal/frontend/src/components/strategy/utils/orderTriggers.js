/**
 * Build display-ready trigger rows from a signal preview instrument payload.
 */
export function buildTriggerRows({ instrumentResult, rules = [], symbol = '' } = {}) {
  if (!instrumentResult) return []
  const ruleLookup = new Map((Array.isArray(rules) ? rules : []).map((rule) => [rule.id, rule]))
  const toIso = (value) => {
    if (!value) return null
    if (typeof value === 'number' && Number.isFinite(value)) {
      return new Date(value * 1000).toISOString()
    }
    if (typeof value === 'string') {
      const trimmed = value.trim()
      if (!trimmed) return null
      if (/^\d+$/.test(trimmed)) {
        const numeric = Number(trimmed)
        if (Number.isFinite(numeric)) return new Date(numeric * 1000).toISOString()
      }
      const parsed = new Date(trimmed)
      if (!Number.isNaN(parsed.getTime())) return parsed.toISOString()
    }
    return null
  }
  const toEpoch = (value) => {
    const iso = toIso(value)
    if (!iso) return 0
    const parsed = new Date(iso)
    if (Number.isNaN(parsed.getTime())) return 0
    return Math.floor(parsed.getTime() / 1000)
  }
  const normalizeReasons = (value) => {
    if (!value) return []
    if (Array.isArray(value)) return value
    if (typeof value === 'string') return [value]
    return []
  }
  const extractSignalMeta = (signal = {}, entry = {}) => {
    const metadata = signal?.metadata && typeof signal.metadata === 'object' ? signal.metadata : {}
    const ruleId = entry.rule_id || signal.rule_id || metadata.rule_id || signal.pattern_id || metadata.pattern_id || null
    const indicatorId = metadata.indicator_id || signal.indicator_id || entry.indicator_id || null
    const runtimeScope = metadata.runtime_scope || signal.runtime_scope || null
    const signalTime = signal.signal_time || signal.time || signal.timestamp || metadata.signal_time || metadata.time || entry.timestamp || null
    const knownAt = metadata.known_at || signal.known_at || null
    const signalId = metadata.signal_id || signal.signal_id || metadata.trace_id || signal.trace_id || null
    const eventId = metadata.event_id || signal.event_id || metadata.dedupe_key || signal.dedupe_key || metadata.event_signature || signal.event_signature || null
    const traceId = metadata.trace_id || signal.trace_id || null
    const level = metadata.level_price || metadata.boundary_price || signal.level_price || signal.boundary_price || null
    const reasons = normalizeReasons(signal.reasons || entry.reasons || entry.conditions_met || metadata.reasons)
    return {
      metadata,
      ruleId,
      indicatorId,
      runtimeScope,
      signalTime,
      knownAt,
      signalId,
      eventId,
      traceId,
      level,
      reasons,
    }
  }

  const toRows = (entries = [], direction = 'BUY') => {
    return entries.flatMap((entry, index) => {
      if (!entry) return []
      const signals = Array.isArray(entry.signals) ? entry.signals : []
      if (signals.length) {
        return signals.map((signal, sigIndex) => {
          const meta = extractSignalMeta(signal, entry)
          const timestamp = toIso(meta.signalTime) || toIso(entry.timestamp)
          const epoch = toEpoch(timestamp)
          const triggerType = entry.trigger_type || signal.trigger_type || entry.action || signal.action || signal.type || 'entry'
          const eventKeyParts = [
            direction,
            entry.rule_id || meta.ruleId || '',
            String(epoch || 0),
            meta.eventId || meta.signalId || meta.traceId || `${index}-${sigIndex}`,
          ]
          return {
            id: eventKeyParts.join('|'),
            rowKey: eventKeyParts.join('|'),
            direction,
            ruleId: meta.ruleId,
            triggerType,
            triggerLabel: String(triggerType || '').toLowerCase(),
            timestamp: timestamp || null,
            epoch,
            reasons: meta.reasons,
            matched: entry.matched,
            indicatorId: meta.indicatorId,
            runtimeScope: meta.runtimeScope,
            knownAt: toIso(meta.knownAt),
            signalId: meta.signalId,
            eventId: meta.eventId,
            traceId: meta.traceId,
            level: meta.level,
            signalType: signal.type || signal.signal_type || null,
            confidence: signal.confidence ?? meta.metadata.confidence ?? null,
          }
        })
      }
      const fallbackTimestamp = toIso(entry.timestamp)
      const fallbackEpoch = toEpoch(fallbackTimestamp)
      return [{
        id: `${direction}|${entry.rule_id || 'rule'}|${String(fallbackEpoch || 0)}|${index}`,
        rowKey: `${direction}|${entry.rule_id || 'rule'}|${String(fallbackEpoch || 0)}|${index}`,
        direction,
        ruleId: entry.rule_id,
        triggerType: entry.trigger_type || entry.action,
        triggerLabel: String(entry.trigger_type || entry.action || 'entry').toLowerCase(),
        timestamp: fallbackTimestamp,
        epoch: fallbackEpoch,
        reasons: normalizeReasons(entry.reasons || entry.conditions_met),
        matched: entry.matched,
        indicatorId: entry.indicator_id || null,
        runtimeScope: null,
        knownAt: null,
        signalId: null,
        eventId: null,
        traceId: null,
        level: null,
        signalType: null,
        confidence: null,
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
      instrumentId: instrumentResult?.window?.instrument_id || instrumentResult?.window?.instrumentId || null,
      ruleRef: String(row.ruleId || rule?.id || '').trim(),
      indicatorRef: String(row.indicatorId || '').trim(),
      signalRef: String(row.signalId || '').trim(),
      eventRef: String(row.eventId || row.traceId || '').trim(),
      runtimeRef: String(row.runtimeScope || '').trim(),
    }
  }).sort((a, b) => (b.epoch || 0) - (a.epoch || 0))
}
