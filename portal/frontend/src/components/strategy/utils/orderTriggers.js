const formatEventLabel = (value) => {
  const raw = typeof value === 'string' ? value.trim() : ''
  if (!raw) return 'Signal'
  return raw
    .split(/[_-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ')
}

const formatPrimitiveValue = (value) => {
  if (value === null || value === undefined || value === '') {
    return 'Unavailable'
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No'
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return 'Unavailable'
    }
    const fixed = value.toFixed(Math.abs(value) >= 100 ? 2 : 4)
    return fixed.replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1')
  }
  return String(value)
}

const isStructuredObject = (value) => (
  value && typeof value === 'object' && !Array.isArray(value)
)

const summarizeStructuredValue = (value) => {
  if (Array.isArray(value)) {
    return `${value.length} item${value.length === 1 ? '' : 's'}`
  }
  if (isStructuredObject(value)) {
    const count = Object.keys(value).length
    return `${count} field${count === 1 ? '' : 's'}`
  }
  return formatPrimitiveValue(value)
}

const outputRefLabel = (outputRef, field) => {
  const base = String(outputRef || '').trim()
  const suffix = String(field || '').trim()
  if (!base) return suffix || 'State'
  if (!suffix) return base
  return `${base}.${suffix}`
}

const countMatchedWindowEntries = (entries = []) => (
  entries.filter((entry) => Boolean(entry?.matched || entry?.event_present)).length
)

const buildGuardChecks = (guards = []) => (
  guards.map((guard, index) => {
    if (guard?.type === 'context_match') {
      return {
        key: `${guard.output_ref || 'context'}:${guard.field || 'state'}:${index}`,
        label: outputRefLabel(guard.output_ref, guard.field),
        status: guard.matched ? 'matched' : (guard.ready ? 'failed' : 'not_ready'),
        detail: Array.isArray(guard.expected) && guard.expected.length
          ? `Expected ${guard.expected.map((value) => formatPrimitiveValue(value)).join(' or ')}`
          : 'Context check',
        note: guard.ready ? `Actual ${formatPrimitiveValue(guard.actual)}` : 'Context output not ready',
      }
    }

    if (guard?.type === 'metric_match') {
      return {
        key: `${guard.output_ref || 'metric'}:${guard.field || 'value'}:${index}`,
        label: outputRefLabel(guard.output_ref, guard.field),
        status: guard.matched ? 'matched' : (guard.ready ? 'failed' : 'not_ready'),
        detail: `${guard.operator || '='} ${formatPrimitiveValue(guard.expected)}`,
        note: guard.ready ? `Actual ${formatPrimitiveValue(guard.actual)}` : 'Metric output not ready',
      }
    }

    if (guard?.type === 'holds_for_bars') {
      const windowResults = Array.isArray(guard.window_results) ? guard.window_results : []
      const matchedBars = countMatchedWindowEntries(windowResults)
      return {
        key: `${guard.guard?.output_ref || 'history'}:holds_for_bars:${index}`,
        label: `${outputRefLabel(guard.guard?.output_ref, guard.guard?.field)} held`,
        status: guard.matched ? 'matched' : (guard.insufficient_history ? 'not_ready' : 'failed'),
        detail: `${matchedBars}/${guard.bars || 0} bars matched`,
        note: guard.insufficient_history ? 'Not enough bars in preview window yet' : null,
      }
    }

    if (guard?.type === 'signal_seen_within_bars' || guard?.type === 'signal_absent_within_bars') {
      return {
        key: `${guard.output_ref || 'signal'}:${guard.event_key || guard.type}:${index}`,
        label: outputRefLabel(guard.output_ref, guard.event_key),
        status: guard.matched ? 'matched' : 'failed',
        detail: guard.type === 'signal_seen_within_bars' ? 'Seen in lookback' : 'Stayed absent in lookback',
        note: `${guard.lookback_bars || 0} bar window`,
      }
    }

    return {
      key: `guard:${index}`,
      label: formatEventLabel(guard?.type || 'guard'),
      status: guard?.matched ? 'matched' : 'failed',
      detail: 'Guard check',
      note: null,
    }
  })
)

const resolveIndicatorTypeLabel = (indicatorLookup, indicatorId) => {
  const meta = indicatorLookup?.get?.(indicatorId) || indicatorLookup?.[indicatorId]
  return meta?.type || meta?.manifest?.type || meta?.meta?.type || indicatorId || 'indicator'
}

const buildHumanOutputLabel = (outputRef, indicatorLookup) => {
  const [indicatorId = '', outputName = ''] = String(outputRef || '').split('.', 2)
  const indicatorType = resolveIndicatorTypeLabel(indicatorLookup, indicatorId)
  if (!outputName) return indicatorType
  return `${indicatorType}.${outputName}`
}

const buildValueNode = (value, { key, label }) => {
  if (Array.isArray(value)) {
    return {
      key,
      label,
      kind: 'array',
      summary: summarizeStructuredValue(value),
      children: value.map((item, index) => buildValueNode(item, {
        key: `${key}:${index}`,
        label: `[${index}]`,
      })),
    }
  }

  if (isStructuredObject(value)) {
    return {
      key,
      label,
      kind: 'object',
      summary: summarizeStructuredValue(value),
      children: Object.entries(value).map(([childKey, childValue]) => buildValueNode(childValue, {
        key: `${key}:${childKey}`,
        label: childKey,
      })),
    }
  }

  return {
    key,
    label,
    kind: 'scalar',
    value: formatPrimitiveValue(value),
  }
}

const normalizeOutputEntries = (outputs = {}, { indicatorLookup } = {}) => {
  if (!outputs || typeof outputs !== 'object') return []
  return Object.entries(outputs)
    .filter(([outputRef, output]) => outputRef && output && typeof output === 'object')
    .map(([outputRef, output]) => {
      const type = typeof output.type === 'string' ? output.type : 'output'
      const ready = Boolean(output.ready)
      const fields = output.fields && typeof output.fields === 'object'
        ? Object.entries(output.fields).map(([field, value]) => buildValueNode(value, {
          key: `${outputRef}:${field}`,
          label: field,
        }))
        : []
      const events = Array.isArray(output.events)
        ? output.events.map((event, index) => ({
          key: `${outputRef}:event:${event?.key || index}`,
          eventKey: event?.key ? formatEventLabel(event.key) : 'Signal event',
          direction: event?.direction ? String(event.direction).toUpperCase() : null,
          knownAt: event?.known_at || null,
        }))
        : []
      const eventCount = Number.isFinite(Number(output.event_count)) ? Number(output.event_count) : events.length

      return {
        key: outputRef,
        outputRef,
        label: buildHumanOutputLabel(outputRef, indicatorLookup),
        type,
        ready,
        barTime: output.bar_time || null,
        fields,
        fieldCount: fields.length,
        events,
        eventCount,
        eventKeys: Array.isArray(output.event_keys) ? output.event_keys.map((value) => formatEventLabel(value)) : [],
      }
    })
    .filter((entry) => entry.type !== 'signal')
}

export function buildTriggerRows({ instrumentResult, rules = [], symbol = '' } = {}) {
  if (!instrumentResult) return []

  const ruleLookup = new Map((Array.isArray(rules) ? rules : []).map((rule) => [rule.id, rule]))
  const artifacts = Array.isArray(instrumentResult?.machine?.decision_artifacts)
    ? instrumentResult.machine.decision_artifacts
    : []
  const signals = Array.isArray(instrumentResult?.machine?.signals)
    ? instrumentResult.machine.signals
    : []
  const signalByDecisionId = new Map(
    signals
      .filter((signal) => signal && typeof signal === 'object')
      .map((signal) => [String(signal?.decision_id || '').trim(), signal]),
  )

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
      const decisionId = String(artifact?.decision_id || '').trim()
      const signal = signalByDecisionId.get(decisionId) || null
      return {
        id: decisionId || `${artifact?.rule_id || 'rule'}|${epoch || 0}|${index}`,
        rowKey: decisionId || `${artifact?.rule_id || 'rule'}|${epoch || 0}|${index}`,
        decisionId: decisionId || null,
        signalId: typeof signal?.signal_id === 'string' ? signal.signal_id : null,
        sourceType: typeof signal?.source_type === 'string' ? signal.source_type : null,
        sourceId: typeof signal?.source_id === 'string' ? signal.source_id : null,
        direction,
        ruleId: artifact?.rule_id || null,
        ruleName: artifact?.rule_name || rule?.name || artifact?.rule_id || 'Rule',
        triggerType: artifact?.trigger?.event_key || 'event',
        triggerDisplay: formatEventLabel(artifact?.trigger?.event_key || 'event'),
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
        artifact,
        signal,
      }
    })
    .sort((a, b) => (b.epoch || 0) - (a.epoch || 0))
}

export function buildTriggerDetail(row, options = {}) {
  if (!row) {
    return null
  }

  const guards = Array.isArray(row.guards) ? row.guards : []

  return {
    summary: {
      direction: row.direction || 'BUY',
      ruleName: row.ruleName || 'Rule',
      triggerDisplay: row.triggerDisplay || formatEventLabel(row.triggerType),
      timestamp: row.timestamp || null,
      instrument: row.instrument || '—',
      indicatorRef: row.indicatorRef || null,
      outputRef: row.outputRef || null,
    },
    references: [
      { key: 'signal_id', label: 'Signal ID', value: row.signalId || null },
      { key: 'decision_id', label: 'Decision ID', value: row.decisionId || null },
      { key: 'preview_id', label: 'Preview ID', value: row.sourceId || null },
    ].filter((item) => item.value),
    observedOutputs: normalizeOutputEntries(row.artifact?.observed_outputs, options),
    referencedOutputs: normalizeOutputEntries(row.artifact?.referenced_outputs, options),
    guardChecks: buildGuardChecks(guards),
  }
}
