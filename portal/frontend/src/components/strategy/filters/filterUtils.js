import { FILTER_OPERATORS, getFilterType, listFilterTypes, operatorOptionsFor } from './registry.js'

const operatorLabelMap = new Map(FILTER_OPERATORS.map((op) => [op.value, op.label]))

export const buildFilterSummary = (filter) => {
  const dsl = filter?.dsl || filter
  if (!dsl) return 'Filter'
  const groupKey = dsl.all ? 'all' : dsl.any ? 'any' : null
  const predicates = groupKey ? dsl[groupKey] : [dsl]
  const parts = predicates
    .filter(Boolean)
    .slice(0, 2)
    .map((predicate) => {
      const type = getFilterType(predicate.source)
      const field = type?.fields?.find((entry) => entry.path === predicate.path)
      const sourceLabel = type?.label || predicate.source || 'Filter'
      const fieldLabel = field?.label || predicate.path || 'Field'
      const operatorLabel = operatorLabelMap.get(predicate.operator) || predicate.operator || '='
      const valueLabel = formatValue(predicate)
      if (predicate.operator === 'exists' || predicate.operator === 'missing') {
        return `${sourceLabel}.${fieldLabel} ${operatorLabel}`
      }
      return `${sourceLabel}.${fieldLabel} ${operatorLabel} ${valueLabel}`
    })
  const tail = predicates.length > 2 ? ` +${predicates.length - 2} more` : ''
  const connector = groupKey === 'any' ? ' OR ' : ' AND '
  return parts.join(connector) + tail
}

const formatValue = (predicate) => {
  const value = predicate?.value
  if (Array.isArray(value)) {
    return value.join(', ')
  }
  if (value === null || value === undefined) return '—'
  return String(value)
}

export const createEmptyPredicate = () => {
  const defaultType = listFilterTypes()[0]
  const defaultField = defaultType?.fields?.[0]
  return {
    source: defaultType?.key || 'regime_stats',
    path: defaultField?.path || '$.structure.state',
    operator: 'eq',
    value: '',
    missing_data_policy: 'fail',
    stats_version: '',
    regime_version: '',
    fieldMode: 'preset',
  }
}

export const parseFilterToDraft = (filter) => {
  if (!filter) return null
  const dsl = filter.dsl || {}
  const groupKey = dsl.all ? 'all' : dsl.any ? 'any' : null
  const predicates = groupKey ? dsl[groupKey] : [dsl]
  return {
    id: filter.id,
    name: filter.name || '',
    description: filter.description || '',
    enabled: filter.enabled !== false,
    groupMode: groupKey || 'all',
    predicates: predicates.map((predicate) => ({
      source: predicate.source || 'regime_stats',
      path: predicate.path || '$.structure.state',
      operator: predicate.operator || 'eq',
      value: predicate.value ?? '',
      missing_data_policy: predicate.missing_data_policy || 'fail',
      stats_version: predicate.stats_version || '',
      regime_version: predicate.regime_version || '',
      fieldMode: predicate.path ? 'preset' : 'advanced',
    })),
  }
}

export const buildFilterPayload = (draft) => {
  const predicates = (draft.predicates || []).filter((predicate) => predicate.source && predicate.path)
  const normalizedPredicates = predicates.map((predicate) => {
    const value = normalizeValue(predicate)
    const base = {
      source: predicate.source,
      path: predicate.path,
      operator: predicate.operator || 'eq',
      missing_data_policy: predicate.missing_data_policy || 'fail',
    }
    if (predicate.source === 'candle_stats' && predicate.stats_version) {
      base.stats_version = predicate.stats_version
    }
    if (predicate.source === 'regime_stats' && predicate.regime_version) {
      base.regime_version = predicate.regime_version
    }
    if (base.operator !== 'exists' && base.operator !== 'missing') {
      base.value = value
    }
    return base
  })
  const groupMode = draft.groupMode === 'any' ? 'any' : 'all'
  const dsl = normalizedPredicates.length > 1
    ? { [groupMode]: normalizedPredicates }
    : normalizedPredicates[0] || {}
  const name = draft.name?.trim() || buildFilterSummary({ dsl })
  return {
    name,
    description: draft.description?.trim() || null,
    enabled: draft.enabled !== false,
    dsl,
  }
}

export const buildFilterPreview = (draft) => buildFilterSummary({ dsl: buildFilterPayload(draft).dsl })

export const normalizeValue = (predicate) => {
  const operator = predicate.operator
  const raw = predicate.value
  if (operator === 'between') {
    const parts = String(raw || '').split(',').map((entry) => entry.trim()).filter(Boolean)
    if (parts.length === 2) {
      return parts.map((value) => maybeNumber(value))
    }
    return [0, 0]
  }
  if (operator === 'in' || operator === 'not_in') {
    return String(raw || '')
      .split(',')
      .map((entry) => entry.trim())
      .filter(Boolean)
      .map((value) => maybeNumber(value))
  }
  return maybeNumber(raw)
}

const maybeNumber = (value) => {
  if (value === '' || value === null || value === undefined) return value
  const numeric = Number(value)
  return Number.isNaN(numeric) ? value : numeric
}

export const buildPredicateDefaults = (source) => {
  const type = getFilterType(source)
  const field = type?.fields?.[0]
  return {
    source: source || 'regime_stats',
    path: field?.path || '$.structure.state',
    operator: operatorOptionsFor(source, field?.path)?.[0]?.value || 'eq',
    value: '',
    missing_data_policy: 'fail',
    stats_version: '',
    regime_version: '',
    fieldMode: 'preset',
  }
}
