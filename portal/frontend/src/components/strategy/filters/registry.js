const defaultOperators = [
  { value: 'eq', label: '=' },
  { value: 'ne', label: '≠' },
  { value: 'gt', label: '>' },
  { value: 'gte', label: '≥' },
  { value: 'lt', label: '<' },
  { value: 'lte', label: '≤' },
  { value: 'in', label: 'In' },
  { value: 'not_in', label: 'Not in' },
  { value: 'between', label: 'Between' },
  { value: 'exists', label: 'Exists' },
  { value: 'missing', label: 'Missing' },
]

const registry = new Map()

export const registerFilterType = (type) => {
  if (!type?.key) return
  registry.set(type.key, type)
}

export const getFilterType = (key) => registry.get(key)

export const listFilterTypes = () => Array.from(registry.values())

export const operatorOptionsFor = (typeKey, fieldPath) => {
  const type = registry.get(typeKey)
  if (!type) return defaultOperators
  const field = type.fields?.find((entry) => entry.path === fieldPath)
  if (Array.isArray(field?.operators) && field.operators.length) {
    return field.operators
  }
  return type.operators || defaultOperators
}

registerFilterType({
  key: 'regime_stats',
  label: 'Regime',
  fields: [
    { label: 'Trend state', path: '$.structure.state', valueType: 'enum' },
    { label: 'Volatility state', path: '$.volatility.state', valueType: 'enum' },
    { label: 'Expansion state', path: '$.expansion.state', valueType: 'enum' },
    { label: 'Liquidity state', path: '$.liquidity.state', valueType: 'enum' },
    { label: 'Confidence', path: '$.confidence', valueType: 'number' },
  ],
  operators: defaultOperators,
})

registerFilterType({
  key: 'candle_stats',
  label: 'Candle',
  fields: [
    { label: 'Body %', path: '$.body_pct', valueType: 'number' },
    { label: 'TR %', path: '$.tr_pct', valueType: 'number' },
    { label: 'ATR ratio', path: '$.atr_ratio', valueType: 'number' },
    { label: 'ATR slope', path: '$.atr_slope', valueType: 'number' },
    { label: 'Range position', path: '$.range_position', valueType: 'number' },
    { label: 'Directional efficiency', path: '$.directional_efficiency', valueType: 'number' },
    { label: 'Volume z-score', path: '$.volume_zscore', valueType: 'number' },
  ],
  operators: defaultOperators,
})

export const FILTER_OPERATORS = defaultOperators
