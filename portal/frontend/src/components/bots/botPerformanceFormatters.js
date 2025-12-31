export function formatTimestamp(value) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleTimeString([], { hour12: false })
}

export function formatStatValue(key, value, quoteCurrency) {
  if (value === undefined || value === null) return '—'
  const numeric = Number(value)
  if (['expectancy_r', 'total_r'].includes(key)) {
    if (!Number.isFinite(numeric)) return '—'
    const formatted = numeric.toFixed(2)
    return `${numeric >= 0 ? '+' : ''}${formatted} R`
  }
  if (['max_drawdown', 'expectancy_value', 'avg_win', 'avg_loss'].includes(key)) {
    if (!Number.isFinite(numeric)) return '—'
    const formatted = numeric.toFixed(2)
    return quoteCurrency ? `${formatted} ${quoteCurrency}` : formatted
  }
  const hasCurrency = ['gross_pnl', 'fees_paid', 'net_pnl'].includes(key)
  if (hasCurrency && Number.isFinite(numeric)) {
    const formatted = numeric.toFixed(2)
    return quoteCurrency ? `${formatted} ${quoteCurrency}` : formatted
  }
  if (typeof value === 'number' && !Number.isInteger(value)) {
    return value.toFixed(2)
  }
  if (Number.isFinite(numeric) && `${value}`.trim() !== '') {
    return numeric
  }
  return value
}

export function describeLog(entry) {
  if (!entry) return '—'
  if (entry.message) return entry.message
  const parts = []
  if (entry.symbol) parts.push(entry.symbol)
  if (entry.direction) parts.push(entry.direction.toUpperCase())
  if (entry.leg) parts.push(entry.leg)
  if (entry.price !== undefined && entry.price !== null) {
    const price = Number(entry.price)
    parts.push(Number.isFinite(price) ? `@ ${price.toFixed(4)}` : `@ ${entry.price}`)
  }
  if (entry.reason) parts.push(String(entry.reason).replace(/_/g, ' '))
  if (entry.targets && Array.isArray(entry.targets)) {
    parts.push(`targets: ${entry.targets.map((t) => t.name).join(', ')}`)
  }
  return parts.length ? parts.join(' • ') : '—'
}

export function isTradeLog(entry) {
  if (!entry) return false
  if (entry.trade_id) return true
  const type = (entry.event || entry.type || '').toLowerCase()
  const keywords = [
    'entry',
    'exit',
    'close',
    'target',
    'stop',
    'tp',
    'sl',
    'fill',
    'order',
    'open',
    'trade',
    'execution',
    'rejected',
  ]
  return keywords.some((keyword) => type.includes(keyword))
}
