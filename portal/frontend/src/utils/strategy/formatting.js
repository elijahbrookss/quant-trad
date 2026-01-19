const CURRENCY_FORMATTER = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
})

const formatCurrency = (value) => {
  const numericValue = Number(value)
  return CURRENCY_FORMATTER.format(Number.isFinite(numericValue) ? numericValue : 0)
}

const formatNumber = (value, precision = 2) => {
  if (value === null || value === undefined) return '—'
  const numericValue = Number(value)
  if (!Number.isFinite(numericValue)) return value
  return Number(numericValue).toFixed(precision).replace(/\.0+$/, '').replace(/\.([1-9]*)0+$/, '.$1')
}

const parseNumericOr = (value, fallback) => {
  if (value === '' || value === null || value === undefined) return fallback
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

export {
  CURRENCY_FORMATTER,
  formatCurrency,
  formatNumber,
  parseNumericOr,
}
