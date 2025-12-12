/**
 * Centralized formatting utilities for consistent display across the application.
 */

/**
 * Format a number with appropriate precision and grouping.
 * @param {number|string|null|undefined} value - The value to format
 * @param {number} decimals - Number of decimal places (default: 2)
 * @returns {string} Formatted number or em dash for null/undefined
 */
export function formatNumber(value, decimals = 2) {
  if (value === null || value === undefined || value === '') {
    return '—'
  }

  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return '—'
  }

  // For very large or very small numbers, use appropriate precision
  if (Math.abs(numeric) >= 1) {
    return numeric.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: decimals
    })
  }

  // For small decimals, use significant figures
  return numeric.toPrecision(Math.max(decimals, 4))
}

/**
 * Format a fraction (0-1) as a percentage.
 * @param {number|null|undefined} fraction - Fraction between 0 and 1
 * @param {number} decimals - Number of decimal places (default: 1)
 * @returns {string} Formatted percentage (e.g., "33.3%")
 */
export function formatPercent(fraction, decimals = 1) {
  if (fraction === null || fraction === undefined) {
    return '—'
  }

  const percent = Number(fraction) * 100
  return `${percent.toFixed(decimals)}%`
}

/**
 * Format a value as an R multiple.
 * @param {number|null|undefined} value - R multiple value
 * @param {number} decimals - Number of decimal places (default: 1)
 * @returns {string} Formatted R value (e.g., "2.5R")
 */
export function formatR(value, decimals = 1) {
  if (value === null || value === undefined) {
    return '—'
  }

  return `${formatNumber(value, decimals)}R`
}

/**
 * Format a value as currency.
 * @param {number|null|undefined} value - Currency value
 * @param {string} currency - Currency symbol (default: '$')
 * @param {number} decimals - Number of decimal places (default: 2)
 * @returns {string} Formatted currency (e.g., "$1,234.56")
 */
export function formatCurrency(value, currency = '$', decimals = 2) {
  if (value === null || value === undefined) {
    return '—'
  }

  return `${currency}${formatNumber(value, decimals)}`
}

/**
 * Format an instrument number (tick size, contract size, etc.).
 * Handles very small and very large numbers appropriately.
 * @param {number|string|null|undefined} value - The value to format
 * @returns {string} Formatted number
 */
export function formatInstrumentNumber(value) {
  if (value === null || value === undefined || value === '') {
    return '—'
  }

  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return String(value)
  }

  if (Math.abs(numeric) >= 1) {
    return numeric.toLocaleString(undefined, { maximumFractionDigits: 4 })
  }

  return numeric.toPrecision(4)
}

/**
 * Format a timeframe for display.
 * @param {string} timeframe - Timeframe string (e.g., "15m", "1h", "1d")
 * @returns {string} Formatted timeframe
 */
export function formatTimeframe(timeframe) {
  if (!timeframe) return '—'
  return String(timeframe).toUpperCase()
}

/**
 * Format a symbol list for display.
 * @param {Array<string>} symbols - Array of symbol strings
 * @param {number} maxDisplay - Maximum symbols to display before truncating
 * @returns {string} Formatted symbol list
 */
export function formatSymbols(symbols, maxDisplay = 3) {
  if (!Array.isArray(symbols) || symbols.length === 0) {
    return '—'
  }

  if (symbols.length <= maxDisplay) {
    return symbols.join(', ')
  }

  const displayed = symbols.slice(0, maxDisplay).join(', ')
  const remaining = symbols.length - maxDisplay
  return `${displayed} +${remaining} more`
}
