/**
 * Data conversion utilities for transforming between different representations.
 */

/**
 * Convert a fraction (0-1) to a percentage (0-100).
 * @param {number|null|undefined} fraction - Fraction between 0 and 1
 * @returns {number|null} Percentage value or null
 */
export function fractionToPercent(fraction) {
  if (fraction === null || fraction === undefined) {
    return null
  }
  return Number(fraction) * 100
}

/**
 * Convert a percentage (0-100) to a fraction (0-1).
 * @param {number|null|undefined} percent - Percentage value
 * @returns {number|null} Fraction value or null
 */
export function percentToFraction(percent) {
  if (percent === null || percent === undefined) {
    return null
  }
  return Number(percent) / 100
}

/**
 * Normalize a take profit order to ensure both size_fraction and size_percent are set.
 * @param {Object} order - Take profit order object
 * @returns {Object} Normalized order
 */
export function normalizeTargetSize(order) {
  const normalized = { ...order }

  // If we have size_fraction, ensure size_percent is set
  if (normalized.size_fraction != null) {
    normalized.size_percent = fractionToPercent(normalized.size_fraction)
  }
  // If we have size_percent, ensure size_fraction is set
  else if (normalized.size_percent != null) {
    normalized.size_fraction = percentToFraction(normalized.size_percent)
  }

  return normalized
}

/**
 * Distribute size evenly across multiple targets.
 * @param {number} count - Number of targets
 * @returns {Array<number>} Array of percentages that sum to 100
 */
export function distributeEvenPercents(count) {
  if (count <= 0) return []
  if (count === 1) return [100]

  const basePercent = Math.floor(10000 / count) / 100
  const percents = new Array(count).fill(basePercent)

  // Distribute remainder to first targets
  let sum = percents.reduce((a, b) => a + b, 0)
  let idx = 0
  while (Math.abs(sum - 100) > 0.01 && idx < count) {
    const diff = 100 - sum
    percents[idx] += diff > 0 ? 0.01 : -0.01
    percents[idx] = Math.round(percents[idx] * 100) / 100
    sum = percents.reduce((a, b) => a + b, 0)
    idx++
  }

  return percents
}

/**
 * Convert stop R multiple to positive value (backend stores positive, display positive).
 * @param {number|null|undefined} value - Stop R multiple
 * @returns {number|null} Absolute value or null
 */
export function normalizeStopR(value) {
  if (value === null || value === undefined) {
    return null
  }
  return Math.abs(Number(value))
}

/**
 * Coerce a value to a number or return null.
 * @param {*} value - Value to coerce
 * @param {number|null} defaultValue - Default value if coercion fails
 * @returns {number|null} Coerced number or default
 */
export function coerceNumber(value, defaultValue = null) {
  if (value === null || value === undefined || value === '') {
    return defaultValue
  }

  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : defaultValue
}

/**
 * Clamp a number between min and max values.
 * @param {number} value - Value to clamp
 * @param {number} min - Minimum value
 * @param {number} max - Maximum value
 * @returns {number} Clamped value
 */
export function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max)
}
