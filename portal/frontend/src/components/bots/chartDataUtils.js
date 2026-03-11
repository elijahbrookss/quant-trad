export const toSec = (value) => {
  if (value == null) return value
  if (typeof value === 'number') {
    return value > 2e10 ? Math.floor(value / 1000) : value
  }
  const ts = Date.parse(value)
  if (Number.isFinite(ts)) {
    return Math.floor(ts / 1000)
  }
  return null
}

export const toFiniteNumber = (value) => {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

// Toggle verbose BotLens console diagnostics with VITE_BOTLENS_DEBUG=true
export const BOTLENS_DEBUG = Boolean(import.meta?.env?.VITE_BOTLENS_DEBUG === 'true')

export const coalesce = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null) {
      return value
    }
  }
  return undefined
}

export const buildCandleLookup = (candles = []) => {
  const map = new Map()
  for (const candle of candles || []) {
    const epoch = toSec(candle?.time)
    if (Number.isFinite(epoch)) {
      map.set(epoch, candle)
    }
  }
  return map
}
