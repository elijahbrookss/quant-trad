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

export const coalesce = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null) {
      return value
    }
  }
  return undefined
}

export const normalizeCandles = (candles = []) => {
  if (!Array.isArray(candles)) return []
  const normalized = candles
    .map((candle) => ({
      time: toSec(candle?.time),
      open: toFiniteNumber(candle?.open),
      high: toFiniteNumber(candle?.high),
      low: toFiniteNumber(candle?.low),
      close: toFiniteNumber(candle?.close),
    }))
    .filter(
      (entry) =>
        Number.isFinite(entry.time) &&
        Number.isFinite(entry.open) &&
        Number.isFinite(entry.high) &&
        Number.isFinite(entry.low) &&
        Number.isFinite(entry.close),
    )

  return normalized.sort((a, b) => a.time - b.time)
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
