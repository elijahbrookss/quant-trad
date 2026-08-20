const DEFAULT_ARTIFACT_REFRESH_BARS = 10

const parseTimeframeSeconds = (rawTimeframe) => {
  const text = String(rawTimeframe || '').trim().toLowerCase()
  const match = text.match(/^(\d+)\s*([a-z]+)$/)
  if (!match) return null
  const amount = Number(match[1])
  const unit = match[2]
  if (!Number.isFinite(amount) || amount <= 0) return null
  if (['s', 'sec', 'secs', 'second', 'seconds'].includes(unit)) return amount
  if (['m', 'min', 'mins', 'minute', 'minutes'].includes(unit)) return amount * 60
  if (['h', 'hr', 'hrs', 'hour', 'hours'].includes(unit)) return amount * 3600
  if (['d', 'day', 'days'].includes(unit)) return amount * 86400
  if (['w', 'wk', 'wks', 'week', 'weeks'].includes(unit)) return amount * 7 * 86400
  if (['mo', 'mon', 'month', 'months'].includes(unit)) return amount * 30 * 86400
  if (['y', 'yr', 'yrs', 'year', 'years'].includes(unit)) return amount * 365 * 86400
  return null
}

const deriveSpacing = (candles, timeframe) => {
  const configured = parseTimeframeSeconds(timeframe)
  if (Number.isFinite(configured) && configured > 0) return configured
  const last = Number(candles?.[candles.length - 1]?.time)
  const previous = Number(candles?.[candles.length - 2]?.time)
  const observed = last - previous
  return Number.isFinite(observed) && observed > 0 ? observed : 1
}

export const resolveChartArtifactRefreshKey = ({
  candles = [],
  timeframe = null,
  dataUpdateToken = null,
  refreshBars = DEFAULT_ARTIFACT_REFRESH_BARS,
} = {}) => {
  const safeCandles = Array.isArray(candles) ? candles : []
  const lastTime = Number(safeCandles[safeCandles.length - 1]?.time)
  if (!Number.isFinite(lastTime)) return `empty:${String(dataUpdateToken || '')}`
  const spacing = deriveSpacing(safeCandles, timeframe)
  const interval = Math.max(Number(refreshBars) || DEFAULT_ARTIFACT_REFRESH_BARS, 1)
  const barBucket = Math.floor(lastTime / spacing / interval)
  return `${String(dataUpdateToken || '')}:${spacing}:${barBucket}`
}

export { DEFAULT_ARTIFACT_REFRESH_BARS, parseTimeframeSeconds }
