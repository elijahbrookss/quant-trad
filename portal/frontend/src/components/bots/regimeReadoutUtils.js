export const glyphForAxisState = (axis, state) => {
  const key = (state || '').toLowerCase()
  if (axis === 'volatility') {
    if (['high', 'expanding'].includes(key)) return '↑'
    if (['low', 'compressing'].includes(key)) return '↓'
    return '→'
  }
  if (axis === 'liquidity') {
    if (key === 'thin') return '○'
    if (key === 'normal') return '◐'
    if (['heavy', 'thick'].includes(key)) return '●'
    return '○'
  }
  if (axis === 'expansion') {
    if (key === 'expanding') return '↗'
    if (key === 'compressing') return '↘'
    return '↔'
  }
  if (axis === 'structure') {
    if (key === 'trend') return '↑'
    if (key === 'range') return '↔'
    if (key === 'transition') return '?'
    return '↓'
  }
  return ''
}

export const buildRegimeSnapshots = (points = []) => {
  if (!Array.isArray(points) || points.length === 0) return []
  return points
    .filter((point) => Number.isFinite(point?.time))
    .map((point) => ({
      ts: point.time,
      structure: point.structure || {},
      volatility: point.volatility || {},
      liquidity: point.liquidity || {},
      expansion: point.expansion || {},
      confidence: point.confidence ?? null,
    }))
}

export const nearestSnapshot = (snapshots, ts) => {
  if (!Array.isArray(snapshots) || snapshots.length === 0 || !Number.isFinite(ts)) return null
  let low = 0
  let high = snapshots.length - 1
  while (low <= high) {
    const mid = Math.floor((low + high) / 2)
    const midTs = snapshots[mid]?.ts
    if (!Number.isFinite(midTs)) {
      return null
    }
    if (midTs === ts) {
      return snapshots[mid]
    }
    if (midTs < ts) {
      low = mid + 1
    } else {
      high = mid - 1
    }
  }
  if (low <= 0) return snapshots[0]
  if (low >= snapshots.length) return snapshots[snapshots.length - 1]
  const prev = snapshots[low - 1]
  const next = snapshots[low]
  if (!prev || !next) return prev || next || null
  return Math.abs((prev.ts ?? 0) - ts) <= Math.abs((next.ts ?? 0) - ts) ? prev : next
}

export const buildAxisTooltip = (axis, payload = {}) => {
  const state = payload?.state ?? 'unknown'
  const confidence = payload?.confidence
  const drivers = []
  if (axis === 'structure') {
    drivers.push(['directional_efficiency', payload?.directional_efficiency])
    drivers.push(['slope_stability', payload?.slope_stability])
    drivers.push(['range_position', payload?.range_position])
  }
  if (axis === 'volatility') {
    drivers.push(['atr_ratio', payload?.atr_ratio])
    drivers.push(['atr_zscore', payload?.atr_zscore])
    drivers.push(['tr_pct', payload?.tr_pct])
  }
  if (axis === 'liquidity') {
    drivers.push(['volume_zscore', payload?.volume_zscore])
    drivers.push(['volume_vs_median', payload?.volume_vs_median])
  }
  if (axis === 'expansion') {
    drivers.push(['atr_slope', payload?.atr_slope])
    drivers.push(['overlap_pct', payload?.overlap_pct])
    drivers.push(['range_contraction', payload?.range_contraction])
  }
  const driverText = drivers
    .filter(([, value]) => value !== undefined && value !== null)
    .slice(0, 3)
    .map(([key, value]) => `${key}: ${Number.isFinite(Number(value)) ? Number(value).toFixed(3) : value}`)
    .join(', ')
  const confidenceText = Number.isFinite(Number(confidence))
    ? `confidence ${(Number(confidence) * 100).toFixed(0)}%`
    : 'confidence n/a'
  return `${state} • ${confidenceText}${driverText ? ` • ${driverText}` : ''}`
}
