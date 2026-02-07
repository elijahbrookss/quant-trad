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

export const buildRegimeSnapshots = (blocks = []) => {
  if (!Array.isArray(blocks) || blocks.length === 0) return []
  return blocks
    .filter((block) => Number.isFinite(block?.x1) && Number.isFinite(block?.x2))
    .map((block) => ({
      x1: block.x1,
      x2: block.x2,
      known_at: Number.isFinite(block?.known_at) ? block.known_at : block.x1,
      structure: block.structure || {},
      volatility: block.volatility || {},
      liquidity: block.liquidity || {},
      expansion: block.expansion || {},
      confidence: block.confidence ?? null,
      regime_key: block.regime_key,
      block_id: block.block_id,
    }))
    .sort((a, b) => (a.x1 ?? 0) - (b.x1 ?? 0))
}

export const findSnapshotForTime = (snapshots, ts) => {
  if (!Array.isArray(snapshots) || snapshots.length === 0 || !Number.isFinite(ts)) return null
  let lastKnown = null
  for (const snapshot of snapshots) {
    const start = snapshot?.x1
    const end = snapshot?.x2
    const knownAt = Number.isFinite(snapshot?.known_at) ? snapshot.known_at : snapshot?.x1
    if (!Number.isFinite(start) || !Number.isFinite(end) || !Number.isFinite(knownAt)) {
      continue
    }
    if (knownAt <= ts) {
      lastKnown = snapshot
    }
    if (ts >= start && ts <= end && knownAt <= ts) {
      return snapshot
    }
  }
  return lastKnown
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
