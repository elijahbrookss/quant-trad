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

const normalizeEpoch = (value) => {
  if (!Number.isFinite(Number(value))) return null
  return Number(value)
}

const findNearestIndex = (times, ts) => {
  if (!Array.isArray(times) || times.length === 0 || !Number.isFinite(ts)) return null
  let low = 0
  let high = times.length - 1
  while (low <= high) {
    const mid = Math.floor((low + high) / 2)
    const midTs = times[mid]
    if (midTs === ts) return mid
    if (midTs < ts) {
      low = mid + 1
    } else {
      high = mid - 1
    }
  }
  if (low <= 0) return 0
  if (low >= times.length) return times.length - 1
  const prev = times[low - 1]
  const next = times[low]
  if (!Number.isFinite(prev)) return low
  if (!Number.isFinite(next)) return low - 1
  return Math.abs(prev - ts) <= Math.abs(next - ts) ? low - 1 : low
}

export const buildRegimeBlockSnapshots = (blocks = []) => {
  if (!Array.isArray(blocks) || blocks.length === 0) return []
  return blocks
    .map((block) => ({
      x1: normalizeEpoch(block?.x1),
      x2: normalizeEpoch(block?.x2),
      known_at: normalizeEpoch(block?.known_at ?? block?.x1),
      structure: block?.structure || {},
      volatility: block?.volatility || {},
      liquidity: block?.liquidity || {},
      expansion: block?.expansion || {},
      confidence: block?.confidence ?? null,
      entry_confidence: block?.entry_confidence ?? null,
      score_margin: block?.score_margin ?? null,
      bars: block?.bars ?? null,
      regime_key: block?.regime_key,
      block_id: block?.block_id,
      known_at: normalizeEpoch(block?.known_at ?? block?.x1),
      trend_direction: block?.structure?.trend_direction ?? 'neutral',
    }))
    .filter((block) => Number.isFinite(block?.x1) && Number.isFinite(block?.x2))
    .sort((a, b) => (a.x1 ?? 0) - (b.x1 ?? 0))
}

export const buildCandleSnapshots = (points = []) => {
  if (!Array.isArray(points) || points.length === 0) return []
  return points
    .map((point) => ({
      ts: normalizeEpoch(point?.time),
      structure: point?.structure || {},
      volatility: point?.volatility || {},
      liquidity: point?.liquidity || {},
      expansion: point?.expansion || {},
      confidence: point?.confidence ?? null,
    }))
    .filter((point) => Number.isFinite(point?.ts))
    .sort((a, b) => (a.ts ?? 0) - (b.ts ?? 0))
}

export const getActiveRegimeBlock = (blocks, ts) => {
  if (!Array.isArray(blocks) || blocks.length === 0 || !Number.isFinite(ts)) return null
  const startTimes = blocks.map((block) => block?.x1)
  const idx = findNearestIndex(startTimes, ts)
  if (idx === null) return null
  let cursor = idx
  while (cursor >= 0) {
    const block = blocks[cursor]
    const start = block?.x1
    const end = block?.x2
    const knownAt = Number.isFinite(block?.known_at) ? block.known_at : start
    if (!Number.isFinite(start) || !Number.isFinite(end)) {
      cursor -= 1
      continue
    }
    if (knownAt != null && knownAt > ts) {
      cursor -= 1
      continue
    }
    if (ts >= start && ts <= end) {
      return block
    }
    if (ts < start) {
      cursor -= 1
      continue
    }
    if (ts > end) {
      return block
    }
    cursor -= 1
  }
  return null
}

export const getNearestCandleStats = (points, ts) => {
  if (!Array.isArray(points) || points.length === 0 || !Number.isFinite(ts)) return null
  const times = points.map((point) => point?.ts)
  const idx = findNearestIndex(times, ts)
  if (idx === null) return null
  return points[idx] || null
}

export const buildReadoutSnapshot = ({ focusTs, blocks, points, lastSnapshot }) => {
  if (!Number.isFinite(focusTs)) return lastSnapshot || null
  const block = getActiveRegimeBlock(blocks, focusTs)
  const candle = getNearestCandleStats(points, focusTs)
  if (!candle) return lastSnapshot || null
  const structureConfidence =
    block && Number.isFinite(block?.confidence)
      ? Number(block.confidence)
      : block && Number.isFinite(block?.entry_confidence)
        ? Number(block.entry_confidence)
        : null
  return {
      structure: {
        state: block?.structure?.state ?? 'unknown',
        confidence: structureConfidence,
        block_id: block?.block_id ?? null,
        bars: block?.bars ?? null,
        score_margin: block?.score_margin ?? null,
        trend_direction: block?.trend_direction ?? block?.structure?.trend_direction ?? 'neutral',
        known_at: block?.known_at ?? null,
      },
    volatility: {
      ...(candle?.volatility || {}),
      state: candle?.volatility?.state ?? 'unknown',
      confidence: candle?.volatility?.confidence ?? candle?.confidence ?? null,
    },
    liquidity: {
      ...(candle?.liquidity || {}),
      state: candle?.liquidity?.state ?? 'unknown',
      confidence: candle?.liquidity?.confidence ?? candle?.confidence ?? null,
    },
    expansion: {
      ...(candle?.expansion || {}),
      state: candle?.expansion?.state ?? 'unknown',
      confidence: candle?.expansion?.confidence ?? candle?.confidence ?? null,
    },
  }
}

export const findNearestCandleTime = (candles, ts) => {
  if (!Array.isArray(candles) || candles.length === 0 || !Number.isFinite(ts)) return null
  const times = candles.map((candle) => candle?.time)
  const idx = findNearestIndex(times, ts)
  if (idx === null) return null
  const value = times[idx]
  return Number.isFinite(value) ? value : null
}

export const buildAxisTooltip = (axis, payload = {}) => {
  const state = payload?.state ?? 'unknown'
  const confidence = payload?.confidence
  if (axis === 'structure') {
    const blockId = payload?.block_id ?? 'n/a'
    const bars = Number.isFinite(Number(payload?.bars)) ? Number(payload?.bars) : null
    const confidenceText = Number.isFinite(Number(confidence))
      ? `avg_conf ${(Number(confidence) * 100).toFixed(0)}%`
      : 'avg_conf n/a'
    const marginText = Number.isFinite(Number(payload?.score_margin))
      ? `margin ${Number(payload.score_margin).toFixed(2)}`
      : 'margin n/a'
    const barsText = bars ? `bars ${bars}` : 'bars n/a'
    const direction = payload?.trend_direction && payload?.trend_direction !== 'neutral'
      ? ` • ${payload.trend_direction}`
      : ''
    return `${state}${direction} • ${confidenceText} • ${marginText} • ${barsText} • block ${blockId}`
  }
  const drivers = []
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
    .slice(0, 2)
    .map(([key, value]) => `${key}: ${Number.isFinite(Number(value)) ? Number(value).toFixed(3) : value}`)
    .join(', ')
  const confidenceText = Number.isFinite(Number(confidence))
    ? `confidence ${(Number(confidence) * 100).toFixed(0)}%`
    : 'confidence n/a'
  return `${state} • ${confidenceText}${driverText ? ` • ${driverText}` : ''}`
}
