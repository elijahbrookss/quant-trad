import { ATLAS_FAMILIES } from '../types/atlasTypes.js'
import { buildDistrictLayouts, normalizeDistrictKey } from './WorldLayoutSystem.js'

const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5))

const FAMILY_BY_STRATEGY = {
  breakout: ATLAS_FAMILIES.bridge,
  'mean-reversion': ATLAS_FAMILIES.ring,
  momentum: ATLAS_FAMILIES.spire,
  regime: ATLAS_FAMILIES.triad,
  scalper: ATLAS_FAMILIES.cluster,
  arbitrage: ATLAS_FAMILIES.twin,
  volatility: ATLAS_FAMILIES.obelisk,
}

const SYMBOL_COLORS = {
  BTC: '#f59e0b',
  ETH: '#60a5fa',
  SOL: '#34d399',
  LINK: '#38bdf8',
  AVAX: '#fb7185',
  DOGE: '#fbbf24',
  XRP: '#a78bfa',
  ADA: '#2dd4bf',
  BNB: '#facc15',
  MATIC: '#c084fc',
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

export function hashString(input) {
  let hash = 2166136261
  const value = String(input || '')
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0
}

function seededUnit(seed) {
  let value = seed >>> 0
  value += 0x6d2b79f5
  value = Math.imul(value ^ (value >>> 15), value | 1)
  value ^= value + Math.imul(value ^ (value >>> 7), value | 61)
  return ((value ^ (value >>> 14)) >>> 0) / 4294967296
}

function seededRange(seed, min, max) {
  return min + seededUnit(seed) * (max - min)
}

function formatSymbolRoot(symbol) {
  return String(symbol || '').split('-')[0].toUpperCase()
}

function buildOrbitingSymbols(run, seed) {
  return run.symbols.map((symbol, index) => {
    const root = formatSymbolRoot(symbol)
    return {
      symbol,
      root,
      radius: 1.35 + index * 0.34 + seededRange(seed + index * 13, 0, 0.24),
      angle: seededRange(seed + index * 29, 0, Math.PI * 2),
      speed: seededRange(seed + index * 37, 0.12, 0.34) * (index % 2 === 0 ? 1 : -1),
      y: 0.76 + index * 0.28,
      color: SYMBOL_COLORS[root] || '#cbd5e1',
    }
  })
}

function buildWindows(run, height, seed) {
  const count = clamp(Math.round(run.tradeCount / 2.7), 7, 58)
  return Array.from({ length: count }, (_, index) => ({
    angle: (index % 8) / 8 * Math.PI * 2,
    yRatio: 0.12 + ((Math.floor(index / 8) % 9) / 9) * 0.78 + seededRange(seed + index * 11, -0.018, 0.018),
    size: seededRange(seed + index * 17, 0.052, 0.1),
    intensity: seededRange(seed + index * 23, 0.34, 1),
    warm: index % 5 === 0,
  })).filter((window) => window.yRatio > 0.08 && window.yRatio < 0.95)
}

function buildCracks(damage, seed) {
  const count = Math.round(clamp(damage * 12, 0, 12))
  return Array.from({ length: count }, (_, index) => ({
    angle: seededRange(seed + index * 41, 0, Math.PI * 2),
    yRatio: seededRange(seed + index * 43, 0.2, 0.88),
    length: seededRange(seed + index * 47, 0.18, 0.54),
    skew: seededRange(seed + index * 53, -0.7, 0.7),
    opacity: seededRange(seed + index * 59, 0.34, 0.78),
  }))
}

function getArtifactColor(run, family, districtColor) {
  if (run.status === 'failed') {
    return {
      color: '#1f2937',
      emissive: '#7f1d1d',
      glow: '#fb7185',
    }
  }

  if (run.pnl < 0) {
    return {
      color: '#3b2f2f',
      emissive: '#9f1239',
      glow: '#fb7185',
    }
  }

  if (family === ATLAS_FAMILIES.ring) {
    return { color: '#0f2f2c', emissive: '#14b8a6', glow: '#5eead4' }
  }
  if (family === ATLAS_FAMILIES.spire) {
    return { color: '#172554', emissive: '#38bdf8', glow: '#7dd3fc' }
  }
  if (family === ATLAS_FAMILIES.triad) {
    return { color: '#312e81', emissive: '#a78bfa', glow: '#c4b5fd' }
  }
  if (family === ATLAS_FAMILIES.cluster) {
    return { color: '#1c2b22', emissive: '#22c55e', glow: '#86efac' }
  }
  if (family === ATLAS_FAMILIES.twin) {
    return { color: '#11343b', emissive: '#2dd4bf', glow: '#99f6e4' }
  }
  if (family === ATLAS_FAMILIES.obelisk) {
    return { color: '#3b2a13', emissive: '#f59e0b', glow: '#fcd34d' }
  }
  return { color: '#142033', emissive: districtColor, glow: '#93c5fd' }
}

function buildArtifact(run, index, districtIndex, district, districtTotal) {
  const seed = hashString(`${run.id}:${run.strategy}:${run.experiment}`)
  const family = run.status === 'failed'
    ? ATLAS_FAMILIES.ruin
    : FAMILY_BY_STRATEGY[run.strategyFamily] || FAMILY_BY_STRATEGY[run.strategyFamily?.toLowerCase?.()] || ATLAS_FAMILIES.obelisk
  const profitability = clamp((run.pnl + 2600) / 7600, 0, 1)
  const damage = clamp((run.drawdown / 4200) + (run.pnl < 0 ? 0.22 : 0) + (run.status === 'failed' ? 0.38 : 0), 0, 1)
  const liveHeight = 2.1 + Math.log1p(run.tradeCount) * 0.28 + Math.max(0, run.pnl) / 720
  const lossHeight = 2.2 + Math.abs(Math.min(0, run.pnl)) / 1800
  const height = clamp((run.pnl >= 0 ? liveHeight : lossHeight) * (run.status === 'failed' ? 0.52 : 1), 1.25, 9.8)
  const width = clamp(0.72 + Math.log1p(run.tradeCount) * 0.06, 0.78, 1.28)
  const districtRadius = Math.max(2.4, district.radius - 1.2)
  const ring = Math.floor(districtIndex / 6)
  const angle = districtIndex * GOLDEN_ANGLE + seededRange(seed, -0.28, 0.28)
  const radius = clamp(2.2 + Math.sqrt(districtIndex + 1) * 2.15 + ring * 0.5, 1.6, districtRadius)
  const position = {
    x: district.anchor.x + Math.cos(angle) * radius,
    y: 0,
    z: district.anchor.z + Math.sin(angle) * radius,
  }
  const colors = getArtifactColor(run, family, district.color)

  return {
    id: run.id,
    index,
    seed,
    run,
    family,
    district,
    districtIndex,
    districtTotal,
    position,
    height,
    width,
    damage,
    profitability,
    brightness: clamp(0.28 + profitability * 1.35, 0.28, 1.72),
    windowDensity: clamp(run.tradeCount / 140, 0.12, 1),
    colors,
    orbitingSymbols: buildOrbitingSymbols(run, seed),
    windows: buildWindows(run, height, seed),
    cracks: buildCracks(damage, seed),
  }
}

export function buildAtlasArtifacts(runs) {
  const districts = buildDistrictLayouts(runs)
  const sortedRuns = [...runs].sort((a, b) => Date.parse(b.completedAt) - Date.parse(a.completedAt))
  const districtCounters = new Map()
  const districtTotals = sortedRuns.reduce((totals, run) => {
    const key = normalizeDistrictKey(run.experiment)
    totals.set(key, (totals.get(key) || 0) + 1)
    return totals
  }, new Map())

  return sortedRuns.map((run, index) => {
    const key = normalizeDistrictKey(run.experiment)
    const district = districts.get(key) || {
      key,
      label: run.experiment || 'Unassigned',
      anchor: { x: 0, z: 0 },
      radius: 7,
      color: '#94a3b8',
    }
    const districtIndex = districtCounters.get(key) || 0
    districtCounters.set(key, districtIndex + 1)
    return buildArtifact(run, index, districtIndex, district, districtTotals.get(key) || 1)
  })
}
