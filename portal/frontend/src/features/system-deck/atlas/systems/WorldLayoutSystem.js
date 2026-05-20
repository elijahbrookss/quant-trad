const DISTRICT_PRESETS = [
  { key: 'volatility-cartography', label: 'Volatility Cartography', anchor: { x: -18, z: -8 }, radius: 8.5, color: '#38bdf8' },
  { key: 'liquidity-gardens', label: 'Liquidity Gardens', anchor: { x: 4, z: -14 }, radius: 8, color: '#34d399' },
  { key: 'regime-vault', label: 'Regime Vault', anchor: { x: 18, z: 2 }, radius: 7.2, color: '#a78bfa' },
  { key: 'momentum-array', label: 'Momentum Array', anchor: { x: -7, z: 12 }, radius: 7.8, color: '#f59e0b' },
  { key: 'pair-relay', label: 'Pair Relay', anchor: { x: 14, z: 16 }, radius: 7, color: '#2dd4bf' },
  { key: 'failure-archive', label: 'Failure Archive', anchor: { x: -24, z: 14 }, radius: 6.6, color: '#fb7185' },
]

export function normalizeDistrictKey(experiment) {
  return String(experiment || 'unassigned')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '') || 'unassigned'
}

export function formatDistrictLabel(experiment) {
  return String(experiment || 'Unassigned')
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(' ')
}

export function buildDistrictLayouts(runs) {
  const known = new Map(DISTRICT_PRESETS.map((district) => [district.key, district]))
  const keys = Array.from(new Set(runs.map((run) => normalizeDistrictKey(run.experiment))))
  const fallbackKeys = keys.filter((key) => !known.has(key)).sort()

  const fallbackLayouts = fallbackKeys.map((key, index) => {
    const angle = index * Math.PI * 0.62 + Math.PI * 0.18
    const distance = 28 + index * 2.5
    return {
      key,
      label: formatDistrictLabel(key),
      anchor: {
        x: Math.cos(angle) * distance,
        z: Math.sin(angle) * distance,
      },
      radius: 7.4,
      color: '#94a3b8',
    }
  })

  return new Map(
    [...DISTRICT_PRESETS, ...fallbackLayouts]
      .filter((district) => keys.includes(district.key))
      .map((district) => [district.key, district]),
  )
}

export function getDistrictSummaries(artifacts) {
  const summaries = new Map()
  for (const artifact of artifacts) {
    const current = summaries.get(artifact.district.key) || {
      key: artifact.district.key,
      label: artifact.district.label,
      color: artifact.district.color,
      anchor: artifact.district.anchor,
      radius: artifact.district.radius,
      count: 0,
      profitable: 0,
      losing: 0,
      pnl: 0,
    }
    current.count += 1
    current.pnl += artifact.run.pnl
    if (artifact.run.pnl >= 0) current.profitable += 1
    else current.losing += 1
    summaries.set(artifact.district.key, current)
  }
  return Array.from(summaries.values()).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
}
