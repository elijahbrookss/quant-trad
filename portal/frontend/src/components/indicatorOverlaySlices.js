const overlaySource = (entry) => {
  const source = entry?.source
  return typeof source === 'string' && source.trim() ? source.trim() : 'indicator'
}

const retainIdsForSource = (retainBySource = {}, source) => {
  const direct = retainBySource?.[source]
  if (direct instanceof Set) return direct
  if (Array.isArray(direct)) return new Set(direct)
  const fallback = retainBySource?.indicator
  if (fallback instanceof Set) return fallback
  if (Array.isArray(fallback)) return new Set(fallback)
  return new Set()
}

export function retainIndicatorArtifacts(overlays = [], retainBySource = {}) {
  return (overlays || []).filter((entry) => {
    const indicatorId = entry?.ind_id
    if (!indicatorId) return true
    return retainIdsForSource(retainBySource, overlaySource(entry)).has(indicatorId)
  })
}

export function replaceIndicatorArtifactSlice(
  overlays = [],
  {
    indicatorId,
    source = 'indicator',
    nextSlice = [],
    retainBySource = {},
  } = {},
) {
  if (!indicatorId) return retainIndicatorArtifacts(overlays, retainBySource)

  const retained = (overlays || []).filter((entry) => {
    const currentIndicatorId = entry?.ind_id
    if (!currentIndicatorId) return true
    const currentSource = overlaySource(entry)
    const allowedIds = retainIdsForSource(retainBySource, currentSource)
    if (!allowedIds.has(currentIndicatorId)) return false
    return currentIndicatorId !== indicatorId || currentSource !== source
  })

  return [...retained, ...(Array.isArray(nextSlice) ? nextSlice : [])]
}

export function writeIndicatorArtifactSliceCache(
  cache = {},
  {
    indicatorId,
    source = 'indicator',
    nextSlice = [],
  } = {},
) {
  if (!indicatorId) return cache || {}
  return {
    ...(cache || {}),
    [indicatorId]: {
      ...((cache || {})[indicatorId] || {}),
      [source]: Array.isArray(nextSlice) ? nextSlice : [],
    },
  }
}

export function pruneIndicatorArtifactSliceCache(cache = {}, allowedIndicatorIds = null) {
  if (!(allowedIndicatorIds instanceof Set)) return cache || {}
  const next = {}
  for (const [indicatorId, slices] of Object.entries(cache || {})) {
    if (!allowedIndicatorIds.has(indicatorId)) continue
    next[indicatorId] = slices
  }
  return next
}

export function rebuildIndicatorArtifactsFromCache(cache = {}, retainBySource = {}) {
  const overlays = []
  for (const [indicatorId, slices] of Object.entries(cache || {})) {
    for (const [source, slice] of Object.entries(slices || {})) {
      if (!retainIdsForSource(retainBySource, source).has(indicatorId)) continue
      if (!Array.isArray(slice) || !slice.length) continue
      overlays.push(...slice)
    }
  }
  return overlays
}

export function seedIndicatorArtifactSliceCache(cache = {}, overlays = []) {
  let next = cache || {}
  for (const entry of overlays || []) {
    const indicatorId = entry?.ind_id
    if (!indicatorId) continue
    const source = overlaySource(entry)
    if (Array.isArray(next?.[indicatorId]?.[source])) continue
    const grouped = (overlays || []).filter((candidate) => (
      candidate?.ind_id === indicatorId && overlaySource(candidate) === source
    ))
    next = writeIndicatorArtifactSliceCache(next, {
      indicatorId,
      source,
      nextSlice: grouped,
    })
  }
  return next
}
