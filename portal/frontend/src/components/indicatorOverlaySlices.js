export function retainActiveIndicatorOverlays(overlays = [], activeIndicatorIds = new Set()) {
  return (overlays || []).filter((entry) => {
    const indicatorId = entry?.ind_id
    if (!indicatorId) return true
    return activeIndicatorIds.has(indicatorId)
  })
}

export function replaceIndicatorOverlaySlice(
  overlays = [],
  {
    indicatorId,
    nextSlice = [],
    activeIndicatorIds = new Set(),
  } = {},
) {
  if (!indicatorId) return retainActiveIndicatorOverlays(overlays, activeIndicatorIds)

  const retained = (overlays || []).filter((entry) => {
    const currentIndicatorId = entry?.ind_id
    if (!currentIndicatorId) return true
    if (!activeIndicatorIds.has(currentIndicatorId)) return false
    return currentIndicatorId !== indicatorId
  })

  return [...retained, ...(Array.isArray(nextSlice) ? nextSlice : [])]
}

