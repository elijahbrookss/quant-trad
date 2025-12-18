import { useCallback, useEffect, useRef } from 'react'
import { createSeriesMarkers } from 'lightweight-charts'

export const useMarkerManager = ({ seriesRef, markersApiRef, markerCacheRef }) => {
  const layersRef = useRef(new Map())

  const ensureApi = useCallback(() => {
    if (markersApiRef.current) return markersApiRef.current
    if (!seriesRef.current) return null
    markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
    return markersApiRef.current
  }, [markersApiRef, seriesRef])

  const setLayer = useCallback(
    (name, markers = [], options = {}) => {
      if (!name) return
      const normalized = Array.isArray(markers) ? markers.filter(Boolean) : []
      layersRef.current.set(name, {
        markers: normalized,
        signature: options.signature ?? null,
        createdAt: performance.now(),
        ttlMs: options.ttlMs ?? null,
      })
    },
    [],
  )

  const clearLayer = useCallback((name) => {
    layersRef.current.delete(name)
  }, [])

  const flush = useCallback(() => {
    const api = ensureApi()
    if (!api) return
    const now = performance.now()
    const merged = []
    const counts = {}
    Array.from(layersRef.current.entries()).forEach(([name, layer]) => {
      const expired = layer.ttlMs && layer.createdAt + layer.ttlMs < now
      if (expired) {
        layersRef.current.delete(name)
        return
      }
      counts[name] = (layer.markers || []).length
      merged.push(...(layer.markers || []))
    })
    merged.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
    api.setMarkers(merged)
    if (markerCacheRef) {
      markerCacheRef.current = merged
    }
    console.debug('[BotLensChart] marker flush', { counts, total: merged.length })
  }, [ensureApi, markerCacheRef])

  useEffect(() => () => markersApiRef && (markersApiRef.current = null), [markersApiRef])

  return { setLayer, clearLayer, flush, ensureApi }
}
