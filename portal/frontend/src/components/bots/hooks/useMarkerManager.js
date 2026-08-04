import { useCallback, useEffect, useRef } from 'react'
import { createSeriesMarkers } from 'lightweight-charts'
import { BOTLENS_DEBUG } from '../chartDataUtils.js'

export const useMarkerManager = ({ seriesRef, markersApiRef, markerCacheRef }) => {
  const layersRef = useRef(new Map())
  const lastSignatureRef = useRef('')

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
    const dedupedByIdentity = new Map()
    merged.forEach((marker) => {
      const identity = marker?.id || [
        marker?.trade_id || '',
        Number.isFinite(marker?.time) ? Number(marker.time) : '',
        marker?.position || '',
        marker?.shape || '',
        marker?.text || '',
      ].join('|')
      if (!dedupedByIdentity.has(identity)) dedupedByIdentity.set(identity, marker)
    })
    const deduped = Array.from(dedupedByIdentity.values())
      .sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
    const signature = deduped
      .map((marker) => {
        const time = Number.isFinite(marker?.time) ? Number(marker.time) : ''
        const position = marker?.position || ''
        const shape = marker?.shape || ''
        const color = marker?.color || ''
        const text = marker?.text || ''
        const id = marker?.id || marker?.trade_id || ''
        return `${time}|${position}|${shape}|${color}|${text}|${id}`
      })
      .join('||')
    if (signature !== lastSignatureRef.current) {
      api.setMarkers(deduped)
      lastSignatureRef.current = signature
    }
    if (markerCacheRef) {
      markerCacheRef.current = deduped
    }
    if (BOTLENS_DEBUG) {
      console.debug('[BotLensChart] marker flush', {
        counts,
        total: deduped.length,
        duplicates_removed: merged.length - deduped.length,
      })
    }
  }, [ensureApi, markerCacheRef])

  useEffect(
    () => () => {
      if (markersApiRef) markersApiRef.current = null
      lastSignatureRef.current = ''
    },
    [markersApiRef],
  )

  return { setLayer, clearLayer, flush, ensureApi }
}
