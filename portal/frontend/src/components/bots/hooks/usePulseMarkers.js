import { useCallback, useEffect, useRef } from 'react'
import { toFiniteNumber, toSec } from '../chartDataUtils.js'

export const usePulseMarkers = ({ seriesRef, markerCacheRef, markersApiRef }) => {
  const pulseLineHandlesRef = useRef([])
  const pulseTimeoutRef = useRef(null)

  const clearPulseArtifacts = useCallback(() => {
    pulseLineHandlesRef.current.forEach((handle) => {
      try {
        seriesRef.current?.removePriceLine(handle)
      } catch {
        /* noop */
      }
    })
    pulseLineHandlesRef.current = []
    if (markersApiRef.current) {
      markersApiRef.current.setMarkers(markerCacheRef.current)
    }
  }, [markerCacheRef, markersApiRef, seriesRef])

  const pulseTradeElements = useCallback(
    (trade) => {
      if (!trade || !seriesRef.current) return
      clearPulseArtifacts()
      if (pulseTimeoutRef.current) {
        clearTimeout(pulseTimeoutRef.current)
        pulseTimeoutRef.current = null
      }
      const entryTime = toSec(trade?.entry_time)
      const stopPrice = toFiniteNumber(trade?.stop_price)
      const targets = Array.from(
        new Set(
          (trade.legs || [])
            .map((leg) => toFiniteNumber(leg?.target_price))
            .filter((value) => Number.isFinite(value)),
        ),
      )
      const pulseMarkers = []
      if (Number.isFinite(entryTime)) {
        pulseMarkers.push({
          time: entryTime,
          position: (trade?.direction || '').toLowerCase() === 'short' ? 'aboveBar' : 'belowBar',
          shape: 'circle',
          color: 'rgba(125,211,252,0.95)',
          text: ' ',
        })
      }
      const lineFor = (price, isTarget = false) => {
        if (!Number.isFinite(price)) return null
        return seriesRef.current.createPriceLine({
          price,
          color: isTarget ? 'rgba(16,185,129,0.9)' : 'rgba(239,68,68,0.9)',
          lineWidth: 2,
          lineStyle: 0,
          axisLabelVisible: false,
        })
      }
      if (Number.isFinite(stopPrice)) {
        const handle = lineFor(stopPrice, false)
        if (handle) pulseLineHandlesRef.current.push(handle)
      }
      targets.forEach((price) => {
        const handle = lineFor(price, true)
        if (handle) pulseLineHandlesRef.current.push(handle)
      })
      if (pulseMarkers.length && markersApiRef.current) {
        markersApiRef.current.setMarkers(
          [...markerCacheRef.current, ...pulseMarkers].sort((a, b) => (a.time ?? 0) - (b.time ?? 0)),
        )
      }
      pulseTimeoutRef.current = setTimeout(() => {
        clearPulseArtifacts()
        pulseTimeoutRef.current = null
      }, 450)
    },
    [clearPulseArtifacts, markerCacheRef, markersApiRef, seriesRef],
  )

  useEffect(() => {
    return () => {
      if (pulseTimeoutRef.current) {
        clearTimeout(pulseTimeoutRef.current)
        pulseTimeoutRef.current = null
      }
      clearPulseArtifacts()
    }
  }, [clearPulseArtifacts])

  return { pulseTradeElements, clearPulseArtifacts }
}

