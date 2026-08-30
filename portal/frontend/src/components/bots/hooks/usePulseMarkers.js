import { useCallback, useEffect, useRef } from 'react'
import { toFiniteNumber, toSec } from '../chartDataUtils.js'
import { buildTradeFocusPulseMarkers } from './pulseMarkerArtifacts.js'
import { isClosedTrade } from './tradeMarkerArtifacts.js'

export { buildTradeFocusPulseMarkers } from './pulseMarkerArtifacts.js'

export const usePulseMarkers = ({ seriesRef, markerManager, latestCandlesRef }) => {
  const pulseLineHandlesRef = useRef([])
  const pulseTimeoutRef = useRef(null)
  const pulseIntervalRef = useRef(null)

  const clearPulseArtifacts = useCallback(() => {
    if (pulseTimeoutRef.current) clearTimeout(pulseTimeoutRef.current)
    if (pulseIntervalRef.current) clearInterval(pulseIntervalRef.current)
    pulseTimeoutRef.current = null
    pulseIntervalRef.current = null
    pulseLineHandlesRef.current.forEach((handle) => {
      try {
        seriesRef.current?.removePriceLine(handle)
      } catch {
        /* noop */
      }
    })
    pulseLineHandlesRef.current = []
    markerManager?.clearLayer('pulse')
    markerManager?.flush()
  }, [markerManager, seriesRef])

  const pulseTradeElements = useCallback(
    (trade) => {
      if (!trade || !seriesRef.current) return
      clearPulseArtifacts()
      if (isClosedTrade(trade)) {
        let phase = 0
        const renderPulse = () => {
          markerManager?.setLayer(
            'pulse',
            buildTradeFocusPulseMarkers(trade, latestCandlesRef?.current || [], phase),
          )
          markerManager?.flush()
          phase += 1
        }
        renderPulse()
        pulseIntervalRef.current = setInterval(renderPulse, 180)
        pulseTimeoutRef.current = setTimeout(clearPulseArtifacts, 2200)
        return
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
          color: isTarget ? 'rgba(16,185,129,0.85)' : 'rgba(239,68,68,0.85)',
          lineWidth: isTarget ? 2 : 2.5,
          lineStyle: isTarget ? 0 : 2,
          axisLabelVisible: true,
          axisLabelColor: isTarget ? 'rgba(16,185,129,0.95)' : 'rgba(239,68,68,0.95)',
          axisLabelTextColor: '#0b1620',
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
      if (pulseMarkers.length) {
        markerManager?.setLayer('pulse', pulseMarkers, { ttlMs: 450 })
        markerManager?.flush()
      }
      pulseTimeoutRef.current = setTimeout(() => {
        clearPulseArtifacts()
      }, 450)
    },
    [clearPulseArtifacts, latestCandlesRef, markerManager, seriesRef],
  )

  useEffect(() => {
    return () => {
      clearPulseArtifacts()
    }
  }, [clearPulseArtifacts])

  return { pulseTradeElements, clearPulseArtifacts }
}
