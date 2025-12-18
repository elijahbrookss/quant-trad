import { useCallback } from 'react'
import { coalesce, toFiniteNumber, toSec } from '../chartDataUtils.js'
import { useViewportManager } from './useViewportManager.js'

export const useCameraLock = ({
  chartRef,
  levelSeriesRef,
  barSpacingRef,
  latestCandlesRef,
}) => {
  const {
    lock,
    unlock,
    recenter,
    applyViewport,
    autoScroll,
    attachRangeGuards,
    cameraLockedRef,
  } = useViewportManager({ chartRef, levelSeriesRef, barSpacingRef, latestCandlesRef })

  const focusAtTime = useCallback(
    (time, priceHint, candleLookup) => {
      if (!chartRef.current) return null
      const epoch = toSec(time)
      if (!Number.isFinite(epoch)) return null
      const ts = chartRef.current.timeScale()
      const span = barSpacingRef.current ? Math.max(barSpacingRef.current * 20, 30) : 60
      ts.setVisibleRange({ from: epoch - span, to: epoch + span })
      const candle = candleLookup.get(epoch)
      const price = toFiniteNumber(coalesce(priceHint, candle?.close, candle?.open, candle?.high, candle?.low))
      const highlight = Number.isFinite(price)
        ? { time: epoch, position: 'aboveBar', shape: 'circle', color: 'rgba(125,211,252,0.9)', text: ' ' }
        : null
      return highlight
    },
    [barSpacingRef, chartRef],
  )

  return {
    lock,
    unlock,
    recenter,
    enforceViewport: applyViewport,
    attachRangeGuards,
    autoScroll,
    focusAtTime,
    cameraLockedRef,
  }
}
