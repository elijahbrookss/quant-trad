import { useCallback } from 'react'
import { coalesce, toFiniteNumber, toSec } from '../chartDataUtils.js'
import { CameraIntents, useViewportController } from './useViewportController.js'

export const useCameraLock = ({
  chartRef,
  levelSeriesRef,
  barSpacingRef,
  latestCandlesRef,
  markerManager,
  debugRanges = false,
}) => {
  const {
    setLocked,
    requestIntent,
    notifyUserInteraction,
    setAnimationActive,
    attachRangeGuards,
    lockedRef,
    resetViewport,
  } = useViewportController({ chartRef, levelSeriesRef, barSpacingRef, latestCandlesRef, debugRanges })

  const lock = useCallback(() => setLocked(true), [setLocked])
  const unlock = useCallback(() => setLocked(false), [setLocked])

  const focusAtTime = useCallback(
    (time, priceHint, candleLookup) => {
      const epoch = toSec(time)
      if (!Number.isFinite(epoch)) return null
      const span = barSpacingRef.current ? Math.max(barSpacingRef.current * 20, 30) : 60
      requestIntent({
        intent: CameraIntents.FOCUS_TIME_SPAN,
        payload: { center: epoch, span },
        reason: 'focus',
        isUser: true,
      })
      const candle = candleLookup.get(epoch)
      const price = toFiniteNumber(coalesce(priceHint, candle?.close, candle?.open, candle?.high, candle?.low))
      const highlight = Number.isFinite(price)
        ? { time: epoch, position: 'aboveBar', shape: 'circle', color: 'rgba(125,211,252,0.9)', text: ' ' }
        : null
      if (highlight && markerManager) {
        markerManager.setLayer('focus', [highlight], { ttlMs: 600 })
        markerManager.flush()
      }
      return highlight
    },
    [barSpacingRef, markerManager, requestIntent],
  )

  const recenter = useCallback(() => {
    lock()
    requestIntent({ intent: CameraIntents.RECENTER, reason: 'recenter', isUser: true })
  }, [lock, requestIntent])

  return {
    lock,
    unlock,
    recenter,
    resetViewport,
    requestIntent,
    attachRangeGuards,
    notifyUserInteraction,
    setAnimationActive,
    cameraLockedRef: lockedRef,
    focusAtTime,
  }
}
