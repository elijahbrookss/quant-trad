import { useCallback, useRef } from 'react'

const deriveSpacing = (candles = [], barSpacingRef) => {
  const last = candles[candles.length - 1]
  const prev = candles[candles.length - 2]
  const lastTime = last?.time
  const prevTime = prev?.time
  if (Number.isFinite(lastTime) && Number.isFinite(prevTime)) {
    const spacing = lastTime - prevTime
    if (Number.isFinite(spacing) && spacing > 0) return spacing
  }
  if (Number.isFinite(barSpacingRef?.current)) return barSpacingRef.current
  return null
}

const computeSeriesRange = (candles = [], spacing, minBars, maxBars) => {
  const lastIndex = candles.length - 1
  const lastTime = candles[lastIndex]?.time
  const barsToShow = Math.min(maxBars, Math.max(minBars, candles.length))
  const fromIndex = Math.max(0, lastIndex - barsToShow + 1)
  const fromTime = candles[fromIndex]?.time
  const padTime = candles.length >= barsToShow ? 0 : Math.max(spacing ?? 0, 0)
  const rawTo = Number.isFinite(lastTime) ? lastTime + padTime : lastIndex + (padTime ? 1 : 0)
  const rawFrom = Number.isFinite(fromTime) ? fromTime : fromIndex
  const safeSpacing = Math.max(spacing ?? 1, 1)
  const to = rawTo <= rawFrom ? rawFrom + safeSpacing : rawTo
  const logicalFrom = Math.max(0, fromIndex)
  const logicalTo = lastIndex + (padTime ? 1 : 0)
  const logicalRange = { from: logicalFrom, to: Math.max(logicalFrom + 1, logicalTo) }
  if (Number.isFinite(rawFrom) && Number.isFinite(to)) {
    return { range: { from: rawFrom, to }, logicalRange }
  }
  return { range: null, logicalRange }
}

const computeSegmentRange = (candles = [], segments = [], spacing) => {
  if (!segments.length) return null
  const lastTime = candles[candles.length - 1]?.time
  const candidateTimes = segments
    .flatMap((segment) => [segment?.x1, segment?.x2])
    .filter((value) => Number.isFinite(value))
  if (!candidateTimes.length || !Number.isFinite(lastTime)) return null
  const minTime = Math.min(...candidateTimes, lastTime)
  const maxTime = Math.max(...candidateTimes, lastTime)
  if (!Number.isFinite(minTime) || !Number.isFinite(maxTime) || maxTime <= minTime) return null
  const span = Math.max(maxTime - minTime, spacing ?? 60)
  const pad = Math.max(span * 0.05, spacing ?? 0)
  return { from: minTime - pad, to: maxTime + pad }
}

const applyRange = (chartRef, range, logicalRange) => {
  const ts = chartRef.current?.timeScale?.()
  if (!ts) return
  if (range && Number.isFinite(range.from) && Number.isFinite(range.to)) {
    ts.setVisibleRange(range)
  } else if (logicalRange) {
    ts.setVisibleLogicalRange(logicalRange)
  }
}

const buildGhostPoints = (candles = [], segments = []) => {
  const ghostPoints = []
  const lastIndex = candles.length - 1
  const lastTime = candles[lastIndex]?.time
  if (Number.isFinite(lastTime)) {
    const lastCandle = candles[lastIndex]
    ghostPoints.push({ time: lastTime - 1, value: lastCandle?.low ?? lastCandle?.close ?? 0 })
    ghostPoints.push({ time: lastTime, value: lastCandle?.high ?? lastCandle?.close ?? 0 })
  }
  segments
    .flatMap((segment) => [segment?.y1, segment?.y2])
    .filter((price) => Number.isFinite(price))
    .forEach((price, idx) => {
      if (!Number.isFinite(lastTime)) return
      ghostPoints.push({ time: lastTime + idx + 1, value: price })
    })
  ghostPoints.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
  return ghostPoints
}

export const useViewportManager = ({ chartRef, levelSeriesRef, barSpacingRef, latestCandlesRef }) => {
  const cameraLockedRef = useRef(true)
  const userInteractedRef = useRef(false)

  const lock = useCallback(() => {
    cameraLockedRef.current = true
  }, [])

  const unlock = useCallback(() => {
    cameraLockedRef.current = false
  }, [])

  const recenter = useCallback(() => {
    const ts = chartRef.current?.timeScale?.()
    if (!ts) return

    lock()

    const candles = latestCandlesRef?.current || []
    const last = candles[candles.length - 1]?.time
    const first = candles[0]?.time
    if (Number.isFinite(last) && Number.isFinite(first)) {
      const span = Math.max(90, Math.round((last - first) / 4))
      ts.setVisibleRange({ from: last - span, to: last })
      ts.scrollToPosition(0, false)
    } else {
      ts.scrollToRealTime()
    }
  }, [chartRef, latestCandlesRef, lock])

  const applyViewport = useCallback(
    (candleData = [], tradeSegments = []) => {
      if (!chartRef.current || candleData.length === 0) return
      if (!cameraLockedRef.current || userInteractedRef.current) return
      const spacing = deriveSpacing(candleData, barSpacingRef)
      const overlayRange = computeSegmentRange(candleData, tradeSegments, spacing)
      const { range, logicalRange } = computeSeriesRange(candleData, spacing, 30, 200)
      applyRange(chartRef, overlayRange ?? range, logicalRange)
      if (levelSeriesRef?.current) {
        levelSeriesRef.current.setData(buildGhostPoints(candleData, tradeSegments))
      }
    },
    [barSpacingRef, chartRef, levelSeriesRef],
  )

  const autoScroll = useCallback(
    (candleData = []) => {
      if (!chartRef.current || candleData.length === 0) return
      const spacing = deriveSpacing(candleData, barSpacingRef)
      const { range, logicalRange } = computeSeriesRange(candleData, spacing, 30, 80)
      if (cameraLockedRef.current && !userInteractedRef.current) {
        applyRange(chartRef, range, logicalRange)
      } else {
        chartRef.current.timeScale().fitContent()
      }
    },
    [barSpacingRef, chartRef],
  )

  const attachRangeGuards = useCallback(
    (containerEl) => {
      if (!containerEl || !chartRef.current) return () => {}
      const ts = chartRef.current.timeScale()
      const handleVisibleRangeChange = () => {
        if (userInteractedRef.current) {
          unlock()
        }
        userInteractedRef.current = false
      }
      ts.subscribeVisibleLogicalRangeChange(handleVisibleRangeChange)
      const markInteraction = () => {
        userInteractedRef.current = true
      }
      containerEl.addEventListener('mousedown', markInteraction)
      containerEl.addEventListener('touchstart', markInteraction)
      return () => {
        ts.unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange)
        containerEl.removeEventListener('mousedown', markInteraction)
        containerEl.removeEventListener('touchstart', markInteraction)
      }
    },
    [chartRef, unlock],
  )

  return {
    lock,
    unlock,
    recenter,
    applyViewport,
    autoScroll,
    attachRangeGuards,
    cameraLockedRef,
    userInteractedRef,
  }
}
