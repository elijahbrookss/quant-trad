import { useCallback, useRef } from 'react'
import { coalesce, toFiniteNumber, toSec } from '../chartDataUtils.js'

export const useCameraLock = ({
  chartRef,
  levelSeriesRef,
  barSpacingRef,
  latestCandlesRef,
}) => {
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

    const candles = latestCandlesRef.current || []
    const last = candles[candles.length - 1]?.time
    const first = candles[0]?.time
    if (Number.isFinite(last) && Number.isFinite(first)) {
      const span = Math.max(90, Math.round((last - first) / 4))
      ts.setVisibleRange({ from: last - span, to: last })
      ts.scrollToPosition(0, false)
    } else {
      ts.scrollToRealTime()
    }
  }, [latestCandlesRef, chartRef, lock])

  const enforceViewport = useCallback(
    (candleData = [], tradeSegments = []) => {
      if (!chartRef.current || candleData.length === 0) return
      const timeScale = chartRef.current.timeScale()
      const lastIndex = candleData.length - 1
      const lastTime = candleData[lastIndex]?.time
      const normalizedSegments = (tradeSegments || []).filter(
        (segment) => Number.isFinite(segment?.x1) || Number.isFinite(segment?.x2),
      )
      let appliedRange = false
      if (normalizedSegments.length && Number.isFinite(lastTime)) {
        const candidateTimes = normalizedSegments
          .flatMap((segment) => [segment.x1, segment.x2])
          .filter((value) => Number.isFinite(value))
        if (candidateTimes.length) {
          const minTime = Math.min(...candidateTimes, lastTime)
          const maxTime = Math.max(...candidateTimes, lastTime)
          if (Number.isFinite(minTime) && Number.isFinite(maxTime) && maxTime > minTime) {
            const span = Math.max(maxTime - minTime, 60)
            const pad = span * 0.05
            timeScale.setVisibleRange({ from: minTime - pad, to: maxTime + pad })
            appliedRange = true
          }
        }
      }
      if (!appliedRange) {
        const barsToShow = Math.min(200, Math.max(30, candleData.length))
        const from = Math.max(0, lastIndex - barsToShow + 1)
        const to = lastIndex + 5
        timeScale.setVisibleLogicalRange({ from, to })
      }
      if (!levelSeriesRef.current) return
      const overlayPrices = normalizedSegments.flatMap((segment) => {
        const prices = []
        if (Number.isFinite(segment?.y1)) prices.push(segment.y1)
        if (Number.isFinite(segment?.y2)) prices.push(segment.y2)
        return prices
      })
      const ghostPoints = []
      if (Number.isFinite(lastTime)) {
        const lastCandle = candleData[lastIndex]
        ghostPoints.push({ time: lastTime - 1, value: lastCandle?.low ?? lastCandle?.close ?? 0 })
        ghostPoints.push({ time: lastTime, value: lastCandle?.high ?? lastCandle?.close ?? 0 })
      }
      overlayPrices.forEach((price, idx) => {
        if (!Number.isFinite(lastTime)) return
        ghostPoints.push({ time: lastTime + idx + 1, value: price })
      })
      ghostPoints.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
      levelSeriesRef.current.setData(ghostPoints)
    },
    [chartRef, levelSeriesRef],
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

  const autoScroll = useCallback(
    (candleData = []) => {
      if (!chartRef.current || candleData.length === 0) return
      const timeScale = chartRef.current.timeScale()
      const lastIndex = candleData.length - 1
      if (cameraLockedRef.current) {
        const barsToShow = Math.min(80, Math.max(30, candleData.length))
        const from = Math.max(0, lastIndex - barsToShow + 1)
        const to = lastIndex + 5
        timeScale.setVisibleLogicalRange({ from, to })
      } else {
        timeScale.fitContent()
      }
    },
    [chartRef],
  )

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
    enforceViewport,
    attachRangeGuards,
    autoScroll,
    focusAtTime,
    cameraLockedRef,
  }
}
