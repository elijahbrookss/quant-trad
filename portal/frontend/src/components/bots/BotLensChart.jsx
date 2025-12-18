import { useEffect, useMemo, useRef, useCallback } from 'react'
import { useChartState } from '../../contexts/ChartStateContext.jsx'
import { buildCandleLookup, normalizeCandles, toSec } from './chartDataUtils.js'
import { useCameraLock } from './hooks/useCameraLock.js'
import { useOverlaySync } from './hooks/useOverlaySync.js'
import { useTradeMarkers } from './hooks/useTradeMarkers.js'
import { useBotLensChartCore } from './hooks/useBotLensChartCore.js'
import { usePulseMarkers } from './hooks/usePulseMarkers.js'
import { useMarkerTooltip } from './hooks/useMarkerTooltip.js'
import { MarkerTooltip } from './MarkerTooltip.jsx'

const chartOptions = {
  layout: {
    textColor: '#d4d7e1',
    background: { type: 'solid', color: '#10121a' },
  },
  grid: {
    vertLines: { color: 'rgba(150, 150, 150, 0.05)' },
    horzLines: { color: 'rgba(150, 150, 150, 0.05)' },
  },
  timeScale: { borderVisible: false },
  rightPriceScale: { borderVisible: false },
}

const seriesOptions = {
  upColor: '#34d399',
  downColor: '#f97316',
  borderVisible: false,
  wickUpColor: '#34d399',
  wickDownColor: '#f97316',
  priceLineVisible: false,
}

export function BotLensChart({ chartId, candles = [], trades = [], overlays = [], playbackSpeed = 10 }) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)
  const seriesRef = useRef(null)
  const levelSeriesRef = useRef(null)
  const paneMgrRef = useRef(null)
  const markersApiRef = useRef(null)
  const overlayHandlesRef = useRef({ priceLines: [] })
  const barSpacingRef = useRef(null)
  const latestCandlesRef = useRef([])
  const markerCacheRef = useRef([])
  const prevPriceLinesRef = useRef([])
  const markerDetailsRef = useRef([])
  const prevCandleDataRef = useRef([])
  const candleAnimationRef = useRef(null)
  const diagLoggedRef = useRef(false)
  const frameSampleRef = useRef({ total: 0, count: 0, logged: false })
  const { registerChart } = useChartState()

  const resolvedCandles = Array.isArray(candles) ? candles : []
  const resolvedTrades = Array.isArray(trades) ? trades : []
  const resolvedOverlays = Array.isArray(overlays) ? overlays : []
  const instantPlayback = Number(playbackSpeed) <= 0

  const candleLookup = useMemo(() => buildCandleLookup(resolvedCandles), [resolvedCandles])
  const candleData = useMemo(() => normalizeCandles(resolvedCandles), [resolvedCandles])

  useEffect(() => {
    latestCandlesRef.current = candleData
  }, [candleData])

  const activeTradeAtLastCandle = useMemo(() => {
    const lastTime = candleData[candleData.length - 1]?.time
    if (!Number.isFinite(lastTime)) return false
    return resolvedTrades.some((trade) => {
      const entry = toSec(trade?.entry_time)
      if (!Number.isFinite(entry) || entry > lastTime) return false
      const closed = toSec(trade?.closed_at)
      const legs = Array.isArray(trade?.legs) ? trade.legs : []
      const openLeg = legs.some((leg) => {
        const exit = toSec(leg?.exit_time)
        if (!Number.isFinite(exit)) return true
        return exit >= lastTime
      })
      if (openLeg) return true
      if (!Number.isFinite(closed)) return true
      return closed >= lastTime
    })
  }, [candleData, resolvedTrades])

  useEffect(() => {
    if (!candleData.length) {
      diagLoggedRef.current = false
      return
    }
    let previous = null
    let violation = null
    for (let idx = 0; idx < candleData.length; idx += 1) {
      const current = candleData[idx]
      if (!Number.isFinite(current?.time)) {
        continue
      }
      if (previous !== null && current.time < previous) {
        violation = { index: idx, prev: previous, current: current.time }
        break
      }
      previous = current.time
    }
    if (violation) {
      console.error('[BotLensChart] Candle order violation', {
        chartId,
        count: candleData.length,
        ...violation,
      })
      return
    }
    if (!diagLoggedRef.current) {
      const first = candleData[0]?.time
      const last = candleData[candleData.length - 1]?.time
      console.debug('[BotLensChart] Candle range', {
        chartId,
        count: candleData.length,
        first,
        last,
      })
      diagLoggedRef.current = true
    }
  }, [candleData, chartId])

  const { markers: tradeMarkers, tooltips: tradeMarkerTooltips, regions: tradeRegions, priceLines: tradePriceLines } =
    useTradeMarkers(resolvedTrades, candleLookup, candleData)

  const { lock, unlock, recenter, enforceViewport, attachRangeGuards, autoScroll, focusAtTime } = useCameraLock({
    chartRef,
    levelSeriesRef,
    barSpacingRef,
    latestCandlesRef,
  })

  const { pulseTradeElements, clearPulseArtifacts } = usePulseMarkers({
    seriesRef,
    markerCacheRef,
    markersApiRef,
  })

  useBotLensChartCore({
    chartId,
    containerRef,
    chartOptions,
    seriesOptions,
    registerChart,
    candleLookup,
    focusAtTime,
    pulseTrade: pulseTradeElements,
    clearPulse: clearPulseArtifacts,
    recenter,
    attachRangeGuards,
    markerCacheRef,
    markerDetailsRef,
    chartRef,
    seriesRef,
    levelSeriesRef,
    paneMgrRef,
    markersApiRef,
    overlayHandlesRef,
    barSpacingRef,
  })

  const syncOverlays = useOverlaySync({
    seriesRef,
    paneMgrRef,
    barSpacingRef,
    markersApiRef,
    overlayHandlesRef,
    markerCacheRef,
    markerDetailsRef,
    prevPriceLinesRef,
    applyViewport: enforceViewport,
  })

  const markerTooltip = useMarkerTooltip({ chartRef, markerDetailsRef })

  const animateCandle = useCallback(
    (from, to, speed) => {
      if (!seriesRef.current || !to || !from) return
      if (candleAnimationRef.current) {
        cancelAnimationFrame(candleAnimationRef.current)
        candleAnimationRef.current = null
      }
      const baseDuration = 380
      const safeSpeed = Number.isFinite(speed) ? Math.max(speed, 0.25) : 1
      const duration = Math.min(Math.max(baseDuration / safeSpeed, 80), 600)
      const start = performance.now()
      const frame = (now) => {
        const progress = Math.min(1, (now - start) / duration)
        const interp = (a, b) => a + (b - a) * progress
        const current = {
          time: to.time,
          open: interp(from.open, to.open),
          high: interp(from.high, to.high),
          low: interp(from.low, to.low),
          close: interp(from.close, to.close),
        }
        seriesRef.current.update(current)
        if (progress < 1) {
          candleAnimationRef.current = requestAnimationFrame(frame)
        } else {
          candleAnimationRef.current = null
        }
      }
      candleAnimationRef.current = requestAnimationFrame(frame)
    },
    [seriesRef],
  )

  useEffect(
    () => () => {
      if (candleAnimationRef.current) {
        cancelAnimationFrame(candleAnimationRef.current)
        candleAnimationRef.current = null
      }
    },
    [],
  )

  useEffect(() => {
    if (!seriesRef.current) return
    const previous = prevCandleDataRef.current || []
    const next = candleData
    const prevLast = previous[previous.length - 1]
    const nextLast = next[next.length - 1]
    const prevLastTime = prevLast?.time
    const nextLastTime = nextLast?.time

    const timeAdvanced = Number.isFinite(prevLastTime) && Number.isFinite(nextLastTime) && nextLastTime > prevLastTime
    const isAppend = timeAdvanced && next.length === previous.length + 1
    const isSameCandle = next.length === previous.length && Number.isFinite(nextLastTime) && nextLastTime === prevLastTime
    const historyRewound =
      Number.isFinite(prevLastTime) && Number.isFinite(nextLastTime) && (next.length < previous.length || nextLastTime < prevLastTime)
    const longJump = next.length > previous.length + 1
    const requiresReset = !previous.length || !next.length || historyRewound || longJump
    const shouldAnimate = isSameCandle && activeTradeAtLastCandle && !instantPlayback

    const sample = frameSampleRef.current
    const start = performance.now()

    if (requiresReset) {
      seriesRef.current.setData(next)
      frameSampleRef.current = { total: 0, count: 0, logged: false }
      if (!previous.length || timeAdvanced) {
        autoScroll(next)
      }
    } else if (shouldAnimate) {
      const prevMatch = previous.find((candle) => Number.isFinite(candle?.time) && candle.time === nextLastTime)
      animateCandle(prevMatch, nextLast, playbackSpeed)
    } else if (isAppend) {
      seriesRef.current.update(nextLast)
      if (timeAdvanced) autoScroll(next)
    } else if (isSameCandle) {
      seriesRef.current.update(nextLast)
    } else {
      seriesRef.current.setData(next)
      if (timeAdvanced) autoScroll(next)
    }

    const duration = performance.now() - start
    sample.total += duration
    sample.count += 1
    if (!sample.logged && sample.count >= 30 && next.length >= 200) {
      const avgMs = Number((sample.total / sample.count).toFixed(2))
      console.debug('[BotLensChart] Candle frame average', { chartId, samples: sample.count, avgMs, candles: next.length })
      sample.logged = true
    }

    prevCandleDataRef.current = next
  }, [activeTradeAtLastCandle, animateCandle, autoScroll, candleData, instantPlayback, playbackSpeed, seriesRef])

  useEffect(() => {
    const last = candleData[candleData.length - 1]?.time ?? null
    const prev = candleData[candleData.length - 2]?.time ?? null
    if (Number.isFinite(last) && Number.isFinite(prev)) {
      const spacing = last - prev
      if (Number.isFinite(spacing) && spacing > 0) {
        barSpacingRef.current = spacing
      }
    }
    paneMgrRef.current?.updateVABlockContext({
      lastSeriesTime: last,
      barSpacing: barSpacingRef.current,
    })
  }, [barSpacingRef, candleData])

  useEffect(() => {
    syncOverlays({
      overlayPayloads: resolvedOverlays,
      tradeMarkers,
      tradeTooltips: tradeMarkerTooltips,
      tradeRegions,
      tradePriceLines,
      candleData,
    })
  }, [candleData, resolvedOverlays, syncOverlays, tradeMarkerTooltips, tradeMarkers, tradePriceLines, tradeRegions])

  useEffect(() => {
    enforceViewport(candleData, [])
  }, [candleData, enforceViewport])

  return (
    <div
      ref={containerRef}
      className="relative h-[360px] w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0f1118]"
      onMouseEnter={lock}
      onMouseLeave={unlock}
    >
      <MarkerTooltip markerTooltip={markerTooltip} />
    </div>
  )
}
