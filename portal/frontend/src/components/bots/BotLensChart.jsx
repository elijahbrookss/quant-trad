import { useEffect, useMemo, useRef, useCallback, useState } from 'react'
import { createChart, CandlestickSeries, LineSeries } from 'lightweight-charts'
import { useChartState } from '../../contexts/ChartStateContext.jsx'
import { PaneViewManager } from '../../chart/paneViews/factory.js'
import { buildCandleLookup, normalizeCandles, toFiniteNumber, toSec } from './chartDataUtils.js'
import { useCameraLock } from './hooks/useCameraLock.js'
import { useOverlaySync } from './hooks/useOverlaySync.js'
import { useTradeMarkers } from './hooks/useTradeMarkers.js'

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

const MarkerTooltip = ({ markerTooltip }) => {
  if (!markerTooltip?.entries?.length) return null
  return (
    <div
      className="pointer-events-none absolute z-10 rounded-lg border border-white/10 bg-black/70 px-3 py-2 text-xs text-white shadow-lg backdrop-blur"
      style={{ left: markerTooltip.x, top: markerTooltip.y - 12 }}
    >
      <p className="text-[11px] uppercase tracking-[0.25em] text-slate-300">TP / SL breakdown</p>
      <ul className="mt-1 space-y-0.5 text-slate-100">
        {markerTooltip.entries.map((line, idx) => (
          <li key={`${line}-${idx}`} className="whitespace-nowrap">
            {line}
          </li>
        ))}
      </ul>
    </div>
  )
}

export function BotLensChart({ chartId, candles = [], trades = [], overlays = [], playbackSpeed = 10 }) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)
  const seriesRef = useRef(null)
  const resizeObserverRef = useRef(null)
  const { registerChart } = useChartState()
  const markersApiRef = useRef(null)
  const paneMgrRef = useRef(null)
  const overlayHandlesRef = useRef({ priceLines: [] })
  const barSpacingRef = useRef(null)
  const levelSeriesRef = useRef(null)
  const latestCandlesRef = useRef([])
  const markerCacheRef = useRef([])
  const prevPriceLinesRef = useRef([])
  const focusTimeoutRef = useRef(null)
  const pulseTimeoutRef = useRef(null)
  const markerDetailsRef = useRef([])
  const prevCandleDataRef = useRef([])
  const candleAnimationRef = useRef(null)
  const pulseLineHandlesRef = useRef([])
  const diagLoggedRef = useRef(false)
  const [markerTooltip, setMarkerTooltip] = useState(null)

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

  const { lock, unlock, recenter, enforceViewport, attachRangeGuards, autoScroll, focusAtTime, cameraLockedRef } =
    useCameraLock({
      chartRef,
      levelSeriesRef,
      barSpacingRef,
      latestCandlesRef,
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
  }, [])

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
    [clearPulseArtifacts],
  )

  const animateCandle = useCallback((from, to) => {
    if (!seriesRef.current || !to) return
    if (candleAnimationRef.current) {
      cancelAnimationFrame(candleAnimationRef.current)
      candleAnimationRef.current = null
    }
    const duration = 380
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
  }, [])

  useEffect(() => {
    const el = containerRef.current
    if (!el || chartRef.current) return
    const chart = createChart(el, {
      ...chartOptions,
      width: el.clientWidth,
      height: el.clientHeight || 360,
    })
    const series = chart.addSeries(CandlestickSeries, seriesOptions)
    const levelSeries = chart.addSeries(LineSeries, {
      color: 'rgba(0,0,0,0)',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    })
    chartRef.current = chart
    seriesRef.current = series
    levelSeriesRef.current = levelSeries
    paneMgrRef.current = new PaneViewManager(chart)
    registerChart?.(chartId, {
      get chart() {
        return chartRef.current
      },
      get series() {
        return seriesRef.current
      },
      focusAtTime: (time, priceHint) => {
        const highlight = focusAtTime(time, priceHint, candleLookup)
        if (!highlight || !markersApiRef.current) return
        const combined = [...markerCacheRef.current, highlight].sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
        markersApiRef.current.setMarkers(combined)
        if (focusTimeoutRef.current) {
          clearTimeout(focusTimeoutRef.current)
        }
        focusTimeoutRef.current = setTimeout(() => {
          markersApiRef.current?.setMarkers?.(markerCacheRef.current)
          focusTimeoutRef.current = null
        }, 600)
      },
      pulseTrade: pulseTradeElements,
      clearPulse: clearPulseArtifacts,
      zoomIn: () => chartRef.current?.timeScale?.().zoomIn?.(),
      zoomOut: () => chartRef.current?.timeScale?.().zoomOut?.(),
      centerView: recenter,
    })

    const cleanupGuards = attachRangeGuards(el)

    resizeObserverRef.current = new ResizeObserver(([entry]) => {
      const rect = entry?.contentRect
      if (!rect || !chartRef.current) return
      chartRef.current.applyOptions({ width: rect.width, height: rect.height })
    })
    resizeObserverRef.current.observe(el)

    return () => {
      cleanupGuards?.()
      resizeObserverRef.current?.disconnect()
      resizeObserverRef.current = null
      if (focusTimeoutRef.current) {
        clearTimeout(focusTimeoutRef.current)
        focusTimeoutRef.current = null
      }
      if (pulseTimeoutRef.current) {
        clearTimeout(pulseTimeoutRef.current)
        pulseTimeoutRef.current = null
      }
      if (candleAnimationRef.current) {
        cancelAnimationFrame(candleAnimationRef.current)
        candleAnimationRef.current = null
      }
      clearPulseArtifacts()
      markersApiRef.current?.setMarkers?.([])
      markersApiRef.current = null
      paneMgrRef.current?.destroy()
      paneMgrRef.current = null
      overlayHandlesRef.current.priceLines = []
      markerCacheRef.current = []
      if (levelSeriesRef.current) {
        try {
          chart.removeSeries(levelSeriesRef.current)
        } catch {
          /* ignore */
        }
      }
      levelSeriesRef.current = null
      seriesRef.current = null
      chartRef.current?.remove()
      chartRef.current = null
    }
  }, [attachRangeGuards, chartId, clearPulseArtifacts, focusAtTime, pulseTradeElements, recenter, registerChart, candleLookup])

  useEffect(() => {
    if (!seriesRef.current) return
    const previous = prevCandleDataRef.current || []
    const next = candleData
    const prevLast = previous[previous.length - 1]
    const nextLast = next[next.length - 1]
    const prevMatch = nextLast ? previous.find((candle) => Number.isFinite(candle?.time) && candle.time === nextLast.time) : null

    const isSameCandle = prevMatch && nextLast && prevMatch.time === nextLast.time
    const shouldAnimate = isSameCandle && activeTradeAtLastCandle && !instantPlayback

    if (shouldAnimate) {
      seriesRef.current.update(nextLast)
    } else {
      seriesRef.current.setData(next)
      autoScroll(next)
    }
    prevCandleDataRef.current = next
  }, [activeTradeAtLastCandle, autoScroll, candleData, instantPlayback])

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
  }, [candleData])

  useEffect(() => {
    if (!chartRef.current) return undefined
    const handler = (param) => {
      const epoch = toSec(param?.time)
      if (!Number.isFinite(epoch) || !param?.point) {
        setMarkerTooltip(null)
        return
      }
      const detail = (markerDetailsRef.current || []).find((entry) => entry.time === epoch)
      if (detail) {
        setMarkerTooltip({ x: param.point.x, y: param.point.y, entries: detail.entries })
      } else {
        setMarkerTooltip(null)
      }
    }
    chartRef.current.subscribeCrosshairMove(handler)
    return () => chartRef.current?.unsubscribeCrosshairMove?.(handler)
  }, [])

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
    >
      <MarkerTooltip markerTooltip={markerTooltip} />
    </div>
  )
}
