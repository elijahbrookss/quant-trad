import { useEffect, useMemo, useRef, useCallback } from 'react'
import { createChart, CandlestickSeries, LineSeries, createSeriesMarkers } from 'lightweight-charts'
import { useChartState } from '../../contexts/ChartStateContext.jsx'
import { PaneViewManager } from '../../chart/paneViews/factory.js'
import { adaptPayload, getPaneViewsFor } from '../../chart/indicators/registry.js'

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

const toSec = (value) => {
  if (value == null) return value
  if (typeof value === 'number') {
    return value > 2e10 ? Math.floor(value / 1000) : value
  }
  const ts = Date.parse(value)
  if (Number.isFinite(ts)) {
    return Math.floor(ts / 1000)
  }
  return null
}

const toFiniteNumber = (value) => {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

const coalesce = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null) {
      return value
    }
  }
  return undefined
}

const toRgba = (hex, alpha = 0.16) => {
  if (typeof hex !== 'string') return undefined
  const normalized = hex.trim().replace('#', '')
  if (!(normalized.length === 3 || normalized.length === 6)) return undefined
  const expand = (value) => value.split('').map((c) => c + c).join('')
  const raw = normalized.length === 3 ? expand(normalized) : normalized
  const r = Number.parseInt(raw.slice(0, 2), 16)
  const g = Number.parseInt(raw.slice(2, 4), 16)
  const b = Number.parseInt(raw.slice(4, 6), 16)
  if ([r, g, b].some((n) => Number.isNaN(n))) return undefined
  const clampedAlpha = Math.min(Math.max(alpha, 0), 1)
  return `rgba(${r},${g},${b},${clampedAlpha})`
}

const markerForTrade = (trade) => {
  const entryTime = trade?.entry_time ? Math.floor(new Date(trade.entry_time).getTime() / 1000) : null
  if (!entryTime) return []
  const isLong = trade.direction === 'long'
  const entryMarker = {
    time: entryTime,
    position: isLong ? 'belowBar' : 'aboveBar',
    shape: isLong ? 'arrowUp' : 'arrowDown',
    color: isLong ? '#34d399' : '#f97316',
    text: `${isLong ? 'Buy' : 'Sell'} ${trade.legs?.length || 0}x`,
  }
  const exitMarkers = []
  for (const leg of trade.legs || []) {
    if (!leg?.exit_time || !leg?.status) continue
    const ts = Math.floor(new Date(leg.exit_time).getTime() / 1000)
    exitMarkers.push({
      time: ts,
      position: isLong ? 'aboveBar' : 'belowBar',
      shape: leg.status === 'target' ? 'circle' : 'square',
      color: leg.status === 'target' ? '#22d3ee' : '#f87171',
      text: `${leg.name} ${leg.status === 'target' ? 'TP' : 'SL'}`,
    })
  }
  return [entryMarker, ...exitMarkers]
}

export function BotLensChart({ chartId, candles = [], trades = [], overlays = [] }) {
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
  const tradeSegmentsRef = useRef([])
  const diagLoggedRef = useRef(false)

  const resolvedCandles = Array.isArray(candles) ? candles : []
  const resolvedTrades = Array.isArray(trades) ? trades : []
  const resolvedOverlays = Array.isArray(overlays) ? overlays : []

  const candleData = useMemo(() => {
    if (!Array.isArray(resolvedCandles)) {
      return []
    }
    const normalized = resolvedCandles
      .map((candle) => ({
        time: toSec(candle?.time),
        open: toFiniteNumber(candle?.open),
        high: toFiniteNumber(candle?.high),
        low: toFiniteNumber(candle?.low),
        close: toFiniteNumber(candle?.close),
      }))
      .filter(
        (entry) =>
          Number.isFinite(entry.time) &&
          Number.isFinite(entry.open) &&
          Number.isFinite(entry.high) &&
          Number.isFinite(entry.low) &&
          Number.isFinite(entry.close),
      )

    return normalized.sort((a, b) => a.time - b.time)
  }, [resolvedCandles])

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

  const tradeMarkers = useMemo(() => {
    if (!Array.isArray(resolvedTrades)) {
      return []
    }
    const markers = resolvedTrades.flatMap((trade) => markerForTrade(trade))
    return markers.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
  }, [resolvedTrades])

  const updateViewport = useCallback(
    (tradeSegments = []) => {
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
        ghostPoints.push({ time: lastTime, value: lastCandle?.high ?? lastCandle?.close ?? 0 })
        ghostPoints.push({ time: lastTime - 1, value: lastCandle?.low ?? lastCandle?.close ?? 0 })
      }
      overlayPrices.forEach((price, idx) => {
        if (!Number.isFinite(lastTime)) return
        ghostPoints.push({ time: lastTime + idx + 1, value: price })
      })
      levelSeriesRef.current.setData(ghostPoints)
    },
    [candleData],
  )

  const syncOverlays = useCallback(
    (overlayPayloads = [], tradeMarkerPayload = []) => {
      if (!seriesRef.current || !paneMgrRef.current) return
      overlayHandlesRef.current.priceLines.forEach((handle) => {
        try {
          seriesRef.current.removePriceLine(handle)
        } catch {
          /* noop */
        }
      })
      overlayHandlesRef.current.priceLines = []
      if (!markersApiRef.current) {
        markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
      } else {
        markersApiRef.current.setMarkers([])
      }
      paneMgrRef.current.clearFrame()

      const markers = [...tradeMarkerPayload]
      const touchPoints = []
      const boxes = []
      const segments = []
      const tradeSegments = []
      const polylines = []
      const bubbles = []
      const lastSeriesTime = candleData[candleData.length - 1]?.time ?? null
      const normaliseSegment = (segment = {}) => {
        const x1 = toSec(coalesce(segment.x1, segment.start, segment.start_date, segment.startDate, segment.time))
        const x2 = toSec(coalesce(segment.x2, segment.end, segment.end_date, segment.endDate, segment.time))
        const y1 = toFiniteNumber(coalesce(segment.y1, segment.price, segment.value, segment.y))
        const y2 = toFiniteNumber(coalesce(segment.y2, segment.price, segment.value, segment.y))
        return { ...segment, x1, x2, y1, y2 }
      }

      for (const overlay of overlayPayloads) {
        const { type, payload, color } = overlay || {}
        if (!payload) continue
        const paneViews = getPaneViewsFor(type)
        const paneSet = new Set(paneViews || [])
        const norm = adaptPayload(type, payload, color)
        if (Array.isArray(payload.price_lines)) {
          payload.price_lines.forEach((pl) => {
            const price = toFiniteNumber(pl?.price)
            if (!Number.isFinite(price)) return
            try {
              const handle = seriesRef.current.createPriceLine({
                price,
                color: pl.color ?? color ?? '#a5b4fc',
                lineWidth: pl.lineWidth ?? 1,
                lineStyle: pl.lineStyle ?? 0,
                axisLabelVisible: pl.axisLabelVisible ?? false,
                title: pl.title || type || '',
              })
              overlayHandlesRef.current.priceLines.push(handle)
            } catch {
              /* ignore */
            }
          })
        }
        if (Array.isArray(norm.markers)) {
          markers.push(...norm.markers)
        }
        const wantsTouch = paneSet.has('touch') || (Array.isArray(norm.touchPoints) && norm.touchPoints.length > 0)
        if (wantsTouch && Array.isArray(norm.touchPoints) && norm.touchPoints.length) {
          touchPoints.push(
            ...norm.touchPoints
              .map((point) => ({
                ...point,
                time: toSec(point.time),
              }))
              .filter((point) => Number.isFinite(point.time) && Number.isFinite(point.price))
          )
        }
        const wantsBoxes = paneSet.has('va_box') || (Array.isArray(norm.boxes) && norm.boxes.length > 0)
        if (wantsBoxes && Array.isArray(norm.boxes) && norm.boxes.length) {
          const normalizedBoxes = norm.boxes
            .map((box) => {
              const x1 = toSec(coalesce(box.x1, box.start, box.start_date, box.startDate))
              const requestedX2 = toSec(coalesce(box.x2, box.end, box.end_date, box.endDate))
              const extend = box.extend !== undefined ? Boolean(box.extend) : false
              let x2 = requestedX2
              if (!Number.isFinite(x2)) {
                x2 = extend && Number.isFinite(lastSeriesTime) ? lastSeriesTime : x1
              } else if (extend && Number.isFinite(lastSeriesTime) && lastSeriesTime > x2) {
                x2 = lastSeriesTime
              }
              const y1 = toFiniteNumber(coalesce(box.y1, box.val, box.VAL))
              const y2 = toFiniteNumber(coalesce(box.y2, box.vah, box.VAH))
              return {
                x1,
                x2,
                y1,
                y2,
                color: box.color,
                border: box.border,
                precision: box.precision,
              }
            })
            .filter((entry) => Number.isFinite(entry.x1) && Number.isFinite(entry.x2))
          boxes.push(...normalizedBoxes)
        }
        const wantsSegments = paneSet.has('segment') || (Array.isArray(norm.segments) && norm.segments.length > 0)
        if (wantsSegments && Array.isArray(norm.segments) && norm.segments.length) {
          const normalisedSegments = norm.segments
            .map((segment) => {
              const normalised = normaliseSegment(segment)
              if (!Number.isFinite(normalised.y2) && Number.isFinite(normalised.y1)) {
                normalised.y2 = normalised.y1
              }
              if (!Number.isFinite(normalised.y1) && Number.isFinite(normalised.y2)) {
                normalised.y1 = normalised.y2
              }
              if (!Number.isFinite(normalised.x2) && Number.isFinite(normalised.x1)) {
                normalised.x2 = normalised.x1
              }
              if (!Number.isFinite(normalised.x1) && Number.isFinite(normalised.x2)) {
                normalised.x1 = normalised.x2
              }
              return normalised
            })
            .filter(
              (segment) =>
                Number.isFinite(segment.x1) &&
                Number.isFinite(segment.x2) &&
                Number.isFinite(segment.y1) &&
                Number.isFinite(segment.y2),
            )
          segments.push(...normalisedSegments)
          if (type === 'bot_trade_rays') {
            tradeSegments.push(...normalisedSegments)
          }
        }
        const wantsPolylines = paneSet.has('polyline') || (Array.isArray(norm.polylines) && norm.polylines.length > 0)
        if (wantsPolylines && Array.isArray(norm.polylines) && norm.polylines.length) {
          polylines.push(...norm.polylines)
        }
        const wantsBubbles = paneSet.has('signal_bubble') || (Array.isArray(norm.bubbles) && norm.bubbles.length > 0)
        if (wantsBubbles && Array.isArray(norm.bubbles) && norm.bubbles.length) {
          const bubbleColor = color ? toRgba(color, 0.16) : undefined
          const tinted = color
            ? norm.bubbles.map((bubble) => ({
                ...bubble,
                accentColor: color,
                backgroundColor: bubbleColor || bubble.backgroundColor,
              }))
            : norm.bubbles
          bubbles.push(...tinted)
        }
      }

      markers.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))

      markersApiRef.current?.setMarkers?.(markers)
      paneMgrRef.current.setTouchPoints(touchPoints)
      paneMgrRef.current.setVABlocks(boxes, {
        lastSeriesTime,
        barSpacing: barSpacingRef.current,
      })
      paneMgrRef.current.setSegments(segments)
      paneMgrRef.current.setPolylines(polylines)
      paneMgrRef.current.setSignalBubbles(bubbles)
      tradeSegmentsRef.current = tradeSegments
      updateViewport(tradeSegments)
    },
    [candleData, updateViewport]
  )

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
    markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
    paneMgrRef.current = new PaneViewManager(chart)
    registerChart?.(chartId, {
      get chart() {
        return chartRef.current
      },
      get series() {
        return seriesRef.current
      },
    })

    resizeObserverRef.current = new ResizeObserver(([entry]) => {
      const rect = entry?.contentRect
      if (!rect || !chartRef.current) return
      chartRef.current.applyOptions({ width: rect.width, height: rect.height })
    })
    resizeObserverRef.current.observe(el)

    return () => {
      resizeObserverRef.current?.disconnect()
      resizeObserverRef.current = null
      markersApiRef.current?.setMarkers?.([])
      markersApiRef.current = null
      paneMgrRef.current?.destroy()
      paneMgrRef.current = null
      overlayHandlesRef.current.priceLines = []
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
  }, [chartId, registerChart])

  useEffect(() => {
    if (!seriesRef.current) return
    seriesRef.current.setData(candleData)
    if (!markersApiRef.current) {
      markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
    }
    chartRef.current?.timeScale().fitContent()
  }, [candleData])

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
    syncOverlays(resolvedOverlays, tradeMarkers)
  }, [resolvedOverlays, tradeMarkers, syncOverlays])

  useEffect(() => {
    updateViewport(tradeSegmentsRef.current || [])
  }, [candleData, updateViewport])

  return (
    <div
      ref={containerRef}
      className="h-[360px] w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0f1118]"
    />
  )
}
