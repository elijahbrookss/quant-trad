import { useEffect, useMemo, useRef, useCallback } from 'react'
import { createChart, CandlestickSeries, createSeriesMarkers } from 'lightweight-charts'
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

  const candleData = useMemo(() => {
    return candles.map((candle) => ({
      time: Math.floor(new Date(candle.time).getTime() / 1000),
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
    }))
  }, [candles])

  const tradeMarkers = useMemo(() => {
    const markers = trades.flatMap((trade) => markerForTrade(trade))
    return markers.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
  }, [trades])

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
      const polylines = []
      const bubbles = []
      const lastSeriesTime = candleData[candleData.length - 1]?.time ?? null

      for (const overlay of overlayPayloads) {
        const { type, payload, color } = overlay || {}
        if (!payload) continue
        const paneViews = getPaneViewsFor(type)
        const norm = adaptPayload(type, payload, color)
        if (Array.isArray(payload.price_lines)) {
          payload.price_lines.forEach((pl) => {
            try {
              const handle = seriesRef.current.createPriceLine({
                price: pl.price,
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
        if (paneViews.includes('touch') && Array.isArray(norm.touchPoints) && norm.touchPoints.length) {
          touchPoints.push(
            ...norm.touchPoints
              .map((point) => ({
                ...point,
                time: toSec(point.time),
              }))
              .filter((point) => Number.isFinite(point.time) && Number.isFinite(point.price))
          )
        }
        if (paneViews.includes('va_box') && Array.isArray(norm.boxes) && norm.boxes.length) {
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
        if (paneViews.includes('segment') && Array.isArray(norm.segments) && norm.segments.length) {
          segments.push(...norm.segments)
        }
        if (paneViews.includes('polyline') && Array.isArray(norm.polylines) && norm.polylines.length) {
          polylines.push(...norm.polylines)
        }
        if (paneViews.includes('signal_bubble') && Array.isArray(norm.bubbles) && norm.bubbles.length) {
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
    },
    [candleData]
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
    chartRef.current = chart
    seriesRef.current = series
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
    syncOverlays(overlays, tradeMarkers)
  }, [overlays, tradeMarkers, syncOverlays])

  return (
    <div
      ref={containerRef}
      className="h-[360px] w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0f1118]"
    />
  )
}
