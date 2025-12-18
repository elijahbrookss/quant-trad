import { useEffect, useMemo, useRef, useCallback, useState } from 'react'
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

export const toSec = (value) => {
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
    color: isLong ? 'rgba(52,211,153,0.82)' : 'rgba(249,115,22,0.82)',
    text: `${isLong ? 'Buy' : 'Sell'} ${trade.legs?.length || 0}x`,
    kind: 'entry',
  }
  const grouped = new Map()
  const targetSummary = []
  const stopSummary = []
  for (const leg of trade.legs || []) {
    if (!leg?.exit_time || !leg?.status) continue
    const ts = Math.floor(new Date(leg.exit_time).getTime() / 1000)
    if (!grouped.has(ts)) grouped.set(ts, [])
    grouped.get(ts).push(leg)
  }
  const exitMarkers = []
  for (const [time, legs] of grouped.entries()) {
    const targets = legs.filter((leg) => leg.status === 'target')
    const stops = legs.filter((leg) => leg.status !== 'target')
    if (targets.length) {
      targetSummary.push(
        ...targets.map((leg) => ({
          name: leg.name || 'TP',
          price: leg.target_price || leg.exit_price,
        })),
      )
    }
    if (stops.length) {
      stopSummary.push(
        ...stops.map((leg) => ({
          name: leg.name || 'SL',
          price: leg.target_price || leg.exit_price || leg.stop_price,
        })),
      )
    }
    exitMarkers.push({
      time,
      position: isLong ? 'aboveBar' : 'belowBar',
      shape: stops.length > 0 ? 'square' : 'circle',
      color: stops.length > 0 ? 'rgba(248,113,113,0.82)' : 'rgba(34,211,238,0.82)',
      text: `${targets.length ? `TP x${targets.length}` : ''}${targets.length && stops.length ? ' / ' : ''}${
        stops.length ? `SL x${stops.length}` : ''
      }`,
      kind: stops.length ? 'stop' : 'target',
    })
  }
  const summaryLabel = []
  if (targetSummary.length) summaryLabel.push(`TP x${targetSummary.length}`)
  if (stopSummary.length) summaryLabel.push(`SL x${stopSummary.length}`)
  const summaryMarker = summaryLabel.length
    ? {
        time: entryTime,
        position: isLong ? 'aboveBar' : 'belowBar',
        shape: 'arrowUp',
        color: 'rgba(148,163,184,0.6)',
        text: summaryLabel.join(' / '),
        kind: 'tp-sl-summary',
        tooltip: {
          entries: [
            ...targetSummary.map((entry) => `${entry.name}: ${entry.price ?? '—'}`),
            ...stopSummary.map((entry) => `${entry.name}: ${entry.price ?? '—'}`),
          ],
        },
      }
    : null
  const markers = summaryMarker ? [entryMarker, summaryMarker, ...exitMarkers] : [entryMarker, ...exitMarkers]
  return markers
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
  const tradeSegmentsRef = useRef([])
  const diagLoggedRef = useRef(false)
  const markerCacheRef = useRef([])
  const prevPriceLinesRef = useRef([])
  const focusTimeoutRef = useRef(null)
  const pulseTimeoutRef = useRef(null)
  const markerDetailsRef = useRef([])
  const prevCandleDataRef = useRef([])
  const candleAnimationRef = useRef(null)
  const pulseLineHandlesRef = useRef([])
  const cameraLockedRef = useRef(true) // Track if camera should follow newest candle
  const userInteractedRef = useRef(false) // Track if user manually panned/zoomed
  const autoScrollRef = useRef(false) // Track programmatic camera moves
  const lastVisibleRangeRef = useRef(null)
  const relockTimeoutRef = useRef(null)
  const pointerStateRef = useRef({ active: false, x: 0, y: 0 })
  const [markerTooltip, setMarkerTooltip] = useState(null)

  const resolvedCandles = Array.isArray(candles) ? candles : []
  const resolvedTrades = Array.isArray(trades) ? trades : []
  const resolvedOverlays = Array.isArray(overlays) ? overlays : []
  const instantPlayback = Number(playbackSpeed) <= 0

  const candleLookup = useMemo(() => {
    const map = new Map()
    for (const candle of resolvedCandles || []) {
      const epoch = toSec(candle?.time)
      if (Number.isFinite(epoch)) {
        map.set(epoch, candle)
      }
    }
    return map
  }, [resolvedCandles])

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

  const tradeMarkerBundle = useMemo(() => {
    if (!Array.isArray(resolvedTrades)) {
      return { markers: [], tooltips: [] }
    }
    const markers = []
    const tooltips = []
    for (const trade of resolvedTrades) {
      const entries = markerForTrade(trade)
      markers.push(...entries)
      entries
        .filter((entry) => entry?.kind === 'tp-sl-summary' && Array.isArray(entry?.tooltip?.entries))
        .forEach((entry) => {
          tooltips.push({ time: entry.time, entries: entry.tooltip.entries })
        })
    }
    return {
      markers: markers.sort((a, b) => (a.time ?? 0) - (b.time ?? 0)),
      tooltips,
    }
  }, [resolvedTrades])

  const tradeMarkers = tradeMarkerBundle.markers
  const tradeMarkerTooltips = tradeMarkerBundle.tooltips

  const tradeRegions = useMemo(() => {
    if (!Array.isArray(resolvedTrades)) return []
    const regions = []
    for (const trade of resolvedTrades) {
      const entryTime = toSec(trade?.entry_time)
      if (!Number.isFinite(entryTime)) continue
      const isLong = (trade?.direction || '').toLowerCase() === 'long'
      const baseColor = isLong ? '#34d399' : '#f87171'
      const fill = toRgba(baseColor, 0.08)
      const border = toRgba(baseColor, 0.22)
      for (const leg of trade.legs || []) {
        const exitTime = toSec(leg?.exit_time || trade?.closed_at)
        if (!Number.isFinite(exitTime)) continue
        const entryPrice = toFiniteNumber(coalesce(leg?.entry_price, trade?.entry_price))
        const exitPrice = toFiniteNumber(coalesce(leg?.exit_price, trade?.stop_price, leg?.target_price))
        const entryCandle = candleLookup.get(entryTime)
        const exitCandle = candleLookup.get(exitTime)
        const inferredEntry = toFiniteNumber(coalesce(entryPrice, entryCandle?.close, entryCandle?.open))
        const inferredExit = toFiniteNumber(coalesce(exitPrice, exitCandle?.close, exitCandle?.open))
        const prices = [inferredEntry, inferredExit].filter(Number.isFinite)
        if (!prices.length) continue
        const y1 = Math.min(...prices)
        const y2 = Math.max(...prices)
        regions.push({
          x1: entryTime,
          x2: exitTime,
          y1,
          y2,
          color: fill,
          border: border,
          precision: 4,
        })
      }
    }
    return regions
  }, [candleLookup, resolvedTrades])

  const runWithAutoScrollGuard = useCallback((operation) => {
    autoScrollRef.current = true
    try {
      operation?.()
    } finally {
      // Defer reset until after the chart has a chance to emit range callbacks
      setTimeout(() => {
        autoScrollRef.current = false
      }, 0)
    }
  }, [])

  const enforceCameraLock = useCallback(() => {
    if (!cameraLockedRef.current || !chartRef.current) return
    const ts = chartRef.current.timeScale()
    const lastIndex = (latestCandlesRef.current?.length || 0) - 1
    if (lastIndex < 0) return
    const range = ts.getVisibleLogicalRange?.() || lastVisibleRangeRef.current
    const needsRealign = !range?.to || range.to < lastIndex - 0.5
    if (needsRealign) {
      const barsToShow = Math.min(80, Math.max(30, lastIndex + 1))
      const from = Math.max(0, lastIndex - barsToShow + 1)
      const to = lastIndex + 5
      runWithAutoScrollGuard(() => {
        ts.setVisibleLogicalRange({ from, to })
        ts.scrollToPosition?.(0, false)
      })
    }
  }, [runWithAutoScrollGuard])

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
            runWithAutoScrollGuard(() =>
              timeScale.setVisibleRange({ from: minTime - pad, to: maxTime + pad }),
            )
            appliedRange = true
          }
        }
      }
      if (!appliedRange) {
        const barsToShow = Math.min(200, Math.max(30, candleData.length))
        const from = Math.max(0, lastIndex - barsToShow + 1)
        const to = lastIndex + 5
        runWithAutoScrollGuard(() => timeScale.setVisibleLogicalRange({ from, to }))
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
    [candleData, runWithAutoScrollGuard],
  )

  const syncOverlays = useCallback(
    (overlayPayloads = [], tradeMarkerPayload = [], markerDetails = []) => {
      if (!seriesRef.current || !paneMgrRef.current) return
      if (!markersApiRef.current) {
        markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
      }
      paneMgrRef.current.clearFrame()

      markerDetailsRef.current = Array.isArray(markerDetails) ? markerDetails : []
      const hasAnyEntry = resolvedTrades.some((trade) => Number.isFinite(toSec(trade?.entry_time)))
      const baseMarkers = [...tradeMarkerPayload]
      const overlayMarkers = []
      const touchPoints = []
      const boxes = [...tradeRegions]
      const segments = []
      const tradeSegments = []
      const polylines = []
      const bubbles = []
      const priceLines = []

      // Add price lines for active trades (Entry, TP, SL)
      if (Array.isArray(resolvedTrades)) {
        for (const trade of resolvedTrades) {
          const entryTime = toSec(trade?.entry_time)
          const entryPrice = toFiniteNumber(trade?.entry_price)
          if (!Number.isFinite(entryTime) || !Number.isFinite(entryPrice)) continue

          // Check if trade is still active (not all legs closed)
          const hasOpenLegs = (trade.legs || []).some((leg) => !leg?.exit_time || leg.status === 'open')
          if (hasOpenLegs) {
            // Calculate unrealized P&L for entry line
            const lastCandle = candleData[candleData.length - 1]
            const currentPrice = toFiniteNumber(lastCandle?.close)
            const isLong = (trade?.direction || '').toLowerCase() === 'long'
            let pnl = null
            let pnlPercent = null
            if (Number.isFinite(currentPrice) && Number.isFinite(entryPrice)) {
              pnl = isLong ? currentPrice - entryPrice : entryPrice - currentPrice
              pnlPercent = (pnl / entryPrice) * 100
            }

            // Entry price line with P&L
            priceLines.push({
              price: entryPrice,
              title: 'Entry',
              color: '#94a3b8',
              source: 'active_trade_entry',
              precision: 2,
              pnl,
              pnlPercent,
            })

            // Stop loss price line
            const stopPrice = toFiniteNumber(trade?.stop_price)
            if (Number.isFinite(stopPrice)) {
              priceLines.push({
                price: stopPrice,
                title: 'SL',
                color: '#ef4444',
                source: 'active_trade_sl',
                precision: 2,
              })
            }

            // Target price lines (from open legs)
            const openLegs = (trade.legs || []).filter((leg) => !leg?.exit_time || leg.status === 'open')
            for (const leg of openLegs) {
              const targetPrice = toFiniteNumber(leg?.target_price)
              if (Number.isFinite(targetPrice)) {
                priceLines.push({
                  price: targetPrice,
                  title: 'TP',
                  color: '#10b981',
                  source: 'active_trade_tp',
                  precision: 2,
                })
              }
            }
          }
        }
      }

      const lastSeriesTime = candleData[candleData.length - 1]?.time ?? null
      const lastCandle = candleData[candleData.length - 1]
      const prevCandle = candleData[candleData.length - 2]
      if (lastCandle && Number.isFinite(lastCandle?.time)) {
        const halfSpan = (() => {
          const prevTime = prevCandle?.time
          if (Number.isFinite(prevTime)) return Math.max(5, Math.abs(lastCandle.time - prevTime) / 2)
          if (Number.isFinite(barSpacingRef.current)) return Math.max(5, barSpacingRef.current / 2)
          return 15
        })()
        boxes.unshift({
          x1: lastCandle.time - halfSpan,
          x2: lastCandle.time + halfSpan,
          y1: Math.min(lastCandle.low, lastCandle.high, lastCandle.open, lastCandle.close),
          y2: Math.max(lastCandle.low, lastCandle.high, lastCandle.open, lastCandle.close),
          color: 'rgba(125,211,252,0.08)',
          border: { color: 'rgba(125,211,252,0.28)', width: 1 },
          precision: 4,
        })
      }
      const normaliseSegment = (segment = {}) => {
        const x1 = toSec(coalesce(segment.x1, segment.start, segment.start_date, segment.startDate, segment.time))
        const x2 = toSec(coalesce(segment.x2, segment.end, segment.end_date, segment.endDate, segment.time))
        const y1 = toFiniteNumber(coalesce(segment.y1, segment.price, segment.value, segment.y))
        const y2 = toFiniteNumber(coalesce(segment.y2, segment.price, segment.value, segment.y))
        return { ...segment, x1, x2, y1, y2 }
      }

      for (const overlay of overlayPayloads || []) {
        const { type, payload, color, ind_id } = overlay || {}
        if (!payload) continue
        const paneViews = getPaneViewsFor(type)
        const paneSet = new Set(paneViews || [])
        const norm = adaptPayload(type, payload, color)
        // Note: TP/SL price lines are filtered out in adaptPayload registry
        if (Array.isArray(payload.price_lines)) {
          payload.price_lines.forEach((pl) => {
            const price = toFiniteNumber(pl?.price)
            if (!Number.isFinite(price)) return
            priceLines.push({
              ...pl,
              price,
              color: pl.color ?? color,
              source: type || pl.title || 'overlay',
            })
          })
        }
        if (Array.isArray(norm.markers)) {
          overlayMarkers.push(...norm.markers)
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
              const x1 = box.x1
              const requestedX2 = box.x2
              const extend = box.extend !== undefined ? Boolean(box.extend) : false
              let x2 = requestedX2
              if (!Number.isFinite(x2)) {
                x2 = extend && Number.isFinite(lastSeriesTime) ? lastSeriesTime : x1
              } else if (extend && Number.isFinite(lastSeriesTime) && lastSeriesTime > x2) {
                x2 = lastSeriesTime
              }
              const y1 = box.y1
              const y2 = box.y2
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

      const markerKey = (marker) => `${marker.time}-${marker.position}-${marker.text}-${marker.kind || ''}`

      const applyMarkers = (set, animate = false) => {
        const sorted = [...set].sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
        if (!animate) {
          markerCacheRef.current = sorted
          markersApiRef.current?.setMarkers?.(sorted)
          return
        }
        const previous = new Set((markerCacheRef.current || []).map((marker) => markerKey(marker)))
        const staged = sorted.map((marker) => {
          if (!previous.has(markerKey(marker))) {
            if (marker.kind === 'entry') {
              return { ...marker, size: (marker.size || 1) * 0.85, color: marker.color?.replace('0.82', '0.7') }
            }
            if (marker.kind === 'target') {
              return { ...marker, color: marker.color?.replace('0.82', '0.6'), text: marker.text, __fadeIn: true }
            }
            if (marker.kind === 'stop') {
              return { ...marker, color: marker.color?.replace('0.82', '0.55'), text: marker.text, __fadeIn: true }
            }
            return { ...marker, color: marker.color?.replace('0.82', '0.65'), __fadeIn: true }
          }
          return marker
        })
        markerCacheRef.current = staged
        markersApiRef.current?.setMarkers?.(staged)
        if (staged.some((marker) => marker.__fadeIn)) {
          setTimeout(() => {
            const finalized = sorted.map((marker) => {
              const clone = { ...marker }
              delete clone.__fadeIn
              return clone
            })
            markerCacheRef.current = finalized
            markersApiRef.current?.setMarkers?.(finalized)
          }, 200)
        }
      }

      const groupedPriceLines = (() => {
        const normalised = []
        const seen = new Map()
        const toRole = (title = '') => {
          const value = typeof title === 'string' ? title.toLowerCase() : ''
          if (value.includes('sl') || value.includes('stop')) return 'sl'
          if (value.includes('tp') || value.includes('target')) return 'tp'
          return 'level'
        }
        const toPrecision = (pl) => {
          const precise = Number(pl?.precision)
          if (Number.isFinite(precise)) return precise
          return 2
        }
        for (const line of priceLines) {
          const role = toRole(line?.title)
          const price = line.price
          const key = `${role}-${price}`
          const existing = seen.get(key)
          if (existing) {
            existing.count += 1
            existing.labels.push(line?.title)
          } else {
            seen.set(key, {
              price,
              role,
              count: 1,
              labels: [line?.title].filter(Boolean),
              precision: toPrecision(line),
              color: line?.color,
              pnl: line?.pnl,
              pnlPercent: line?.pnlPercent,
              source: line?.source,
            })
          }
        }
        seen.forEach((value) => normalised.push(value))
        return normalised
      })()

      const applyPriceLines = () => {
        if (!seriesRef.current) return
        const signature = groupedPriceLines.map((line) => ({
          price: line.price,
          role: line.role,
          count: line.count,
          labels: line.labels.join('|'),
          precision: line.precision,
          color: line.color,
          pnl: line.pnl,
          pnlPercent: line.pnlPercent,
        }))
        const prevSignature = prevPriceLinesRef.current
        const unchanged =
          prevSignature.length === signature.length &&
          signature.every((entry, idx) => {
            const prev = prevSignature[idx]
            return (
              prev &&
              prev.price === entry.price &&
              prev.role === entry.role &&
              prev.count === entry.count &&
              prev.labels === entry.labels &&
              prev.precision === entry.precision &&
              prev.color === entry.color &&
              prev.pnl === entry.pnl &&
              prev.pnlPercent === entry.pnlPercent
            )
          })
        if (!unchanged) {
          overlayHandlesRef.current.priceLines.forEach((handle) => {
            try {
              seriesRef.current.removePriceLine(handle)
            } catch {
              /* noop */
            }
          })
          overlayHandlesRef.current.priceLines = []
          groupedPriceLines.forEach((line) => {
            const isStop = line.role === 'sl'
            const isTarget = line.role === 'tp'
            const isEntry = line.role === 'level' && (line.labels[0] === 'Entry' || line.source === 'active_trade_entry')

            // Calculate dynamic color for entry based on P&L
            let baseColor = isStop ? '#ef4444' : isTarget ? '#10b981' : (line.color || '#94a3b8')
            if (isEntry && Number.isFinite(line.pnl)) {
              baseColor = line.pnl >= 0 ? '#10b981' : '#ef4444'
            }

            const lineColor = toRgba(baseColor, 0.9) || 'rgba(148,163,184,0.85)'
            const labelBg = toRgba(baseColor, 1.0) || 'rgba(148,163,184,0.9)'
            const precision = Number.isFinite(line.precision) ? line.precision : 2
            const priceLabel = Number(line.price).toFixed(precision)
            const labelSource = line.labels[0] || (isTarget ? 'Target' : isStop ? 'Stop Loss' : isEntry ? 'Entry' : 'Level')
            const labelCount = line.count > 1 && isTarget ? ` x${line.count}` : ''

            // Keep axis labels compact to avoid covering the live price marker
            const title = `${labelSource}${labelCount ? labelCount : ''} ${priceLabel}`
            const priceLineOptions = {
              price: line.price,
              color: lineColor,
              lineWidth: 2,
              lineStyle: 0, // 0 = solid, 1 = dotted, 2 = dashed, 3 = large dashed, 4 = sparse dotted
              axisLabelVisible: true,
              axisLabelColor: labelBg,
              axisLabelTextColor: '#ffffff',
              title,
            }
            try {
              const handle = seriesRef.current.createPriceLine(priceLineOptions)
              overlayHandlesRef.current.priceLines.push(handle)
            } catch (err) {
              console.warn('[BotLensChart] Failed to create price line:', err)
            }
          })
          prevPriceLinesRef.current = signature
        }
      }

      applyMarkers([...baseMarkers, ...overlayMarkers], true)
      applyPriceLines()

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
      [candleData, resolvedTrades, tradeRegions, updateViewport]
    )

  const reLockCamera = useCallback(() => {
    if (relockTimeoutRef.current) {
      clearTimeout(relockTimeoutRef.current)
      relockTimeoutRef.current = null
    }
    cameraLockedRef.current = true
    userInteractedRef.current = false
    const ts = chartRef.current?.timeScale?.()
    if (ts) {
      runWithAutoScrollGuard(() => {
        ts.scrollToPosition(0, false)
      })
    }
  }, [runWithAutoScrollGuard])

  const focusAtTime = useCallback(
    (time, priceHint) => {
      if (!chartRef.current || !seriesRef.current) return
      const epoch = toSec(time)
      if (!Number.isFinite(epoch)) return
      const ts = chartRef.current.timeScale()
      const span = barSpacingRef.current ? Math.max(barSpacingRef.current * 20, 30) : 60
      runWithAutoScrollGuard(() => ts.setVisibleRange({ from: epoch - span, to: epoch + span }))
      const candle = candleLookup.get(epoch)
      const price = toFiniteNumber(coalesce(priceHint, candle?.close, candle?.open, candle?.high, candle?.low))
      const highlight = Number.isFinite(price)
        ? { time: epoch, position: 'aboveBar', shape: 'circle', color: 'rgba(125,211,252,0.9)', text: ' ' }
        : null
      if (highlight && markersApiRef.current) {
        const combined = [...markerCacheRef.current, highlight].sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
        markersApiRef.current.setMarkers(combined)
        if (focusTimeoutRef.current) {
          clearTimeout(focusTimeoutRef.current)
        }
        focusTimeoutRef.current = setTimeout(() => {
          markersApiRef.current?.setMarkers?.(markerCacheRef.current)
          focusTimeoutRef.current = null
        }, 600)
      }
    },
    [candleLookup, runWithAutoScrollGuard],
  )

  const zoomIn = useCallback(() => {
    const ts = chartRef.current?.timeScale?.()
    ts?.zoomIn?.()
  }, [])

  const zoomOut = useCallback(() => {
    const ts = chartRef.current?.timeScale?.()
    ts?.zoomOut?.()
  }, [])

  const centerView = useCallback(() => {
    const ts = chartRef.current?.timeScale?.()
    if (!ts) return

    // Re-lock camera when user explicitly centers view
    reLockCamera()

    const candles = latestCandlesRef.current || []
    const last = candles[candles.length - 1]?.time
    const first = candles[0]?.time
    if (Number.isFinite(last) && Number.isFinite(first)) {
      const span = Math.max(90, Math.round((last - first) / 4))
      runWithAutoScrollGuard(() => {
        ts.setVisibleRange({ from: last - span, to: last })
        ts.scrollToPosition(0, false)
      })
    } else {
      runWithAutoScrollGuard(() => ts.scrollToRealTime())
    }
  }, [reLockCamera, runWithAutoScrollGuard])

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
    const start = performance.now()
    const duration = 200
    const frame = (now) => {
      const progress = Math.min(1, (now - start) / duration)
      const eased = progress < 1 ? progress * progress * (3 - 2 * progress) : 1
      const lerp = (a, b) => a + (b - a) * eased
      const next = {
        time: to.time,
        open: lerp(from?.open ?? to.open, to.open),
        high: lerp(from?.high ?? to.high, to.high),
        low: lerp(from?.low ?? to.low, to.low),
        close: lerp(from?.close ?? to.close, to.close),
      }
      seriesRef.current.update(next)
      if (progress < 1) {
        candleAnimationRef.current = requestAnimationFrame(frame)
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
    markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
    paneMgrRef.current = new PaneViewManager(chart)
    registerChart?.(chartId, {
      get chart() {
        return chartRef.current
      },
      get series() {
        return seriesRef.current
      },
      focusAtTime,
      pulseTrade: pulseTradeElements,
      clearPulse: clearPulseArtifacts,
      zoomIn,
      zoomOut,
      centerView,
    })

    const scheduleRelock = () => {
      if (relockTimeoutRef.current) {
        clearTimeout(relockTimeoutRef.current)
      }
      relockTimeoutRef.current = setTimeout(() => {
        reLockCamera()
        relockTimeoutRef.current = null
      }, 3000)
    }

    // Subscribe to visible range changes to detect user pan/zoom
    const timeScale = chart.timeScale()
    const handleVisibleRangeChange = (range) => {
      const prevRange = lastVisibleRangeRef.current
      lastVisibleRangeRef.current = range
      if (autoScrollRef.current) {
        userInteractedRef.current = false
        return
      }
      if (cameraLockedRef.current) {
        enforceCameraLock()
      }
      const hasRangeDelta =
        range &&
        prevRange &&
        (Math.abs((range.from ?? 0) - (prevRange.from ?? 0)) > 0.01 ||
          Math.abs((range.to ?? 0) - (prevRange.to ?? 0)) > 0.01)
      if (userInteractedRef.current && hasRangeDelta) {
        cameraLockedRef.current = false
        scheduleRelock()
      }
      // Reset flag after handling
      userInteractedRef.current = false
    }
    timeScale.subscribeVisibleLogicalRangeChange(handleVisibleRangeChange)

    // Detect mouse/touch interactions on the chart
    const handlePointerDown = (evt) => {
      pointerStateRef.current = { active: true, x: evt.clientX ?? 0, y: evt.clientY ?? 0 }
    }
    const handlePointerMove = (evt) => {
      if (!pointerStateRef.current.active) return
      const dx = Math.abs((evt.clientX ?? 0) - pointerStateRef.current.x)
      const dy = Math.abs((evt.clientY ?? 0) - pointerStateRef.current.y)
      if (dx > 2 || dy > 2) {
        userInteractedRef.current = true
      }
    }
    const handlePointerUp = () => {
      pointerStateRef.current = { active: false, x: 0, y: 0 }
    }
    const handleWheel = (evt) => {
      if (Math.abs(evt.deltaY) > 0 || Math.abs(evt.deltaX) > 0) {
        userInteractedRef.current = true
      }
    }
    el.addEventListener('pointerdown', handlePointerDown)
    el.addEventListener('pointermove', handlePointerMove)
    el.addEventListener('pointerup', handlePointerUp)
    el.addEventListener('pointercancel', handlePointerUp)
    el.addEventListener('wheel', handleWheel, { passive: true })

    resizeObserverRef.current = new ResizeObserver(([entry]) => {
      const rect = entry?.contentRect
      if (!rect || !chartRef.current) return
      chartRef.current.applyOptions({ width: rect.width, height: rect.height })
    })
    resizeObserverRef.current.observe(el)

    return () => {
      timeScale.unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange)
      el.removeEventListener('pointerdown', handlePointerDown)
      el.removeEventListener('pointermove', handlePointerMove)
      el.removeEventListener('pointerup', handlePointerUp)
      el.removeEventListener('pointercancel', handlePointerUp)
      el.removeEventListener('wheel', handleWheel)
      if (relockTimeoutRef.current) {
        clearTimeout(relockTimeoutRef.current)
        relockTimeoutRef.current = null
      }
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
  }, [chartId, registerChart, reLockCamera, runWithAutoScrollGuard])

  useEffect(() => {
    if (!seriesRef.current) return
    if (!markersApiRef.current) {
      markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
    }
    const previous = prevCandleDataRef.current || []
    const next = candleData
    const prevLast = previous[previous.length - 1]
    const nextLast = next[next.length - 1]
    const prevMatch = nextLast
      ? previous.find((candle) => Number.isFinite(candle?.time) && candle.time === nextLast.time)
      : null

    const isSameCandle = prevMatch && nextLast && prevMatch.time === nextLast.time
    const shouldAnimate = isSameCandle && activeTradeAtLastCandle && !instantPlayback

    if (shouldAnimate) {
      // During intrabar updates, just update the last candle directly without animation
      // to avoid jitter and timestamp errors
      seriesRef.current.update(nextLast)
    } else {
      seriesRef.current.setData(next)

      // Camera lock: auto-scroll to follow newest candle during playback
      if (cameraLockedRef.current && chartRef.current && next.length > 0) {
        const timeScale = chartRef.current.timeScale()
        const lastIndex = next.length - 1
        const barsToShow = Math.min(80, Math.max(30, next.length))
        const from = Math.max(0, lastIndex - barsToShow + 1)
        const to = lastIndex + 5
        runWithAutoScrollGuard(() => {
          timeScale.setVisibleLogicalRange({ from, to })
          timeScale.scrollToPosition?.(0, false)
        })
      } else if (!cameraLockedRef.current) {
        // If camera is unlocked, just fit content once
        runWithAutoScrollGuard(() => chartRef.current?.timeScale().fitContent())
      }
    }
    prevCandleDataRef.current = next
  }, [activeTradeAtLastCandle, candleData, instantPlayback, runWithAutoScrollGuard])

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
    syncOverlays(resolvedOverlays, tradeMarkers, tradeMarkerTooltips)
  }, [candleData, resolvedOverlays, tradeMarkerTooltips, tradeMarkers, syncOverlays])

  useEffect(() => {
    updateViewport(tradeSegmentsRef.current || [])
  }, [candleData, updateViewport])

  useEffect(() => {
    if (Number(playbackSpeed) > 0) {
      reLockCamera()
    }
  }, [playbackSpeed, reLockCamera])

  return (
    <div
      ref={containerRef}
      className="relative h-[360px] w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0f1118]"
    >
      {markerTooltip?.entries?.length ? (
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
      ) : null}
    </div>
  )
}
