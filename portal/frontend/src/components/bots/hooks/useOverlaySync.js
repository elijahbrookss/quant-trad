import { useCallback, useRef } from 'react'
import { createSeriesMarkers } from 'lightweight-charts'
import { adaptPayload, getPaneViewsFor } from '../../../chart/indicators/registry.js'
import { coalesce, toFiniteNumber, toSec } from '../chartDataUtils.js'

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

export const useOverlaySync = ({
  seriesRef,
  paneMgrRef,
  barSpacingRef,
  markersApiRef,
  overlayHandlesRef,
  markerCacheRef,
  markerDetailsRef,
  prevPriceLinesRef,
  applyViewport,
}) => {
  const initialViewportAppliedRef = useRef(false)
  const tradeViewportSignatureRef = useRef(null)

  return useCallback(
    ({
      overlayPayloads = [],
      tradeMarkers = [],
      tradeTooltips = [],
      tradeRegions = [],
      tradePriceLines = [],
      candleData = [],
    }) => {
      if (!seriesRef.current || !paneMgrRef.current) return
      if (!markersApiRef.current) {
        markersApiRef.current = createSeriesMarkers(seriesRef.current, [])
      }
      paneMgrRef.current.clearFrame()

      markerDetailsRef.current = Array.isArray(tradeTooltips) ? tradeTooltips : []
      const baseMarkers = [...tradeMarkers]
      const overlayMarkers = []
      const touchPoints = []
      const boxes = [...tradeRegions]
      const segments = []
      const tradeSegments = []
      const polylines = []
      const bubbles = []
      const priceLines = [...tradePriceLines]

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
                text: point.text || point.label || '',
                textColor: point.textColor || color,
                ind_id,
              }))
              .filter((point) => Number.isFinite(point.time)),
          )
        }
        const wantsBoxes = paneSet.has('va_box') || (Array.isArray(norm.boxes) && norm.boxes.length > 0)
        if (wantsBoxes && Array.isArray(norm.boxes) && norm.boxes.length) {
          const normalizedBoxes = norm.boxes
            .map((box) => ({
              ...box,
              x1: toSec(box.x1 ?? box.start ?? box.start_date ?? box.startDate),
              x2: toSec(box.x2 ?? box.end ?? box.end_date ?? box.endDate),
            }))
            .filter((box) => Number.isFinite(box?.x1) && Number.isFinite(box?.x2))
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
                Number.isFinite(segment.x1) && Number.isFinite(segment.x2) && Number.isFinite(segment.y1) && Number.isFinite(segment.y2),
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

            let baseColor = isStop ? '#ef4444' : isTarget ? '#10b981' : line.color || '#94a3b8'
            if (isEntry && Number.isFinite(line.pnl)) {
              baseColor = line.pnl >= 0 ? '#10b981' : '#ef4444'
            }

            const lineColor = toRgba(baseColor, 0.9) || 'rgba(148,163,184,0.85)'
            const labelBg = toRgba(baseColor, 1.0) || 'rgba(148,163,184,0.9)'
            const precision = Number.isFinite(line.precision) ? line.precision : 2
            const priceLabel = Number(line.price).toFixed(precision)
            const labelSource = line.labels[0] || (isTarget ? 'Target' : isStop ? 'Stop Loss' : isEntry ? 'Entry' : 'Level')
            const labelCount = line.count > 1 && isTarget ? ` x${line.count}` : ''

            let title = `${labelSource}${labelCount ? labelCount : ''} ${priceLabel}`
            if (isEntry && Number.isFinite(line.pnl) && Number.isFinite(line.pnlPercent)) {
              const pnlSign = line.pnl >= 0 ? '+' : ''
              const pnlValue = line.pnl.toFixed(2)
              const pnlPct = line.pnlPercent.toFixed(2)
              title = `Entry ${priceLabel} | ${pnlSign}${pnlValue} (${pnlSign}${pnlPct}%)`
            }
            const priceLineOptions = {
              price: line.price,
              color: lineColor,
              lineWidth: 2,
              lineStyle: 0,
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
      const tradeExtentSignature = (() => {
        if (!tradeSegments.length) return null
        const candidateTimes = tradeSegments
          .flatMap((segment) => [segment.x1, segment.x2])
          .filter((value) => Number.isFinite(value))
        if (!candidateTimes.length) return null
        const min = Math.min(...candidateTimes)
        const max = Math.max(...candidateTimes)
        return `${min}-${max}-${lastSeriesTime}`
      })()

      if (tradeExtentSignature) {
        if (tradeExtentSignature !== tradeViewportSignatureRef.current) {
          applyViewport(candleData, tradeSegments)
          tradeViewportSignatureRef.current = tradeExtentSignature
          initialViewportAppliedRef.current = true
        }
      } else if (!initialViewportAppliedRef.current && candleData.length) {
        applyViewport(candleData, [])
        initialViewportAppliedRef.current = true
        tradeViewportSignatureRef.current = null
      } else {
        tradeViewportSignatureRef.current = null
      }
    },
    [applyViewport, barSpacingRef, markerCacheRef, markersApiRef, overlayHandlesRef, paneMgrRef, prevPriceLinesRef, seriesRef],
  )
}
