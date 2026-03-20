import { useCallback, useRef } from 'react'
import { collectActivePaneKeys } from '../../../chart/panes/registry.js'
import { projectOverlayPayloads } from '../../../chart/overlays/projectOverlayPayloads.js'
import { BOTLENS_DEBUG, coalesce, toFiniteNumber, toSec } from '../chartDataUtils.js'

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
  overlayHandlesRef,
  markerDetailsRef,
  prevPriceLinesRef,
  markerManager,
}) => {
  const tradeViewportSignatureRef = useRef(null)

  const computeArtifacts = useCallback(
    ({ overlayPayloads = [], tradeMarkers = [], tradeTooltips = [], tradeRegions = [], tradePriceLines = [], candleData = [] }) => {
      const markerDetails = Array.isArray(tradeTooltips) ? [...tradeTooltips] : []
      const baseMarkersByPane = { price: [...tradeMarkers] }
      const boxesByPane = { price: [...tradeRegions] }
      const tradeSegments = []
      const priceLines = [...tradePriceLines]
      const projectedTradeSegments = []

      const firstSeriesTime = candleData[0]?.time ?? null
      const lastSeriesTime = candleData[candleData.length - 1]?.time ?? null
      const hasSeriesWindow =
        Number.isFinite(firstSeriesTime) &&
        Number.isFinite(lastSeriesTime) &&
        Number(firstSeriesTime) <= Number(lastSeriesTime)
      const lastCandle = candleData[candleData.length - 1]
      const prevCandle = candleData[candleData.length - 2]
      if (lastCandle && Number.isFinite(lastCandle?.time)) {
        const halfSpan = (() => {
          const prevTime = prevCandle?.time
          if (Number.isFinite(prevTime)) return Math.max(5, Math.abs(lastCandle.time - prevTime) / 2)
          if (Number.isFinite(barSpacingRef.current)) return Math.max(5, barSpacingRef.current / 2)
          return 15
        })()
        boxesByPane.price.unshift({
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

      const isWithinSeriesWindow = (epoch) => {
        if (!Number.isFinite(epoch)) return false
        if (!hasSeriesWindow) return true
        return Number(epoch) >= Number(firstSeriesTime) && Number(epoch) <= Number(lastSeriesTime)
      }

      const toScopedRange = (left, right) => {
        let x1 = toFiniteNumber(left)
        let x2 = toFiniteNumber(right)
        if (!Number.isFinite(x1) && Number.isFinite(x2)) x1 = x2
        if (!Number.isFinite(x2) && Number.isFinite(x1)) x2 = x1
        if (!Number.isFinite(x1) || !Number.isFinite(x2)) return null

        let start = Math.min(x1, x2)
        let end = Math.max(x1, x2)

        if (hasSeriesWindow) {
          if (end < Number(firstSeriesTime) || start > Number(lastSeriesTime)) return null
          start = Math.max(start, Number(firstSeriesTime))
          end = Math.min(end, Number(lastSeriesTime))
        }
        return { x1: start, x2: end }
      }

      const dedupeByKey = (entries, keyFn) => {
        const out = []
        const seen = new Set()
        for (const entry of entries || []) {
          const key = keyFn(entry)
          if (seen.has(key)) continue
          seen.add(key)
          out.push(entry)
        }
        return out
      }

      const projected = projectOverlayPayloads({
        overlays: overlayPayloads,
        bubbleAlpha: 0.16,
        normalizeTime: toSec,
        onOverlayProjected: ({ overlay, paneViews, normalized }) => {
          const { type, payload } = overlay || {}
          if (type === 'bot_trade_rays' && Array.isArray(normalized?.segments) && normalized.segments.length) {
            projectedTradeSegments.push(...normalized.segments)
          }
          if (!(BOTLENS_DEBUG && type === 'regime_overlay')) return
          const boxes = Array.isArray(payload?.boxes) ? payload.boxes.length : 0
          const segmentsLen = Array.isArray(payload?.segments) ? payload.segments.length : 0
          const markersLen = Array.isArray(payload?.markers) ? payload.markers.length : 0
          const times = []
          if (Array.isArray(payload?.boxes)) {
            payload.boxes.forEach((b) => {
              if (Number.isFinite(b?.x1)) times.push(toSec(b.x1))
              if (Number.isFinite(b?.x2)) times.push(toSec(b.x2))
            })
          }
          if (Array.isArray(payload?.segments)) {
            payload.segments.forEach((s) => {
              if (Number.isFinite(s?.x1)) times.push(toSec(s.x1))
              if (Number.isFinite(s?.x2)) times.push(toSec(s.x2))
            })
          }
          const span =
            times.length > 0
              ? { from: Math.min(...times.filter(Number.isFinite)), to: Math.max(...times.filter(Number.isFinite)) }
              : null
          console.debug('[BotLensChart] regime_overlay_payload', {
            pane_views: paneViews,
            boxes,
            segments: segmentsLen,
            markers: markersLen,
            span,
            instrument_id: overlay?.instrument_id,
            symbol: overlay?.symbol,
          })
        },
      })

      const overlayMarkersByPane = projected.markersByPane
      const touchPointsByPane = projected.touchPointsByPane
      const segmentsByPane = projected.segmentsByPane
      const polylinesByPane = projected.polylinesByPane
      const bubblesByPane = projected.bubblesByPane
      markerDetails.push(...(projected.signalDetails || []))
      Object.entries(projected.boxesByPane || {}).forEach(([paneKey, entries]) => {
        if (!boxesByPane[paneKey]) boxesByPane[paneKey] = []
        boxesByPane[paneKey].push(...entries)
      })
      projected.priceLines.forEach((pl) => {
        const price = toFiniteNumber(pl?.price)
        if (!Number.isFinite(price)) return
        priceLines.push({
          ...pl,
          price,
          source: pl?.source || pl?.title || 'overlay',
        })
      })
      tradeSegments.push(...projectedTradeSegments)

      const toScopedSegment = (segment) => {
        const normalised = normaliseSegment(segment)
        if (!Number.isFinite(normalised.y2) && Number.isFinite(normalised.y1)) {
          normalised.y2 = normalised.y1
        }
        if (!Number.isFinite(normalised.y1) && Number.isFinite(normalised.y2)) {
          normalised.y1 = normalised.y2
        }
        const range = toScopedRange(normalised.x1, normalised.x2)
        if (!range) return null
        if (!Number.isFinite(normalised.y1) || !Number.isFinite(normalised.y2)) return null
        return {
          ...normalised,
          x1: range.x1,
          x2: range.x2,
        }
      }

      const scopeEntriesByPane = (collection, mapper) =>
        Object.fromEntries(
          Object.entries(collection || {}).map(([paneKey, entries]) => [
            paneKey,
            (entries || []).map(mapper).filter(Boolean),
          ]),
        )

      const scopedMarkersByPane = Object.fromEntries(
        Object.entries({ ...baseMarkersByPane, ...overlayMarkersByPane }).map(([paneKey, entries]) => [
          paneKey,
          (entries || [])
            .map((marker) => ({ ...marker, time: toSec(marker?.time) }))
            .filter((marker) => isWithinSeriesWindow(marker?.time)),
        ]),
      )

      const scopedMarkerDetails = (markerDetails || [])
        .map((detail) => {
          const epoch = toSec(detail?.time)
          if (!Number.isFinite(epoch)) return null
          return { ...detail, time: epoch }
        })
        .filter((detail) => detail && isWithinSeriesWindow(detail.time))

      const scopedTouchPointsByPane = scopeEntriesByPane(
        touchPointsByPane,
        (point) => {
          const next = { ...point, time: toSec(point?.time) }
          return isWithinSeriesWindow(next?.time) ? next : null
        },
      )

      const scopedBoxesByPane = Object.fromEntries(
        Object.entries(boxesByPane || {}).map(([paneKey, entries]) => [
          paneKey,
          dedupeByKey(
            (entries || [])
          .map((box) => {
            const range = toScopedRange(
              toSec(coalesce(box?.x1, box?.start, box?.start_date, box?.startDate)),
              toSec(coalesce(box?.x2, box?.end, box?.end_date, box?.endDate, box?.x1, box?.start, box?.start_date, box?.startDate)),
            )
            const y1 = toFiniteNumber(coalesce(box?.y1, box?.val, box?.VAL))
            const y2 = toFiniteNumber(coalesce(box?.y2, box?.vah, box?.VAH))
            if (!range || !Number.isFinite(y1) || !Number.isFinite(y2)) return null
            return {
              ...box,
              x1: range.x1,
              x2: range.x2,
              y1,
              y2,
            }
          })
          .filter(Boolean),
            (box) => `${box.x1}|${box.x2}|${box.y1}|${box.y2}|${box.color || ''}|${box?.border?.color || ''}|${box?.border?.width || 0}`,
          ),
        ]),
      )

      const scopedSegmentsByPane = Object.fromEntries(
        Object.entries(segmentsByPane || {}).map(([paneKey, entries]) => [
          paneKey,
          dedupeByKey(
            (entries || []).map((segment) => toScopedSegment(segment)).filter(Boolean),
            (segment) =>
              `${segment.x1}|${segment.x2}|${segment.y1}|${segment.y2}|${segment.color || ''}|${segment.lineStyle || 0}|${segment.lineWidth || 1}`,
          ),
        ]),
      )

      const scopedTradeSegments = dedupeByKey(
        (tradeSegments || []).map((segment) => toScopedSegment(segment)).filter(Boolean),
        (segment) =>
          `${segment.x1}|${segment.x2}|${segment.y1}|${segment.y2}|${segment.color || ''}|${segment.lineStyle || 0}|${segment.lineWidth || 1}`,
      )

      const scopedPolylinesByPane = Object.fromEntries(
        Object.entries(polylinesByPane || {}).map(([paneKey, entries]) => [
          paneKey,
          (entries || [])
            .map((polyline) => {
              const points = (polyline?.points || [])
                .map((point) => ({
                  ...point,
                  time: toSec(point?.time),
                  price: toFiniteNumber(point?.price),
                }))
                .filter((point) => isWithinSeriesWindow(point?.time) && Number.isFinite(point?.price))
              if (!points.length) return null
              return { ...polyline, points }
            })
            .filter(Boolean),
        ]),
      )

      const scopedBubblesByPane = scopeEntriesByPane(
        bubblesByPane,
        (bubble) => {
          const next = { ...bubble, time: toSec(bubble?.time) }
          return isWithinSeriesWindow(next?.time) ? next : null
        },
      )

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

      const extentSignature = (() => {
        if (!scopedTradeSegments.length) return null
        const candidateTimes = scopedTradeSegments
          .flatMap((segment) => [segment.x1, segment.x2])
          .filter((value) => Number.isFinite(value))
        if (!candidateTimes.length) return null
        const min = Math.min(...candidateTimes)
        const max = Math.max(...candidateTimes)
        return `${min}-${max}-${scopedTradeSegments.length}`
      })()

      const extents = (() => {
        if (!extentSignature) return null
        const candidateTimes = scopedTradeSegments
          .flatMap((segment) => [segment.x1, segment.x2])
          .filter((value) => Number.isFinite(value))
        if (!candidateTimes.length) return null
        const min = Math.min(...candidateTimes)
        const max = Math.max(...candidateTimes)
        const span = Math.max(max - min, barSpacingRef.current ?? 30)
        const pad = Math.max(span * 0.05, barSpacingRef.current ?? 0)
        return { from: min - pad, to: max + pad }
      })()

      return {
        markersByPane: scopedMarkersByPane,
        markerDetails: scopedMarkerDetails,
        touchPointsByPane: scopedTouchPointsByPane,
        boxesByPane: scopedBoxesByPane,
        segmentsByPane: scopedSegmentsByPane,
        polylinesByPane: scopedPolylinesByPane,
        bubblesByPane: scopedBubblesByPane,
        priceLines: groupedPriceLines,
        tradeSegments: scopedTradeSegments,
        lastSeriesTime,
        extentSignature,
        extents,
      }
    },
    [barSpacingRef],
  )

  const applyArtifacts = useCallback(
    (artifacts = {}) => {
      const {
        markersByPane,
        markerDetails,
        touchPointsByPane,
        boxesByPane,
        segmentsByPane,
        polylinesByPane,
        bubblesByPane,
        priceLines,
        extentSignature,
        extents,
        lastSeriesTime,
      } = artifacts
      if (!seriesRef.current || !paneMgrRef.current) return { extentChanged: false }

      markerDetailsRef.current = markerDetails || []
      markerManager?.setLayer('base', markersByPane?.price || [])
      markerManager?.flush()

      const applyPriceLines = () => {
        const signature = (priceLines || []).map((line) => ({
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
          ;(priceLines || []).forEach((line) => {
            const isStop = line.role === 'sl'
            const isTarget = line.role === 'tp'
            const isEntry = line.role === 'level' && (line.labels[0] === 'Entry' || line.source === 'active_trade_entry')

            let baseColor = isStop ? '#ef4444' : isTarget ? '#10b981' : line.color || '#f59e0b'

            const lineColor = toRgba(baseColor, 0.85) || 'rgba(148,163,184,0.85)'
            const labelBg = toRgba(baseColor, 0.95) || 'rgba(148,163,184,0.9)'
            const precision = Number.isFinite(line.precision) ? line.precision : 2
            const priceLabel = Number(line.price).toFixed(precision)
            const labelSource = line.labels[0] || (isTarget ? 'TP' : isStop ? 'SL' : isEntry ? 'Entry' : 'Level')
            const labelCount = line.count > 1 && isTarget ? ` x${line.count}` : ''
            const badge = `${labelSource}${labelCount}`.trim()

            let title = `${badge || labelSource} | ${priceLabel}`
            if (isEntry && Number.isFinite(line.pnl) && Number.isFinite(line.pnlPercent)) {
              const pnlSign = line.pnl >= 0 ? '+' : ''
              const pnlValue = line.pnl.toFixed(2)
              const pnlPct = line.pnlPercent.toFixed(2)
              title = `Entry ${priceLabel} | ${pnlSign}${pnlValue} (${pnlSign}${pnlPct}%)`
            }
            const priceLineOptions = {
              price: line.price,
              color: lineColor,
              lineWidth: isStop ? 2.5 : 2,
              lineStyle: isStop ? 2 : 0,
              axisLabelVisible: true,
              axisLabelColor: labelBg,
              axisLabelTextColor: '#0b1620',
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

      applyPriceLines()

      const paneKeys = new Set([
        'price',
        ...Object.keys(markersByPane || {}),
        ...Object.keys(touchPointsByPane || {}),
        ...Object.keys(boxesByPane || {}),
        ...Object.keys(segmentsByPane || {}),
        ...Object.keys(polylinesByPane || {}),
        ...Object.keys(bubblesByPane || {}),
      ])
      paneMgrRef.current.syncActivePanes([...paneKeys])
      paneKeys.forEach((paneKey) => {
        if (paneKey !== 'price') {
          paneMgrRef.current.setMarkers(markersByPane?.[paneKey] || [], paneKey)
        }
        paneMgrRef.current.setTouchPoints(touchPointsByPane?.[paneKey] || [], paneKey)
        paneMgrRef.current.setVABlocks(
          boxesByPane?.[paneKey] || [],
          {
            lastSeriesTime,
            barSpacing: barSpacingRef.current,
          },
          paneKey,
        )
        paneMgrRef.current.setSegments(segmentsByPane?.[paneKey] || [], paneKey)
        paneMgrRef.current.setPolylines(polylinesByPane?.[paneKey] || [], paneKey)
        paneMgrRef.current.setSignalBubbles(bubblesByPane?.[paneKey] || [], paneKey)
      })

      const extentChanged = extentSignature && extentSignature !== tradeViewportSignatureRef.current
      if (extentChanged && BOTLENS_DEBUG) {
        console.debug('[BotLensChart] overlay extents changed', { signature: extentSignature, extents })
      }
      tradeViewportSignatureRef.current = extentSignature || null

      return { extentChanged, extents, signature: extentSignature }
    },
    [barSpacingRef, markerDetailsRef, markerManager, overlayHandlesRef, paneMgrRef, prevPriceLinesRef, seriesRef],
  )

  return { computeArtifacts, applyArtifacts }
}
