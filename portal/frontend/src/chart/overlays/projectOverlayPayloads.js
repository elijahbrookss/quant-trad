import { adaptPayload, getPaneKeyForOverlay, getPaneViewsForOverlay } from '../indicators/registry.js'
import { getPaneDefinition } from '../panes/registry.js'

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

export const appendToPaneGroup = (collection, paneKey, entries) => {
  if (!Array.isArray(entries) || !entries.length) return
  if (!collection[paneKey]) collection[paneKey] = []
  collection[paneKey].push(...entries)
}

export function projectOverlayPayloads({
  overlays = [],
  bubbleAlpha = 0.16,
  normalizeTime = (value) => value,
  onOverlayProjected = null,
} = {}) {
  const priceLines = []
  const markersByPane = {}
  const touchPointsByPane = {}
  const boxesByPane = {}
  const segmentsByPane = {}
  const polylinesByPane = {}
  const bubblesByPane = {}
  const signalDetails = []
  const legendEntriesByPane = {}

  const appendLegendEntry = (paneKey, entry) => {
    if (!entry?.label) return
    const pane = getPaneDefinition(paneKey)
    if (!pane?.showLegend) return
    if (!legendEntriesByPane[paneKey]) legendEntriesByPane[paneKey] = []
    const exists = legendEntriesByPane[paneKey].some(
      (candidate) =>
        candidate.label === entry.label
        && candidate.color === entry.color
        && candidate.overlayType === entry.overlayType,
    )
    if (!exists) {
      legendEntriesByPane[paneKey].push(entry)
    }
  }

  for (const overlay of overlays || []) {
    const { type, payload, color, ind_id } = overlay || {}
    if (!payload) continue

    const resolvedColor = overlay?.ui?.color || color || null
    const paneKey = getPaneKeyForOverlay(overlay)
    const paneViews = getPaneViewsForOverlay(overlay)
    const paneSet = new Set(paneViews || [])
    const normalized = adaptPayload(type, payload, resolvedColor)
    appendLegendEntry(paneKey, {
      label: overlay?.ui?.label || type || 'Overlay',
      color: resolvedColor,
      overlayType: type || null,
    })

    onOverlayProjected?.({
      overlay,
      paneKey,
      paneViews,
      normalized,
      resolvedColor,
    })

    if (Array.isArray(payload.price_lines) && payload.price_lines.length) {
      priceLines.push(
        ...payload.price_lines.map((line) => ({
          ...line,
          color: line?.color ?? resolvedColor,
          source: type || line?.title || 'overlay',
        })),
      )
    }

    appendToPaneGroup(markersByPane, paneKey, normalized.markers)

    if (Array.isArray(normalized.bubbles) && normalized.bubbles.length) {
      const tinted = resolvedColor
        ? normalized.bubbles.map((bubble) => ({
            ...bubble,
            accentColor: resolvedColor,
            backgroundColor: toRgba(resolvedColor, bubbleAlpha) || bubble.backgroundColor,
          }))
        : normalized.bubbles
      appendToPaneGroup(bubblesByPane, paneKey, tinted)
      for (const bubble of tinted) {
        const epoch = normalizeTime(bubble?.time)
        if (!Number.isFinite(epoch)) continue
        const lines = []
        const label = typeof bubble?.label === 'string' ? bubble.label.trim() : ''
        const detail = typeof bubble?.detail === 'string' ? bubble.detail.trim() : ''
        const meta = typeof bubble?.meta === 'string' ? bubble.meta.trim() : ''
        if (label) lines.push(label)
        if (detail) lines.push(detail)
        if (meta) lines.push(meta)
        if (lines.length) {
          signalDetails.push({ time: epoch, kind: 'signal', entries: lines })
        }
      }
    }

    const wantsTouch = paneSet.has('touch') || (Array.isArray(normalized.touchPoints) && normalized.touchPoints.length > 0)
    if (wantsTouch && Array.isArray(normalized.touchPoints) && normalized.touchPoints.length) {
      appendToPaneGroup(
        touchPointsByPane,
        paneKey,
        normalized.touchPoints
          .map((point) => ({
            ...point,
            time: normalizeTime(point.time),
            ind_id,
          }))
          .filter((point) => Number.isFinite(point.time)),
      )
    }

    const wantsBoxes = paneSet.has('va_box') || (Array.isArray(normalized.boxes) && normalized.boxes.length > 0)
    if (wantsBoxes && Array.isArray(normalized.boxes) && normalized.boxes.length) {
      appendToPaneGroup(boxesByPane, paneKey, normalized.boxes)
    }

    const wantsSegments = paneSet.has('segment') || (Array.isArray(normalized.segments) && normalized.segments.length > 0)
    if (wantsSegments && Array.isArray(normalized.segments) && normalized.segments.length) {
      appendToPaneGroup(segmentsByPane, paneKey, normalized.segments)
    }

    const wantsPolylines = paneSet.has('polyline') || (Array.isArray(normalized.polylines) && normalized.polylines.length > 0)
    if (wantsPolylines && Array.isArray(normalized.polylines) && normalized.polylines.length) {
      appendToPaneGroup(polylinesByPane, paneKey, normalized.polylines)
    }
  }

  return {
    priceLines,
    markersByPane,
    touchPointsByPane,
    boxesByPane,
    segmentsByPane,
    polylinesByPane,
    bubblesByPane,
    signalDetails,
    legendEntriesByPane,
  }
}
