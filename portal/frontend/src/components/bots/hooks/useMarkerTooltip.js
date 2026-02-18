import { useEffect, useState } from 'react'
import { toSec } from '../chartDataUtils.js'

export const useMarkerTooltip = ({ chartRef, markerDetailsRef }) => {
  const [markerTooltip, setMarkerTooltip] = useState(null)
  const nearestDetail = (details = [], epoch) => {
    if (!Array.isArray(details) || !Number.isFinite(epoch)) return null
    let best = null
    let bestDelta = Number.POSITIVE_INFINITY
    for (const detail of details) {
      const t = Number(detail?.time)
      if (!Number.isFinite(t)) continue
      const delta = Math.abs(t - epoch)
      if (delta < bestDelta) {
        bestDelta = delta
        best = detail
      }
    }
    if (!best || bestDelta > 1) return null
    return { detail: best, delta: bestDelta }
  }

  useEffect(() => {
    if (!chartRef.current) return undefined
    const handler = (param) => {
      const epoch = toSec(param?.time)
      if (!Number.isFinite(epoch) || !param?.point) {
        setMarkerTooltip(null)
        return
      }
      const allDetails = markerDetailsRef.current || []
      const details = allDetails.filter((entry) => Number(entry?.time) === Number(epoch))
      const resolved = details.length ? details : (() => {
        const nearest = nearestDetail(allDetails, epoch)
        return nearest ? [nearest.detail] : []
      })()
      if (resolved.length) {
        const entries = resolved.flatMap((detail) => (Array.isArray(detail?.entries) ? detail.entries : []))
        const kinds = [...new Set(resolved.map((detail) => detail?.kind).filter(Boolean))]
        setMarkerTooltip({ x: param.point.x, y: param.point.y, entries, kinds })
      } else {
        setMarkerTooltip(null)
      }
    }
    chartRef.current.subscribeCrosshairMove(handler)
    return () => chartRef.current?.unsubscribeCrosshairMove?.(handler)
  }, [chartRef, markerDetailsRef])

  return markerTooltip
}
