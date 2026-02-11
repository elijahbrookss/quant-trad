import { useEffect, useState } from 'react'
import { toSec } from '../chartDataUtils.js'

export const useMarkerTooltip = ({ chartRef, markerDetailsRef }) => {
  const [markerTooltip, setMarkerTooltip] = useState(null)

  useEffect(() => {
    if (!chartRef.current) return undefined
    const handler = (param) => {
      const epoch = toSec(param?.time)
      if (!Number.isFinite(epoch) || !param?.point) {
        setMarkerTooltip(null)
        return
      }
      const details = (markerDetailsRef.current || []).filter((entry) => entry.time === epoch)
      if (details.length) {
        const entries = details.flatMap((detail) => (Array.isArray(detail?.entries) ? detail.entries : []))
        const kinds = [...new Set(details.map((detail) => detail?.kind).filter(Boolean))]
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
