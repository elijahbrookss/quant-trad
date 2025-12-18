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
      const detail = (markerDetailsRef.current || []).find((entry) => entry.time === epoch)
      if (detail) {
        setMarkerTooltip({ x: param.point.x, y: param.point.y, entries: detail.entries })
      } else {
        setMarkerTooltip(null)
      }
    }
    chartRef.current.subscribeCrosshairMove(handler)
    return () => chartRef.current?.unsubscribeCrosshairMove?.(handler)
  }, [chartRef, markerDetailsRef])

  return markerTooltip
}

