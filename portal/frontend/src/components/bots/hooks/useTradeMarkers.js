import { useMemo } from 'react'
import { buildTradeMarkerArtifacts } from './tradeMarkerArtifacts.js'

export {
  buildTradeMarkerArtifacts,
  isClosedTrade,
  projectTradeEventToCandle,
} from './tradeMarkerArtifacts.js'

export const useTradeMarkers = (
  trades = [],
  candleLookup = new Map(),
  candleData = [],
  { selectedTradeId = null, showActiveTradeLevels = true } = {},
) => {
  return useMemo(
    () => buildTradeMarkerArtifacts(trades, candleLookup, candleData, { selectedTradeId, showActiveTradeLevels }),
    [candleData, candleLookup, selectedTradeId, showActiveTradeLevels, trades],
  )
}
