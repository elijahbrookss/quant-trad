import {
  isClosedTrade,
  projectTradeEventToCandle,
} from './tradeMarkerArtifacts.js'

const toOptionalNumber = (value) => {
  if (value === null || value === undefined || value === '') return null
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : null
}

const closedTradeExitPrice = (trade, entryPrice) => {
  const explicit = toOptionalNumber(trade?.exit_price)
  if (Number.isFinite(explicit) && !(explicit === 0 && Math.abs(entryPrice || 0) >= 1)) return explicit
  const legs = Array.isArray(trade?.legs) ? trade.legs : []
  const exitedLeg = legs.find((leg) => Number.isFinite(toOptionalNumber(leg?.exit_price)))
  const legPrice = toOptionalNumber(exitedLeg?.exit_price)
  return Number.isFinite(legPrice) ? legPrice : null
}

export const buildTradeFocusPulseMarkers = (trade, candleData = [], phase = 0) => {
  if (!trade || !isClosedTrade(trade)) return []
  const tradeId = String(trade?.trade_id || 'trade')
  const isLong = String(trade?.side || trade?.direction || '').trim().toLowerCase() !== 'short'
  const entryPrice = toOptionalNumber(trade?.entry_price)
  const exitPrice = closedTradeExitPrice(trade, entryPrice)
  const entryProjection = projectTradeEventToCandle(
    trade?.entry_time || trade?.opened_at || trade?.bar_time,
    candleData,
  )
  const exitProjection = projectTradeEventToCandle(
    trade?.exit_time || trade?.closed_at,
    candleData,
  )
  const expanded = Math.abs(Number(phase || 0)) % 2 === 1
  const size = expanded ? 2.7 : 1.45
  const alpha = expanded ? 0.98 : 0.58
  const netPnl = toOptionalNumber(trade?.net_pnl ?? trade?.trade_net_pnl)
  const exitColor = Number.isFinite(netPnl) && netPnl < 0
    ? 'rgba(248,113,113,' + alpha + ')'
    : 'rgba(34,211,238,' + alpha + ')'
  const markers = []

  if (Number.isFinite(entryProjection.time)) {
    markers.push({
      id: 'focus:' + tradeId + ':entry',
      trade_id: trade?.trade_id,
      time: entryProjection.time,
      position: Number.isFinite(entryPrice) ? 'atPriceMiddle' : isLong ? 'belowBar' : 'aboveBar',
      ...(Number.isFinite(entryPrice) ? { price: entryPrice } : {}),
      shape: 'circle',
      color: 'rgba(251,191,36,' + alpha + ')',
      text: 'SELECTED ENTRY',
      size,
    })
  }
  if (Number.isFinite(exitProjection.time)) {
    markers.push({
      id: 'focus:' + tradeId + ':exit',
      trade_id: trade?.trade_id,
      time: exitProjection.time,
      position: Number.isFinite(exitPrice) ? 'atPriceMiddle' : isLong ? 'aboveBar' : 'belowBar',
      ...(Number.isFinite(exitPrice) ? { price: exitPrice } : {}),
      shape: 'circle',
      color: exitColor,
      text: 'SELECTED EXIT',
      size,
    })
  }
  return markers
}
