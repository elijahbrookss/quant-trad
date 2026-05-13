import { useMemo } from 'react'
import { coalesce, toSec } from '../chartDataUtils.js'

const MAX_PRICE_LINE_TRADES = 12

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

const cleanText = (value) => String(value || '').trim()

const toTradeNumber = (value) => {
  if (value === null || value === undefined || value === '') return null
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : null
}

const toTradePrice = (value, referencePrice = null) => {
  const numeric = toTradeNumber(value)
  if (!Number.isFinite(numeric)) return null
  const reference = toTradeNumber(referencePrice)
  if (numeric === 0 && Number.isFinite(reference) && Math.abs(reference) >= 1) return null
  return numeric
}

const normalizeSide = (trade) => cleanText(trade?.side || trade?.direction).toLowerCase()

const normalizeStatus = (value) => cleanText(value).toLowerCase()

const tradeStatus = (trade) => normalizeStatus(trade?.status || trade?.trade_state)

const isClosedTrade = (trade) => {
  const status = tradeStatus(trade)
  return Boolean(trade?.closed_at || trade?.exit_time || status === 'closed' || status === 'completed' || status === 'complete')
}

const closeReason = (trade) => cleanText(trade?.close_reason || trade?.reason_code || trade?.exit_reason).toUpperCase()

const isBacktestEnd = (trade, leg = null) => (
  closeReason(trade) === 'BACKTEST_END' || normalizeStatus(leg?.status) === 'backtest_end'
)

const isTargetLike = (leg) => {
  const status = normalizeStatus(leg?.status || leg?.exit_kind || leg?.type || leg?.reason_code)
  return status.includes('target') || status === 'tp' || status === 'take_profit'
}

const isStopLike = (trade, leg) => {
  const status = normalizeStatus(leg?.status || leg?.exit_kind || leg?.type || leg?.reason_code || closeReason(trade))
  return status.includes('stop') || status === 'sl' || status === 'stop_loss'
}

const formatNumber = (value, digits = 2) => {
  const numeric = toTradeNumber(value)
  if (!Number.isFinite(numeric)) return null
  return numeric.toFixed(digits)
}

const formatMoney = (value) => {
  const numeric = toTradeNumber(value)
  if (!Number.isFinite(numeric)) return null
  const sign = numeric > 0 ? '+' : ''
  return `${sign}${numeric.toFixed(2)}`
}

const formatDelta = (entry, exit) => {
  const start = toTradeNumber(entry)
  const end = toTradeNumber(exit)
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null
  const delta = end - start
  const percent = start ? (delta / start) * 100 : null
  const sign = delta >= 0 ? '+' : ''
  const deltaLabel = `${sign}${delta.toFixed(2)}`
  const percentLabel = Number.isFinite(percent) ? `${sign}${percent.toFixed(2)}%` : null
  return { delta, percent, label: percentLabel ? `${deltaLabel} (${percentLabel})` : deltaLabel }
}

const tooltipLine = (label, value) => {
  if (value === null || value === undefined || value === '') return null
  return `${label}: ${value}`
}

const getLegs = (trade) => (Array.isArray(trade?.legs) ? trade.legs.filter((leg) => leg && typeof leg === 'object') : [])

const getEntryTime = (trade) => toSec(coalesce(trade?.entry_time, trade?.opened_at, trade?.bar_time))

const getExitTime = (trade) => toSec(coalesce(trade?.exit_time, trade?.closed_at))

const getQuantity = (trade) => toTradeNumber(coalesce(trade?.quantity, trade?.qty, trade?.filled_qty))

const weightedExitPrice = (legs, referencePrice = null) => {
  let total = 0
  let weighted = 0
  for (const leg of legs || []) {
    const price = toTradePrice(leg?.exit_price, referencePrice)
    const contracts = toTradeNumber(leg?.contracts)
    if (!Number.isFinite(price) || !Number.isFinite(contracts)) continue
    const safeContracts = Math.max(contracts, 0)
    weighted += price * safeContracts
    total += safeContracts
  }
  return total > 0 ? weighted / total : null
}

const resolveExitPrice = (trade, legs = getLegs(trade), referencePrice = null) => {
  const explicit = toTradePrice(trade?.exit_price, referencePrice)
  if (Number.isFinite(explicit)) return explicit
  const weighted = weightedExitPrice(legs, referencePrice)
  if (Number.isFinite(weighted)) return weighted
  return null
}

const resolveCurrentTradeEnd = (trade, candleData = []) => {
  const explicitExit = getExitTime(trade)
  if (Number.isFinite(explicitExit)) return explicitExit
  const last = candleData[candleData.length - 1]?.time
  return Number.isFinite(last) ? last : getEntryTime(trade)
}

const markerTooltipEntries = (trade, { entryPrice, exitPrice, label, leg = null } = {}) => {
  const reason = closeReason(trade)
  const delta = formatDelta(entryPrice, exitPrice)
  return [
    tooltipLine('Trade', trade?.trade_id),
    tooltipLine('Side', normalizeSide(trade).toUpperCase()),
    tooltipLine('Entry', formatNumber(entryPrice, 4)),
    tooltipLine('Exit', formatNumber(exitPrice, 4)),
    tooltipLine('Stop', formatNumber(trade?.stop_price, 4)),
    tooltipLine('Quantity', formatNumber(getQuantity(trade), 4)),
    tooltipLine('Gross PnL', formatMoney(coalesce(trade?.gross_pnl, trade?.realized_pnl, leg?.gross_pnl, leg?.pnl))),
    tooltipLine('Fees', formatMoney(trade?.fees_paid)),
    tooltipLine('Net PnL', formatMoney(coalesce(trade?.net_pnl, trade?.trade_net_pnl))),
    tooltipLine('Close Reason', reason || label),
    delta ? `Delta: ${delta.label}` : null,
  ].filter(Boolean)
}

const exitMarkerStyle = (trade, legs) => {
  if (isBacktestEnd(trade, legs[0])) {
    return { color: 'rgba(148,163,184,0.9)', shape: 'arrowDown', text: 'END', kind: 'backtest_end' }
  }
  if (legs.some((leg) => isStopLike(trade, leg))) {
    return { color: 'rgba(248,113,113,0.9)', shape: 'square', text: 'SL', kind: 'stop' }
  }
  if (legs.some((leg) => isTargetLike(leg))) {
    return { color: 'rgba(34,211,238,0.9)', shape: 'circle', text: 'TP', kind: 'target' }
  }
  return { color: 'rgba(148,163,184,0.82)', shape: 'circle', text: 'Exit', kind: 'exit' }
}

const markerForTrade = (trade) => {
  const entryTime = getEntryTime(trade)
  const entryPrice = toTradePrice(trade?.entry_price)
  if (!Number.isFinite(entryTime)) return []

  const side = normalizeSide(trade)
  const isLong = side !== 'short'
  const legs = getLegs(trade)
  const markers = []
  const entryText = side ? `${side.toUpperCase()} Entry` : 'Entry'
  markers.push({
    time: entryTime,
    position: Number.isFinite(entryPrice) ? 'atPriceMiddle' : isLong ? 'belowBar' : 'aboveBar',
    ...(Number.isFinite(entryPrice) ? { price: entryPrice } : {}),
    shape: 'square',
    color: 'rgba(245,158,11,0.95)',
    text: entryText,
    kind: 'entry',
    tooltip: { entries: markerTooltipEntries(trade, { entryPrice, label: 'Entry' }) },
  })

  const grouped = new Map()
  for (const leg of legs) {
    const exitTime = toSec(leg?.exit_time)
    if (!Number.isFinite(exitTime)) continue
    if (!grouped.has(exitTime)) grouped.set(exitTime, [])
    grouped.get(exitTime).push(leg)
  }
  if (!grouped.size && isClosedTrade(trade)) {
    const exitTime = getExitTime(trade)
    if (Number.isFinite(exitTime)) grouped.set(exitTime, [{ status: closeReason(trade) || 'closed', exit_price: trade?.exit_price }])
  }

  for (const [time, exitLegs] of grouped.entries()) {
    const exitPrices = exitLegs
      .map((leg) => toTradePrice(coalesce(leg?.exit_price, leg?.target_price, trade?.exit_price), entryPrice))
      .filter(Number.isFinite)
    const avgExitPrice = exitPrices.length ? exitPrices.reduce((a, b) => a + b, 0) / exitPrices.length : resolveExitPrice(trade, exitLegs, entryPrice)
    const style = exitMarkerStyle(trade, exitLegs)
    const delta = formatDelta(entryPrice, avgExitPrice)
    const countLabel = exitLegs.length > 1 ? ` x${exitLegs.length}` : ''
    markers.push({
      time,
      position: Number.isFinite(avgExitPrice) ? 'atPriceMiddle' : isLong ? 'aboveBar' : 'belowBar',
      ...(Number.isFinite(avgExitPrice) ? { price: avgExitPrice } : {}),
      shape: style.shape,
      color: style.color,
      text: `${style.text}${countLabel}${delta ? ` ${delta.label}` : ''}`,
      kind: style.kind,
      tooltip: {
        entries: exitLegs
          .flatMap((leg) => markerTooltipEntries(trade, {
            entryPrice,
            exitPrice: coalesce(leg?.exit_price, leg?.target_price, avgExitPrice),
            label: style.text,
            leg,
          }))
          .filter(Boolean),
      },
    })
  }

  return markers
}

const buildTradeRegions = (trades, candleLookup, candleData = []) => {
  if (!Array.isArray(trades)) return []
  const regions = []
  for (const trade of trades) {
    const entryTime = getEntryTime(trade)
    if (!Number.isFinite(entryTime)) continue
    const exitTime = resolveCurrentTradeEnd(trade, candleData)
    if (!Number.isFinite(exitTime) || exitTime <= entryTime) continue
    const isLong = normalizeSide(trade) !== 'short'
    const net = toTradeNumber(coalesce(trade?.net_pnl, trade?.trade_net_pnl))
    const baseColor = Number.isFinite(net)
      ? net >= 0 ? '#22d3ee' : '#f87171'
      : isLong ? '#34d399' : '#f97316'
    const entryPrice = toTradePrice(trade?.entry_price)
    const exitPrice = resolveExitPrice(trade, getLegs(trade), entryPrice)
    const entryCandle = candleLookup.get(entryTime)
    const exitCandle = candleLookup.get(exitTime)
    const prices = [
      entryPrice,
      exitPrice,
      toTradePrice(trade?.stop_price, entryPrice),
      ...getLegs(trade).map((leg) => toTradePrice(leg?.target_price, entryPrice)),
      toTradePrice(coalesce(entryCandle?.close, entryCandle?.open), entryPrice),
      toTradePrice(coalesce(exitCandle?.close, exitCandle?.open), entryPrice),
    ].filter(Number.isFinite)
    if (!prices.length) continue
    regions.push({
      x1: entryTime,
      x2: exitTime,
      y1: Math.min(...prices),
      y2: Math.max(...prices),
      color: toRgba(baseColor, 0.065),
      border: { color: toRgba(baseColor, 0.24), width: 1 },
      precision: 4,
    })
  }
  return regions
}

const buildTradeSegments = (trades, candleData = []) => {
  if (!Array.isArray(trades)) return []
  return trades
    .map((trade) => {
      const entryTime = getEntryTime(trade)
      const endTime = resolveCurrentTradeEnd(trade, candleData)
      const entryPrice = toTradePrice(trade?.entry_price)
      const exitPrice = resolveExitPrice(trade, getLegs(trade), entryPrice)
      const lastPrice = toTradePrice(candleData[candleData.length - 1]?.close, entryPrice)
      const y2 = Number.isFinite(exitPrice) ? exitPrice : lastPrice
      if (![entryTime, endTime, entryPrice, y2].every(Number.isFinite) || endTime <= entryTime) return null
      const net = toTradeNumber(coalesce(trade?.net_pnl, trade?.trade_net_pnl))
      const side = normalizeSide(trade)
      const color = Number.isFinite(net)
        ? net >= 0 ? 'rgba(34,211,238,0.92)' : 'rgba(248,113,113,0.92)'
        : side === 'short' ? 'rgba(249,115,22,0.82)' : 'rgba(52,211,153,0.82)'
      return {
        x1: entryTime,
        x2: endTime,
        y1: entryPrice,
        y2,
        color,
        lineWidth: isClosedTrade(trade) ? 2 : 2.5,
        lineStyle: isClosedTrade(trade) ? 0 : 2,
        trade_id: trade?.trade_id,
      }
    })
    .filter(Boolean)
}

const buildTradePriceLines = (trades, candleData) => {
  if (!Array.isArray(trades)) return []
  const ordered = [...trades]
    .filter((trade) => !isClosedTrade(trade))
    .sort((left, right) => (getEntryTime(left) || 0) - (getEntryTime(right) || 0))
    .slice(-MAX_PRICE_LINE_TRADES)
  const priceLines = []
  const lastCandle = candleData[candleData.length - 1]
  for (const trade of ordered) {
    const entryTime = getEntryTime(trade)
    const entryPrice = toTradePrice(trade?.entry_price)
    if (!Number.isFinite(entryTime) || !Number.isFinite(entryPrice)) continue
    const currentPrice = toTradePrice(lastCandle?.close, entryPrice)
    const isLong = normalizeSide(trade) !== 'short'
    const pnl = Number.isFinite(currentPrice) ? (isLong ? currentPrice - entryPrice : entryPrice - currentPrice) : null
    const pnlPercent = Number.isFinite(pnl) ? (pnl / entryPrice) * 100 : null
    priceLines.push({
      price: entryPrice,
      title: 'Entry',
      color: '#f59e0b',
      source: 'active_trade_entry',
      precision: 2,
      pnl,
      pnlPercent,
    })
    const stopPrice = toTradePrice(trade?.stop_price, entryPrice)
    if (Number.isFinite(stopPrice)) {
      priceLines.push({
        price: stopPrice,
        title: 'SL',
        color: '#ef4444',
        source: 'active_trade_sl',
        precision: 2,
      })
    }
    for (const leg of getLegs(trade).filter((entry) => !entry?.exit_time && normalizeStatus(entry?.status) !== 'filled')) {
      const targetPrice = toTradePrice(leg?.target_price, entryPrice)
      if (!Number.isFinite(targetPrice)) continue
      priceLines.push({
        price: targetPrice,
        title: 'TP',
        color: '#10b981',
        source: 'active_trade_tp',
        precision: 2,
      })
    }
  }
  return priceLines
}

export const buildTradeMarkerArtifacts = (trades = [], candleLookup = new Map(), candleData = []) => {
  const resolvedTrades = Array.isArray(trades) ? trades : []
  const markers = []
  const tooltips = []
  for (const trade of resolvedTrades) {
    const entries = markerForTrade(trade)
    markers.push(...entries)
    entries
      .filter((entry) => Array.isArray(entry?.tooltip?.entries))
      .forEach((entry) => {
        tooltips.push({ time: entry.time, entries: entry.tooltip.entries, kind: entry.kind, trade_id: trade?.trade_id })
      })
  }

  return {
    markers: markers.sort((a, b) => (a.time ?? 0) - (b.time ?? 0)),
    tooltips,
    regions: buildTradeRegions(resolvedTrades, candleLookup, candleData),
    segments: buildTradeSegments(resolvedTrades, candleData),
    priceLines: buildTradePriceLines(resolvedTrades, candleData),
  }
}

export const useTradeMarkers = (trades = [], candleLookup = new Map(), candleData = []) => {
  return useMemo(
    () => buildTradeMarkerArtifacts(trades, candleLookup, candleData),
    [candleData, candleLookup, trades],
  )
}
