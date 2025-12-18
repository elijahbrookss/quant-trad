import { useMemo } from 'react'
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

const buildTradeRegions = (trades, candleLookup) => {
  if (!Array.isArray(trades)) return []
  const regions = []
  for (const trade of trades) {
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
}

const buildTradePriceLines = (trades, candleData) => {
  if (!Array.isArray(trades)) return []
  const priceLines = []
  const lastCandle = candleData[candleData.length - 1]
  for (const trade of trades) {
    const entryTime = toSec(trade?.entry_time)
    const entryPrice = toFiniteNumber(trade?.entry_price)
    if (!Number.isFinite(entryTime) || !Number.isFinite(entryPrice)) continue

    const hasOpenLegs = (trade.legs || []).some((leg) => !leg?.exit_time || leg.status === 'open')
    if (!hasOpenLegs) continue

    const currentPrice = toFiniteNumber(lastCandle?.close)
    const isLong = (trade?.direction || '').toLowerCase() === 'long'
    let pnl = null
    let pnlPercent = null
    if (Number.isFinite(currentPrice) && Number.isFinite(entryPrice)) {
      pnl = isLong ? currentPrice - entryPrice : entryPrice - currentPrice
      pnlPercent = (pnl / entryPrice) * 100
    }

    priceLines.push({
      price: entryPrice,
      title: 'Entry',
      color: '#94a3b8',
      source: 'active_trade_entry',
      precision: 2,
      pnl,
      pnlPercent,
    })

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
  return priceLines
}

export const useTradeMarkers = (trades = [], candleLookup = new Map(), candleData = []) => {
  return useMemo(() => {
    const resolvedTrades = Array.isArray(trades) ? trades : []
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
      regions: buildTradeRegions(resolvedTrades, candleLookup),
      priceLines: buildTradePriceLines(resolvedTrades, candleData),
    }
  }, [candleData, candleLookup, trades])
}
