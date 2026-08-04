import { useCallback, useEffect, useRef } from 'react'
import { toFiniteNumber, toSec } from '../chartDataUtils.js'
import { isClosedTrade, projectTradeEventToCandle } from './useTradeMarkers.js'

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
    ? `rgba(248,113,113,${alpha})`
    : `rgba(34,211,238,${alpha})`
  const markers = []

  if (Number.isFinite(entryProjection.time)) {
    markers.push({
      id: `focus:${tradeId}:entry`,
      trade_id: trade?.trade_id,
      time: entryProjection.time,
      position: Number.isFinite(entryPrice) ? 'atPriceMiddle' : isLong ? 'belowBar' : 'aboveBar',
      ...(Number.isFinite(entryPrice) ? { price: entryPrice } : {}),
      shape: 'circle',
      color: `rgba(251,191,36,${alpha})`,
      text: 'SELECTED ENTRY',
      size,
    })
  }
  if (Number.isFinite(exitProjection.time)) {
    markers.push({
      id: `focus:${tradeId}:exit`,
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

export const usePulseMarkers = ({ seriesRef, markerManager, latestCandlesRef }) => {
  const pulseLineHandlesRef = useRef([])
  const pulseTimeoutRef = useRef(null)
  const pulseIntervalRef = useRef(null)

  const clearPulseArtifacts = useCallback(() => {
    if (pulseTimeoutRef.current) clearTimeout(pulseTimeoutRef.current)
    if (pulseIntervalRef.current) clearInterval(pulseIntervalRef.current)
    pulseTimeoutRef.current = null
    pulseIntervalRef.current = null
    pulseLineHandlesRef.current.forEach((handle) => {
      try {
        seriesRef.current?.removePriceLine(handle)
      } catch {
        /* noop */
      }
    })
    pulseLineHandlesRef.current = []
    markerManager?.clearLayer('pulse')
    markerManager?.flush()
  }, [markerManager, seriesRef])

  const pulseTradeElements = useCallback(
    (trade) => {
      if (!trade || !seriesRef.current) return
      clearPulseArtifacts()
      if (isClosedTrade(trade)) {
        let phase = 0
        const renderPulse = () => {
          markerManager?.setLayer(
            'pulse',
            buildTradeFocusPulseMarkers(trade, latestCandlesRef?.current || [], phase),
          )
          markerManager?.flush()
          phase += 1
        }
        renderPulse()
        pulseIntervalRef.current = setInterval(renderPulse, 180)
        pulseTimeoutRef.current = setTimeout(clearPulseArtifacts, 2200)
        return
      }
      const entryTime = toSec(trade?.entry_time)
      const stopPrice = toFiniteNumber(trade?.stop_price)
      const targets = Array.from(
        new Set(
          (trade.legs || [])
            .map((leg) => toFiniteNumber(leg?.target_price))
            .filter((value) => Number.isFinite(value)),
        ),
      )
      const pulseMarkers = []
      if (Number.isFinite(entryTime)) {
        pulseMarkers.push({
          time: entryTime,
          position: (trade?.direction || '').toLowerCase() === 'short' ? 'aboveBar' : 'belowBar',
          shape: 'circle',
          color: 'rgba(125,211,252,0.95)',
          text: ' ',
        })
      }
      const lineFor = (price, isTarget = false) => {
        if (!Number.isFinite(price)) return null
        return seriesRef.current.createPriceLine({
          price,
          color: isTarget ? 'rgba(16,185,129,0.85)' : 'rgba(239,68,68,0.85)',
          lineWidth: isTarget ? 2 : 2.5,
          lineStyle: isTarget ? 0 : 2,
          axisLabelVisible: true,
          axisLabelColor: isTarget ? 'rgba(16,185,129,0.95)' : 'rgba(239,68,68,0.95)',
          axisLabelTextColor: '#0b1620',
        })
      }
      if (Number.isFinite(stopPrice)) {
        const handle = lineFor(stopPrice, false)
        if (handle) pulseLineHandlesRef.current.push(handle)
      }
      targets.forEach((price) => {
        const handle = lineFor(price, true)
        if (handle) pulseLineHandlesRef.current.push(handle)
      })
      if (pulseMarkers.length) {
        markerManager?.setLayer('pulse', pulseMarkers, { ttlMs: 450 })
        markerManager?.flush()
      }
      pulseTimeoutRef.current = setTimeout(() => {
        clearPulseArtifacts()
      }, 450)
    },
    [clearPulseArtifacts, latestCandlesRef, markerManager, seriesRef],
  )

  useEffect(() => {
    return () => {
      clearPulseArtifacts()
    }
  }, [clearPulseArtifacts])

  return { pulseTradeElements, clearPulseArtifacts }
}
