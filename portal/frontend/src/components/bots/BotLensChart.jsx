import { useEffect, useMemo, useRef } from 'react'
import { createChart, CandlestickSeries } from 'lightweight-charts'
import { useChartState } from '../../contexts/ChartStateContext.jsx'

const chartOptions = {
  layout: {
    textColor: '#d4d7e1',
    background: { type: 'solid', color: '#10121a' },
  },
  grid: {
    vertLines: { color: 'rgba(150, 150, 150, 0.05)' },
    horzLines: { color: 'rgba(150, 150, 150, 0.05)' },
  },
  timeScale: { borderVisible: false },
  rightPriceScale: { borderVisible: false },
}

const seriesOptions = {
  upColor: '#34d399',
  downColor: '#f97316',
  borderVisible: false,
  wickUpColor: '#34d399',
  wickDownColor: '#f97316',
  priceLineVisible: false,
}

const markerForTrade = (trade) => {
  const entryTime = trade?.entry_time ? Math.floor(new Date(trade.entry_time).getTime() / 1000) : null
  if (!entryTime) return []
  const isLong = trade.direction === 'long'
  const entryMarker = {
    time: entryTime,
    position: isLong ? 'belowBar' : 'aboveBar',
    shape: isLong ? 'arrowUp' : 'arrowDown',
    color: isLong ? '#34d399' : '#f97316',
    text: `${isLong ? 'Buy' : 'Sell'} ${trade.legs?.length || 0}x`,
  }
  const exitMarkers = []
  for (const leg of trade.legs || []) {
    if (!leg?.exit_time || !leg?.status) continue
    const ts = Math.floor(new Date(leg.exit_time).getTime() / 1000)
    exitMarkers.push({
      time: ts,
      position: isLong ? 'aboveBar' : 'belowBar',
      shape: leg.status === 'target' ? 'circle' : 'square',
      color: leg.status === 'target' ? '#22d3ee' : '#f87171',
      text: `${leg.name} ${leg.status === 'target' ? 'TP' : 'SL'}`,
    })
  }
  return [entryMarker, ...exitMarkers]
}

export function BotLensChart({ chartId, candles = [], trades = [] }) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)
  const seriesRef = useRef(null)
  const resizeObserverRef = useRef(null)
  const { registerChart } = useChartState()

  const candleData = useMemo(() => {
    return candles.map((candle) => ({
      time: Math.floor(new Date(candle.time).getTime() / 1000),
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
    }))
  }, [candles])

  const markers = useMemo(() => {
    return trades.flatMap((trade) => markerForTrade(trade))
  }, [trades])

  useEffect(() => {
    const el = containerRef.current
    if (!el || chartRef.current) return
    const chart = createChart(el, {
      ...chartOptions,
      width: el.clientWidth,
      height: el.clientHeight || 360,
    })
    const series = chart.addSeries(CandlestickSeries, seriesOptions)
    chartRef.current = chart
    seriesRef.current = series
    registerChart?.(chartId, { chart, series })

    resizeObserverRef.current = new ResizeObserver(([entry]) => {
      const rect = entry?.contentRect
      if (!rect || !chartRef.current) return
      chartRef.current.applyOptions({ width: rect.width, height: rect.height })
    })
    resizeObserverRef.current.observe(el)

    return () => {
      resizeObserverRef.current?.disconnect()
      resizeObserverRef.current = null
      seriesRef.current = null
      chartRef.current?.remove()
      chartRef.current = null
    }
  }, [chartId, registerChart])

  useEffect(() => {
    if (!seriesRef.current) return
    seriesRef.current.setData(candleData)
    seriesRef.current.setMarkers(markers)
  }, [candleData, markers])

  return (
    <div
      ref={containerRef}
      className="h-[360px] w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0f1118]"
    />
  )
}
