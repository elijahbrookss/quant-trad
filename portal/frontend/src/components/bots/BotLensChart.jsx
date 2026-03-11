import { useEffect, useMemo, useRef, useState } from 'react'
import { useChartState } from '../../contexts/ChartStateContext.jsx'
import { BOTLENS_DEBUG, buildCandleLookup } from './chartDataUtils.js'
import { useCameraLock } from './hooks/useCameraLock.js'
import { useOverlaySync } from './hooks/useOverlaySync.js'
import { useTradeMarkers } from './hooks/useTradeMarkers.js'
import { useBotLensChartCore } from './hooks/useBotLensChartCore.js'
import { usePulseMarkers } from './hooks/usePulseMarkers.js'
import { useMarkerTooltip } from './hooks/useMarkerTooltip.js'
import { useIntrabarCandleAnimator, AnimatorStates } from './hooks/useIntrabarCandleAnimator.js'
import { useMarkerManager } from './hooks/useMarkerManager.js'
import { CameraIntents } from './hooks/useViewportController.js'
import { MarkerTooltip } from './MarkerTooltip.jsx'
import { RegimeReadoutBar } from './RegimeReadoutBar.jsx'
import { createLogger } from '../../utils/logger.js'
import {
  buildCandleSnapshots,
  buildReadoutSnapshot,
  buildRegimeBlockSnapshots,
  findNearestCandleTime,
} from './regimeReadoutUtils.js'
import { validateCanonicalCandles } from './botlensProjection.js'

const AUTO_FIT_OVERLAY_EXTENTS = String(import.meta.env?.VITE_BOTLENS_AUTO_FIT_OVERLAY_EXTENTS || '')
  .trim()
  .toLowerCase() === 'true'

const parseTimeframeToSeconds = (rawTimeframe) => {
  const text = (rawTimeframe || '').toString().trim().toLowerCase()
  if (!text) return null
  const match = text.match(/^(\d+)\s*([a-z]+)$/)
  if (!match) return null
  const amount = Number(match[1])
  const unit = match[2]
  if (!Number.isFinite(amount) || amount <= 0) return null

  if (unit === 's' || unit === 'sec' || unit === 'secs' || unit === 'second' || unit === 'seconds') {
    return amount
  }
  if (unit === 'm' || unit === 'min' || unit === 'mins' || unit === 'minute' || unit === 'minutes') {
    return amount * 60
  }
  if (unit === 'h' || unit === 'hr' || unit === 'hrs' || unit === 'hour' || unit === 'hours') {
    return amount * 3600
  }
  if (unit === 'd' || unit === 'day' || unit === 'days') {
    return amount * 86400
  }
  if (unit === 'w' || unit === 'wk' || unit === 'wks' || unit === 'week' || unit === 'weeks') {
    return amount * 7 * 86400
  }
  if (unit === 'mo' || unit === 'mon' || unit === 'month' || unit === 'months') {
    return amount * 30 * 86400
  }
  if (unit === 'y' || unit === 'yr' || unit === 'yrs' || unit === 'year' || unit === 'years') {
    return amount * 365 * 86400
  }
  return null
}

const buildTickMarkFormatter = (timeframeSeconds) => {
  const intraday = Number.isFinite(timeframeSeconds) && timeframeSeconds < 86400
  const minuteGranularity = Number.isFinite(timeframeSeconds) && timeframeSeconds < 3600
  const intradayFormatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
  })
  const dayFormatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: '2-digit',
    day: '2-digit',
  })
  const dateFormatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: '2-digit',
  })

  return (timeValue) => {
    const epoch = typeof timeValue === 'number'
      ? timeValue
      : typeof timeValue?.timestamp === 'function'
        ? Number(timeValue.timestamp())
        : Number.isFinite(timeValue?.timestamp)
          ? Number(timeValue.timestamp)
          : null
    if (!Number.isFinite(epoch)) return ''
    const date = new Date(epoch * 1000)
    if (Number.isNaN(date.getTime())) return ''

    if (!intraday) {
      return dateFormatter.format(date)
    }

    if (minuteGranularity) {
      return intradayFormatter.format(date)
    }

    const hour = date.getUTCHours()
    const minute = date.getUTCMinutes()
    if (hour === 0 && minute === 0) {
      return dayFormatter.format(date)
    }
    return intradayFormatter.format(date)
  }
}

const deriveTimeScaleOptions = (timeframe) => {
  const timeframeSeconds = parseTimeframeToSeconds(timeframe)
  const intraday = Number.isFinite(timeframeSeconds) && timeframeSeconds < 86400
  const showSeconds = Number.isFinite(timeframeSeconds) && timeframeSeconds < 60
  return {
    borderVisible: false,
    timeVisible: intraday || !Number.isFinite(timeframeSeconds),
    secondsVisible: showSeconds,
    tickMarkFormatter: buildTickMarkFormatter(timeframeSeconds),
  }
}

const chartOptions = {
  layout: {
    textColor: '#d4d7e1',
    background: { type: 'solid', color: '#10121a' },
  },
  grid: {
    vertLines: { color: 'rgba(150, 150, 150, 0.05)' },
    horzLines: { color: 'rgba(150, 150, 150, 0.05)' },
  },
  timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
  rightPriceScale: {
    borderVisible: false,
    scaleMargins: {
      top: 0.1,
      bottom: 0.1,
    },
  },
}

const seriesOptions = {
  upColor: '#34d399',
  downColor: '#f97316',
  borderVisible: false,
  wickUpColor: '#34d399',
  wickDownColor: '#f97316',
  priceLineVisible: false,
}

export function BotLensChart({
  chartId,
  candles = [],
  trades = [],
  overlays = [],
  playbackSpeed = 1,
  mode,
  debugRanges = false,
  className = '',
  heightClass = 'h-[360px]',
  timeframe = null,
  overlayVisibility = {},
  followLive = true,
}) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)
  const seriesRef = useRef(null)
  const levelSeriesRef = useRef(null)
  const paneMgrRef = useRef(null)
  const markersApiRef = useRef(null)
  const overlayHandlesRef = useRef({ priceLines: [] })
  const barSpacingRef = useRef(null)
  const latestCandlesRef = useRef([])
  const [hoveredEpoch, setHoveredEpoch] = useState(null)
  const hoveredEpochRef = useRef(null)
  const hoverRafRef = useRef(0)
  const seriesInstanceRef = useRef(null)
  const markerCacheRef = useRef([])
  const prevPriceLinesRef = useRef([])
  const markerDetailsRef = useRef([])
  const prevCandleDataRef = useRef([])
  const diagLoggedRef = useRef(false)
  const frameSampleRef = useRef({ total: 0, count: 0, logged: false })
  const pendingCameraIntentRef = useRef(null)
  const { registerChart } = useChartState()
  const logger = useMemo(() => createLogger('BotLensChart', { chartId }), [chartId])

  const resolvedCandles = Array.isArray(candles) ? candles : []
  const resolvedTrades = Array.isArray(trades) ? trades : []
  const resolvedOverlays = Array.isArray(overlays) ? overlays : []
  const instantPlayback = Number(playbackSpeed) <= 0 || String(mode || '').toLowerCase() === 'instant'
  const playbackProfile = useMemo(() => {
    const speed = Number(playbackSpeed)
    const isFast = Number.isFinite(speed) && speed > 1
    return {
      speed,
      isFast,
      allowIntrabar: !isFast && !instantPlayback,
    }
  }, [instantPlayback, playbackSpeed])
  const showRegimeReadout = overlayVisibility.regime_readout !== false

  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    chart.applyOptions({ timeScale: deriveTimeScaleOptions(timeframe) })
  }, [timeframe])

  useEffect(() => {
    const summary = resolvedOverlays.reduce((acc, ov) => {
      const type = ov?.type || 'unknown'
      acc[type] = (acc[type] || 0) + 1
      return acc
    }, {})
    const regime = summary.regime_overlay || 0
    const regimeMarkers = summary.regime_markers || 0
    if (BOTLENS_DEBUG) {
      logger.debug('overlay_render_input', {
        overlays_total: resolvedOverlays.length,
        overlays_by_type: summary,
        regime_overlay: regime,
        regime_markers: regimeMarkers,
      })
      console.debug('[BotLensChart] overlays received', { total: resolvedOverlays.length, summary, regime, regimeMarkers })
    }
  }, [logger, resolvedOverlays])

  const candleData = resolvedCandles
  const candleLookup = useMemo(() => buildCandleLookup(candleData), [candleData])
  const candleLookupRef = useRef(candleLookup)

  useEffect(() => {
    latestCandlesRef.current = candleData
  }, [candleData])

  useEffect(() => {
    candleLookupRef.current = candleLookup
  }, [candleLookup])

  useEffect(() => {
    if (!candleData.length) {
      diagLoggedRef.current = false
      return
    }
    const violation = validateCanonicalCandles(candleData)
    if (violation) {
      console.error('[BotLensChart] Candle order violation', {
        chartId,
        count: candleData.length,
        ...violation,
      })
      return
    }
    if (BOTLENS_DEBUG && !diagLoggedRef.current) {
      const first = candleData[0]?.time
      const last = candleData[candleData.length - 1]?.time
      console.debug('[BotLensChart] Candle range', {
        chartId,
        count: candleData.length,
        first,
        last,
      })
      if (debugRanges) {
        logger.info('candles_validated', {
          count: candleData.length,
          first,
          last,
        })
      }
      diagLoggedRef.current = true
    }
  }, [candleData, chartId, debugRanges, logger])

  const { markers: tradeMarkers, tooltips: tradeMarkerTooltips, regions: tradeRegions, priceLines: tradePriceLines } =
    useTradeMarkers(resolvedTrades, candleLookup, candleData)

  const showTradeMarkers = overlayVisibility.trade_markers !== false
  const showTradeRays = overlayVisibility.trade_rays !== false
  const showTradeRegions = overlayVisibility.trade_regions !== false

  const markerManager = useMarkerManager({ seriesRef, markersApiRef, markerCacheRef })

  const { lock, unlock, recenter, requestIntent, attachRangeGuards, setAnimationActive, focusAtTime } = useCameraLock({
    chartRef,
    levelSeriesRef,
    barSpacingRef,
    latestCandlesRef,
    markerManager,
    debugRanges,
  })

  const { pulseTradeElements, clearPulseArtifacts } = usePulseMarkers({
    seriesRef,
    markerManager,
  })

  useEffect(() => {
    if (followLive) {
      lock()
      requestIntent({
        intent: CameraIntents.FOLLOW_LATEST,
        reason: 'follow-live-enabled',
        isUser: true,
      })
      return
    }
    unlock()
  }, [followLive, lock, requestIntent, unlock])

  const regimeOverlay = useMemo(
    () => resolvedOverlays.find((overlay) => overlay?.type === 'regime_overlay'),
    [resolvedOverlays],
  )
  const regimeBlocks = regimeOverlay?.payload?.regime_blocks || []
  const regimePoints = regimeOverlay?.payload?.regime_points || []
  const blockSnapshots = useMemo(() => buildRegimeBlockSnapshots(regimeBlocks), [regimeBlocks])
  const candleSnapshots = useMemo(() => buildCandleSnapshots(regimePoints), [regimePoints])
  const lastCandleEpoch = candleData[candleData.length - 1]?.time
  const lastReadoutSnapshotRef = useRef(null)

  const readoutSnapshot = useMemo(() => {
    if (!showRegimeReadout) return null
    const focusEpoch = Number.isFinite(hoveredEpoch)
      ? findNearestCandleTime(candleData, hoveredEpoch)
      : lastCandleEpoch
    const snapshot = buildReadoutSnapshot({
      focusTs: focusEpoch,
      blocks: blockSnapshots,
      points: candleSnapshots,
      lastSnapshot: lastReadoutSnapshotRef.current,
    })
    if (snapshot) {
      lastReadoutSnapshotRef.current = snapshot
      return snapshot
    }
    return lastReadoutSnapshotRef.current
  }, [blockSnapshots, candleSnapshots, hoveredEpoch, lastCandleEpoch, candleData, showRegimeReadout])

  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return undefined

    const queueHoveredEpoch = (nextEpoch) => {
      const normalized = Number.isFinite(nextEpoch) ? Math.floor(nextEpoch) : null
      if (hoveredEpochRef.current === normalized) return
      hoveredEpochRef.current = normalized
      if (hoverRafRef.current) return
      hoverRafRef.current = window.requestAnimationFrame(() => {
        hoverRafRef.current = 0
        setHoveredEpoch(hoveredEpochRef.current)
      })
    }

    const handleCrosshair = (param) => {
      if (!param?.time) {
        queueHoveredEpoch(null)
        return
      }
      const epoch = typeof param.time === 'number' ? param.time : param.time.timestamp?.()
      if (!Number.isFinite(epoch)) {
        queueHoveredEpoch(null)
        return
      }
      queueHoveredEpoch(epoch)
    }

    chart.subscribeCrosshairMove(handleCrosshair)
    return () => {
      chart.unsubscribeCrosshairMove(handleCrosshair)
      if (hoverRafRef.current) {
        window.cancelAnimationFrame(hoverRafRef.current)
        hoverRafRef.current = 0
      }
    }
  }, [chartRef])

  useBotLensChartCore({
    chartId,
    containerRef,
    chartOptions,
    seriesOptions,
    registerChart,
    candleLookupRef,
    focusAtTime,
    pulseTrade: pulseTradeElements,
    clearPulse: clearPulseArtifacts,
    recenter,
    attachRangeGuards,
    markerCacheRef,
    markerDetailsRef,
    markerManager,
    chartRef,
    seriesRef,
    levelSeriesRef,
    paneMgrRef,
    markersApiRef,
    overlayHandlesRef,
    barSpacingRef,
  })

  const { computeArtifacts, applyArtifacts } = useOverlaySync({
    seriesRef,
    paneMgrRef,
    barSpacingRef,
    overlayHandlesRef,
    markerDetailsRef,
    prevPriceLinesRef,
    markerManager,
  })

  const markerTooltip = useMarkerTooltip({ chartRef, markerDetailsRef })

  const { start: startAnimator, cancel: cancelAnimator, onLifecycleEvent, stateRef: animatorStateRef } =
    useIntrabarCandleAnimator()

  useEffect(
    () =>
      onLifecycleEvent((event) => {
        if (event.state === AnimatorStates.ANIMATING) {
          setAnimationActive(true)
        }
        if (event.state === AnimatorStates.CANCELLED || event.state === AnimatorStates.COMMITTED) {
          setAnimationActive(false)
        }
        if (BOTLENS_DEBUG) {
          console.debug('[BotLensChart] intrabar animator', event)
        }
      }),
    [onLifecycleEvent, setAnimationActive],
  )

  useEffect(() => {
    if (!seriesRef.current) return
    if (seriesRef.current !== seriesInstanceRef.current) {
      seriesInstanceRef.current = seriesRef.current
      prevCandleDataRef.current = []
      frameSampleRef.current = { total: 0, count: 0, logged: false }
      diagLoggedRef.current = false
    }
    const previous = prevCandleDataRef.current || []
    const next = candleData
    const prevLast = previous[previous.length - 1]
    const nextLast = next[next.length - 1]
    const prevLastTime = prevLast?.time
    const nextLastTime = nextLast?.time

    const timeAdvanced = Number.isFinite(prevLastTime) && Number.isFinite(nextLastTime) && nextLastTime > prevLastTime
    const isAppend = timeAdvanced && next.length === previous.length + 1
    const isSameCandle = next.length === previous.length && Number.isFinite(nextLastTime) && nextLastTime === prevLastTime
    const historyRewound =
      Number.isFinite(prevLastTime) && Number.isFinite(nextLastTime) && (next.length < previous.length || nextLastTime < prevLastTime)
    const longJump = next.length > previous.length + 1
    const requiresReset = !previous.length || !next.length || historyRewound || longJump
    const shouldAnimate = isSameCandle && playbackProfile.allowIntrabar

    const sample = frameSampleRef.current
    const start = performance.now()

    if (requiresReset) {
      cancelAnimator('reset')
      seriesRef.current.setData(next)
      frameSampleRef.current = { total: 0, count: 0, logged: false }
      if (!previous.length || timeAdvanced) {
        pendingCameraIntentRef.current = { intent: CameraIntents.FOLLOW_LATEST, reason: 'reset' }
      }
    } else if (shouldAnimate) {
      const prevMatch = previous.find((candle) => Number.isFinite(candle?.time) && candle.time === nextLastTime)
      pendingCameraIntentRef.current = { intent: CameraIntents.FOLLOW_LATEST, reason: 'intrabar-animate' }
      startAnimator({ series: seriesRef.current, fromCandle: prevMatch, toCandle: nextLast, speed: playbackSpeed })
    } else if (isAppend) {
      cancelAnimator('append')
      seriesRef.current.update(nextLast)
      if (timeAdvanced) pendingCameraIntentRef.current = { intent: CameraIntents.FOLLOW_LATEST, reason: 'append' }
    } else if (isSameCandle) {
      cancelAnimator('same-candle')
      seriesRef.current.update(nextLast)
    } else {
      cancelAnimator('fallback')
      seriesRef.current.setData(next)
      if (timeAdvanced) pendingCameraIntentRef.current = { intent: CameraIntents.FOLLOW_LATEST, reason: 'fallback' }
    }

    const duration = performance.now() - start
    sample.total += duration
    sample.count += 1
    if (!sample.logged && sample.count >= 30 && next.length >= 200) {
      const avgMs = Number((sample.total / sample.count).toFixed(2))
      if (BOTLENS_DEBUG) {
        console.debug('[BotLensChart] Candle frame average', { chartId, samples: sample.count, avgMs, candles: next.length })
      }
      sample.logged = true
    }

    prevCandleDataRef.current = next

    if (debugRanges) {
      const timeScale = chartRef.current?.timeScale?.()
      const range = timeScale?.getVisibleRange?.() || null
      const logicalRange = timeScale?.getVisibleLogicalRange?.() || null
      logger.info('series_update', {
        count: next.length,
        requiresReset,
        isAppend,
        isSameCandle,
        historyRewound,
        longJump,
        range,
        logicalRange,
      })
    }
  }, [cancelAnimator, candleData, debugRanges, logger, playbackProfile, seriesRef, startAnimator])

  useEffect(() => {
    const last = candleData[candleData.length - 1]?.time ?? null
    const prev = candleData[candleData.length - 2]?.time ?? null
    if (Number.isFinite(last) && Number.isFinite(prev)) {
      const spacing = last - prev
      if (Number.isFinite(spacing) && spacing > 0) {
        barSpacingRef.current = spacing
      }
    }
    paneMgrRef.current?.updateVABlockContext({
      lastSeriesTime: last,
      barSpacing: barSpacingRef.current,
    })
  }, [barSpacingRef, candleData])

  useEffect(() => {
    const artifacts = computeArtifacts({
      overlayPayloads: resolvedOverlays,
      tradeMarkers: showTradeMarkers ? tradeMarkers : [],
      tradeTooltips: showTradeMarkers ? tradeMarkerTooltips : [],
      tradeRegions: showTradeRegions ? tradeRegions : [],
      tradePriceLines: showTradeRays ? tradePriceLines : [],
      candleData,
    })
    if (BOTLENS_DEBUG) {
      logger.debug('overlay_render_artifacts', {
        overlays_total: resolvedOverlays.length,
        markers: Array.isArray(artifacts?.markers) ? artifacts.markers.length : 0,
        touch_points: Array.isArray(artifacts?.touchPoints) ? artifacts.touchPoints.length : 0,
        boxes: Array.isArray(artifacts?.boxes) ? artifacts.boxes.length : 0,
        segments: Array.isArray(artifacts?.segments) ? artifacts.segments.length : 0,
        polylines: Array.isArray(artifacts?.polylines) ? artifacts.polylines.length : 0,
        bubbles: Array.isArray(artifacts?.bubbles) ? artifacts.bubbles.length : 0,
        price_lines: Array.isArray(artifacts?.priceLines) ? artifacts.priceLines.length : 0,
      })
    }
    const overlayResult = applyArtifacts(artifacts)
    if (debugRanges) {
      const markerTimes = (artifacts?.markers || [])
        .map((marker) => marker?.time)
        .filter((value) => Number.isFinite(value))
      const unique = new Set(markerTimes)
      logger.info('marker_times', {
        total: markerTimes.length,
        unique: unique.size,
        first: markerTimes[0] ?? null,
        last: markerTimes[markerTimes.length - 1] ?? null,
      })
    }
    if (AUTO_FIT_OVERLAY_EXTENTS && overlayResult.extentChanged && overlayResult.extents) {
      requestIntent({
        intent: CameraIntents.FIT_OVERLAY_EXTENTS,
        payload: { extents: overlayResult.extents, signature: overlayResult.signature, segments: artifacts.tradeSegments },
        reason: 'overlay-extents',
      })
    }
    if (pendingCameraIntentRef.current) {
      const pending = pendingCameraIntentRef.current
      requestIntent({
        ...pending,
        payload: { ...(pending.payload || {}), segments: artifacts.tradeSegments },
        reason: pending.reason,
      })
      pendingCameraIntentRef.current = null
    }
  }, [applyArtifacts, candleData, computeArtifacts, requestIntent, resolvedOverlays, tradeMarkerTooltips, tradeMarkers, tradePriceLines, tradeRegions])

  const containerClasses = [
    'relative w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0f1118]',
    heightClass,
    className,
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div ref={containerRef} className={containerClasses}>
      {showRegimeReadout ? <RegimeReadoutBar snapshot={readoutSnapshot} /> : null}
      <MarkerTooltip markerTooltip={markerTooltip} />
    </div>
  )
}
