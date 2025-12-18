import { useCallback, useEffect, useRef } from 'react'
import { toSec } from '../chartDataUtils.js'

export const CameraIntents = {
  FOLLOW_LATEST: 'FOLLOW_LATEST',
  FIT_OVERLAY_EXTENTS: 'FIT_OVERLAY_EXTENTS',
  FOCUS_TIME_SPAN: 'FOCUS_TIME_SPAN',
  RECENTER: 'RECENTER',
  USER_OVERRIDE: 'USER_OVERRIDE',
}

const deriveSpacing = (candles = [], barSpacingRef) => {
  const last = candles[candles.length - 1]
  const prev = candles[candles.length - 2]
  if (Number.isFinite(last?.time) && Number.isFinite(prev?.time)) {
    const spacing = last.time - prev.time
    if (Number.isFinite(spacing) && spacing > 0) return spacing
  }
  if (Number.isFinite(barSpacingRef?.current)) return barSpacingRef.current
  return null
}

const clampBars = (value, min = 10, max = 400) => Math.min(Math.max(value ?? min, min), max)

const computeFollowRange = (candles = [], spacing, { lookbackBars = 24, forwardPadBars = 1.25 } = {}) => {
  const lastIndex = candles.length - 1
  const lastTime = candles[lastIndex]?.time
  if (!Number.isFinite(lastTime)) return { range: null, logicalRange: null }

  const safeSpacing = Math.max(spacing ?? 1, 1)
  const lookback = clampBars(lookbackBars, 8, 480)
  const forwardPad = Math.max(forwardPadBars, 0)
  const spanBars = lookback + forwardPad
  const to = lastTime + safeSpacing * forwardPad
  const from = to - safeSpacing * spanBars

  const logicalTo = lastIndex + forwardPad
  const logicalFrom = Math.max(0, logicalTo - spanBars)
  const logicalRange = { from: logicalFrom, to: Math.max(logicalFrom + 1, logicalTo) }

  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) {
    return { range: null, logicalRange }
  }

  return { range: { from, to }, logicalRange }
}

const buildGhostPoints = (candles = [], segments = []) => {
  const ghostPoints = []
  const lastIndex = candles.length - 1
  const lastTime = candles[lastIndex]?.time
  if (Number.isFinite(lastTime)) {
    const lastCandle = candles[lastIndex]
    ghostPoints.push({ time: lastTime - 1, value: lastCandle?.low ?? lastCandle?.close ?? 0 })
    ghostPoints.push({ time: lastTime, value: lastCandle?.high ?? lastCandle?.close ?? 0 })
  }
  segments
    .flatMap((segment) => [segment?.y1, segment?.y2])
    .filter((price) => Number.isFinite(price))
    .forEach((price, idx) => {
      if (!Number.isFinite(lastTime)) return
      ghostPoints.push({ time: lastTime + idx + 1, value: price })
    })
  ghostPoints.sort((a, b) => (a.time ?? 0) - (b.time ?? 0))
  return ghostPoints
}

export const useViewportController = ({ chartRef, levelSeriesRef, barSpacingRef, latestCandlesRef }) => {
  const lockedRef = useRef(true)
  const animationActiveRef = useRef(false)
  const userOverrideUntilRef = useRef(0)
  const pendingFollowRef = useRef(false)
  const lastOverlaySignatureRef = useRef(null)
  const preferredSpanBarsRef = useRef(24)
  const lastLogicalRangeRef = useRef(null)
  const interactionRef = useRef({ dragging: false })

  const applyRange = useCallback((range, logicalRange) => {
    const ts = chartRef.current?.timeScale?.()
    if (!ts) return
    if (range && Number.isFinite(range.from) && Number.isFinite(range.to)) {
      ts.setVisibleRange(range)
    } else if (logicalRange) {
      ts.setVisibleLogicalRange(logicalRange)
    }
  }, [chartRef])

  const logIntent = useCallback((payload) => {
    console.debug('[BotLensChart] camera intent applied', {
      ...payload,
      locked: lockedRef.current,
      userOverrideUntil: userOverrideUntilRef.current,
      animationActive: animationActiveRef.current,
    })
  }, [])

  const setLocked = useCallback((locked) => {
    lockedRef.current = locked
    if (locked) {
      userOverrideUntilRef.current = 0
    }
  }, [])

  const setAnimationActive = useCallback(
    (active) => {
      animationActiveRef.current = active
      if (!active && pendingFollowRef.current) {
        pendingFollowRef.current = false
        const candles = latestCandlesRef?.current || []
        const spacing = deriveSpacing(candles, barSpacingRef)
        const follow = computeFollowRange(candles, spacing, {
          lookbackBars: preferredSpanBarsRef.current,
          forwardPadBars: 1.25,
        })
        applyRange(follow.range, follow.logicalRange)
        if (levelSeriesRef?.current) {
          levelSeriesRef.current.setData(buildGhostPoints(candles, []))
        }
        logIntent({ intent: CameraIntents.FOLLOW_LATEST, reason: 'animation-complete', range: follow.range })
      }
    },
    [applyRange, barSpacingRef, latestCandlesRef, levelSeriesRef, logIntent],
  )

  const notifyUserInteraction = useCallback((ttlMs = 2400) => {
    userOverrideUntilRef.current = performance.now() + ttlMs
    lockedRef.current = false
  }, [])

  const applyGhostSeries = useCallback(
    (candles, segments) => {
      if (!levelSeriesRef?.current) return
      levelSeriesRef.current.setData(buildGhostPoints(candles, segments))
    },
    [levelSeriesRef],
  )

  const requestIntent = useCallback(
    ({ intent, payload = {}, reason = 'unspecified', isUser = false }) => {
      const now = performance.now()
      const ttlActive = !isUser && now < userOverrideUntilRef.current
      if (ttlActive) {
        logIntent({ intent, reason: `${reason}-suppressed`, ttl: userOverrideUntilRef.current - now })
        return
      }
      if (intent === CameraIntents.USER_OVERRIDE) {
        notifyUserInteraction(payload.ttlMs ?? 2400)
        return
      }
      const candles = latestCandlesRef?.current || []
      const spacing = deriveSpacing(candles, barSpacingRef)
      const follow = computeFollowRange(candles, spacing, {
        lookbackBars: preferredSpanBarsRef.current,
        forwardPadBars: 1.25,
      })
      const tsRange = (() => {
        if (intent === CameraIntents.FOLLOW_LATEST) {
          if (animationActiveRef.current && !isUser) {
            pendingFollowRef.current = true
            logIntent({ intent, reason: `${reason}-deferred` })
            return null
          }
          return follow.range ?? null
        }
        if (intent === CameraIntents.FIT_OVERLAY_EXTENTS) {
          const { extents, signature } = payload
          if (signature && signature === lastOverlaySignatureRef.current) return null
          lastOverlaySignatureRef.current = signature
          if (extents?.from && extents?.to) {
            return { from: extents.from, to: extents.to }
          }
          if (extents?.from === 0 || extents?.to === 0) {
            return { from: extents.from, to: extents.to }
          }
          if (extents?.range) return extents.range
          return null
        }
        if (intent === CameraIntents.FOCUS_TIME_SPAN) {
          const center = toSec(payload?.center)
          const span = Number(payload?.span) || Math.max(spacing || 30, 30)
          if (!Number.isFinite(center)) return null
          return { from: center - span, to: center + span }
        }
        if (intent === CameraIntents.RECENTER) {
          if (follow.range) return follow.range
          return null
        }
        return null
      })()

      if (!lockedRef.current && !isUser && intent !== CameraIntents.FOCUS_TIME_SPAN) {
        logIntent({ intent, reason: `${reason}-unlocked` })
        return
      }

      if (tsRange || follow.logicalRange) {
        applyRange(tsRange, follow.logicalRange)
        applyGhostSeries(candles, payload?.segments || [])
        logIntent({ intent, reason, range: tsRange, logicalRange: follow.logicalRange })
      }
    },
    [applyGhostSeries, applyRange, barSpacingRef, latestCandlesRef, logIntent, notifyUserInteraction],
  )

  const attachRangeGuards = useCallback(
    (containerEl) => {
      if (!containerEl || !chartRef.current) return () => {}
      const ts = chartRef.current.timeScale()
      const handleRangeChange = (logicalRange) => {
        const prev = lastLogicalRangeRef.current
        lastLogicalRangeRef.current = logicalRange
        const span = logicalRange ? logicalRange.to - logicalRange.from : null
        const prevSpan = prev ? prev.to - prev.from : null
        const spanChanged =
          Number.isFinite(span) &&
          Number.isFinite(prevSpan) &&
          Math.abs(span - prevSpan) > 0.25
        const seedSpan = Number.isFinite(span) && !Number.isFinite(prevSpan)

        if ((spanChanged || seedSpan) && Number.isFinite(span)) {
          preferredSpanBarsRef.current = clampBars(span, 8, 480)
          return
        }

        if (interactionRef.current.dragging) {
          notifyUserInteraction()
        }
      }

      const markDragStart = () => {
        interactionRef.current.dragging = true
      }
      const markDragEnd = () => {
        interactionRef.current.dragging = false
      }
      const markWheel = () => {
        interactionRef.current.dragging = false
      }

      ts.subscribeVisibleLogicalRangeChange(handleRangeChange)
      containerEl.addEventListener('mousedown', markDragStart)
      containerEl.addEventListener('mouseup', markDragEnd)
      containerEl.addEventListener('mouseleave', markDragEnd)
      containerEl.addEventListener('touchstart', markDragStart)
      containerEl.addEventListener('touchend', markDragEnd)
      containerEl.addEventListener('wheel', markWheel, { passive: true })
      return () => {
        ts.unsubscribeVisibleLogicalRangeChange(handleRangeChange)
        containerEl.removeEventListener('mousedown', markDragStart)
        containerEl.removeEventListener('mouseup', markDragEnd)
        containerEl.removeEventListener('mouseleave', markDragEnd)
        containerEl.removeEventListener('touchstart', markDragStart)
        containerEl.removeEventListener('touchend', markDragEnd)
        containerEl.removeEventListener('wheel', markWheel)
      }
    },
    [chartRef, notifyUserInteraction],
  )

  useEffect(() => {
    return () => {
      pendingFollowRef.current = false
      lastOverlaySignatureRef.current = null
    }
  }, [])

  return {
    setLocked,
    requestIntent,
    notifyUserInteraction,
    setAnimationActive,
    attachRangeGuards,
    lockedRef,
    userOverrideUntilRef,
  }
}
