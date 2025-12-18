import { useCallback, useEffect, useRef } from 'react'

export const AnimatorStates = {
  IDLE: 'IDLE',
  ANIMATING: 'ANIMATING',
  CANCELLED: 'CANCELLED',
  COMMITTED: 'COMMITTED',
}

export const useIntrabarCandleAnimator = () => {
  const rafRef = useRef(null)
  const stateRef = useRef({ state: AnimatorStates.IDLE, candleTime: null, duration: 0 })
  const listenersRef = useRef(new Set())

  const notify = useCallback((event) => {
    listenersRef.current.forEach((cb) => {
      try {
        cb(event)
      } catch (err) {
        console.warn('[BotLensAnimator] listener error', err)
      }
    })
  }, [])

  const cancel = useCallback(
    (reason = 'cancel') => {
      if (rafRef.current) {
        cancelAnimationFrame(rafRef.current)
        rafRef.current = null
      }
      stateRef.current = { ...stateRef.current, state: AnimatorStates.CANCELLED }
      notify({ state: AnimatorStates.CANCELLED, reason })
    },
    [notify],
  )

  const start = useCallback(
    ({ series, fromCandle, toCandle, durationMs = 380, speed = 1 }) => {
      if (!series || !fromCandle || !toCandle) return
      cancel('restart')
      const safeSpeed = Number.isFinite(speed) ? Math.max(speed, 0.25) : 1
      const duration = Math.min(Math.max(durationMs / safeSpeed, 80), 600)
      const startTs = performance.now()
      stateRef.current = { state: AnimatorStates.ANIMATING, candleTime: toCandle.time, duration }
      notify({ state: AnimatorStates.ANIMATING, candleTime: toCandle.time, duration })

      const frame = (now) => {
        const progress = Math.min(1, (now - startTs) / duration)
        const interp = (a, b) => a + (b - a) * progress
        const current = {
          time: toCandle.time,
          open: interp(fromCandle.open, toCandle.open),
          high: interp(fromCandle.high, toCandle.high),
          low: interp(fromCandle.low, toCandle.low),
          close: interp(fromCandle.close, toCandle.close),
        }
        series.update(current)
        if (progress < 1) {
          rafRef.current = requestAnimationFrame(frame)
        } else {
          rafRef.current = null
          stateRef.current = { state: AnimatorStates.COMMITTED, candleTime: toCandle.time, duration }
          notify({ state: AnimatorStates.COMMITTED, candleTime: toCandle.time, duration })
        }
      }

      rafRef.current = requestAnimationFrame(frame)
    },
    [cancel, notify],
  )

  const isAnimating = useCallback(() => stateRef.current.state === AnimatorStates.ANIMATING, [])

  const onLifecycleEvent = useCallback((cb) => {
    if (!cb) return () => {}
    listenersRef.current.add(cb)
    return () => listenersRef.current.delete(cb)
  }, [])

  useEffect(() => () => cancel('unmount'), [cancel])

  return { start, cancel, isAnimating, onLifecycleEvent, stateRef }
}
