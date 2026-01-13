import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  fetchBotPerformance,
  openBotStream,
  pauseBot,
  resumeBot,
  updateBot,
} from '../../../adapters/bot.adapter.js'
import { toSec } from '../chartDataUtils.js'

const logCandleDiagnostics = (label, seriesList, botId) => {
  if (!Array.isArray(seriesList) || seriesList.length === 0) {
    return
  }
  for (const series of seriesList) {
    const candles = Array.isArray(series?.candles) ? series.candles : []
    if (!candles.length) continue
    let previous = null
    let violation = null
    let first = null
    let last = null
    for (let idx = 0; idx < candles.length; idx += 1) {
      const raw = candles[idx]?.time
      const epoch = toSec(raw)
      if (!Number.isFinite(epoch)) {
        continue
      }
      if (first === null) first = epoch
      last = epoch
      if (previous !== null && epoch < previous) {
        violation = { index: idx, prev: previous, current: epoch }
        break
      }
      previous = epoch
    }
    const context = {
      botId,
      label,
      symbol: series?.symbol,
      count: candles.length,
      first,
      last,
    }
    if (violation) {
      console.error('[BotPerformanceModal] Candle order violation', { ...context, ...violation })
    } else {
      console.debug('[BotPerformanceModal] Candle payload received', context)
    }
  }
}

export function useBotPerformance({ bot, open, onRefresh }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [payload, setPayload] = useState(null)
  const [action, setAction] = useState(null)
  const [streamStatus, setStreamStatus] = useState('idle')
  const streamRef = useRef(null)
  const playbackDebounceRef = useRef(null)
  const focusDebounceRef = useRef(null)
  const [playbackDraft, setPlaybackDraft] = useState(() => {
    const initial = bot?.runtime?.playback_speed ?? bot?.playback_speed ?? 10
    const raw = Number(initial)
    return Number.isFinite(raw) ? raw : 10
  })
  const [speedSaving, setSpeedSaving] = useState(false)

  const baseStatus = (bot?.runtime?.status || bot?.status || 'idle').toLowerCase()
  const runtimeStatus = (payload?.runtime?.status || baseStatus).toLowerCase()
  const streamEligible = useMemo(
    () => ['running', 'starting', 'paused', 'booting', 'initialising'].includes(runtimeStatus),
    [runtimeStatus],
  )

  useEffect(() => {
    const candidate =
      payload?.runtime?.playback_speed ?? bot?.runtime?.playback_speed ?? bot?.playback_speed ?? 10
    const numeric = Number(candidate)
    if (Number.isFinite(numeric)) {
      setPlaybackDraft(numeric)
    } else {
      setPlaybackDraft(10)
    }
  }, [payload?.runtime?.playback_speed, bot?.runtime?.playback_speed, bot?.playback_speed, bot?.id])

  useEffect(
    () => () => {
      if (playbackDebounceRef.current) {
        clearTimeout(playbackDebounceRef.current)
      }
      if (focusDebounceRef.current) {
        clearTimeout(focusDebounceRef.current)
      }
    },
    [],
  )

  const applyPayload = useCallback((incoming) => {
    if (!incoming) return
    setPayload(incoming)
  }, [])

  const loadPerformance = useCallback(
    async (withLoader = true) => {
      if (!bot?.id) return
      if (withLoader) setLoading(true)
      setError(null)
      try {
        const data = await fetchBotPerformance(bot.id)
        logCandleDiagnostics('initial_fetch', data?.series, bot?.id)
        applyPayload(data)
      } catch (err) {
        setError(err?.message || 'Unable to fetch performance')
      } finally {
        if (withLoader) setLoading(false)
      }
    },
    [bot?.id, applyPayload],
  )

  useEffect(() => {
    if (open) {
      loadPerformance(true)
    }
  }, [open, loadPerformance])

  useEffect(() => {
    if (!open || !bot?.id || !streamEligible) {
      streamRef.current?.close?.()
      streamRef.current = null
      setStreamStatus('idle')
      return undefined
    }
    const source = openBotStream(bot.id)
    if (!source) return undefined
    streamRef.current = source
    setStreamStatus('connecting')
    const events = ['snapshot', 'bar', 'status', 'live_refresh', 'pause', 'resume', 'start', 'stop', 'intrabar']

    const handler = (event) => {
      try {
        const data = JSON.parse(event.data)
        logCandleDiagnostics(event.type || 'message', data?.series, bot?.id)
        applyPayload(data)
        setStreamStatus('open')
      } catch (err) {
        console.error('bot stream parse failed', err)
      }
    }
    source.onmessage = handler
    for (const evt of events) {
      source.addEventListener(evt, handler)
    }
    source.onerror = () => {
      setStreamStatus('error')
    }
    source.onopen = () => setStreamStatus('open')
    return () => {
      for (const evt of events) {
        source.removeEventListener(evt, handler)
      }
      source.close()
      streamRef.current = null
      setStreamStatus('closed')
    }
  }, [open, bot?.id, applyPayload, streamEligible])

  const persistPlaybackSpeed = useCallback(
    async (value) => {
      if (!bot?.id) return
      setSpeedSaving(true)
      try {
        await updateBot(bot.id, { playback_speed: Number.isFinite(value) ? value : 0 })
        onRefresh?.()
      } catch (err) {
        console.error('bot playback update failed', err)
        setError(err?.message || 'Unable to update playback speed')
      } finally {
        setSpeedSaving(false)
      }
    },
    [bot?.id, onRefresh],
  )

  const handlePlaybackInput = useCallback(
    (event) => {
      const raw = Number(event?.target?.value)
      const next = Number.isFinite(raw) ? raw : 0
      setPlaybackDraft(next)
      if (playbackDebounceRef.current) {
        clearTimeout(playbackDebounceRef.current)
      }
      playbackDebounceRef.current = setTimeout(() => {
        playbackDebounceRef.current = null
        persistPlaybackSpeed(next)
      }, 300)
    },
    [persistPlaybackSpeed],
  )

  const handleFocusSymbolChange = useCallback(
    (symbol) => {
      if (!bot?.id) return
      if (focusDebounceRef.current) {
        clearTimeout(focusDebounceRef.current)
      }
      focusDebounceRef.current = setTimeout(async () => {
        focusDebounceRef.current = null
        try {
          await updateBot(bot.id, { focus_symbol: symbol || null })
        } catch (err) {
          console.error('bot focus symbol update failed', err)
        }
      }, 150)
    },
    [bot?.id],
  )

  const handlePause = useCallback(async () => {
    if (!bot?.id) return
    setAction('pause')
    setError(null)
    try {
      await pauseBot(bot.id)
      await loadPerformance(false)
      onRefresh?.()
    } catch (err) {
      setError(err?.message || 'Unable to pause bot')
    } finally {
      setAction(null)
    }
  }, [bot?.id, loadPerformance, onRefresh])

  const handleResume = useCallback(async () => {
    if (!bot?.id) return
    setAction('resume')
    setError(null)
    try {
      await resumeBot(bot.id)
      await loadPerformance(false)
      onRefresh?.()
    } catch (err) {
      setError(err?.message || 'Unable to resume bot')
    } finally {
      setAction(null)
    }
  }, [bot?.id, loadPerformance, onRefresh])

  const playbackLabel = useMemo(() => (playbackDraft <= 0 ? 'Instant' : `${playbackDraft.toFixed(2)}x`), [playbackDraft])

  return {
    action,
    applyPayload,
    error,
    handlePause,
    handlePlaybackInput,
    handleFocusSymbolChange,
    handleResume,
    loadPerformance,
    payload,
    playbackDraft,
    playbackLabel,
    runtimeStatus,
    setError,
    speedSaving,
    streamEligible,
    streamStatus,
    loading,
  }
}
