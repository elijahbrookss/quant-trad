import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  fetchBotPerformance,
  openBotStream,
  pauseBot,
  resumeBot,
  updateBot,
} from '../../../adapters/bot.adapter.js'
import { BOTLENS_DEBUG, toSec } from '../chartDataUtils.js'
import { createLogger } from '../../../utils/logger.js'

const log = createLogger('BotPerformance')

const summarizeOverlays = (overlays) => {
  const list = Array.isArray(overlays) ? overlays : []
  const summary = {}
  for (const overlay of list) {
    const type = String(overlay?.type || 'unknown')
    summary[type] = (summary[type] || 0) + 1
  }
  return { total: list.length, byType: summary }
}

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
    } else if (BOTLENS_DEBUG) {
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
  const focusDebounceRef = useRef(null)

  const baseStatus = (bot?.runtime?.status || bot?.status || 'idle').toLowerCase()
  const runtimeStatus = (payload?.runtime?.status || baseStatus).toLowerCase()
  const streamEligible = useMemo(
    () => ['running', 'starting', 'paused', 'booting', 'initialising'].includes(runtimeStatus),
    [runtimeStatus],
  )

  const extractRuntimeError = useCallback((incoming) => {
    if (!incoming || typeof incoming !== 'object') return null
    const runtimeError = incoming?.runtime?.error
    if (runtimeError && typeof runtimeError === 'object') {
      const msg = runtimeError.message || runtimeError.detail
      if (msg) return String(msg)
    }
    if (incoming?.error && typeof incoming.error === 'object') {
      const msg = incoming.error.message || incoming.error.detail
      if (msg) return String(msg)
    }
    if (typeof incoming?.error === 'string' && incoming.error.trim()) {
      return incoming.error.trim()
    }
    const status = String(incoming?.runtime?.status || '').toLowerCase()
    if (status === 'error' || status === 'crashed') {
      return 'Bot runtime failed. Check runtime logs for details.'
    }
    return null
  }, [])

  useEffect(
    () => () => {
      if (focusDebounceRef.current) {
        clearTimeout(focusDebounceRef.current)
      }
    },
    [],
  )

  const payloadRef = useRef(null)

  const buildOverlayState = useCallback((overlays) => {
    const entries = {}
    const order = []
    const list = Array.isArray(overlays) ? overlays : []
    for (let idx = 0; idx < list.length; idx += 1) {
      const overlay = list[idx]
      if (!overlay || typeof overlay !== 'object') continue
      const explicit = overlay.id
      const key = explicit
        ? String(explicit)
        : [
            String(overlay.type || 'overlay'),
            String(overlay.strategy_id || ''),
            String(overlay.symbol || ''),
            String(overlay.timeframe || ''),
            String(overlay.instrument_id || ''),
            String(overlay.source || ''),
            String(idx),
          ].join('|')
      entries[key] = overlay
      order.push(key)
    }
    return { entries, order }
  }, [])

  const applyOverlayDelta = useCallback((existing, delta) => {
    if (!delta || typeof delta !== 'object') return existing
    const nextSeq = Number(delta.seq)
    const baseSeq = Number(delta.base_seq)
    const currentSeq = Number(existing?._overlay_seq || 0)
    const currentState =
      existing?._overlay_state && typeof existing._overlay_state === 'object'
        ? existing._overlay_state
        : buildOverlayState(existing?.overlays)

    const ops = Array.isArray(delta.ops) ? delta.ops : []
    const resetOp = ops.find((op) => op && String(op.op || '').toLowerCase() === 'reset')
    const hasAuthoritativeReset = Boolean(delta.authoritative_snapshot) || Boolean(resetOp)

    if (Number.isFinite(baseSeq) && baseSeq !== currentSeq) {
      if (!hasAuthoritativeReset) {
        console.warn('[BotPerformanceModal] overlay delta base_seq mismatch -> hard reset', {
          expected: currentSeq,
          received: baseSeq,
        })
        return {
          ...existing,
          overlays: [],
          _overlay_state: { entries: {}, order: [] },
          _overlay_seq: Number.isFinite(nextSeq) ? nextSeq : 0,
        }
      }
    }

    let entries = { ...(currentState.entries || {}) }
    let order = Array.isArray(currentState.order) ? [...currentState.order] : []

    for (const op of ops) {
      if (!op || typeof op !== 'object') continue
      const opType = String(op.op || '').toLowerCase()

      if (opType === 'reset') {
        const resetEntries = {}
        const resetOrder = []
        const resetList = Array.isArray(op.entries) ? op.entries : []
        for (let idx = 0; idx < resetList.length; idx += 1) {
          const overlay = resetList[idx]
          if (!overlay || typeof overlay !== 'object') continue
          const explicit = overlay.id
          const key = explicit
            ? String(explicit)
            : [
                String(overlay.type || 'overlay'),
                String(overlay.strategy_id || ''),
                String(overlay.symbol || ''),
                String(overlay.timeframe || ''),
                String(overlay.instrument_id || ''),
                String(overlay.source || ''),
                String(idx),
              ].join('|')
          resetEntries[key] = overlay
          resetOrder.push(key)
        }
        entries = resetEntries
        order = resetOrder
        continue
      }

      const key = String(op.key || '')
      if (!key) continue
      if (opType === 'remove') {
        delete entries[key]
        order = order.filter((candidate) => candidate !== key)
        continue
      }
      if (opType === 'upsert' && op.overlay && typeof op.overlay === 'object') {
        entries[key] = op.overlay
        if (!order.includes(key)) order.push(key)
      }
    }
    const overlays = order.map((key) => entries[key]).filter(Boolean)
    return {
      ...existing,
      overlays,
      _overlay_state: { entries, order },
      _overlay_seq: Number.isFinite(nextSeq) ? nextSeq : currentSeq,
    }
  }, [buildOverlayState])

  const applyPayload = useCallback((incoming) => {
    if (!incoming) return
    if (payloadRef.current === incoming) return
    payloadRef.current = incoming
    const incomingSeries = Array.isArray(incoming?.series) ? incoming.series : []
    const primarySeries = incomingSeries[0]
    const primaryOverlaySummary = summarizeOverlays(primarySeries?.overlays)
    log.info('overlay_payload_received', {
      bot_id: bot?.id || null,
      event_type: String(incoming?.type || 'snapshot'),
      series_count: incomingSeries.length,
      primary_symbol: primarySeries?.symbol || null,
      primary_timeframe: primarySeries?.timeframe || null,
      overlays_total: primaryOverlaySummary.total,
      overlays_by_type: primaryOverlaySummary.byType,
      has_overlay_delta: Boolean(primarySeries?.overlay_delta),
      logs_count: Array.isArray(incoming?.logs) ? incoming.logs.length : null,
    })
    setPayload((prev) => {
      const kind = String(incoming?.type || '').toLowerCase()
      if (kind !== 'delta') {
        return incoming
      }
      const base = prev && typeof prev === 'object' ? { ...prev } : {}
      if (incoming.runtime) base.runtime = incoming.runtime
      if (incoming.logs) base.logs = incoming.logs
      if (incoming.decisions) base.decisions = incoming.decisions
      if (incoming.stats) base.stats = incoming.stats

      const seriesDeltas = Array.isArray(incoming.series) ? incoming.series : []
      const currentSeries = Array.isArray(base.series) ? [...base.series] : []
      for (const delta of seriesDeltas) {
        const strategyId = delta?.strategy_id
        const symbol = delta?.symbol
        const timeframe = delta?.timeframe
        const idx = currentSeries.findIndex(
          (item) => item?.strategy_id === strategyId && item?.symbol === symbol && item?.timeframe === timeframe,
        )
        const existing = idx >= 0 && currentSeries[idx] ? { ...currentSeries[idx] } : {
          strategy_id: strategyId,
          symbol,
          timeframe,
          candles: [],
        }
        const candles = Array.isArray(existing.candles) ? [...existing.candles] : []
        if (delta?.candle) {
          if (delta?.replace_last && candles.length) {
            candles[candles.length - 1] = delta.candle
          } else {
            const nextTs = toSec(delta.candle?.time)
            const lastTs = candles.length ? toSec(candles[candles.length - 1]?.time) : null
            if (Number.isFinite(nextTs) && Number.isFinite(lastTs) && nextTs <= lastTs) {
              candles[candles.length - 1] = delta.candle
            } else {
              candles.push(delta.candle)
            }
          }
        }
        existing.candles = candles
        if (Array.isArray(delta?.overlays)) {
          existing.overlays = delta.overlays
          existing._overlay_state = buildOverlayState(delta.overlays)
          existing._overlay_seq = 0
          const fullOverlaySummary = summarizeOverlays(delta.overlays)
          log.info('overlay_full_snapshot_received', {
            bot_id: bot?.id || null,
            strategy_id: strategyId || null,
            symbol: symbol || null,
            timeframe: timeframe || null,
            overlays_total: fullOverlaySummary.total,
            overlays_by_type: fullOverlaySummary.byType,
          })
        }
        if (delta?.overlay_delta) {
          const ops = Array.isArray(delta.overlay_delta?.ops) ? delta.overlay_delta.ops : []
          const opCounts = ops.reduce((acc, op) => {
            const name = String(op?.op || 'unknown').toLowerCase()
            acc[name] = (acc[name] || 0) + 1
            return acc
          }, {})
          const applied = applyOverlayDelta(existing, delta.overlay_delta)
          existing.overlays = applied.overlays
          existing._overlay_state = applied._overlay_state
          existing._overlay_seq = applied._overlay_seq
          const postDeltaSummary = summarizeOverlays(applied.overlays)
          log.info('overlay_delta_received', {
            bot_id: bot?.id || null,
            strategy_id: strategyId || null,
            symbol: symbol || null,
            timeframe: timeframe || null,
            seq: Number(delta.overlay_delta?.seq),
            base_seq: Number(delta.overlay_delta?.base_seq),
            ops_total: ops.length,
            ops_by_type: opCounts,
            overlays_total_after: postDeltaSummary.total,
            overlays_by_type_after: postDeltaSummary.byType,
          })
        }
        if (Array.isArray(delta?.trades)) existing.trades = delta.trades
        if (delta?.stats && typeof delta.stats === 'object') existing.stats = delta.stats
        if (typeof delta?.bar_index === 'number') existing.bar_index = delta.bar_index
        if (idx >= 0) currentSeries[idx] = existing
        else currentSeries.push(existing)
      }
      base.series = currentSeries

      // Keep backward-compatible top-level chart keys synced to the primary series.
      const primary = currentSeries[0]
      if (primary) {
        base.candles = Array.isArray(primary.candles) ? primary.candles : []
        base.trades = Array.isArray(primary.trades) ? primary.trades : []
        base.overlays = Array.isArray(primary.overlays) ? primary.overlays : []
      }
      return base
    })
    const runtimeErr = extractRuntimeError(incoming)
    if (runtimeErr) {
      setError(runtimeErr)
    }
  }, [bot?.id, extractRuntimeError, applyOverlayDelta, buildOverlayState])

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
    const events = ['snapshot', 'delta', 'bar', 'status', 'live_refresh', 'pause', 'resume', 'start', 'stop', 'intrabar', 'error']

    const handler = (event) => {
      try {
        const data = JSON.parse(event.data)
        logCandleDiagnostics(event.type || 'message', data?.series, bot?.id)
        applyPayload(data)
        setStreamStatus('open')
      } catch (err) {
        console.error('bot stream parse failed', err)
        setError('Bot stream payload parse failed')
      }
    }
    source.onmessage = handler
    for (const evt of events) {
      source.addEventListener(evt, handler)
    }
    source.onerror = () => {
      setStreamStatus('error')
      setError((prev) => prev || 'Bot stream connection error')
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

  return {
    action,
    applyPayload,
    error,
    handlePause,
    handleFocusSymbolChange,
    handleResume,
    loadPerformance,
    payload,
    runtimeStatus,
    setError,
    streamEligible,
    streamStatus,
    loading,
  }
}
