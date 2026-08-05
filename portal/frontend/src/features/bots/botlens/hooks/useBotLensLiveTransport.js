import { startTransition, useCallback, useEffect, useRef, useState } from 'react'

import { openBotLensLiveStream } from '../../../../adapters/bot.adapter.js'
import { normalizeSeriesKey } from '../../../../components/bots/botlensProjection.js'

const WEBSOCKET_OPEN_STATE = typeof WebSocket === 'function' ? WebSocket.OPEN : 1
export const BOTLENS_LIVE_MESSAGE_LIMIT = 256
export const BOTLENS_LIVE_BYTE_LIMIT = 2 * 1024 * 1024
export const BOTLENS_LIVE_FRAME_MESSAGE_LIMIT = 24
export const BOTLENS_LIVE_FRAME_BYTE_LIMIT = 256 * 1024
export const BOTLENS_LIVE_RENDER_INTERVAL_MS = 100
export const BOTLENS_LIVE_STABLE_CONNECTION_MS = 30_000

export function botLensReconnectDelayMs(attempt, randomValue = 0) {
  const normalizedAttempt = Math.max(1, Number(attempt || 1) || 1)
  const baseDelay = Math.min(300 * (2 ** Math.min(normalizedAttempt - 1, 6)), 10_000)
  const jitterRatio = Math.min(Math.max(Number(randomValue || 0) || 0, 0), 1)
  return Math.round(baseDelay + (baseDelay * 0.2 * jitterRatio))
}

export function isBotLensLiveDocumentVisible(visibilityState) {
  return String(visibilityState || '').trim().toLowerCase() !== 'hidden'
}

export function shouldOpenBotLensLiveTransport({
  open,
  botId,
  runId,
  transportEligible,
  documentVisible = true,
}) {
  return Boolean(open && botId && runId && transportEligible && documentVisible)
}

export function buildBotLensLiveTransportEpoch({
  open,
  botId,
  runId,
  transportEligible,
  documentVisible = true,
  reconnectTick = 0,
}) {
  if (!shouldOpenBotLensLiveTransport({
    open,
    botId,
    runId,
    transportEligible,
    documentVisible,
  })) {
    return 'closed'
  }
  return [
    String(botId || '').trim(),
    String(runId || '').trim(),
    Math.max(0, Number(reconnectTick || 0) || 0),
  ].join(':')
}

export function shouldSendBotLensSelectedSymbolSubscription({
  socketReadyState,
  selectedSymbolKey,
  selectedSymbolReady,
  subscribedSymbolKey,
  subscriptionSocketMatches,
}) {
  const normalizedSymbolKey = normalizeSeriesKey(selectedSymbolKey || '')
  if (socketReadyState !== WEBSOCKET_OPEN_STATE || !normalizedSymbolKey || !selectedSymbolReady) {
    return false
  }
  return !(subscriptionSocketMatches && subscribedSymbolKey === normalizedSymbolKey)
}

export function buildSelectedSymbolSubscriptionPayload({ selectedSymbolKey, resumeFromSeq = 0, streamSessionId = null }) {
  const normalizedSymbolKey = normalizeSeriesKey(selectedSymbolKey || '')
  if (!normalizedSymbolKey) return null
  return {
    type: 'set_selected_symbol',
    symbol_key: normalizedSymbolKey,
    resume_from_seq: Math.max(0, Number(resumeFromSeq || 0) || 0),
    stream_session_id: String(streamSessionId || '').trim() || null,
  }
}

export function useBotLensLiveTransport({
  open,
  botId,
  runId,
  transportEligible,
  selectedSymbolKey,
  selectedSymbolReady,
  streamSessionId,
  resumeFromSeq,
  dispatch,
  refreshSession,
  logger,
}) {
  const socketRef = useRef(null)
  const reconnectRef = useRef(0)
  const sessionTokenRef = useRef(0)
  const reconnectTimerRef = useRef(null)
  const stableConnectionTimerRef = useRef(null)
  const subscriptionRef = useRef({ socket: null, symbolKey: null })
  const pendingMessagesRef = useRef([])
  const pendingBytesRef = useRef(0)
  const pendingFrameRef = useRef(null)
  const bufferOverflowRef = useRef(false)
  const latestSelectionRef = useRef({
    selectedSymbolKey: null,
    selectedSymbolReady: false,
  })
  const latestCursorRef = useRef({
    resumeFromSeq: 0,
    streamSessionId: null,
  })
  const [reconnectTick, setReconnectTick] = useState(0)
  const [documentVisible, setDocumentVisible] = useState(() => (
    typeof document !== 'object'
      ? true
      : isBotLensLiveDocumentVisible(document.visibilityState)
  ))
  const previousDocumentVisibleRef = useRef(documentVisible)
  const transportEpoch = buildBotLensLiveTransportEpoch({
    open,
    botId,
    runId,
    transportEligible,
    documentVisible,
    reconnectTick,
  })

  useEffect(() => {
    if (typeof document !== 'object' || typeof document.addEventListener !== 'function') {
      return undefined
    }
    const handleVisibilityChange = () => {
      setDocumentVisible(isBotLensLiveDocumentVisible(document.visibilityState))
    }
    handleVisibilityChange()
    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange)
  }, [])

  useEffect(() => {
    if (previousDocumentVisibleRef.current && !documentVisible) {
      logger?.info?.('botlens_run_ws_suspended_hidden', {
        bot_id: botId,
        run_id: runId,
        resume_from_seq: latestCursorRef.current.resumeFromSeq,
        stream_session_id: latestCursorRef.current.streamSessionId || null,
      })
    }
    previousDocumentVisibleRef.current = documentVisible
  }, [botId, documentVisible, logger, runId])

  const clearPendingMessages = useCallback(() => {
    if (pendingFrameRef.current !== null) {
      window.clearTimeout(pendingFrameRef.current)
    }
    pendingFrameRef.current = null
    pendingMessagesRef.current = []
    pendingBytesRef.current = 0
    bufferOverflowRef.current = false
  }, [])

  const flushPendingMessages = useCallback(() => {
    pendingFrameRef.current = null
    const pending = pendingMessagesRef.current
    if (!pending.length) return

    let count = 0
    let bytes = 0
    while (count < pending.length && count < BOTLENS_LIVE_FRAME_MESSAGE_LIMIT) {
      const nextBytes = Math.max(0, Number(pending[count]?.rawBytes || 0) || 0)
      if (count > 0 && bytes + nextBytes > BOTLENS_LIVE_FRAME_BYTE_LIMIT) break
      bytes += nextBytes
      count += 1
    }
    const entries = pending.splice(0, Math.max(count, 1))
    pendingBytesRef.current = Math.max(
      0,
      pendingBytesRef.current - entries.reduce(
        (total, entry) => total + Math.max(0, Number(entry?.rawBytes || 0) || 0),
        0,
      ),
    )
    const messages = entries.map((entry) => entry.message)
    startTransition(() => {
      dispatch({ type: 'live/messagesReceived', messages })
    })

    if (pending.length) {
      pendingFrameRef.current = window.setTimeout(flushPendingMessages, BOTLENS_LIVE_RENDER_INTERVAL_MS)
    }
  }, [dispatch])

  const queueLiveMessage = useCallback((message, rawBytes) => {
    const nextCount = pendingMessagesRef.current.length + 1
    const nextBytes = pendingBytesRef.current + Math.max(0, Number(rawBytes || 0) || 0)
    const withinLimit = nextCount <= BOTLENS_LIVE_MESSAGE_LIMIT && nextBytes <= BOTLENS_LIVE_BYTE_LIMIT
    // Keep the boundary message before closing the socket so the ordered
    // reducer can advance its cursor and the reconnect can safely resume.
    pendingMessagesRef.current.push({ message, rawBytes })
    pendingBytesRef.current = nextBytes
    if (pendingFrameRef.current === null) {
      pendingFrameRef.current = window.setTimeout(flushPendingMessages, 0)
    }
    return withinLimit
  }, [flushPendingMessages])

  const closeSocket = useCallback(() => {
    if (reconnectTimerRef.current) {
      window.clearTimeout(reconnectTimerRef.current)
      reconnectTimerRef.current = null
    }
    if (stableConnectionTimerRef.current) {
      window.clearTimeout(stableConnectionTimerRef.current)
      stableConnectionTimerRef.current = null
    }
    if (socketRef.current) {
      try {
        socketRef.current.close()
      } catch (closeError) {
        void closeError
      }
    }
    socketRef.current = null
    subscriptionRef.current = { socket: null, symbolKey: null }
    clearPendingMessages()
  }, [clearPendingMessages])

  useEffect(() => {
    latestSelectionRef.current = {
      selectedSymbolKey,
      selectedSymbolReady,
    }
  }, [selectedSymbolKey, selectedSymbolReady])

  useEffect(() => {
    latestCursorRef.current = {
      resumeFromSeq: Math.max(0, Number(resumeFromSeq || 0) || 0),
      streamSessionId: String(streamSessionId || '').trim() || null,
    }
  }, [resumeFromSeq, streamSessionId])

  const syncSelectedSymbolSubscription = useCallback((socket = socketRef.current) => {
    const { selectedSymbolKey: latestSelectedSymbolKey, selectedSymbolReady: latestSelectedSymbolReady } =
      latestSelectionRef.current
    const normalizedSymbolKey = normalizeSeriesKey(latestSelectedSymbolKey || '')
    const currentSubscription = subscriptionRef.current

    if (!shouldSendBotLensSelectedSymbolSubscription({
      socketReadyState: socket?.readyState,
      selectedSymbolKey: normalizedSymbolKey,
      selectedSymbolReady: latestSelectedSymbolReady,
      subscribedSymbolKey: currentSubscription?.symbolKey || null,
      subscriptionSocketMatches: currentSubscription?.socket === socket,
    })) {
      return false
    }

    try {
      const cursor = latestCursorRef.current
      socket.send(JSON.stringify(buildSelectedSymbolSubscriptionPayload({
        selectedSymbolKey: normalizedSymbolKey,
        resumeFromSeq: cursor.resumeFromSeq,
        streamSessionId: cursor.streamSessionId,
      })))
      subscriptionRef.current = { socket, symbolKey: normalizedSymbolKey }
      dispatch({ type: 'live/subscribedSymbol', symbolKey: normalizedSymbolKey })
      return true
    } catch (err) {
      logger?.warn?.(
        'botlens_run_ws_subscribe_failed',
        {
          bot_id: botId,
          run_id: runId,
          selected_symbol_key: normalizedSymbolKey,
        },
        err,
      )
      return false
    }
  }, [botId, dispatch, logger, runId])

  useEffect(() => {
    if (!shouldOpenBotLensLiveTransport({
      open,
      botId,
      runId,
      transportEligible,
      documentVisible,
    })) {
      closeSocket()
      reconnectRef.current = 0
      return undefined
    }

    let cancelled = false
    const token = ++sessionTokenRef.current
    dispatch({ type: 'live/connectionStateChanged', connectionState: 'connecting' })
    const latestCursor = latestCursorRef.current
    const latestSelection = latestSelectionRef.current
    const socket = openBotLensLiveStream(botId, {
      resumeFromSeq: latestCursor.resumeFromSeq,
      streamSessionId: latestCursor.streamSessionId,
      selectedSymbolKey: latestSelection.selectedSymbolReady
        ? (normalizeSeriesKey(latestSelection.selectedSymbolKey || '') || null)
        : null,
    })

    if (!socket) {
      dispatch({ type: 'live/connectionStateChanged', connectionState: 'error' })
      dispatch({ type: 'ui/error', error: 'BotLens live websocket unavailable' })
      return undefined
    }

    socketRef.current = socket

    socket.onopen = () => {
      if (cancelled || token !== sessionTokenRef.current) return
      bufferOverflowRef.current = false
      if (stableConnectionTimerRef.current) {
        window.clearTimeout(stableConnectionTimerRef.current)
      }
      stableConnectionTimerRef.current = window.setTimeout(() => {
        stableConnectionTimerRef.current = null
        reconnectRef.current = 0
        dispatch({ type: 'live/reconnectAttempt', attempt: 0 })
      }, BOTLENS_LIVE_STABLE_CONNECTION_MS)
      dispatch({ type: 'live/connectionStateChanged', connectionState: 'open' })
      const currentSelection = latestSelectionRef.current
      const currentCursor = latestCursorRef.current
      subscriptionRef.current = {
        socket,
        symbolKey: currentSelection.selectedSymbolReady
          ? (normalizeSeriesKey(currentSelection.selectedSymbolKey || '') || null)
          : null,
      }
      logger?.info?.('botlens_run_ws_open', {
        bot_id: botId,
        run_id: runId,
        selected_symbol_key: currentSelection.selectedSymbolKey || null,
        resume_from_seq: currentCursor.resumeFromSeq,
        stream_session_id: currentCursor.streamSessionId || null,
      })
      syncSelectedSymbolSubscription(socket)
    }

    socket.onmessage = (event) => {
      if (bufferOverflowRef.current) return
      if (cancelled || token !== sessionTokenRef.current) return
      try {
        const message = JSON.parse(event.data)
        if (String(message?.type || '') === 'botlens_live_connected') {
          dispatch({ type: 'live/connected', message })
          return
        }
        if (String(message?.type || '') === 'botlens_live_reset_required') {
          dispatch({ type: 'live/connectionStateChanged', connectionState: 'stale' })
          logger?.warn?.('botlens_run_ws_reset_required', {
            bot_id: botId,
            run_id: runId,
            reason: message?.reason || null,
            current_stream_seq: message?.current_stream_seq || null,
          })
          refreshSession()
          return
        }
        const rawBytes = typeof event.data === 'string' ? event.data.length * 2 : 0
        if (!queueLiveMessage(message, rawBytes)) {
          bufferOverflowRef.current = true
          dispatch({ type: 'live/connectionStateChanged', connectionState: 'reconnecting' })
          logger?.warn?.('botlens_run_ws_client_buffer_overflow', {
            bot_id: botId,
            run_id: runId,
            queued_messages: pendingMessagesRef.current.length,
            queued_bytes: pendingBytesRef.current,
            recovery: 'ordered_resume',
          })
          try {
            socket.close(4001, 'client_backpressure_resume')
          } catch (closeError) {
            logger?.warn?.('botlens_run_ws_backpressure_close_failed', {
              bot_id: botId,
              run_id: runId,
            }, closeError)
          }
        }
      } catch (err) {
        logger?.warn?.('botlens_run_ws_parse_failed', { bot_id: botId }, err)
      }
    }

    socket.onerror = (err) => {
      if (cancelled || token !== sessionTokenRef.current) return
      logger?.warn?.('botlens_run_ws_error', { bot_id: botId }, err)
      dispatch({ type: 'live/connectionStateChanged', connectionState: 'error' })
    }

    socket.onclose = () => {
      if (cancelled || token !== sessionTokenRef.current) return
      socketRef.current = null
      subscriptionRef.current = { socket: null, symbolKey: null }
      if (stableConnectionTimerRef.current) {
        window.clearTimeout(stableConnectionTimerRef.current)
        stableConnectionTimerRef.current = null
      }
      const shouldRetry = Boolean(runId && open && transportEligible && documentVisible)
      const nextAttempt = shouldRetry ? reconnectRef.current + 1 : reconnectRef.current
      if (shouldRetry) {
        reconnectRef.current = nextAttempt
        dispatch({ type: 'live/reconnectAttempt', attempt: nextAttempt })
      }
      dispatch({
        type: 'live/connectionStateChanged',
        connectionState: shouldRetry ? 'reconnecting' : 'closed',
      })
      if (!shouldRetry) return
      const reconnectDelayMs = botLensReconnectDelayMs(nextAttempt, Math.random())
      logger?.info?.('botlens_run_ws_reconnect_scheduled', {
        bot_id: botId,
        run_id: runId,
        attempt: nextAttempt,
        delay_ms: reconnectDelayMs,
        resume_from_seq: latestCursorRef.current.resumeFromSeq,
      })
      const reconnectWhenDrained = () => {
        reconnectTimerRef.current = null
        if (cancelled || token !== sessionTokenRef.current) return
        if (pendingMessagesRef.current.length) {
          reconnectTimerRef.current = window.setTimeout(reconnectWhenDrained, 50)
          return
        }
        setReconnectTick((value) => value + 1)
      }
      reconnectTimerRef.current = window.setTimeout(reconnectWhenDrained, reconnectDelayMs)
    }

    return () => {
      cancelled = true
      closeSocket()
    }
  }, [
    botId,
    closeSocket,
    dispatch,
    documentVisible,
    logger,
    open,
    refreshSession,
    runId,
    queueLiveMessage,
    syncSelectedSymbolSubscription,
    transportEligible,
    transportEpoch,
  ])

  useEffect(() => {
    syncSelectedSymbolSubscription()
  }, [selectedSymbolKey, selectedSymbolReady, syncSelectedSymbolSubscription])

  return { closeSocket }
}
