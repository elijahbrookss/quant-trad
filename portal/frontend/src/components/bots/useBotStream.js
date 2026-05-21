import { useEffect, useRef, useState } from 'react'
import { createLogger } from '../../utils/logger.js'
import { openBotsStream } from '../../adapters/bot.adapter.js'

const EVENT_SOURCE_CLOSED_STATE = typeof EventSource === 'function' ? EventSource.CLOSED : 2

/**
 * Subscribes to the bots SSE stream for authoritative fleet lifecycle updates.
 * Consumers supply explicit replace/upsert/runtime/delete handlers so ownership stays obvious.
 */
export function mapBotsStreamReadyState(readyState) {
  return readyState === EVENT_SOURCE_CLOSED_STATE ? 'error' : 'connecting'
}

export function resolveBotsStreamMutation(eventType, payload) {
  const normalizedEventType = String(eventType || '').trim().toLowerCase()

  if (normalizedEventType === 'bot_deleted') {
    const botId = String(payload?.bot_id || '').trim()
    return botId ? { type: 'remove', botId } : null
  }

  if (normalizedEventType === 'bot_runtime') {
    const botId = String(payload?.bot_id || '').trim()
    const runtime = payload?.runtime
    if (!botId || !runtime || typeof runtime !== 'object') return null
    return { type: 'runtime', botId, runtime }
  }

  if (normalizedEventType === 'snapshot') {
    const bots = Array.isArray(payload) ? payload : Array.isArray(payload?.bots) ? payload.bots : null
    return bots ? { type: 'replace', bots, hydrated: true } : null
  }

  if (Array.isArray(payload)) {
    return { type: 'replace', bots: payload, hydrated: false }
  }

  if (Array.isArray(payload?.bots)) {
    return { type: 'replace', bots: payload.bots, hydrated: false }
  }

  if (payload?.bot && typeof payload.bot === 'object') {
    return { type: 'upsert', bot: payload.bot }
  }

  if (payload?.id) {
    return { type: 'upsert', bot: payload }
  }

  return null
}

export function useBotStream({ replaceBots, upsertBot, mergeBotRuntime, removeBot }) {
  const [botStreamState, setBotStreamState] = useState('idle')
  const [hasReceivedSnapshot, setHasReceivedSnapshot] = useState(false)
  const botStreamRef = useRef(null)
  const loggerRef = useRef(createLogger('BotStream'))
  const replaceBotsRef = useRef(replaceBots)
  const upsertBotRef = useRef(upsertBot)
  const mergeBotRuntimeRef = useRef(mergeBotRuntime)
  const removeBotRef = useRef(removeBot)

  // Keep refs up to date
  useEffect(() => {
    replaceBotsRef.current = replaceBots
    upsertBotRef.current = upsertBot
    mergeBotRuntimeRef.current = mergeBotRuntime
    removeBotRef.current = removeBot
  })

  useEffect(() => {
    const source = openBotsStream()
    if (!source) {
      loggerRef.current.info('bot_stream_unavailable')
      setBotStreamState('error')
      return undefined
    }

    loggerRef.current.info('bot_stream_connecting')
    botStreamRef.current = source
    setBotStreamState('connecting')

    const handlePayload = (event) => {
      try {
        const eventType = event?.type
        const payload = JSON.parse(event.data)
        const mutation = resolveBotsStreamMutation(eventType, payload)
        if (mutation?.type === 'remove') {
          removeBotRef.current?.(mutation.botId)
        } else if (mutation?.type === 'replace') {
          replaceBotsRef.current?.(mutation.bots)
          if (mutation.hydrated) setHasReceivedSnapshot(true)
        } else if (mutation?.type === 'upsert') {
          upsertBotRef.current?.(mutation.bot)
        } else if (mutation?.type === 'runtime') {
          mergeBotRuntimeRef.current?.(mutation.botId, mutation.runtime)
        }
        setBotStreamState('open')
      } catch (err) {
        loggerRef.current.warn('bot_stream_payload_parse_failed', { message: err?.message }, err)
      }
    }

    const handleError = (event) => {
      loggerRef.current.warn('bot_stream_error', { message: event?.message })
      setBotStreamState(mapBotsStreamReadyState(source.readyState))
    }

    source.onmessage = handlePayload
    source.addEventListener('snapshot', handlePayload)
    source.addEventListener('update', handlePayload)
    source.addEventListener('bot', handlePayload)
    source.addEventListener('bot_status', handlePayload)
    source.addEventListener('bot_runtime', handlePayload)
    source.addEventListener('bot_deleted', handlePayload)
    source.onerror = handleError
    source.onopen = () => {
      loggerRef.current.info('bot_stream_open')
      setBotStreamState('open')
    }

    return () => {
      if (botStreamRef.current) {
        loggerRef.current.info('bot_stream_closed')
        botStreamRef.current.close()
        botStreamRef.current = null
      }
    }
  }, [])

  return { state: botStreamState, hasReceivedSnapshot }
}
