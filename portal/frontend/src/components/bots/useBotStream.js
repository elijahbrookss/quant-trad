import { useCallback, useEffect, useRef, useState } from 'react'
import { createLogger } from '../../utils/logger.js'
import { openBotsStream } from '../../adapters/bot.adapter.js'

/**
 * Subscribes to the bots SSE stream and falls back to polling when unavailable.
 * Consumers supply merge/upsert helpers so this hook stays presentationally agnostic.
 */
export function useBotStream({ mergeBots, upsertBot, applyRuntime, loadBots }) {
  const [botStreamState, setBotStreamState] = useState('idle')
  const [connectKey, setConnectKey] = useState(0)
  const botStreamRef = useRef(null)
  const loggerRef = useRef(createLogger('BotStream'))
  const mergeBotsRef = useRef(mergeBots)
  const upsertBotRef = useRef(upsertBot)
  const applyRuntimeRef = useRef(applyRuntime)
  const loadBotsRef = useRef(loadBots)

  // Keep refs up to date
  useEffect(() => {
    mergeBotsRef.current = mergeBots
    upsertBotRef.current = upsertBot
    applyRuntimeRef.current = applyRuntime
    loadBotsRef.current = loadBots
  })

  useEffect(() => {
    let retryTimer = null

    const connectStream = () => {
      if (botStreamRef.current) {
        botStreamRef.current.close()
        botStreamRef.current = null
      }

      const source = openBotsStream()
      if (!source) {
        loggerRef.current.info('bot_stream_unavailable')
        setBotStreamState('error')
        return
      }

      loggerRef.current.info('bot_stream_connecting')
      botStreamRef.current = source
      setBotStreamState('connecting')

      const handlePayload = (event) => {
        try {
          const data = JSON.parse(event.data)
          if (Array.isArray(data)) {
            mergeBotsRef.current(data)
          } else if (Array.isArray(data?.bots)) {
            mergeBotsRef.current(data.bots)
          } else if (data?.bot) {
            upsertBotRef.current(data.bot)
            if (data.bot?.id && data.bot?.runtime) {
              applyRuntimeRef.current(data.bot.id, data.bot.runtime)
            }
          } else if (data?.id) {
            upsertBotRef.current(data)
            if (data?.id && data?.runtime) {
              applyRuntimeRef.current(data.id, data.runtime)
            }
          } else if (data?.bot_id && data?.runtime) {
            applyRuntimeRef.current(data.bot_id, data.runtime)
          }
          setBotStreamState('open')
        } catch (err) {
          loggerRef.current.warn('bot_stream_payload_parse_failed', { message: err?.message }, err)
        }
      }

      const handleRuntime = (event) => {
        try {
          const data = JSON.parse(event.data)
          const botId = data?.bot_id || data?.bot?.id
          const runtime = data?.runtime || data?.bot?.runtime
          if (botId && runtime) {
            applyRuntimeRef.current(botId, runtime)
            setBotStreamState('open')
          }
        } catch (err) {
          loggerRef.current.warn('bot_stream_runtime_parse_failed', { message: err?.message }, err)
        }
      }

      const handleError = (event) => {
        loggerRef.current.warn('bot_stream_error', { message: event?.message })
        setBotStreamState('error')
        retryTimer = setTimeout(connectStream, 2500)
      }

      source.onmessage = handlePayload
      source.addEventListener('snapshot', handlePayload)
      source.addEventListener('update', handlePayload)
      source.addEventListener('bot_runtime', handleRuntime)
      source.onerror = handleError
      source.onopen = () => {
        loggerRef.current.info('bot_stream_open')
        setBotStreamState('open')
      }
    }

    connectStream()

    return () => {
      if (retryTimer) clearTimeout(retryTimer)
      if (botStreamRef.current) {
        loggerRef.current.info('bot_stream_closed')
        botStreamRef.current.close()
        botStreamRef.current = null
      }
    }
  }, [connectKey])

  useEffect(() => {
    if (botStreamState === 'open') return undefined
    const intervalMs = botStreamState === 'error' ? 2000 : 4000
    const id = setInterval(() => loadBotsRef.current(false), intervalMs)
    return () => clearInterval(id)
  }, [botStreamState])

  const reconnect = useCallback(() => {
    setConnectKey((prev) => prev + 1)
  }, [])

  return { state: botStreamState, reconnect }
}
