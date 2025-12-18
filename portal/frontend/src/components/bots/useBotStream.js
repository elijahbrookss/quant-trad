import { useEffect, useRef, useState } from 'react'
import { openBotsStream } from '../../adapters/bot.adapter.js'

/**
 * Subscribes to the bots SSE stream and falls back to polling when unavailable.
 * Consumers supply merge/upsert helpers so this hook stays presentationally agnostic.
 */
export function useBotStream({ mergeBots, upsertBot, applyRuntime, loadBots }) {
  const [botStreamState, setBotStreamState] = useState('idle')
  const botStreamRef = useRef(null)
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
        console.info('[BotPanel] bot stream unavailable, skipping SSE attach')
        setBotStreamState('error')
        return
      }

      console.info('[BotPanel] connecting bot stream')
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
          console.warn('[BotPanel] bot stream payload parse failed', err)
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
          console.warn('[BotPanel] bot runtime payload parse failed', err)
        }
      }

      const handleError = (event) => {
        console.info('[BotPanel] bot stream errored, scheduling retry', event?.message)
        setBotStreamState('error')
        retryTimer = setTimeout(connectStream, 2500)
      }

      source.onmessage = handlePayload
      source.addEventListener('snapshot', handlePayload)
      source.addEventListener('update', handlePayload)
      source.addEventListener('bot_runtime', handleRuntime)
      source.onerror = handleError
      source.onopen = () => setBotStreamState('open')
    }

    connectStream()

    return () => {
      if (retryTimer) clearTimeout(retryTimer)
      if (botStreamRef.current) {
        console.info('[BotPanel] closing bot stream')
        botStreamRef.current.close()
        botStreamRef.current = null
      }
    }
  }, []) // No dependencies - using refs instead

  useEffect(() => {
    if (botStreamState === 'open') return undefined
    const intervalMs = botStreamState === 'error' ? 2000 : 4000
    const id = setInterval(() => loadBotsRef.current(false), intervalMs)
    return () => clearInterval(id)
  }, [botStreamState])

  return botStreamState
}

