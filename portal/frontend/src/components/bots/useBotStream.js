import { useEffect, useRef, useState } from 'react'
import { openBotsStream } from '../../adapters/bot.adapter.js'

/**
 * Subscribes to the bots SSE stream and falls back to polling when unavailable.
 * Consumers supply merge/upsert helpers so this hook stays presentationally agnostic.
 */
export function useBotStream({ mergeBots, upsertBot, applyRuntime, loadBots }) {
  const [botStreamState, setBotStreamState] = useState('idle')
  const botStreamRef = useRef(null)

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
          console.log('[useBotStream] event=payload_received', {
            type: event.type,
            dataType: Array.isArray(data) ? 'array' : typeof data,
            hasBot: !!data?.bot,
            hasBotId: !!data?.id,
            hasRuntime: !!data?.runtime,
          })
          if (Array.isArray(data)) {
            mergeBots(data)
          } else if (Array.isArray(data?.bots)) {
            mergeBots(data.bots)
          } else if (data?.bot) {
            upsertBot(data.bot)
            if (data.bot?.id && data.bot?.runtime) {
              console.log('[useBotStream] event=applying_runtime', {
                botId: data.bot.id,
                hasOverlays: !!data.bot.runtime?.overlays,
                overlayCount: data.bot.runtime?.overlays?.length || 0,
              })
              applyRuntime(data.bot.id, data.bot.runtime)
            }
          } else if (data?.id) {
            upsertBot(data)
            if (data?.id && data?.runtime) {
              console.log('[useBotStream] event=applying_runtime', {
                botId: data.id,
                hasOverlays: !!data.runtime?.overlays,
                overlayCount: data.runtime?.overlays?.length || 0,
              })
              applyRuntime(data.id, data.runtime)
            }
          } else if (data?.bot_id && data?.runtime) {
            console.log('[useBotStream] event=applying_runtime', {
              botId: data.bot_id,
              hasOverlays: !!data.runtime?.overlays,
              overlayCount: data.runtime?.overlays?.length || 0,
            })
            applyRuntime(data.bot_id, data.runtime)
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
          console.log('[useBotStream] event=runtime_received', {
            botId,
            hasOverlays: !!runtime?.overlays,
            overlayCount: runtime?.overlays?.length || 0,
          })
          if (botId && runtime) {
            applyRuntime(botId, runtime)
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
  }, [applyRuntime, mergeBots, upsertBot])

  useEffect(() => {
    if (botStreamState === 'open') return undefined
    const intervalMs = botStreamState === 'error' ? 2000 : 4000
    const id = setInterval(() => loadBots(false), intervalMs)
    return () => clearInterval(id)
  }, [botStreamState, loadBots])

  return botStreamState
}

