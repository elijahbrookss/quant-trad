import { useCallback, useEffect, useMemo, useState } from 'react'
import { listBots } from '../../../adapters/bot.adapter.js'
import { fetchStrategies } from '../../../adapters/strategy.adapter.js'
import { useBotStream } from '../../../components/bots/useBotStream.js'
import {
  replaceFleetBotsSnapshot,
  upsertFleetBotRecord,
  mergeFleetBotRuntime,
  removeFleetBotRecord,
} from './useBotsPageController.js'
import { sortBots } from '../fleet/buildBotFleetViewModel.js'

/**
 * Shared bots + strategies feed: initial fetch, SSE delta stream, and a
 * live clock tick for duration/age displays. Used by both the Fleet page
 * and the Overview page so the fetch/stream wiring exists exactly once.
 */
export function useFleetBotsFeed() {
  const [bots, setBots] = useState([])
  const [strategies, setStrategies] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [nowEpochMs, setNowEpochMs] = useState(Date.now())
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  const { hasReceivedSnapshot } = useBotStream({
    replaceBots: (incoming) => setBots(replaceFleetBotsSnapshot(incoming)),
    upsertBot: (bot) => setBots((prev) => upsertFleetBotRecord(prev, bot)),
    mergeBotRuntime: (id, runtime) => setBots((prev) => mergeFleetBotRuntime(prev, id, runtime)),
    removeBot: (id) => setBots((prev) => removeFleetBotRecord(prev, id)),
  })

  useEffect(() => {
    let mounted = true
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const [botList, strategyList] = await Promise.all([listBots(), fetchStrategies()])
        if (!mounted) return
        setBots(replaceFleetBotsSnapshot(botList))
        setStrategies(Array.isArray(strategyList) ? strategyList : [])
      } catch (err) {
        if (mounted) setError(err?.message || 'Unable to load fleet')
      } finally {
        if (mounted) setLoading(false)
      }
    }
    load()
    return () => {
      mounted = false
    }
  }, [refreshRevision])

  useEffect(() => {
    const id = setInterval(() => setNowEpochMs(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  const strategyLookup = useMemo(() => new Map(strategies.map((strategy) => [strategy.id, strategy])), [strategies])
  const sortedBots = useMemo(() => sortBots(bots), [bots])

  return { bots, sortedBots, strategies, strategyLookup, loading, error, hasReceivedSnapshot, nowEpochMs, refresh }
}
