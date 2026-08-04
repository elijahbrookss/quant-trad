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
export function useFleetBotsFeed({ enabled = true } = {}) {
  const [bots, setBots] = useState([])
  const [strategies, setStrategies] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [nowEpochMs, setNowEpochMs] = useState(Date.now())
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  const { state: botStreamState, hasReceivedSnapshot } = useBotStream({
    replaceBots: (incoming) => {
      setBots(replaceFleetBotsSnapshot(incoming))
      setLoading(false)
      setError(null)
    },
    upsertBot: (bot) => setBots((prev) => upsertFleetBotRecord(prev, bot)),
    mergeBotRuntime: (id, runtime) => setBots((prev) => mergeFleetBotRuntime(prev, id, runtime)),
    removeBot: (id) => setBots((prev) => removeFleetBotRecord(prev, id)),
    enabled,
  })

  useEffect(() => {
    if (!enabled) return undefined
    let mounted = true
    async function loadStrategies() {
      try {
        const strategyList = await fetchStrategies()
        if (!mounted) return
        setStrategies(Array.isArray(strategyList) ? strategyList : [])
      } catch (err) {
        if (mounted) setError(err?.message || 'Unable to load strategy labels')
      }
    }
    loadStrategies()
    return () => {
      mounted = false
    }
  }, [enabled])

  useEffect(() => {
    if (!enabled) return undefined
    if (hasReceivedSnapshot) return undefined
    let mounted = true
    const timer = setTimeout(async () => {
      try {
        const botList = await listBots()
        if (!mounted) return
        setBots(replaceFleetBotsSnapshot(botList))
        setError(null)
      } catch (err) {
        if (mounted) setError(err?.message || 'Unable to load fleet')
      } finally {
        if (mounted) setLoading(false)
      }
    }, botStreamState === 'error' ? 0 : 4000)
    return () => {
      mounted = false
      clearTimeout(timer)
    }
  }, [botStreamState, enabled, hasReceivedSnapshot])

  useEffect(() => {
    if (!enabled) return undefined
    if (refreshRevision === 0) return undefined
    let mounted = true
    setLoading(true)
    setError(null)
    listBots()
      .then((botList) => {
        if (mounted) setBots(replaceFleetBotsSnapshot(botList))
      })
      .catch((err) => {
        if (mounted) setError(err?.message || 'Unable to refresh fleet')
      })
      .finally(() => {
        if (mounted) setLoading(false)
      })
    return () => {
      mounted = false
    }
  }, [enabled, refreshRevision])

  useEffect(() => {
    const id = setInterval(() => setNowEpochMs(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  const strategyLookup = useMemo(() => new Map(strategies.map((strategy) => [strategy.id, strategy])), [strategies])
  const sortedBots = useMemo(() => sortBots(bots), [bots])

  return { bots, sortedBots, strategies, strategyLookup, loading, error, hasReceivedSnapshot, nowEpochMs, refresh }
}
