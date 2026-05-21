import { useCallback, useEffect, useState } from 'react'

import {
  fetchATMTemplates,
  fetchStrategies,
  fetchStrategy,
} from '../../adapters/strategy.adapter.js'
import { fetchIndicators } from '../../adapters/indicator.adapter.js'

const parseUpdatedAt = (value) => {
  if (!value) return null
  const epoch = Date.parse(value)
  return Number.isFinite(epoch) ? epoch : null
}

export const mergeStrategyState = (current, incoming) => {
  if (!incoming?.id) {
    return current || null
  }
  if (!current?.id) {
    return incoming
  }

  const currentUpdatedAt = parseUpdatedAt(current?.updated_at || current?.strategy?.updated_at)
  const incomingUpdatedAt = parseUpdatedAt(incoming?.updated_at || incoming?.strategy?.updated_at)

  if (
    currentUpdatedAt !== null
    && incomingUpdatedAt !== null
    && incomingUpdatedAt < currentUpdatedAt
  ) {
    return current
  }

  return {
    ...current,
    ...incoming,
  }
}

const useStrategyData = ({ logger } = {}) => {
  const [strategies, setStrategies] = useState([])
  const [indicators, setIndicators] = useState([])
  const [atmTemplates, setAtmTemplates] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const upsertStrategy = useCallback((incoming) => {
    if (!incoming?.id) {
      return null
    }

    setStrategies((prev) => {
      const next = Array.isArray(prev) ? [...prev] : []
      const index = next.findIndex((entry) => entry?.id === incoming.id)
      if (index === -1) {
        next.push(incoming)
        return next
      }
      next[index] = mergeStrategyState(next[index], incoming)
      return next
    })

    return incoming
  }, [])

  const removeStrategy = useCallback((strategyId) => {
    if (!strategyId) {
      return
    }
    setStrategies((prev) => prev.filter((strategy) => strategy?.id !== strategyId))
  }, [])

  const refreshStrategies = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const summaries = await fetchStrategies()
      setStrategies((prev) => {
        const existing = new Map((Array.isArray(prev) ? prev : []).map((strategy) => [strategy?.id, strategy]))
        return (Array.isArray(summaries) ? summaries : []).map((summary) => {
          const current = existing.get(summary?.id)
          return current ? mergeStrategyState(current, summary) : summary
        })
      })
      return Array.isArray(summaries) ? summaries : []
    } catch (err) {
      const message = err?.message || 'Unable to load strategies'
      setError(message)
      if (logger?.error) {
        logger.error('strategy_load_failed', err)
      }
      return []
    } finally {
      setLoading(false)
    }
  }, [logger])

  const refreshStrategyDetail = useCallback(
    async (strategyId) => {
      if (!strategyId) {
        return null
      }
      try {
        const detail = await fetchStrategy(strategyId)
        return upsertStrategy(detail)
      } catch (err) {
        if (logger?.warn) {
          logger.warn('strategy_detail_load_failed', { strategyId, error: err?.message || err })
        }
        throw err
      }
    },
    [logger, upsertStrategy],
  )

  const refreshTemplates = useCallback(async () => {
    try {
      const payload = await fetchATMTemplates()
      setAtmTemplates(Array.isArray(payload) ? payload : [])
    } catch (err) {
      if (logger?.warn) {
        logger.warn('atm_templates_fetch_failed', err)
      }
    }
  }, [logger])

  const refreshIndicators = useCallback(async () => {
    try {
      const payload = await fetchIndicators()
      setIndicators(Array.isArray(payload) ? payload : [])
    } catch (err) {
      if (logger?.warn) {
        logger.warn('indicator_fetch_failed', err)
      }
    }
  }, [logger])

  useEffect(() => {
    refreshStrategies()
    refreshTemplates()
  }, [refreshStrategies, refreshTemplates])

  useEffect(() => {
    refreshIndicators()
  }, [refreshIndicators])

  return {
    strategies,
    setStrategies,
    upsertStrategy,
    removeStrategy,
    indicators,
    setIndicators,
    atmTemplates,
    loading,
    error,
    refreshStrategies,
    refreshStrategyDetail,
    refreshTemplates,
    refreshIndicators,
  }
}

export default useStrategyData
