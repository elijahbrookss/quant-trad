import { useCallback, useEffect, useState } from 'react'

import { fetchATMTemplates, fetchStrategies } from '../../adapters/strategy.adapter.js'
import { fetchIndicators } from '../../adapters/indicator.adapter.js'

const useStrategyData = ({ logger } = {}) => {
  const [strategies, setStrategies] = useState([])
  const [indicators, setIndicators] = useState([])
  const [atmTemplates, setAtmTemplates] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const refreshStrategies = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const payload = await fetchStrategies()
      const list = Array.isArray(payload) ? payload : []
      setStrategies(list)
      return list
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
    indicators,
    setIndicators,
    atmTemplates,
    loading,
    error,
    refreshStrategies,
    refreshTemplates,
    refreshIndicators,
  }
}

export default useStrategyData
