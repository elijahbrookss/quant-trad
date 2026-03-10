import { useCallback, useMemo } from 'react'

import { fetchIndicator, fetchIndicatorStrategies } from '../../adapters/indicator.adapter.js'

const useIndicatorCache = ({ indicators, setIndicators, logger } = {}) => {
  const indicatorLookup = useMemo(() => {
    const map = new Map()
    for (const indicator of indicators || []) {
      if (!indicator?.id) continue
      map.set(indicator.id, indicator)
    }
    return map
  }, [indicators])

  const ensureIndicatorDetails = useCallback(
    async (indicatorId) => {
      if (typeof indicatorId !== 'string') {
        return null
      }
      const trimmed = indicatorId.trim()
      if (!trimmed.length) {
        return null
      }
      const existing = indicatorLookup.get(trimmed)
      if (existing?.signal_rules && existing.signal_rules.length > 0) {
        return existing
      }
      try {
        const [payload, relatedStrategies] = await Promise.all([
          fetchIndicator(trimmed),
          fetchIndicatorStrategies(trimmed).catch(() => []),
        ])
        if (!payload) {
          return existing || null
        }
        const enriched = {
          ...payload,
          strategies: Array.isArray(relatedStrategies) ? relatedStrategies : [],
        }
        setIndicators((prev) => {
          const map = new Map(prev.map((indicator) => [indicator.id, indicator]))
          const merged = { ...(map.get(enriched.id) || {}), ...enriched }
          map.set(enriched.id, merged)
          return Array.from(map.values())
        })
        return enriched
      } catch (err) {
        logger?.warn?.('indicator_detail_fetch_failed', { indicatorId: trimmed }, err)
        return existing || null
      }
    },
    [indicatorLookup, logger, setIndicators],
  )

  return {
    indicatorLookup,
    ensureIndicatorDetails,
  }
}

export default useIndicatorCache
