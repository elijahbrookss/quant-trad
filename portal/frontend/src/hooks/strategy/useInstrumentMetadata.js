import { useCallback, useState } from 'react'

import { fetchTickMetadata } from '../../adapters/provider.adapter.js'

const useInstrumentMetadata = ({ selectedStrategy, refreshStrategies, logger } = {}) => {
  const [instrumentRefreshStatus, setInstrumentRefreshStatus] = useState({})

  const refreshInstrumentMetadata = useCallback(
    async (symbol) => {
      if (!selectedStrategy || !symbol) return
      const providerId = (selectedStrategy.datasource || '').trim().toUpperCase()
      const venueId = (selectedStrategy.exchange || '').trim().toUpperCase()
      const symbolKey = String(symbol).trim().toUpperCase()
      if (!providerId || !venueId) {
        logger?.warn?.('instrument_metadata_refresh_missing_provider', { symbol })
        return
      }
      setInstrumentRefreshStatus((prev) => ({
        ...prev,
        [symbolKey]: { loading: true },
      }))
      try {
        const response = await fetchTickMetadata({
          symbol: symbolKey,
          provider_id: providerId,
          venue_id: venueId,
          timeframe: selectedStrategy.timeframe,
          refresh: true,
          strategy_id: selectedStrategy.id,
        })
        if (response?.errors) {
          const firstError = Object.values(response.errors).find(Boolean)
          throw new Error(firstError || 'Tick metadata unavailable')
        }
        await refreshStrategies()
        logger?.info?.('instrument_metadata_refreshed', { symbol, provider_id: providerId, venue_id: venueId })
        setInstrumentRefreshStatus((prev) => ({
          ...prev,
          [symbolKey]: { loading: false },
        }))
      } catch (err) {
        logger?.error?.('instrument_metadata_refresh_failed', err)
        setInstrumentRefreshStatus((prev) => ({
          ...prev,
          [symbolKey]: { loading: false },
        }))
      }
    },
    [logger, refreshStrategies, selectedStrategy],
  )

  return {
    instrumentRefreshStatus,
    refreshInstrumentMetadata,
  }
}

export default useInstrumentMetadata
