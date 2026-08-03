import { useCallback, useEffect, useState } from 'react'
import {
  fetchMarketStructureSnapshot,
  openMarketStructureStream,
} from '../../adapters/marketData.adapter.js'

export function useMarketStructureFeed() {
  const [definitions, setDefinitions] = useState([])
  const [sessions, setSessions] = useState([])
  const [normalizationSpecs, setNormalizationSpecs] = useState([])
  const [statusByDefinition, setStatusByDefinition] = useState({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [streamError, setStreamError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    let mounted = true
    let source = null

    function applyPayload(payload) {
      if (!mounted || !payload) return
      const nextDefinitions = Array.isArray(payload.definitions) ? payload.definitions : []
      const summaries = payload.status_by_definition && typeof payload.status_by_definition === 'object'
        ? payload.status_by_definition
        : {}
      setDefinitions(nextDefinitions)
      setSessions(Array.isArray(payload.sessions) ? payload.sessions : [])
      setNormalizationSpecs(Array.isArray(payload.normalization_specs) ? payload.normalization_specs : [])
      setStatusByDefinition(Object.fromEntries(nextDefinitions.map((definition) => {
        const value = summaries[definition.id]
        return [definition.id, value
          ? { available: true, value }
          : { available: false, value: null, error: 'Status unavailable' }]
      })))
      setObservedAt(payload.observed_at || new Date().toISOString())
      setError(null)
      setLoading(false)
    }

    function applyEvent(event) {
      try {
        applyPayload(JSON.parse(event.data))
        setStreamError(null)
      } catch (_error) {
        setStreamError('Live market update could not be read; reconnecting.')
      }
    }

    async function load() {
      setLoading(true)
      try {
        applyPayload(await fetchMarketStructureSnapshot({ sessionLimit: 250 }))
      } catch (loadError) {
        if (mounted) {
          setError(loadError?.message || 'Market evidence unavailable')
          setLoading(false)
        }
      }
    }

    load()
    source = openMarketStructureStream({ sessionLimit: 250 })
    if (source) {
      source.addEventListener('snapshot', applyEvent)
      source.addEventListener('delta', applyEvent)
      source.onerror = () => {
        if (mounted) setStreamError('Live market updates disconnected; reconnecting.')
      }
    } else {
      setStreamError('Live market updates are unavailable in this browser.')
    }

    return () => {
      mounted = false
      source?.close()
    }
  }, [refreshRevision])

  return {
    definitions,
    sessions,
    normalizationSpecs,
    statusByDefinition,
    loading,
    error,
    streamError,
    observedAt,
    refresh,
  }
}
