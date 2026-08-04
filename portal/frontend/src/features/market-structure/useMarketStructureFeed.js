import { useCallback, useEffect, useState } from 'react'
import {
  fetchMarketStructureSnapshot,
  openMarketStructureStream,
} from '../../adapters/marketData.adapter.js'

export function formatMarketStructureComponentError(error) {
  if (!error) return null
  return [error.message, error.details || error.code].filter(Boolean).join(' ')
}

export function useMarketStructureFeed({ enabled = true } = {}) {
  const [definitions, setDefinitions] = useState([])
  const [sessions, setSessions] = useState([])
  const [normalizationSpecs, setNormalizationSpecs] = useState([])
  const [statusByDefinition, setStatusByDefinition] = useState({})
  const [componentErrors, setComponentErrors] = useState({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [streamError, setStreamError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      return undefined
    }
    let mounted = true
    let source = null
    let fallbackTimerId = null
    let receivedStreamSnapshot = false

    function applyPayload(payload) {
      if (!mounted || !payload) return
      const nextDefinitions = Array.isArray(payload.definitions) ? payload.definitions : []
      const summaries = payload.status_by_definition && typeof payload.status_by_definition === 'object'
        ? payload.status_by_definition
        : {}
      setDefinitions(nextDefinitions)
      setSessions(Array.isArray(payload.sessions) ? payload.sessions : [])
      setNormalizationSpecs(Array.isArray(payload.normalization_specs) ? payload.normalization_specs : [])
      setComponentErrors(payload.component_errors && typeof payload.component_errors === 'object' ? payload.component_errors : {})
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
        receivedStreamSnapshot = true
        applyPayload(JSON.parse(event.data))
        setStreamError(null)
      } catch {
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

    source = openMarketStructureStream({ sessionLimit: 250 })
    if (source) {
      source.addEventListener('snapshot', applyEvent)
      source.addEventListener('delta', applyEvent)
      source.onerror = () => {
        if (mounted) setStreamError('Live market updates disconnected; reconnecting.')
      }
      fallbackTimerId = setTimeout(() => {
        if (!receivedStreamSnapshot) load()
      }, 4_000)
    } else {
      setStreamError('Live market updates are unavailable in this browser.')
      load()
    }
    if (refreshRevision > 0) load()

    return () => {
      mounted = false
      source?.close()
      if (fallbackTimerId) clearTimeout(fallbackTimerId)
    }
  }, [enabled, refreshRevision])

  return {
    definitions,
    sessions,
    normalizationSpecs,
    statusByDefinition,
    componentErrors,
    loading,
    error,
    streamError,
    observedAt,
    refresh,
  }
}
