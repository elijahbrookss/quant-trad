import { useCallback, useEffect, useRef, useState } from 'react'
import {
  fetchMarketStructureStatus,
  listMarketNormalizationSpecs,
  listMarketStructureDefinitions,
  listMarketStructureSessions,
} from '../../adapters/marketData.adapter.js'

const POLL_INTERVAL_MS = 60_000

export function useMarketStructureFeed() {
  const [definitions, setDefinitions] = useState([])
  const [sessions, setSessions] = useState([])
  const [normalizationSpecs, setNormalizationSpecs] = useState([])
  const [statusByDefinition, setStatusByDefinition] = useState({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const definitionsRef = useRef([])
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    let mounted = true
    let timeoutId = null

    async function load() {
      const sourceResults = await Promise.allSettled([
        listMarketStructureDefinitions(),
        listMarketStructureSessions({ limit: 250 }),
        listMarketNormalizationSpecs(),
      ])
      if (!mounted) return

      const errors = []
      const [definitionResult, sessionResult, specResult] = sourceResults
      let definitionRows = definitionsRef.current

      if (definitionResult.status === 'fulfilled') {
        definitionRows = definitionResult.value
        definitionsRef.current = definitionRows
        setDefinitions(definitionRows)
      } else {
        errors.push(
          'Stream definitions: ' +
          (definitionResult.reason?.message || 'evidence unavailable'),
        )
      }

      if (sessionResult.status === 'fulfilled') {
        setSessions(sessionResult.value)
      } else {
        errors.push(
          'Stream sessions: ' +
          (sessionResult.reason?.message || 'evidence unavailable'),
        )
      }

      if (specResult.status === 'fulfilled') {
        setNormalizationSpecs(specResult.value)
      } else {
        errors.push(
          'Normalization specs: ' +
          (specResult.reason?.message || 'evidence unavailable'),
        )
      }

      const statusResults = await Promise.allSettled(
        definitionRows.map((definition) =>
          fetchMarketStructureStatus(definition.id),
        ),
      )
      if (!mounted) return

      const nextStatus = {}
      let unavailableCount = 0
      statusResults.forEach((result, index) => {
        const definitionId = definitionRows[index]?.id
        if (!definitionId) return
        if (result.status === 'fulfilled') {
          nextStatus[definitionId] = {
            available: true,
            value: result.value,
          }
        } else {
          unavailableCount += 1
          nextStatus[definitionId] = {
            available: false,
            error: result.reason?.message || 'Status unavailable',
            value: null,
          }
        }
      })
      setStatusByDefinition(nextStatus)
      if (unavailableCount) {
        errors.push(
          'Evidence status unavailable for ' +
          unavailableCount +
          ' stream definition' +
          (unavailableCount === 1 ? '.' : 's.'),
        )
      }
      setError(errors.join(' ') || null)
      setObservedAt(new Date().toISOString())
      setLoading(false)
      timeoutId = setTimeout(load, POLL_INTERVAL_MS)
    }

    setLoading(true)
    load()
    return () => {
      mounted = false
      if (timeoutId) clearTimeout(timeoutId)
    }
  }, [refreshRevision])

  return {
    definitions,
    sessions,
    normalizationSpecs,
    statusByDefinition,
    loading,
    error,
    observedAt,
    refresh,
  }
}
