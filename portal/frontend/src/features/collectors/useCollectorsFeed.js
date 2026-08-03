import { useCallback, useEffect, useState } from 'react'
import { listCollectorDefinitions, fetchCollectorAttempts, listInstruments } from '../../adapters/marketData.adapter.js'

const POLL_INTERVAL_MS = 30_000
const ATTEMPTS_LIMIT = 5

/**
 * Collectors have no SSE/WS feed (no per-collector container, no heartbeat —
 * see collectorHealth.js), so this polls on an interval instead of streaming.
 * Reschedules only after the previous fetch settles, so a slow response never
 * causes overlapping requests.
 */
export function useCollectorsFeed() {
  const [collectors, setCollectors] = useState([])
  const [instruments, setInstruments] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    let mounted = true
    let timeoutId = null

    async function load() {
      try {
        const [definitions, instrumentRows] = await Promise.all([
          listCollectorDefinitions(),
          listInstruments(),
        ])
        const attemptResults = await Promise.allSettled(
          definitions.map(async (definition) => {
            const attempts = await fetchCollectorAttempts(definition.id, {
              limit: ATTEMPTS_LIMIT,
            })
            return { definition, attempts }
          }),
        )
        if (!mounted) return
        const failures = attemptResults.filter(
          (result) => result.status === 'rejected',
        )
        const withAttempts = attemptResults.map((result, index) => (
          result.status === 'fulfilled'
            ? { ...result.value, attemptsAvailable: true, attemptsError: null }
            : {
                definition: definitions[index],
                attempts: [],
                attemptsAvailable: false,
                attemptsError: result.reason?.message || 'Attempt history unavailable',
              }
        ))
        setCollectors(withAttempts)
        setInstruments(instrumentRows)
        setError(
          failures.length
            ? `Attempt history unavailable for ${failures.length} collector${failures.length === 1 ? '' : 's'}.`
            : null,
        )
        setObservedAt(new Date().toISOString())
      } catch (err) {
        if (mounted) setError(err?.message || 'Unable to load collectors')
      } finally {
        if (mounted) {
          setLoading(false)
          timeoutId = setTimeout(load, POLL_INTERVAL_MS)
        }
      }
    }

    load()
    return () => {
      mounted = false
      if (timeoutId) clearTimeout(timeoutId)
    }
  }, [refreshRevision])

  return { collectors, instruments, loading, error, observedAt, refresh }
}
