import { useCallback, useEffect, useMemo, useState } from 'react'
import { fetchBotRuns } from '../../adapters/bot.adapter.js'

const RUNS_PER_DEFINITION = 50

export function useRunInventory(definitions = []) {
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)

  const scopeKey = useMemo(
    () => definitions
      .map((definition) => [
        definition?.id,
        definition?.active_run_id,
        definition?.latest_run_id,
      ].join(':'))
      .sort()
      .join('|'),
    [definitions],
  )
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    let mounted = true
    if (!definitions.length) {
      setRuns([])
      setLoading(false)
      setError(null)
      setObservedAt(new Date().toISOString())
      return undefined
    }

    async function load() {
      setLoading(true)
      try {
        const results = await Promise.allSettled(
          definitions.map(async (definition) => ({
            definition,
            payload: await fetchBotRuns(definition.id, {
              limit: RUNS_PER_DEFINITION,
            }),
          })),
        )
        if (!mounted) return
        const failures = results.filter((result) => result.status === 'rejected')
        const nextRuns = results
          .filter((result) => result.status === 'fulfilled')
          .flatMap((result) => {
            const definition = result.value.definition
            const rows = Array.isArray(result.value.payload?.runs)
              ? result.value.payload.runs
              : []
            return rows.map((run) => ({ ...run, definition }))
          })
        setRuns(nextRuns)
        setError(
          failures.length
            ? `Run history unavailable for ${failures.length} definition${failures.length === 1 ? '' : 's'}.`
            : null,
        )
        setObservedAt(new Date().toISOString())
      } finally {
        if (mounted) setLoading(false)
      }
    }

    load()
    return () => {
      mounted = false
    }
  }, [definitions, scopeKey, refreshRevision])

  return { runs, loading, error, observedAt, refresh }
}
