import { useCallback, useEffect, useMemo, useState } from 'react'
import { fetchRunInventory } from '../../adapters/bot.adapter.js'

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
    async function load() {
      setLoading(true)
      try {
        const payload = await fetchRunInventory({ limit: 100 })
        if (!mounted) return
        const definitionById = new Map(
          definitions.map((definition) => [String(definition?.id || ''), definition]),
        )
        const rows = Array.isArray(payload?.runs) ? payload.runs : []
        setRuns(rows.map((run) => ({
          ...run,
          definition: definitionById.get(String(run?.bot_id || '')) || null,
        })))
        setError(null)
        setObservedAt(payload?.observed_at || new Date().toISOString())
      } catch (loadError) {
        if (mounted) {
          setError(loadError?.message || 'Run inventory unavailable')
        }
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
