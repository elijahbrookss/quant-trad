import { useCallback, useEffect, useMemo, useState } from 'react'
import { fetchRunInventory } from '../../adapters/bot.adapter.js'

export function useRunInventory(definitions = [], { enabled = true } = {}) {
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [nextCursor, setNextCursor] = useState(null)
  const [loadingMore, setLoadingMore] = useState(false)
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
    if (!enabled) {
      setLoading(false)
      return undefined
    }
    let mounted = true
    async function load() {
      setLoading(true)
      try {
        const payload = await fetchRunInventory({ limit: 50 })
        if (!mounted) return
        const definitionById = new Map(
          definitions.map((definition) => [String(definition?.id || ''), definition]),
        )
        const rows = Array.isArray(payload?.runs) ? payload.runs : []
        setRuns(rows.map((run) => ({
          ...run,
          definition: definitionById.get(String(run?.bot_id || '')) || null,
        })))
        setNextCursor(payload?.next_cursor || null)
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
  }, [definitions, enabled, scopeKey, refreshRevision])

  const loadMore = useCallback(async () => {
    if (!nextCursor || loadingMore || !enabled) return
    setLoadingMore(true)
    try {
      const payload = await fetchRunInventory({
        limit: 50,
        beforeSortAt: nextCursor.before_sort_at,
        beforeRunId: nextCursor.before_run_id,
      })
      const definitionById = new Map(
        definitions.map((definition) => [String(definition?.id || ''), definition]),
      )
      const incoming = (Array.isArray(payload?.runs) ? payload.runs : []).map((run) => ({
        ...run,
        definition: definitionById.get(String(run?.bot_id || '')) || null,
      }))
      setRuns((current) => {
        const byId = new Map(current.map((run) => [run.run_id, run]))
        incoming.forEach((run) => byId.set(run.run_id, run))
        return [...byId.values()]
      })
      setNextCursor(payload?.next_cursor || null)
      setError(null)
      setObservedAt(payload?.observed_at || new Date().toISOString())
    } catch (loadError) {
      setError(loadError?.message || 'Older run history unavailable')
    } finally {
      setLoadingMore(false)
    }
  }, [definitions, enabled, loadingMore, nextCursor])

  return {
    runs,
    loading,
    loadingMore,
    hasMore: Boolean(nextCursor),
    error,
    observedAt,
    refresh,
    loadMore,
  }
}
