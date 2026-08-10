import { useCallback, useEffect, useState } from 'react'
import {
  fetchCollectorProviderSummary,
  fetchProviderCollectors,
  openCollectorProviderSummaryStream,
  searchCollectors,
} from '../../adapters/marketData.adapter.js'

const PAGE_REFRESH_MS = 15_000

/**
 * One lightweight provider-level stream drives fleet awareness. Collector
 * telemetry is fetched only for the provider or search page the operator opens.
 */
export function useCollectorsFeed({ enabled = true } = {}) {
  const [snapshot, setSnapshot] = useState(null)
  const [streamStatus, setStreamStatus] = useState('connecting')
  const [streamError, setStreamError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      return undefined
    }

    let mounted = true
    let fallbackTimerId = null
    let receivedStreamSnapshot = false
    setLoading(true)

    function applySnapshot(next) {
      if (!mounted || !next) return
      setSnapshot(next)
      setError(null)
      setLoading(false)
    }

    async function loadSnapshot() {
      try {
        applySnapshot(await fetchCollectorProviderSummary())
      } catch (err) {
        if (!mounted) return
        setError(err?.message || 'Unable to load collector provider summary')
        setLoading(false)
      }
    }

    function onStreamSnapshot(event) {
      try {
        receivedStreamSnapshot = true
        applySnapshot(JSON.parse(event.data))
        setStreamStatus('connected')
        setStreamError(null)
      } catch (err) {
        setStreamStatus('invalid')
        setStreamError(err?.message || 'Live provider summary was invalid')
      }
    }

    const source = openCollectorProviderSummaryStream()
    if (source) {
      source.addEventListener('snapshot', onStreamSnapshot)
      source.addEventListener('delta', onStreamSnapshot)
      source.onopen = () => {
        if (!mounted) return
        setStreamStatus('connected')
        setStreamError(null)
      }
      source.onerror = () => {
        if (!mounted) return
        setStreamStatus('reconnecting')
        setStreamError('Provider summaries are reconnecting; the last good view remains visible.')
      }
      fallbackTimerId = setTimeout(() => {
        if (!receivedStreamSnapshot) loadSnapshot()
      }, 4_000)
    } else {
      setStreamStatus('unavailable')
      setStreamError('Live provider summaries are unavailable; use Refresh for a bounded snapshot.')
      loadSnapshot()
    }

    if (refreshRevision > 0) loadSnapshot()

    return () => {
      mounted = false
      source?.close()
      if (fallbackTimerId) clearTimeout(fallbackTimerId)
    }
  }, [enabled, refreshRevision])

  return {
    snapshot,
    providers: Array.isArray(snapshot?.providers) ? snapshot.providers : [],
    fleet: snapshot?.fleet || null,
    workerFleet: snapshot?.worker_fleet || null,
    streamStatus,
    streamError,
    loading,
    error,
    observedAt: snapshot?.observed_at || null,
    refresh,
  }
}

export function useCollectorPage({
  provider = null,
  query = '',
  attentionOnly = false,
  enabled = true,
  offset = 0,
  limit = 50,
} = {}) {
  const [page, setPage] = useState(null)
  const [loading, setLoading] = useState(Boolean(enabled))
  const [error, setError] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      return undefined
    }

    let mounted = true
    let intervalId = null

    async function load({ quiet = false } = {}) {
      if (!quiet) setLoading(true)
      try {
        const next = provider
          ? await fetchProviderCollectors(provider, {
              query,
              attentionOnly,
              offset,
              limit,
            })
          : await searchCollectors({
              query,
              attentionOnly,
              offset,
              limit,
            })
        if (mounted) {
          setPage(next)
          setError(null)
        }
      } catch (err) {
        if (mounted) setError(err?.message || 'Collector page unavailable')
      } finally {
        if (mounted && !quiet) setLoading(false)
      }
    }

    load()
    intervalId = setInterval(() => {
      if (document.visibilityState === 'visible') load({ quiet: true })
    }, PAGE_REFRESH_MS)

    return () => {
      mounted = false
      if (intervalId) clearInterval(intervalId)
    }
  }, [attentionOnly, enabled, limit, offset, provider, query, refreshRevision])

  return {
    page,
    collectors: Array.isArray(page?.collectors) ? page.collectors : [],
    total: Number(page?.total || 0),
    loading,
    error,
    refresh,
  }
}
