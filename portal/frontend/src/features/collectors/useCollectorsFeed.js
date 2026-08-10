import { useCallback, useEffect, useState } from 'react'
import {
  fetchCollectorOperationsSnapshot,
  fetchMarketDataPlaneSnapshot,
  openCollectorOperationsStream,
} from '../../adapters/marketData.adapter.js'

const ATTEMPTS_LIMIT = 5

/**
 * The backend owns lifecycle and health semantics. This hook only transports
 * complete canonical snapshots and preserves the last good snapshot while SSE
 * reconnects.
 */
export function useCollectorsFeed({ enabled = true } = {}) {
  const [snapshot, setSnapshot] = useState(null)
  const [dataPlane, setDataPlane] = useState(null)
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
    let planeIntervalId = null
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
        applySnapshot(await fetchCollectorOperationsSnapshot({ attemptLimit: ATTEMPTS_LIMIT }))
      } catch (err) {
        if (!mounted) return
        setError(err?.message || 'Unable to load collector operations')
        setLoading(false)
      }
    }

    async function loadPlane() {
      try {
        const next = await fetchMarketDataPlaneSnapshot()
        if (mounted) setDataPlane(next)
      } catch (err) {
        if (mounted) setError((current) => current || err?.message || 'Market-data-plane metrics unavailable')
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
        setStreamError(err?.message || 'Live collector snapshot was invalid')
      }
    }

    const source = openCollectorOperationsStream({ attemptLimit: ATTEMPTS_LIMIT })
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
        setStreamError('Live collector status is reconnecting; the last durable snapshot remains visible.')
      }
      fallbackTimerId = setTimeout(() => {
        if (!receivedStreamSnapshot) loadSnapshot()
      }, 4_000)
    } else {
      setStreamStatus('unavailable')
      setStreamError('Live collector status is unavailable; use Refresh for a bounded snapshot.')
      loadSnapshot()
    }

    loadPlane()
    planeIntervalId = setInterval(loadPlane, 30_000)
    if (refreshRevision > 0) {
      loadSnapshot()
      loadPlane()
    }

    return () => {
      mounted = false
      source?.close()
      if (fallbackTimerId) clearTimeout(fallbackTimerId)
      if (planeIntervalId) clearInterval(planeIntervalId)
    }
  }, [enabled, refreshRevision])

  return {
    snapshot,
    dataPlane,
    collectors: Array.isArray(snapshot?.collectors) ? snapshot.collectors : [],
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
