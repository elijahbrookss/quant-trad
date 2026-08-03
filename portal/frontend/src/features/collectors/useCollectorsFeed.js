import { useCallback, useEffect, useState } from 'react'
import { fetchCollectorSnapshot, openCollectorsStream } from '../../adapters/marketData.adapter.js'

const ATTEMPTS_LIMIT = 5

/**
 * One bounded persisted snapshot hydrates the collector inventory. The SSE
 * projection replaces it atomically as liveness, attempts, or schedules change.
 * A failed live channel never discards the last persisted snapshot.
 */
export function useCollectorsFeed({ enabled = true } = {}) {
  const [collectors, setCollectors] = useState([])
  const [instruments, setInstruments] = useState([])
  const [workers, setWorkers] = useState([])
  const [workerHealth, setWorkerHealth] = useState({ status: 'unknown' })
  const [streamStatus, setStreamStatus] = useState('connecting')
  const [streamError, setStreamError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [observedAt, setObservedAt] = useState(null)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      return undefined
    }
    setLoading(true)
    let mounted = true
    let fallbackIntervalId = null
    const source = openCollectorsStream({ attemptLimit: ATTEMPTS_LIMIT })

    function applySnapshot(snapshot) {
      if (!mounted || !snapshot) return
      const rows = Array.isArray(snapshot.collectors)
        ? snapshot.collectors.map((entry) => ({
            definition: {
              ...entry.definition,
              worker_health: snapshot.worker_health || { status: "unknown" },
            },
            attempts: Array.isArray(entry.attempts) ? entry.attempts : [],
            attemptsAvailable: entry.attempts_available !== false,
            attemptsError: entry.attempts_error || null,
          }))
        : []
      const instrumentMap = new Map()
      rows.forEach(({ definition }) => {
        if (!definition?.instrument_id) return
        instrumentMap.set(String(definition.instrument_id), {
          id: definition.instrument_id,
          symbol: definition.instrument_symbol || definition.instrument_id,
          instrument_type: definition.instrument_type || null,
          datasource: definition.provider || null,
          exchange: definition.venue || null,
        })
      })
      setCollectors(rows)
      setInstruments([...instrumentMap.values()])
      setWorkers(Array.isArray(snapshot.workers) ? snapshot.workers : [])
      setWorkerHealth(snapshot.worker_health || { status: "unknown" })
      setObservedAt(snapshot.observed_at || new Date().toISOString())
      setError(null)
      setLoading(false)
    }

    async function loadSnapshot() {
      try {
        const snapshot = await fetchCollectorSnapshot({ attemptLimit: ATTEMPTS_LIMIT })
        applySnapshot(snapshot)
      } catch (err) {
        if (!mounted) return
        setError(err?.message || "Unable to load market collection status")
        setLoading(false)
      }
    }

    function onStreamSnapshot(event) {
      try {
        applySnapshot(JSON.parse(event.data))
        setStreamStatus("connected")
        setStreamError(null)
      } catch (err) {
        setStreamStatus("invalid")
        setStreamError(err?.message || "Live market status update was invalid")
      }
    }

    loadSnapshot()
    if (source) {
      source.addEventListener("snapshot", onStreamSnapshot)
      source.addEventListener("delta", onStreamSnapshot)
      source.onopen = () => {
        if (!mounted) return
        setStreamStatus("connected")
        setStreamError(null)
      }
      source.onerror = () => {
        if (!mounted) return
        setStreamStatus("reconnecting")
        setStreamError("Live market status is reconnecting; persisted snapshot remains visible.")
      }
    } else {
      setStreamStatus("unavailable")
      setStreamError("Live market status is unavailable; using bounded snapshot refresh.")
      fallbackIntervalId = setInterval(loadSnapshot, 30_000)
    }

    return () => {
      mounted = false
      source?.close()
      if (fallbackIntervalId) clearInterval(fallbackIntervalId)
    }
  }, [enabled, refreshRevision])

  return {
    collectors,
    instruments,
    workers,
    workerHealth,
    streamStatus,
    streamError,
    loading,
    error,
    observedAt,
    refresh,
  }
}
