import { useCallback, useEffect, useState } from 'react'
import {
  fetchActiveRuns,
  openActiveRunsStream,
} from '../../adapters/bot.adapter.js'

const TERMINAL_STATUSES = new Set([
  'canceled',
  'cancelled',
  'completed',
  'crashed',
  'degraded_terminal',
  'failed',
  'startup_failed',
  'stopped',
])

function payloadRuns(payload) {
  if (Array.isArray(payload)) return payload
  return Array.isArray(payload?.runs) ? payload.runs : []
}

function mergeRuntime(rows, payload) {
  const runId = String(payload?.run_id || '').trim()
  const runtime = payload?.runtime && typeof payload.runtime === 'object'
    ? payload.runtime
    : null
  if (!runId || !runtime) return rows
  const status = String(runtime.status || '').trim().toLowerCase()
  if (TERMINAL_STATUSES.has(status)) {
    return rows.filter((row) => String(row?.run_id || '') !== runId)
  }
  return rows.map((row) => {
    if (String(row?.run_id || '') !== runId) return row
    const knownAt = runtime.last_event_at
      || runtime.last_useful_progress_at
      || row.known_at
      || null
    return {
      ...row,
      runtime: { ...(row.runtime || {}), ...runtime },
      runtime_status: status || row.runtime_status,
      progress: runtime.progress ?? row.progress ?? null,
      progress_unit: runtime.progress == null ? row.progress_unit : 'fraction',
      known_at: knownAt,
      liveness: {
        ...(row.liveness || {}),
        state: 'alive',
        last_update_at: knownAt,
      },
    }
  })
}

export function useActiveRunsFeed({ enabled = true } = {}) {
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [streamState, setStreamState] = useState('connecting')
  const [hasReceivedSnapshot, setHasReceivedSnapshot] = useState(false)
  const [refreshRevision, setRefreshRevision] = useState(0)
  const refresh = useCallback(() => setRefreshRevision((value) => value + 1), [])

  const loadSnapshot = useCallback(async () => {
    const payload = await fetchActiveRuns()
    setRuns(payloadRuns(payload))
    setError(null)
    setLoading(false)
  }, [])

  useEffect(() => {
    if (!enabled) {
      setStreamState('idle')
      setHasReceivedSnapshot(false)
      setLoading(false)
      return undefined
    }
    setLoading(true)
    setError(null)
    setHasReceivedSnapshot(false)
    const source = openActiveRunsStream()
    if (!source) {
      setStreamState('error')
      return undefined
    }
    const handlePayload = (event) => {
      try {
        const payload = JSON.parse(event.data)
        if (event.type === 'snapshot') {
          setRuns(payloadRuns(payload))
          setHasReceivedSnapshot(true)
          setLoading(false)
          setError(null)
        } else if (event.type === 'run_runtime') {
          setRuns((current) => mergeRuntime(current, payload))
        } else if (event.type === 'active_runs_changed') {
          loadSnapshot().catch((nextError) => {
            setError(nextError?.message || 'Active runs unavailable')
          })
        }
        setStreamState('open')
      } catch (nextError) {
        setError(nextError?.message || 'Active run update was invalid')
      }
    }
    source.addEventListener('snapshot', handlePayload)
    source.addEventListener('run_runtime', handlePayload)
    source.addEventListener('active_runs_changed', handlePayload)
    source.onopen = () => setStreamState('open')
    source.onerror = () => setStreamState('error')
    return () => source.close()
  }, [enabled, loadSnapshot])

  useEffect(() => {
    if (!enabled) return undefined
    if (hasReceivedSnapshot) return undefined
    const timer = setTimeout(() => {
      loadSnapshot().catch((nextError) => {
        setError(nextError?.message || 'Active runs unavailable')
        setLoading(false)
      })
    }, streamState === 'error' ? 0 : 4000)
    return () => clearTimeout(timer)
  }, [enabled, hasReceivedSnapshot, loadSnapshot, streamState])

  useEffect(() => {
    if (!enabled) return
    if (refreshRevision === 0) return
    setLoading(true)
    loadSnapshot().catch((nextError) => {
      setError(nextError?.message || 'Active runs unavailable')
      setLoading(false)
    })
  }, [enabled, loadSnapshot, refreshRevision])

  return {
    runs,
    loading,
    error,
    streamState,
    hasReceivedSnapshot,
    refresh,
  }
}
