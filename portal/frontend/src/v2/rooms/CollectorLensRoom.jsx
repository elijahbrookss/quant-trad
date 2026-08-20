import { useCallback, useEffect, useState } from 'react'
import { useLocation, useNavigate, useParams } from 'react-router-dom'
import {
  executeCollectorAction,
  fetchCollectorDiagnostics,
  fetchCollectorEvents,
  fetchCollectorGaps,
  fetchCollectorOperationsDetail,
} from '../../adapters/marketData.adapter.js'
import { CollectorLensContent } from '../../features/collectors/CollectorLensContent.jsx'
import { OperatorErrorNotice, OperatorSkeleton } from '../components/OperatorErrorNotice.jsx'

const DETAIL_LIMIT = 100

function safeOrigin(value) {
  if (value === '/overview') return value
  if (String(value || '').startsWith('/operations')) return value
  return '/operations?tab=market'
}

export function CollectorLensRoom() {
  const { collectorKind, collectorId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const [detail, setDetail] = useState(null)
  const [diagnostics, setDiagnostics] = useState(null)
  const [events, setEvents] = useState(null)
  const [gaps, setGaps] = useState(null)
  const [detailError, setDetailError] = useState(null)
  const [diagnosticsError, setDiagnosticsError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [actionBusy, setActionBusy] = useState(false)
  const [actionError, setActionError] = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    setDetailError(null)
    const results = await Promise.allSettled([
      fetchCollectorOperationsDetail(collectorKind, collectorId, { limit: DETAIL_LIMIT }),
      fetchCollectorDiagnostics(collectorKind, collectorId),
      fetchCollectorEvents(collectorKind, collectorId, { limit: DETAIL_LIMIT }),
      fetchCollectorGaps(collectorKind, collectorId, { limit: DETAIL_LIMIT }),
    ])
    const [detailResult, diagnosticsResult, eventsResult, gapsResult] = results
    if (detailResult.status === 'fulfilled') setDetail(detailResult.value)
    else setDetailError(detailResult.reason?.message || 'Unable to load collector operations')
    if (diagnosticsResult.status === 'fulfilled') {
      setDiagnostics(diagnosticsResult.value)
      setDiagnosticsError(null)
    } else setDiagnosticsError(diagnosticsResult.reason?.message || 'Collector diagnostics unavailable')
    if (eventsResult.status === 'fulfilled') setEvents(eventsResult.value)
    if (gapsResult.status === 'fulfilled') setGaps(gapsResult.value)
    setLoading(false)
  }, [collectorId, collectorKind])

  const runDiagnostics = useCallback(async () => {
    setDiagnosticsError(null)
    try {
      setDiagnostics(await fetchCollectorDiagnostics(collectorKind, collectorId))
    } catch (error) {
      setDiagnosticsError(error?.message || 'Collector diagnostics unavailable')
    }
  }, [collectorId, collectorKind])

  const runAction = useCallback(async (action, reason) => {
    setActionBusy(true)
    setActionError(null)
    try {
      await executeCollectorAction(collectorKind, collectorId, action, {
        reason,
        confirmation: `${collectorKind}:${collectorId}:${action}`,
      })
      await load()
      return true
    } catch (error) {
      setActionError(error?.message || `Collector ${action} failed`)
      return false
    } finally {
      setActionBusy(false)
    }
  }, [collectorId, collectorKind, load])

  useEffect(() => { load() }, [load])

  const from = safeOrigin(location.state?.from)
  const handleClose = () => navigate(from)

  if (loading && !detail) return <div className="qt2-route-modal"><div className="qt2-route-modal-card"><OperatorSkeleton rows={7} label="Loading collector operations" /></div></div>
  if (detailError && !detail) return <div className="qt2-route-modal"><div className="qt2-route-modal-card"><OperatorErrorNotice error={detailError} /></div></div>
  if (!detail) return <div className="qt2-room"><div className="qt2-empty">Collector not found.</div></div>

  return <div className="qt2-route-modal qt2-lens-backdrop"><div className="qt2-market-lens-dialog qt2-lens-dialog qt-ops-shell flex w-full flex-col overflow-hidden"><CollectorLensContent detail={detail} diagnostics={diagnostics} events={events} gaps={gaps} diagnosticsError={diagnosticsError} actionError={actionError} actionBusy={actionBusy} onClose={handleClose} onRefresh={load} onRunDiagnostics={runDiagnostics} onAction={runAction} /></div></div>
}
