import { useCallback, useEffect, useState } from 'react'
import { useLocation, useNavigate, useParams } from 'react-router-dom'
import { fetchCollectorAttempts, fetchCollectorFactHistory, listCollectorDefinitions } from '../../adapters/marketData.adapter.js'
import { CollectorLensContent } from '../../features/collectors/CollectorLensContent.jsx'
import { OperatorErrorNotice, OperatorSkeleton } from '../components/OperatorErrorNotice.jsx'

const ATTEMPTS_LIMIT = 20

function safeOrigin(value) {
  if (value === '/overview') return value
  if (String(value || '').startsWith('/operations')) return value
  return '/operations?tab=market'
}

export function CollectorLensRoom() {
  const { definitionId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const [definition, setDefinition] = useState(null)
  const [attempts, setAttempts] = useState([])
  const [definitionError, setDefinitionError] = useState(null)
  const [attemptsError, setAttemptsError] = useState(null)
  const [factsError, setFactsError] = useState(null)
  const [factHistory, setFactHistory] = useState(null)
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    setDefinitionError(null)
    setAttemptsError(null)
    setFactsError(null)
    const [definitionResult, attemptsResult, factsResult] = await Promise.allSettled([
      listCollectorDefinitions({ definitionId }),
      fetchCollectorAttempts(definitionId, { limit: ATTEMPTS_LIMIT }),
      fetchCollectorFactHistory(definitionId, { hours: 24, limit: 240 }),
    ])
    if (definitionResult.status === 'fulfilled') {
      setDefinition(definitionResult.value[0] || null)
    } else {
      setDefinitionError(definitionResult.reason?.message || 'Unable to load market definition')
    }
    if (attemptsResult.status === 'fulfilled') {
      setAttempts(attemptsResult.value)
    } else {
      setAttemptsError(attemptsResult.reason?.message || 'Attempt evidence unavailable')
    }
    if (factsResult.status === 'fulfilled') {
      setFactHistory(factsResult.value)
    } else {
      setFactsError(factsResult.reason?.message || 'Fact history unavailable')
    }
    setLoading(false)
  }, [definitionId])

  useEffect(() => {
    load()
  }, [load])

  const from = safeOrigin(location.state?.from)
  const handleClose = () => navigate(from)

  if (loading && !definition) {
    return (
      <div className="qt2-route-modal"><div className="qt2-route-modal-card"><OperatorSkeleton rows={5} label="Loading Market Lens" /></div></div>
    )
  }

  if (definitionError && !definition) {
    return (
      <div className="qt2-route-modal"><div className="qt2-route-modal-card"><OperatorErrorNotice error={definitionError} /></div></div>
    )
  }

  if (!definition) {
    return (
      <div className="qt2-room">
        <div className="qt2-empty">Collector not found.</div>
      </div>
    )
  }

  return (
    <div className="qt2-route-modal qt2-lens-backdrop">
      <div className="qt2-market-lens-dialog qt2-lens-dialog qt-ops-shell flex w-full flex-col overflow-hidden">
      <CollectorLensContent definition={definition} attempts={attempts} factHistory={factHistory} attemptsError={attemptsError} factsError={factsError} onClose={handleClose} onRefresh={load} />
      </div>
    </div>
  )
}
