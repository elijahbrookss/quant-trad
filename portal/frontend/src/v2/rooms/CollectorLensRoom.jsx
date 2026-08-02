import { useCallback, useEffect, useState } from 'react'
import { useLocation, useNavigate, useParams } from 'react-router-dom'
import { listCollectorDefinitions, fetchCollectorAttempts } from '../../adapters/marketData.adapter.js'
import { CollectorLensContent } from '../../features/collectors/CollectorLensContent.jsx'

const ATTEMPTS_LIMIT = 20

function safeOrigin(value) {
  if (value === '/overview') return value
  if (String(value || '').startsWith('/operations')) return value
  return '/operations?tab=data-plane'
}

export function CollectorLensRoom() {
  const { definitionId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const [definition, setDefinition] = useState(null)
  const [attempts, setAttempts] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [definitions, attemptRows] = await Promise.all([
        listCollectorDefinitions({ definitionId }),
        fetchCollectorAttempts(definitionId, { limit: ATTEMPTS_LIMIT }),
      ])
      setDefinition(definitions[0] || null)
      setAttempts(attemptRows)
    } catch (err) {
      setError(err?.message || 'Unable to load collector')
    } finally {
      setLoading(false)
    }
  }, [definitionId])

  useEffect(() => {
    load()
  }, [load])

  const from = safeOrigin(location.state?.from)
  const handleClose = () => navigate(from)

  if (loading && !definition) {
    return (
      <div className="qt2-room">
        <div className="qt2-empty">Loading…</div>
      </div>
    )
  }

  if (error && !definition) {
    return (
      <div className="qt2-room">
        <div className="qt2-error">{error}</div>
      </div>
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
    <div className="qt2-lens-shell qt-ops-shell qt-botlens-shell flex w-full flex-col overflow-hidden">
      <CollectorLensContent definition={definition} attempts={attempts} onClose={handleClose} onRefresh={load} />
    </div>
  )
}
