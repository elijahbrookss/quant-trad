import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate, useParams } from 'react-router-dom'
import { fetchRun } from '../../adapters/bot.adapter.js'
import { fetchRunResearchEvidence } from '../../adapters/research.adapter.js'
import { BotLensRuntimeContainer } from '../../features/bots/botlens/BotLensRuntimeContainer.jsx'
import { assertRunInspectionScope, initialRunInspection, projectRunAsBot, safeRunLensOrigin } from '../../features/operations/runLensRouting.js'

function RunEvidenceStrip({ inspection, researchEvidence, refreshError }) {
  const run = inspection?.run || {}
  const report = run.report_materialization || {}
  const dataQuality = researchEvidence?.data_quality || {}
  return (
    <div className="qt2-run-evidence-strip">
      <span><small>Run identity</small><strong className="qt-mono">{run.run_id}</strong></span>
      <span><small>Lifecycle</small><strong>{run.runtime_status || run.status || 'Unavailable'}</strong></span>
      <span><small>Dataset input</small><strong className="qt-mono">{run.data_snapshot_hash || 'Unavailable'}</strong></span>
      <span><small>Research dataset</small><strong>{researchEvidence?.readiness?.dataset_status || 'Unavailable'}</strong></span>
      <span><small>Comparable</small><strong>{researchEvidence ? (researchEvidence.readiness?.safe_to_compare ? 'Yes' : 'No') : 'Unavailable'}</strong></span>
      <span><small>Report artifact</small><strong>{report.status || 'Unavailable'}</strong></span>
      <span><small>Quality</small><strong>{dataQuality.status || 'Unavailable'}</strong></span>
      <span><small>Observed</small><strong>{inspection?.observed_at ? new Date(inspection.observed_at).toLocaleString() : 'Navigation hint'}</strong></span>
      {refreshError ? <span className="is-error"><small>Refresh</small><strong>{refreshError}</strong></span> : null}
    </div>
  )
}

export function BotLensRoom() {
  const { runId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const [inspection, setInspection] = useState(() => initialRunInspection(location.state, runId))
  const [researchEvidence, setResearchEvidence] = useState(null)
  const [loadError, setLoadError] = useState(null)

  useEffect(() => {
    let mounted = true
    setLoadError(null)
    Promise.allSettled([fetchRun(runId), fetchRunResearchEvidence(runId)])
      .then(([inspectionResult, evidenceResult]) => {
        if (!mounted) return
        const errors = []
        if (inspectionResult.status === 'fulfilled') {
          try {
            setInspection(assertRunInspectionScope(inspectionResult.value, runId))
          } catch (error) {
            errors.push(error.message)
          }
        } else {
          errors.push(inspectionResult.reason?.message || 'Unable to load authoritative run')
        }
        if (evidenceResult.status === 'fulfilled') setResearchEvidence(evidenceResult.value)
        else errors.push(evidenceResult.reason?.message || 'Research evidence unavailable')
        setLoadError(errors.join(' · ') || null)
      })
    return () => { mounted = false }
  }, [runId])

  const bot = useMemo(() => inspection ? projectRunAsBot(inspection) : null, [inspection])
  const from = safeRunLensOrigin(location.state?.from)
  const handleClose = () => navigate(from)

  if (!bot) {
    return (
      <div className="qt2-room">
        {loadError ? <div className="qt2-error">{loadError}</div> : <div className="qt2-empty">Loading authoritative run evidence…</div>}
      </div>
    )
  }

  return (
    <div className="qt2-botlens-page">
      <RunEvidenceStrip inspection={inspection} researchEvidence={researchEvidence} refreshError={loadError} />
      <BotLensRuntimeContainer bot={bot} runId={runId} open onClose={handleClose} variant="page" />
    </div>
  )
}
