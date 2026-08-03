import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate, useParams } from 'react-router-dom'
import { fetchRun } from '../../adapters/bot.adapter.js'
import { fetchRunResearchEvidence } from '../../adapters/research.adapter.js'
import { BotLensRuntimeContainer } from '../../features/bots/botlens/BotLensRuntimeContainer.jsx'
import { assertRunInspectionScope, initialRunInspection, projectRunAsBot, safeRunLensOrigin } from '../../features/operations/runLensRouting.js'
import { OperatorErrorNotice, OperatorSkeleton } from '../components/OperatorErrorNotice.jsx'

function shortIdentity(value) {
  const text = String(value || '')
  return text.length > 20 ? text.slice(0, 10) + '…' + text.slice(-6) : text || 'Unavailable'
}

function RunEvidenceStrip({ inspection, researchEvidence, researchError, runError }) {
  const run = inspection?.run || {}
  const projection = run.projection || {}
  return (
    <div>
      <div className="qt2-run-evidence-strip">
      <span><small>Run state</small><strong>{run.runtime_status || run.status || 'Unavailable'}</strong></span>
      <span title={run.config_snapshot?.dataset_binding?.dataset_id || run.data_snapshot_hash || ''}><small>Dataset</small><strong className="qt-mono">{shortIdentity(run.config_snapshot?.dataset_binding?.dataset_id || run.data_snapshot_hash)}</strong></span>
      <span><small>BotLens evidence</small><strong>{projection.available ? (run.is_active ? 'Live projection' : 'Rebuildable') : projection.reason || 'Unavailable'}</strong></span>
      <span><small>Research result</small><strong>{researchError ? 'Unavailable' : researchEvidence ? (researchEvidence?.readiness?.comparison_status || researchEvidence?.readiness?.dataset_status || 'Evidence loaded') : 'Loading independently'}</strong></span>
      </div>
      {runError ? <OperatorErrorNotice error={runError} compact /> : null}
      {researchError ? <OperatorErrorNotice error={researchError} compact /> : null}
    </div>
  )
}

export function BotLensRoom() {
  const { runId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const [inspection, setInspection] = useState(() => initialRunInspection(location.state, runId))
  const [researchEvidence, setResearchEvidence] = useState(null)
  const [runError, setRunError] = useState(null)
  const [researchError, setResearchError] = useState(null)

  useEffect(() => {
    let mounted = true
    setInspection(initialRunInspection(location.state, runId))
    setResearchEvidence(null)
    setRunError(null)
    setResearchError(null)

    fetchRun(runId)
      .then((payload) => {
        if (!mounted) return
        setInspection(assertRunInspectionScope(payload, runId))
      })
      .catch((error) => {
        if (mounted) setRunError(error?.message || 'Unable to load authoritative run')
      })

    fetchRunResearchEvidence(runId)
      .then((payload) => {
        if (mounted) setResearchEvidence(payload)
      })
      .catch((error) => {
        if (mounted) setResearchError(error?.message || 'Research evidence unavailable')
      })

    return () => { mounted = false }
  }, [location.state, runId])

  const bot = useMemo(() => inspection ? projectRunAsBot(inspection) : null, [inspection])
  const from = safeRunLensOrigin(location.state?.from)
  const handleClose = () => navigate(from)

  if (!bot) {
    return (
      <div className="qt2-route-modal">
        <div className="qt2-route-modal-card">
          {runError ? <OperatorErrorNotice error={runError} /> : <OperatorSkeleton rows={6} label="Loading authoritative run evidence" />}
        </div>
      </div>
    )
  }

  const contextHeader = (
    <>
      <RunEvidenceStrip
        inspection={inspection}
        researchEvidence={researchEvidence}
        researchError={researchError}
        runError={runError}
      />
    </>
  )

  return (
    <BotLensRuntimeContainer
      bot={bot}
      runId={runId}
      open
      onClose={handleClose}
      variant="dialog"
      contextHeader={contextHeader}
    />
  )
}
