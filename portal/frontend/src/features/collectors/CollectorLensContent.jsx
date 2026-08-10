import { useMemo, useState } from 'react'
import { AlertTriangle, CheckCircle2, Pause, Play, RefreshCcw, RotateCcw, Square, Stethoscope, X } from 'lucide-react'
import { OperatorErrorNotice } from '../../v2/components/OperatorErrorNotice.jsx'
import { COLLECTOR_STATE_COPY } from './buildCollectorCardViewModel.js'

const TABS = [
  ['runtime', 'Runtime'],
  ['activity', 'Activity'],
  ['facts', 'Facts'],
  ['quality', 'Data quality'],
  ['diagnostics', 'Diagnostics'],
  ['configuration', 'Configuration'],
]

const ACTIONS = {
  start: { label: 'Start', Icon: Play },
  stop: { label: 'Stop', Icon: Square, disruptive: true },
  restart: { label: 'Restart', Icon: RotateCcw, disruptive: true },
  pause: { label: 'Pause', Icon: Pause },
  resume: { label: 'Resume', Icon: Play },
}

function formatTime(value) {
  if (!value) return 'Unavailable'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString()
}

function formatDuration(value) {
  const seconds = Number(value)
  if (!Number.isFinite(seconds)) return 'Unavailable'
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`
  return `${Math.floor(seconds / 3_600)}h ${Math.floor((seconds % 3_600) / 60)}m`
}

function formatCount(value) {
  return Number.isFinite(Number(value)) ? Number(value).toLocaleString() : 'Unavailable'
}

function tone(value) {
  const normalized = String(value || '').toUpperCase()
  if (['HEALTHY', 'PASS', 'SUCCEEDED'].includes(normalized)) return 'success'
  if (['FAILED', 'FAIL'].includes(normalized)) return 'danger'
  if (['DEGRADED', 'RETRYING', 'RECOVERING', 'WARNING'].includes(normalized)) return 'warning'
  if (['STARTING'].includes(normalized)) return 'info'
  return 'neutral'
}

function Badge({ value }) {
  return <span className={'qt2-evidence-state is-' + tone(value)}>{value || 'UNKNOWN'}</span>
}

function Panel({ title, aside, children, wide = false }) {
  return <section className={'qt2-collector-panel' + (wide ? ' is-wide' : '')}><header><h2>{title}</h2>{aside}</header><div>{children}</div></section>
}

function Readout({ label, value, mono = false }) {
  return <div className="qt2-collector-readout"><dt>{label}</dt><dd className={mono ? 'qt-mono' : ''}>{value ?? 'Unavailable'}</dd></div>
}

function JsonBlock({ value }) {
  return <pre className="qt2-json-block">{JSON.stringify(value ?? null, null, 2)}</pre>
}

function Empty({ children }) {
  return <div className="qt2-empty">{children}</div>
}

function ActionDialog({ collector, action, busy, error, onCancel, onConfirm }) {
  const spec = ACTIONS[action]
  const [reason, setReason] = useState('')
  if (!spec) return null
  return <div className="qt2-action-backdrop" role="presentation"><section className="qt2-action-dialog" role="dialog" aria-modal="true" aria-labelledby="collector-action-title">
    <header><div><span>Audited collector action</span><h2 id="collector-action-title">{spec.label} {collector.collector_id}</h2></div><button type="button" onClick={onCancel} aria-label="Cancel action"><X size={17} /></button></header>
    {spec.disruptive ? <p className="qt2-action-warning"><AlertTriangle size={17} />This action interrupts acquisition. The worker will drain or restart through the canonical lifecycle path.</p> : <p>The desired state will change through the canonical backend command path.</p>}
    <label>Operator reason<textarea value={reason} onChange={(event) => setReason(event.target.value)} placeholder="Why is this action being taken?" /></label>
    <dl><Readout label="Collector" value={`${collector.collector_kind}:${collector.collector_id}`} mono /><Readout label="Prior actual state" value={collector.actual_state} /><Readout label="Prior desired state" value={collector.desired_state} /></dl>
    {error ? <OperatorErrorNotice error={error} compact /> : null}
    <footer><button type="button" className="qt2-button" onClick={onCancel} disabled={busy}>Cancel</button><button type="button" className={'qt2-button ' + (spec.disruptive ? 'qt2-button-danger' : 'qt2-button-primary')} onClick={() => onConfirm(reason)} disabled={busy || !reason.trim()}>{busy ? 'Recording action…' : `Confirm ${spec.label.toLowerCase()}`}</button></footer>
  </section></div>
}

function RuntimeTab({ collector }) {
  return <div className="qt2-collector-panel-grid">
    <Panel title="Lifecycle"><dl><Readout label="Configured state" value={collector.configured_state} /><Readout label="Desired state" value={collector.desired_state} /><Readout label="Actual state" value={collector.actual_state} /><Readout label="Control generation" value={collector.control_generation} /><Readout label="Active" value={collector.runtime?.active ? 'Yes' : 'No'} /><Readout label="Restart count" value={collector.runtime?.restart_count} /></dl></Panel>
    <Panel title="Worker"><dl><Readout label="Identity" value={collector.worker?.identity} mono /><Readout label="State" value={collector.worker?.state} /><Readout label="Heartbeat" value={formatTime(collector.worker?.heartbeat_at)} /><Readout label="Heartbeat age" value={formatDuration(collector.worker?.heartbeat_age_seconds)} /><Readout label="Uptime" value={formatDuration(collector.worker?.uptime_seconds)} /><Readout label="Lease owner" value={collector.runtime?.lease_owner} mono /></dl></Panel>
    <Panel title="Acquisition"><dl><Readout label="Provider" value={`${collector.provider} · ${collector.venue}`} /><Readout label="Trigger" value={collector.acquisition?.trigger || (collector.acquisition?.cadence_seconds ? `every ${collector.acquisition.cadence_seconds}s` : null)} /><Readout label="Last provider success" value={formatTime(collector.acquisition?.last_provider_success_at)} /><Readout label="Last accepted fact" value={formatTime(collector.acquisition?.last_accepted_fact_at)} /><Readout label="Last observation" value={formatTime(collector.acquisition?.last_observation_time)} /><Readout label="Freshness" value={formatDuration(collector.acquisition?.freshness_seconds)} /></dl></Panel>
    <Panel title="Delivery"><dl><Readout label="Accepted · 1 minute" value={formatCount(collector.throughput?.accepted_last_minute)} /><Readout label="Accepted · 5 minutes" value={formatCount(collector.throughput?.accepted_last_five_minutes)} /><Readout label="Recent rejects" value={formatCount(collector.throughput?.rejected_recent)} /><Readout label="Retry active" value={collector.retry?.active ? 'Yes' : 'No'} /><Readout label="Consecutive failures" value={formatCount(collector.retry?.consecutive_failures)} /><Readout label="Active error" value={collector.error?.message || 'None'} /></dl></Panel>
  </div>
}

function ActivityTab({ events, operations }) {
  const rows = useMemo(() => [
    ...(events?.events || []),
  ], [events])
  return <div className="qt2-collector-panel-grid"><Panel title="Collector event timeline" aside={<span>{rows.length} events</span>} wide>{rows.length ? <ol className="qt2-event-timeline">{rows.map((event, index) => <li key={`${event.occurred_at}:${event.event_type}:${index}`}><span className={'is-' + tone(event.status)} /><div><strong>{event.event_type}</strong><small>{formatTime(event.occurred_at)}</small><p>{event.status || 'Evidence recorded'}</p></div><details><summary>Evidence</summary><JsonBlock value={event.evidence} /></details></li>)}</ol> : <Empty>No runtime or quality events are recorded.</Empty>}</Panel><Panel title="Operator audit history" aside={<span>{operations.length} actions</span>} wide>{operations.length ? <div className="qt2-audit-list">{operations.map((operation) => <article key={operation.id || operation.request_id}><span><Badge value={operation.status} /><strong>{operation.action}</strong></span><small>{formatTime(operation.requested_at)} · {operation.actor_id || 'actor unavailable'}</small><JsonBlock value={operation} /></article>)}</div> : <Empty>No operator mutations have been recorded.</Empty>}</Panel></div>
}

function FactsTab({ facts }) {
  return <Panel title="Recent canonical Facts" aside={<span>{facts.length} facts</span>} wide>{facts.length ? <div className="qt2-fact-records">{facts.map((fact) => <article key={fact.id || `${fact.series_id}:${fact.observation_key}:${fact.revision}`}><header><span><strong>{fact.fact_type}</strong><small>{fact.payload_schema_id}</small></span><Badge value={`r${fact.revision}`} /></header><dl><Readout label="Subject" value={fact.instrument_id} /><Readout label="Observation time" value={formatTime(fact.observation_time)} /><Readout label="Known at" value={formatTime(fact.known_at)} /><Readout label="Accepted at" value={formatTime(fact.accepted_at)} /><Readout label="Provider" value={`${fact.provider} · ${fact.venue}`} /><Readout label="Transformation" value={fact.transformation_id} mono /></dl><details open><summary>Typed payload</summary><JsonBlock value={fact.payload} /></details><details><summary>Provenance</summary><JsonBlock value={fact.provenance} /></details><details><summary>Quality</summary><JsonBlock value={fact.quality} /></details></article>)}</div> : <Empty>No canonical facts are visible in this bounded read.</Empty>}</Panel>
}

function QualityTab({ gaps, qualityEvents }) {
  return <div className="qt2-collector-panel-grid"><Panel title="Gap evidence" aside={<span>{gaps.length} records</span>} wide>{gaps.length ? <div className="qt2-evidence-records">{gaps.map((gap, index) => <article key={gap.id || index}><span><strong>{gap.gap_type || gap.reason || 'Gap evidence'}</strong><small>{formatTime(gap.created_at)}</small></span><JsonBlock value={gap} /></article>)}</div> : <Empty>No recent gap evidence is recorded.</Empty>}</Panel><Panel title="Quality and schema events" aside={<span>{qualityEvents.length} records</span>} wide>{qualityEvents.length ? <div className="qt2-evidence-records">{qualityEvents.map((event, index) => <article key={event.id || index}><span><strong>{event.classification || 'Quality event'}</strong><small>{formatTime(event.detected_at)}</small></span><JsonBlock value={event} /></article>)}</div> : <Empty>No recent malformed, rejected, or schema-quality evidence is recorded.</Empty>}</Panel></div>
}

function DiagnosticsTab({ diagnostics, error, onRefresh }) {
  if (error) return <OperatorErrorNotice error={error} />
  if (!diagnostics) return <Empty>Run diagnostics to inspect the collector boundaries.</Empty>
  return <Panel title="Canonical boundary diagnostics" aside={<button type="button" className="qt2-button" onClick={onRefresh}><Stethoscope size={14} />Run again</button>} wide><div className="qt2-diagnostic-summary"><span><small>Likely failing boundary</small><strong>{diagnostics.likely_failing_boundary || 'None'}</strong></span><span><small>Recommended action</small><strong>{diagnostics.recommended_action?.replaceAll('_', ' ') || 'No action'}</strong></span></div><div className="qt2-diagnostic-grid">{(diagnostics.boundaries || []).map((boundary) => <article key={boundary.boundary}><header>{boundary.status === 'pass' ? <CheckCircle2 size={16} /> : <AlertTriangle size={16} />}<span><strong>{boundary.boundary.replaceAll('_', ' ')}</strong><Badge value={boundary.status} /></span></header><p>{boundary.summary}</p><details><summary>Structured evidence</summary><JsonBlock value={boundary.evidence} /></details></article>)}</div></Panel>
}

export function CollectorLensContent({ detail, diagnostics, events, gaps, diagnosticsError, actionError, actionBusy, onClose, onRefresh, onRunDiagnostics, onAction }) {
  const collector = detail.collector
  const [activeTab, setActiveTab] = useState('runtime')
  const [pendingAction, setPendingAction] = useState(null)
  const schemas = collector.fact_schemas || []
  const subjects = collector.subjects || []
  const allowedActions = collector.capabilities?.actions || []

  async function confirmAction(reason) {
    const action = pendingAction
    const succeeded = await onAction(action, reason)
    if (succeeded) setPendingAction(null)
  }

  return <>
    <header className="qt2-collector-lens-head"><div><span>Market-data collector</span><div><h1>{subjects.map((subject) => subject.provider_product_id || subject.symbol || subject.instrument_id).filter(Boolean).join(', ') || collector.collector_id}</h1><Badge value={collector.actual_state} /></div><p>{collector.provider} · {collector.collector_kind.replaceAll('_', ' ')} · {schemas.map((schema) => schema.fact_type).join(', ')}</p></div><div><button type="button" className="qt2-button" onClick={onRefresh}><RefreshCcw size={14} />Refresh</button><button type="button" className="qt2-button" onClick={onClose}><X size={14} />Exit</button></div></header>
    <div className="qt2-collector-state-strip"><p>{COLLECTOR_STATE_COPY[collector.actual_state] || 'Lifecycle explanation unavailable.'}</p><span>desired {collector.desired_state} · configured {collector.configured_state}</span></div>
    <div className="qt2-collector-actions" aria-label="Safe collector actions"><button type="button" onClick={onRunDiagnostics}><Stethoscope size={14} />Diagnose</button>{allowedActions.map((action) => { const spec = ACTIONS[action]; if (!spec) return null; const Icon = spec.Icon; return <button type="button" key={action} onClick={() => setPendingAction(action)} disabled={actionBusy}><Icon size={14} />{spec.label}</button> })}</div>
    <nav className="qt2-lens-tabs" aria-label="Collector evidence sections">{TABS.map(([id, label]) => <button type="button" key={id} className={activeTab === id ? 'is-active' : ''} onClick={() => setActiveTab(id)}>{label}</button>)}</nav>
    <div className="qt2-collector-lens-body">
      {activeTab === 'runtime' ? <RuntimeTab collector={collector} /> : null}
      {activeTab === 'activity' ? <ActivityTab events={events} operations={detail.operations || []} /> : null}
      {activeTab === 'facts' ? <FactsTab facts={detail.recent_facts || []} /> : null}
      {activeTab === 'quality' ? <QualityTab gaps={gaps?.gaps || detail.gaps || []} qualityEvents={gaps?.quality_events || detail.quality_events || []} /> : null}
      {activeTab === 'diagnostics' ? <DiagnosticsTab diagnostics={diagnostics} error={diagnosticsError} onRefresh={onRunDiagnostics} /> : null}
      {activeTab === 'configuration' ? <Panel title="Read-only code-owned configuration" wide><p className="qt2-panel-note">Collector creation, schemas, provider credentials, and material behavior remain code-reviewed concerns.</p><JsonBlock value={detail.read_only_configuration} /></Panel> : null}
    </div>
    {pendingAction ? <ActionDialog collector={collector} action={pendingAction} busy={actionBusy} error={actionError} onCancel={() => setPendingAction(null)} onConfirm={confirmAction} /> : null}
  </>
}
