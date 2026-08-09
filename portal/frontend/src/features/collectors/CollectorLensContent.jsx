import { useState } from 'react'
import { RefreshCcw, X } from 'lucide-react'
import { formatRelativeTime } from '../bots/state/botRuntimeStatus.js'
import { deriveCollectorHealth, COLLECTOR_HEALTH_COPY } from './collectorHealth.js'
import { OperatorErrorNotice } from '../../v2/components/OperatorErrorNotice.jsx'

const STATUS_TONE_CLASS = {
  healthy: 'border-emerald-400/60 bg-emerald-400/15 text-emerald-100',
  failed: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
  offline: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
  stalled: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
  disabled: 'border-white/10 bg-white/5 text-slate-200',
  overdue: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
  stale: 'border-amber-500/45 bg-amber-500/10 text-amber-200',
  unknown: 'border-white/10 bg-white/5 text-slate-200',
}

const STATUS_LABEL = {
  healthy: 'On schedule',
  failed: 'Latest attempt failed',
  offline: 'Worker offline',
  stalled: 'Attempt stalled',
  disabled: 'Scheduler disabled',
  overdue: 'Overdue',
  stale: 'Stale',
  unknown: 'Unknown',
}

function ReadoutRow({ label, value }) {
  return (
    <div className="flex items-center justify-between gap-4 px-4 py-3">
      <span className="text-xs font-medium text-slate-500">{label}</span>
      <span className="max-w-[60%] text-right text-sm text-slate-200">{value ?? '—'}</span>
    </div>
  )
}

function ReadoutPanel({ title, children }) {
  return (
    <section className="qt-ops-console overflow-hidden">
      <header className="border-b border-white/8 px-4 py-3">
        <p className="text-sm font-semibold text-slate-100">{title}</p>
      </header>
      <div className="divide-y divide-white/6">{children}</div>
    </section>
  )
}

function FactHistoryPanel({ history }) {
  const samples = Array.isArray(history?.samples) ? history.samples : []
  const values = samples.map((sample) => Number(sample?.fact?.value)).filter(Number.isFinite)
  const minimum = values.length ? Math.min(...values) : null
  const maximum = values.length ? Math.max(...values) : null
  const span = Math.max((maximum ?? 0) - (minimum ?? 0), 1e-12)
  const points = values.map((value, index) => {
    const x = values.length <= 1 ? 50 : (index / (values.length - 1)) * 100
    const y = 44 - ((value - minimum) / span) * 38
    return x.toFixed(2) + "," + y.toFixed(2)
  }).join(" ")
  const latest = samples[samples.length - 1] || null
  return (
    <ReadoutPanel title="Canonical fact history · last 24 hours">
      {samples.length ? (
        <div className="qt2-fact-chart">
          <svg viewBox="0 0 100 48" preserveAspectRatio="none" role="img" aria-label="Canonical fact value history">
            <polyline points={points} fill="none" vectorEffect="non-scaling-stroke" />
          </svg>
          <div><span><small>Latest</small><strong>{latest?.fact?.value ?? "—"} {latest?.fact?.unit || ""}</strong></span><span><small>Range</small><strong>{minimum?.toLocaleString()} – {maximum?.toLocaleString()}</strong></span><span><small>Samples</small><strong>{samples.length}{history?.truncated ? "+" : ""}</strong></span></div>
        </div>
      ) : <div className="px-4 py-8 text-sm text-slate-500">No canonical facts were persisted in this bounded window.</div>}
      {latest ? <><ReadoutRow label="Latest sample" value={latest.fact?.sample_time} /><ReadoutRow label="Known at" value={latest.fact?.known_at} /><ReadoutRow label="Provider" value={latest.source?.provider} /><ReadoutRow label="Revision" value={latest.revision} /></> : null}
    </ReadoutPanel>
  )
}

function AttemptLatencyPanel({ attempts }) {
  const rows = attempts.slice(0, 16).reverse().map((attempt) => {
    const started = Date.parse(attempt?.started_at || "")
    const finished = Date.parse(attempt?.finished_at || "")
    const totalMs = Number.isFinite(started) && Number.isFinite(finished) ? Math.max(0, finished - started) : 0
    const providerMs = Number(attempt?.evidence?.timings_ms?.provider_request || 0)
    return { attempt, totalMs, providerMs }
  })
  const maximum = Math.max(...rows.map((row) => row.totalMs), 1)
  const average = rows.length ? rows.reduce((sum, row) => sum + row.totalMs, 0) / rows.length : 0
  return (
    <ReadoutPanel title="Collection latency · recent attempts">
      {rows.length ? <div className="qt2-latency-chart">
        <div className="qt2-latency-bars">{rows.map(({ attempt, totalMs, providerMs }) => <span key={attempt.id} className={attempt.status === "succeeded" ? "is-success" : "is-failure"} style={{ height: Math.max(8, (totalMs / maximum) * 100) + "%" }} title={attempt.status + " · " + totalMs.toFixed(0) + "ms total · " + (providerMs ? providerMs.toFixed(0) + "ms provider" : "provider timing unavailable")} />)}</div>
        <div className="qt2-latency-summary"><span><small>Average</small><strong>{average.toFixed(0)}ms</strong></span><span><small>Slowest</small><strong>{maximum.toFixed(0)}ms</strong></span><span><small>Provider split</small><strong>{rows.some((row) => row.providerMs > 0) ? "Reported" : "Awaiting upgraded worker"}</strong></span></div>
      </div> : <div className="px-4 py-8 text-sm text-slate-500">No attempt timing evidence is available.</div>}
    </ReadoutPanel>
  )
}

/**
 * Market Lens reads durable definition, worker, attempt, and typed-fact evidence.
 * Refresh is explicit; process liveness comes from the worker heartbeat projection.
 */
export function CollectorLensContent({ definition, attempts, factHistory, attemptsError, factsError, onClose, onRefresh, nowEpochMs = Date.now() }) {
  const health = deriveCollectorHealth(definition, attempts, nowEpochMs)
  const toneClass = STATUS_TONE_CLASS[health.status] || STATUS_TONE_CLASS.unknown
  const [activeTab, setActiveTab] = useState('status')
  const samples = Array.isArray(factHistory?.samples) ? factHistory.samples : []
  const revisedSamples = samples.filter((sample) => Number(sample?.revision || 0) > 1).length
  const missingProvenance = samples.filter((sample) => !sample?.source?.provider || !sample?.source_identity_key).length
  const failedAttempts = attempts.filter((attempt) => attempt?.status === 'failed').length

  return (
    <>
      <header className="border-b border-white/8 px-4 py-3 sm:px-5">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <p className="text-xs font-semibold text-slate-400">Market Lens</p>
            <div className="mt-2 flex flex-wrap items-center gap-2">
              <h1 className="text-[1.4rem] font-semibold tracking-[0.01em] text-slate-50">
                {definition?.instrument_symbol || definition?.instrument_id || 'Market evidence'}
              </h1>
              <span className={`inline-flex items-center gap-1.5 rounded-[3px] border px-2.5 py-1 text-sm font-semibold ${toneClass}`}>
                {STATUS_LABEL[health.status] || 'Unknown'}
              </span>
            </div>
            <p className="mt-2 text-sm text-slate-300">
              {definition?.provider} · {definition?.config?.provider_product_id || definition?.instrument_id} · {definition?.fact_type}
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={onRefresh}
              className="inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
            >
              <RefreshCcw className="size-3.5" />
              Refresh
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
            >
              <X className="size-3.5" />
              Exit Lens
            </button>
          </div>
        </div>
      </header>

      <div className="flex flex-wrap gap-2 border-b border-white/8 px-4 py-3 text-sm text-slate-300 sm:px-5">
        {COLLECTOR_HEALTH_COPY[health.status]}
        {' · '}{health.workerAlive ? 'Worker heartbeat current' : health.workerLivenessKnown ? 'Worker heartbeat expired' : 'Worker heartbeat unavailable'}
      </div>

      <nav className="qt2-lens-tabs" aria-label="Market Lens sections">
        {[
          ['status', 'Status'],
          ['facts', 'Facts'],
          ['attempts', 'Attempts'],
          ['quality', 'Quality'],
        ].map(([id, label]) => <button type="button" key={id} className={activeTab === id ? 'is-active' : ''} aria-pressed={activeTab === id} onClick={() => setActiveTab(id)}>{label}</button>)}
      </nav>

      <div className="min-h-0 flex-1 overflow-auto px-4 py-4 sm:px-5">
        <div className="grid gap-4 xl:grid-cols-2">
          {activeTab === 'status' ? <>
          <ReadoutPanel title="Definition">
            <ReadoutRow label="Provider" value={definition?.provider} />
            <ReadoutRow label="Venue" value={definition?.venue} />
            <ReadoutRow label="Instrument" value={definition?.instrument_id} />
            <ReadoutRow label="Fact type" value={definition?.fact_type} />
            <ReadoutRow label="Poll interval" value={definition?.poll_interval_seconds ? `${definition.poll_interval_seconds}s` : '—'} />
            <ReadoutRow label="Max attempts" value={definition?.max_attempts} />
            <ReadoutRow label="Scheduler enabled" value={health.schedulerEnabled ? 'Yes' : 'No'} />
          </ReadoutPanel>

          <ReadoutPanel title="Health facts">
            <ReadoutRow label="Last attempt" value={health.lastAttemptAt ? `${formatRelativeTime(health.lastAttemptAt, { nowEpochMs }) || health.lastAttemptAt} (${health.lastAttemptStatus})` : 'none recorded'} />
            <ReadoutRow label="Last success" value={health.lastSuccessAt ? formatRelativeTime(health.lastSuccessAt, { nowEpochMs }) || health.lastSuccessAt : 'none recorded'} />
            <ReadoutRow label="Next expected" value={health.nextExpectedAt ? formatRelativeTime(health.nextExpectedAt, { nowEpochMs }) || health.nextExpectedAt : '—'} />
            <ReadoutRow label="Overdue" value={health.overdue ? 'Yes' : 'No'} />
            <ReadoutRow label="Stale" value={health.stale ? 'Yes' : 'No'} />
          </ReadoutPanel>
          <div className="xl:col-span-2"><AttemptLatencyPanel attempts={attempts} /></div>
          </> : null}

          {activeTab === 'facts' ? <div className="xl:col-span-2">
            {factsError ? <OperatorErrorNotice error={factsError} /> : null}
            <FactHistoryPanel history={factHistory} />
          </div> : null}

          {activeTab === 'attempts' ? <>
          {attemptsError ? <div className="xl:col-span-2"><OperatorErrorNotice error={attemptsError} /></div> : null}
          <div className="xl:col-span-2"><AttemptLatencyPanel attempts={attempts} /></div>
          <section className="qt-ops-console overflow-hidden xl:col-span-2">
            <header className="border-b border-white/8 px-4 py-3">
              <p className="text-sm font-semibold text-slate-100">Recent attempts</p>
            </header>
            {attempts.length ? (
              <div className="overflow-auto">
                <table className="min-w-full text-left text-sm text-slate-200">
                  <thead className="border-b border-white/8 bg-black/25 text-xs text-slate-500">
                    <tr>
                      <th className="px-4 py-3">Status</th>
                      <th className="px-4 py-3">Started</th>
                      <th className="px-4 py-3">Finished</th>
                      <th className="px-4 py-3">Error</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/6">
                    {attempts.map((attempt) => (
                      <tr key={attempt.id}>
                        <td className="qt-mono px-4 py-3">{attempt.status}</td>
                        <td className="px-4 py-3">{formatRelativeTime(attempt.started_at, { nowEpochMs }) || attempt.started_at}</td>
                        <td className="px-4 py-3">{attempt.finished_at ? formatRelativeTime(attempt.finished_at, { nowEpochMs }) || attempt.finished_at : '—'}</td>
                        <td className="px-4 py-3 text-rose-300">{attempt.error || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="px-4 py-8 text-sm text-slate-500">No attempts recorded yet.</div>
            )}
          </section>
          </> : null}

          {activeTab === 'quality' ? <div className="xl:col-span-2">
            {factsError ? <OperatorErrorNotice error={factsError} /> : null}
            {attemptsError ? <OperatorErrorNotice error={attemptsError} /> : null}
            <ReadoutPanel title="Evidence quality">
              <ReadoutRow label="Worker heartbeat" value={health.workerAlive ? 'Current' : health.workerLivenessKnown ? 'Expired' : 'Unavailable'} />
              <ReadoutRow label="Canonical samples in window" value={samples.length} />
              <ReadoutRow label="Revised samples" value={revisedSamples} />
              <ReadoutRow label="Missing provider provenance" value={missingProvenance} />
              <ReadoutRow label="Failed recent attempts" value={failedAttempts} />
              <ReadoutRow label="History window" value={factHistory?.truncated ? 'Truncated at bounded limit' : 'Complete for requested bounded read'} />
              <ReadoutRow label="Gap catalog" value="Not exposed by this scheduled-fact lens" />
            </ReadoutPanel>
          </div> : null}
        </div>
      </div>
    </>
  )
}
