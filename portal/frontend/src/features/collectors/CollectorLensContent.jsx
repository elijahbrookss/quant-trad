import { useEffect, useState } from 'react'
import { RefreshCcw, X } from 'lucide-react'
import { formatRelativeTime } from '../bots/state/botRuntimeStatus.js'
import { fetchLatestFundingRate, fetchLatestOpenInterest } from '../../adapters/marketData.adapter.js'
import { deriveCollectorHealth, COLLECTOR_HEALTH_COPY } from './collectorHealth.js'

const OPEN_INTEREST_FACT_TYPE = 'derivatives.open_interest'
const FUNDING_RATE_FACT_TYPE = 'derivatives.funding_rate'
const LATEST_VALUE_MAX_STALENESS_SECONDS = 3600

const STATUS_TONE_CLASS = {
  healthy: 'border-emerald-400/60 bg-emerald-400/15 text-emerald-100',
  failed: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
  disabled: 'border-white/10 bg-white/5 text-slate-200',
  overdue: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
  stale: 'border-amber-500/45 bg-amber-500/10 text-amber-200',
  unknown: 'border-white/10 bg-white/5 text-slate-200',
}

const STATUS_LABEL = {
  healthy: 'On schedule',
  failed: 'Latest attempt failed',
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

function LatestValuePanel({ definition }) {
  const [value, setValue] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const factType = definition?.fact_type

  useEffect(() => {
    if (factType !== OPEN_INTEREST_FACT_TYPE && factType !== FUNDING_RATE_FACT_TYPE) return
    let mounted = true
    setLoading(true)
    const fetcher = factType === OPEN_INTEREST_FACT_TYPE ? fetchLatestOpenInterest : fetchLatestFundingRate
    fetcher({
      instrumentId: definition.instrument_id,
      decisionTime: new Date().toISOString(),
      maxStalenessSeconds: LATEST_VALUE_MAX_STALENESS_SECONDS,
    })
      .then((payload) => {
        if (mounted) setValue(payload)
      })
      .catch((err) => {
        if (mounted) setError(err?.message || 'Unable to load latest value')
      })
      .finally(() => {
        if (mounted) setLoading(false)
      })
    return () => {
      mounted = false
    }
  }, [factType, definition?.instrument_id])

  if (factType !== OPEN_INTEREST_FACT_TYPE && factType !== FUNDING_RATE_FACT_TYPE) return null

  const label = factType === OPEN_INTEREST_FACT_TYPE ? 'Open interest' : 'Funding rate'

  return (
    <ReadoutPanel title={`Latest ${label.toLowerCase()}`}>
      {loading ? <ReadoutRow label="Status" value="Loading…" /> : null}
      {error ? <ReadoutRow label="Status" value={`Unavailable — ${error}`} /> : null}
      {!loading && !error && value?.available === false ? (
        <ReadoutRow label="Status" value="Unavailable — no fact within the staleness window" />
      ) : null}
      {!loading && !error && value?.available !== false && value ? (
        <>
          <ReadoutRow label="Value" value={JSON.stringify(value.fact?.value ?? value.fact ?? '—')} />
          <ReadoutRow label="Provenance" value={value.source?.provider} />
          <ReadoutRow label="Revision" value={value.revision} />
        </>
      ) : null}
    </ReadoutPanel>
  )
}

/**
 * Collector lens body. Unlike the bot lens, this is poll/refresh-on-demand
 * only — there is no per-collector heartbeat or WS (market-data-collector is
 * a single fixed compose service, not a container-per-collector), so this
 * deliberately never renders a "live" badge.
 */
export function CollectorLensContent({ definition, attempts, onClose, onRefresh, nowEpochMs = Date.now() }) {
  const health = deriveCollectorHealth(definition, attempts, nowEpochMs)
  const toneClass = STATUS_TONE_CLASS[health.status] || STATUS_TONE_CLASS.unknown

  return (
    <>
      <header className="border-b border-white/8 px-4 py-3 sm:px-5">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <p className="text-xs font-semibold text-slate-400">Collector</p>
            <div className="mt-2 flex flex-wrap items-center gap-2">
              <h1 className="text-[1.4rem] font-semibold tracking-[0.01em] text-slate-50">
                {[definition?.provider, definition?.fact_type].filter(Boolean).join(' · ') || 'Collector'}
              </h1>
              <span className={`inline-flex items-center gap-1.5 rounded-[3px] border px-2.5 py-1 text-sm font-semibold ${toneClass}`}>
                {STATUS_LABEL[health.status] || 'Unknown'}
              </span>
            </div>
            <p className="mt-2 text-sm text-slate-300">
              {definition?.instrument_id} · {definition?.venue}
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
        {' · '}Process liveness unknown — no heartbeat exists; this status is derived from schedule and attempt evidence.
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-4 py-4 sm:px-5">
        <div className="grid gap-4 xl:grid-cols-2">
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

          <LatestValuePanel definition={definition} />

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
        </div>
      </div>
    </>
  )
}
