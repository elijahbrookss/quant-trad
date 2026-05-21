import { useCallback, useEffect, useMemo, useState } from 'react'
import { AlertTriangle, ArrowLeft, GitCompare, RefreshCw } from 'lucide-react'
import { reportService } from '../../services/reportService.js'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters.js'
import {
  buildRunComparisonView,
  comparisonTone,
  metricDeltaState,
  normalizeComparisonLabel,
} from './reportComparisonViewModel.js'

const toneClasses = {
  good: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
  warn: 'border-amber-500/30 bg-amber-500/10 text-amber-200',
  bad: 'border-rose-500/30 bg-rose-500/10 text-rose-200',
  neutral: 'border-white/10 bg-white/[0.04] text-slate-300',
}

const formatValue = (value, format = 'number') => {
  if (value === null || value === undefined || value === '') return 'Not available'
  if (format === 'currency') return formatCurrency(value)
  if (format === 'percent') return formatPercent(value, 2)
  if (format === 'integer') return formatNumber(value, 0)
  return formatNumber(value, 2)
}

const formatSignedDelta = (value, format) => {
  if (value === null || value === undefined || value === '') return 'Not comparable'
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not comparable'
  const prefix = numeric > 0 ? '+' : ''
  if (format === 'currency') return `${prefix}${formatCurrency(numeric)}`
  if (format === 'percent') return `${prefix}${formatPercent(numeric, 2)}`
  return `${prefix}${formatValue(numeric, format)}`
}

function StatusBadge({ value, title }) {
  const tone = comparisonTone(value)
  return (
    <span className={`inline-flex rounded-[6px] border px-2 py-1 text-[10px] font-medium ${toneClasses[tone]}`} title={title}>
      {normalizeComparisonLabel(value)}
    </span>
  )
}

function SectionShell({ title, children, description }) {
  return (
    <section className="rounded-[8px] border border-white/8 bg-[#141923]/80 p-4">
      <div className="mb-4">
        <h2 className="text-sm font-semibold text-slate-100">{title}</h2>
        {description ? <p className="mt-1 text-xs text-slate-500">{description}</p> : null}
      </div>
      {children}
    </section>
  )
}

function DeltaCard({ metric }) {
  const { state, label, format } = metric
  return (
    <div className={`min-h-[8.5rem] rounded-[8px] border p-3 ${state.valid ? 'border-white/10 bg-black/24' : 'border-white/[0.07] bg-white/[0.025]'}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 text-[10px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
        <StatusBadge value={state.valid ? 'comparable' : 'not_available'} title={state.invalidReason || undefined} />
      </div>
      <div className={`mt-3 font-mono text-xl ${state.valid ? 'text-slate-100' : 'text-slate-500'}`}>{formatSignedDelta(state.delta, format)}</div>
      <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] text-slate-500">
        <div>
          <div className="uppercase tracking-[0.12em]">Left</div>
          <div className="mt-1 font-mono text-slate-300">{formatValue(state.left, format)}</div>
        </div>
        <div>
          <div className="uppercase tracking-[0.12em]">Right</div>
          <div className="mt-1 font-mono text-slate-300">{formatValue(state.right, format)}</div>
        </div>
      </div>
      {!state.valid ? <div className="mt-2 text-[11px] leading-4 text-slate-500">{state.invalidReason}</div> : null}
    </div>
  )
}

function TrustMatrix({ rows }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-left text-sm">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.16em] text-slate-500">
            <th className="pb-3 pr-4">Check</th>
            <th className="pb-3 pr-4">Left</th>
            <th className="pb-3 pr-4">Right</th>
            <th className="pb-3">Result</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.key} className="border-b border-white/5">
              <td className="py-3 pr-4 text-slate-200">{row.label}</td>
              <td className="py-3 pr-4 text-slate-400">{normalizeComparisonLabel(row.left)}</td>
              <td className="py-3 pr-4 text-slate-400">{row.right === '' ? '' : normalizeComparisonLabel(row.right)}</td>
              <td className="py-3"><StatusBadge value={row.status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function KeyValue({ label, value }) {
  return (
    <div className="rounded-[7px] border border-white/8 bg-black/20 p-3">
      <div className="text-[10px] uppercase tracking-[0.16em] text-slate-500">{label}</div>
      <div className="mt-1 break-all font-mono text-sm text-slate-100">{value ?? 'Not available'}</div>
    </div>
  )
}

function BehaviorPanel({ behavior }) {
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
      <KeyValue label="Decisions" value={behavior.decisionCountDelta ?? 'Not available'} />
      <KeyValue label="Accepted" value={behavior.acceptedDelta ?? 'Not available'} />
      <KeyValue label="Rejected" value={behavior.rejectedDelta ?? 'Not available'} />
      <KeyValue label="Entries / Exits" value={`${behavior.entryCountDelta ?? 'Not available'} / ${behavior.exitCountDelta ?? 'Not available'}`} />
      <KeyValue label="Trade Lifecycle" value={behavior.tradeLifecycleEqual === null ? behavior.tradeLifecycleSource : normalizeComparisonLabel(behavior.tradeLifecycleEqual)} />
      <KeyValue label="Verdict Changes" value={behavior.verdictChanges ?? behavior.goldenArtifactStatus} />
      <KeyValue label="Missing Decisions" value={behavior.missingDecisionIds.length} />
      <KeyValue label="Extra Decisions" value={behavior.extraDecisionIds.length} />
    </div>
  )
}

function GoldenEvidencePanel({ evidence }) {
  if (!evidence.available) {
    return (
      <div className="rounded-[8px] border border-white/8 bg-black/20 p-4 text-sm text-slate-400">
        Golden evidence not available. Report-level comparison remains visible.
      </div>
    )
  }
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge value={evidence.verdict} />
        <StatusBadge value={evidence.semanticFingerprintMatch === true ? 'semantic_match' : evidence.semanticFingerprintMatch === false ? 'semantic_drift' : 'unknown'} />
        <StatusBadge value={evidence.operationalFingerprintMatch === true ? 'operational_match' : evidence.operationalFingerprintMatch === false ? 'operational_drift_only' : 'unknown'} />
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <KeyValue label="Decisions" value={`${evidence.decisionCountLeft ?? 'NA'} / ${evidence.decisionCountRight ?? 'NA'}`} />
        <KeyValue label="Missing / Extra" value={`${evidence.missingDecisionCount ?? 'NA'} / ${evidence.extraDecisionCount ?? 'NA'}`} />
        <KeyValue label="Verdict Changes" value={evidence.verdictChangeCount ?? 'Not available'} />
        <KeyValue label="Decision Lists" value={evidence.decisionDiffFullListsAvailable ? 'Full evidence' : 'Partial evidence'} />
        <KeyValue label="Verdict Change Rows" value={evidence.verdictChangesFullAvailable ? 'Full evidence' : 'Partial evidence'} />
        <KeyValue label="Trade Lifecycle" value={normalizeComparisonLabel(evidence.tradeLifecycleEqual)} />
        <KeyValue label="Trades" value={`${evidence.tradeCountLeft ?? 'NA'} / ${evidence.tradeCountRight ?? 'NA'}`} />
        <KeyValue label="Wallet Trace Missing" value={`${evidence.walletTraceMissingLeft ?? 'NA'} / ${evidence.walletTraceMissingRight ?? 'NA'}`} />
        <KeyValue label="Market-Time Overtakes" value={`${evidence.walletMarketTimeOvertakeLeft ?? 'NA'} / ${evidence.walletMarketTimeOvertakeRight ?? 'NA'}`} />
        <KeyValue label="Entry Order Timeouts" value={`${evidence.entryDecisionOrderTimeoutLeft ?? 'NA'} / ${evidence.entryDecisionOrderTimeoutRight ?? 'NA'}`} />
        <KeyValue label="Runtime Ordering" value={`${evidence.runtimeOrderingLeft?.status || 'NA'} / ${evidence.runtimeOrderingRight?.status || 'NA'}`} />
        <KeyValue label="Runtime Gaps" value={`${evidence.runtimeOrderingLeft?.gap_count ?? 'NA'} / ${evidence.runtimeOrderingRight?.gap_count ?? 'NA'}`} />
        <KeyValue label="Artifact" value={evidence.artifactPath || 'Not available'} />
        <KeyValue label="Generated" value={evidence.generatedAt || 'Not available'} />
      </div>
      {evidence.failReasons.length ? (
        <div className="rounded-[8px] border border-rose-500/20 bg-rose-500/10 p-3 text-xs leading-5 text-rose-100">
          {evidence.failReasons.join(', ')}
        </div>
      ) : null}
      {evidence.missingDecisionIds.length || evidence.extraDecisionIds.length ? (
        <div className="grid gap-3 md:grid-cols-2">
          <KeyValue label="Missing Decision IDs" value={evidence.missingDecisionIds.join(', ') || 'None'} />
          <KeyValue label="Extra Decision IDs" value={evidence.extraDecisionIds.join(', ') || 'None'} />
        </div>
      ) : null}
    </div>
  )
}

function SymbolDeltaTable({ rows }) {
  if (!rows.length) return <div className="rounded-[8px] border border-white/8 bg-black/20 p-5 text-sm text-slate-500">No symbol deltas available.</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-left text-sm">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.16em] text-slate-500">
            <th className="pb-3 pr-4">Symbol</th>
            <th className="pb-3 pr-4 text-right">Trades</th>
            <th className="pb-3 pr-4 text-right">Net PnL</th>
            <th className="pb-3 pr-4 text-right">Fees</th>
            <th className="pb-3 pr-4 text-right">Win Rate</th>
            <th className="pb-3 text-right">Decisions</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const netPnl = metricDeltaState(row.net_pnl_delta)
            const fees = metricDeltaState(row.fees_delta)
            const winRate = metricDeltaState(row.win_rate_delta)
            return (
              <tr key={row.symbol} className="border-b border-white/5">
                <td className="py-3 pr-4 font-mono text-slate-200">
                  {row.symbol}
                  {row.missing_on_left || row.missing_on_right ? <span className="ml-2 text-[10px] text-amber-200">missing side</span> : null}
                </td>
                <td className="py-3 pr-4 text-right font-mono text-slate-300">{row.trade_count_delta ?? 'Not available'}</td>
                <td className="py-3 pr-4 text-right font-mono text-slate-300">{netPnl.valid ? formatSignedDelta(netPnl.delta, 'currency') : 'Not comparable'}</td>
                <td className="py-3 pr-4 text-right font-mono text-slate-300">{fees.valid ? formatSignedDelta(fees.delta, 'currency') : 'Not comparable'}</td>
                <td className="py-3 pr-4 text-right font-mono text-slate-300">{winRate.valid ? formatSignedDelta(winRate.delta, 'percent') : 'Not comparable'}</td>
                <td className="py-3 text-right font-mono text-slate-300">{row.decision_delta ?? 'Not available'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function FirstDivergencePanel({ divergence }) {
  if (!divergence.present) {
    return (
      <div className="rounded-[8px] border border-white/8 bg-black/20 p-4 text-sm text-slate-300">
        {divergence.divergenceType === 'none' ? 'No semantic divergence detected.' : divergence.explanation || 'First divergence not computed.'}
      </div>
    )
  }
  return (
    <div className="rounded-[8px] border border-rose-500/20 bg-rose-500/10 p-4 text-sm text-rose-100">
      <div className="font-semibold">{normalizeComparisonLabel(divergence.divergenceType)}</div>
      <div className="mt-2 text-xs leading-5">{divergence.explanation}</div>
      <div className="mt-3 grid gap-2 md:grid-cols-2">
        <KeyValue label="Field" value={divergence.fieldPath || 'Not available'} />
        <KeyValue label="Source" value={normalizeComparisonLabel(divergence.source)} />
        <KeyValue label="Left" value={String(divergence.leftValue ?? 'Not available')} />
        <KeyValue label="Right" value={String(divergence.rightValue ?? 'Not available')} />
      </div>
    </div>
  )
}

function OperationalDriftPanel({ operational }) {
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge value={operational.operational_drift_summary || 'not_available'} />
        <StatusBadge value={operational.operational_fingerprint_match === true ? 'match' : operational.operational_fingerprint_match === false ? 'mismatch' : 'unknown'} />
      </div>
      {operational.statement ? <div className="rounded-[8px] border border-white/8 bg-black/20 p-3 text-sm text-slate-300">{operational.statement}</div> : null}
      {operational.diagnostic_only_differences?.length ? (
        <div className="text-xs leading-5 text-slate-500">{operational.diagnostic_only_differences.join(', ')}</div>
      ) : null}
    </div>
  )
}

export function RunComparisonPage({ leftRunId, rightRunId, onBack }) {
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const loadComparison = useCallback(async (options = {}) => {
    if (!leftRunId || !rightRunId) return
    setLoading(true)
    setError(null)
    try {
      const next = await reportService.compareRunReports(leftRunId, rightRunId, options)
      setPayload(next)
    } catch (err) {
      setError(err?.message || 'Comparison request failed')
    } finally {
      setLoading(false)
    }
  }, [leftRunId, rightRunId])

  useEffect(() => {
    loadComparison()
  }, [loadComparison])

  const view = useMemo(() => buildRunComparisonView(payload || {}), [payload])

  if (!leftRunId || !rightRunId) {
    return (
      <div className="rounded-[8px] border border-amber-500/20 bg-amber-500/10 p-6 text-sm text-amber-100">
        Select two ready terminal reports before opening comparison.
      </div>
    )
  }

  return (
    <div className="space-y-5">
      <header className="rounded-[8px] border border-white/8 bg-[#151924]/85 p-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0">
            <button type="button" onClick={onBack} className="mb-3 inline-flex items-center gap-2 text-xs text-slate-400 hover:text-slate-200">
              <ArrowLeft className="size-3.5" />
              Reports
            </button>
            <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.24em] text-slate-500">
              <GitCompare className="size-3.5" />
              Report Comparison
            </div>
            <h1 className="mt-2 break-all text-xl font-semibold text-slate-100">
              {leftRunId.slice(0, 8)} vs {rightRunId.slice(0, 8)}
            </h1>
            <div className="mt-3 flex flex-wrap gap-2">
              <StatusBadge value={view.comparisonVerdict} />
              <StatusBadge value={view.comparisonStatus} />
              <StatusBadge value={view.canCompare ? 'can_compare' : view.blockedReason || 'blocked'} />
            </div>
          </div>
          <button
            type="button"
            onClick={() => loadComparison({ force: true })}
            disabled={loading}
            className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100 disabled:opacity-50"
          >
            <RefreshCw className={`size-3.5 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </header>

      {loading && !payload ? (
        <div className="flex items-center justify-center rounded-[8px] border border-white/8 bg-black/20 p-10 text-sm text-slate-400">
          <RefreshCw className="mr-3 size-4 animate-spin text-[color:var(--accent-text-soft)]" />
          Loading comparison...
        </div>
      ) : error ? (
        <div className="rounded-[8px] border border-rose-500/20 bg-rose-500/10 p-5 text-sm text-rose-100">
          <div className="flex items-center gap-2">
            <AlertTriangle className="size-4" />
            {error}
          </div>
        </div>
      ) : payload ? (
        <>
          {view.blockedReason ? (
            <div className="rounded-[8px] border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-100">
              {normalizeComparisonLabel(view.blockedReason)}
            </div>
          ) : null}
          <SectionShell title="Trust Comparison Matrix">
            <TrustMatrix rows={view.trustRows} />
          </SectionShell>
          <SectionShell title="Performance Delta Grid">
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {view.performanceMetrics.map((metric) => (
                <DeltaCard key={metric.key} metric={metric} />
              ))}
            </div>
          </SectionShell>
          <SectionShell title="Behavior Delta Panel">
            <BehaviorPanel behavior={view.behavior} />
          </SectionShell>
          <SectionShell title="Golden Evidence">
            <GoldenEvidencePanel evidence={view.goldenEvidence} />
          </SectionShell>
          <SectionShell title="Symbol Delta Table">
            <SymbolDeltaTable rows={view.symbolDeltas} />
          </SectionShell>
          <SectionShell title="First Divergence Panel">
            <FirstDivergencePanel divergence={view.firstDivergence} />
          </SectionShell>
          <SectionShell title="Operational Drift Panel">
            <OperationalDriftPanel operational={view.operationalDrift} />
          </SectionShell>
        </>
      ) : null}
    </div>
  )
}
