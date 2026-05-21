import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  Clock3,
  Database,
  Download,
  ExternalLink,
  FileJson,
  RefreshCw,
  ShieldCheck,
  WalletCards,
} from 'lucide-react'
import { reportService } from '../../services/reportService.js'
import { formatCurrency, formatNumber, formatPercent, formatSymbols, formatTimeframe } from '../../utils/formatters.js'
import { formatExecutionModeLabel } from '../../features/bots/executionMode.js'
import { metricDisplayState, runReportView } from './reportContractViewModel.js'

const UNKNOWN = 'Unknown'

const statusTone = (value) => {
  const normalized = String(value ?? '').toLowerCase()
  if (!normalized || ['unknown', 'not_available', 'not_computed', 'unavailable'].includes(normalized)) return 'neutral'
  if (
    [
      'ready',
      'research_ready',
      'research_valid',
      'completed',
      'pass',
      'passed',
      'certified',
      'clean',
      'gapless',
      'complete',
      'true',
      'ok',
      'match',
      'matched',
      'stable',
    ].includes(normalized)
  ) {
    return 'good'
  }
  if (
    [
      'partial',
      'degraded',
      'ready_with_caveats',
      'caution',
      'warning',
      'operational_drift',
      'operational_only_drift',
    ].includes(normalized)
  ) {
    return 'warn'
  }
  if (
    [
      'blocked',
      'failed',
      'fail',
      'mismatch',
      'missing',
      'incomplete',
      'false',
      'invalid',
      'drift',
    ].includes(normalized)
  ) {
    return 'bad'
  }
  return 'neutral'
}

const toneClasses = {
  good: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
  warn: 'border-amber-500/30 bg-amber-500/10 text-amber-200',
  bad: 'border-rose-500/30 bg-rose-500/10 text-rose-200',
  neutral: 'border-white/10 bg-white/[0.04] text-slate-300',
}

const normalizeLabel = (value) => {
  if (value === true) return 'Complete'
  if (value === false) return 'Incomplete'
  if (value === null || value === undefined || value === '') return UNKNOWN
  return String(value)
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

const shortHash = (value) => {
  const text = String(value || '').trim()
  if (!text) return 'Not available'
  if (text.length <= 16) return text
  return `${text.slice(0, 10)}...${text.slice(-6)}`
}

const formatDateTime = (value) => {
  if (!value) return 'Not available'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return String(value)
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export const formatDuration = (seconds) => {
  if (seconds === null || seconds === undefined || seconds === '') return 'Not available'
  const numeric = Number(seconds)
  if (!Number.isFinite(numeric) || numeric < 0) return 'Not available'
  const total = Math.floor(numeric)
  const hours = Math.floor(total / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  const secs = total % 60
  if (hours) return `${hours}h ${String(minutes).padStart(2, '0')}m`
  if (minutes) return `${minutes}m ${String(secs).padStart(2, '0')}s`
  return `${secs}s`
}

const formatMetricValue = (metric, format = 'number') => {
  const state = metricDisplayState(metric)
  if (!state.valid) return 'Not available'
  if (format === 'currency') return formatCurrency(state.value)
  if (format === 'percent') return formatPercent(state.value, 2)
  if (format === 'integer') return formatNumber(state.value, 0)
  if (format === 'duration') return formatDuration(state.value)
  return formatNumber(state.value, 2)
}

const metricMetaTitle = (metric) => {
  const state = metricDisplayState(metric)
  const parts = []
  if (state.method) parts.push(`Method: ${state.method}`)
  if (state.source) parts.push(`Source: ${state.source}`)
  if (state.sampleCount !== null) parts.push(`Samples: ${state.sampleCount}`)
  if (state.minimumSampleCount !== null) parts.push(`Minimum: ${state.minimumSampleCount}`)
  if (!state.valid && state.invalidReason) parts.push(`Invalid: ${state.invalidReason}`)
  return parts.join(' | ') || undefined
}

export function MetricValidityBadge({ metric }) {
  const state = metricDisplayState(metric)
  return (
    <span
      className={`inline-flex items-center rounded-[6px] border px-2 py-0.5 text-[9px] uppercase tracking-[0.16em] ${
        state.valid ? toneClasses.good : toneClasses.neutral
      }`}
    >
      {state.valid ? 'Valid' : 'Not available'}
    </span>
  )
}

export function MetricCard({ label, metric, format = 'number', emphasis = false }) {
  const state = metricDisplayState(metric)
  return (
    <div
      className={`min-h-[8.5rem] rounded-[8px] border p-3 ${
        state.valid ? 'border-white/10 bg-black/24' : 'border-white/[0.07] bg-white/[0.025]'
      }`}
      title={metricMetaTitle(metric)}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 text-[10px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
        <MetricValidityBadge metric={metric} />
      </div>
      <div className={`mt-3 font-mono ${emphasis ? 'text-2xl' : 'text-xl'} ${state.valid ? 'text-slate-100' : 'text-slate-500'}`}>
        {formatMetricValue(metric, format)}
      </div>
      <div className="mt-2 min-h-[2rem] text-[11px] leading-4 text-slate-500">
        {state.valid ? (
          <span>{[state.method, state.source].filter(Boolean).join(' / ') || 'Backend computed metric'}</span>
        ) : (
          <span>{state.invalidReason || 'Metric not computed by backend'}</span>
        )}
      </div>
      {state.caveats.length ? (
        <div className="mt-2 text-[11px] leading-4 text-amber-200/80">{state.caveats.slice(0, 2).join(' | ')}</div>
      ) : null}
    </div>
  )
}

function StatusBadge({ label, value, title }) {
  const tone = statusTone(value)
  return (
    <div className={`rounded-[8px] border p-3 ${toneClasses[tone]}`} title={title}>
      <div className="text-[9px] uppercase tracking-[0.18em] opacity-70">{label}</div>
      <div className="mt-1 break-words text-sm font-semibold">{normalizeLabel(value)}</div>
    </div>
  )
}

export function TrustStrip({ trust = {} }) {
  const semanticValue = trust.semantic_fingerprint ? 'present' : 'not_available'
  const operationalValue = trust.operational_fingerprint ? 'present' : 'not_available'
  const walletValue = trust.wallet_trace_complete
  const observerValue = trust.observer_invariance_status || 'unknown'

  return (
    <section className="rounded-[8px] border border-white/8 bg-[#141923]/80 p-4">
      <div className="mb-3 flex items-center gap-2">
        <ShieldCheck className="size-4 text-[color:var(--accent-text-soft)]" />
        <h2 className="text-sm font-semibold text-slate-100">Research Trust</h2>
      </div>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatusBadge label="Lifecycle" value={trust.lifecycle_status || UNKNOWN} title={trust.terminal_reason || undefined} />
        <StatusBadge label="Readiness" value={trust.readiness_status || UNKNOWN} />
        <StatusBadge label="Golden" value={trust.golden_status || trust.golden_candidate_status || 'not_available'} />
        <StatusBadge label="Semantic FP" value={semanticValue} title={trust.semantic_fingerprint || undefined} />
        <StatusBadge label="Operational FP" value={operationalValue} title={trust.operational_fingerprint || undefined} />
        <StatusBadge label="Runtime Ordering" value={trust.runtime_ordering_status || UNKNOWN} />
        <StatusBadge label="Wallet Trace" value={walletValue} />
        <StatusBadge label="Candle Continuity" value={trust.candle_continuity_status || UNKNOWN} />
        <StatusBadge label="Observer Safety" value={observerValue} />
        <StatusBadge label="Canonical Continuity" value={trust.canonical_continuity_evidence_status || UNKNOWN} />
        <StatusBadge label="Market Overtakes" value={trust.wallet_market_time_overtake_count ?? 'not_available'} />
        <StatusBadge label="Order Timeouts" value={trust.entry_decision_order_timeout_count ?? 'not_available'} />
      </div>
      {trust.first_failure_reason ? (
        <div className="mt-3 rounded-[8px] border border-rose-500/20 bg-rose-500/10 p-3 text-sm text-rose-100">
          First failure: {trust.first_failure_reason}
        </div>
      ) : null}
      {trust.readiness_blockers?.length ? (
        <div className="mt-3 rounded-[8px] border border-amber-500/20 bg-amber-500/10 p-3 text-xs leading-5 text-amber-100">
          Readiness blockers: {trust.readiness_blockers.join(', ')}
        </div>
      ) : null}
      {trust.caveats?.length ? (
        <div className="mt-3 rounded-[8px] border border-white/8 bg-black/20 p-3 text-xs leading-5 text-slate-400">
          Caveats: {trust.caveats.join(' | ')}
        </div>
      ) : null}
    </section>
  )
}

function SectionShell({ title, icon: Icon, children, description }) {
  return (
    <section className="rounded-[8px] border border-white/8 bg-[#141923]/80 p-4">
      <div className="mb-4 flex flex-col gap-1 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            {Icon ? <Icon className="size-4 text-[color:var(--accent-text-soft)]" /> : null}
            <h2 className="text-sm font-semibold text-slate-100">{title}</h2>
          </div>
          {description ? <p className="mt-1 text-xs text-slate-500">{description}</p> : null}
        </div>
      </div>
      {children}
    </section>
  )
}

function ReportIdentityHeader({ view, durationSeconds, onBack, onRefresh, onExport, exporting }) {
  const { identity, trust, runId } = view
  const symbols = identity.symbols || []
  const timeframe = identity.timeframe || identity.timeframes?.[0]
  return (
    <header className="rounded-[8px] border border-white/8 bg-[#151924]/85 p-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0 space-y-3">
          <button
            type="button"
            onClick={onBack}
            className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100"
          >
            <ArrowLeft className="size-3.5" />
            Reports
          </button>
          <div>
            <div className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Run Report v2</div>
            <h1 className="mt-1 break-all font-mono text-xl font-semibold text-slate-100">{runId || UNKNOWN}</h1>
            <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-400">
              <span>{identity.bot_name || identity.bot_id || 'Bot unknown'}</span>
              <span className="text-slate-700">/</span>
              <span>{identity.strategy_name || identity.strategy_id || 'Strategy unknown'}</span>
              <span className="text-slate-700">/</span>
              <span>{identity.run_type || 'run type unknown'}</span>
              <span className="text-slate-700">/</span>
              <span>{formatSymbols(symbols, 4)}</span>
              <span>{formatTimeframe(timeframe)}</span>
            </div>
          </div>
        </div>
        <div className="grid gap-2 text-xs sm:grid-cols-2 lg:min-w-[26rem]">
          <div className="rounded-[7px] border border-white/8 bg-black/20 p-3">
            <div className="text-[9px] uppercase tracking-[0.18em] text-slate-500">Started</div>
            <div className="mt-1 text-slate-200">{formatDateTime(identity.wall_clock_window?.start)}</div>
          </div>
          <div className="rounded-[7px] border border-white/8 bg-black/20 p-3">
            <div className="text-[9px] uppercase tracking-[0.18em] text-slate-500">Ended</div>
            <div className="mt-1 text-slate-200">{formatDateTime(identity.wall_clock_window?.end)}</div>
          </div>
          <div className="rounded-[7px] border border-white/8 bg-black/20 p-3">
            <div className="text-[9px] uppercase tracking-[0.18em] text-slate-500">Duration</div>
            <div className="mt-1 text-slate-200">{formatDuration(durationSeconds)}</div>
          </div>
          <div className="rounded-[7px] border border-white/8 bg-black/20 p-3">
            <div className="text-[9px] uppercase tracking-[0.18em] text-slate-500">Execution</div>
            <div className="mt-1 text-slate-200">{formatExecutionModeLabel(identity.execution_mode)}</div>
          </div>
        </div>
      </div>
      <div className="mt-4 grid gap-2 text-xs md:grid-cols-3">
        <HashItem label="Config Hash" value={trust.config_hash || trust.material_config_hash} />
        <HashItem label="Data Snapshot Hash" value={trust.data_snapshot_hash} />
        <HashItem label="Strategy Hash" value={trust.strategy_hash} />
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        <button
          type="button"
          onClick={onRefresh}
          className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100"
        >
          <RefreshCw className="size-3.5" />
          Refresh
        </button>
        <button
          type="button"
          onClick={onExport}
          disabled={exporting}
          className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100 disabled:opacity-50"
        >
          <Download className="size-3.5" />
          {exporting ? 'Exporting...' : 'Export'}
        </button>
        <a
          href={`/bots?runId=${encodeURIComponent(runId || '')}`}
          className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100"
        >
          <ExternalLink className="size-3.5" />
          Open BotLens
        </a>
        <button
          type="button"
          disabled
          title="Run comparison is planned for Phase 3"
          className="inline-flex cursor-not-allowed items-center gap-2 rounded-[7px] border border-white/[0.06] bg-white/[0.02] px-3 py-2 text-xs text-slate-600"
        >
          Compare
        </button>
      </div>
    </header>
  )
}

function HashItem({ label, value }) {
  return (
    <div className="rounded-[7px] border border-white/8 bg-black/20 p-3">
      <div className="text-[9px] uppercase tracking-[0.18em] text-slate-500">{label}</div>
      <div className="mt-1 font-mono text-slate-300" title={value || undefined}>
        {shortHash(value)}
      </div>
    </div>
  )
}

function PerformanceSummaryGrid({ performance = {} }) {
  const cards = [
    ['Net PnL', performance.net_pnl, 'currency', true],
    ['Total Return', performance.total_return_pct, 'percent', true],
    ['Max Drawdown', performance.max_drawdown_pct, 'percent', true],
    ['Sharpe', performance.sharpe, 'number', false],
    ['Sortino', performance.sortino, 'number', false],
    ['Calmar', performance.calmar, 'number', false],
    ['Profit Factor', performance.profit_factor, 'number', false],
    ['Expectancy', performance.expectancy, 'currency', false],
    ['Win Rate', performance.win_rate, 'percent', false],
    ['Trade Count', performance.trade_count, 'integer', false],
    ['Fees', performance.fees, 'currency', false],
    ['Time In Market', performance.time_in_market_pct, 'percent', false],
  ]
  return (
    <SectionShell title="Performance Summary" icon={Activity} description="Backend-computed strategy performance with validity metadata.">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {cards.map(([label, metric, format, emphasis]) => (
          <MetricCard key={label} label={label} metric={metric} format={format} emphasis={emphasis} />
        ))}
      </div>
    </SectionShell>
  )
}

function SymbolBreakdownTable({ rows = [] }) {
  return (
    <SectionShell title="Symbol Breakdown" icon={Database}>
      {rows.length ? (
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-left text-sm text-slate-200">
            <thead>
              <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.16em] text-slate-500">
                <th className="pb-3 pr-4">Symbol</th>
                <th className="pb-3 pr-4 text-right">Trades</th>
                <th className="pb-3 pr-4 text-right">Decisions</th>
                <th className="pb-3 pr-4 text-right">Accepted</th>
                <th className="pb-3 pr-4 text-right">Rejected</th>
                <th className="pb-3 pr-4 text-right">Net PnL</th>
                <th className="pb-3 pr-4 text-right">Fees</th>
                <th className="pb-3 pr-4 text-right">Win Rate</th>
                <th className="pb-3 text-right">Contribution</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.symbol} className="border-b border-white/5">
                  <td className="py-3 pr-4 font-semibold text-slate-100">{row.symbol}</td>
                  <td className="py-3 pr-4 text-right font-mono">{formatNumber(row.trade_count, 0)}</td>
                  <td className="py-3 pr-4 text-right font-mono">{row.decision_count ?? 'Not available'}</td>
                  <td className="py-3 pr-4 text-right font-mono">{row.accepted_decisions ?? 'Not available'}</td>
                  <td className="py-3 pr-4 text-right font-mono">{row.rejected_decisions ?? row.rejection_count ?? 'Not available'}</td>
                  <td className="py-3 pr-4 text-right font-mono">{formatMetricValue(row.net_pnl, 'currency')}</td>
                  <td className="py-3 pr-4 text-right font-mono">{formatMetricValue(row.fees, 'currency')}</td>
                  <td className="py-3 pr-4 text-right font-mono">{formatMetricValue(row.win_rate, 'percent')}</td>
                  <td className="py-3 text-right font-mono">{formatMetricValue(row.contribution_pct, 'percent')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyPanel message="Per-symbol aggregates are not available for this run." />
      )}
    </SectionShell>
  )
}

function KeyValueGrid({ items }) {
  return (
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {items.map((item) => (
        <div key={item.label} className="rounded-[8px] border border-white/8 bg-black/20 p-3">
          <div className="text-[9px] uppercase tracking-[0.18em] text-slate-500">{item.label}</div>
          <div className="mt-1 break-words text-sm text-slate-100">{item.value ?? 'Not available'}</div>
        </div>
      ))}
    </div>
  )
}

function DecisionBehaviorPanel({ behavior = {} }) {
  return (
    <SectionShell title="Decision Behavior" icon={Activity}>
      <KeyValueGrid
        items={[
          { label: 'Signals', value: formatNumber(behavior.total_signals, 0) },
          { label: 'Decisions', value: formatNumber(behavior.total_decisions, 0) },
          { label: 'Accepted', value: formatNumber(behavior.accepted_decisions, 0) },
          { label: 'Rejected', value: formatNumber(behavior.rejected_decisions, 0) },
          { label: 'Entries', value: behavior.entry_count ?? 'Not available' },
          { label: 'Exits', value: behavior.exit_count ?? 'Not available' },
          { label: 'Margin Rejects', value: behavior.margin_rejection_count ?? 'Not available' },
          { label: 'Policy Rejects', value: behavior.position_policy_rejection_count ?? 'Not available' },
        ]}
      />
      <div className="mt-3 grid gap-3 lg:grid-cols-2">
        <DictionaryPanel title="Action Distribution" data={behavior.action_distribution} />
        <DictionaryPanel title="Rejection Reasons" data={behavior.rejection_reasons} />
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Average Holding" metric={behavior.average_holding_period} format="duration" />
        <MetricCard label="Median Holding" metric={behavior.median_holding_period} format="duration" />
        <MetricCard label="Longest Trade" metric={behavior.longest_trade_duration} format="duration" />
        <MetricCard label="Shortest Trade" metric={behavior.shortest_trade_duration} format="duration" />
      </div>
    </SectionShell>
  )
}

function DictionaryPanel({ title, data = {} }) {
  const entries = Object.entries(data || {})
  return (
    <div className="rounded-[8px] border border-white/8 bg-black/20 p-3">
      <div className="text-[10px] uppercase tracking-[0.18em] text-slate-500">{title}</div>
      {entries.length ? (
        <div className="mt-3 space-y-2">
          {entries.map(([key, value]) => (
            <div key={key} className="flex items-center justify-between gap-3 text-sm">
              <span className="break-words text-slate-300">{normalizeLabel(key)}</span>
              <span className="font-mono text-slate-100">{formatNumber(value, 0)}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="mt-3 text-sm text-slate-500">Not available</div>
      )}
    </div>
  )
}

function WalletEvidencePanel({ wallet = {} }) {
  return (
    <SectionShell title="Wallet Evidence" icon={WalletCards}>
      <KeyValueGrid
        items={[
          { label: 'Trace Complete', value: normalizeLabel(wallet.wallet_trace_complete) },
          { label: 'Missing Traces', value: wallet.missing_wallet_trace_count ?? 'Not available' },
          { label: 'Projection Status', value: normalizeLabel(wallet.wallet_projection_status) },
          { label: 'Reservation Leaks', value: Object.keys(wallet.reservation_leaks || {}).length ? 'Present' : 'None reported' },
        ]}
      />
      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <MetricCard label="Final Wallet Value" metric={wallet.final_wallet_value} format="currency" />
        <MetricCard label="Final Cash / Collateral" metric={wallet.final_cash_collateral} format="currency" />
      </div>
      {wallet.margin_warnings?.length ? (
        <WarningList title="Margin Warnings" warnings={wallet.margin_warnings} />
      ) : null}
      {wallet.caveats?.length ? <CaveatLine caveats={wallet.caveats} /> : null}
    </SectionShell>
  )
}

function CoordinatorWaitPanel({ waits = {} }) {
  const available = waits.status && waits.status !== 'not_available'
  return (
    <SectionShell title="Coordinator Waits" icon={Clock3}>
      {available ? (
        <>
          <KeyValueGrid
            items={[
              { label: 'Total Wait', value: formatDuration((waits.total_wait_ms || 0) / 1000) },
              { label: 'Max Wait', value: formatDuration((waits.max_wait_ms || 0) / 1000) },
              { label: 'Wait Count', value: waits.wait_count ?? 'Not available' },
              { label: 'Fail Count', value: waits.fail_count ?? 'Not available' },
            ]}
          />
          {waits.top_waits?.length ? (
            <div className="mt-4 overflow-x-auto">
              <table className="w-full border-collapse text-left text-sm text-slate-200">
                <thead>
                  <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.16em] text-slate-500">
                    <th className="pb-3 pr-4">Candidate</th>
                    <th className="pb-3 pr-4">Bar Time</th>
                    <th className="pb-3 pr-4 text-right">Wait</th>
                    <th className="pb-3 pr-4">Blockers</th>
                    <th className="pb-3 pr-4">First Watermark</th>
                    <th className="pb-3 pr-4">Release Watermark</th>
                    <th className="pb-3">Release</th>
                  </tr>
                </thead>
                <tbody>
                  {waits.top_waits.map((wait, index) => (
                    <tr key={`${wait.candidate_id || wait.decision_id || index}`} className="border-b border-white/5">
                      <td className="py-3 pr-4 font-semibold text-slate-100">
                        {wait.candidate_symbol || 'Unknown'} {formatTimeframe(wait.candidate_timeframe)}
                      </td>
                      <td className="py-3 pr-4 text-xs text-slate-400">{formatDateTime(wait.candidate_bar_time)}</td>
                      <td className="py-3 pr-4 text-right font-mono">{formatDuration((wait.wait_elapsed_ms || 0) / 1000)}</td>
                      <td className="py-3 pr-4 text-xs text-slate-300">{wait.blocker_symbols?.join(', ') || 'Not available'}</td>
                      <td className="py-3 pr-4 text-xs text-slate-400">{formatWatermarks(wait.first_blocker_watermarks)}</td>
                      <td className="py-3 pr-4 text-xs text-slate-400">{formatWatermarks(wait.release_watermarks)}</td>
                      <td className="py-3 text-xs text-slate-300">
                        {normalizeLabel(wait.final_action)} / {normalizeLabel(wait.release_reason)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyPanel message="No top waits were reported." />
          )}
        </>
      ) : (
        <EmptyPanel message="Coordinator wait diagnostics not available for this run." />
      )}
      {waits.caveats?.length ? <CaveatLine caveats={waits.caveats} /> : null}
    </SectionShell>
  )
}

function formatWatermarks(items = []) {
  if (!Array.isArray(items) || !items.length) return 'Not available'
  return items
    .slice(0, 2)
    .map((item) => {
      const symbol = item.symbol || item.participant_symbol || item.participant_key || 'participant'
      const watermark = item.next_bar_time || item.current_bar_time || item.release_next_bar_time || item.next_bar_epoch || 'unknown'
      return `${symbol}: ${watermark}`
    })
    .join(' | ')
}

function OperationalDiagnosticsPanel({ diagnostics = {} }) {
  return (
    <SectionShell title="Operational Diagnostics" icon={AlertTriangle}>
      <KeyValueGrid
        items={[
          { label: 'Operational Drift', value: normalizeLabel(diagnostics.operational_drift_status) },
          { label: 'Diagnostics Status', value: normalizeLabel(diagnostics.diagnostics_degraded_status) },
          { label: 'DB Slow Writes', value: diagnostics.db_slow_write_warning_count ?? 'Not available' },
          { label: 'Operational FP', value: shortHash(diagnostics.operational_fingerprint) },
        ]}
      />
      {diagnostics.telemetry_warnings?.length ? <WarningList title="Telemetry Warnings" warnings={diagnostics.telemetry_warnings} /> : null}
      {diagnostics.step_trace_warnings?.length ? <WarningList title="Step Trace Warnings" warnings={diagnostics.step_trace_warnings} /> : null}
      {diagnostics.botlens_diagnostic_caveats?.length ? <CaveatLine caveats={diagnostics.botlens_diagnostic_caveats} /> : null}
      {diagnostics.caveats?.length ? <CaveatLine caveats={diagnostics.caveats} /> : null}
    </SectionShell>
  )
}

function WarningList({ title, warnings = [] }) {
  return (
    <div className="mt-3 rounded-[8px] border border-amber-500/20 bg-amber-500/10 p-3">
      <div className="text-[10px] uppercase tracking-[0.18em] text-amber-200/80">{title}</div>
      <pre className="mt-2 max-h-52 overflow-auto whitespace-pre-wrap text-xs leading-5 text-amber-50/80">
        {JSON.stringify(warnings, null, 2)}
      </pre>
    </div>
  )
}

function CaveatLine({ caveats = [] }) {
  return (
    <div className="mt-3 rounded-[8px] border border-white/8 bg-black/20 p-3 text-xs leading-5 text-slate-400">
      Caveats: {caveats.join(' | ')}
    </div>
  )
}

function EmptyPanel({ message }) {
  return <div className="rounded-[8px] border border-white/8 bg-black/20 p-4 text-sm text-slate-500">{message}</div>
}

function RawDataDisclosure({ view }) {
  return (
    <details className="rounded-[8px] border border-white/8 bg-[#141923]/80 p-4">
      <summary className="flex cursor-pointer items-center gap-2 text-sm font-semibold text-slate-200">
        <FileJson className="size-4 text-[color:var(--accent-text-soft)]" />
        Raw References
      </summary>
      <pre className="mt-4 max-h-[34rem] overflow-auto rounded-[8px] border border-white/8 bg-black/30 p-3 text-xs leading-5 text-slate-300">
        {JSON.stringify({ raw_refs: view.rawRefs, sections: view.sections, raw: view.raw }, null, 2)}
      </pre>
    </details>
  )
}

function LoadingState() {
  return (
    <div className="rounded-[8px] border border-white/8 bg-[#151924]/80 p-8 text-sm text-slate-400">
      <div className="flex items-center gap-3">
        <RefreshCw className="size-4 animate-spin text-[color:var(--accent-text-soft)]" />
        Loading run report...
      </div>
    </div>
  )
}

function ErrorState({ title, message, onBack, onRetry }) {
  return (
    <div className="rounded-[8px] border border-rose-500/20 bg-rose-500/10 p-6">
      <div className="text-sm font-semibold text-rose-100">{title}</div>
      <div className="mt-2 text-sm text-rose-100/80">{message}</div>
      <div className="mt-4 flex flex-wrap gap-2">
        <button
          type="button"
          onClick={onBack}
          className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-200"
        >
          <ArrowLeft className="size-3.5" />
          Reports
        </button>
        {onRetry ? (
          <button
            type="button"
            onClick={onRetry}
            className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-200"
          >
            <RefreshCw className="size-3.5" />
            Retry
          </button>
        ) : null}
      </div>
    </div>
  )
}

function MaterializationState({ status, onBack, onRefresh }) {
  const normalized = String(status?.status || 'not_started').toLowerCase()
  const building = normalized === 'building'
  const failed = normalized === 'failed'
  const title = building ? 'Report generating...' : failed ? 'Report generation failed' : 'Report not ready'
  const message = failed
    ? status?.error || 'The backend could not materialize this report. Retry will request a rebuild.'
    : 'The run is terminal, but the Run Report v2 artifact is not ready yet.'

  return (
    <div className="rounded-[8px] border border-white/10 bg-[#151924]/80 p-6">
      <div className="flex items-center gap-3">
        {building ? <RefreshCw className="size-4 animate-spin text-[color:var(--accent-text-soft)]" /> : <Clock3 className="size-4 text-[color:var(--accent-text-soft)]" />}
        <div>
          <div className="text-sm font-semibold text-slate-100">{title}</div>
          <div className="mt-1 text-sm text-slate-400">{message}</div>
        </div>
      </div>
      <div className="mt-4 grid gap-2 text-xs text-slate-500 sm:grid-cols-2 lg:grid-cols-4">
        <div>Status: {normalizeLabel(status?.status)}</div>
        <div>Started: {formatDateTime(status?.started_at)}</div>
        <div>Built: {formatDateTime(status?.built_at)}</div>
        <div>Duration: {status?.duration_ms ? `${formatNumber(Number(status.duration_ms) / 1000, 1)}s` : 'Not available'}</div>
      </div>
      <div className="mt-5 flex flex-wrap gap-2">
        <button
          type="button"
          onClick={onBack}
          className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-200"
        >
          <ArrowLeft className="size-3.5" />
          Reports
        </button>
        <button
          type="button"
          onClick={onRefresh}
          className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-200"
        >
          <RefreshCw className="size-3.5" />
          Refresh
        </button>
      </div>
    </div>
  )
}

export function RunReportPage({ runId, onBack }) {
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [exporting, setExporting] = useState(false)

  const loadReport = useCallback(async (options = {}) => {
    if (!runId) return
    setLoading(true)
    setError(null)
    try {
      const nextPayload = await reportService.getRunReport(runId, options)
      setPayload(nextPayload)
    } catch (err) {
      setError(err)
    } finally {
      setLoading(false)
    }
  }, [runId])

  useEffect(() => {
    loadReport({ force: true })
  }, [loadReport])

  const view = useMemo(() => (payload ? runReportView(payload) : null), [payload])
  const materializationStatus = payload?.schema_version === 'run_report_materialization_status.v1' ? payload.report_status : null
  const durationSeconds = useMemo(() => {
    if (!view?.identity?.wall_clock_window?.start || !view?.identity?.wall_clock_window?.end) return null
    const start = new Date(view.identity.wall_clock_window.start).getTime()
    const end = new Date(view.identity.wall_clock_window.end).getTime()
    if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return null
    return Math.round((end - start) / 1000)
  }, [view])

  const handleExport = useCallback(async () => {
    if (!runId || exporting) return
    setExporting(true)
    try {
      const { blob, filename } = await reportService.exportReport(runId, {})
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename || `run_${runId}_report_export.zip`
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
    } finally {
      setExporting(false)
    }
  }, [exporting, runId])

  if (loading) return <LoadingState />

  if (error) {
    const detail = error.payload?.detail && typeof error.payload.detail === 'object' ? error.payload.detail : null
    const title =
      detail?.code === 'run_not_terminal'
        ? 'Run is still active'
        : error.status === 404
          ? 'Report not found'
          : error.status === 409
            ? 'Report not available'
            : 'Report load failed'
    return <ErrorState title={title} message={error.message || 'Unable to load report.'} onBack={onBack} onRetry={() => loadReport({ force: true })} />
  }

  if (materializationStatus) {
    return <MaterializationState status={materializationStatus} onBack={onBack} onRefresh={() => loadReport({ force: true })} />
  }

  if (!view?.supported) {
    return (
      <ErrorState
        title="Unsupported report contract"
        message={`Expected run_report_v2, received ${view?.contractVersion || 'unknown'}.`}
        onBack={onBack}
        onRetry={() => loadReport({ force: true })}
      />
    )
  }

  return (
    <div className="space-y-5">
      <ReportIdentityHeader
        view={view}
        durationSeconds={durationSeconds}
        onBack={onBack}
        onRefresh={() => loadReport({ force: true })}
        onExport={handleExport}
        exporting={exporting}
      />
      <TrustStrip trust={view.trust} />
      <PerformanceSummaryGrid performance={view.performance} />
      <SymbolBreakdownTable rows={view.symbolBreakdown} />
      <DecisionBehaviorPanel behavior={view.behavior} />
      <WalletEvidencePanel wallet={view.wallet} />
      <CoordinatorWaitPanel waits={view.coordinatorWaits} />
      <OperationalDiagnosticsPanel diagnostics={view.operationalDiagnostics} />
      <RawDataDisclosure view={view} />
    </div>
  )
}
