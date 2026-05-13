import { useCallback, useEffect, useMemo, useState } from 'react'
import { AlertTriangle, GitCompare, X } from 'lucide-react'
import { reportService } from '../../services/reportService.js'
import { Badge } from '../ui/Badge.jsx'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters.js'
import LoadingOverlay from '../LoadingOverlay.jsx'

const formatDelta = (value, formatter) => {
  if (value === null || value === undefined) return '--'
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '--'
  const formatted = formatter ? formatter(numeric) : formatNumber(numeric, 2)
  return numeric > 0 ? `+${formatted}` : formatted
}

const MetricDeltaRow = ({ label, value, formatter, preferLower = false }) => {
  const numeric = Number(value)
  const hasValue = Number.isFinite(numeric)
  const better = preferLower ? numeric < 0 : numeric > 0
  const tone = hasValue && numeric !== 0 ? (better ? 'text-emerald-300' : 'text-rose-300') : 'text-slate-200'
  return (
    <tr className="border-b border-white/5 text-xs">
      <td className="py-2 pr-3 text-slate-500">{label}</td>
      <td className={`py-2 text-right font-mono ${tone}`}>{formatDelta(value, formatter)}</td>
    </tr>
  )
}

export function CompareModal({ runIds, open, onClose }) {
  const [comparison, setComparison] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchComparison = useCallback(async () => {
    if (!runIds?.length) return
    setLoading(true)
    setError(null)
    try {
      const payload = await reportService.compareReports(runIds)
      setComparison(payload)
    } catch (err) {
      setError(err?.message || 'Failed to load comparison')
    } finally {
      setLoading(false)
    }
  }, [runIds])

  useEffect(() => {
    if (open && runIds?.length >= 2) {
      fetchComparison()
    }
  }, [fetchComparison, open, runIds])

  const baseline = useMemo(() => comparison?.reports?.[0], [comparison])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/80 px-4 py-8 backdrop-blur-sm">
      <div className="w-full max-w-5xl rounded-[8px] border border-white/10 bg-[#0d1117] shadow-2xl">
        <header className="sticky top-0 z-10 flex flex-wrap items-start justify-between gap-4 rounded-t-[8px] border-b border-white/10 bg-[#0d1117]/95 p-5 backdrop-blur">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <GitCompare className="size-4 text-[color:var(--accent-text-soft)]" />
              <span className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Compare Runs</span>
              <Badge variant={comparison?.status === 'ready' ? 'success' : 'warning'} size="sm">
                {comparison?.status || 'loading'}
              </Badge>
            </div>
            <h3 className="mt-2 truncate text-xl font-semibold text-slate-100">
              {baseline?.metadata?.strategy_name || `${runIds?.length || 0} selected runs`}
            </h3>
            <div className="mt-1 font-mono text-xs text-slate-500">{(runIds || []).join(' / ')}</div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-10 w-10 items-center justify-center rounded-[6px] border border-white/10 bg-white/5 text-slate-300 transition hover:border-white/20"
            aria-label="Close comparison"
          >
            <X className="size-4" />
          </button>
        </header>

        {loading ? (
          <div className="relative h-80">
            <LoadingOverlay message="Checking comparison readiness..." />
          </div>
        ) : error ? (
          <div className="p-6">
            <div className="rounded-[6px] border border-rose-500/20 bg-rose-500/10 p-4 text-sm text-rose-300">{error}</div>
            <button
              type="button"
              onClick={fetchComparison}
              className="mt-4 rounded-[6px] border border-white/10 bg-white/5 px-4 py-2 text-xs text-slate-200"
            >
              Retry
            </button>
          </div>
        ) : comparison?.status === 'blocked' ? (
          <div className="space-y-4 p-5">
            <div className="rounded-[6px] border border-amber-500/25 bg-amber-500/10 p-4">
              <div className="flex items-center gap-2 text-amber-100">
                <AlertTriangle className="size-4" />
                <span className="text-sm font-medium">Comparison blocked</span>
              </div>
              <div className="mt-2 text-sm text-amber-100/80">
                Comparison deltas are withheld until readiness and compatibility checks pass.
              </div>
            </div>
            <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
              <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">Blocking Reasons</div>
              <div className="space-y-2">
                {(comparison.blocked_reasons || []).map((reason, index) => (
                  <div key={`${reason.code || 'reason'}-${index}`} className="rounded-[6px] border border-white/8 bg-black/20 p-3">
                    <div className="font-mono text-xs text-slate-200">{reason.code || 'blocked'}</div>
                    <div className="mt-1 text-sm text-slate-400">{reason.message || 'Comparison is blocked.'}</div>
                    {reason.run_id ? <div className="mt-1 font-mono text-[11px] text-slate-600">{reason.run_id}</div> : null}
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="space-y-5 p-5">
            {(comparison?.comparisons || []).map((entry) => (
              <div key={entry.compare_run_id} className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                  {entry.base_run_id?.slice(0, 8)} vs {entry.compare_run_id?.slice(0, 8)}
                </div>
                <table className="w-full border-collapse">
                  <tbody>
                    <MetricDeltaRow label="Net PnL" value={entry.summary_delta?.net_pnl} formatter={formatCurrency} />
                    <MetricDeltaRow label="Gross PnL" value={entry.summary_delta?.gross_pnl} formatter={formatCurrency} />
                    <MetricDeltaRow label="Fees" value={entry.summary_delta?.fees} formatter={formatCurrency} preferLower />
                    <MetricDeltaRow label="Return" value={entry.summary_delta?.return_pct} formatter={(v) => formatPercent(v, 2)} />
                    <MetricDeltaRow label="Max Drawdown" value={entry.summary_delta?.max_drawdown_pct} formatter={(v) => formatPercent(v, 2)} preferLower />
                    <MetricDeltaRow label="Profit Factor" value={entry.summary_delta?.profit_factor} formatter={(v) => formatNumber(v, 2)} />
                    <MetricDeltaRow label="Expectancy" value={entry.summary_delta?.expectancy} formatter={formatCurrency} />
                    <MetricDeltaRow label="Closed Trades" value={entry.summary_delta?.closed_trades} formatter={(v) => formatNumber(v, 0)} />
                  </tbody>
                </table>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
