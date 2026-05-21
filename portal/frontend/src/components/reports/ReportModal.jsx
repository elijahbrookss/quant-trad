import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Activity, AlertTriangle, BarChart3, Database, Download, LineChart, RefreshCw, Table2, X } from 'lucide-react'
import LoadingOverlay from '../LoadingOverlay.jsx'
import { formatCurrency, formatNumber, formatPercent, formatTimeframe } from '../../utils/formatters.js'
import { reportService } from '../../services/reportService.js'
import { SemanticStatusBadge } from '../ui/StatusBadge.jsx'
import { formatExecutionModeLabel } from '../../features/bots/executionMode.js'
import { reportSummaryView, sectionByName } from './reportContractViewModel.js'

const formatDateTime = (value) => {
  if (!value) return '--'
  try {
    return new Date(value).toLocaleDateString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  } catch {
    return value
  }
}

const TabButton = ({ active, children, onClick }) => (
  <button
    type="button"
    onClick={onClick}
    className={`rounded-[6px] px-3 py-2 text-sm transition ${
      active ? 'bg-white/10 text-slate-100' : 'text-slate-400 hover:bg-white/5 hover:text-slate-200'
    }`}
  >
    {children}
  </button>
)

const MetricTile = ({ label, value, tone = 'neutral' }) => {
  const toneClass = {
    positive: 'text-emerald-300',
    negative: 'text-rose-300',
    warning: 'text-amber-300',
    neutral: 'text-slate-100',
  }[tone]
  return (
    <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-3">
      <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">{label}</div>
      <div className={`mt-1 text-lg font-semibold tabular-nums ${toneClass}`}>{value}</div>
    </div>
  )
}

const SectionState = ({ label, section }) => {
  const available = section?.available !== false
  return (
    <div className="flex items-center justify-between border-b border-white/5 py-2 text-xs">
      <span className="text-slate-400">{label}</span>
      <span className={available ? 'text-emerald-300' : 'text-amber-300'}>
        {available ? `${formatNumber(section?.row_count, 0)} rows` : section?.reason || 'Unavailable'}
      </span>
    </div>
  )
}

const DetailRow = ({ label, value }) => (
  <div className="flex items-center justify-between border-b border-white/5 py-2 text-xs">
    <span className="text-slate-400">{label}</span>
    <span className="text-slate-200">{value}</span>
  </div>
)

const DiagnosticsPanel = ({ diagnostics }) => {
  const items = diagnostics?.items || []
  if (!items.length) {
    return <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4 text-sm text-slate-400">No diagnostics for this report.</div>
  }
  return (
    <div className="space-y-3">
      {items.map((item, index) => {
        const severity = String(item.severity || 'info').toLowerCase()
        const tone =
          severity === 'critical'
            ? 'border-rose-500/30 bg-rose-500/10 text-rose-100'
            : severity === 'warning'
              ? 'border-amber-500/30 bg-amber-500/10 text-amber-100'
              : 'border-sky-500/25 bg-sky-500/10 text-sky-100'
        return (
          <div key={`${item.code || 'diagnostic'}-${index}`} className={`rounded-[6px] border p-3 ${tone}`}>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="font-mono text-xs uppercase">{item.code || 'diagnostic'}</div>
              <div className="text-[10px] uppercase tracking-[0.18em] opacity-75">{item.source || 'report'}</div>
            </div>
            <div className="mt-2 text-sm">{item.message}</div>
            {item.suggested_next_step ? <div className="mt-2 text-xs opacity-80">{item.suggested_next_step}</div> : null}
          </div>
        )
      })}
    </div>
  )
}

const TradeRows = ({ page }) => {
  const rows = page?.items || []
  if (!rows.length) return <div className="py-8 text-center text-sm text-slate-500">No trades available.</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-xs text-slate-200">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.18em] text-slate-500">
            <th className="pb-2 pr-3 text-left">Exit</th>
            <th className="pb-2 pr-3 text-left">Instrument</th>
            <th className="pb-2 pr-3 text-left">Side</th>
            <th className="pb-2 pr-3 text-right">Gross</th>
            <th className="pb-2 pr-3 text-right">Fees</th>
            <th className="pb-2 text-right">Net</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((trade) => {
            const net = Number(trade.net_pnl || 0)
            return (
              <tr key={trade.trade_id} className="border-b border-white/5">
                <td className="py-2 pr-3 text-slate-400">{formatDateTime(trade.exit_time || trade.entry_time)}</td>
                <td className="py-2 pr-3 font-mono">{trade.symbol || trade.instrument_id || '--'}</td>
                <td className="py-2 pr-3 text-slate-400">{trade.side || '--'}</td>
                <td className="py-2 pr-3 text-right font-mono">{formatCurrency(trade.gross_pnl)}</td>
                <td className="py-2 pr-3 text-right font-mono text-slate-400">{formatCurrency(trade.fees_paid)}</td>
                <td className={`py-2 text-right font-mono ${net >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                  {formatCurrency(trade.net_pnl)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const DecisionRows = ({ page }) => {
  const rows = page?.items || []
  if (!rows.length) return <div className="py-8 text-center text-sm text-slate-500">No decisions available.</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-xs text-slate-200">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.18em] text-slate-500">
            <th className="pb-2 pr-3 text-left">Bar</th>
            <th className="pb-2 pr-3 text-left">Instrument</th>
            <th className="pb-2 pr-3 text-left">Rule</th>
            <th className="pb-2 pr-3 text-left">State</th>
            <th className="pb-2 text-left">Reason</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((decision) => (
            <tr key={decision.decision_id} className="border-b border-white/5">
              <td className="py-2 pr-3 text-slate-400">{formatDateTime(decision.bar_time)}</td>
              <td className="py-2 pr-3 font-mono">{decision.symbol || decision.instrument_id || '--'}</td>
              <td className="py-2 pr-3">{decision.rule_name || decision.rule_id || '--'}</td>
              <td className={`py-2 pr-3 ${decision.accepted ? 'text-emerald-300' : 'text-amber-300'}`}>
                {decision.accepted ? 'accepted' : 'rejected'}
              </td>
              <td className="py-2 text-slate-400">{decision.reason_code || '--'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const SmallRowsTable = ({ rows = [], columns = [], empty = 'No rows available.' }) => {
  if (!rows.length) return <div className="py-8 text-center text-sm text-slate-500">{empty}</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-xs text-slate-200">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.18em] text-slate-500">
            {columns.map((column) => (
              <th key={column.key} className={`pb-2 pr-3 ${column.align === 'right' ? 'text-right' : 'text-left'}`}>{column.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={row.id || row.trade_id || row.decision_id || row.signal_id || `${row.timestamp || 'row'}-${index}`} className="border-b border-white/5">
              {columns.map((column) => (
                <td key={column.key} className={`py-2 pr-3 ${column.align === 'right' ? 'text-right' : 'text-left'} ${column.mono ? 'font-mono' : ''}`}>
                  {column.render ? column.render(row) : row[column.key] ?? '--'}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const StatusLine = ({ label, value }) => (
  <div className="flex items-center justify-between border-b border-white/5 py-2 text-xs">
    <span className="text-slate-400">{label}</span>
    <span className="font-mono text-slate-200">{value ?? '--'}</span>
  </div>
)

export function ReportModal({ runId, open, onClose }) {
  const [readiness, setReadiness] = useState(null)
  const [summary, setSummary] = useState(null)
  const [sections, setSections] = useState(null)
  const [diagnostics, setDiagnostics] = useState(null)
  const [trades, setTrades] = useState(null)
  const [decisions, setDecisions] = useState(null)
  const [signals, setSignals] = useState(null)
  const [timeseries, setTimeseries] = useState(null)
  const [contextRows, setContextRows] = useState(null)
  const [candleCatalog, setCandleCatalog] = useState(null)
  const [operationalHealth, setOperationalHealth] = useState(null)
  const [exportManifest, setExportManifest] = useState(null)
  const [loading, setLoading] = useState(false)
  const [sectionLoading, setSectionLoading] = useState(false)
  const [manifestLoading, setManifestLoading] = useState(false)
  const [error, setError] = useState(null)
  const [activeTab, setActiveTab] = useState('summary')
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState(null)
  const requestSeqRef = useRef(0)

  const loadReport = useCallback(async (options = {}) => {
    if (!runId) return
    const requestId = requestSeqRef.current + 1
    requestSeqRef.current = requestId
    setLoading(true)
    setSectionLoading(false)
    setManifestLoading(false)
    setError(null)
    setExportError(null)
    try {
      const requestOptions = options.force ? { force: true } : undefined
      const nextReadiness = await reportService.getReportReadiness(runId, requestOptions)
      if (requestSeqRef.current !== requestId) return
      setReadiness(nextReadiness)
      setLoading(false)
      setSectionLoading(true)
      const [nextSummary, nextSections, nextDiagnostics] =
        await Promise.all([
          reportService.getReportSummary(runId, requestOptions),
          reportService.getReportSections(runId, requestOptions),
          reportService.getReportDiagnostics(runId, requestOptions),
        ])
      if (requestSeqRef.current !== requestId) return
      setSummary(nextSummary)
      setSections(nextSections)
      setDiagnostics(nextDiagnostics)
    } catch (err) {
      if (requestSeqRef.current !== requestId) return
      setError(err?.message || 'Failed to load report')
    } finally {
      if (requestSeqRef.current === requestId) {
        setLoading(false)
        setSectionLoading(false)
      }
    }
  }, [runId])

  useEffect(() => {
    if (open) {
      setActiveTab('summary')
      setReadiness(null)
      setSummary(null)
      setSections(null)
      setDiagnostics(null)
      setTrades(null)
      setDecisions(null)
      setSignals(null)
      setTimeseries(null)
      setContextRows(null)
      setCandleCatalog(null)
      setOperationalHealth(null)
      setExportManifest(null)
      loadReport()
    }
    return () => {
      requestSeqRef.current += 1
    }
  }, [loadReport, open])

  const view = useMemo(() => reportSummaryView(summary || { run_id: runId, readiness: readiness || {} }), [readiness, runId, summary])
  const sectionsByName = useMemo(() => sectionByName(sections || summary?.sections), [sections, summary])

  const loadTradePage = useCallback(async () => {
    if (!runId || trades) return
    const requestId = requestSeqRef.current
    setSectionLoading(true)
    try {
      const nextTrades = await reportService.getTradeDataset(runId, { limit: 100 })
      if (requestSeqRef.current === requestId) setTrades(nextTrades)
    } catch (err) {
      if (requestSeqRef.current === requestId) setError(err?.message || 'Failed to load trades')
    } finally {
      if (requestSeqRef.current === requestId) setSectionLoading(false)
    }
  }, [runId, trades])

  const loadDecisionTrace = useCallback(async () => {
    if (!runId || (decisions && signals)) return
    const requestId = requestSeqRef.current
    setSectionLoading(true)
    try {
      const [nextDecisions, nextSignals] = await Promise.all([
        decisions ? Promise.resolve(decisions) : reportService.getDecisionDataset(runId, { limit: 100 }),
        signals ? Promise.resolve(signals) : reportService.getSignalDataset(runId, { limit: 100 }),
      ])
      if (requestSeqRef.current === requestId) {
        setDecisions(nextDecisions)
        setSignals(nextSignals)
      }
    } catch (err) {
      if (requestSeqRef.current === requestId) setError(err?.message || 'Failed to load decision trace')
    } finally {
      if (requestSeqRef.current === requestId) setSectionLoading(false)
    }
  }, [decisions, runId, signals])

  const loadPerformanceData = useCallback(async () => {
    if (!runId || timeseries) return
    const requestId = requestSeqRef.current
    setSectionLoading(true)
    try {
      const [equityCurve, drawdownCurve, returnsSeries, rollingExpectancy] = await Promise.all([
        reportService.getTimeseriesDataset(runId, 'equity_curve', { limit: 200 }),
        reportService.getTimeseriesDataset(runId, 'drawdown_curve', { limit: 200 }),
        reportService.getTimeseriesDataset(runId, 'returns_series', { limit: 200 }),
        reportService.getTimeseriesDataset(runId, 'rolling_expectancy', { limit: 200 }),
      ])
      if (requestSeqRef.current === requestId) {
        setTimeseries({ equityCurve, drawdownCurve, returnsSeries, rollingExpectancy })
      }
    } catch (err) {
      if (requestSeqRef.current === requestId) setError(err?.message || 'Failed to load performance data')
    } finally {
      if (requestSeqRef.current === requestId) setSectionLoading(false)
    }
  }, [runId, timeseries])

  const loadContextData = useCallback(async () => {
    if (!runId || contextRows) return
    const requestId = requestSeqRef.current
    setSectionLoading(true)
    try {
      const [decisionContext, tradeContext, indicatorSnapshots, marketState] = await Promise.all([
        reportService.getContextDataset(runId, { section: 'decision_context', limit: 100 }),
        reportService.getContextDataset(runId, { section: 'trade_context', limit: 100 }),
        reportService.getContextDataset(runId, { section: 'indicator_snapshots', limit: 100 }),
        reportService.getContextDataset(runId, { section: 'market_state', limit: 100 }),
      ])
      if (requestSeqRef.current === requestId) {
        setContextRows({ decisionContext, tradeContext, indicatorSnapshots, marketState })
      }
    } catch (err) {
      if (requestSeqRef.current === requestId) setError(err?.message || 'Failed to load context data')
    } finally {
      if (requestSeqRef.current === requestId) setSectionLoading(false)
    }
  }, [contextRows, runId])

  const loadCandleCatalog = useCallback(async () => {
    if (!runId || candleCatalog) return
    const requestId = requestSeqRef.current
    setSectionLoading(true)
    try {
      const nextCatalog = await reportService.getCandleCatalog(runId)
      if (requestSeqRef.current === requestId) setCandleCatalog(nextCatalog)
    } catch (err) {
      if (requestSeqRef.current === requestId) setError(err?.message || 'Failed to load candle catalog')
    } finally {
      if (requestSeqRef.current === requestId) setSectionLoading(false)
    }
  }, [candleCatalog, runId])

  const loadOperationalHealth = useCallback(async () => {
    if (!runId || operationalHealth) return
    const requestId = requestSeqRef.current
    setSectionLoading(true)
    try {
      const nextHealth = await reportService.getOperationalHealth(runId)
      if (requestSeqRef.current === requestId) setOperationalHealth(nextHealth)
    } catch (err) {
      if (requestSeqRef.current === requestId) setError(err?.message || 'Failed to load operational health')
    } finally {
      if (requestSeqRef.current === requestId) setSectionLoading(false)
    }
  }, [operationalHealth, runId])

  const loadExportManifest = useCallback(async () => {
    if (!runId || exportManifest || manifestLoading) return
    const requestId = requestSeqRef.current
    setManifestLoading(true)
    setExportError(null)
    try {
      const nextManifest = await reportService.getExportManifest(runId)
      if (requestSeqRef.current === requestId) setExportManifest(nextManifest)
    } catch (err) {
      if (requestSeqRef.current === requestId) setExportError(err?.message || 'Failed to load export manifest')
    } finally {
      if (requestSeqRef.current === requestId) setManifestLoading(false)
    }
  }, [exportManifest, manifestLoading, runId])

  const handleSelectTab = useCallback(
    (tab) => {
      setActiveTab(tab)
      if (tab === 'trades') {
        void loadTradePage()
      } else if (tab === 'decisions') {
        void loadDecisionTrace()
      } else if (tab === 'performance') {
        void loadPerformanceData()
      } else if (tab === 'context') {
        void loadContextData()
      } else if (tab === 'candles') {
        void loadCandleCatalog()
      } else if (tab === 'operations') {
        void loadOperationalHealth()
      } else if (tab === 'export') {
        void loadExportManifest()
      }
    },
    [loadCandleCatalog, loadContextData, loadDecisionTrace, loadExportManifest, loadOperationalHealth, loadPerformanceData, loadTradePage],
  )

  const handleExport = useCallback(async ({ includeCandles = false } = {}) => {
    if (!runId || exporting) return
    setExporting(true)
    setExportError(null)
    try {
      const { blob, filename } = await reportService.exportReport(runId, { include_candles: includeCandles })
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename || `run_${runId}_report_export.zip`
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(url)
    } catch (err) {
      setExportError(err?.message || 'Failed to export report')
    } finally {
      setExporting(false)
    }
  }, [exporting, runId])

  if (!open) return null

  const pnlTone = Number(view.netPnl || 0) > 0 ? 'positive' : Number(view.netPnl || 0) < 0 ? 'negative' : 'neutral'
  const returnTone = Number(view.returnPct || 0) > 0 ? 'positive' : Number(view.returnPct || 0) < 0 ? 'negative' : 'neutral'

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/80 px-4 py-8 backdrop-blur-sm">
      <div className="relative w-full max-w-6xl rounded-[8px] border border-white/10 bg-[#0d1117] shadow-2xl">
        <header className="sticky top-0 z-10 flex flex-wrap items-start justify-between gap-4 rounded-t-[8px] border-b border-white/10 bg-[#0d1117]/95 p-5 backdrop-blur">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Run Report</span>
              <SemanticStatusBadge kind="report" value={view.reportStatus} />
              <SemanticStatusBadge kind="comparison" value={view.comparisonStatus} />
            </div>
            <h3 className="mt-2 truncate text-xl font-semibold text-slate-100">
              {view.metadata?.strategy_name || 'Strategy'} <span className="text-slate-600">/</span> {view.metadata?.run_id || runId}
            </h3>
            <div className="mt-1 flex flex-wrap items-center gap-3 text-xs text-slate-400">
              <span>{formatTimeframe(view.timeframe)}</span>
              <span>{formatExecutionModeLabel(view.executionMode)}</span>
              <span>{(view.symbols || []).join(', ') || '--'}</span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              disabled={exporting || !runId}
              onClick={handleExport}
              className="inline-flex items-center gap-2 rounded-[6px] border border-white/10 bg-white/5 px-4 py-2.5 text-xs text-slate-200 transition hover:border-white/20 disabled:opacity-50"
            >
              {exporting ? <RefreshCw className="size-3.5 animate-spin" /> : <Download className="size-3.5" />}
              {exporting ? 'Exporting' : 'Export'}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-10 w-10 items-center justify-center rounded-[6px] border border-white/10 bg-white/5 text-slate-300 transition hover:border-white/20"
              aria-label="Close report"
            >
              <X className="size-4" />
            </button>
          </div>
        </header>

        {exportError ? (
          <div className="border-b border-rose-500/20 bg-rose-500/10 px-5 py-3 text-sm text-rose-200">{exportError}</div>
        ) : null}

        {loading ? (
          <div className="relative h-80">
            <LoadingOverlay message="Loading report readiness..." />
          </div>
        ) : error ? (
          <div className="p-6">
            <div className="rounded-[6px] border border-rose-500/20 bg-rose-500/10 p-4 text-sm text-rose-300">{error}</div>
            <button
              type="button"
              onClick={() => loadReport({ force: true })}
              className="mt-4 rounded-[6px] border border-white/10 bg-white/5 px-4 py-2 text-xs text-slate-200"
            >
              Retry
            </button>
          </div>
        ) : (
          <>
            <div className="border-b border-white/5 px-5">
              <div className="flex flex-wrap gap-1">
                <TabButton active={activeTab === 'summary'} onClick={() => handleSelectTab('summary')}>Summary</TabButton>
                <TabButton active={activeTab === 'performance'} onClick={() => handleSelectTab('performance')}>Performance</TabButton>
                <TabButton active={activeTab === 'trades'} onClick={() => handleSelectTab('trades')}>Trades</TabButton>
                <TabButton active={activeTab === 'decisions'} onClick={() => handleSelectTab('decisions')}>Decisions</TabButton>
                <TabButton active={activeTab === 'context'} onClick={() => handleSelectTab('context')}>Context</TabButton>
                <TabButton active={activeTab === 'candles'} onClick={() => handleSelectTab('candles')}>Candles</TabButton>
                <TabButton active={activeTab === 'diagnostics'} onClick={() => handleSelectTab('diagnostics')}>Diagnostics</TabButton>
                <TabButton active={activeTab === 'operations'} onClick={() => handleSelectTab('operations')}>Operations</TabButton>
                <TabButton active={activeTab === 'export'} onClick={() => handleSelectTab('export')}>Export</TabButton>
              </div>
            </div>

            {sectionLoading ? (
              <div className="border-b border-white/5 px-5 py-2 text-xs text-slate-500">Loading selected report data...</div>
            ) : null}

            <div className="p-5">
              {activeTab === 'summary' ? (
                <div className="space-y-5">
                  <div className="grid gap-3 rounded-[6px] border border-white/8 bg-white/[0.03] p-4 md:grid-cols-3">
                    <StatusLine label="Dataset" value={readiness?.dataset_status || '--'} />
                    <StatusLine label="Results" value={readiness?.results_status || '--'} />
                    <StatusLine label="Comparison" value={readiness?.comparison_status || '--'} />
                    <StatusLine label="Export" value={readiness?.export_status || '--'} />
                    <StatusLine label="Data quality" value={readiness?.data_quality_status || '--'} />
                    <StatusLine label="Execution quality" value={readiness?.execution_quality_status || '--'} />
                  </div>
                  {(readiness?.caveats || []).length ? (
                    <div className="rounded-[6px] border border-amber-500/25 bg-amber-500/10 p-3 text-xs text-amber-100">
                      {(readiness.caveats || []).slice(0, 4).join(' | ')}
                    </div>
                  ) : null}
                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    <MetricTile label="Net PnL" value={formatCurrency(view.netPnl)} tone={pnlTone} />
                    <MetricTile label="Return" value={formatPercent(view.returnPct, 2)} tone={returnTone} />
                    <MetricTile label="Equity" value={`${formatCurrency(view.equityStart)} → ${formatCurrency(view.equityEnd)}`} />
                    <MetricTile label="Fees" value={formatCurrency(view.fees)} tone="warning" />
                  </div>
                  <div className="grid gap-4 lg:grid-cols-3">
                    <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                      <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        <BarChart3 className="size-3.5" />
                        Performance
                      </div>
                      <DetailRow label="Max drawdown" value={formatPercent(view.maxDrawdownPct, 2)} />
                      <DetailRow label="Sharpe" value={formatNumber(view.sharpe, 2)} />
                      <DetailRow label="Sortino" value={formatNumber(view.sortino, 2)} />
                      <DetailRow label="Calmar" value={formatNumber(view.calmar, 2)} />
                      <DetailRow label="Annualized vol" value={formatPercent(view.annualizedVolatility, 2)} />
                      <DetailRow label="Exposure" value={formatPercent(view.exposurePct, 2)} />
                      <DetailRow label="Win rate" value={formatPercent(view.winRate, 1)} />
                      <DetailRow label="Profit factor" value={formatNumber(view.profitFactor, 2)} />
                      <DetailRow label="Expectancy" value={formatCurrency(view.expectancy)} />
                    </div>
                    <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                      <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        <Table2 className="size-3.5" />
                        Trace Counts
                      </div>
                      <SectionState label="Trades" section={sectionsByName.get('trades')} />
                      <SectionState label="Decisions" section={sectionsByName.get('decisions')} />
                      <SectionState label="Signals" section={sectionsByName.get('signals')} />
                      <SectionState label="Diagnostics" section={sectionsByName.get('diagnostics')} />
                    </div>
                    <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                      <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        <AlertTriangle className="size-3.5" />
                        Readiness
                      </div>
                      <DetailRow label="Results" value={readiness?.results_ready ? 'Ready' : readiness?.reason || 'Unavailable'} />
                      <DetailRow label="Comparison" value={readiness?.safe_to_compare ? 'Eligible' : readiness?.reason || 'Blocked'} />
                      <SectionState label="Export" section={sectionsByName.get('export')} />
                      <SectionState label="Data quality" section={sectionsByName.get('data_quality')} />
                    </div>
                  </div>
                </div>
              ) : null}

              {activeTab === 'performance' ? (
                <div className="grid gap-4 lg:grid-cols-2">
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                      <LineChart className="size-3.5" />
                      Equity Curve
                    </div>
                    <SmallRowsTable
                      rows={timeseries?.equityCurve?.items || []}
                      columns={[
                        { key: 'timestamp', label: 'Time' },
                        { key: 'symbol', label: 'Instrument', mono: true },
                        { key: 'equity', label: 'Equity', align: 'right', render: (row) => formatCurrency(row.equity ?? row.value) },
                      ]}
                      empty="No equity curve rows available."
                    />
                  </div>
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                      <BarChart3 className="size-3.5" />
                      Rolling Expectancy
                    </div>
                    <SmallRowsTable
                      rows={timeseries?.rollingExpectancy?.items || []}
                      columns={[
                        { key: 'timestamp', label: 'Time' },
                        { key: 'trade_id', label: 'Trade', mono: true },
                        { key: 'value', label: 'Expectancy', align: 'right', render: (row) => formatCurrency(row.value) },
                      ]}
                      empty="No rolling expectancy rows available."
                    />
                  </div>
                </div>
              ) : null}

              {activeTab === 'trades' ? (
                sectionLoading && !trades ? (
                  <div className="py-8 text-center text-sm text-slate-500">Loading trades...</div>
                ) : (
                  <TradeRows page={trades} />
                )
              ) : null}

              {activeTab === 'decisions' ? (
                <div className="space-y-5">
                  {sectionLoading && !decisions ? (
                    <div className="py-8 text-center text-sm text-slate-500">Loading decisions...</div>
                  ) : (
                    <DecisionRows page={decisions} />
                  )}
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">Signal Traceability</div>
                    <div className="text-sm text-slate-300">
                      {formatNumber(signals?.total || 0, 0)} signals available through the signal dataset.
                    </div>
                  </div>
                </div>
              ) : null}

              {activeTab === 'context' ? (
                <div className="grid gap-4 lg:grid-cols-2">
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                      <Database className="size-3.5" />
                      Decision Context
                    </div>
                    <SmallRowsTable
                      rows={contextRows?.decisionContext?.items || []}
                      columns={[
                        { key: 'bar_time', label: 'Bar' },
                        { key: 'symbol', label: 'Instrument', mono: true },
                        { key: 'status', label: 'Status' },
                        { key: 'reason', label: 'Reason' },
                      ]}
                      empty="No decision context rows available."
                    />
                  </div>
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">World State</div>
                    <StatusLine label="Indicator snapshots" value={formatNumber(contextRows?.indicatorSnapshots?.total || 0, 0)} />
                    <StatusLine label="Market state rows" value={formatNumber(contextRows?.marketState?.total || 0, 0)} />
                    <StatusLine label="Trade context rows" value={formatNumber(contextRows?.tradeContext?.total || 0, 0)} />
                  </div>
                </div>
              ) : null}

              {activeTab === 'candles' ? (
                <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                  <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                    <Database className="size-3.5" />
                    Candle Catalog
                  </div>
                  <SmallRowsTable
                    rows={candleCatalog?.items || []}
                    columns={[
                      { key: 'symbol', label: 'Instrument', mono: true },
                      { key: 'timeframe', label: 'TF' },
                      { key: 'provider', label: 'Source' },
                      { key: 'candle_count', label: 'Candles', align: 'right', render: (row) => formatNumber(row.candle_count, 0) },
                      { key: 'gap_count', label: 'Gaps', align: 'right', render: (row) => formatNumber(row.gap_count, 0) },
                      { key: 'continuity_status', label: 'Continuity' },
                    ]}
                    empty="No candle catalog rows available."
                  />
                </div>
              ) : null}

              {activeTab === 'diagnostics' ? <DiagnosticsPanel diagnostics={diagnostics} /> : null}

              {activeTab === 'operations' ? (
                <div className="grid gap-4 lg:grid-cols-2">
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                      <Activity className="size-3.5" />
                      Runtime Latency
                    </div>
                    <StatusLine label="Wall clock seconds" value={formatNumber(operationalHealth?.runtime_step_latency_summary?.wall_clock_duration_seconds, 2)} />
                    <StatusLine label="p50 ms" value={formatNumber(operationalHealth?.runtime_step_latency_summary?.p50_ms, 2)} />
                    <StatusLine label="p95 ms" value={formatNumber(operationalHealth?.runtime_step_latency_summary?.p95_ms, 2)} />
                    <StatusLine label="p99 ms" value={formatNumber(operationalHealth?.runtime_step_latency_summary?.p99_ms, 2)} />
                  </div>
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">Event Volume</div>
                    <StatusLine label="Total events" value={formatNumber(operationalHealth?.event_volume_summary?.total, 0)} />
                    <StatusLine label="Diagnostics" value={formatNumber(operationalHealth?.diagnostic_timeline?.length || 0, 0)} />
                    <StatusLine label="Projection events" value={formatNumber(operationalHealth?.projection_health_timeline?.length || 0, 0)} />
                  </div>
                </div>
              ) : null}

              {activeTab === 'export' ? (
                <div className="space-y-4">
                  <div className="rounded-[6px] border border-white/8 bg-white/[0.03] p-4">
                    <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Export Bundle</div>
                    <div className="mt-2 text-sm text-slate-300">{exportManifest?.filename || `run_${runId}_report_export.zip`}</div>
                    <div className="mt-1 text-xs text-slate-500">
                      {manifestLoading ? 'Loading manifest...' : `${exportManifest?.files?.length || 0} files in manifest`}
                    </div>
                    <button
                      type="button"
                      onClick={handleExport}
                      disabled={exporting}
                      className="mt-4 inline-flex items-center gap-2 rounded-[6px] border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200 transition hover:border-white/20 disabled:opacity-50"
                    >
                      {exporting ? <RefreshCw className="size-4 animate-spin" /> : <Download className="size-4" />}
                      Standard Export
                    </button>
                    <button
                      type="button"
                      onClick={() => handleExport({ includeCandles: true })}
                      disabled={exporting}
                      className="ml-2 mt-4 inline-flex items-center gap-2 rounded-[6px] border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200 transition hover:border-white/20 disabled:opacity-50"
                    >
                      {exporting ? <RefreshCw className="size-4 animate-spin" /> : <Download className="size-4" />}
                      Research Export
                    </button>
                  </div>
                  {(exportManifest?.unavailable_sections || []).length ? (
                    <div className="rounded-[6px] border border-amber-500/25 bg-amber-500/10 p-4">
                      <div className="mb-2 text-[10px] uppercase tracking-[0.2em] text-amber-200">Unavailable Sections</div>
                      {(exportManifest.unavailable_sections || []).map((section) => (
                        <div key={section.name} className="py-1 text-sm text-amber-100">
                          {section.name}: {section.reason}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
