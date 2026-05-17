import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import {
  BarChart3,
  ChevronLeft,
  ChevronRight,
  Download,
  ExternalLink,
  Filter,
  GitCompare,
  Grid3X3,
  List,
  RefreshCw,
  Search,
  X,
} from 'lucide-react'
import { reportService } from '../../services/reportService.js'
import { formatCurrency, formatNumber, formatPercent, formatSymbols, formatTimeframe } from '../../utils/formatters.js'
import { formatExecutionModeLabel } from '../../features/bots/executionMode.js'
import { reportListItemView } from './reportContractViewModel.js'
import { reportComparableForSelection } from './reportComparisonViewModel.js'
import { RunComparisonPage } from './RunComparisonPage.jsx'
import { RunReportPage, formatDuration } from './RunReportPage.jsx'

const PAGE_SIZE = 25

const SORT_OPTIONS = [
  { value: 'completedAt', label: 'Date' },
  { value: 'netPnl', label: 'Net PnL' },
  { value: 'totalReturn', label: 'Return' },
  { value: 'sharpe', label: 'Sharpe' },
  { value: 'maxDrawdownPct', label: 'Drawdown' },
  { value: 'trades', label: 'Trades' },
]

const formatDateTimeShort = (value) => {
  if (!value) return 'Not available'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return String(value)
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

const normalizeLabel = (value) => {
  if (value === true) return 'Complete'
  if (value === false) return 'Incomplete'
  if (value === null || value === undefined || value === '') return 'Unknown'
  return String(value)
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

const statusTone = (value) => {
  const normalized = String(value ?? '').toLowerCase()
  if (!normalized || ['unknown', 'not_available', 'not_computed', 'unavailable', 'not_implemented'].includes(normalized)) return 'neutral'
  if (['ready', 'completed', 'pass', 'passed', 'certified', 'research_ready', 'research_valid', 'match', 'matched', 'present'].includes(normalized)) {
    return 'good'
  }
  if (['partial', 'degraded', 'ready_with_caveats', 'caution', 'warning', 'operational_drift', 'operational_only_drift'].includes(normalized)) {
    return 'warn'
  }
  if (['blocked', 'failed', 'fail', 'mismatch', 'missing', 'invalid', 'drift'].includes(normalized)) return 'bad'
  return 'neutral'
}

const badgeClasses = {
  good: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
  warn: 'border-amber-500/30 bg-amber-500/10 text-amber-200',
  bad: 'border-rose-500/30 bg-rose-500/10 text-rose-200',
  neutral: 'border-white/10 bg-white/[0.04] text-slate-300',
}

function StatusPill({ value, title }) {
  const tone = statusTone(value)
  return (
    <span className={`inline-flex rounded-[6px] border px-2 py-1 text-[10px] font-medium ${badgeClasses[tone]}`} title={title}>
      {normalizeLabel(value)}
    </span>
  )
}

const formatMetricNumber = (value, decimals = 2) => (value === null || value === undefined ? 'Not available' : formatNumber(value, decimals))
const formatMetricCurrency = (value) => (value === null || value === undefined ? 'Not available' : formatCurrency(value))
const formatMetricPercent = (value) => (value === null || value === undefined ? 'Not available' : formatPercent(value, 2))
const reportActionLabel = (report) => {
  if (report?.canViewReport) return 'View Report'
  const status = String(report?.reportStatus || '').toLowerCase()
  if (['building', 'preparing'].includes(status)) return 'Report Generating...'
  if (status === 'failed') return 'Report Failed'
  if (status === 'not_started') return 'Generate Report'
  return 'Report Unavailable'
}
const reportActionDisabled = (report) => {
  if (report?.canViewReport) return false
  const status = String(report?.reportStatus || '').toLowerCase()
  return !['not_started', 'failed', 'stale'].includes(status)
}

function ReportCard({ report, onOpen, onExport, exportingRunId, selectedForCompare = false, onToggleCompare }) {
  const compareSelectable = reportComparableForSelection(report)
  return (
    <article
      className="cursor-pointer rounded-[8px] border border-white/10 bg-[#141923]/80 p-4 transition hover:border-white/20 hover:bg-[#171e2b]"
      onClick={() => onOpen(report)}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-slate-100">{report.botName || 'Bot unknown'}</div>
          <div className="mt-1 truncate text-xs text-slate-500">{report.strategyName || 'Strategy unknown'}</div>
          <div className="mt-2 break-all font-mono text-[11px] text-slate-500">{report.runId || 'No run ID'}</div>
        </div>
        <StatusPill value={report.researchStatus || report.readinessStatus || report.reportStatus} />
      </div>

      <div className="mt-4 grid grid-cols-2 gap-2">
        <MetricTile label="Net PnL" value={formatMetricCurrency(report.netPnl)} />
        <MetricTile label="Return" value={formatMetricPercent(report.totalReturn)} />
        <MetricTile label="Max DD" value={formatMetricPercent(report.maxDrawdownPct)} />
        <MetricTile
          label="Sharpe"
          value={report.sharpeMetric && report.sharpeMetric.valid === false ? 'Not available' : formatMetricNumber(report.sharpe, 2)}
          title={report.sharpeMetric?.invalid_reason || report.sharpeMetric?.method || undefined}
        />
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <span>{formatSymbols(report.symbols, 3)}</span>
        <span>{formatTimeframe(report.timeframe)}</span>
        <span>{report.executionMode ? formatExecutionModeLabel(report.executionMode) : 'Execution unknown'}</span>
        <span>{formatDuration(report.durationSeconds)}</span>
      </div>

      <div className="mt-4 flex flex-wrap gap-2 border-t border-white/8 pt-3">
        <RowAction label={reportActionLabel(report)} disabled={reportActionDisabled(report)} onClick={() => onOpen(report)} />
        <a
          href={`/bots?runId=${encodeURIComponent(report.runId || '')}`}
          onClick={(event) => event.stopPropagation()}
          className="inline-flex items-center gap-1.5 rounded-[6px] border border-white/10 bg-white/[0.04] px-2.5 py-1.5 text-xs text-slate-300 hover:border-white/20"
        >
          <ExternalLink className="size-3" />
          Open BotLens
        </a>
        <RowAction
          label={exportingRunId === report.runId ? 'Exporting' : 'Export'}
          icon={Download}
          disabled={exportingRunId === report.runId}
          onClick={() => onExport(report)}
        />
        <button
          type="button"
          disabled={!compareSelectable}
          title={compareSelectable ? 'Select for comparison' : 'Report comparison requires a ready terminal report'}
          onClick={(event) => {
            event.stopPropagation()
            if (compareSelectable) onToggleCompare?.(report)
          }}
          className={`inline-flex items-center gap-1.5 rounded-[6px] border px-2.5 py-1.5 text-xs transition disabled:cursor-not-allowed disabled:opacity-45 ${
            selectedForCompare
              ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-soft)]'
              : 'border-white/10 bg-white/[0.04] text-slate-300 hover:border-white/20'
          }`}
        >
          <GitCompare className="size-3" />
          {selectedForCompare ? 'Selected' : 'Compare'}
        </button>
      </div>
    </article>
  )
}

function MetricTile({ label, value, title }) {
  return (
    <div className="rounded-[7px] border border-white/8 bg-black/20 p-2.5" title={title}>
      <div className="text-[9px] uppercase tracking-[0.16em] text-slate-500">{label}</div>
      <div className="mt-1 font-mono text-sm text-slate-100">{value}</div>
    </div>
  )
}

function RowAction({ label, icon: Icon, disabled = false, onClick }) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={(event) => {
        event.stopPropagation()
        if (!disabled) onClick?.()
      }}
      className="inline-flex items-center gap-1.5 rounded-[6px] border border-white/10 bg-white/[0.04] px-2.5 py-1.5 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100 disabled:opacity-50"
    >
      {Icon ? <Icon className="size-3" /> : null}
      {label}
    </button>
  )
}

export function ReportsPage() {
  const { routeRunId } = useParams()
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const requestSeqRef = useRef(0)
  const [reports, setReports] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')
  const [botFilter, setBotFilter] = useState('')
  const [instrumentFilter, setInstrumentFilter] = useState('')
  const [timeframeFilter, setTimeframeFilter] = useState('')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [page, setPage] = useState(0)
  const [viewMode, setViewMode] = useState('table')
  const [sortBy, setSortBy] = useState('completedAt')
  const [sortDir, setSortDir] = useState('desc')
  const [filtersExpanded, setFiltersExpanded] = useState(false)
  const [exportingRunId, setExportingRunId] = useState(null)
  const [selectedCompareRunIds, setSelectedCompareRunIds] = useState([])

  const isCompareRoute = routeRunId === 'compare'
  const compareLeftRunId = searchParams.get('left')
  const compareRightRunId = searchParams.get('right')
  const runId = isCompareRoute ? null : routeRunId || searchParams.get('runId')

  const fetchReports = useCallback(async () => {
    const seq = requestSeqRef.current + 1
    requestSeqRef.current = seq
    setLoading(true)
    setError(null)
    try {
      const payload = await reportService.listReports({
        type: 'backtest',
        limit: PAGE_SIZE,
        offset: page * PAGE_SIZE,
        search,
        botId: botFilter,
        instrument: instrumentFilter,
        timeframe: timeframeFilter,
        start: startDate || undefined,
        end: endDate || undefined,
      })
      if (requestSeqRef.current !== seq) return
      const baseRows = (payload?.items || []).map(reportListItemView)
      setReports(baseRows)
      setTotal(payload?.total || 0)
    } catch (err) {
      if (requestSeqRef.current === seq) {
        setError(err?.message || 'Failed to load reports')
      }
    } finally {
      if (requestSeqRef.current === seq) {
        setLoading(false)
      }
    }
  }, [botFilter, endDate, instrumentFilter, page, search, startDate, timeframeFilter])

  useEffect(() => {
    fetchReports()
  }, [fetchReports])

  const handleBackToIndex = useCallback(() => {
    navigate('/reports')
  }, [navigate])

  const handleOpenReport = useCallback((report) => {
    if (!report?.runId) return
    navigate(`/reports/${encodeURIComponent(report.runId)}`)
  }, [navigate])

  const handleToggleCompareSelection = useCallback((report) => {
    if (!report?.runId || !reportComparableForSelection(report)) return
    setSelectedCompareRunIds((prev) => {
      if (prev.includes(report.runId)) return prev.filter((runId) => runId !== report.runId)
      return [...prev.slice(-1), report.runId]
    })
  }, [])

  const handleExport = useCallback(async (report) => {
    if (!report?.runId || exportingRunId) return
    setExportingRunId(report.runId)
    try {
      const { blob, filename } = await reportService.exportReport(report.runId, {})
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename || `run_${report.runId}_report_export.zip`
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
    } finally {
      setExportingRunId(null)
    }
  }, [exportingRunId])

  const handleResetPage = () => setPage(0)

  const handleClearFilters = () => {
    setSearch('')
    setBotFilter('')
    setInstrumentFilter('')
    setTimeframeFilter('')
    setStartDate('')
    setEndDate('')
    handleResetPage()
  }

  const botOptions = useMemo(() => {
    const unique = new Map()
    reports.forEach((report) => {
      if (report?.botId && report?.botName) unique.set(report.botId, report.botName)
    })
    return [{ value: '', label: 'All Bots' }, ...Array.from(unique.entries()).map(([value, label]) => ({ value, label }))]
  }, [reports])

  const instrumentOptions = useMemo(() => {
    const unique = new Set()
    reports.forEach((report) => (report?.symbols || []).forEach((symbol) => unique.add(symbol)))
    return [{ value: '', label: 'All Instruments' }, ...Array.from(unique).map((value) => ({ value, label: value }))]
  }, [reports])

  const timeframeOptions = useMemo(() => {
    const unique = new Set()
    reports.forEach((report) => {
      if (report?.timeframe) unique.add(report.timeframe)
    })
    return [{ value: '', label: 'All Timeframes' }, ...Array.from(unique).map((value) => ({ value, label: value }))]
  }, [reports])

  const sortedReports = useMemo(() => {
    return [...reports].sort((a, b) => {
      let aVal = a[sortBy]
      let bVal = b[sortBy]
      if (sortBy === 'completedAt') {
        aVal = aVal ? new Date(aVal).getTime() : 0
        bVal = bVal ? new Date(bVal).getTime() : 0
      }
      if (typeof aVal === 'string' || typeof bVal === 'string') {
        return sortDir === 'desc'
          ? String(bVal || '').localeCompare(String(aVal || ''))
          : String(aVal || '').localeCompare(String(bVal || ''))
      }
      if (aVal == null) aVal = sortDir === 'desc' ? -Infinity : Infinity
      if (bVal == null) bVal = sortDir === 'desc' ? -Infinity : Infinity
      return sortDir === 'desc' ? bVal - aVal : aVal - bVal
    })
  }, [reports, sortBy, sortDir])

  const pageCount = Math.ceil(total / PAGE_SIZE)
  const hasActiveFilters = search || botFilter || instrumentFilter || timeframeFilter || startDate || endDate
  const selectedCompareReports = useMemo(() => {
    const byId = new Map(reports.map((report) => [report.runId, report]))
    return selectedCompareRunIds.map((runId) => byId.get(runId)).filter(Boolean)
  }, [reports, selectedCompareRunIds])
  const canCompareSelected = selectedCompareReports.length === 2 && selectedCompareReports.every(reportComparableForSelection)
  const compareSelectionLabel = selectedCompareRunIds.length ? `${Math.min(selectedCompareRunIds.length, 2)}/2 selected` : 'Select 2 reports'

  const handleOpenComparison = useCallback(() => {
    if (!canCompareSelected) return
    const [left, right] = selectedCompareRunIds
    navigate(`/reports/compare?left=${encodeURIComponent(left)}&right=${encodeURIComponent(right)}`)
  }, [canCompareSelected, navigate, selectedCompareRunIds])

  if (isCompareRoute) {
    return <RunComparisonPage leftRunId={compareLeftRunId} rightRunId={compareRightRunId} onBack={handleBackToIndex} />
  }

  if (runId) {
    return <RunReportPage runId={runId} onBack={handleBackToIndex} />
  }

  return (
    <div className="space-y-5">
      <header className="rounded-[8px] border border-white/8 bg-[#151924]/85 p-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <div className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Research Reports</div>
            <h1 className="mt-1 text-2xl font-semibold text-slate-100">Reports Index</h1>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
              Trust and performance are loaded from backend report contracts. Missing fields stay explicit instead of being inferred in the UI.
            </p>
          </div>
          <button
            type="button"
            onClick={fetchReports}
            disabled={loading}
            className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100 disabled:opacity-50"
          >
            <RefreshCw className={`size-3.5 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </header>

      <section className="rounded-[8px] border border-white/8 bg-[#151924]/80 p-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-1 items-center gap-3">
            <div className="relative flex-1 lg:max-w-md">
              <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
              <input
                type="text"
                value={search}
                onChange={(event) => {
                  setSearch(event.target.value)
                  handleResetPage()
                }}
                placeholder="Search reports..."
                className="w-full rounded-[7px] border border-white/10 bg-black/40 py-2.5 pl-10 pr-4 text-sm text-slate-100 placeholder:text-slate-500 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              />
            </div>
            <button
              type="button"
              onClick={() => setFiltersExpanded((prev) => !prev)}
              className={`inline-flex items-center gap-2 rounded-[7px] border px-3 py-2.5 text-sm transition ${
                filtersExpanded || hasActiveFilters
                  ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-soft)]'
                  : 'border-white/10 bg-white/[0.04] text-slate-300 hover:border-white/20'
              }`}
            >
              <Filter className="size-4" />
              Filters
              {hasActiveFilters ? (
                <span className="rounded-full bg-[color:var(--accent-alpha-30)] px-1.5 text-[10px]">
                  {[search, botFilter, instrumentFilter, timeframeFilter, startDate, endDate].filter(Boolean).length}
                </span>
              ) : null}
            </button>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center rounded-[7px] border border-white/10 bg-white/[0.04] p-1">
              <button
                type="button"
                onClick={() => setViewMode('cards')}
                className={`rounded-[6px] p-2 ${viewMode === 'cards' ? 'bg-white/10 text-slate-100' : 'text-slate-500 hover:text-slate-300'}`}
                title="Card view"
              >
                <Grid3X3 className="size-4" />
              </button>
              <button
                type="button"
                onClick={() => setViewMode('table')}
                className={`rounded-[6px] p-2 ${viewMode === 'table' ? 'bg-white/10 text-slate-100' : 'text-slate-500 hover:text-slate-300'}`}
                title="Table view"
              >
                <List className="size-4" />
              </button>
            </div>
            <select
              value={`${sortBy}-${sortDir}`}
              onChange={(event) => {
                const [field, direction] = event.target.value.split('-')
                setSortBy(field)
                setSortDir(direction)
              }}
              className="rounded-[7px] border border-white/10 bg-[#101520] px-3 py-2.5 text-sm text-slate-300 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            >
              {SORT_OPTIONS.map((option) => (
                <option key={`${option.value}-desc`} value={`${option.value}-desc`}>
                  {option.label} desc
                </option>
              ))}
              {SORT_OPTIONS.map((option) => (
                <option key={`${option.value}-asc`} value={`${option.value}-asc`}>
                  {option.label} asc
                </option>
              ))}
            </select>
          </div>
        </div>

        {filtersExpanded ? (
          <div className="mt-4 grid gap-3 rounded-[8px] border border-white/8 bg-black/20 p-4 lg:grid-cols-5">
            <FilterSelect label="Bot" value={botFilter} onChange={setBotFilter} options={botOptions} onResetPage={handleResetPage} />
            <FilterSelect label="Instrument" value={instrumentFilter} onChange={setInstrumentFilter} options={instrumentOptions} onResetPage={handleResetPage} />
            <FilterSelect label="Timeframe" value={timeframeFilter} onChange={setTimeframeFilter} options={timeframeOptions} onResetPage={handleResetPage} />
            <DateInput label="Start" value={startDate} onChange={setStartDate} onResetPage={handleResetPage} />
            <DateInput label="End" value={endDate} onChange={setEndDate} onResetPage={handleResetPage} />
            {hasActiveFilters ? (
              <button
                type="button"
                onClick={handleClearFilters}
                className="inline-flex items-center justify-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 hover:border-white/20 lg:col-span-5 lg:w-fit"
              >
                <X className="size-3.5" />
                Clear filters
              </button>
            ) : null}
          </div>
        ) : null}
      </section>

      <section className="rounded-[8px] border border-white/8 bg-[#151924]/80 p-4">
        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <div className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Run Archive</div>
            <div className="mt-1 text-sm text-slate-300">
              {total} report{total === 1 ? '' : 's'}
            </div>
          </div>
          <div className="flex items-center gap-1">
            <button
              type="button"
              disabled={!canCompareSelected}
              onClick={handleOpenComparison}
              className="mr-2 inline-flex items-center gap-2 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-300 transition hover:border-white/20 hover:text-slate-100 disabled:cursor-not-allowed disabled:opacity-45"
              title={canCompareSelected ? 'Compare selected reports' : 'Select two ready terminal reports'}
            >
              <GitCompare className="size-3.5" />
              Compare
              <span className="text-slate-500">{compareSelectionLabel}</span>
            </button>
            <button
              type="button"
              disabled={page === 0}
              onClick={() => setPage((prev) => Math.max(prev - 1, 0))}
              className="inline-flex h-8 w-8 items-center justify-center rounded-[7px] border border-white/10 bg-white/[0.04] text-slate-300 disabled:opacity-40"
            >
              <ChevronLeft className="size-4" />
            </button>
            <span className="px-3 text-xs text-slate-400">
              {page + 1} / {Math.max(pageCount, 1)}
            </span>
            <button
              type="button"
              disabled={page + 1 >= pageCount}
              onClick={() => setPage((prev) => prev + 1)}
              className="inline-flex h-8 w-8 items-center justify-center rounded-[7px] border border-white/10 bg-white/[0.04] text-slate-300 disabled:opacity-40"
            >
              <ChevronRight className="size-4" />
            </button>
          </div>
        </div>

        {loading ? (
          <LoadingPanel message="Loading reports..." />
        ) : error ? (
          <ErrorPanel message={error} onRetry={fetchReports} />
        ) : sortedReports.length === 0 ? (
          <EmptyPanel message={hasActiveFilters ? 'No reports match your filters.' : 'No reports are available yet.'} onClear={hasActiveFilters ? handleClearFilters : null} />
        ) : viewMode === 'cards' ? (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {sortedReports.map((report) => (
              <ReportCard
                key={report.runId}
                report={report}
                onOpen={handleOpenReport}
                onExport={handleExport}
                exportingRunId={exportingRunId}
                selectedForCompare={selectedCompareRunIds.includes(report.runId)}
                onToggleCompare={handleToggleCompareSelection}
              />
            ))}
          </div>
        ) : (
          <ReportTable
            reports={sortedReports}
            onOpen={handleOpenReport}
            onExport={handleExport}
            exportingRunId={exportingRunId}
            selectedCompareRunIds={selectedCompareRunIds}
            onToggleCompare={handleToggleCompareSelection}
          />
        )}
      </section>
    </div>
  )
}

function FilterSelect({ label, value, onChange, options, onResetPage }) {
  return (
    <label className="block">
      <span className="mb-1.5 block text-[10px] uppercase tracking-[0.18em] text-slate-500">{label}</span>
      <select
        value={value}
        onChange={(event) => {
          onChange(event.target.value)
          onResetPage()
        }}
        className="w-full rounded-[7px] border border-white/10 bg-[#101520] px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  )
}

function DateInput({ label, value, onChange, onResetPage }) {
  return (
    <label className="block">
      <span className="mb-1.5 block text-[10px] uppercase tracking-[0.18em] text-slate-500">{label}</span>
      <input
        type="date"
        value={value}
        onChange={(event) => {
          onChange(event.target.value)
          onResetPage()
        }}
        className="w-full rounded-[7px] border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
      />
    </label>
  )
}

function ReportTable({ reports, onOpen, onExport, exportingRunId, selectedCompareRunIds = [], onToggleCompare }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-left text-sm text-slate-200">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.16em] text-slate-500">
            <th className="pb-3 pr-4">Run</th>
            <th className="pb-3 pr-4">Bot / Strategy</th>
            <th className="pb-3 pr-4">Trust</th>
            <th className="pb-3 pr-4">Readiness</th>
            <th className="pb-3 pr-4">Golden</th>
            <th className="pb-3 pr-4">Semantic</th>
            <th className="pb-3 pr-4">Operational</th>
            <th className="pb-3 pr-4">Symbols</th>
            <th className="pb-3 pr-4">Started / Ended</th>
            <th className="pb-3 pr-4">Duration</th>
            <th className="pb-3 pr-4 text-right">Net PnL</th>
            <th className="pb-3 pr-4 text-right">Return</th>
            <th className="pb-3 pr-4 text-right">Max DD</th>
            <th className="pb-3 pr-4 text-right">Sharpe</th>
            <th className="pb-3 pr-4 text-right">Trades</th>
            <th className="pb-3 text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          {reports.map((report) => {
            const compareSelectable = reportComparableForSelection(report)
            const selectedForCompare = selectedCompareRunIds.includes(report.runId)
            return (
              <tr key={report.runId} onClick={() => onOpen(report)} className="cursor-pointer border-b border-white/5 transition hover:bg-white/[0.04]">
                <td className="py-3 pr-4">
                  <div className="font-mono text-xs text-slate-200">{report.runId?.slice(0, 8) || 'Unknown'}</div>
                  <div className="mt-1 text-[10px] uppercase tracking-[0.14em] text-slate-600">{report.runType || 'run'}</div>
                </td>
                <td className="py-3 pr-4">
                  <div className="max-w-[14rem] truncate font-medium text-slate-200">{report.botName || 'Bot unknown'}</div>
                  <div className="max-w-[14rem] truncate text-xs text-slate-500">{report.strategyName || 'Strategy unknown'}</div>
                </td>
                <td className="py-3 pr-4"><StatusPill value={report.researchStatus || report.reportStatus} /></td>
                <td className="py-3 pr-4"><StatusPill value={report.readinessStatus || report.reportStatus} /></td>
                <td className="py-3 pr-4"><StatusPill value={report.goldenStatus} /></td>
                <td className="py-3 pr-4"><StatusPill value={report.semanticFingerprint ? 'present' : 'not_available'} title={report.semanticFingerprint || undefined} /></td>
                <td className="py-3 pr-4"><StatusPill value={report.operationalDriftStatus || (report.operationalFingerprint ? 'present' : 'not_available')} title={report.operationalFingerprint || undefined} /></td>
                <td className="py-3 pr-4 text-xs" title={(report.symbols || []).join(', ')}>
                  <div>{formatSymbols(report.symbols, 2)}</div>
                  <div className="mt-1 text-slate-500">{formatTimeframe(report.timeframe)}</div>
                </td>
                <td className="py-3 pr-4 text-xs text-slate-400">
                  <div>{formatDateTimeShort(report.startedAt || report.wallClockWindow?.start || report.completedAt)}</div>
                  <div className="mt-1">{formatDateTimeShort(report.endedAt || report.wallClockWindow?.end || report.completedAt)}</div>
                </td>
                <td className="py-3 pr-4 text-xs text-slate-400">{formatDuration(report.durationSeconds)}</td>
                <td className="py-3 pr-4 text-right font-mono">{formatMetricCurrency(report.netPnl)}</td>
                <td className="py-3 pr-4 text-right font-mono">{formatMetricPercent(report.totalReturn)}</td>
                <td className="py-3 pr-4 text-right font-mono">{formatMetricPercent(report.maxDrawdownPct)}</td>
                <td className="py-3 pr-4 text-right font-mono" title={report.sharpeMetric?.invalid_reason || report.sharpeMetric?.method || undefined}>
                  {report.sharpeMetric && report.sharpeMetric.valid === false ? 'Not available' : formatMetricNumber(report.sharpe, 2)}
                </td>
                <td className="py-3 pr-4 text-right font-mono">{formatMetricNumber(report.trades, 0)}</td>
                <td className="py-3 text-right">
                  <div className="flex justify-end gap-1.5">
                    <RowAction label={report.canViewReport ? 'View' : '...'} disabled={reportActionDisabled(report)} onClick={() => onOpen(report)} />
                    <a
                      href={`/bots?runId=${encodeURIComponent(report.runId || '')}`}
                      onClick={(event) => event.stopPropagation()}
                      className="inline-flex items-center justify-center rounded-[6px] border border-white/10 bg-white/[0.04] p-1.5 text-slate-300 hover:border-white/20"
                      title="Open BotLens"
                    >
                      <ExternalLink className="size-3.5" />
                    </a>
                    <RowAction
                      label={exportingRunId === report.runId ? '...' : ''}
                      icon={Download}
                      disabled={exportingRunId === report.runId}
                      onClick={() => onExport(report)}
                    />
                    <button
                      type="button"
                      disabled={!compareSelectable}
                      title={compareSelectable ? 'Select for comparison' : 'Report comparison requires a ready terminal report'}
                      onClick={(event) => {
                        event.stopPropagation()
                        if (compareSelectable) onToggleCompare?.(report)
                      }}
                      className={`inline-flex items-center justify-center rounded-[6px] border p-1.5 transition disabled:cursor-not-allowed disabled:opacity-45 ${
                        selectedForCompare
                          ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-soft)]'
                          : 'border-white/10 bg-white/[0.04] text-slate-300 hover:border-white/20'
                      }`}
                    >
                      <GitCompare className="size-3.5" />
                    </button>
                  </div>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function LoadingPanel({ message }) {
  return (
    <div className="flex items-center justify-center rounded-[8px] border border-white/8 bg-black/20 p-10 text-sm text-slate-400">
      <RefreshCw className="mr-3 size-4 animate-spin text-[color:var(--accent-text-soft)]" />
      {message}
    </div>
  )
}

function ErrorPanel({ message, onRetry }) {
  return (
    <div className="rounded-[8px] border border-rose-500/20 bg-rose-500/10 p-6 text-sm text-rose-100">
      <div>{message}</div>
      <button type="button" onClick={onRetry} className="mt-3 rounded-[7px] border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-slate-200">
        Retry
      </button>
    </div>
  )
}

function EmptyPanel({ message, onClear }) {
  return (
    <div className="rounded-[8px] border border-white/8 bg-black/20 p-10 text-center">
      <BarChart3 className="mx-auto size-9 text-slate-600" />
      <div className="mt-4 text-sm text-slate-400">{message}</div>
      {onClear ? (
        <button type="button" onClick={onClear} className="mt-3 text-xs text-[color:var(--accent-text-soft)] hover:underline">
          Clear filters
        </button>
      ) : null}
    </div>
  )
}
