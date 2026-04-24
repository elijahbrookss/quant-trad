import { useCallback, useEffect, useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import {
  ArrowDownAZ,
  ArrowUpDown,
  BarChart3,
  Calendar,
  ChevronLeft,
  ChevronRight,
  Filter,
  GitCompare,
  Grid3X3,
  List,
  RefreshCw,
  Search,
  TrendingDown,
  TrendingUp,
  X,
} from 'lucide-react'
import { reportService } from '../../services/reportService.js'
import { formatCurrency, formatNumber, formatPercent, formatTimeframe, formatSymbols } from '../../utils/formatters.js'
import { Button } from '../ui/Button.jsx'
import { ReportModal } from './ReportModal.jsx'
import { CompareModal } from './CompareModal.jsx'
import { SemanticStatusBadge } from '../ui/StatusBadge.jsx'
import { mapRunToViewModel } from '../../features/bots/viewModels/runViewModel.js'

const PAGE_SIZE = 25

const formatDateTime = (value) => {
  if (!value) return '--'
  try {
    const date = new Date(value)
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
  } catch {
    return value
  }
}

const formatDateTimeShort = (value) => {
  if (!value) return '--'
  try {
    const date = new Date(value)
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  } catch {
    return value
  }
}

const formatRunTime = (seconds) => {
  if (seconds === null || seconds === undefined) return '--'
  const numeric = Number(seconds)
  if (!Number.isFinite(numeric) || numeric < 0) return '--'
  const totalSeconds = Math.floor(numeric)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const secs = totalSeconds % 60
  if (hours) {
    return `${hours}h ${String(minutes).padStart(2, '0')}m`
  }
  if (minutes) {
    return `${minutes}m ${String(secs).padStart(2, '0')}s`
  }
  return `${secs}s`
}

const SORT_OPTIONS = [
  { value: 'completed_at', label: 'Date' },
  { value: 'net_pnl', label: 'Net PnL' },
  { value: 'total_return', label: 'Return' },
  { value: 'sharpe', label: 'Sharpe' },
  { value: 'max_drawdown_pct', label: 'Drawdown' },
  { value: 'trades', label: 'Trades' },
]

const StatCard = ({ label, value, subValue, icon: Icon, tone = 'neutral' }) => {
  const toneClasses = {
    positive: 'text-emerald-400',
    negative: 'text-rose-400',
    neutral: 'text-slate-100',
    accent: 'text-[color:var(--accent-text-soft)]',
  }

  return (
    <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <div className="text-[11px] uppercase tracking-[0.3em] text-slate-500">{label}</div>
          <div className={`text-xl font-semibold ${toneClasses[tone]}`}>{value}</div>
          {subValue && <div className="text-xs text-slate-500">{subValue}</div>}
        </div>
        {Icon && (
          <div className="rounded-lg bg-white/5 p-2">
            <Icon className="size-4 text-slate-400" />
          </div>
        )}
      </div>
    </div>
  )
}

const ReportStateShell = ({ runView, selected = false }) => (
  <section className="rounded-2xl border border-white/8 bg-[#111722]/70 p-4">
    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
      <div className="min-w-0">
        <div className="text-[11px] font-medium text-slate-500">{selected ? 'Selected run' : 'Report state'}</div>
        <div className="mt-1 flex flex-wrap items-center gap-2">
          <span className="font-mono text-sm text-slate-200">{runView.runId || 'No run selected'}</span>
          {runView.name ? <span className="text-sm text-slate-500">{runView.name}</span> : null}
        </div>
      </div>
      <div className="flex flex-wrap gap-2">
        <SemanticStatusBadge kind="report" value={runView.reportStatus} />
        <SemanticStatusBadge kind="comparison" value={runView.comparisonStatus} />
      </div>
    </div>
    {runView.reportStatus === 'unknown' ? (
      <div className="mt-4 rounded-lg border border-white/8 bg-white/[0.03] p-3">
        <p className="text-sm font-medium text-slate-200">Report status unknown</p>
        <p className="mt-1 text-xs leading-5 text-slate-500">
          The current backend response does not yet expose report readiness. Showing available run data only.
        </p>
      </div>
    ) : null}
    {runView.comparisonStatus === 'unknown' ? (
      <div className="mt-3 rounded-lg border border-white/8 bg-white/[0.03] p-3">
        <p className="text-sm font-medium text-slate-200">Comparison eligibility unknown</p>
        <p className="mt-1 text-xs leading-5 text-slate-500">
          The current backend response does not yet expose whether this run is eligible for comparison.
        </p>
      </div>
    ) : null}
  </section>
)

const ReportCard = ({ report, onSelect, isSelected, onToggleCompare }) => {
  const pnl = report.net_pnl || 0
  const isProfit = pnl > 0
  const isLoss = pnl < 0
  const runView = mapRunToViewModel(report)
  const canCompare = runView.comparisonStatus === 'eligible'

  return (
    <div
      className={`group relative cursor-pointer rounded-2xl border bg-black/30 p-4 transition hover:border-white/20 hover:bg-black/40 ${
        isSelected ? 'border-[color:var(--accent-alpha-60)] ring-1 ring-[color:var(--accent-ring)]' : 'border-white/10'
      }`}
      onClick={() => onSelect(report)}
    >
      <div className="absolute right-3 top-3 opacity-0 transition group-hover:opacity-100">
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation()
            if (canCompare) onToggleCompare(report.run_id)
          }}
          disabled={!canCompare}
          className={`rounded-full border p-1.5 transition ${
            isSelected
              ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-soft)]'
              : canCompare
                ? 'border-white/10 bg-white/5 text-slate-400 hover:border-white/20 hover:text-slate-300'
                : 'cursor-not-allowed border-white/8 bg-white/[0.03] text-slate-600'
          }`}
          title={canCompare ? (isSelected ? 'Remove from comparison' : 'Add to comparison') : 'Comparison eligibility unknown'}
        >
          <GitCompare className="size-3" />
        </button>
      </div>

      <div className="mb-3 flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-medium text-slate-100">{report.bot_name || 'Bot'}</div>
          <div className="mt-0.5 truncate text-xs text-slate-500">{report.strategy_name || 'Strategy'}</div>
        </div>
        <SemanticStatusBadge kind="report" value={runView.reportStatus} />
      </div>

      <div className="mb-4 flex items-center gap-2 text-xs text-slate-400">
        <Calendar className="size-3" />
        <span>{formatDateTimeShort(report.date_range?.start)}</span>
        <span className="text-slate-600">→</span>
        <span>{formatDateTimeShort(report.date_range?.end)}</span>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="rounded-lg bg-white/5 p-2.5">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Net PnL</div>
          <div
            className={`mt-1 text-sm font-semibold ${isProfit ? 'text-emerald-400' : isLoss ? 'text-rose-400' : 'text-slate-300'}`}
          >
            {formatCurrency(pnl)}
          </div>
        </div>
        <div className="rounded-lg bg-white/5 p-2.5">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Return</div>
          <div
            className={`mt-1 text-sm font-semibold ${
              (report.total_return || 0) > 0
                ? 'text-emerald-400'
                : (report.total_return || 0) < 0
                  ? 'text-rose-400'
                  : 'text-slate-300'
            }`}
          >
            {formatPercent(report.total_return, 2)}
          </div>
        </div>
        <div className="rounded-lg bg-white/5 p-2.5">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Sharpe</div>
          <div className="mt-1 text-sm font-semibold text-slate-200">{formatNumber(report.sharpe, 2)}</div>
        </div>
        <div className="rounded-lg bg-white/5 p-2.5">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Max DD</div>
          <div className="mt-1 text-sm font-semibold text-rose-400">{formatPercent(report.max_drawdown_pct, 2)}</div>
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between border-t border-white/5 pt-3">
        <div className="flex items-center gap-1.5 text-xs text-slate-500">
          <span>{report.trades || 0} trades</span>
          <span className="text-slate-700">•</span>
          <span>{formatTimeframe(report.timeframe)}</span>
        </div>
        <div className="text-[10px] text-slate-600">{report.run_id?.slice(0, 8)}</div>
      </div>
    </div>
  )
}

export function ReportsPage() {
  const { routeRunId } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()
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
  const [sortBy, setSortBy] = useState('completed_at')
  const [sortDir, setSortDir] = useState('desc')
  const [filtersExpanded, setFiltersExpanded] = useState(false)
  const [compareIds, setCompareIds] = useState([])
  const [compareOpen, setCompareOpen] = useState(false)

  const runId = routeRunId || searchParams.get('runId')

  const fetchReports = useCallback(async () => {
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
      setReports(payload?.items || [])
      setTotal(payload?.total || 0)
    } catch (err) {
      setError(err?.message || 'Failed to load reports')
    } finally {
      setLoading(false)
    }
  }, [botFilter, endDate, instrumentFilter, page, search, startDate, timeframeFilter])

  useEffect(() => {
    fetchReports()
  }, [fetchReports])

  useEffect(() => {
    if (compareIds.length < 2) {
      setCompareOpen(false)
    }
  }, [compareIds])

  const botOptions = useMemo(() => {
    const unique = new Map()
    reports.forEach((report) => {
      if (report?.bot_id && report?.bot_name) {
        unique.set(report.bot_id, report.bot_name)
      }
    })
    return [{ value: '', label: 'All Bots' }, ...Array.from(unique.entries()).map(([value, label]) => ({ value, label }))]
  }, [reports])

  const instrumentOptions = useMemo(() => {
    const unique = new Set()
    reports.forEach((report) => {
      ;(report?.symbols || []).forEach((symbol) => unique.add(symbol))
    })
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
    const sorted = [...reports].sort((a, b) => {
      let aVal = a[sortBy]
      let bVal = b[sortBy]
      if (sortBy === 'completed_at') {
        aVal = aVal ? new Date(aVal).getTime() : 0
        bVal = bVal ? new Date(bVal).getTime() : 0
      }
      if (aVal == null) aVal = sortDir === 'desc' ? -Infinity : Infinity
      if (bVal == null) bVal = sortDir === 'desc' ? -Infinity : Infinity
      return sortDir === 'desc' ? bVal - aVal : aVal - bVal
    })
    return sorted
  }, [reports, sortBy, sortDir])

  const stats = useMemo(() => {
    if (!reports.length) return null
    const profitable = reports.filter((r) => (r.net_pnl || 0) > 0).length
    const totalPnl = reports.reduce((sum, r) => sum + (r.net_pnl || 0), 0)
    const avgReturn =
      reports.reduce((sum, r) => sum + (r.total_return || 0), 0) / reports.length
    const avgSharpe =
      reports.filter((r) => r.sharpe != null).reduce((sum, r) => sum + r.sharpe, 0) /
        reports.filter((r) => r.sharpe != null).length || 0
    const totalTrades = reports.reduce((sum, r) => sum + (r.trades || 0), 0)
    return { profitable, totalPnl, avgReturn, avgSharpe, totalTrades, total: reports.length }
  }, [reports])

  const pageCount = Math.ceil(total / PAGE_SIZE)
  const selectedReport = useMemo(() => {
    if (!runId) return null
    return reports.find((report) => report?.run_id === runId) || null
  }, [reports, runId])
  const selectedRunView = useMemo(
    () => mapRunToViewModel(selectedReport || { run_id: runId, name: selectedReport?.bot_name || 'Selected run' }),
    [runId, selectedReport],
  )

  const handleRowClick = (report) => {
    if (!report?.run_id) return
    const next = new URLSearchParams(searchParams)
    next.set('runId', report.run_id)
    setSearchParams(next)
  }

  const handleCloseModal = () => {
    const next = new URLSearchParams(searchParams)
    next.delete('runId')
    setSearchParams(next)
  }

  const handleResetPage = () => setPage(0)

  const handleToggleCompare = (runId) => {
    const report = reports.find((item) => item?.run_id === runId)
    const runView = mapRunToViewModel(report || { run_id: runId })
    if (runView.comparisonStatus !== 'eligible') return
    setCompareIds((prev) => (prev.includes(runId) ? prev.filter((id) => id !== runId) : [...prev, runId].slice(-4)))
  }

  const handleClearFilters = () => {
    setSearch('')
    setBotFilter('')
    setInstrumentFilter('')
    setTimeframeFilter('')
    setStartDate('')
    setEndDate('')
    handleResetPage()
  }

  const hasActiveFilters = search || botFilter || instrumentFilter || timeframeFilter || startDate || endDate

  return (
    <div className="space-y-6">
      {runId ? <ReportStateShell runView={selectedRunView} selected /> : null}

      {stats && (
        <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          <StatCard
            label="Total Reports"
            value={stats.total}
            subValue={`${stats.profitable} profitable`}
            icon={BarChart3}
            tone="accent"
          />
          <StatCard
            label="Combined PnL"
            value={formatCurrency(stats.totalPnl)}
            icon={stats.totalPnl >= 0 ? TrendingUp : TrendingDown}
            tone={stats.totalPnl > 0 ? 'positive' : stats.totalPnl < 0 ? 'negative' : 'neutral'}
          />
          <StatCard
            label="Avg Return"
            value={formatPercent(stats.avgReturn, 2)}
            tone={stats.avgReturn > 0 ? 'positive' : stats.avgReturn < 0 ? 'negative' : 'neutral'}
          />
          <StatCard label="Avg Sharpe" value={formatNumber(stats.avgSharpe, 2)} />
          <StatCard label="Total Trades" value={formatNumber(stats.totalTrades, 0)} />
        </section>
      )}

      <section className="rounded-3xl border border-white/8 bg-[#1a1d27]/80 p-5 shadow-[0_40px_120px_-70px_rgba(0,0,0,0.85)]">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex flex-1 items-center gap-3">
              <div className="relative flex-1 lg:max-w-md">
                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500">
                  <Search className="size-4" />
                </span>
                <input
                  type="text"
                  value={search}
                  onChange={(event) => {
                    setSearch(event.target.value)
                    handleResetPage()
                  }}
                  placeholder="Search reports..."
                  className="w-full rounded-xl border border-white/10 bg-black/40 py-2.5 pl-10 pr-4 text-sm text-slate-100 placeholder:text-slate-500 focus:border-[color:var(--accent-alpha-40)] focus:outline-none focus:ring-1 focus:ring-[color:var(--accent-ring)]"
                />
              </div>

              <button
                type="button"
                onClick={() => setFiltersExpanded((prev) => !prev)}
                className={`inline-flex items-center gap-2 rounded-xl border px-3 py-2.5 text-sm transition ${
                  filtersExpanded || hasActiveFilters
                    ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-soft)]'
                    : 'border-white/10 bg-white/5 text-slate-300 hover:border-white/20'
                }`}
              >
                <Filter className="size-4" />
                <span className="hidden sm:inline">Filters</span>
                {hasActiveFilters && (
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-[color:var(--accent-alpha-30)] text-[10px] font-medium">
                    {[search, botFilter, instrumentFilter, timeframeFilter, startDate, endDate].filter(Boolean).length}
                  </span>
                )}
              </button>

              <button
                type="button"
                onClick={fetchReports}
                disabled={loading}
                className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-slate-300 transition hover:border-white/20 disabled:opacity-50"
              >
                <RefreshCw className={`size-4 ${loading ? 'animate-spin' : ''}`} />
              </button>
            </div>

            <div className="flex items-center gap-2">
              <div className="flex items-center rounded-xl border border-white/10 bg-white/5 p-1">
                <button
                  type="button"
                  onClick={() => setViewMode('cards')}
                  className={`rounded-lg p-2 transition ${
                    viewMode === 'cards' ? 'bg-white/10 text-slate-100' : 'text-slate-500 hover:text-slate-300'
                  }`}
                  title="Card view"
                >
                  <Grid3X3 className="size-4" />
                </button>
                <button
                  type="button"
                  onClick={() => setViewMode('table')}
                  className={`rounded-lg p-2 transition ${
                    viewMode === 'table' ? 'bg-white/10 text-slate-100' : 'text-slate-500 hover:text-slate-300'
                  }`}
                  title="Table view"
                >
                  <List className="size-4" />
                </button>
              </div>

              <div className="relative">
                <select
                  value={`${sortBy}-${sortDir}`}
                  onChange={(e) => {
                    const [field, dir] = e.target.value.split('-')
                    setSortBy(field)
                    setSortDir(dir)
                  }}
                  className="appearance-none rounded-xl border border-white/10 bg-white/5 py-2.5 pl-3 pr-8 text-sm text-slate-300 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                >
                  {SORT_OPTIONS.map((opt) => (
                    <option key={`${opt.value}-desc`} value={`${opt.value}-desc`}>
                      {opt.label} ↓
                    </option>
                  ))}
                  {SORT_OPTIONS.map((opt) => (
                    <option key={`${opt.value}-asc`} value={`${opt.value}-asc`}>
                      {opt.label} ↑
                    </option>
                  ))}
                </select>
                <ArrowUpDown className="pointer-events-none absolute right-2.5 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
              </div>
            </div>
          </div>

          {filtersExpanded && (
            <div className="flex flex-wrap items-end gap-3 rounded-2xl border border-white/5 bg-black/20 p-4">
              <div className="min-w-[140px] flex-1">
                <label className="mb-1.5 block text-[11px] uppercase tracking-[0.2em] text-slate-500">Bot</label>
                <select
                  value={botFilter}
                  onChange={(e) => {
                    setBotFilter(e.target.value)
                    handleResetPage()
                  }}
                  className="w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                >
                  {botOptions.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className="min-w-[140px] flex-1">
                <label className="mb-1.5 block text-[11px] uppercase tracking-[0.2em] text-slate-500">Instrument</label>
                <select
                  value={instrumentFilter}
                  onChange={(e) => {
                    setInstrumentFilter(e.target.value)
                    handleResetPage()
                  }}
                  className="w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                >
                  {instrumentOptions.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className="min-w-[140px] flex-1">
                <label className="mb-1.5 block text-[11px] uppercase tracking-[0.2em] text-slate-500">Timeframe</label>
                <select
                  value={timeframeFilter}
                  onChange={(e) => {
                    setTimeframeFilter(e.target.value)
                    handleResetPage()
                  }}
                  className="w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                >
                  {timeframeOptions.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className="min-w-[140px] flex-1">
                <label className="mb-1.5 block text-[11px] uppercase tracking-[0.2em] text-slate-500">Date Range</label>
                <div className="flex items-center gap-2">
                  <input
                    type="date"
                    value={startDate}
                    onChange={(e) => {
                      setStartDate(e.target.value)
                      handleResetPage()
                    }}
                    className="flex-1 rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  />
                  <span className="text-slate-600">→</span>
                  <input
                    type="date"
                    value={endDate}
                    onChange={(e) => {
                      setEndDate(e.target.value)
                      handleResetPage()
                    }}
                    className="flex-1 rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  />
                </div>
              </div>
              {hasActiveFilters && (
                <button
                  type="button"
                  onClick={handleClearFilters}
                  className="inline-flex items-center gap-1.5 rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-xs text-slate-400 transition hover:border-white/20 hover:text-slate-300"
                >
                  <X className="size-3" />
                  Clear
                </button>
              )}
            </div>
          )}
        </div>
      </section>

      {compareIds.length > 0 && (
        <section className="rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-05)] p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <GitCompare className="size-5 text-[color:var(--accent-text-soft)]" />
              <span className="text-sm font-medium text-[color:var(--accent-text-soft)]">
                {compareIds.length} report{compareIds.length !== 1 ? 's' : ''} selected for comparison
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCompareIds([])}
              >
                Clear
              </Button>
              <Button
                variant="primary"
                size="sm"
                onClick={() => setCompareOpen(true)}
                disabled={compareIds.length < 2}
                title={compareIds.length < 2 ? 'Select at least two comparison-eligible reports' : undefined}
              >
                Compare
              </Button>
            </div>
          </div>
        </section>
      )}

      <section className="rounded-3xl border border-white/8 bg-[#1a1d27]/80 p-5 shadow-[0_40px_120px_-70px_rgba(0,0,0,0.85)]">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <div className="text-xs uppercase tracking-[0.3em] text-slate-500">Report Archive</div>
            <div className="mt-1 text-sm text-slate-300">
              {total} report{total !== 1 ? 's' : ''}
              {hasActiveFilters && (
                <span className="ml-2 text-slate-500">(filtered)</span>
              )}
            </div>
          </div>
          <div className="flex items-center gap-1">
            <button
              type="button"
              disabled={page === 0}
              onClick={() => setPage((prev) => Math.max(prev - 1, 0))}
              className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:border-white/20 disabled:opacity-40"
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
              className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:border-white/20 disabled:opacity-40"
            >
              <ChevronRight className="size-4" />
            </button>
          </div>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-16">
            <div className="flex items-center gap-3">
              <RefreshCw className="size-5 animate-spin text-[color:var(--accent-text-soft)]" />
              <span className="text-sm text-slate-400">Loading reports...</span>
            </div>
          </div>
        ) : error ? (
          <div className="rounded-2xl border border-rose-500/20 bg-rose-500/10 p-6 text-center">
            <div className="text-sm text-rose-300">{error}</div>
            <button
              type="button"
              onClick={fetchReports}
              className="mt-3 rounded-lg border border-white/10 bg-white/5 px-4 py-2 text-xs text-slate-200 transition hover:border-white/20"
            >
              Retry
            </button>
          </div>
        ) : sortedReports.length === 0 ? (
          <div className="rounded-2xl border border-white/5 bg-black/20 p-12 text-center">
            <BarChart3 className="mx-auto size-10 text-slate-600" />
            <div className="mt-4 text-sm text-slate-400">
              {hasActiveFilters ? 'No reports match your filters.' : 'No reports are available yet.'}
            </div>
            {hasActiveFilters && (
              <button
                type="button"
                onClick={handleClearFilters}
                className="mt-3 text-xs text-[color:var(--accent-text-soft)] hover:underline"
              >
                Clear filters
              </button>
            )}
          </div>
        ) : viewMode === 'cards' ? (
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {sortedReports.map((report) => (
              <ReportCard
                key={report.run_id}
                report={report}
                onSelect={handleRowClick}
                isSelected={compareIds.includes(report.run_id)}
                onToggleCompare={handleToggleCompare}
              />
            ))}
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-left text-sm text-slate-200">
              <thead>
                <tr className="border-b border-white/10 text-[11px] uppercase tracking-[0.2em] text-slate-500">
                  <th className="pb-3 pr-4">Completed</th>
                  <th className="pb-3 pr-4">Bot / Strategy</th>
                  <th className="pb-3 pr-4">Instruments</th>
                  <th className="pb-3 pr-4">Date Range</th>
                  <th className="pb-3 pr-4">Run Time</th>
                  <th className="pb-3 pr-4">TF</th>
                  <th className="pb-3 pr-4 text-right">Net PnL</th>
                  <th className="pb-3 pr-4 text-right">Return</th>
                  <th className="pb-3 pr-4 text-right">Max DD</th>
                  <th className="pb-3 pr-4 text-right">Sharpe</th>
                  <th className="pb-3 text-right">Trades</th>
                </tr>
              </thead>
              <tbody>
                {sortedReports.map((report) => {
                  const pnl = report.net_pnl || 0
                  const ret = report.total_return || 0
                  const runTimeLabel = formatRunTime(report.run_duration_seconds)
                  return (
                    <tr
                      key={report.run_id}
                      onClick={() => handleRowClick(report)}
                      className="cursor-pointer border-b border-white/5 transition hover:bg-white/5"
                    >
                      <td className="py-3 pr-4 text-xs text-slate-400">{formatDateTime(report.completed_at)}</td>
                      <td className="py-3 pr-4">
                        <div className="font-medium text-slate-200">{report.bot_name || '--'}</div>
                        <div className="text-xs text-slate-500">{report.strategy_name || '--'}</div>
                      </td>
                      <td className="py-3 pr-4 text-xs" title={(report.symbols || []).join(', ')}>
                        {formatSymbols(report.symbols, 2)}
                      </td>
                      <td className="py-3 pr-4 text-xs text-slate-400">
                        {formatDateTimeShort(report.date_range?.start)} → {formatDateTimeShort(report.date_range?.end)}
                      </td>
                      <td className="py-3 pr-4 text-xs text-slate-400">{runTimeLabel}</td>
                      <td className="py-3 pr-4 text-xs">{formatTimeframe(report.timeframe)}</td>
                      <td
                        className={`py-3 pr-4 text-right font-mono ${pnl > 0 ? 'text-emerald-400' : pnl < 0 ? 'text-rose-400' : ''}`}
                      >
                        {formatCurrency(pnl)}
                      </td>
                      <td
                        className={`py-3 pr-4 text-right font-mono ${ret > 0 ? 'text-emerald-400' : ret < 0 ? 'text-rose-400' : ''}`}
                      >
                        {formatPercent(ret, 2)}
                      </td>
                      <td className="py-3 pr-4 text-right font-mono text-rose-400">
                        {formatPercent(report.max_drawdown_pct, 2)}
                      </td>
                      <td className="py-3 pr-4 text-right font-mono">{formatNumber(report.sharpe, 2)}</td>
                      <td className="py-3 text-right font-mono">{formatNumber(report.trades, 0)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <ReportModal runId={runId} open={Boolean(runId)} onClose={handleCloseModal} />
      <CompareModal runIds={compareIds} open={compareOpen} onClose={() => setCompareOpen(false)} />
    </div>
  )
}
