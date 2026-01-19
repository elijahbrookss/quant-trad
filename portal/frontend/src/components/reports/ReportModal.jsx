import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  ArrowDown,
  ArrowUp,
  Calendar,
  ChevronDown,
  ChevronUp,
  Download,
  Info,
  Layers,
  RefreshCw,
  TrendingDown,
  TrendingUp,
  X,
} from 'lucide-react'
import { createChart, LineSeries, AreaSeries, HistogramSeries, LineType } from 'lightweight-charts'
import LoadingOverlay from '../LoadingOverlay.jsx'
import { formatCurrency, formatNumber, formatPercent, formatTimeframe } from '../../utils/formatters.js'
import { reportService } from '../../services/reportService.js'
import { Badge } from '../ui/Badge.jsx'
import { getStatDescription, getChartDescription } from './statDescriptions.js'
import DecisionTable from '../bots/DecisionTrace/DecisionTable.jsx'

const formatDateTime = (value) => {
  if (!value) return '--'
  try {
    const date = new Date(value)
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch {
    return value
  }
}

const formatDateShort = (value) => {
  if (!value) return '--'
  try {
    const date = new Date(value)
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  } catch {
    return value
  }
}

// Chart configuration for dark theme
const chartOptions = {
  layout: {
    background: { type: 'solid', color: 'transparent' },
    textColor: '#64748b',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    fontSize: 10,
  },
  grid: {
    vertLines: { color: 'rgba(255, 255, 255, 0.03)' },
    horzLines: { color: 'rgba(255, 255, 255, 0.03)' },
  },
  crosshair: {
    vertLine: { color: 'rgba(255, 255, 255, 0.1)', labelBackgroundColor: '#1e293b' },
    horzLine: { color: 'rgba(255, 255, 255, 0.1)', labelBackgroundColor: '#1e293b' },
  },
  rightPriceScale: {
    borderColor: 'rgba(255, 255, 255, 0.05)',
    scaleMargins: { top: 0.1, bottom: 0.1 },
  },
  timeScale: {
    borderColor: 'rgba(255, 255, 255, 0.05)',
    timeVisible: true,
    secondsVisible: false,
  },
  handleScroll: { mouseWheel: true, pressedMouseMove: true },
  handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
}

// Lightweight chart wrapper component
const LightweightChart = ({ data, type = 'area', color = '#38bdf8', height = 160 }) => {
  const containerRef = useRef(null)
  const chartRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return

    const chart = createChart(containerRef.current, {
      ...chartOptions,
      width: containerRef.current.clientWidth,
      height,
    })
    chartRef.current = chart

    // Process data for lightweight-charts format
    const chartData = (data || [])
      .map((point) => {
        if (!point) return null
        const time = point.time
        const value = Number(point.value)
        if (!Number.isFinite(value)) return null

        // Convert ISO string to unix timestamp if needed
        let timestamp
        if (typeof time === 'string') {
          timestamp = Math.floor(new Date(time).getTime() / 1000)
        } else if (typeof time === 'number') {
          timestamp = time > 1e12 ? Math.floor(time / 1000) : time
        } else {
          return null
        }

        if (!Number.isFinite(timestamp)) return null
        return { time: timestamp, value }
      })
      .filter(Boolean)

    chartData.sort((a, b) => a.time - b.time)

    // Deduplicate identical timestamps to satisfy lightweight-charts' strict ordering.
    const uniqueChartData = []
    for (const point of chartData) {
      const last = uniqueChartData[uniqueChartData.length - 1]
      if (last && last.time === point.time) {
        uniqueChartData[uniqueChartData.length - 1] = point
      } else {
        uniqueChartData.push(point)
      }
    }

    if (uniqueChartData.length === 0) {
      chart.remove()
      return
    }

    if (type === 'area') {
      const series = chart.addSeries(AreaSeries, {
        lineColor: color,
        topColor: `${color}40`,
        bottomColor: `${color}05`,
        lineWidth: 2,
        lineType: LineType.Curved,
        priceLineVisible: false,
        lastValueVisible: false,
      })
      series.setData(uniqueChartData)
    } else if (type === 'line') {
      const series = chart.addSeries(LineSeries, {
        color,
        lineWidth: 2,
        lineType: LineType.Curved,
        priceLineVisible: false,
        lastValueVisible: false,
      })
      series.setData(uniqueChartData)
    } else if (type === 'histogram') {
      const series = chart.addSeries(HistogramSeries, {
        color,
        priceLineVisible: false,
        lastValueVisible: false,
      })
      series.setData(uniqueChartData)
    }

    chart.timeScale().fitContent()

    const handleResize = () => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth })
      }
    }

    const resizeObserver = new ResizeObserver(handleResize)
    resizeObserver.observe(containerRef.current)

    return () => {
      resizeObserver.disconnect()
      chart.remove()
      chartRef.current = null
    }
  }, [data, type, color, height])

  const hasData = Array.isArray(data) && data.length > 1

  if (!hasData) {
    return (
      <div className="flex items-center justify-center text-xs text-slate-500" style={{ height }}>
        No chart data
      </div>
    )
  }

  return <div ref={containerRef} style={{ height }} />
}

// Histogram chart for returns distribution
const ReturnsHistogram = ({ data, height = 160, color = '#38bdf8' }) => {
  const containerRef = useRef(null)
  const chartRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current || !Array.isArray(data) || data.length === 0) return

    const chart = createChart(containerRef.current, {
      ...chartOptions,
      width: containerRef.current.clientWidth,
      height,
      timeScale: { visible: false },
    })
    chartRef.current = chart

    // Convert histogram bins to chart data
    const chartData = data.map((bin, idx) => ({
      time: idx,
      value: bin?.count || 0,
      color: color,
    }))

    const series = chart.addSeries(HistogramSeries, {
      color,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    series.setData(chartData)
    chart.timeScale().fitContent()

    const handleResize = () => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth })
      }
    }

    const resizeObserver = new ResizeObserver(handleResize)
    resizeObserver.observe(containerRef.current)

    return () => {
      resizeObserver.disconnect()
      chart.remove()
      chartRef.current = null
    }
  }, [data, height, color])

  if (!Array.isArray(data) || data.length === 0) {
    return (
      <div className="flex items-center justify-center text-xs text-slate-500" style={{ height }}>
        No histogram data
      </div>
    )
  }

  return <div ref={containerRef} style={{ height }} />
}

// Info tooltip component
const InfoTooltip = ({ description }) => {
  const [show, setShow] = useState(false)
  const timeoutRef = useRef(null)

  const handleMouseEnter = () => {
    timeoutRef.current = setTimeout(() => setShow(true), 200)
  }

  const handleMouseLeave = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current)
    setShow(false)
  }

  if (!description) return null

  return (
    <div className="relative inline-flex">
      <button
        type="button"
        className="ml-1 inline-flex items-center text-slate-600 transition hover:text-slate-400"
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        onFocus={handleMouseEnter}
        onBlur={handleMouseLeave}
        aria-label="More info"
      >
        <Info className="size-3" />
      </button>
      {show && (
        <div className="absolute bottom-full left-1/2 z-50 mb-2 w-56 -translate-x-1/2 rounded-lg border border-white/10 bg-slate-900 p-2.5 text-xs text-slate-300 shadow-xl">
          {description}
          <div className="absolute left-1/2 top-full -translate-x-1/2 border-4 border-transparent border-t-slate-900" />
        </div>
      )}
    </div>
  )
}

// Stat row component for grouped sections
const StatRow = ({ label, value, tone, mono = false, statKey }) => {
  const toneClasses = {
    positive: 'text-emerald-400',
    negative: 'text-rose-400',
    neutral: 'text-slate-200',
    warning: 'text-amber-400',
  }

  const description = statKey ? getStatDescription(statKey) : null

  return (
    <div className="flex items-center justify-between py-1.5">
      <span className="flex items-center text-xs text-slate-500">
        {label}
        <InfoTooltip description={description} />
      </span>
      <span className={`text-sm ${mono ? 'font-mono' : ''} ${tone ? toneClasses[tone] : 'text-slate-200'}`}>
        {value}
      </span>
    </div>
  )
}

// Stat section component
const StatSection = ({ title, children }) => (
  <div>
    <div className="mb-2 text-[10px] font-medium uppercase tracking-[0.2em] text-slate-600">{title}</div>
    <div className="divide-y divide-white/5">{children}</div>
  </div>
)

const Heatmap = ({ data }) => {
  if (!Array.isArray(data) || data.length === 0) {
    return <div className="text-xs text-slate-500">No monthly returns</div>
  }
  return (
    <div className="grid grid-cols-4 gap-2 text-xs sm:grid-cols-6">
      {data.map((entry) => {
        const value = Number(entry?.return)
        const intensity = Math.min(Math.abs(value || 0) * 10, 1)
        const tone = Number.isFinite(value)
          ? value >= 0
            ? `rgba(16, 185, 129, ${0.15 + intensity * 0.35})`
            : `rgba(244, 63, 94, ${0.15 + intensity * 0.35})`
          : 'rgba(100, 116, 139, 0.2)'
        const textTone = Number.isFinite(value)
          ? value >= 0
            ? 'text-emerald-300'
            : 'text-rose-300'
          : 'text-slate-400'
        return (
          <div
            key={entry?.month}
            className="rounded-lg px-2 py-2"
            style={{ backgroundColor: tone }}
          >
            <div className="text-[9px] uppercase tracking-[0.15em] text-slate-400">{entry?.month}</div>
            <div className={`mt-0.5 font-semibold ${textTone}`}>{formatPercent(entry?.return, 1)}</div>
          </div>
        )
      })}
    </div>
  )
}

const TradeTable = ({ trades, expanded, onToggle }) => {
  const [sortBy, setSortBy] = useState('exit_time')
  const [sortDir, setSortDir] = useState('desc')

  const sortedTrades = useMemo(() => {
    if (!trades?.length) return []
    return [...trades].sort((a, b) => {
      let aVal = a[sortBy]
      let bVal = b[sortBy]
      if (sortBy === 'exit_time' || sortBy === 'entry_time') {
        aVal = aVal ? new Date(aVal).getTime() : 0
        bVal = bVal ? new Date(bVal).getTime() : 0
      }
      if (aVal == null) aVal = sortDir === 'desc' ? -Infinity : Infinity
      if (bVal == null) bVal = sortDir === 'desc' ? -Infinity : Infinity
      return sortDir === 'desc' ? bVal - aVal : aVal - bVal
    })
  }, [trades, sortBy, sortDir])

  const handleSort = (field) => {
    if (sortBy === field) {
      setSortDir((prev) => (prev === 'desc' ? 'asc' : 'desc'))
    } else {
      setSortBy(field)
      setSortDir('desc')
    }
  }

  const displayTrades = expanded ? sortedTrades : sortedTrades.slice(0, 5)

  if (!trades?.length) {
    return <div className="py-6 text-center text-xs text-slate-500">No trades recorded</div>
  }

  return (
    <div className="space-y-3">
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-xs">
          <thead>
            <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.15em] text-slate-500">
              <th
                className="cursor-pointer pb-2 pr-3 text-left hover:text-slate-300"
                onClick={() => handleSort('exit_time')}
              >
                Exit {sortBy === 'exit_time' && (sortDir === 'desc' ? '↓' : '↑')}
              </th>
              <th className="pb-2 pr-3 text-left">Symbol</th>
              <th className="pb-2 pr-3 text-left">Dir</th>
              <th
                className="cursor-pointer pb-2 pr-3 text-right hover:text-slate-300"
                onClick={() => handleSort('net_pnl')}
              >
                Net PnL {sortBy === 'net_pnl' && (sortDir === 'desc' ? '↓' : '↑')}
              </th>
              <th className="pb-2 text-right">Fees</th>
            </tr>
          </thead>
          <tbody>
            {displayTrades.map((trade, idx) => {
              const pnl = trade.net_pnl || 0
              const isProfit = pnl > 0
              const isLoss = pnl < 0
              return (
                <tr key={trade.trade_id || idx} className="border-b border-white/5">
                  <td className="py-2 pr-3 text-slate-400">{formatDateShort(trade.exit_time)}</td>
                  <td className="py-2 pr-3 font-mono text-slate-200">{trade.symbol || '--'}</td>
                  <td className="py-2 pr-3">
                    <span
                      className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-medium uppercase ${
                        trade.direction === 'long'
                          ? 'bg-emerald-500/15 text-emerald-300'
                          : trade.direction === 'short'
                            ? 'bg-rose-500/15 text-rose-300'
                            : 'bg-slate-500/15 text-slate-400'
                      }`}
                    >
                      {trade.direction === 'long' ? <ArrowUp className="size-2.5" /> : <ArrowDown className="size-2.5" />}
                      {trade.direction || '--'}
                    </span>
                  </td>
                  <td
                    className={`py-2 pr-3 text-right font-mono ${isProfit ? 'text-emerald-400' : isLoss ? 'text-rose-400' : 'text-slate-300'}`}
                  >
                    {formatCurrency(pnl)}
                  </td>
                  <td className="py-2 text-right font-mono text-slate-500">{formatCurrency(trade.fees_paid)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {sortedTrades.length > 5 && (
        <button
          type="button"
          onClick={onToggle}
          className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-white/5 bg-white/5 py-2 text-xs text-slate-400 transition hover:border-white/10 hover:text-slate-300"
        >
          {expanded ? (
            <>
              <ChevronUp className="size-3" />
              Show less
            </>
          ) : (
            <>
              <ChevronDown className="size-3" />
              Show all {sortedTrades.length} trades
            </>
          )}
        </button>
      )}
    </div>
  )
}

const TabButton = ({ active, children, onClick }) => (
  <button
    type="button"
    onClick={onClick}
    className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
      active
        ? 'bg-white/10 text-slate-100'
        : 'text-slate-400 hover:bg-white/5 hover:text-slate-300'
    }`}
  >
    {children}
  </button>
)

export function ReportModal({ runId, open, onClose }) {
  const [report, setReport] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [activeTab, setActiveTab] = useState('overview')
  const [tradesExpanded, setTradesExpanded] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [exportToast, setExportToast] = useState(null)
  const exportToastTimer = useRef(null)

  const fetchReport = useCallback(async () => {
    if (!runId) return
    setLoading(true)
    setError(null)
    try {
      const payload = await reportService.getReport(runId)
      setReport(payload)
    } catch (err) {
      setError(err?.message || 'Failed to load report')
    } finally {
      setLoading(false)
    }
  }, [runId])

  const handleExport = useCallback(async () => {
    if (!runId || exporting) return
    setExporting(true)
    setExportToast(null)
    try {
      const { blob, filename } = await reportService.exportReport(runId)
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename || `run_${runId}_llm_export.zip`
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(url)
    } catch (err) {
      const message = err?.message || 'Failed to export report'
      setExportToast(message)
      if (exportToastTimer.current) {
        clearTimeout(exportToastTimer.current)
      }
      exportToastTimer.current = setTimeout(() => {
        setExportToast(null)
      }, 4200)
    } finally {
      setExporting(false)
    }
  }, [exporting, runId])

  useEffect(() => {
    if (open) {
      fetchReport()
      setActiveTab('overview')
      setTradesExpanded(false)
    }
  }, [fetchReport, open])

  useEffect(() => {
    return () => {
      if (exportToastTimer.current) {
        clearTimeout(exportToastTimer.current)
      }
    }
  }, [])

  if (!open) return null

  const summary = report?.summary || {}
  const charts = report?.charts || {}
  const analytics = report?.trade_analytics || {}
  const runConfig = report?.run_config || {}
  const trades = report?.tables?.trades || []
  const decisionLedger = report?.decision_ledger || []
  const balances = runConfig?.wallet_start?.balances || {}
  const balanceEntries = Object.entries(balances)
  const startingBalanceLabel = balanceEntries.length
    ? balanceEntries.map(([currency, amount]) => `${formatNumber(amount, 0)} ${currency}`).join(', ')
    : '--'

  const pnlTone = (summary.net_pnl || 0) > 0 ? 'positive' : (summary.net_pnl || 0) < 0 ? 'negative' : 'neutral'
  const returnTone = (summary.total_return || 0) > 0 ? 'positive' : (summary.total_return || 0) < 0 ? 'negative' : 'neutral'

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/80 px-4 py-8 backdrop-blur-sm">
      <div className="relative w-full max-w-5xl rounded-2xl border border-white/10 bg-[#0d1117] shadow-2xl">
        {exportToast ? (
          <div
            role="status"
            aria-live="polite"
            className="absolute right-5 top-5 z-20 rounded-xl border border-rose-500/30 bg-rose-500/15 px-4 py-2 text-xs text-rose-200 shadow-lg"
          >
            {exportToast}
          </div>
        ) : null}
        <header className="sticky top-0 z-10 flex flex-wrap items-start justify-between gap-4 rounded-t-2xl border-b border-white/10 bg-[#0d1117]/95 p-5 backdrop-blur">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Backtest Report</span>
              <Badge variant="success" size="sm">Completed</Badge>
            </div>
            <h3 className="mt-2 truncate text-xl font-semibold text-slate-100">
              {report?.bot_name || 'Bot'} <span className="text-slate-500">•</span> {report?.strategy_name || 'Strategy'}
            </h3>
            <div className="mt-1 flex items-center gap-3 text-xs text-slate-400">
              <span className="font-mono">{report?.run_id?.slice(0, 8) || '--'}</span>
              <span className="text-slate-600">•</span>
              <span className="flex items-center gap-1">
                <Calendar className="size-3" />
                {formatDateTime(report?.completed_at)}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              disabled={exporting || !runId}
              onClick={handleExport}
              className={`inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/5 px-4 py-2.5 text-xs text-slate-200 transition hover:border-white/20 hover:bg-white/10 ${
                exporting ? 'opacity-70' : ''
              }`}
            >
              {exporting ? (
                <RefreshCw className="size-3.5 animate-spin" />
              ) : (
                <Download className="size-3.5" />
              )}
              {exporting ? 'Exporting...' : 'Export'}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:border-white/20 hover:bg-white/10"
              aria-label="Close report"
            >
              <X className="size-4" />
            </button>
          </div>
        </header>

        {loading ? (
          <div className="relative h-80">
            <LoadingOverlay message="Loading report..." />
          </div>
        ) : error ? (
          <div className="p-6">
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 p-4 text-sm text-rose-300">
              {error}
            </div>
            <button
              type="button"
              onClick={fetchReport}
              className="mt-4 rounded-lg border border-white/10 bg-white/5 px-4 py-2 text-xs text-slate-200 transition hover:border-white/20"
            >
              Retry
            </button>
          </div>
        ) : (
          <>
            <div className="border-b border-white/5 px-5">
              <div className="flex gap-1">
                <TabButton active={activeTab === 'overview'} onClick={() => setActiveTab('overview')}>
                  Overview
                </TabButton>
                <TabButton active={activeTab === 'charts'} onClick={() => setActiveTab('charts')}>
                  Charts
                </TabButton>
                <TabButton active={activeTab === 'trades'} onClick={() => setActiveTab('trades')}>
                  Trades ({trades.length})
                </TabButton>
                <TabButton active={activeTab === 'config'} onClick={() => setActiveTab('config')}>
                  Config
                </TabButton>
              </div>
            </div>

            <div className="p-5">
              {activeTab === 'overview' && (
                <div className="space-y-6">
                  {/* Key metrics header */}
                  <div className="grid grid-cols-2 gap-6 border-b border-white/5 pb-6 lg:grid-cols-4">
                    <div>
                      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        <TrendingUp className="size-3" />
                        Total Return
                        <InfoTooltip description={getStatDescription('total_return')} />
                      </div>
                      <div className={`mt-1 text-2xl font-semibold tabular-nums ${returnTone === 'positive' ? 'text-emerald-400' : returnTone === 'negative' ? 'text-rose-400' : 'text-slate-100'}`}>
                        {formatPercent(summary.total_return, 2)}
                      </div>
                    </div>
                    <div>
                      <div className="flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Net PnL
                        <InfoTooltip description={getStatDescription('net_pnl')} />
                      </div>
                      <div className={`mt-1 text-2xl font-semibold tabular-nums ${pnlTone === 'positive' ? 'text-emerald-400' : pnlTone === 'negative' ? 'text-rose-400' : 'text-slate-100'}`}>
                        {formatCurrency(summary.net_pnl)}
                      </div>
                    </div>
                    <div>
                      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        <TrendingDown className="size-3" />
                        Max Drawdown
                        <InfoTooltip description={getStatDescription('max_drawdown')} />
                      </div>
                      <div className="mt-1 text-2xl font-semibold tabular-nums text-rose-400">
                        {formatPercent(summary.max_drawdown_pct, 2)}
                      </div>
                    </div>
                    <div>
                      <div className="flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Sharpe Ratio
                        <InfoTooltip description={getStatDescription('sharpe')} />
                      </div>
                      <div className="mt-1 text-2xl font-semibold tabular-nums text-slate-100">
                        {formatNumber(summary.sharpe, 2)}
                      </div>
                    </div>
                  </div>

                  {/* Stats in grouped sections */}
                  <div className="grid gap-6 lg:grid-cols-3">
                    <StatSection title="Risk Metrics">
                      <StatRow label="Sortino" value={formatNumber(summary.sortino, 2)} statKey="sortino" />
                      <StatRow label="Calmar" value={formatNumber(summary.calmar, 2)} statKey="calmar" />
                      <StatRow label="Volatility" value={formatPercent(summary.annualized_volatility, 2)} statKey="volatility" />
                      <StatRow label="Exposure" value={formatPercent(summary.exposure_pct, 1)} statKey="exposure" />
                    </StatSection>

                    <StatSection title="Trade Performance">
                      <StatRow
                        label="Win Rate"
                        value={formatPercent(summary.win_rate, 1)}
                        tone={(summary.win_rate || 0) >= 0.5 ? 'positive' : 'warning'}
                        statKey="win_rate"
                      />
                      <StatRow label="Profit Factor" value={formatNumber(summary.profit_factor, 2)} statKey="profit_factor" />
                      <StatRow label="Expectancy" value={formatCurrency(summary.expectancy)} mono statKey="expectancy" />
                      <StatRow label="Payoff Ratio" value={formatNumber(summary.payoff_ratio, 2)} statKey="payoff_ratio" />
                    </StatSection>

                    <StatSection title="Summary">
                      <StatRow label="Total Trades" value={formatNumber(summary.total_trades, 0)} statKey="total_trades" />
                      <StatRow label="Avg Win" value={formatCurrency(summary.avg_win)} tone="positive" mono statKey="avg_win" />
                      <StatRow label="Avg Loss" value={formatCurrency(summary.avg_loss)} tone="negative" mono statKey="avg_loss" />
                      <StatRow label="Fees Paid" value={formatCurrency(summary.fees)} mono statKey="fees" />
                    </StatSection>
                  </div>

                  {/* Equity curve */}
                  <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                    <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                      Equity Curve
                      <InfoTooltip description={getChartDescription('equity_curve')} />
                    </div>
                    <LightweightChart data={charts.equity_curve} type="area" color="#38bdf8" height={180} />
                  </div>

                  {/* Direction breakdown */}
                  <div className="grid gap-4 lg:grid-cols-2">
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Long vs Short
                        <InfoTooltip description={getChartDescription('direction_breakdown')} />
                      </div>
                      <div className="space-y-2">
                        {['long', 'short'].map((side) => {
                          const data = analytics?.direction_breakdown?.[side] || {}
                          const pnl = data.net_pnl || 0
                          return (
                            <div key={side} className="flex items-center justify-between py-2">
                              <div className="flex items-center gap-2">
                                <span
                                  className={`inline-flex items-center gap-1 rounded px-2 py-1 text-xs font-medium uppercase ${
                                    side === 'long'
                                      ? 'bg-emerald-500/15 text-emerald-300'
                                      : 'bg-rose-500/15 text-rose-300'
                                  }`}
                                >
                                  {side === 'long' ? <ArrowUp className="size-3" /> : <ArrowDown className="size-3" />}
                                  {side}
                                </span>
                                <span className="text-xs text-slate-500">{data.count || 0} trades</span>
                              </div>
                              <span
                                className={`font-mono text-sm ${pnl > 0 ? 'text-emerald-400' : pnl < 0 ? 'text-rose-400' : 'text-slate-300'}`}
                              >
                                {formatCurrency(pnl)}
                              </span>
                            </div>
                          )
                        })}
                      </div>
                    </div>

                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Per Instrument
                        <InfoTooltip description={getChartDescription('instrument_breakdown')} />
                      </div>
                      <div className="space-y-1">
                        {(analytics.instrument_breakdown || []).length === 0 ? (
                          <div className="py-4 text-center text-xs text-slate-500">No instrument breakdown</div>
                        ) : (
                          analytics.instrument_breakdown.slice(0, 5).map((entry) => {
                            const pnl = entry.net_pnl || 0
                            return (
                              <div key={entry.symbol} className="flex items-center justify-between py-1.5">
                                <div className="flex items-center gap-2">
                                  <span className="font-mono text-sm text-slate-200">{entry.symbol}</span>
                                  <span className="text-xs text-slate-500">{entry.trades} trades</span>
                                </div>
                                <span
                                  className={`font-mono text-sm ${pnl > 0 ? 'text-emerald-400' : pnl < 0 ? 'text-rose-400' : 'text-slate-300'}`}
                                >
                                  {formatCurrency(pnl)}
                                </span>
                              </div>
                            )
                          })
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {activeTab === 'charts' && (
                <div className="space-y-6">
                  <div className="grid gap-4 lg:grid-cols-2">
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Equity Curve
                        <InfoTooltip description={getChartDescription('equity_curve')} />
                      </div>
                      <LightweightChart data={charts.equity_curve} type="area" color="#38bdf8" height={180} />
                    </div>
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Drawdown
                        <InfoTooltip description={getChartDescription('drawdown_curve')} />
                      </div>
                      <LightweightChart data={charts.drawdown_curve} type="area" color="#f97316" height={180} />
                    </div>
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Rolling Sharpe (20-day)
                        <InfoTooltip description={getChartDescription('rolling_sharpe')} />
                      </div>
                      <LightweightChart data={charts.rolling_sharpe} type="line" color="#a3e635" height={180} />
                    </div>
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Returns Distribution
                        <InfoTooltip description={getChartDescription('returns_histogram')} />
                      </div>
                      <ReturnsHistogram data={charts.returns_histogram} color="#38bdf8" height={180} />
                    </div>
                  </div>

                  <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                    <div className="mb-4 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                      Monthly Returns
                      <InfoTooltip description={getChartDescription('monthly_returns')} />
                    </div>
                    <Heatmap data={charts.monthly_returns} />
                  </div>

                  <div className="grid gap-4 lg:grid-cols-2">
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Hold Time Distribution
                        <InfoTooltip description={getChartDescription('hold_time_histogram')} />
                      </div>
                      <ReturnsHistogram data={analytics.hold_time_histogram} color="#f59e0b" height={140} />
                    </div>
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Win/Loss Streaks
                        <InfoTooltip description={getChartDescription('win_loss_streaks')} />
                      </div>
                      <div className="space-y-1">
                        {(analytics.win_loss_streaks || []).length === 0 ? (
                          <div className="py-4 text-center text-xs text-slate-500">No streak data</div>
                        ) : (
                          analytics.win_loss_streaks.slice(0, 6).map((streak, idx) => (
                            <div
                              key={`${streak.type}-${idx}`}
                              className="flex items-center justify-between py-1.5"
                            >
                              <span
                                className={`text-xs font-medium capitalize ${streak.type === 'win' ? 'text-emerald-400' : 'text-rose-400'}`}
                              >
                                {streak.type}
                              </span>
                              <span className="font-mono text-sm text-slate-200">{streak.length} trades</span>
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {activeTab === 'trades' && (
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div className="text-sm text-slate-400">
                      {trades.length} total trade{trades.length !== 1 ? 's' : ''}
                    </div>
                  </div>
                  <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                    <TradeTable trades={trades} expanded={tradesExpanded} onToggle={() => setTradesExpanded((prev) => !prev)} />
                  </div>
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="text-sm text-slate-400">
                        Decision Ledger ({decisionLedger.length})
                      </div>
                    </div>
                    {decisionLedger.length ? (
                      <DecisionTable ledgerEvents={decisionLedger} />
                    ) : (
                      <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4 text-xs text-slate-500">
                        No decision ledger recorded for this run.
                      </div>
                    )}
                  </div>
                </div>
              )}

              {activeTab === 'config' && (
                <div className="space-y-4">
                  <div className="grid gap-4 lg:grid-cols-2">
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 flex items-center gap-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        <Layers className="size-3" />
                        Run Configuration
                      </div>
                      <div className="space-y-0 divide-y divide-white/5 text-sm">
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Starting Balance</span>
                          <span className="font-mono text-slate-200">{startingBalanceLabel}</span>
                        </div>
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Date Range</span>
                          <span className="text-slate-200">
                            {formatDateShort(runConfig?.date_range?.start)} → {formatDateShort(runConfig?.date_range?.end)}
                          </span>
                        </div>
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Symbols</span>
                          <span className="font-mono text-slate-200">{(runConfig?.symbols || []).join(', ') || '--'}</span>
                        </div>
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Timeframe</span>
                          <span className="text-slate-200">{formatTimeframe(runConfig?.timeframe)}</span>
                        </div>
                      </div>
                    </div>

                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">Risk & Fees</div>
                      <div className="space-y-0 divide-y divide-white/5 text-sm">
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Fee Model</span>
                          <span className="text-slate-200">{runConfig?.fee_model || 'Default'}</span>
                        </div>
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Slippage Model</span>
                          <span className="text-slate-200">{runConfig?.slippage_model || 'Default'}</span>
                        </div>
                        <div className="flex items-center justify-between py-2">
                          <span className="text-slate-400">Risk Settings</span>
                          <span className="text-slate-200">
                            {Object.keys(runConfig?.risk_settings || {}).length ? 'Configured' : 'Default'}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>

                  {runConfig?.strategies?.length > 0 && (
                    <div className="rounded-lg border border-white/5 bg-white/[0.02] p-4">
                      <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        Strategies ({runConfig.strategies.length})
                      </div>
                      <div className="space-y-2">
                        {runConfig.strategies.map((strategy) => (
                          <div key={strategy.id} className="rounded-lg bg-white/5 p-3">
                            <div className="flex items-center justify-between">
                              <span className="font-medium text-slate-200">{strategy.name}</span>
                              <span className="font-mono text-xs text-slate-500">{strategy.id?.slice(0, 8)}</span>
                            </div>
                            <div className="mt-1 flex items-center gap-3 text-xs text-slate-500">
                              <span>{formatTimeframe(strategy.timeframe)}</span>
                              <span>•</span>
                              <span>{strategy.datasource}</span>
                              <span>•</span>
                              <span>{strategy.exchange}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
