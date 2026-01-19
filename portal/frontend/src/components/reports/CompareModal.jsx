import { useCallback, useEffect, useMemo, useState } from 'react'
import { Calendar, GitCompare, X } from 'lucide-react'
import { reportService } from '../../services/reportService.js'
import { Badge } from '../ui/Badge.jsx'
import { formatCurrency, formatNumber, formatPercent, formatTimeframe } from '../../utils/formatters.js'
import LoadingOverlay from '../LoadingOverlay.jsx'

const formatDateTime = (value) => {
  if (!value) return '--'
  try {
    const date = new Date(value)
    return date.toLocaleDateString(undefined, {
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

const formatDateShort = (value) => {
  if (!value) return '--'
  try {
    const date = new Date(value)
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
  } catch {
    return value
  }
}

const formatDuration = (start, end) => {
  if (!start || !end) return '--'
  const startMs = new Date(start).getTime()
  const endMs = new Date(end).getTime()
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return '--'
  const totalMinutes = Math.max(Math.round((endMs - startMs) / 60000), 0)
  const hours = Math.floor(totalMinutes / 60)
  const minutes = totalMinutes % 60
  if (hours === 0) return `${minutes}m`
  return `${hours}h ${minutes}m`
}

const formatRiskSettings = (settings = {}) => {
  const entries = Object.entries(settings)
  if (!entries.length) return '--'
  return entries.map(([key, value]) => `${key}:${value}`).join(', ')
}

const MetricRow = ({ label, formatter, values, baselineIndex = 0, preferLower = false }) => {
  const base = values[baselineIndex]
  return (
    <tr className="border-b border-white/5 text-xs">
      <td className="py-2 pr-3 text-[10px] uppercase tracking-[0.15em] text-slate-500">{label}</td>
      {values.map((value, idx) => {
        const formatted = formatter ? formatter(value) : value ?? '--'
        let delta = null
        let tone = 'text-slate-300'
        if (idx !== baselineIndex && Number.isFinite(value) && Number.isFinite(base)) {
          delta = value - base
          const isBetter = preferLower ? delta < 0 : delta > 0
          if (delta !== 0) tone = isBetter ? 'text-emerald-400' : 'text-rose-400'
        }
        return (
          <td key={`${label}-${idx}`} className="py-2 pr-3 text-right">
            <div className="font-mono text-slate-200">{formatted ?? '--'}</div>
            {delta !== null && (
              <div className={`text-[10px] ${tone}`}>
                {delta > 0 ? '+' : ''}
                {formatter ? formatter(delta) : delta}
              </div>
            )}
          </td>
        )
      })}
    </tr>
  )
}

const SummaryTable = ({ reports }) => {
  const rows = [
    { label: 'Mode', values: reports.map((r) => r.run_type || '--') },
    { label: 'Bot', values: reports.map((r) => r.bot_name || '--') },
    { label: 'Strategy', values: reports.map((r) => r.strategy_name || '--') },
    {
      label: 'Date Range',
      values: reports.map((r) => `${formatDateShort(r.run_config?.date_range?.start)} → ${formatDateShort(r.run_config?.date_range?.end)}`),
    },
    { label: 'Timeframe', values: reports.map((r) => formatTimeframe(r.run_config?.timeframe || r.timeframe)) },
    { label: 'Symbols', values: reports.map((r) => (r.run_config?.symbols || r.symbols || []).join(', ') || '--') },
    { label: 'Datasource', values: reports.map((r) => r.datasource || '--') },
    { label: 'Exchange', values: reports.map((r) => r.exchange || '--') },
    {
      label: 'Runtime',
      values: reports.map((r) => formatDuration(r.started_at, r.ended_at || r.completed_at)),
    },
    { label: 'Fee Model', values: reports.map((r) => r.run_config?.fee_model || '--') },
    { label: 'Slippage', values: reports.map((r) => r.run_config?.slippage_model || '--') },
    { label: 'Risk Settings', values: reports.map((r) => formatRiskSettings(r.run_config?.risk_settings)) },
  ]

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-left text-xs text-slate-200">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.2em] text-slate-500">
            <th className="pb-2 pr-3">Run Snapshot</th>
            {reports.map((report) => (
              <th key={report.run_id} className="pb-2 pr-3 text-right">
                {report.run_id?.slice(0, 8) || '--'}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.label} className="border-b border-white/5">
              <td className="py-2 pr-3 text-[10px] uppercase tracking-[0.15em] text-slate-500">{row.label}</td>
              {row.values.map((value, idx) => (
                <td key={`${row.label}-${idx}`} className="py-2 pr-3 text-right text-slate-200">
                  {value || '--'}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const MetricsTable = ({ reports }) => {
  const valuesFor = (key) => reports.map((r) => r.summary?.[key])
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-left text-xs text-slate-200">
        <thead>
          <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.2em] text-slate-500">
            <th className="pb-2 pr-3">Metric</th>
            {reports.map((report) => (
              <th key={report.run_id} className="pb-2 pr-3 text-right">
                {report.run_id?.slice(0, 8) || '--'}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          <MetricRow label="Net PnL" formatter={formatCurrency} values={valuesFor('net_pnl')} />
          <MetricRow label="Total Return" formatter={(v) => formatPercent(v, 2)} values={valuesFor('total_return')} />
          <MetricRow label="CAGR" formatter={(v) => formatPercent(v, 2)} values={valuesFor('cagr')} />
          <MetricRow label="Max Drawdown" formatter={(v) => formatPercent(v, 2)} values={valuesFor('max_drawdown_pct')} preferLower />
          <MetricRow label="Sharpe" formatter={(v) => formatNumber(v, 2)} values={valuesFor('sharpe')} />
          <MetricRow label="Sortino" formatter={(v) => formatNumber(v, 2)} values={valuesFor('sortino')} />
          <MetricRow label="Profit Factor" formatter={(v) => formatNumber(v, 2)} values={valuesFor('profit_factor')} />
          <MetricRow label="Win Rate" formatter={(v) => formatPercent(v, 1)} values={valuesFor('win_rate')} />
          <MetricRow label="Total Trades" formatter={(v) => formatNumber(v, 0)} values={valuesFor('total_trades')} />
          <MetricRow label="Avg Win" formatter={formatCurrency} values={valuesFor('avg_win')} />
          <MetricRow label="Avg Loss" formatter={formatCurrency} values={valuesFor('avg_loss')} />
          <MetricRow label="Fees Paid" formatter={formatCurrency} values={valuesFor('fees')} preferLower />
          <MetricRow label="Exposure" formatter={(v) => formatPercent(v, 1)} values={valuesFor('exposure_pct')} />
          <MetricRow label="Expectancy" formatter={formatCurrency} values={valuesFor('expectancy')} />
        </tbody>
      </table>
    </div>
  )
}

const formatDelta = (value, formatter) => {
  if (!Number.isFinite(value)) return '--'
  const formatted = formatter ? formatter(value) : value
  return value > 0 ? `+${formatted}` : formatted
}

const formatJson = (value) => {
  if (value === undefined) return '--'
  if (value === null) return 'null'
  if (typeof value === 'string') return value
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

const diffLines = (baseText, compareText) => {
  const left = (baseText || '').split('\n')
  const right = (compareText || '').split('\n')
  const leftLen = left.length
  const rightLen = right.length

  const dp = Array.from({ length: leftLen + 1 }, () => new Array(rightLen + 1).fill(0))
  for (let i = 1; i <= leftLen; i += 1) {
    for (let j = 1; j <= rightLen; j += 1) {
      if (left[i - 1] === right[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1] + 1
      } else {
        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1])
      }
    }
  }

  const alignedLeft = []
  const alignedRight = []
  let i = leftLen
  let j = rightLen
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && left[i - 1] === right[j - 1]) {
      alignedLeft.unshift({ type: 'same', text: left[i - 1] })
      alignedRight.unshift({ type: 'same', text: right[j - 1] })
      i -= 1
      j -= 1
    } else if (j > 0 && (i === 0 || dp[i][j - 1] >= dp[i - 1][j])) {
      alignedLeft.unshift({ type: 'empty', text: '' })
      alignedRight.unshift({ type: 'add', text: right[j - 1] })
      j -= 1
    } else {
      alignedLeft.unshift({ type: 'remove', text: left[i - 1] })
      alignedRight.unshift({ type: 'empty', text: '' })
      i -= 1
    }
  }

  return { left: alignedLeft, right: alignedRight }
}

const diffChars = (baseText, compareText) => {
  const left = baseText.split('')
  const right = compareText.split('')
  const leftLen = left.length
  const rightLen = right.length

  const dp = Array.from({ length: leftLen + 1 }, () => new Array(rightLen + 1).fill(0))
  for (let i = 1; i <= leftLen; i += 1) {
    for (let j = 1; j <= rightLen; j += 1) {
      if (left[i - 1] === right[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1] + 1
      } else {
        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1])
      }
    }
  }

  const leftSegments = []
  const rightSegments = []
  let i = leftLen
  let j = rightLen
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && left[i - 1] === right[j - 1]) {
      leftSegments.unshift({ type: 'same', text: left[i - 1] })
      rightSegments.unshift({ type: 'same', text: right[j - 1] })
      i -= 1
      j -= 1
    } else if (j > 0 && (i === 0 || dp[i][j - 1] >= dp[i - 1][j])) {
      rightSegments.unshift({ type: 'add', text: right[j - 1] })
      j -= 1
    } else {
      leftSegments.unshift({ type: 'remove', text: left[i - 1] })
      i -= 1
    }
  }

  return { leftSegments, rightSegments }
}

const buildDiffRows = (baseText, compareText) => {
  const { left, right } = diffLines(baseText, compareText)
  const rows = []

  let idx = 0
  while (idx < left.length) {
    if (left[idx].type === 'remove' && right[idx].type === 'empty') {
      const removeBlock = []
      while (idx < left.length && left[idx].type === 'remove' && right[idx].type === 'empty') {
        removeBlock.push(left[idx].text)
        idx += 1
      }
      const addBlock = []
      let addIdx = idx
      while (addIdx < left.length && left[addIdx].type === 'empty' && right[addIdx].type === 'add') {
        addBlock.push(right[addIdx].text)
        addIdx += 1
      }
      const pairCount = Math.min(removeBlock.length, addBlock.length)
      for (let i = 0; i < pairCount; i += 1) {
        rows.push({
          kind: 'change',
          left: { type: 'remove', text: removeBlock[i] },
          right: { type: 'add', text: addBlock[i] },
        })
      }
      for (let i = pairCount; i < removeBlock.length; i += 1) {
        rows.push({
          kind: 'remove',
          left: { type: 'remove', text: removeBlock[i] },
          right: { type: 'empty', text: '' },
        })
      }
      for (let i = pairCount; i < addBlock.length; i += 1) {
        rows.push({
          kind: 'add',
          left: { type: 'empty', text: '' },
          right: { type: 'add', text: addBlock[i] },
        })
      }
      idx = addIdx
      continue
    }

    rows.push({
      kind: left[idx].type === 'same' && right[idx].type === 'same' ? 'same' : left[idx].type || right[idx].type,
      left: left[idx],
      right: right[idx],
    })
    idx += 1
  }

  return rows.map((row) => {
    if (row.kind === 'change' && row.left.text && row.right.text) {
      const segments = diffChars(row.left.text, row.right.text)
      return { ...row, leftSegments: segments.leftSegments, rightSegments: segments.rightSegments }
    }
    return row
  })
}

const JsonDiffBlock = ({ base, compare }) => {
  const baseText = formatJson(base)
  const compareText = formatJson(compare)
  const rows = useMemo(() => buildDiffRows(baseText, compareText), [baseText, compareText])

  const renderSegments = (segments, side) => {
    return segments.map((segment, idx) => {
      let className = 'text-slate-200'
      if (segment.type === 'remove' && side === 'left') {
        className = 'bg-rose-500/20 text-rose-300'
      }
      if (segment.type === 'add' && side === 'right') {
        className = 'bg-emerald-500/20 text-emerald-300'
      }
      return (
        <span key={`${side}-seg-${idx}`} className={className}>
          {segment.text}
        </span>
      )
    })
  }

  const renderLine = (line, row, idx, side) => (
    <div
      key={`${side}-${idx}`}
      className={`whitespace-pre-wrap break-words ${
        line.type === 'same'
          ? 'text-slate-200'
          : line.type === 'add'
            ? 'bg-emerald-500/10 text-emerald-300'
            : line.type === 'remove'
              ? 'bg-rose-500/10 text-rose-300'
              : 'text-slate-700'
      }`}
    >
      {row?.kind === 'change' && side === 'left' && row.leftSegments
        ? renderSegments(row.leftSegments, 'left')
        : row?.kind === 'change' && side === 'right' && row.rightSegments
          ? renderSegments(row.rightSegments, 'right')
          : line.text || ' '}
    </div>
  )

  return (
    <div className="grid gap-2 lg:grid-cols-2">
      <div className="rounded-md border border-white/5 bg-black/30 p-2">
        <div className="mb-1 text-[9px] uppercase tracking-[0.2em] text-slate-600">Base</div>
        <pre className="max-h-56 overflow-auto font-mono text-[11px]">
          {rows.map((row, idx) => renderLine(row.left, row, idx, 'left'))}
        </pre>
      </div>
      <div className="rounded-md border border-white/5 bg-black/30 p-2">
        <div className="mb-1 text-[9px] uppercase tracking-[0.2em] text-slate-600">Compare</div>
        <pre className="max-h-56 overflow-auto font-mono text-[11px]">
          {rows.map((row, idx) => renderLine(row.right, row, idx, 'right'))}
        </pre>
      </div>
    </div>
  )
}

const TradeAlignment = ({ alignment, summaryDelta, runtimeDelta, configDiff }) => {
  const topMoves = alignment?.top_deltas || []
  const matchRate = alignment?.match_rate

  const deltaRows = [
    { label: 'Net PnL', value: summaryDelta?.net_pnl, formatter: formatCurrency },
    { label: 'Total Return', value: summaryDelta?.total_return, formatter: (v) => formatPercent(v, 2) },
    { label: 'Fees', value: summaryDelta?.fees, formatter: formatCurrency },
    { label: 'Sharpe', value: summaryDelta?.sharpe, formatter: (v) => formatNumber(v, 2) },
    { label: 'Max Drawdown', value: summaryDelta?.max_drawdown_pct, formatter: (v) => formatPercent(v, 2) },
    { label: 'Total Trades', value: summaryDelta?.total_trades, formatter: (v) => formatNumber(v, 0) },
  ]

  return (
    <div className="space-y-3">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Matched Trades</div>
          <div className="mt-1 text-lg font-semibold text-slate-100">{formatNumber(alignment?.matched_count, 0)}</div>
          <div className="text-[11px] text-slate-500">
            {matchRate !== null ? formatPercent(matchRate, 1) : '--'} of baseline
          </div>
        </div>
        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Missing Trades</div>
          <div className="mt-1 text-lg font-semibold text-rose-400">{formatNumber(alignment?.base_only_count, 0)}</div>
          <div className="text-[11px] text-slate-500">baseline only</div>
        </div>
        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Extra Trades</div>
          <div className="mt-1 text-lg font-semibold text-amber-400">{formatNumber(alignment?.compare_only_count, 0)}</div>
          <div className="text-[11px] text-slate-500">compare only</div>
        </div>
        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
          <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Matched PnL Delta</div>
          <div className={`mt-1 text-lg font-semibold ${alignment?.matched_pnl_delta >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
            {formatCurrency(alignment?.matched_pnl_delta)}
          </div>
          <div className="text-[11px] text-slate-500">
            Fees delta {formatCurrency(alignment?.matched_fees_delta)}
          </div>
        </div>
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
          <div className="mb-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">Key Deltas</div>
          <div className="space-y-1">
            {deltaRows.map((row) => (
              <div key={row.label} className="flex items-center justify-between py-1 text-xs text-slate-400">
                <span>{row.label}</span>
                <span className="font-mono text-slate-200">{formatDelta(row.value, row.formatter)}</span>
              </div>
            ))}
            <div className="flex items-center justify-between py-1 text-xs text-slate-400">
              <span>Runtime</span>
              <span className="font-mono text-slate-200">
                {Number.isFinite(runtimeDelta) ? `${runtimeDelta > 0 ? '+' : ''}${formatNumber(runtimeDelta, 0)}s` : '--'}
              </span>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
          <div className="mb-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">Config Differences</div>
          {configDiff && Object.keys(configDiff).length ? (
            <div className="space-y-4">
              {Object.entries(configDiff).map(([key, value]) => (
                <div key={key} className="rounded-lg border border-white/5 bg-black/20 p-2.5">
                  <div className="mb-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">{key}</div>
                  <JsonDiffBlock base={value?.base} compare={value?.compare} />
                </div>
              ))}
            </div>
          ) : (
            <div className="py-4 text-center text-xs text-slate-500">No config differences</div>
          )}
        </div>
      </div>

      <div className="rounded-xl border border-white/5 bg-white/[0.02] p-3">
        <div className="mb-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">Largest Trade Deltas</div>
        {topMoves.length === 0 ? (
          <div className="py-4 text-center text-xs text-slate-500">No matched trades</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-xs text-slate-200">
              <thead>
                <tr className="border-b border-white/10 text-[10px] uppercase tracking-[0.2em] text-slate-500">
                  <th className="pb-2 pr-3 text-left">Entry</th>
                  <th className="pb-2 pr-3 text-left">Symbol</th>
                  <th className="pb-2 pr-3 text-left">Dir</th>
                  <th className="pb-2 pr-3 text-right">Baseline PnL</th>
                  <th className="pb-2 pr-3 text-right">Compare PnL</th>
                  <th className="pb-2 pr-3 text-right">Delta</th>
                  <th className="pb-2 text-right">Fee Delta</th>
                </tr>
              </thead>
              <tbody>
                {topMoves.map((pair, idx) => (
                  <tr key={`${pair.base_trade_id || pair.compare_trade_id || idx}`} className="border-b border-white/5">
                    <td className="py-2 pr-3 text-slate-400">{formatDateShort(pair.entry_time)}</td>
                    <td className="py-2 pr-3 font-mono">{pair.symbol || '--'}</td>
                    <td className="py-2 pr-3 text-slate-400">{pair.direction || '--'}</td>
                    <td className="py-2 pr-3 text-right font-mono">{formatCurrency(pair.base_net_pnl)}</td>
                    <td className="py-2 pr-3 text-right font-mono">{formatCurrency(pair.compare_net_pnl)}</td>
                    <td
                      className={`py-2 pr-3 text-right font-mono ${pair.delta >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                    >
                      {formatCurrency(pair.delta)}
                    </td>
                    <td
                      className={`py-2 text-right font-mono ${pair.fee_delta >= 0 ? 'text-amber-400' : 'text-emerald-400'}`}
                    >
                      {formatCurrency(pair.fee_delta)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

export function CompareModal({ runIds, open, onClose }) {
  const [comparison, setComparison] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchReports = useCallback(async () => {
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
      fetchReports()
    }
  }, [fetchReports, open, runIds])

  const reports = comparison?.reports || []
  const comparisons = comparison?.comparisons || []
  const baseline = useMemo(
    () => reports.find((report) => report.run_id === comparison?.baseline_run_id) || reports[0],
    [comparison?.baseline_run_id, reports],
  )
  const reportById = useMemo(() => {
    const map = new Map()
    reports.forEach((report) => {
      map.set(report.run_id, report)
    })
    return map
  }, [reports])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/80 px-4 py-8 backdrop-blur-sm">
      <div className="w-full max-w-6xl rounded-2xl border border-white/10 bg-[#0d1117] shadow-2xl">
        <header className="sticky top-0 z-10 flex flex-wrap items-start justify-between gap-4 rounded-t-2xl border-b border-white/10 bg-[#0d1117]/95 p-5 backdrop-blur">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <GitCompare className="size-4 text-[color:var(--accent-text-soft)]" />
              <span className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Compare Runs</span>
              <Badge variant="success" size="sm">{runIds?.length || 0} selected</Badge>
            </div>
            <h3 className="mt-2 truncate text-xl font-semibold text-slate-100">
              {baseline?.strategy_name || 'Strategy'} comparison
            </h3>
            <div className="mt-1 flex items-center gap-3 text-xs text-slate-400">
              <span className="font-mono">{baseline?.run_id?.slice(0, 8) || '--'}</span>
              <span className="text-slate-600">•</span>
              <span className="flex items-center gap-1">
                <Calendar className="size-3" />
                {formatDateTime(baseline?.completed_at)}
              </span>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:border-white/20 hover:bg-white/10"
            aria-label="Close comparison"
          >
            <X className="size-4" />
          </button>
        </header>

        {loading ? (
          <div className="relative h-80">
            <LoadingOverlay message="Loading comparison..." />
          </div>
        ) : error ? (
          <div className="p-6">
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 p-4 text-sm text-rose-300">
              {error}
            </div>
            <button
              type="button"
              onClick={fetchReports}
              className="mt-4 rounded-lg border border-white/10 bg-white/5 px-4 py-2 text-xs text-slate-200 transition hover:border-white/20"
            >
              Retry
            </button>
          </div>
        ) : reports.length < 2 ? (
          <div className="p-6 text-sm text-slate-400">Select at least two runs to compare.</div>
        ) : (
          <div className="space-y-6 p-5">
            <div className="rounded-2xl border border-white/5 bg-white/[0.02] p-4">
              <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">Run Snapshot</div>
              <SummaryTable reports={reports} />
            </div>

            <div className="rounded-2xl border border-white/5 bg-white/[0.02] p-4">
              <div className="mb-3 text-[10px] uppercase tracking-[0.2em] text-slate-500">Key Metrics</div>
              <MetricsTable reports={reports} />
            </div>

            {baseline && comparisons.length > 0 && (
              <div className="space-y-5">
                {comparisons.map((entry) => {
                  const compareReport = reportById.get(entry.compare_run_id)
                  return (
                    <div key={entry.compare_run_id} className="rounded-2xl border border-white/5 bg-white/[0.02] p-4">
                    <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Trade Alignment</div>
                        <div className="mt-1 text-sm text-slate-200">
                          {baseline?.run_id?.slice(0, 8)} vs {compareReport?.run_id?.slice(0, 8)}
                        </div>
                      </div>
                      <div className="text-xs text-slate-500">
                        {compareReport?.bot_name || '--'} • {compareReport?.run_type || '--'}
                      </div>
                    </div>
                    <TradeAlignment
                      alignment={entry.trade_alignment}
                      summaryDelta={entry.summary_delta}
                      runtimeDelta={entry.runtime_delta_seconds}
                      configDiff={entry.config_diff}
                    />
                  </div>
                  )
                })}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
