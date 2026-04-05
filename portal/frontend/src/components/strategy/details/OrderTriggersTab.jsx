import { useCallback, useEffect, useMemo, useState } from 'react'
import { Check, ChevronRight, Copy, Crosshair } from 'lucide-react'

import { Button } from '../../ui'
import { StrategyPreviewCharts, StrategyPreviewSummary } from '../preview'
import { buildTriggerDetail, buildTriggerRows } from '../utils/orderTriggers.js'

const getInstrumentDisplay = (instrument) => {
  if (!instrument) return ''
  const baseCurrency = instrument?.metadata?.instrument_fields?.base_currency || instrument?.base_currency
  if (baseCurrency) return baseCurrency
  const symbol = instrument?.symbol || ''
  if (symbol.length <= 8) return symbol
  return `${symbol.slice(0, 6)}…`
}

const DATE_PRESETS = [
  { label: '7d', days: 7 },
  { label: '30d', days: 30 },
  { label: '90d', days: 90 },
  { label: '6mo', days: 180 },
  { label: '1y', days: 365 },
]

const guardLabel = (guard) => {
  if (guard?.type === 'context_match') {
    return `${guard.output_ref || 'context'}.${guard.field || 'state'} = ${guard.actual ?? '—'}`
  }
  if (guard?.type === 'metric_match') {
    return `${guard.output_ref || 'metric'}.${guard.field} ${guard.operator} ${guard.expected} (actual ${guard.actual ?? '—'})`
  }
  if (guard?.type === 'holds_for_bars') {
    const matchedBars = Array.isArray(guard?.bars_evaluated)
      ? guard.bars_evaluated.filter((entry) => Boolean(entry?.matched)).length
      : 0
    return `${guard.base?.output_ref || 'signal'} held ${matchedBars}/${guard.bars || 0} bars`
  }
  if (guard?.type === 'signal_seen_within_bars') {
    return `${guard.output_ref || 'signal'}.${guard.event_key} seen within ${guard.lookback_bars} bars`
  }
  if (guard?.type === 'signal_absent_within_bars') {
    return `${guard.output_ref || 'signal'}.${guard.event_key} absent within ${guard.lookback_bars} bars`
  }
  return 'Guard'
}

const statusTone = (status) => {
  if (status === 'matched') {
    return 'border-emerald-500/25 bg-emerald-500/10 text-emerald-100'
  }
  if (status === 'not_ready') {
    return 'border-amber-500/25 bg-amber-500/10 text-amber-100'
  }
  return 'border-rose-500/25 bg-rose-500/10 text-rose-100'
}

const CopyValueButton = ({ copyKey, label, value, copiedKey, onCopy }) => (
  <div className="rounded-lg border border-white/8 bg-black/20 px-3 py-2.5">
    <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-slate-500">{label}</p>
    <div className="mt-2 flex items-start justify-between gap-3">
      <span className="min-w-0 flex-1 break-all font-mono text-[11px] leading-5 text-slate-200" title={value}>
        {value}
      </span>
      <button
        type="button"
        onClick={() => onCopy(copyKey, value)}
        className="inline-flex shrink-0 items-center gap-1 rounded-md border border-white/10 bg-white/5 px-2 py-1 text-[10px] font-medium text-slate-200 transition hover:border-white/20 hover:bg-white/10"
        aria-label={`Copy ${label}`}
      >
        {copiedKey === copyKey ? <Check className="size-3.5 text-emerald-300" /> : <Copy className="size-3.5" />}
      </button>
    </div>
  </div>
)

const typeTone = (type) => {
  if (type === 'signal') return 'border-sky-500/20 bg-sky-500/10 text-sky-100'
  if (type === 'context') return 'border-violet-500/20 bg-violet-500/10 text-violet-100'
  if (type === 'metric') return 'border-amber-500/20 bg-amber-500/10 text-amber-100'
  return 'border-white/10 bg-white/[0.04] text-slate-200'
}

const ValueNode = ({ node, depth = 0 }) => {
  if (!node) return null

  if (node.kind === 'scalar') {
    return (
      <div className="min-w-0 rounded-md border border-white/8 bg-white/[0.03] px-3 py-2 md:col-span-1">
        <p className="overflow-hidden text-ellipsis whitespace-nowrap text-[10px] uppercase tracking-[0.16em] text-slate-500" title={node.label}>{node.label}</p>
        <p className="mt-1 break-words text-sm text-slate-100" title={node.value}>{node.value}</p>
      </div>
    )
  }

  return (
    <details className="group rounded-md border border-white/8 bg-white/[0.03] md:col-span-2" open={depth === 0 ? undefined : false}>
      <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <ChevronRight className="size-3.5 shrink-0 text-slate-500 transition group-open:rotate-90" />
          <span className="truncate text-[11px] uppercase tracking-[0.16em] text-slate-400" title={node.label}>{node.label}</span>
        </div>
        <span className="shrink-0 text-[11px] text-slate-500">{node.summary}</span>
      </summary>
      <div className="border-t border-white/8 px-3 py-3">
        <div className="grid gap-2 md:grid-cols-3">
          {node.children.map((child) => (
            <ValueNode key={child.key} node={child} depth={depth + 1} />
          ))}
        </div>
      </div>
    </details>
  )
}

const OutputSnapshotSection = ({ title, subtitle, outputs }) => {
  if (!outputs.length) return null
  return (
    <div className="space-y-2">
      <div>
        <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">{title}</p>
        <p className="mt-1 text-xs text-slate-400">{subtitle}</p>
      </div>
      <div className="max-h-[24rem] space-y-2 overflow-y-auto pr-1">
        {outputs.map((output) => (
          <details key={output.key} className="group rounded-lg border border-white/8 bg-black/20">
            <summary className="flex cursor-pointer list-none flex-wrap items-start justify-between gap-2 px-3 py-2.5">
              <div className="flex min-w-0 flex-1 items-start gap-2.5">
                <ChevronRight className="mt-0.5 size-4 shrink-0 text-slate-500 transition group-open:rotate-90" />
                <div className="min-w-0 flex-1">
                  <p className="min-w-0 truncate text-sm leading-5 text-slate-100" title={output.label}>{output.label}</p>
                  {output.barTime ? (
                    <p className="mt-1 text-[11px] text-slate-500">{output.barTime}</p>
                  ) : null}
                </div>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <span className={`rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] ${typeTone(output.type)}`}>
                  {output.type}
                </span>
                <span className={`rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-[0.16em] ${
                  output.ready
                    ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-100'
                    : 'border-amber-500/20 bg-amber-500/10 text-amber-100'
                }`}>
                  {output.ready ? 'Ready' : 'Pending'}
                </span>
              </div>
            </summary>

            <div className="border-t border-white/8 px-3 py-3">
              {!output.ready ? (
                <p className="text-xs text-slate-500">Output was not ready at the selected decision bar.</p>
              ) : null}

              {output.ready && output.fields.length === 0 ? (
                <p className="text-xs text-slate-500">No current values were emitted for this output.</p>
              ) : null}

              {output.fields.length > 0 ? (
                <div className="grid gap-2 md:grid-cols-3">
                  {output.fields.map((field) => (
                    <ValueNode key={field.key} node={field} />
                  ))}
                </div>
              ) : null}
            </div>
          </details>
        ))}
      </div>
    </div>
  )
}

export const OrderTriggersTab = ({
  strategy,
  instruments = [],
  indicatorLookup,
  previewWindow,
  previewLoading,
  previewResult,
  previewInstrumentId,
  onInstrumentChange,
  onSubmit,
  onDateRangeChange,
  DateRangePickerComponent,
}) => {
  const [filterQuery, setFilterQuery] = useState('')
  const [filterSide, setFilterSide] = useState('ALL')
  const [sortBy, setSortBy] = useState('timestamp')
  const [sortDir, setSortDir] = useState('desc')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [focusRequest, setFocusRequest] = useState(null)
  const [selectedRowKey, setSelectedRowKey] = useState(null)
  const [copiedKey, setCopiedKey] = useState(null)

  const activeInstrument = instruments.find((instrument) => instrument?.id === previewInstrumentId) || null
  const symbol = activeInstrument?.symbol || '—'
  const interval = strategy.timeframe || '—'
  const datasource = activeInstrument?.datasource || '—'
  const exchange = activeInstrument?.exchange || '—'
  const instrumentResult = previewResult?.instruments?.[previewInstrumentId] || null

  const handlePresetClick = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - days)
    onDateRangeChange([start, end])
  }

  const triggerRows = buildTriggerRows({ instrumentResult, rules: strategy.rules, symbol })
  const latestTrigger = triggerRows[0] || null
  const uniqueRuleIds = new Set(triggerRows.map((row) => row.ruleId).filter(Boolean))
  const uniqueIndicatorIds = new Set(triggerRows.map((row) => row.indicatorId).filter(Boolean))
  const longCount = triggerRows.filter((row) => row.direction === 'BUY').length
  const shortCount = triggerRows.filter((row) => row.direction === 'SELL').length

  const filteredRows = useMemo(() => {
    const query = String(filterQuery || '').trim().toLowerCase()
    const rows = triggerRows.filter((row) => {
      if (filterSide !== 'ALL' && row.direction !== filterSide) return false
      if (!query) return true
      const haystack = [
        row.timestamp,
        row.direction,
        row.ruleName,
        row.ruleId,
        row.triggerType,
        row.instrument,
        row.indicatorId,
        row.outputName,
        ...(row.guards || []).map((guard) => guardLabel(guard)),
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
    rows.sort((a, b) => {
      const read = (entry) => {
        if (sortBy === 'timestamp') return entry.epoch || 0
        if (sortBy === 'side') return entry.direction || ''
        if (sortBy === 'rule') return entry.ruleName || ''
        if (sortBy === 'trigger') return entry.triggerType || ''
        if (sortBy === 'indicator') return entry.indicatorId || ''
        if (sortBy === 'guards') return entry.guardCount || 0
        return ''
      }
      const aValue = read(a)
      const bValue = read(b)
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDir === 'asc' ? aValue - bValue : bValue - aValue
      }
      const cmp = String(aValue).localeCompare(String(bValue))
      return sortDir === 'asc' ? cmp : -cmp
    })
    return rows
  }, [triggerRows, filterQuery, filterSide, sortBy, sortDir])

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize))
  const safePage = Math.min(page, totalPages)
  const pagedRows = useMemo(() => {
    const start = (safePage - 1) * pageSize
    return filteredRows.slice(start, start + pageSize)
  }, [filteredRows, safePage, pageSize])

  useEffect(() => {
    if (page !== safePage) setPage(safePage)
  }, [page, safePage])

  useEffect(() => {
    if (!filteredRows.length) {
      setSelectedRowKey(null)
      return
    }
    if (!filteredRows.some((row) => (row.rowKey || row.id) === selectedRowKey)) {
      setSelectedRowKey(filteredRows[0].rowKey || filteredRows[0].id)
    }
  }, [filteredRows, selectedRowKey])

  useEffect(() => {
    if (!copiedKey) return undefined
    const timer = window.setTimeout(() => setCopiedKey(null), 2200)
    return () => window.clearTimeout(timer)
  }, [copiedKey])

  const selectedRow = useMemo(
    () => filteredRows.find((row) => (row.rowKey || row.id) === selectedRowKey) || filteredRows[0] || null,
    [filteredRows, selectedRowKey],
  )
  const selectedDetail = useMemo(
    () => buildTriggerDetail(selectedRow, { indicatorLookup }),
    [selectedRow, indicatorLookup],
  )

  const onSort = (key) => {
    if (sortBy === key) {
      setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortBy(key)
    setSortDir('desc')
  }

  const focusRowOnChart = useCallback((row) => {
    if (!row?.epoch) return
    setFocusRequest({
      rowKey: row.rowKey || row.id,
      epoch: Number(row.epoch),
      instrumentId: row.instrumentId || previewInstrumentId || null,
      at: Date.now(),
    })
  }, [previewInstrumentId])

  const handleRowSelect = useCallback((row) => {
    setSelectedRowKey(row.rowKey || row.id)
    focusRowOnChart(row)
  }, [focusRowOnChart])

  const handleCopy = useCallback(async (key, value) => {
    if (!value || !navigator?.clipboard?.writeText) return
    try {
      await navigator.clipboard.writeText(String(value))
      setCopiedKey(key)
    } catch {
      setCopiedKey(null)
    }
  }, [])

  return (
    <div className="space-y-4">
      <form onSubmit={onSubmit} className="space-y-3" aria-label="Order trigger preview controls">
        <div className="rounded border border-white/[0.08] bg-[#0a0d13] p-3">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Order triggers</p>
              <p className="text-xs text-slate-400">Read-only preview of typed rule evaluation and indicator-owned overlays.</p>
            </div>
          </div>

          <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-[1.7fr_1fr]">
            <div className="rounded-sm border border-white/[0.08] bg-white/[0.02] p-2.5">
              <div className="mb-2 flex items-center justify-between">
                <span className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Date range</span>
                <div className="flex gap-1">
                  {DATE_PRESETS.map((preset) => (
                    <button
                      key={preset.label}
                      type="button"
                      onClick={() => handlePresetClick(preset.days)}
                      className="qt-mono rounded px-2 py-0.5 text-[10px] font-medium text-slate-400 transition hover:bg-white/5 hover:text-white"
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
              </div>
              <DateRangePickerComponent
                dateRange={previewWindow.dateRange}
                setDateRange={onDateRangeChange}
              />
            </div>

            <div className="rounded-sm border border-white/[0.08] bg-white/[0.02] p-2.5">
              <div className="mb-2 flex items-center justify-between gap-2">
                <span className="block text-[10px] uppercase tracking-[0.2em] text-slate-500">Instrument</span>
              </div>
              {instruments.length ? (
                <div className="flex items-center gap-1.5 overflow-x-auto pb-1">
                  {instruments.map((instrument) => (
                    <button
                      key={`preview-instrument-${instrument?.id || instrument?.symbol}`}
                      type="button"
                      onClick={() => onInstrumentChange(instrument?.id)}
                      className={`qt-mono shrink-0 rounded-sm border px-2.5 py-1 text-xs transition ${
                        instrument?.id === previewInstrumentId
                          ? 'border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-15)] text-white'
                          : 'border-white/10 bg-white/[0.03] text-slate-400 hover:border-white/20 hover:text-white'
                      }`}
                      title={instrument?.symbol}
                    >
                      {getInstrumentDisplay(instrument) || 'Instrument'}
                    </button>
                  ))}
                </div>
              ) : (
                <p className="text-xs text-slate-500">No instruments configured</p>
              )}
            </div>
          </div>

          <div className="mt-2 flex items-center justify-between gap-3 border-t border-white/[0.06] pt-3">
            <div className="flex flex-wrap items-center gap-4 text-[11px] text-slate-500">
              <span>SYM <span className="qt-mono text-slate-200" title={activeInstrument?.symbol}>{getInstrumentDisplay(activeInstrument) || symbol}</span></span>
              <span>TF <span className="qt-mono text-slate-200">{interval}</span></span>
              <span>SRC <span className="qt-mono text-slate-200">{datasource}</span></span>
              <span>EX <span className="text-slate-200">{exchange}</span></span>
            </div>
            <Button
              type="submit"
              disabled={previewLoading || !previewInstrumentId}
              loading={previewLoading}
              className="px-6 py-2 text-sm font-semibold"
            >
              {previewLoading ? 'Running…' : 'Run preview'}
            </Button>
          </div>
        </div>
      </form>

      {previewResult && (
        <div className="flex flex-wrap items-center gap-4 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2">
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-emerald-500" />
            <span className="text-sm font-medium text-white">{longCount}</span>
            <span className="text-xs text-slate-500">long decisions</span>
          </div>
          <div className="h-4 w-px bg-white/10" />
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-rose-500" />
            <span className="text-sm font-medium text-white">{shortCount}</span>
            <span className="text-xs text-slate-500">short decisions</span>
          </div>
          <div className="h-4 w-px bg-white/10" />
          <span className="text-xs text-slate-400">
            {uniqueRuleIds.size} rules • {uniqueIndicatorIds.size} indicators
          </span>
        </div>
      )}

      {previewResult && (
        <div className="space-y-4">
          <StrategyPreviewSummary result={previewResult} instrumentId={previewInstrumentId} />

          {!previewResult?.instruments && (
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2 text-xs text-rose-200">
              Strategy preview payload is missing instrument data for multi-instrument previews.
            </div>
          )}

          <StrategyPreviewCharts
            strategy={strategy}
            instruments={instruments}
            previewInstrumentId={previewInstrumentId}
            previewResult={previewResult}
            focusRequest={focusRequest}
          />

          <div className="border-t border-white/[0.08] pt-3 mt-3">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Recent decision events</p>
                <p className="text-xs text-slate-400">These rows are built directly from canonical decision artifacts selected by rule evaluation.</p>
                {latestTrigger?.timestamp && (
                  <p className="qt-mono mt-1 text-[11px] text-slate-500">
                    Latest: <span className="text-slate-300">{latestTrigger.timestamp}</span>
                  </p>
                )}
              </div>
              <div className="flex flex-col items-end gap-1">
                <span className="rounded-sm border border-white/10 bg-black/50 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300">
                  <span className="qt-mono">{triggerRows.length || 0}</span> decisions
                </span>
              </div>
            </div>
            <div className="mt-3 flex flex-wrap gap-2 text-[10px] uppercase tracking-[0.2em]">
              <span className="rounded-sm border border-white/10 bg-black/40 px-2 py-1 text-slate-300">
                rules <span className="qt-mono">{uniqueRuleIds.size}</span>
              </span>
              <span className="rounded-sm border border-white/10 bg-black/40 px-2 py-1 text-slate-300">
                indicators <span className="qt-mono">{uniqueIndicatorIds.size}</span>
              </span>
            </div>

            {triggerRows.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">Run a preview to see when this strategy would attempt orders.</p>
            ) : (
              <div className="mt-3 grid gap-4 xl:grid-cols-[minmax(0,1.3fr)_minmax(360px,1fr)] xl:items-stretch">
                <div className="flex min-h-[52rem] flex-col rounded-xl border border-white/[0.08] bg-[#0b1019] p-4">
                  <div className="grid grid-cols-1 gap-2 md:grid-cols-4">
                    <input
                      value={filterQuery}
                      onChange={(event) => { setFilterQuery(event.target.value); setPage(1) }}
                      placeholder="Search rule, event, indicator, guard…"
                      className="rounded border border-white/10 bg-black/40 px-3 py-2 text-xs text-slate-200 placeholder:text-slate-500 focus:border-white/20 focus:outline-none"
                    />
                    <select
                      value={filterSide}
                      onChange={(event) => { setFilterSide(event.target.value); setPage(1) }}
                      className="rounded border border-white/10 bg-black/40 px-3 py-2 text-xs text-slate-200 focus:border-white/20 focus:outline-none"
                    >
                      <option value="ALL">All sides</option>
                      <option value="BUY">BUY</option>
                      <option value="SELL">SELL</option>
                    </select>
                    <div className="flex items-center gap-2 text-[11px] text-slate-500">
                      <span>Sort</span>
                      <select
                        value={sortBy}
                        onChange={(event) => setSortBy(event.target.value)}
                        className="rounded border border-white/10 bg-black/40 px-2 py-2 text-xs text-slate-200 focus:border-white/20 focus:outline-none"
                      >
                        <option value="timestamp">Time</option>
                        <option value="side">Side</option>
                        <option value="rule">Rule</option>
                        <option value="trigger">Trigger</option>
                        <option value="indicator">Indicator</option>
                        <option value="guards">Guards</option>
                      </select>
                      <button
                        type="button"
                        onClick={() => setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'))}
                        className="rounded border border-white/10 bg-black/40 px-2 py-2 text-xs text-slate-200"
                      >
                        {sortDir === 'asc' ? 'Asc' : 'Desc'}
                      </button>
                    </div>
                    <div className="flex items-center justify-end gap-2">
                      <span className="text-[11px] text-slate-500">Rows</span>
                      <select
                        value={String(pageSize)}
                        onChange={(event) => {
                          const next = Number(event.target.value)
                          setPageSize(Number.isFinite(next) ? next : 20)
                          setPage(1)
                        }}
                        className="rounded border border-white/10 bg-black/40 px-2 py-2 text-xs text-slate-200 focus:border-white/20 focus:outline-none"
                      >
                        <option value="10">10</option>
                        <option value="20">20</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                      </select>
                    </div>
                  </div>

                  <div className="mt-3 min-h-0 flex-1 overflow-hidden rounded-sm border border-white/[0.08]">
                    <div className="h-full divide-y divide-white/[0.04] overflow-y-auto">
                      {pagedRows.map((row) => {
                        const rowSelected = (selectedRowKey || selectedRow?.rowKey || selectedRow?.id) === (row.rowKey || row.id)
                        return (
                          <button
                            key={row.rowKey || row.id}
                            type="button"
                            className={`group block w-full cursor-pointer px-3 py-2 text-left transition hover:bg-white/[0.03] ${
                              rowSelected
                                ? 'border-l-2 border-l-[color:var(--accent-base)] bg-[color:var(--accent-alpha-05)]'
                                : ''
                            }`}
                            onClick={() => handleRowSelect(row)}
                          >
                            <div className="flex items-baseline gap-3 text-xs">
                              <span className={`w-8 shrink-0 text-[10px] font-bold ${
                                row.direction === 'BUY' ? 'text-emerald-400' : 'text-rose-400'
                              }`}>
                                {row.direction}
                              </span>
                              <span className="qt-mono shrink-0 text-[10px] text-slate-600">{row.timestamp || '—'}</span>
                              <span className="truncate text-slate-200">{row.ruleName}</span>
                              <span className="truncate text-[11px] text-slate-500">
                                {row.triggerDisplay}
                                {row.outputRef ? ` → ${row.outputRef}` : ''}
                              </span>
                              {row.guards.length > 0 && (
                                <span className="truncate text-[10px] text-amber-500/60">
                                  [{row.guards.map((guard) => guardLabel(guard)).join(' · ')}]
                                </span>
                              )}
                            </div>
                          </button>
                        )
                      })}
                    </div>
                  </div>

                  <div className="mt-3 flex items-center justify-between">
                    <p className="text-[11px] text-slate-500">
                      Showing {pagedRows.length ? ((safePage - 1) * pageSize) + 1 : 0}-
                      {Math.min(safePage * pageSize, filteredRows.length)} of {filteredRows.length}
                    </p>
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                        disabled={safePage <= 1}
                        className="rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-300 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        Prev
                      </button>
                      <span className="text-[11px] text-slate-400">Page {safePage} / {totalPages}</span>
                      <button
                        type="button"
                        onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                        disabled={safePage >= totalPages}
                        className="rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-300 disabled:cursor-not-allowed disabled:opacity-40"
                      >
                        Next
                      </button>
                    </div>
                  </div>
                </div>

                <div className="min-h-[52rem] max-h-[52rem] overflow-hidden rounded-xl border border-white/[0.08] bg-[#0b1019] p-4">
                  {selectedRow && selectedDetail ? (
                    <div className="flex h-full flex-col overflow-y-auto pr-1">
                      <div className="space-y-4">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <span className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
                              selectedDetail.summary.direction === 'BUY'
                                ? 'bg-emerald-500/15 text-emerald-200'
                                : 'bg-rose-500/15 text-rose-200'
                            }`}>
                              {selectedDetail.summary.direction}
                            </span>
                            <span className="rounded-full border border-white/10 bg-white/[0.04] px-2.5 py-1 text-[10px] uppercase tracking-[0.18em] text-slate-400">
                              {selectedDetail.summary.triggerDisplay}
                            </span>
                          </div>
                          <h4 className="mt-3 text-sm font-semibold text-white">{selectedDetail.summary.ruleName}</h4>
                          <p className="mt-1 text-xs text-slate-400">
                            {selectedDetail.summary.timestamp || 'Timestamp unavailable'}
                          </p>
                        </div>
                        <button
                          type="button"
                          onClick={() => focusRowOnChart(selectedRow)}
                          className="inline-flex shrink-0 items-center gap-2 rounded-md border border-white/10 bg-white/[0.04] px-2.5 py-2 text-[11px] font-medium text-slate-200 transition hover:border-white/20 hover:bg-white/[0.08]"
                        >
                          <Crosshair className="size-3.5" />
                          Reveal
                        </button>
                      </div>

                      <div className="grid gap-2 sm:grid-cols-2">
                        <div className="rounded-lg border border-white/8 bg-black/20 px-3 py-2.5">
                          <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-slate-500">Instrument</p>
                          <p className="mt-2 text-sm text-slate-200">{selectedDetail.summary.instrument}</p>
                        </div>
                        <div className="rounded-lg border border-white/8 bg-black/20 px-3 py-2.5">
                          <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-slate-500">Signal output</p>
                          <p className="mt-2 text-sm text-slate-200">
                            {selectedDetail.summary.outputRef || selectedDetail.summary.triggerDisplay}
                          </p>
                          {selectedDetail.summary.indicatorRef ? (
                            <p className="mt-1 text-[11px] text-slate-500">{selectedDetail.summary.indicatorRef}</p>
                          ) : null}
                        </div>
                      </div>

                      <OutputSnapshotSection
                        title="World state"
                        subtitle="All indicator outputs observed at the selected decision bar."
                        outputs={selectedDetail.observedOutputs}
                      />

                      <OutputSnapshotSection
                        title="Referenced outputs"
                        subtitle="The output subset directly used by the trigger and guard evaluation."
                        outputs={selectedDetail.referencedOutputs}
                      />

                      {selectedDetail.guardChecks.length > 0 ? (
                        <div className="space-y-2">
                          <div>
                            <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Guard checks</p>
                            <p className="mt-1 text-xs text-slate-400">Only the checks that shaped this selected decision.</p>
                          </div>
                          <div className="space-y-2">
                            {selectedDetail.guardChecks.map((guard) => (
                              <div key={guard.key} className={`rounded-lg border px-3 py-2.5 ${statusTone(guard.status)}`}>
                                <div className="flex items-start justify-between gap-3">
                                  <p className="text-sm font-medium">{guard.label}</p>
                                  <span className="rounded-full border border-current/20 px-2 py-0.5 text-[10px] uppercase tracking-[0.18em]">
                                    {guard.status === 'matched' ? 'Pass' : guard.status === 'not_ready' ? 'Pending' : 'Fail'}
                                  </span>
                                </div>
                                <p className="mt-1 text-xs opacity-90">{guard.detail}</p>
                                {guard.note ? (
                                  <p className="mt-1 text-[11px] opacity-75">{guard.note}</p>
                                ) : null}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {selectedDetail.references.length > 0 ? (
                        <div className="space-y-2">
                          <div>
                            <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">References</p>
                            <p className="mt-1 text-xs text-slate-400">Useful IDs for retrieval and traceability.</p>
                          </div>
                          <div className="grid gap-2">
                            {selectedDetail.references.map((reference) => (
                              <CopyValueButton
                                key={reference.key}
                                copyKey={reference.key}
                                label={reference.label}
                                value={reference.value}
                                copiedKey={copiedKey}
                                onCopy={handleCopy}
                              />
                            ))}
                          </div>
                        </div>
                      ) : null}
                      </div>
                    </div>
                  ) : (
                    <div className="flex min-h-[320px] items-center justify-center rounded-lg border border-dashed border-white/10 bg-black/20 px-4 text-center">
                      <div>
                        <p className="text-sm font-medium text-slate-300">Select a decision</p>
                        <p className="mt-2 text-xs text-slate-500">Choose a row to inspect the selected signal, its context, and the guard state that allowed it through.</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
