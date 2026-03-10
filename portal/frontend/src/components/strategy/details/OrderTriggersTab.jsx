import { useEffect, useMemo, useRef, useState } from 'react'
import { Button } from '../../ui'
import { SignalSummary } from '../signals'
import { SignalPreviewCharts } from '../signals/SignalPreviewCharts.jsx'
import { buildTriggerRows } from '../utils/orderTriggers.js'

/**
 * Get display name for an instrument - prefers base_currency from metadata
 */
const getInstrumentDisplay = (instrument) => {
  if (!instrument) return ''

  // Try to get base_currency from instrument metadata
  const baseCurrency = instrument?.metadata?.instrument_fields?.base_currency || instrument?.base_currency
  if (baseCurrency) return baseCurrency

  // Fallback to symbol
  const symbol = instrument?.symbol || ''
  if (symbol.length <= 8) return symbol
  return symbol.slice(0, 6) + '…'
}

/**
 * Date range preset buttons
 */
const DATE_PRESETS = [
  { label: '7d', days: 7 },
  { label: '30d', days: 30 },
  { label: '90d', days: 90 },
  { label: '6mo', days: 180 },
  { label: '1y', days: 365 },
]

/**
 * Order Triggers tab for previewing when a strategy would attempt orders.
 */
export const OrderTriggersTab = ({
  strategy,
  instruments = [],
  attachedIndicators,
  signalWindow,
  signalsLoading,
  signalResult,
  signalInstrumentId,
  onInstrumentChange,
  onSubmit,
  onDateRangeChange,
  DateRangePickerComponent,
  onNavigateToRules,
  onNavigateToExecution,
}) => {
  const [filterQuery, setFilterQuery] = useState('')
  const [filterSide, setFilterSide] = useState('ALL')
  const [filterMatched, setFilterMatched] = useState('ALL')
  const [sortBy, setSortBy] = useState('timestamp')
  const [sortDir, setSortDir] = useState('desc')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [focusRequest, setFocusRequest] = useState(null)
  const [columnWidths, setColumnWidths] = useState({
    timestamp: 210,
    side: 78,
    rule: 230,
    trigger: 180,
    indicator: 120,
    signalId: 120,
    eventId: 120,
    knownAt: 210,
  })
  const resizingRef = useRef(null)
  const activeInstrument = instruments.find((instrument) => instrument?.id === signalInstrumentId) || null
  const symbol = activeInstrument?.symbol || '—'
  const interval = strategy.timeframe || '—'
  const datasource = activeInstrument?.datasource || '—'
  const exchange = activeInstrument?.exchange || '—'
  const instrumentResult = signalResult?.instruments?.[signalInstrumentId] || null

  // Handle preset date range selection
  const handlePresetClick = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - days)
    // DateRangePickerComponent expects [startDate, endDate] array format
    onDateRangeChange([start, end])
  }

  // Get signal counts for mini summary
  const countSignals = (entries = []) => entries.reduce((total, entry) => {
    if (!entry) return total
    if (Array.isArray(entry.signals) && entry.signals.length) {
      return total + entry.signals.length
    }
    return total + (entry.matched ? 1 : 0)
  }, 0)
  const buyCount = instrumentResult
    ? countSignals(instrumentResult.buy_signals || [])
    : signalResult?.summary?.buy_count || 0
  const sellCount = instrumentResult
    ? countSignals(instrumentResult.sell_signals || [])
    : signalResult?.summary?.sell_count || 0
  const rulesMatched = instrumentResult
    ? (instrumentResult.rule_results || []).filter((entry) => entry?.matched).length
    : signalResult?.summary?.rules_matched

  const triggerRows = buildTriggerRows({ instrumentResult, rules: strategy.rules, symbol })
  const latestTrigger = triggerRows[0] || null
  const uniqueRuleIds = new Set(triggerRows.map((row) => row.ruleId).filter(Boolean))
  const uniqueIndicatorIds = new Set(triggerRows.map((row) => row.indicatorId).filter(Boolean))
  const matchedCount = triggerRows.filter((row) => row.matched !== false).length
  const ruleLookup = new Map((Array.isArray(strategy.rules) ? strategy.rules : []).map((rule) => [rule.id, rule]))
  const filterFailures = (instrumentResult?.rule_results || [])
    .filter((entry) => entry?.matched && entry?.final_decision && entry.final_decision.allowed === false)
    .map((entry) => {
      const globalFailed = (entry.global_filters || []).filter((filter) => filter?.passed === false)
      const ruleFailed = (entry.rule_filters || []).filter((filter) => filter?.passed === false)
      if (!globalFailed.length && !ruleFailed.length) return null
      const ruleMeta = ruleLookup.get(entry.rule_id)
      return {
        id: entry.rule_id,
        name: ruleMeta?.name || entry.rule_id || 'Rule',
        action: ruleMeta?.action || entry.action,
        globalFailed,
        ruleFailed,
      }
    })
    .filter(Boolean)

  const sortableValue = (row, key) => {
    if (key === 'timestamp') return row.epoch || 0
    if (key === 'knownAt') return row.knownAt || ''
    if (key === 'side') return row.direction || ''
    if (key === 'rule') return row.ruleName || row.ruleId || ''
    if (key === 'trigger') return row.triggerType || row.signalType || ''
    if (key === 'indicator') return row.indicatorId || ''
    if (key === 'signalId') return row.signalId || ''
    if (key === 'eventId') return row.eventId || row.traceId || ''
    return ''
  }

  const filteredRows = useMemo(() => {
    const query = String(filterQuery || '').trim().toLowerCase()
    const rows = triggerRows.filter((row) => {
      if (filterSide !== 'ALL' && row.direction !== filterSide) return false
      if (filterMatched === 'MATCHED' && row.matched === false) return false
      if (filterMatched === 'UNMATCHED' && row.matched !== false) return false
      if (!query) return true
      const haystack = [
        row.timestamp,
        row.direction,
        row.ruleName,
        row.ruleId,
        row.triggerType,
        row.instrument,
        row.indicatorId,
        row.signalId,
        row.eventId,
        row.traceId,
        row.runtimeScope,
        row.knownAt,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
    rows.sort((a, b) => {
      const aValue = sortableValue(a, sortBy)
      const bValue = sortableValue(b, sortBy)
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDir === 'asc' ? aValue - bValue : bValue - aValue
      }
      const cmp = String(aValue).localeCompare(String(bValue))
      return sortDir === 'asc' ? cmp : -cmp
    })
    return rows
  }, [triggerRows, filterQuery, filterSide, filterMatched, sortBy, sortDir])

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize))
  const safePage = Math.min(page, totalPages)
  const pagedRows = useMemo(() => {
    const start = (safePage - 1) * pageSize
    return filteredRows.slice(start, start + pageSize)
  }, [filteredRows, safePage, pageSize])

  useEffect(() => {
    if (page !== safePage) setPage(safePage)
  }, [page, safePage])

  const onSort = (key) => {
    if (sortBy === key) {
      setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortBy(key)
    setSortDir('desc')
  }

  const beginResize = (event, key) => {
    event.preventDefault()
    const startX = event.clientX
    const startWidth = columnWidths[key] || 120
    resizingRef.current = { key, startX, startWidth }
    const onMove = (moveEvent) => {
      if (!resizingRef.current) return
      const dx = moveEvent.clientX - resizingRef.current.startX
      const next = Math.max(70, resizingRef.current.startWidth + dx)
      setColumnWidths((prev) => ({ ...prev, [key]: next }))
    }
    const onUp = () => {
      resizingRef.current = null
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }

  return (
    <div className="space-y-4">
      <form onSubmit={onSubmit} className="space-y-3" aria-label="Order trigger preview controls">
        <div className="rounded-xl border border-white/[0.08] bg-black/30 p-3">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Order triggers</p>
              <p className="text-xs text-slate-400">Read-only preview of strategy order attempts.</p>
            </div>
            <div className="flex items-center gap-2">
              <button type="button" onClick={onNavigateToRules} className="rounded border border-white/10 bg-white/5 px-2 py-1 text-xs text-slate-200 hover:border-white/20 hover:text-white">
                View rules
              </button>
              <button type="button" onClick={onNavigateToExecution} className="rounded border border-white/10 bg-white/5 px-2 py-1 text-xs text-slate-200 hover:border-white/20 hover:text-white">
                Execution config
              </button>
            </div>
          </div>

          <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-[1.7fr_1fr]">
            <div className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-2.5">
              <div className="mb-2 flex items-center justify-between">
                <span className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Date range</span>
                <div className="flex gap-1">
                  {DATE_PRESETS.map((preset) => (
                    <button
                      key={preset.label}
                      type="button"
                      onClick={() => handlePresetClick(preset.days)}
                      className="rounded px-2 py-0.5 text-[10px] font-medium text-slate-400 transition hover:bg-white/5 hover:text-white"
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
              </div>
              <DateRangePickerComponent
                dateRange={signalWindow.dateRange}
                setDateRange={onDateRangeChange}
              />
            </div>

            <div className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-2.5">
              <div className="mb-2 flex items-center justify-between gap-2">
                <span className="block text-[10px] uppercase tracking-[0.2em] text-slate-500">Instrument</span>
                <Button
                  type="submit"
                  disabled={signalsLoading || !signalInstrumentId}
                  loading={signalsLoading}
                  className="h-8 px-3 text-xs"
                >
                  {signalsLoading ? 'Previewing…' : 'Run preview'}
                </Button>
              </div>
              {instruments.length ? (
                <div className="flex items-center gap-1.5 overflow-x-auto pb-1">
                  {instruments.map((instrument) => (
                    <button
                      key={`signal-instrument-${instrument?.id || instrument?.symbol}`}
                      type="button"
                      onClick={() => onInstrumentChange(instrument?.id)}
                      className={`shrink-0 rounded border px-2.5 py-1 text-xs transition ${
                        instrument?.id === signalInstrumentId
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

          <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2 text-xs">
            <div className="flex items-center gap-1.5">
              <span className="text-slate-500">Symbol</span>
              <span className="text-white" title={activeInstrument?.symbol}>{getInstrumentDisplay(activeInstrument) || symbol}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-500">Interval</span>
              <span className="text-white">{interval}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-500">Source</span>
              <span className="text-white">{datasource}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-500">Exchange</span>
              <span className="text-white">{exchange}</span>
            </div>
          </div>
        </div>
      </form>

      {/* Mini trigger summary - shown if we have results */}
      {signalResult && (
        <div className="flex flex-wrap items-center gap-4 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2">
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-emerald-500" />
            <span className="text-sm font-medium text-white">{buyCount}</span>
            <span className="text-xs text-slate-500">buys</span>
          </div>
          <div className="h-4 w-px bg-white/10" />
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-rose-500" />
            <span className="text-sm font-medium text-white">{sellCount}</span>
            <span className="text-xs text-slate-500">sells</span>
          </div>
          {rulesMatched !== undefined && (
            <>
              <div className="h-4 w-px bg-white/10" />
              <span className="text-xs text-slate-400">
                {rulesMatched}/{strategy.rules?.length || 0} rules matched
              </span>
            </>
          )}
        </div>
      )}

      {/* Trigger results */}
      {signalResult && (
        <div className="space-y-4">
          <SignalSummary result={signalResult} instrumentId={signalInstrumentId} />

          {!signalResult?.instruments && (
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2 text-xs text-rose-200">
              Signal preview payload is missing instrument data for multi-instrument previews.
            </div>
          )}

          <SignalPreviewCharts
            strategy={strategy}
            instruments={instruments}
            previewInstrumentId={signalInstrumentId}
            signalResult={signalResult}
            attachedIndicators={attachedIndicators}
            focusRequest={focusRequest}
          />

          <div className="rounded-xl border border-white/10 bg-white/[0.02] p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-white">Recent order triggers</p>
                <p className="text-xs text-slate-400">Derived from evaluated rules—no indicator edits or recompute actions here.</p>
                {latestTrigger?.timestamp && (
                  <p className="mt-1 text-[11px] text-slate-500">
                    Latest: <span className="text-slate-300">{latestTrigger.timestamp}</span>
                  </p>
                )}
              </div>
              <div className="flex flex-col items-end gap-1">
                <span className="rounded-full border border-white/10 bg-black/50 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300">
                  {triggerRows.length || 0} events
                </span>
                <span className="text-[10px] text-slate-500">
                  {matchedCount}/{triggerRows.length || 0} matched
                </span>
              </div>
            </div>
            <div className="mt-3 flex flex-wrap gap-2 text-[10px] uppercase tracking-[0.2em]">
              <span className="rounded-full border border-white/10 bg-black/40 px-2 py-1 text-slate-300">
                rules {uniqueRuleIds.size}
              </span>
              <span className="rounded-full border border-white/10 bg-black/40 px-2 py-1 text-slate-300">
                indicators {uniqueIndicatorIds.size}
              </span>
            </div>

            {triggerRows.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">Run a preview to see when this strategy would attempt orders.</p>
            ) : (
              <div className="mt-3 space-y-3">
                <div className="grid grid-cols-1 gap-2 md:grid-cols-4">
                  <input
                    value={filterQuery}
                    onChange={(event) => { setFilterQuery(event.target.value); setPage(1) }}
                    placeholder="Search rule, trigger, id, scope…"
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
                  <select
                    value={filterMatched}
                    onChange={(event) => { setFilterMatched(event.target.value); setPage(1) }}
                    className="rounded border border-white/10 bg-black/40 px-3 py-2 text-xs text-slate-200 focus:border-white/20 focus:outline-none"
                  >
                    <option value="ALL">All match states</option>
                    <option value="MATCHED">Matched only</option>
                    <option value="UNMATCHED">Unmatched only</option>
                  </select>
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
                <div className="overflow-x-auto rounded-lg border border-white/10 bg-black/20">
                <table className="min-w-full border-collapse text-left text-xs">
                  <thead className="border-b border-white/10 bg-white/[0.03]">
                    <tr>
                      {[
                        ['timestamp', 'Time'],
                        ['side', 'Side'],
                        ['rule', 'Rule'],
                        ['trigger', 'Trigger'],
                        ['indicator', 'Indicator'],
                        ['signalId', 'Signal ID'],
                        ['eventId', 'Event ID'],
                        ['knownAt', 'Known At'],
                      ].map(([key, label]) => (
                        <th
                          key={key}
                          style={{ width: `${columnWidths[key]}px`, minWidth: `${columnWidths[key]}px` }}
                          className="relative px-3 py-2 text-[10px] uppercase tracking-[0.2em] text-slate-400"
                        >
                          <button
                            type="button"
                            onClick={() => onSort(key)}
                            className="inline-flex items-center gap-1 hover:text-slate-200"
                          >
                            <span>{label}</span>
                            <span className="text-[9px] text-slate-500">{sortBy === key ? (sortDir === 'asc' ? '↑' : '↓') : ''}</span>
                          </button>
                          <span
                            role="separator"
                            onMouseDown={(event) => beginResize(event, key)}
                            className="absolute right-0 top-0 h-full w-1 cursor-col-resize bg-transparent hover:bg-white/20"
                            title="Resize column"
                          />
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {pagedRows.map((row) => {
                      const rowSelected = Boolean(
                        focusRequest
                        && focusRequest.rowKey === (row.rowKey || row.id)
                      )
                      return (
                      <tr
                        key={row.rowKey || row.id}
                        className={`align-top transition ${rowSelected ? 'bg-[color:var(--accent-alpha-12)]' : 'hover:bg-white/[0.02]'} ${row.epoch ? 'cursor-pointer' : ''}`}
                        onClick={() => {
                          if (!row.epoch) return
                          setFocusRequest({
                            rowKey: row.rowKey || row.id,
                            epoch: Number(row.epoch),
                            instrumentId: row.instrumentId || signalInstrumentId || null,
                            at: Date.now(),
                          })
                        }}
                        title={row.epoch ? 'Jump preview chart to this trigger' : undefined}
                      >
                        <td className="px-3 py-2 text-slate-200">{row.timestamp || '—'}</td>
                        <td className="px-3 py-2">
                          <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.2em] ${
                            row.direction === 'BUY'
                              ? 'border-emerald-500/30 bg-emerald-500/15 text-emerald-200'
                              : 'border-rose-500/30 bg-rose-500/15 text-rose-200'
                          }`}>
                            {row.direction}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <div className="text-slate-100">{row.ruleName}</div>
                          <div className="break-all text-[11px] text-slate-500" title={row.ruleRef || row.ruleId || '—'}>
                            {row.ruleRef || row.ruleId || '—'}
                          </div>
                        </td>
                        <td className="px-3 py-2">
                          <div className="text-slate-200">{row.triggerType || 'entry'}</div>
                          <div className="text-[11px] text-slate-500">
                            {row.instrument}
                            {row.matched === false ? ' • not matched' : ''}
                            {row.level != null ? ` • lvl ${row.level}` : ''}
                          </div>
                        </td>
                        <td className="break-all px-3 py-2 text-slate-300" title={row.indicatorRef || '—'}>{row.indicatorRef || '—'}</td>
                        <td className="break-all px-3 py-2 text-slate-300" title={row.signalRef || '—'}>{row.signalRef || '—'}</td>
                        <td className="break-all px-3 py-2 text-slate-300" title={row.eventRef || '—'}>{row.eventRef || '—'}</td>
                        <td className="px-3 py-2 text-slate-400">{row.knownAt || '—'}</td>
                      </tr>
                    )})}
                  </tbody>
                </table>
              </div>
                <div className="flex items-center justify-between">
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
            )}
            {triggerRows.length > 0 && (
              <div className="mt-3 rounded-lg border border-white/10 bg-black/20 p-3">
                <p className="mb-2 text-[10px] uppercase tracking-[0.24em] text-slate-500">Reasons</p>
                <div className="flex flex-wrap gap-2">
                  {triggerRows.flatMap((row) => (Array.isArray(row.reasons) ? row.reasons.slice(0, 2).map((reason, idx) => ({
                    key: `${row.rowKey || row.id}-reason-${idx}`,
                    value: typeof reason === 'string' ? reason : reason?.label || 'reason',
                  })) : [])).slice(0, 40).map((item) => (
                    <span
                      key={item.key}
                      className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[10px] uppercase tracking-[0.18em] text-slate-300"
                    >
                      {item.value}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>

          {filterFailures.length > 0 && (
            <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-amber-100">Why didn’t this fire?</p>
                  <p className="text-xs text-amber-100/80">
                    These rules matched signals but were gated by filters.
                  </p>
                </div>
                <span className="rounded-full border border-amber-500/30 bg-amber-500/20 px-3 py-1 text-[10px] uppercase tracking-[0.24em] text-amber-100">
                  {filterFailures.length} filtered
                </span>
              </div>
              <div className="mt-3 space-y-3 text-xs text-amber-50">
                {filterFailures.map((failure) => (
                  <div key={`filter-failure-${failure.id}`} className="rounded-lg border border-amber-500/20 bg-black/30 p-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div>
                        <p className="text-sm font-semibold text-white">{failure.name}</p>
                        <p className="text-[11px] text-amber-100/80">
                          {failure.action ? `Action: ${String(failure.action).toUpperCase()}` : 'Action pending'}
                        </p>
                      </div>
                    </div>
                    <div className="mt-2 space-y-2">
                      {failure.globalFailed.length > 0 && (
                        <div>
                          <p className="text-[10px] uppercase tracking-[0.24em] text-amber-100/70">Global Filters</p>
                          <div className="mt-1 flex flex-wrap gap-2">
                            {failure.globalFailed.map((filter) => (
                              <span
                                key={`global-fail-${failure.id}-${filter.filter_id}`}
                                className="rounded-full border border-amber-500/30 bg-amber-500/20 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-amber-100"
                              >
                                {filter.name || filter.filter_id}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                      {failure.ruleFailed.length > 0 && (
                        <div>
                          <p className="text-[10px] uppercase tracking-[0.24em] text-amber-100/70">Rule Filters</p>
                          <div className="mt-1 flex flex-wrap gap-2">
                            {failure.ruleFailed.map((filter) => (
                              <span
                                key={`rule-fail-${failure.id}-${filter.filter_id}`}
                                className="rounded-full border border-amber-500/30 bg-amber-500/20 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-amber-100"
                              >
                                {filter.name || filter.filter_id}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
