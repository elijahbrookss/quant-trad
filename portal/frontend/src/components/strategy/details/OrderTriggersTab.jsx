import { useEffect, useMemo, useState } from 'react'

import { Button } from '../../ui'
import { StrategyPreviewCharts, StrategyPreviewSummary } from '../preview'
import { buildTriggerRows } from '../utils/orderTriggers.js'

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

export const OrderTriggersTab = ({
  strategy,
  instruments = [],
  previewWindow,
  previewLoading,
  previewResult,
  previewInstrumentId,
  onInstrumentChange,
  onSubmit,
  onDateRangeChange,
  DateRangePickerComponent,
  onNavigateToRules,
  onNavigateToExecution,
}) => {
  const [filterQuery, setFilterQuery] = useState('')
  const [filterSide, setFilterSide] = useState('ALL')
  const [sortBy, setSortBy] = useState('timestamp')
  const [sortDir, setSortDir] = useState('desc')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [focusRequest, setFocusRequest] = useState(null)

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

  const onSort = (key) => {
    if (sortBy === key) {
      setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortBy(key)
    setSortDir('desc')
  }

  return (
    <div className="space-y-4">
      <form onSubmit={onSubmit} className="space-y-3" aria-label="Order trigger preview controls">
        <div className="rounded-xl border border-white/[0.08] bg-black/30 p-3">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Order triggers</p>
              <p className="text-xs text-slate-400">Read-only preview of typed rule evaluation and indicator-owned overlays.</p>
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
                dateRange={previewWindow.dateRange}
                setDateRange={onDateRangeChange}
              />
            </div>

            <div className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-2.5">
              <div className="mb-2 flex items-center justify-between gap-2">
                <span className="block text-[10px] uppercase tracking-[0.2em] text-slate-500">Instrument</span>
                <Button
                  type="submit"
                  disabled={previewLoading || !previewInstrumentId}
                  loading={previewLoading}
                  className="h-8 px-3 text-xs"
                >
                  {previewLoading ? 'Previewing…' : 'Run preview'}
                </Button>
              </div>
              {instruments.length ? (
                <div className="flex items-center gap-1.5 overflow-x-auto pb-1">
                  {instruments.map((instrument) => (
                    <button
                      key={`preview-instrument-${instrument?.id || instrument?.symbol}`}
                      type="button"
                      onClick={() => onInstrumentChange(instrument?.id)}
                      className={`shrink-0 rounded border px-2.5 py-1 text-xs transition ${
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

          <div className="rounded-xl border border-white/10 bg-white/[0.02] p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-white">Recent decision events</p>
                <p className="text-xs text-slate-400">These rows are built directly from canonical decision artifacts selected by rule evaluation.</p>
                {latestTrigger?.timestamp && (
                  <p className="mt-1 text-[11px] text-slate-500">
                    Latest: <span className="text-slate-300">{latestTrigger.timestamp}</span>
                  </p>
                )}
              </div>
              <div className="flex flex-col items-end gap-1">
                <span className="rounded-full border border-white/10 bg-black/50 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300">
                  {triggerRows.length || 0} decisions
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

                <div className="space-y-2">
                  {pagedRows.map((row) => {
                    const rowSelected = Boolean(
                      focusRequest
                      && focusRequest.rowKey === (row.rowKey || row.id)
                    )
                    return (
                      <button
                        key={row.rowKey || row.id}
                        type="button"
                        className={`w-full rounded-xl border px-4 py-3 text-left transition ${
                          rowSelected
                            ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)]'
                            : 'border-white/10 bg-black/20 hover:border-white/20 hover:bg-white/[0.03]'
                        }`}
                        onClick={() => {
                          if (!row.epoch) return
                          setFocusRequest({
                            rowKey: row.rowKey || row.id,
                            epoch: Number(row.epoch),
                            instrumentId: row.instrumentId || previewInstrumentId || null,
                            at: Date.now(),
                          })
                        }}
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-[220px] flex-1">
                            <div className="flex flex-wrap items-center gap-2">
                              <span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.2em] ${
                                row.direction === 'BUY'
                                  ? 'border-emerald-500/30 bg-emerald-500/15 text-emerald-200'
                                  : 'border-rose-500/30 bg-rose-500/15 text-rose-200'
                              }`}>
                                {row.direction}
                              </span>
                              <span className="text-sm font-semibold text-white">{row.ruleName}</span>
                              <span className="text-xs text-slate-500">{row.timestamp || '—'}</span>
                            </div>
                            <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-300">
                              <span className="rounded-md border border-white/10 bg-white/5 px-2 py-1">
                                Event: {row.triggerType || 'event'}
                              </span>
                              <span className="rounded-md border border-white/10 bg-white/5 px-2 py-1">
                                Signal output: {row.outputRef || '—'}
                              </span>
                              <span className="rounded-md border border-white/10 bg-white/5 px-2 py-1">
                                Indicator: {row.indicatorRef || '—'}
                              </span>
                            </div>
                          </div>
                          <div className="min-w-[220px] max-w-[440px]">
                            <p className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Guards</p>
                            <div className="mt-2 flex flex-wrap gap-2">
                              {row.guards.length ? row.guards.map((guard, index) => (
                                <span
                                  key={`${row.rowKey || row.id}-guard-${index}`}
                                  className="rounded-full border border-amber-400/20 bg-amber-400/10 px-2 py-1 text-[11px] text-amber-100"
                                >
                                  {guardLabel(guard)}
                                </span>
                              )) : (
                                <span className="text-xs text-slate-500">No guards</span>
                              )}
                            </div>
                          </div>
                        </div>
                      </button>
                    )
                  })}
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
          </div>
        </div>
      )}
    </div>
  )
}
