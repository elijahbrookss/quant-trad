import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import {
  Check,
  ChevronLeft,
  ChevronRight,
  Copy,
  Crosshair,
  Info,
  RefreshCcw,
  X,
} from 'lucide-react'

import { useOverlayControls } from '../../../components/bots/hooks/useOverlayControls.js'
import { ChartPanel } from './components/ChartPanel.jsx'
import { OperatorErrorNotice, OperatorSkeleton } from '../../../v2/components/OperatorErrorNotice.jsx'

function toneClass(tone) {
  return {
    emerald: 'border-emerald-400/35 bg-emerald-400/10 text-emerald-100',
    amber: 'border-amber-400/35 bg-amber-400/10 text-amber-100',
    rose: 'border-rose-400/35 bg-rose-400/10 text-rose-100',
    sky: 'border-sky-400/35 bg-sky-400/10 text-sky-100',
    slate: 'border-white/10 bg-white/[0.04] text-slate-200',
  }[tone] || 'border-white/10 bg-white/[0.04] text-slate-200'
}

function severityTone(severity) {
  const value = String(severity || '').toLowerCase()
  if (value === 'critical' || value === 'error') return 'rose'
  if (value === 'warning' || value === 'warn') return 'amber'
  return 'slate'
}

const UNAVAILABLE = 'Unavailable in persisted evidence'

function hasEvidenceValue(value) {
  return value !== undefined && value !== null && value !== ''
}

function lensValue(value) {
  if (!hasEvidenceValue(value)) return UNAVAILABLE
  if (Array.isArray(value) && !value.length) return UNAVAILABLE
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (typeof value === 'object') return JSON.stringify(value, null, 2)
  return String(value)
}

function evidenceMatches(row, query) {
  const needle = String(query || '').trim().toLowerCase()
  if (!needle) return true
  const searchable = {
    ...row,
    technical: row?.technical || null,
  }
  return String(JSON.stringify(searchable) || '').toLowerCase().includes(needle)
}

function RuntimeEmptyState({ mode, detail }) {
  return (
    <div className="qt-ops-console flex min-h-[24rem] items-center justify-center px-6 py-12 text-center">
      <div className="max-w-xl">
        {mode === 'loading' ? <OperatorSkeleton rows={4} label="Opening BotLens" /> : (
          <>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">BotLens</p>
            <p className="mt-3 text-xl font-semibold text-slate-100">Run evidence unavailable</p>
            <p className="mt-2 text-sm leading-6 text-slate-400">{detail}</p>
          </>
        )}
      </div>
    </div>
  )
}

function DetailLens({ detail, onClose }) {
  const [copied, setCopied] = useState(false)
  const [showRaw, setShowRaw] = useState(false)

  useEffect(() => {
    setCopied(false)
    setShowRaw(false)
  }, [detail])

  if (!detail) return null
  const payload = detail.payload || {}
  const compact = detail.layout === 'compact'
  const copy = async () => {
    if (typeof navigator?.clipboard?.writeText !== 'function') return
    await navigator.clipboard.writeText(JSON.stringify(payload, null, 2))
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1400)
  }
  const lens = (
    <div className="fixed inset-0 z-[130] flex items-center justify-center bg-[#02040a]/70 p-4 backdrop-blur-xl" role="dialog" aria-modal="true" aria-label={detail.title}>
      <button className="absolute inset-0 cursor-default" type="button" onClick={onClose} aria-label="Close details" />
      <section className={`qt-ops-console relative z-10 flex w-full flex-col overflow-hidden border-white/15 bg-[#0b0e16]/95 shadow-[0_36px_120px_rgba(0,0,0,0.75)] ${compact ? 'max-h-[min(42rem,calc(100vh-2rem))] max-w-2xl' : 'max-h-[84vh] max-w-4xl'}`}>
        <header className="flex items-start justify-between gap-4 border-b border-white/8 px-5 py-4">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{detail.kicker || 'Evidence'}</p>
            <h2 className="mt-1 text-lg font-semibold text-slate-50">{detail.title}</h2>
            {detail.subtitle ? <p className="mt-1 text-sm text-slate-400">{detail.subtitle}</p> : null}
          </div>
          <button type="button" onClick={onClose} className="rounded-lg border border-white/10 p-2 text-slate-400 transition hover:bg-white/[0.06] hover:text-white">
            <X className="size-4" />
          </button>
        </header>
        <div className="min-h-0 flex-1 overflow-auto p-5">
          {detail.summary?.length ? (
            <div className={`mb-5 grid gap-2 ${compact ? 'sm:grid-cols-3' : 'sm:grid-cols-2 lg:grid-cols-4'}`}>
              {detail.summary.map((row) => (
                <div key={row.label} className="rounded-lg border border-white/8 bg-black/20 px-3 py-2">
                  <p className="text-[10px] uppercase tracking-[0.14em] text-slate-600">{row.label}</p>
                  <p className="mt-1 break-words text-sm text-slate-200">{lensValue(row.value)}</p>
                </div>
              ))}
            </div>
          ) : null}
          {detail.sections?.length ? (
            <div className={`grid gap-4 ${compact ? 'grid-cols-1' : 'lg:grid-cols-2'}`}>
              {detail.sections.map((section) => (
                <section key={section.title} className="overflow-hidden rounded-xl border border-white/8 bg-black/18">
                  <header className="border-b border-white/8 px-4 py-3">
                    <h3 className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-300">{section.title}</h3>
                  </header>
                  <dl className="divide-y divide-white/6">
                    {section.rows.map((row) => {
                      const value = lensValue(row.value)
                      return (
                        <div key={row.label} className="grid gap-1 px-4 py-3 sm:grid-cols-[9rem_minmax(0,1fr)] sm:gap-3">
                          <dt className="text-[11px] text-slate-600">{row.label}</dt>
                          <dd className={`qt-mono whitespace-pre-wrap break-words text-xs leading-5 ${value === UNAVAILABLE ? 'text-amber-200/70' : 'text-slate-300'}`}>{value}</dd>
                        </div>
                      )
                    })}
                  </dl>
                </section>
              ))}
            </div>
          ) : null}
          {showRaw ? (
            <pre className="qt-mono mt-5 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-white/8 bg-black/35 p-4 text-[11px] leading-5 text-slate-300">
              {JSON.stringify(payload, null, 2)}
            </pre>
          ) : null}
        </div>
        <footer className="flex flex-wrap items-center justify-between gap-2 border-t border-white/8 px-5 py-3">
          <button type="button" onClick={() => setShowRaw((value) => !value)} className="rounded-lg border border-white/10 bg-white/[0.025] px-3 py-2 text-xs font-semibold text-slate-400 transition hover:bg-white/[0.07] hover:text-white">
            {showRaw ? 'Hide raw JSON' : 'Show raw JSON'}
          </button>
          <button type="button" onClick={copy} className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-slate-200 transition hover:bg-white/[0.08]">
            {copied ? <Check className="size-3.5 text-emerald-300" /> : <Copy className="size-3.5" />}
            {copied ? 'Copied' : 'Copy troubleshooting details'}
          </button>
        </footer>
      </section>
    </div>
  )
  return typeof document === 'undefined' ? lens : createPortal(lens, document.body)
}

const TopBar = memo(function TopBar({ model, onClose, onRefresh, onDetails }) {
  return (
    <header className="border-b border-white/8 bg-black/10 px-4 py-4 sm:px-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-500">{model.kicker}</p>
            <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold ${toneClass(model.status.tone)}`}>
              {model.status.label}
            </span>
            {model.runMode ? (
              <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold ${toneClass(model.runMode.tone)}`}>
                {model.runMode.label}
              </span>
            ) : null}
          </div>
          <h1 className="mt-2 truncate text-2xl font-semibold tracking-tight text-slate-50">{model.title}</h1>
          <p className="mt-1 truncate text-sm text-slate-400">{model.subtitle}</p>
        </div>
        <div className="flex items-center gap-2">
          <button type="button" onClick={onDetails} className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.035] px-3 py-2 text-sm font-medium text-slate-300 transition hover:bg-white/[0.07] hover:text-white">
            <Info className="size-4" /> Run details
          </button>
          <button type="button" onClick={onRefresh} className="rounded-lg border border-white/10 bg-white/[0.035] p-2.5 text-slate-400 transition hover:bg-white/[0.07] hover:text-white" title="Refresh run evidence">
            <RefreshCcw className="size-4" />
          </button>
          <button type="button" onClick={onClose} className="rounded-lg border border-white/10 bg-white/[0.035] p-2.5 text-slate-400 transition hover:bg-white/[0.07] hover:text-white" title="Exit BotLens">
            <X className="size-4" />
          </button>
        </div>
      </div>
      <div className="mt-4 flex flex-wrap gap-x-6 gap-y-2 border-t border-white/6 pt-3">
        {model.stats.map((stat) => (
          <div key={stat.key} className="flex items-baseline gap-2 text-xs">
            <span className="text-slate-600">{stat.label}</span>
            <span className="font-medium text-slate-200">{stat.value}</span>
          </div>
        ))}
      </div>
    </header>
  )
})

function Notices({ notices }) {
  if (!notices.length) return null
  return (
    <div className="space-y-2 border-b border-white/8 px-4 py-3 sm:px-6">
      {notices.map((notice) => (
        <div key={notice.key} className={`rounded-lg border px-3 py-2 text-sm ${toneClass(notice.tone === 'error' ? 'rose' : notice.tone === 'warning' ? 'amber' : 'slate')}`}>
          {notice.message}
        </div>
      ))}
    </div>
  )
}

function Pager({ model, onPage }) {
  if (!model?.durable && Number(model?.pageCount || 1) <= 1) return null
  const pageIndex = Number(model?.pageIndex || 0)
  const pageCount = Math.max(1, Number(model?.pageCount || 1))
  const total = Math.max(0, Number(model?.total || 0))
  const from = total ? pageIndex * Number(model.limit || 0) + 1 : 0
  const to = Math.min(total, (pageIndex + 1) * Number(model.limit || 0))
  return (
    <div className="flex items-center justify-between gap-3 border-t border-white/8 px-4 py-3">
      <span className="text-xs text-slate-500">{from}–{to} of {total}</span>
      <div className="flex items-center gap-2">
        <button type="button" disabled={pageIndex <= 0 || model.status === 'loading'} onClick={() => onPage(pageIndex - 1)} className="rounded-lg border border-white/10 p-2 text-slate-300 transition hover:bg-white/[0.06] disabled:opacity-30">
          <ChevronLeft className="size-4" />
        </button>
        <span className="qt-mono min-w-20 text-center text-xs text-slate-400">{pageIndex + 1} / {pageCount}</span>
        <button type="button" disabled={pageIndex + 1 >= pageCount || model.status === 'loading'} onClick={() => onPage(pageIndex + 1)} className="rounded-lg border border-white/10 p-2 text-slate-300 transition hover:bg-white/[0.06] disabled:opacity-30">
          <ChevronRight className="size-4" />
        </button>
      </div>
    </div>
  )
}

function VirtualRows({ rows, rowHeight, maxHeight = 520, renderRow, resetKey }) {
  const viewportRef = useRef(null)
  const [scrollTop, setScrollTop] = useState(0)
  const overscan = 5
  const totalHeight = rows.length * rowHeight
  const viewportHeight = Math.min(maxHeight, Math.max(rowHeight, totalHeight))
  const first = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan)
  const visibleCount = Math.ceil(viewportHeight / rowHeight) + overscan * 2
  const last = Math.min(rows.length, first + visibleCount)

  useEffect(() => {
    setScrollTop(0)
    if (viewportRef.current) viewportRef.current.scrollTop = 0
  }, [resetKey])

  return (
    <div
      ref={viewportRef}
      className="overflow-y-auto overscroll-contain"
      style={{ height: viewportHeight }}
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
    >
      <div className="relative" style={{ height: totalHeight }}>
        {rows.slice(first, last).map((row, index) => (
          <div
            key={row.key}
            className="absolute inset-x-0"
            style={{ height: rowHeight, top: (first + index) * rowHeight }}
          >
            {renderRow(row)}
          </div>
        ))}
      </div>
    </div>
  )
}

function EvidenceFilter({ value, onChange, count }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/8 bg-black/10 px-4 py-2.5">
      <input
        type="search"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder="Filter loaded page"
        className="min-w-52 flex-1 rounded-lg border border-white/10 bg-black/25 px-3 py-2 text-xs text-slate-200 outline-none transition placeholder:text-slate-700 focus:border-sky-300/30"
      />
      <span className="text-[11px] text-slate-600">{count} visible in loaded page</span>
    </div>
  )
}

function TableShell({ title, meta, error, status = 'ready', empty, children, pager }) {
  const stageLabel = status === 'loading'
    ? 'Loading'
    : status === 'idle'
      ? 'Queued'
      : status === 'error'
        ? 'Unavailable'
        : 'Ready'
  return (
    <section className="qt-ops-console overflow-hidden">
      <header className="flex items-center justify-between gap-3 border-b border-white/8 px-4 py-3">
        <p className="text-sm font-semibold text-slate-100">{title}</p>
        <div className="flex items-center gap-2">
          <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.1em] ${toneClass(status === 'error' ? 'rose' : status === 'loading' ? 'sky' : 'slate')}`}>{stageLabel}</span>
          <span className="text-xs text-slate-500">{meta}</span>
        </div>
      </header>
      {error ? <div className="p-3"><OperatorErrorNotice error={error} compact /></div> : null}
      {status === 'idle' ? (
        <div className="px-5 py-12 text-center text-sm text-slate-500">Queued until the preceding BotLens evidence stage is ready.</div>
      ) : status === 'loading' && !children ? (
        <div className="p-4"><OperatorSkeleton rows={5} label={`Loading ${title.toLowerCase()}`} /></div>
      ) : children || (
        <div className="px-5 py-12 text-center text-sm text-slate-500">{empty}</div>
      )}
      {pager}
    </section>
  )
}

const DecisionsTab = memo(function DecisionsTab({ model, onPage, onInspect }) {
  const rows = Array.isArray(model.rows) ? model.rows : []
  const [query, setQuery] = useState('')
  const filteredRows = useMemo(
    () => rows.filter((row) => evidenceMatches(row, query)),
    [query, rows],
  )
  return (
    <TableShell
      title="Decision history"
      meta={model.durable ? `${model.total} canonical decisions` : `${rows.length} live decisions`}
      error={model.error}
      status={model.status}
      empty="No decision evidence is available for this instrument."
      pager={<Pager model={model} onPage={onPage} />}
    >
      {rows.length ? (
        <div className="overflow-x-auto">
          <EvidenceFilter value={query} onChange={setQuery} count={filteredRows.length} />
          <div className="min-w-[56rem]">
            <div className="grid grid-cols-[10rem_9rem_9rem_minmax(14rem,1fr)_8rem] border-b border-white/8 bg-black/20 text-[11px] uppercase tracking-[0.11em] text-slate-600">
              <span className="px-4 py-3">Market time</span>
              <span className="px-4 py-3">Action</span>
              <span className="px-4 py-3">Verdict</span>
              <span className="px-4 py-3">Why</span>
              <span className="px-4 py-3 text-right">Price</span>
            </div>
            {filteredRows.length ? (
              <VirtualRows
                rows={filteredRows}
                rowHeight={54}
                resetKey={`${model.offset}:${query}`}
                renderRow={(row) => (
                  <button type="button" onClick={() => onInspect(row)} className="grid h-full w-full grid-cols-[10rem_9rem_9rem_minmax(14rem,1fr)_8rem] items-center border-b border-white/6 text-left text-sm text-slate-300 transition hover:bg-white/[0.035] focus:bg-white/[0.035] focus:outline-none">
                    <span className="truncate px-4 py-3 text-xs text-slate-500">{row.occurredAt}</span>
                    <span className="truncate px-4 py-3"><span className="font-medium text-slate-100">{row.action}</span><span className="ml-2 text-xs text-slate-600">{row.direction}</span></span>
                    <span className="truncate px-4 py-3">{row.verdict}</span>
                    <span className="truncate px-4 py-3 text-slate-400">{row.reason}</span>
                    <span className="qt-mono truncate px-4 py-3 text-right">{row.price}</span>
                  </button>
                )}
              />
            ) : (
              <div className="px-5 py-10 text-center text-sm text-slate-500">No decisions in this loaded page match the filter.</div>
            )}
          </div>
        </div>
      ) : null}
    </TableShell>
  )
})

const TradesTab = memo(function TradesTab({ model, onPage, onFocus, onInspect }) {
  const rows = Array.isArray(model.recentTrades) ? model.recentTrades : []
  const [query, setQuery] = useState('')
  const completedRows = useMemo(() => {
    if (!model.durable) return rows
    return rows.filter((row) => {
      const status = String(row.status || row.technical?.status || '').toLowerCase()
      return hasEvidenceValue(row.technical?.exit_time)
        || hasEvidenceValue(row.technical?.closed_at)
        || ['closed', 'completed', 'exited', 'stopped', 'liquidated'].some((value) => status.includes(value))
    })
  }, [model.durable, rows])
  const filteredRows = useMemo(
    () => completedRows.filter((row) => evidenceMatches(row, query)),
    [completedRows, query],
  )
  return (
    <div className="space-y-4">
      {model.openTrades?.length ? (
        <section className="qt-ops-console p-4">
          <p className="text-sm font-semibold text-slate-100">Active positions / {model.openTrades.length}</p>
          <p className="mt-1 text-xs text-slate-600">Current snapshot state. Active stop and target rays belong only to these positions.</p>
          <div className="mt-3 flex flex-wrap gap-2">
            {model.openTrades.map((entry) => (
              <span key={entry.id} className="rounded-lg border border-emerald-400/25 bg-emerald-400/8 px-3 py-2 text-xs text-emerald-100">
                {entry.chip?.symbol} / {entry.chip?.directionLabel} / {entry.chip?.sizeLabel}
              </span>
            ))}
          </div>
        </section>
      ) : null}
      <TableShell
        title={model.durable ? 'Completed trade history' : 'Recent trade events'}
        meta={model.durable ? `${model.total} canonical completed trades` : `${rows.length} recent trade events`}
        error={model.error}
        status={model.status}
        empty={model.durable ? 'No completed trades are available for this instrument.' : 'No trade events are available for this instrument.'}
        pager={<Pager model={model} onPage={onPage} />}
      >
        {completedRows.length ? (
          <div className="overflow-x-auto">
            <EvidenceFilter value={query} onChange={setQuery} count={filteredRows.length} />
            <div className="min-w-[68rem]">
              <div className="grid grid-cols-[10rem_5rem_7rem_7rem_8rem_minmax(10rem,1fr)_7rem_5rem] border-b border-white/8 bg-black/20 text-[11px] uppercase tracking-[0.11em] text-slate-600">
                <span className="px-4 py-3">Entry</span>
                <span className="px-4 py-3">Side</span>
                <span className="px-4 py-3 text-right">Entry</span>
                <span className="px-4 py-3 text-right">Exit</span>
                <span className="px-4 py-3">Status</span>
                <span className="px-4 py-3">Result</span>
                <span className="px-4 py-3 text-right">Net</span>
                <span />
              </div>
              {filteredRows.length ? (
                <VirtualRows
                  rows={filteredRows}
                  rowHeight={56}
                  resetKey={`${model.offset}:${query}`}
                  renderRow={(row) => (
                    <div
                      role="button"
                      tabIndex={0}
                      onClick={() => onFocus(row)}
                      onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') onFocus(row) }}
                      className="grid h-full cursor-crosshair grid-cols-[10rem_5rem_7rem_7rem_8rem_minmax(10rem,1fr)_7rem_5rem] items-center border-b border-white/6 text-sm text-slate-300 transition hover:bg-sky-400/[0.045] focus:bg-sky-400/[0.045] focus:outline-none"
                    >
                      <span className="truncate px-4 py-3 text-xs text-slate-500">{row.openedAt}</span>
                      <span className="truncate px-4 py-3 font-medium text-slate-100">{row.direction}</span>
                      <span className="qt-mono truncate px-4 py-3 text-right">{row.entryPrice}</span>
                      <span className="qt-mono truncate px-4 py-3 text-right">{row.exitPrice}</span>
                      <span className="truncate px-4 py-3 text-slate-400">{row.status}</span>
                      <span className="truncate px-4 py-3 text-slate-400">{row.exitReason}</span>
                      <span className="qt-mono truncate px-4 py-3 text-right">{row.netPnl}</span>
                      <span className="px-4 py-3 text-right">
                        <button type="button" onClick={(event) => { event.stopPropagation(); onInspect(row) }} className="rounded-md border border-white/8 px-2 py-1 text-[11px] text-slate-500 transition hover:text-white">Details</button>
                      </span>
                    </div>
                  )}
                />
              ) : (
                <div className="px-5 py-10 text-center text-sm text-slate-500">No completed trades in this loaded page match the filter.</div>
              )}
            </div>
            <p className="border-t border-white/6 px-4 py-2 text-[11px] text-slate-600"><Crosshair className="mr-1 inline size-3" /> Select a completed trade to focus its bounded chart window.</p>
          </div>
        ) : null}
      </TableShell>
    </div>
  )
})

const DiagnosticsTab = memo(function DiagnosticsTab({ model, onPage, onInspect }) {
  const entries = Array.isArray(model.entries) ? model.entries : []
  const [query, setQuery] = useState('')
  const filteredEntries = useMemo(
    () => entries.filter((entry) => evidenceMatches(entry, query)),
    [entries, query],
  )
  return (
    <div className="grid gap-4 xl:grid-cols-[18rem_minmax(0,1fr)]">
      <aside className="qt-ops-console self-start overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3"><p className="text-sm font-semibold text-slate-100">Run health</p></header>
        <div className="divide-y divide-white/6">
          {model.checks.slice(0, 8).map((row) => (
            <div key={row.key} className="flex items-center justify-between gap-3 px-4 py-3 text-xs">
              <span className="text-slate-500">{row.label}</span><span className="text-right font-medium text-slate-200">{row.value}</span>
            </div>
          ))}
        </div>
      </aside>
      <TableShell
        title="Diagnostic evidence"
        meta={`${entries.length} groups in page / ${model.total} records`}
        error={model.error}
        status={model.status}
        empty="No retained diagnostics are available."
        pager={<Pager model={{ ...model, durable: true }} onPage={onPage} />}
      >
        {entries.length ? (
          <div>
            <EvidenceFilter value={query} onChange={setQuery} count={filteredEntries.length} />
            {filteredEntries.length ? (
              <VirtualRows
                rows={filteredEntries}
                rowHeight={142}
                maxHeight={568}
                resetKey={`${model.offset}:${query}`}
                renderRow={(entry) => (
                  <button type="button" onClick={() => onInspect(entry)} className="h-full w-full border-b border-white/6 px-4 py-3 text-left transition hover:bg-white/[0.035] focus:bg-white/[0.035] focus:outline-none">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="flex min-w-0 items-center gap-2">
                        <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.11em] ${toneClass(severityTone(entry.severity))}`}>{entry.severity}</span>
                        <span className="truncate text-sm font-semibold text-slate-100">{entry.title || entry.code}</span>
                        <span className="qt-mono shrink-0 text-xs text-slate-500">x{entry.count}</span>
                      </div>
                      <span className="text-[11px] text-slate-600">{entry.occurredAt}</span>
                    </div>
                    <p className="mt-2 line-clamp-2 text-sm leading-6 text-slate-400">{entry.message}</p>
                    <div className="mt-2 flex flex-wrap gap-3 text-[11px] text-slate-600">
                      <span>{entry.component}</span><span>{entry.readinessImpact}</span>
                    </div>
                  </button>
                )}
              />
            ) : (
              <div className="px-5 py-10 text-center text-sm text-slate-500">No diagnostic groups in this loaded page match the filter.</div>
            )}
          </div>
        ) : null}
      </TableShell>
    </div>
  )
})

function TabButton({ tab, active, onClick }) {
  return (
    <button type="button" onClick={onClick} className={`rounded-lg border px-3 py-2 text-sm font-semibold transition ${active ? 'border-sky-300/25 bg-sky-300/10 text-sky-100' : 'border-transparent text-slate-500 hover:border-white/8 hover:bg-white/[0.025] hover:text-slate-200'}`}>
      {tab.label}<span className="ml-2 qt-mono text-xs opacity-60">{tab.badge}</span>
    </button>
  )
}

export function BotLensContent({
  model,
  changeSelectedSymbol,
  loadOlderHistory,
  loadNewerHistory,
  loadDecisionEvidencePage,
  loadTradeEvidencePage,
  loadDiagnosticEvidencePage,
  focusDecision,
  focusTrade,
  onClose,
  open,
  refreshSession,
}) {
  const [activeTab, setActiveTab] = useState('decisions')
  const [overlayPanelCollapsed, setOverlayPanelCollapsed] = useState(false)
  const [detail, setDetail] = useState(null)
  const { overlayOptions, visibility, visibleOverlays, toggleOverlay } = useOverlayControls({
    overlays: model.retrievalPanels.chart.overlays,
  })

  useEffect(() => {
    if (!open) return
    setActiveTab('decisions')
    setDetail(null)
  }, [open, model.botId])

  const chartModel = useMemo(() => ({
    ...model.retrievalPanels.chart,
    overlays: visibleOverlays,
  }), [model.retrievalPanels.chart, visibleOverlays])

  const openRunDetails = useCallback(() => {
    setDetail({
      layout: 'compact',
      kicker: 'Run contract',
      title: model.topBar.title,
      subtitle: model.topBar.subtitle,
      summary: model.topBar.stats.map((row) => ({ label: row.label, value: row.value })),
      sections: [{
        title: 'Identifiers',
        rows: model.topBar.identifiers
          .filter((row) => row.value)
          .map((row) => ({ label: row.label.replaceAll('_', ' '), value: row.value })),
      }],
      payload: Object.fromEntries(model.topBar.identifiers.filter((row) => row.value).map((row) => [row.key, row.value])),
    })
  }, [model.topBar])

  const inspectDecision = useCallback((row) => {
    const evidence = row.technical || {}
    const context = evidence.decision_context && typeof evidence.decision_context === 'object'
      ? evidence.decision_context
      : {}
    focusDecision?.(evidence)
    setDetail({
      kicker: 'Decision evidence',
      title: row.action,
      subtitle: `${row.occurredAt} / ${row.verdict}`,
      summary: [
        { label: 'Direction', value: row.direction },
        { label: 'Price', value: row.price },
        { label: 'Reason', value: row.reason },
        { label: 'Trade', value: row.tradeId },
      ],
      sections: [
        {
          title: 'Signal and verdict',
          rows: [
            { label: 'Action', value: evidence.action ?? context.intent ?? row.action },
            { label: 'Verdict', value: evidence.verdict ?? evidence.status ?? row.verdict },
            { label: 'Direction', value: context.direction ?? evidence.direction ?? evidence.artifact_summary?.side },
            { label: 'Reason code', value: evidence.reason_codes ?? evidence.reason_code ?? evidence.rejection_reason ?? evidence.reason },
            { label: 'Signal ID', value: evidence.signal_id ?? context.signal_id },
          ],
        },
        {
          title: 'Observed market context',
          rows: [
            { label: 'Symbol', value: evidence.symbol ?? context.symbol },
            { label: 'Timeframe', value: evidence.timeframe ?? context.timeframe },
            { label: 'Bar time', value: evidence.bar_time },
            { label: 'Known at', value: evidence.known_at ?? evidence.event_ts },
            { label: 'Selected price', value: evidence.selected_price ?? evidence.price ?? context.signal_price },
            { label: 'Artifact summary', value: evidence.artifact_summary },
          ],
        },
        {
          title: 'Causal engine evidence',
          rows: [
            { label: 'Input evidence', value: evidence.input_evidence ?? evidence.inputs },
            { label: 'Referenced outputs', value: evidence.referenced_outputs },
            { label: 'Output filter trace', value: evidence.output_filter_trace },
            { label: 'Indicator state', value: evidence.indicator_state ?? evidence.indicator_values },
            { label: 'Source references', value: evidence.source_refs },
          ],
        },
        {
          title: 'Risk, sizing, and position',
          rows: [
            { label: 'Quantity', value: evidence.selected_quantity ?? evidence.quantity ?? context.quantity },
            { label: 'Risk request', value: evidence.risk_request ?? context.risk },
            { label: 'Entry request', value: evidence.entry_request ?? evidence.entry_attempt },
            { label: 'Stop / target', value: evidence.stop_target ?? evidence.protective_levels },
            { label: 'Position before', value: evidence.position_before ?? context.position_before },
            { label: 'Position after', value: evidence.position_after ?? context.position_after },
          ],
        },
        {
          title: 'Outcome and linkage',
          rows: [
            { label: 'Accepted', value: evidence.accepted },
            { label: 'Rejected', value: evidence.rejected },
            { label: 'Trade ID', value: evidence.trade_id },
            { label: 'Order ID', value: evidence.order_id },
            { label: 'Rejection detail', value: evidence.rejection_detail ?? evidence.rejection_reason },
          ],
        },
        {
          title: 'Provenance',
          rows: [
            { label: 'Decision ID', value: evidence.decision_id ?? evidence.event_id },
            { label: 'Run ID', value: evidence.run_id },
            { label: 'Bot ID', value: evidence.bot_id },
            { label: 'Strategy ID', value: evidence.strategy_id },
            { label: 'Rule / version', value: evidence.rule_id ?? evidence.strategy_version },
            { label: 'Evidence hash', value: evidence.evidence_hash ?? evidence.payload_hash },
          ],
        },
      ],
      payload: evidence,
    })
  }, [focusDecision])

  const inspectTrade = useCallback((row) => {
    const evidence = row.technical || {}
    const slippage = evidence.slippage ?? (
      hasEvidenceValue(evidence.entry_slippage) || hasEvidenceValue(evidence.exit_slippage)
        ? { entry: evidence.entry_slippage, exit: evidence.exit_slippage }
        : null
    )
    focusTrade?.(evidence)
    setDetail({
      kicker: 'Completed trade evidence',
      title: row.tradeId || row.key,
      subtitle: `${row.openedAt} to ${row.closedAt}`,
      summary: [
        { label: 'Direction', value: row.direction },
        { label: 'Entry', value: row.entryPrice },
        { label: 'Exit', value: row.exitPrice },
        { label: 'Net P&L', value: row.netPnl },
      ],
      sections: [
        {
          title: 'Lifecycle',
          rows: [
            { label: 'Status', value: evidence.status ?? row.status },
            { label: 'Direction', value: evidence.direction ?? evidence.side },
            { label: 'Entry time', value: evidence.entry_time ?? evidence.opened_at ?? evidence.event_ts },
            { label: 'Exit time', value: evidence.exit_time ?? evidence.closed_at },
            { label: 'Exit reason', value: evidence.exit_reason ?? evidence.close_reason },
            { label: 'Bars held', value: evidence.bars_held ?? evidence.holding_bars },
          ],
        },
        {
          title: 'Execution',
          rows: [
            { label: 'Quantity', value: evidence.quantity ?? evidence.qty ?? evidence.contracts },
            { label: 'Entry price', value: evidence.entry_price },
            { label: 'Exit price', value: evidence.exit_price },
            { label: 'Fill evidence', value: evidence.fills ?? evidence.execution_fills },
            { label: 'Slippage', value: slippage },
            { label: 'Partial exits', value: evidence.partial_exits ?? evidence.exit_legs },
          ],
        },
        {
          title: 'Costs and P&L',
          rows: [
            { label: 'Gross P&L', value: evidence.gross_pnl },
            { label: 'Fees', value: evidence.fees ?? evidence.total_fees },
            { label: 'Commission', value: evidence.commission },
            { label: 'Net P&L', value: evidence.net_pnl },
            { label: 'Return', value: evidence.return_pct ?? evidence.return_fraction },
          ],
        },
        {
          title: 'Risk and excursion',
          rows: [
            { label: 'Initial stop', value: evidence.initial_stop ?? evidence.stop_price },
            { label: 'Initial target', value: evidence.initial_target ?? evidence.target_price },
            { label: 'Entry risk', value: evidence.entry_risk ?? evidence.risk_amount },
            { label: 'R multiple', value: evidence.r_multiple ?? evidence.r },
            { label: 'MAE', value: evidence.mae ?? evidence.max_adverse_excursion },
            { label: 'MFE', value: evidence.mfe ?? evidence.max_favorable_excursion },
          ],
        },
        {
          title: 'Provenance',
          rows: [
            { label: 'Trade ID', value: evidence.trade_id },
            { label: 'Decision ID', value: evidence.decision_id },
            { label: 'Run ID', value: evidence.run_id },
            { label: 'Bot ID', value: evidence.bot_id },
            { label: 'Strategy ID', value: evidence.strategy_id },
            { label: 'Source references', value: evidence.source_refs },
          ],
        },
      ],
      payload: evidence,
    })
  }, [focusTrade])

  const focusTradeRow = useCallback((row) => {
    focusTrade?.(row.technical)
  }, [focusTrade])

  const inspectDiagnostic = useCallback((entry) => setDetail({
    kicker: 'Diagnostic group',
    title: entry.title || entry.code,
    subtitle: `${entry.count} occurrence${entry.count === 1 ? '' : 's'} / ${entry.occurredAt}`,
    summary: [
      { label: 'Severity', value: entry.severity },
      { label: 'Source', value: entry.component },
      { label: 'Readiness impact', value: entry.readinessImpact },
      { label: 'Next step', value: entry.suggestedNextStep },
    ],
    sections: [
      {
        title: 'Grouped diagnostic',
        rows: [
          { label: 'Code', value: entry.code },
          { label: 'Occurrence count', value: entry.count },
          { label: 'Observed range', value: entry.occurredAt },
          { label: 'Readiness impact', value: entry.readinessImpact },
          { label: 'Suggested next step', value: entry.suggestedNextStep },
        ],
      },
      {
        title: 'Affected identity',
        rows: [
          { label: 'Component', value: entry.component },
          { label: 'Source', value: entry.technical?.source },
          { label: 'Identity', value: entry.technical?.affected_identity },
          { label: 'Occurrences', value: entry.technical?.occurrences },
        ],
      },
    ],
    payload: entry.technical,
  }), [])

  let tabContent = <DecisionsTab model={model.inspection.decisions} onPage={loadDecisionEvidencePage} onInspect={inspectDecision} />
  if (activeTab === 'trades') {
    tabContent = <TradesTab model={model.inspection.trades} onPage={loadTradeEvidencePage} onFocus={focusTradeRow} onInspect={inspectTrade} />
  } else if (activeTab === 'diagnostics') {
    tabContent = <DiagnosticsTab model={model.inspection.diagnostics} onPage={loadDiagnosticEvidencePage} onInspect={inspectDiagnostic} />
  }

  return (
    <>
      <TopBar model={model.topBar} onClose={onClose} onRefresh={refreshSession} onDetails={openRunDetails} />
      <Notices notices={model.notices} />
      {model.mode !== 'ready' ? (
        <div className="flex-1 p-4 sm:p-6"><RuntimeEmptyState mode={model.mode} detail={model.header.description || model.botLifecycle.detail} /></div>
      ) : (
        <>
          <div className="qt-botlens-chart-zone min-h-0 border-b border-white/8 px-4 py-5 sm:px-6">
            <ChartPanel
              model={chartModel}
              symbolSelector={model.symbolSelector}
              overlayOptions={overlayOptions}
              overlayVisibility={visibility}
              onLoadOlderHistory={loadOlderHistory}
              onLoadNewerHistory={loadNewerHistory}
              onSelectSymbol={changeSelectedSymbol}
              onToggleOverlay={toggleOverlay}
              onToggleOverlayCollapse={() => setOverlayPanelCollapsed((value) => !value)}
              overlayPanelCollapsed={overlayPanelCollapsed}
              viewportResetKey={model.header.meta}
            />
          </div>
          <div className="min-h-0 flex-1 px-4 py-5 sm:px-6">
            <nav className="mb-4 flex flex-wrap gap-1 rounded-xl border border-white/8 bg-black/15 p-1.5">
              {model.tabs.map((tab) => <TabButton key={tab.key} tab={tab} active={activeTab === tab.key} onClick={() => setActiveTab(tab.key)} />)}
            </nav>
            {tabContent}
          </div>
        </>
      )}
      <DetailLens detail={detail} onClose={() => setDetail(null)} />
    </>
  )
}
