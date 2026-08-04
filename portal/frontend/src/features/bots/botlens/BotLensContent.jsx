import { memo, useCallback, useEffect, useMemo, useState } from 'react'
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
  if (!detail) return null
  const payload = detail.payload || {}
  const copy = async () => {
    if (typeof navigator?.clipboard?.writeText !== 'function') return
    await navigator.clipboard.writeText(JSON.stringify(payload, null, 2))
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1400)
  }
  return (
    <div className="fixed inset-0 z-[130] flex items-center justify-center bg-[#02040a]/70 p-4 backdrop-blur-xl" role="dialog" aria-modal="true">
      <button className="absolute inset-0 cursor-default" type="button" onClick={onClose} aria-label="Close details" />
      <section className="qt-ops-console relative z-10 flex max-h-[84vh] w-full max-w-3xl flex-col overflow-hidden border-white/15 bg-[#0b0e16]/95 shadow-[0_36px_120px_rgba(0,0,0,0.75)]">
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
            <div className="mb-5 grid gap-2 sm:grid-cols-2">
              {detail.summary.map((row) => (
                <div key={row.label} className="rounded-lg border border-white/8 bg-black/20 px-3 py-2">
                  <p className="text-[10px] uppercase tracking-[0.14em] text-slate-600">{row.label}</p>
                  <p className="mt-1 text-sm text-slate-200">{row.value}</p>
                </div>
              ))}
            </div>
          ) : null}
          <pre className="qt-mono overflow-auto whitespace-pre-wrap break-words rounded-lg border border-white/8 bg-black/35 p-4 text-[11px] leading-5 text-slate-300">
            {JSON.stringify(payload, null, 2)}
          </pre>
        </div>
        <footer className="flex justify-end border-t border-white/8 px-5 py-3">
          <button type="button" onClick={copy} className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-slate-200 transition hover:bg-white/[0.08]">
            {copied ? <Check className="size-3.5 text-emerald-300" /> : <Copy className="size-3.5" />}
            {copied ? 'Copied' : 'Copy troubleshooting details'}
          </button>
        </footer>
      </section>
    </div>
  )
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

function TableShell({ title, meta, error, loading, empty, children, pager }) {
  return (
    <section className="qt-ops-console overflow-hidden">
      <header className="flex items-center justify-between gap-3 border-b border-white/8 px-4 py-3">
        <p className="text-sm font-semibold text-slate-100">{title}</p>
        <span className="text-xs text-slate-500">{meta}</span>
      </header>
      {error ? <div className="p-3"><OperatorErrorNotice error={error} compact /></div> : null}
      {loading ? <div className="p-4"><OperatorSkeleton rows={5} label={`Loading ${title.toLowerCase()}`} /></div> : children || (
        <div className="px-5 py-12 text-center text-sm text-slate-500">{empty}</div>
      )}
      {pager}
    </section>
  )
}

const DecisionsTab = memo(function DecisionsTab({ model, onPage, onInspect }) {
  const rows = Array.isArray(model.rows) ? model.rows : []
  return (
    <TableShell
      title="Decision history"
      meta={model.durable ? `${model.total} canonical decisions` : `${rows.length} live decisions`}
      error={model.error}
      loading={model.status === 'loading' && !rows.length}
      empty="No decision evidence is available for this instrument."
      pager={<Pager model={model} onPage={onPage} />}
    >
      {rows.length ? (
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="border-b border-white/8 bg-black/20 text-[11px] uppercase tracking-[0.11em] text-slate-600">
              <tr><th className="px-4 py-3">Market time</th><th className="px-4 py-3">Action</th><th className="px-4 py-3">Verdict</th><th className="px-4 py-3">Why</th><th className="px-4 py-3 text-right">Price</th></tr>
            </thead>
            <tbody className="divide-y divide-white/6">
              {rows.map((row) => (
                <tr key={row.key} onClick={() => onInspect(row)} className="cursor-pointer text-slate-300 transition hover:bg-white/[0.035]">
                  <td className="whitespace-nowrap px-4 py-3 text-xs text-slate-500">{row.occurredAt}</td>
                  <td className="px-4 py-3"><span className="font-medium text-slate-100">{row.action}</span><span className="ml-2 text-xs text-slate-600">{row.direction}</span></td>
                  <td className="px-4 py-3">{row.verdict}</td>
                  <td className="max-w-[24rem] truncate px-4 py-3 text-slate-400">{row.reason}</td>
                  <td className="qt-mono px-4 py-3 text-right">{row.price}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </TableShell>
  )
})

const TradesTab = memo(function TradesTab({ model, onPage, onFocus, onInspect }) {
  const rows = Array.isArray(model.recentTrades) ? model.recentTrades : []
  return (
    <div className="space-y-4">
      {model.openTrades?.length ? (
        <section className="qt-ops-console p-4">
          <p className="text-sm font-semibold text-slate-100">Open positions · {model.openTrades.length}</p>
          <div className="mt-3 flex flex-wrap gap-2">
            {model.openTrades.map((entry) => (
              <span key={entry.id} className="rounded-lg border border-emerald-400/25 bg-emerald-400/8 px-3 py-2 text-xs text-emerald-100">
                {entry.chip?.symbol} · {entry.chip?.directionLabel} · {entry.chip?.sizeLabel}
              </span>
            ))}
          </div>
        </section>
      ) : null}
      <TableShell
        title="Trade history"
        meta={model.durable ? `${model.total} canonical trades` : `${rows.length} recent trades`}
        error={model.error}
        loading={model.status === 'loading' && !rows.length}
        empty="No trades are available for this instrument."
        pager={<Pager model={model} onPage={onPage} />}
      >
        {rows.length ? (
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="border-b border-white/8 bg-black/20 text-[11px] uppercase tracking-[0.11em] text-slate-600">
                <tr><th className="px-4 py-3">Entry</th><th className="px-4 py-3">Side</th><th className="px-4 py-3 text-right">Entry</th><th className="px-4 py-3 text-right">Exit</th><th className="px-4 py-3">Result</th><th className="px-4 py-3 text-right">Net</th><th className="px-4 py-3" /></tr>
              </thead>
              <tbody className="divide-y divide-white/6">
                {rows.map((row) => (
                  <tr key={row.key} onClick={() => onFocus(row)} className="group cursor-crosshair text-slate-300 transition hover:bg-sky-400/[0.045]">
                    <td className="whitespace-nowrap px-4 py-3 text-xs text-slate-500">{row.openedAt}</td>
                    <td className="px-4 py-3 font-medium text-slate-100">{row.direction}</td>
                    <td className="qt-mono px-4 py-3 text-right">{row.entryPrice}</td>
                    <td className="qt-mono px-4 py-3 text-right">{row.exitPrice}</td>
                    <td className="px-4 py-3 text-slate-400">{row.exitReason}</td>
                    <td className="qt-mono px-4 py-3 text-right">{row.netPnl}</td>
                    <td className="px-4 py-3 text-right">
                      <button type="button" onClick={(event) => { event.stopPropagation(); onInspect(row) }} className="rounded-md border border-white/8 px-2 py-1 text-[11px] text-slate-500 transition hover:text-white">Details</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="border-t border-white/6 px-4 py-2 text-[11px] text-slate-600"><Crosshair className="mr-1 inline size-3" /> Select a row to focus its bounded chart window.</p>
          </div>
        ) : null}
      </TableShell>
    </div>
  )
})

const DiagnosticsTab = memo(function DiagnosticsTab({ model, onPage, onInspect }) {
  const entries = Array.isArray(model.entries) ? model.entries : []
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
        meta={`${entries.length} groups · ${model.total} records`}
        error={model.error}
        loading={model.status === 'loading' && !entries.length}
        empty="No retained diagnostics are available."
        pager={<Pager model={{ ...model, durable: true }} onPage={onPage} />}
      >
        {entries.length ? (
          <div className="grid gap-2 p-3">
            {entries.map((entry) => (
              <button key={entry.key} type="button" onClick={() => onInspect(entry)} className="rounded-lg border border-white/8 bg-black/18 px-4 py-3 text-left transition hover:border-white/15 hover:bg-white/[0.035]">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.11em] ${toneClass(severityTone(entry.severity))}`}>{entry.severity}</span>
                    <span className="text-sm font-semibold text-slate-100">{entry.title || entry.code}</span>
                    {entry.count > 1 ? <span className="qt-mono text-xs text-slate-500">×{entry.count}</span> : null}
                  </div>
                  <span className="text-[11px] text-slate-600">{entry.occurredAt}</span>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-400">{entry.message}</p>
                <div className="mt-2 flex flex-wrap gap-3 text-[11px] text-slate-600">
                  <span>{entry.component}</span><span>{entry.readinessImpact}</span>
                </div>
              </button>
            ))}
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
  loadDecisionEvidencePage,
  loadTradeEvidencePage,
  loadDiagnosticEvidencePage,
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
      kicker: 'Run contract',
      title: model.topBar.title,
      subtitle: model.topBar.subtitle,
      summary: model.topBar.stats.map((row) => ({ label: row.label, value: row.value })),
      payload: Object.fromEntries(model.topBar.identifiers.filter((row) => row.value).map((row) => [row.key, row.value])),
    })
  }, [model.topBar])

  const inspectDecision = useCallback((row) => setDetail({
    kicker: 'Decision evidence',
    title: row.action,
    subtitle: `${row.occurredAt} · ${row.verdict}`,
    summary: [
      { label: 'Direction', value: row.direction },
      { label: 'Price', value: row.price },
      { label: 'Reason', value: row.reason },
      { label: 'Trade', value: row.tradeId || '—' },
    ],
    payload: row.technical,
  }), [])

  const inspectTrade = useCallback((row) => setDetail({
    kicker: 'Trade evidence',
    title: row.tradeId,
    subtitle: `${row.openedAt} → ${row.closedAt}`,
    summary: [
      { label: 'Direction', value: row.direction },
      { label: 'Entry', value: row.entryPrice },
      { label: 'Exit', value: row.exitPrice },
      { label: 'Net P&L', value: row.netPnl },
    ],
    payload: row.technical,
  }), [])

  const focusTradeRow = useCallback((row) => {
    focusTrade?.(row.technical)
    window.requestAnimationFrame(() => window.scrollTo({ top: 0, behavior: 'smooth' }))
  }, [focusTrade])

  const inspectDiagnostic = useCallback((entry) => setDetail({
    kicker: 'Diagnostic group',
    title: entry.title || entry.code,
    subtitle: `${entry.count} occurrence${entry.count === 1 ? '' : 's'} · ${entry.occurredAt}`,
    summary: [
      { label: 'Severity', value: entry.severity },
      { label: 'Source', value: entry.component },
      { label: 'Readiness impact', value: entry.readinessImpact },
      { label: 'Next step', value: entry.suggestedNextStep || 'Inspect evidence' },
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
