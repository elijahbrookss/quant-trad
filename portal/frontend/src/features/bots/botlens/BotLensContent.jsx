import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Check, Copy, RefreshCcw, X } from 'lucide-react'

import { ActiveTradeChip } from '../../../components/bots/ActiveTradeChip.jsx'
import DecisionTrace from '../../../components/bots/DecisionTrace/index.jsx'
import { useOverlayControls } from '../../../components/bots/hooks/useOverlayControls.js'
import { ChartPanel } from './components/ChartPanel.jsx'
import { OperatorErrorNotice, OperatorSkeleton } from '../../../v2/components/OperatorErrorNotice.jsx'

function noticeClassName(tone) {
  if (tone === 'error') return 'border-rose-500/35 bg-rose-500/10 text-rose-100'
  if (tone === 'warning') return 'border-amber-500/35 bg-amber-500/10 text-amber-100'
  return 'border-white/10 bg-black/25 text-slate-300'
}

function statusToneClass(tone) {
  return {
    emerald: 'border-emerald-400/60 bg-emerald-400/15 text-emerald-100 shadow-[0_0_24px_rgba(52,211,153,0.12)]',
    amber: 'border-amber-500/45 bg-amber-500/10 text-amber-200',
    rose: 'border-rose-500/50 bg-rose-500/10 text-rose-200',
    sky: 'border-sky-500/45 bg-sky-500/10 text-sky-200',
    slate: 'border-white/10 bg-white/5 text-slate-200',
  }[tone] || 'border-white/10 bg-white/5 text-slate-200'
}

function statusDotClass(tone) {
  return {
    emerald: 'bg-emerald-300 shadow-[0_0_14px_rgba(52,211,153,0.55)]',
    amber: 'bg-amber-300',
    rose: 'bg-rose-300',
    sky: 'bg-sky-300',
    slate: 'bg-slate-400',
  }[tone] || 'bg-slate-400'
}

function runModeClass(tone) {
  return {
    amber: 'border-amber-300/50 bg-amber-300/12 text-amber-100',
    rose: 'border-rose-300/55 bg-rose-300/12 text-rose-100',
    sky: 'border-sky-300/45 bg-sky-300/10 text-sky-100',
    slate: 'border-white/10 bg-white/5 text-slate-200',
  }[tone] || 'border-white/10 bg-white/5 text-slate-200'
}

function RuntimeEmptyState({ mode, detail }) {
  const title = mode === 'loading'
    ? 'Bootstrapping BotLens runtime'
    : mode === 'error' || mode === 'unavailable'
      ? 'BotLens evidence unavailable'
      : 'No runtime evidence selected'
  return (
    <div className="qt-ops-console flex min-h-[22rem] items-center justify-center px-6 py-10 text-center">
      <div className="max-w-xl">
        <p className="text-xs font-semibold text-slate-400">BotLens</p>
        <p className="mt-3 text-xl font-semibold text-slate-100">{title}</p>
        <p className="mt-2 text-sm text-slate-400">{detail}</p>
      </div>
    </div>
  )
}

const COPY_RESET_MS = 1400

function IdentifierChip({ identifier, copied, onCopy }) {
  if (!identifier?.value) return null
  return (
    <button
      type="button"
      onClick={() => onCopy(identifier)}
      className="qt-mono inline-flex items-center gap-2 rounded-[4px] border border-white/10 bg-black/20 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-300 transition hover:border-white/16 hover:bg-black/30 hover:text-slate-100"
      title={identifier.value}
      aria-label={`Copy ${identifier.label}`}
    >
      <span className="text-slate-500">{identifier.label}</span>
      <span>{identifier.displayValue || identifier.value}</span>
      {copied ? <Check className="size-3.5 text-emerald-300" /> : <Copy className="size-3.5" />}
    </button>
  )
}

const TopBar = memo(function TopBar({ topBar, onClose, refreshSession }) {
  const [copiedKeys, setCopiedKeys] = useState({})
  const resetTimersRef = useRef({})

  useEffect(() => () => {
    Object.values(resetTimersRef.current).forEach((timerId) => clearTimeout(timerId))
  }, [])

  const handleCopyIdentifier = useCallback(async (identifier) => {
    const value = String(identifier?.value || '').trim()
    if (!value || typeof navigator?.clipboard?.writeText !== 'function') return
    try {
      await navigator.clipboard.writeText(value)
      setCopiedKeys((current) => ({ ...current, [identifier.key]: true }))
      if (resetTimersRef.current[identifier.key]) clearTimeout(resetTimersRef.current[identifier.key])
      resetTimersRef.current[identifier.key] = setTimeout(() => {
        delete resetTimersRef.current[identifier.key]
        setCopiedKeys((current) => ({ ...current, [identifier.key]: false }))
      }, COPY_RESET_MS)
    } catch {
      // Clipboard access is best-effort in restricted browser contexts.
    }
  }, [])

  return (
    <header className="border-b border-white/8 px-4 py-3 sm:px-5">
      <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
        <div className="min-w-0">
          <p className="text-xs font-semibold text-slate-400">{topBar.kicker}</p>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <h1 className="text-[1.4rem] font-semibold tracking-[0.01em] text-slate-50">
              {topBar.title}
            </h1>
            <span className={`inline-flex items-center gap-1.5 rounded-[3px] border px-2.5 py-1 text-sm font-semibold ${statusToneClass(topBar.status.tone)}`}>
              <span className={`size-1.5 rounded-full ${statusDotClass(topBar.status.tone)}`} />
              {topBar.status.label}
            </span>
            {topBar.runMode ? (
              <span className={`inline-flex items-center rounded-[3px] border px-2.5 py-1 text-sm font-semibold ${runModeClass(topBar.runMode.tone)}`}>
                {topBar.runMode.label}
              </span>
            ) : null}
          </div>
          <p className="mt-2 text-sm text-slate-300">{topBar.subtitle}</p>
          <div className="mt-3 flex flex-wrap gap-2">
            {(Array.isArray(topBar.identifiers) ? topBar.identifiers : []).map((identifier) => (
              <IdentifierChip
                key={identifier.key}
                identifier={identifier}
                copied={Boolean(copiedKeys[identifier.key])}
                onCopy={handleCopyIdentifier}
              />
            ))}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={refreshSession}
            className="inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
          >
            <RefreshCcw className="size-3.5" />
            Refresh
          </button>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
          >
            <X className="size-3.5" />
            Exit Lens
          </button>
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2 border-t border-white/8 pt-3">
        {topBar.stats.map((stat) => (
          <span key={stat.key} className="inline-flex items-baseline gap-1.5 text-xs">
            <span className="text-slate-600">{stat.label}</span>
            <span className="font-semibold text-slate-200">{stat.value}</span>
          </span>
        ))}
      </div>
    </header>
  )
})

const NoticesStrip = memo(function NoticesStrip({ notices }) {
  if (!notices.length) return null
  return (
    <div className="flex flex-wrap gap-2 border-b border-white/8 px-4 py-3 sm:px-5">
      {notices.map((notice) => (
        <div key={notice.key} className={`rounded-[3px] border px-3 py-2 text-sm ${noticeClassName(notice.tone)}`}>
          {notice.message}
        </div>
      ))}
    </div>
  )
})

function TabButton({ active, badge, label, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      data-active={active ? 'true' : 'false'}
      className="qt-ops-tab inline-flex items-center gap-2 px-3 py-2 text-sm font-semibold"
    >
      <span>{label}</span>
      {badge !== undefined ? <span className="text-slate-500">{badge}</span> : null}
    </button>
  )
}

function ReadoutTable({ title, rows }) {
  return (
    <section className="qt-ops-console overflow-hidden">
      <header className="border-b border-white/8 px-4 py-3">
        <p className="text-sm font-semibold text-slate-100">{title}</p>
      </header>
      {rows.length ? (
        <div className="divide-y divide-white/6">
          {rows.map((row) => (
            <div key={row.key} className="flex items-center justify-between gap-4 px-4 py-3">
              <span className="text-xs font-medium text-slate-500">{row.label}</span>
              <span className="max-w-[60%] text-right text-sm text-slate-200">{row.value}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="px-4 py-8 text-sm text-slate-500">No data.</div>
      )}
    </section>
  )
}

function EmptyConsole({ message }) {
  return (
    <div className="qt-ops-console flex min-h-[12rem] items-center justify-center px-5 py-10 text-center text-sm text-slate-400">
      {message}
    </div>
  )
}

function RecentTradesTable({ rows }) {
  if (!rows.length) {
    return <EmptyConsole message="No recent selected-symbol trades are available yet." />
  }

  return (
    <div className="qt-ops-console overflow-hidden">
      <header className="border-b border-white/8 px-4 py-3">
        <p className="text-sm font-semibold text-slate-100">Selected symbol trades</p>
      </header>
      <div className="overflow-auto">
        <table className="min-w-full text-left text-sm text-slate-200">
          <thead className="border-b border-white/8 bg-black/25 text-xs text-slate-500">
            <tr>
              <th className="px-4 py-3">Symbol</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3">Dir</th>
              <th className="px-4 py-3">Open</th>
              <th className="px-4 py-3">Entry</th>
              <th className="px-4 py-3">Exit</th>
              <th className="px-4 py-3">Net</th>
              <th className="px-4 py-3">Trade</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/6">
            {rows.map((row) => (
              <tr key={row.key}>
                <td className="qt-mono px-4 py-3">{row.symbol}</td>
                <td className="px-4 py-3">{row.status}</td>
                <td className="qt-mono px-4 py-3">{row.direction}</td>
                <td className="px-4 py-3">{row.openedAt}</td>
                <td className="qt-mono px-4 py-3">{row.entryPrice}</td>
                <td className="qt-mono px-4 py-3">{row.exitPrice}</td>
                <td className="qt-mono px-4 py-3">{row.netPnl}</td>
                <td className="qt-mono px-4 py-3">{row.tradeId}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const TradesTab = memo(function TradesTab({ model, hoveredTradeId, onHoverTrade, onSelectSymbol }) {
  return (
    <div className="grid h-full gap-4 xl:grid-cols-[minmax(0,1.05fr)_minmax(24rem,0.95fr)]">
      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <div className="flex items-center justify-between gap-3">
            <p className="text-sm font-semibold text-slate-100">Open trades</p>
            <span className="text-xs text-slate-500">
              {model.openTrades.length} active
            </span>
          </div>
        </header>
        <div className="space-y-2 px-4 py-4">
          {model.openTrades.length ? (
            model.openTrades.map((entry) => (
              <ActiveTradeChip
                key={entry.id}
                chip={entry.chip}
                trade={entry.trade}
                currentPrice={entry.currentPrice}
                latestBarTime={entry.latestBarTime}
                visible={!hoveredTradeId || hoveredTradeId === entry.id}
                onHover={(hovering) => onHoverTrade(hovering ? entry.id : null)}
                isActiveSymbol={entry.isActiveSymbol}
                onClick={() => {
                  if (entry.trade?.symbol_key) onSelectSymbol(entry.trade.symbol_key)
                }}
              />
            ))
          ) : (
            <div className="px-1 py-10 text-center text-sm text-slate-400">No active trades right now.</div>
          )}
        </div>
      </section>

      <RecentTradesTable rows={model.recentTrades} />
    </div>
  )
})

const DecisionsTab = memo(function DecisionsTab({ model, onLoadMore }) {
  const scrollRef = useRef(null)
  const sentinelRef = useRef(null)

  useEffect(() => {
    if (model.autoLoad === false || !model.hasMore || model.status === 'loading') return undefined
    const root = scrollRef.current
    const sentinel = sentinelRef.current
    if (!root || !sentinel || typeof IntersectionObserver === 'undefined') return undefined
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) onLoadMore?.()
    }, { root, rootMargin: '180px 0px' })
    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [model.autoLoad, model.hasMore, model.status, onLoadMore])

  return (
    <div className="grid h-full gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(18rem,22rem)]">
      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <p className="text-sm font-semibold text-slate-100">Decision replay</p>
            <span className="text-xs text-slate-500">{model.entries.length} events · {model.status}</span>
            <span className="qt-mono text-[11px] text-slate-500">
              cursor {model.nextCursor.afterSeq}:{model.nextCursor.afterRowId}
            </span>
          </div>
        </header>
        {model.error ? (
          <div className="border-b border-white/8 p-3">
            <OperatorErrorNotice error={model.error} compact />
          </div>
        ) : null}
        <div ref={scrollRef} className="h-[calc(100%-3.5rem)] overflow-auto p-3">
          {model.status === 'loading' && !model.entries.length ? (
            <OperatorSkeleton rows={5} label="Loading decision replay evidence" />
          ) : (
            <DecisionTrace ledgerEvents={model.entries} />
          )}
          <div ref={sentinelRef} className="flex min-h-12 items-center justify-center py-3">
            {model.hasMore ? (
              <button
                type="button"
                onClick={onLoadMore}
                disabled={model.status === 'loading'}
                className="rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-slate-100 disabled:cursor-wait disabled:opacity-60"
              >
                {model.status === 'loading'
                  ? 'Loading evidence…'
                  : model.autoLoad === false && model.status === 'idle'
                    ? 'Load durable replay'
                    : 'Continue replay'}
              </button>
            ) : (
              <span className="text-xs text-slate-600">Complete for this instrument.</span>
            )}
          </div>
        </div>
      </section>

      <div className="space-y-4">
        <ReadoutTable title="Loaded Evidence" rows={model.summaryRows} />
        <ReadoutTable title="Capital + P&L" rows={model.walletRows} />
        <ReadoutTable title="Latest Activity" rows={model.latestRows} />
      </div>
    </div>
  )
})

const DiagnosticsTab = memo(function DiagnosticsTab({ model }) {
  return (
    <div className="grid h-full gap-4 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]">
      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <p className="text-sm font-semibold text-slate-100">Runtime health</p>
        </header>
        <div className="divide-y divide-white/6">
          {model.checks.map((row) => (
            <div key={row.key} className="flex items-center justify-between gap-3 px-4 py-3">
              <span className="text-xs font-medium text-slate-500">{row.label}</span>
              <span className="text-sm text-slate-200">{row.value}</span>
            </div>
          ))}
        </div>
        {model.notices.length ? (
          <div className="border-t border-white/8 px-4 py-4">
            <p className="text-sm font-semibold text-slate-100">Notices</p>
            <div className="mt-3 space-y-2">
              {model.notices.map((notice) => (
                <div key={notice.key} className={`rounded-[3px] border px-3 py-2 text-sm ${noticeClassName(notice.tone)}`}>
                  {notice.message}
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </section>

      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <div className="flex items-center justify-between gap-3">
            <p className="text-sm font-semibold text-slate-100">Warnings</p>
            <span className="text-xs text-slate-500">
              {model.warnings.count} active
            </span>
          </div>
        </header>
        <div className="space-y-2 px-4 py-4">
          {model.warnings.items.length ? (
            model.warnings.items.map((warning) => (
              <article key={warning.warning_id} className="qt-ops-panel-muted px-3 py-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-slate-100">{warning.title}</p>
                    <p className="mt-1 text-xs leading-relaxed text-slate-400">{warning.message}</p>
                  </div>
                  <div className="text-right text-xs text-slate-500">
                    <p>x{Math.max(1, Number(warning.count || 1) || 1)}</p>
                    <p className="mt-1">{warning.seenLabel}</p>
                  </div>
                </div>
              </article>
            ))
          ) : (
            <div className="px-1 py-10 text-center text-sm text-slate-400">No runtime warnings are active.</div>
          )}
        </div>
      </section>

      <section className="qt-ops-console overflow-hidden xl:col-span-2">
        <header className="border-b border-white/8 px-4 py-3">
          <div className="flex items-center justify-between gap-3">
            <p className="text-sm font-semibold text-slate-100">Retained diagnostics</p>
            <span className="text-xs text-slate-500">{model.entries.length} recorded</span>
          </div>
        </header>
        <div className="space-y-2 px-4 py-4">
          {model.entries.length ? (
            model.entries.map((entry) => (
              <article key={entry.key} className="qt-ops-panel-muted px-3 py-3">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-500">
                      <span className="qt-mono text-slate-300">{entry.level}</span>
                      <span>{entry.code}</span>
                      <span>{entry.component}</span>
                      <span>{entry.status}</span>
                      <span>{entry.occurredAt}</span>
                    </div>
                    <p className="mt-2 text-sm leading-6 text-slate-200">{entry.message}</p>
                  </div>
                  <button
                    type="button"
                    className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-medium text-slate-300 transition hover:border-white/20 hover:bg-white/[0.08] hover:text-white"
                    onClick={() => navigator.clipboard?.writeText(JSON.stringify(entry.technical, null, 2))}
                  >
                    Copy details
                  </button>
                </div>
              </article>
            ))
          ) : (
            <div className="px-1 py-8 text-center text-sm text-slate-400">No retained diagnostic events are available for this symbol.</div>
          )}
        </div>
      </section>
    </div>
  )
})

/**
 * Presentational BotLens body — TopBar, notices, chart zone, tabs. Shared by
 * both the Dialog shell (v1, BotLensRuntimeView) and the routed page shell
 * (v2, BotLensRuntimePageView) so restyling the outer chrome for v2 never
 * touches v1's modal behavior, and both stay on one implementation of the
 * tab/table/chart logic.
 */
export function BotLensContent({
  model,
  changeSelectedSymbol,
  loadOlderHistory,
  loadMoreDecisionEvidence,
  onClose,
  open,
  refreshSession,
}) {
  const [activeTab, setActiveTab] = useState('decisions')
  const [overlayPanelCollapsed, setOverlayPanelCollapsed] = useState(true)
  const [hoveredTradeId, setHoveredTradeId] = useState(null)

  const { overlayOptions, visibility, visibleOverlays, toggleOverlay } = useOverlayControls({
    overlays: model.retrievalPanels.chart.overlays,
  })

  useEffect(() => {
    if (!open) return
    setActiveTab('decisions')
    setOverlayPanelCollapsed(true)
    setHoveredTradeId(null)
  }, [open, model.botId])

  useEffect(() => {
    if (!model.tabs.some((tab) => tab.key === activeTab)) setActiveTab(model.tabs[0]?.key || 'decisions')
  }, [activeTab, model.tabs])

  useEffect(() => {
    if (!hoveredTradeId) return
    const stillVisible = model.currentStatePanels.tradeActivity.openTrades.some((entry) => entry.id === hoveredTradeId)
    if (!stillVisible) setHoveredTradeId(null)
  }, [hoveredTradeId, model.currentStatePanels.tradeActivity.openTrades])

  const chartModel = useMemo(
    () => ({
      ...model.retrievalPanels.chart,
      overlays: visibleOverlays,
    }),
    [model.retrievalPanels.chart, visibleOverlays],
  )

  let tabContent = (
    <DecisionsTab
      model={model.inspection.decisions}
      onLoadMore={loadMoreDecisionEvidence}
    />
  )
  if (activeTab === 'trades') {
    tabContent = (
      <TradesTab
        model={model.inspection.trades}
        hoveredTradeId={hoveredTradeId}
        onHoverTrade={setHoveredTradeId}
        onSelectSymbol={changeSelectedSymbol}
      />
    )
  } else if (activeTab === 'diagnostics') {
    tabContent = <DiagnosticsTab model={model.inspection.diagnostics} />
  }

  return (
    <>
      <TopBar topBar={model.topBar} onClose={onClose} refreshSession={refreshSession} />
      <NoticesStrip notices={model.notices} />

      {model.mode !== 'ready' ? (
        <div className="flex-1 px-4 py-4 sm:px-5">
          <RuntimeEmptyState
            mode={model.mode}
            detail={model.header.description || model.botLifecycle.detail || 'BotLens runtime is not ready.'}
          />
        </div>
      ) : (
        <>
          <div className="qt-botlens-chart-zone min-h-0 border-b border-white/8 px-4 py-4 sm:px-5">
            <ChartPanel
              model={chartModel}
              symbolSelector={model.symbolSelector}
              overlayOptions={overlayOptions}
              overlayVisibility={visibility}
              onLoadOlderHistory={loadOlderHistory}
              onSelectSymbol={changeSelectedSymbol}
              onToggleOverlay={toggleOverlay}
              onToggleOverlayCollapse={() => setOverlayPanelCollapsed((prev) => !prev)}
              overlayPanelCollapsed={overlayPanelCollapsed}
              viewportResetKey={model.header.meta}
            />
          </div>

          <div className="flex min-h-0 flex-1 flex-col px-4 py-4 sm:px-5">
            <div className="mb-4 flex flex-wrap gap-2">
              {model.tabs.map((tab) => (
                <TabButton
                  key={tab.key}
                  active={activeTab === tab.key}
                  badge={tab.badge}
                  label={tab.label}
                  onClick={() => setActiveTab(tab.key)}
                />
              ))}
            </div>
            <div className="min-h-[20rem] flex-1 overflow-auto">
              {tabContent}
            </div>
          </div>
        </>
      )}
    </>
  )
}
