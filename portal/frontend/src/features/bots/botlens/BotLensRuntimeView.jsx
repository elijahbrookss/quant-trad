import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Check, Copy, RefreshCcw, X } from 'lucide-react'

import { ActiveTradeChip } from '../../../components/bots/ActiveTradeChip.jsx'
import DecisionTrace from '../../../components/bots/DecisionTrace/index.jsx'
import { TradeLogList } from '../../../components/bots/TradeLogList.jsx'
import { useOverlayControls } from '../../../components/bots/hooks/useOverlayControls.js'
import { ChartPanel } from './components/ChartPanel.jsx'

function noticeClassName(tone) {
  if (tone === 'error') return 'border-rose-500/35 bg-rose-500/10 text-rose-100'
  if (tone === 'warning') return 'border-amber-500/35 bg-amber-500/10 text-amber-100'
  return 'border-white/10 bg-black/25 text-slate-300'
}

function statusToneClass(tone) {
  return {
    emerald: 'border-emerald-500/45 bg-emerald-500/10 text-emerald-200',
    amber: 'border-amber-500/45 bg-amber-500/10 text-amber-200',
    rose: 'border-rose-500/50 bg-rose-500/12 text-rose-200',
    sky: 'border-sky-500/45 bg-sky-500/10 text-sky-200',
    slate: 'border-white/10 bg-white/5 text-slate-200',
  }[tone] || 'border-white/10 bg-white/5 text-slate-200'
}

function RuntimeEmptyState({ mode, detail }) {
  const title = mode === 'loading'
    ? 'Bootstrapping BotLens runtime'
    : mode === 'error'
      ? 'BotLens runtime unavailable'
      : 'No active runtime selected'
  return (
    <div className="qt-ops-console flex min-h-[22rem] items-center justify-center px-6 py-10 text-center">
      <div className="max-w-xl">
        <p className="qt-ops-kicker">BotLens</p>
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

function TopBar({ topBar, onClose, refreshSession }) {
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
    } catch {}
  }, [])

  return (
    <header className="border-b border-white/8 px-4 py-4 sm:px-5">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="min-w-0">
          <p className="qt-ops-kicker">{topBar.kicker}</p>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <DialogTitle className="text-[1.4rem] font-semibold tracking-[0.01em] text-slate-50">
              {topBar.title}
            </DialogTitle>
            <span className={`qt-ops-chip ${statusToneClass(topBar.status.tone)}`}>{topBar.status.label}</span>
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
            className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
          >
            <RefreshCcw className="size-3.5" />
            Refresh
          </button>
          <button
            type="button"
            onClick={onClose}
            className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
          >
            <X className="size-3.5" />
            Exit Lens
          </button>
        </div>
      </div>

      <div className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
        {topBar.stats.map((stat) => (
          <div key={stat.key} className="qt-ops-panel-muted px-3 py-2.5">
            <p className="qt-ops-kicker">{stat.label}</p>
            <p className="mt-1 text-sm font-semibold text-slate-100">{stat.value}</p>
          </div>
        ))}
      </div>
    </header>
  )
}

function NoticesStrip({ notices }) {
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
}

function TabButton({ active, badge, label, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      data-active={active ? 'true' : 'false'}
      className="qt-ops-tab qt-mono inline-flex items-center gap-2 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.14em]"
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
        <p className="qt-ops-kicker">{title}</p>
      </header>
      {rows.length ? (
        <div className="divide-y divide-white/6">
          {rows.map((row) => (
            <div key={row.key} className="flex items-center justify-between gap-4 px-4 py-3">
              <span className="qt-ops-kicker">{row.label}</span>
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

function StateTab({ model }) {
  return (
    <div className="grid h-full gap-4 xl:grid-cols-2">
      <ReadoutTable title="Run State" rows={model.runRows} />
      <ReadoutTable title="Selected Symbol State" rows={model.selectedRows} />
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
        <p className="qt-ops-kicker">Selected Symbol Trades</p>
      </header>
      <div className="overflow-auto">
        <table className="min-w-full text-left text-sm text-slate-200">
          <thead className="border-b border-white/8 bg-black/25 text-[11px] uppercase tracking-[0.18em] text-slate-500">
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

function TradesTab({ model, hoveredTradeId, onHoverTrade, onSelectSymbol }) {
  return (
    <div className="grid h-full gap-4 xl:grid-cols-[minmax(0,1.05fr)_minmax(24rem,0.95fr)]">
      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <div className="flex items-center justify-between gap-3">
            <p className="qt-ops-kicker">Open Trades</p>
            <span className="qt-mono text-[11px] uppercase tracking-[0.14em] text-slate-500">
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
}

function DecisionsTab({ model }) {
  return (
    <div className="grid h-full gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(18rem,22rem)]">
      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <p className="qt-ops-kicker">Decision Ledger</p>
            <span className="qt-mono text-[11px] uppercase tracking-[0.14em] text-slate-500">status {model.status}</span>
            <span className="qt-mono text-[11px] uppercase tracking-[0.14em] text-slate-500">
              seq {model.nextCursor.afterSeq}
            </span>
          </div>
        </header>
        <div className="h-[calc(100%-3.5rem)] overflow-auto p-3">
          <DecisionTrace ledgerEvents={model.entries} />
        </div>
      </section>

      <div className="space-y-4">
        <ReadoutTable title="Ledger Summary" rows={model.summaryRows} />
        <ReadoutTable title="Capital + P&L" rows={model.walletRows} />
        <ReadoutTable title="Latest Activity" rows={model.latestRows} />
      </div>
    </div>
  )
}

function LogsTab({ model, logTab, onLogTabChange }) {
  return (
    <div className="h-full">
      <TradeLogList logs={model.entries} logTab={logTab} onTabChange={onLogTabChange} onFocusLog={() => {}} />
    </div>
  )
}

function DiagnosticsTab({ model }) {
  return (
    <div className="grid h-full gap-4 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]">
      <section className="qt-ops-console overflow-hidden">
        <header className="border-b border-white/8 px-4 py-3">
          <p className="qt-ops-kicker">Diagnostics Checks</p>
        </header>
        <div className="divide-y divide-white/6">
          {model.checks.map((row) => (
            <div key={row.key} className="flex items-center justify-between gap-3 px-4 py-3">
              <span className="qt-ops-kicker">{row.label}</span>
              <span className="text-sm text-slate-200">{row.value}</span>
            </div>
          ))}
        </div>
        {model.notices.length ? (
          <div className="border-t border-white/8 px-4 py-4">
            <p className="qt-ops-kicker">Notices</p>
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
            <p className="qt-ops-kicker">Warnings</p>
            <span className="qt-mono text-[11px] uppercase tracking-[0.14em] text-slate-500">
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
                  <div className="qt-mono text-right text-[11px] uppercase tracking-[0.12em] text-slate-500">
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
    </div>
  )
}

export function BotLensRuntimeView({
  model,
  changeSelectedSymbol,
  loadOlderHistory,
  onClose,
  open,
  refreshSession,
}) {
  const [activeTab, setActiveTab] = useState('state')
  const [overlayPanelCollapsed, setOverlayPanelCollapsed] = useState(false)
  const [logTab, setLogTab] = useState('trade')
  const [hoveredTradeId, setHoveredTradeId] = useState(null)

  const { overlayOptions, visibility, visibleOverlays, toggleOverlay } = useOverlayControls({
    overlays: model.retrievalPanels.chart.overlays,
  })

  useEffect(() => {
    if (!open) return
    setActiveTab('state')
    setOverlayPanelCollapsed(false)
    setLogTab('trade')
    setHoveredTradeId(null)
  }, [open, model.botId])

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

  let tabContent = <StateTab model={model.inspection.state} />
  if (activeTab === 'trades') {
    tabContent = (
      <TradesTab
        model={model.inspection.trades}
        hoveredTradeId={hoveredTradeId}
        onHoverTrade={setHoveredTradeId}
        onSelectSymbol={changeSelectedSymbol}
      />
    )
  } else if (activeTab === 'decisions') {
    tabContent = <DecisionsTab model={model.inspection.decisions} />
  } else if (activeTab === 'logs') {
    tabContent = <LogsTab model={model.inspection.logs} logTab={logTab} onLogTabChange={setLogTab} />
  } else if (activeTab === 'diagnostics') {
    tabContent = <DiagnosticsTab model={model.inspection.diagnostics} />
  }

  return (
    <Dialog open={open} onClose={onClose} className="relative z-[75]">
      <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 overflow-y-auto px-3 py-3 sm:px-4 sm:py-4">
        <DialogPanel className="qt-ops-shell mx-auto flex min-h-[calc(100vh-1.5rem)] w-full max-w-[min(96vw,118rem)] flex-col overflow-hidden">
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
              <div className="min-h-0 border-b border-white/8 px-4 py-4 sm:px-5">
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
        </DialogPanel>
      </div>
    </Dialog>
  )
}
