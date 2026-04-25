import { LocateFixed, RefreshCcw } from 'lucide-react'

import { BotLensChart } from '../../../../components/bots/BotLensChart.jsx'
import { OverlayToggleBar } from '../../../../components/bots/OverlayToggleBar.jsx'
import { useChartState } from '../../../../contexts/ChartStateContext.jsx'
import { BotLensPanel } from './BotLensPanel.jsx'
import { SymbolSelectorPanel } from './SymbolSelectorPanel.jsx'

const RUNTIME_CHART_ID = 'botlens-runtime-chart'

export function ChartPanel({
  model,
  symbolSelector,
  overlayOptions,
  overlayVisibility,
  onLoadOlderHistory,
  onSelectSymbol,
  onToggleOverlay,
  onToggleOverlayCollapse,
  overlayPanelCollapsed,
  viewportResetKey,
}) {
  const { getChart } = useChartState()
  const centerView = getChart(RUNTIME_CHART_ID)?.handles?.centerView
  const canRefocus = model.candles.length > 0 && typeof centerView === 'function'

  const actions = (
    <>
      <button
        type="button"
        onClick={() => centerView?.()}
        disabled={!canRefocus}
        className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-white/16 hover:text-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
        aria-label="Refocus chart to the front"
      >
        <LocateFixed className="size-3.5" />
        Refocus
      </button>
      <button
        type="button"
        onClick={onLoadOlderHistory}
        disabled={!model.candles.length}
        className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-white/16 hover:text-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
      >
        <RefreshCcw className="size-3.5" />
        Load Older
      </button>
    </>
  )

  return (
    <BotLensPanel
      eyebrow="Visual Surface"
      title="Runtime chart"
      subtitle={`Selected ${model.selectedSymbol?.label || model.selectedLabel || 'symbol'} · history ${model.historyStatus} · cache ${model.cacheCount}`}
      actions={actions}
      bodyClassName="space-y-5"
    >
      <div className="grid gap-3 lg:grid-cols-[minmax(16rem,20rem)_minmax(0,1fr)]">
        <article className="qt-ops-panel-muted px-4 py-4">
          <p className="qt-ops-kicker">Current Focus</p>
          <p className="mt-2 text-lg font-semibold text-slate-100">{model.selectedSymbol?.label || 'No symbol selected'}</p>
          <p className="mt-2 text-sm text-slate-400">
            {[
              model.selectedSymbol?.status || null,
              model.selectedSymbol?.bootstrapStatus ? `bootstrap ${model.selectedSymbol.bootstrapStatus}` : null,
              model.selectedSymbol?.lastEventAt !== '—' ? `last ${model.selectedSymbol.lastEventAt}` : null,
            ].filter(Boolean).join(' · ') || 'Awaiting runtime data.'}
          </p>
          <div className="qt-mono mt-4 grid gap-2 text-[11px] uppercase tracking-[0.12em] text-slate-500 sm:grid-cols-2 lg:grid-cols-1">
            <span>timeframe {model.selectedSymbol?.timeframe || '—'}</span>
            <span>signals {model.selectedSymbol?.signals || '0'}</span>
            <span>decisions {model.selectedSymbol?.decisions || '0'}</span>
            <span>trades {model.selectedSymbol?.trades || '0'}</span>
            <span>net {model.selectedSymbol?.netPnl || '—'}</span>
          </div>
        </article>

        <SymbolSelectorPanel model={symbolSelector} onSelectSymbol={onSelectSymbol} />
      </div>

      <OverlayToggleBar
        overlays={overlayOptions}
        visibility={overlayVisibility}
        onToggle={onToggleOverlay}
        collapsed={overlayPanelCollapsed}
        onToggleCollapse={onToggleOverlayCollapse}
      />

      {model.status === 'ready' ? (
        <BotLensChart
          chartId={RUNTIME_CHART_ID}
          candles={model.candles}
          trades={model.trades}
          overlays={model.overlays}
          mode={model.mode}
          playbackSpeed={model.playbackSpeed}
          timeframe={model.timeframe}
          overlayVisibility={overlayVisibility}
          viewportResetKey={viewportResetKey}
          heightClass="h-[430px]"
        />
      ) : (
        <div className="qt-ops-console flex h-[430px] items-center justify-center rounded-[3px] border border-dashed border-white/10 text-sm text-slate-400">
          {model.emptyMessage}
        </div>
      )}
    </BotLensPanel>
  )
}
