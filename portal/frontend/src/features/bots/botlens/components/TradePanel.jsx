import { useMemo } from 'react'

import { ActiveTradeChip } from '../../../../components/bots/ActiveTradeChip.jsx'
import { TradeLogList } from '../../../../components/bots/TradeLogList.jsx'
import { BotLensPanel } from './BotLensPanel.jsx'

function countTradeLogs(logs) {
  return (Array.isArray(logs) ? logs : []).filter((entry) => entry?.trade_id).length
}

export function TradePanel({
  model,
  hoveredTradeId,
  logTab,
  onHoverTrade,
  onLogTabChange,
  onSelectSymbol,
}) {
  const tradeLogCount = useMemo(() => countTradeLogs(model.logs), [model.logs])

  return (
    <BotLensPanel
      eyebrow="Current State"
      title="Trades and runtime log"
      subtitle={`Open trades come from current run state. Runtime log entries come from selected-symbol base/live state. Trade logs: ${tradeLogCount}.`}
      bodyClassName="space-y-4"
    >
      <div>
        <div className="mb-3 flex items-center justify-between">
          <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Open trades</p>
          <p className="text-xs text-slate-500">{model.openTrades.length} active</p>
        </div>
        {model.openTrades.length ? (
          <div className="grid gap-2">
            {model.openTrades.map((entry) => (
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
            ))}
          </div>
        ) : (
          <div className="rounded-xl border border-dashed border-white/10 px-4 py-6 text-sm text-slate-400">
            No active trades right now.
          </div>
        )}
      </div>

      <TradeLogList
        logs={model.logs}
        logTab={logTab}
        onTabChange={onLogTabChange}
        onFocusLog={() => {}}
      />
    </BotLensPanel>
  )
}
