import { useMemo } from 'react'
import { describeLog, formatTimestamp, isTradeLog } from './botPerformanceFormatters.js'

export function TradeLogList({ logs, logTab, onTabChange, onFocusLog }) {
  const tradeLogs = useMemo(() => logs.filter((entry) => isTradeLog(entry)), [logs])
  const systemLogs = useMemo(() => logs.filter((entry) => !isTradeLog(entry)), [logs])
  const displayedLogs = logTab === 'trade' ? tradeLogs : systemLogs

  return (
    <div className="space-y-3 rounded-2xl border border-white/10 bg-black/20 p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Runtime Log</p>
          <div className="inline-flex items-center gap-1 rounded-lg border border-white/10 bg-white/5 p-1 text-xs text-white">
            <button
              type="button"
              onClick={() => onTabChange('trade')}
              className={`rounded-md px-3 py-1.5 transition ${
                logTab === 'trade'
                  ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                  : 'text-slate-200 hover:bg-white/10'
              }`}
            >
              Trade Events ({tradeLogs.length})
            </button>
            <button
              type="button"
              onClick={() => onTabChange('system')}
              className={`rounded-md px-3 py-1.5 transition ${
                logTab === 'system'
                  ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                  : 'text-slate-200 hover:bg-white/10'
              }`}
            >
              System Logs ({systemLogs.length})
            </button>
          </div>
        </div>
        <span className="text-xs text-slate-400">Showing last {displayedLogs.length} events</span>
      </div>
      <div className="max-h-64 space-y-2 overflow-y-auto pr-1">
        {displayedLogs.length ? (
          displayedLogs
            .slice()
            .reverse()
            .map((entry, idx) => (
              <article
                key={entry.id || `${entry.timestamp || 'log'}-${idx}`}
                onClick={logTab === 'trade' ? () => onFocusLog(entry) : undefined}
                className={`rounded-xl border border-white/10 bg-white/5 p-3 text-sm text-white ${
                  logTab === 'trade'
                    ? 'cursor-pointer transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-10)]'
                    : ''
                }`}
              >
                <div className="flex items-center justify-between text-xs uppercase tracking-[0.3em] text-slate-400">
                  <span>{entry.event || 'event'}</span>
                  <span>{formatTimestamp(entry.event_time || entry.bar_time || entry.timestamp)}</span>
                </div>
                <p className="mt-1 text-base font-semibold text-white">{describeLog(entry)}</p>
                <div className="mt-1 flex flex-wrap gap-3 text-[11px] uppercase tracking-[0.3em] text-slate-500">
                  {entry.trade_id ? <span>Trade {entry.trade_id.slice(0, 8)}</span> : null}
                  {entry.bar_time ? <span>Bar {formatTimestamp(entry.bar_time)}</span> : null}
                  {entry.symbol ? <span>{entry.symbol}</span> : null}
                </div>
              </article>
            ))
        ) : (
          <div className="rounded-xl border border-dashed border-white/10 p-6 text-center text-sm text-slate-400">
            {logTab === 'trade' ? 'No trade events yet' : 'No system logs yet'}
          </div>
        )}
      </div>
    </div>
  )
}
