import { useMemo } from 'react'
import { describeLog, formatTimestamp, isTradeLog } from './botPerformanceFormatters.js'

export function TradeLogList({ logs, logTab, onTabChange, onFocusLog }) {
  const tradeLogs = useMemo(() => logs.filter((entry) => isTradeLog(entry)), [logs])
  const systemLogs = useMemo(() => logs.filter((entry) => !isTradeLog(entry)), [logs])
  const displayedLogs = logTab === 'trade' ? tradeLogs : systemLogs

  return (
    <div className="space-y-3 rounded-3xl border border-white/5 bg-black/40 p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Runtime log</p>
          <div className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 p-1 text-xs text-white">
            <button
              type="button"
              onClick={() => onTabChange('trade')}
              className={`rounded-full px-3 py-1 ${logTab === 'trade' ? 'bg-sky-500/20 text-white' : 'text-slate-200 hover:bg-white/10'}`}
            >
              Trade Events ({tradeLogs.length})
            </button>
            <button
              type="button"
              onClick={() => onTabChange('system')}
              className={`rounded-full px-3 py-1 ${logTab === 'system' ? 'bg-sky-500/20 text-white' : 'text-slate-200 hover:bg-white/10'}`}
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
                className={`rounded-2xl border border-white/10 bg-white/5 p-3 text-sm text-white ${
                  logTab === 'trade' ? 'cursor-pointer transition hover:border-sky-400/40 hover:bg-sky-500/5' : ''
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
          <div className="rounded-2xl border border-dashed border-white/10 p-6 text-center text-sm text-slate-400">
            {logTab === 'trade' ? 'No trade events yet' : 'No system logs yet'}
          </div>
        )}
      </div>
    </div>
  )
}
