import React from 'react'

/**
 * Component displaying a summary of signal generation results.
 */
export const SignalSummary = ({ result, instrumentId }) => {
  if (!result || !instrumentId) return null

  const instrumentResult = result?.instruments?.[instrumentId]
  if (!instrumentResult) return null

  const {
    window,
    buy_signals: buys = [],
    sell_signals: sells = [],
    rule_results: rules = [],
    status,
    missing_indicators: missingIndicatorsRaw = [],
  } = instrumentResult

  const matchedRules = rules.filter((entry) => entry?.matched).length
  const totalRules = rules.length
  const missingIndicators = Array.isArray(missingIndicatorsRaw)
    ? missingIndicatorsRaw.filter(Boolean)
    : []
  const buySignalCount = buys.reduce((total, entry) => {
    if (!entry) return total
    const signalCount = Array.isArray(entry.signals) && entry.signals.length
      ? entry.signals.length
      : entry.matched
        ? 1
        : 0
    return total + signalCount
  }, 0)
  const sellSignalCount = sells.reduce((total, entry) => {
    if (!entry) return total
    const signalCount = Array.isArray(entry.signals) && entry.signals.length
      ? entry.signals.length
      : entry.matched
        ? 1
        : 0
    return total + signalCount
  }, 0)
  const buyRuleMatches = buys.length
  const sellRuleMatches = sells.length
  const statusLabel = status === 'missing_indicators' ? 'Missing indicators' : 'Complete'
  const statusClasses =
    status === 'missing_indicators'
      ? 'border-amber-500/40 bg-amber-500/10 text-amber-200'
      : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200'

  return (
    <div className="space-y-4 rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] p-4 text-sm text-slate-200">
      <div>
        <h4 className="text-sm font-semibold text-white">Evaluation window</h4>
        <p className="text-xs text-slate-400">
          {window?.start || 'start ?'} → {window?.end || 'end ?'} • {window?.interval || 'interval ?'} •{' '}
          {window?.symbol || 'symbol ?'}
          {window?.datasource ? ` • ${window.datasource}` : ''}
          {window?.exchange ? ` (${window.exchange})` : ''}
        </p>
        <span className={`mt-2 inline-flex rounded-full border px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${statusClasses}`}>
          {statusLabel}
        </span>
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-3 text-emerald-100">
          <p className="text-xs uppercase tracking-[0.3em] text-emerald-200/80">Buy</p>
          <p className="text-lg font-semibold">{buySignalCount}</p>
          <p className="text-[11px] text-emerald-200/70">
            signals · {buyRuleMatches || 0} rule{buyRuleMatches === 1 ? '' : 's'} matched
          </p>
        </div>
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-rose-100">
          <p className="text-xs uppercase tracking-[0.3em] text-rose-200/80">Sell</p>
          <p className="text-lg font-semibold">{sellSignalCount}</p>
          <p className="text-[11px] text-rose-200/70">
            signals · {sellRuleMatches || 0} rule{sellRuleMatches === 1 ? '' : 's'} matched
          </p>
        </div>
        <div className="rounded-xl border border-indigo-500/30 bg-indigo-500/10 p-3 text-indigo-100">
          <p className="text-xs uppercase tracking-[0.3em] text-indigo-200/80">Rules</p>
          <p className="text-lg font-semibold">
            {matchedRules}
            <span className="text-sm text-indigo-200/80">/{totalRules || 0}</span>
          </p>
        </div>
      </div>

      {missingIndicators.length > 0 && (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-100">
          <p className="font-semibold text-amber-200">Indicators unavailable</p>
          <p className="mt-1 text-amber-100/90">
            Recreate or reattach the following indicators before running live checks:
          </p>
          <ul className="mt-1 list-disc space-y-1 pl-4">
            {missingIndicators.map((identifier) => (
              <li key={`missing-${identifier}`} className="text-amber-100">
                {identifier}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
