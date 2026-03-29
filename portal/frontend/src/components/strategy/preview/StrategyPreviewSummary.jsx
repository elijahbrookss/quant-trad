import React from 'react'

export const StrategyPreviewSummary = ({ result, instrumentId }) => {
  if (!result || !instrumentId) return null

  const instrumentResult = result?.instruments?.[instrumentId]
  if (!instrumentResult) return null

  const {
    window,
    decision_artifacts: decisionArtifacts = [],
    overlays: overlays = [],
    status,
    missing_indicators: missingIndicatorsRaw = [],
  } = instrumentResult

  const rows = Array.isArray(decisionArtifacts)
    ? decisionArtifacts.filter((entry) => String(entry?.evaluation_result || '') === 'matched_selected')
    : []
  const buyCount = rows.filter((entry) => String(entry?.emitted_intent || '') === 'enter_long').length
  const sellCount = rows.filter((entry) => String(entry?.emitted_intent || '') === 'enter_short').length
  const matchedRules = new Set(rows.map((entry) => entry?.rule_id).filter(Boolean)).size
  const missingIndicators = Array.isArray(missingIndicatorsRaw)
    ? missingIndicatorsRaw.filter(Boolean)
    : []
  const overlayCount = Array.isArray(overlays) ? overlays.length : 0
  const statusLabel = status === 'missing_indicators' ? 'Missing indicators' : 'Complete'
  const statusClasses =
    status === 'missing_indicators'
      ? 'border-amber-500/40 bg-amber-500/10 text-amber-200'
      : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200'

  return (
    <div className="space-y-4 rounded-xl border border-white/10 bg-[#0f1524]/80 p-4 text-sm text-slate-200">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h4 className="text-sm font-semibold text-white">Evaluation window</h4>
          <p className="text-xs text-slate-400">
            {window?.start || 'start ?'} → {window?.end || 'end ?'} • {window?.interval || 'interval ?'} •{' '}
            {window?.symbol || 'symbol ?'}
            {window?.datasource ? ` • ${window.datasource}` : ''}
            {window?.exchange ? ` (${window.exchange})` : ''}
          </p>
        </div>
        <span className={`inline-flex rounded-full border px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${statusClasses}`}>
          {statusLabel}
        </span>
      </div>

      <div className="grid gap-3 md:grid-cols-4">
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 p-3 text-emerald-100">
          <p className="text-[10px] uppercase tracking-[0.3em] text-emerald-200/80">Buy Triggers</p>
          <p className="text-lg font-semibold">{buyCount}</p>
        </div>
        <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-rose-100">
          <p className="text-[10px] uppercase tracking-[0.3em] text-rose-200/80">Sell Triggers</p>
          <p className="text-lg font-semibold">{sellCount}</p>
        </div>
        <div className="rounded-lg border border-indigo-500/30 bg-indigo-500/10 p-3 text-indigo-100">
          <p className="text-[10px] uppercase tracking-[0.3em] text-indigo-200/80">Rules Hit</p>
          <p className="text-lg font-semibold">{matchedRules}</p>
          <p className="text-[11px] text-indigo-200/80">{rows.length} selected decision{rows.length === 1 ? '' : 's'}</p>
        </div>
        <div className="rounded-lg border border-sky-500/30 bg-sky-500/10 p-3 text-sky-100">
          <p className="text-[10px] uppercase tracking-[0.3em] text-sky-200/80">Overlays</p>
          <p className="text-lg font-semibold">{overlayCount}</p>
        </div>
      </div>

      {missingIndicators.length > 0 && (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-100">
          <p className="font-semibold text-amber-200">Indicators unavailable</p>
          <p className="mt-1 text-amber-100/90">
            Recreate or reattach the following indicators before running preview checks:
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
