import React, { useState, useEffect } from 'react'
import { Button } from '../../ui'

/**
 * Component for managing attached indicators to a strategy.
 */
export const AttachedIndicators = ({
  strategy,
  attached,
  availableIndicators,
  onAttach,
  onDetach,
  DropdownSelect,
  ActionButton
}) => {
  const [selected, setSelected] = useState('')

  useEffect(() => {
    setSelected('')
  }, [strategy?.id])

  const handleAttach = async (event) => {
    event.preventDefault()
    if (!selected) return
    await onAttach(selected)
    setSelected('')
  }

  const entries = Array.isArray(attached) ? attached : []

  const renderSignalBadge = (rule, entryId) => {
    const baseLabel = rule?.label || rule?.id || 'Signal'
    const signalType = rule?.signal_type ? rule.signal_type.toUpperCase() : null
    const directionHint = Array.isArray(rule?.directions) && rule.directions.length === 1
      ? String(rule.directions[0].id || '').toLowerCase()
      : null
    const directionIcon = directionHint === 'long' ? '↗' : directionHint === 'short' ? '↘' : null
    const directionText = directionHint === 'long' ? 'Long' : directionHint === 'short' ? 'Short' : null

    return (
      <span
        key={`${entryId}-${rule?.id || baseLabel}`}
        className="inline-flex items-center gap-1 rounded-lg border border-white/12 bg-white/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.15em] text-slate-200"
      >
        {signalType || baseLabel}
        {directionText && (
          <span className={directionHint === 'long' ? 'text-emerald-300' : 'text-rose-300'}>
            {directionIcon} {directionText}
          </span>
        )}
      </span>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <form onSubmit={handleAttach} className="flex flex-1 items-center gap-2">
          <div className="flex-1">
            <DropdownSelect
              label="Indicator"
              value={selected}
              onChange={setSelected}
              placeholder="Attach indicator…"
              options={availableIndicators.map((indicator) => ({
                value: indicator.id,
                label: indicator.name || indicator.type,
              }))}
              disabled={!availableIndicators.length}
              className="w-full"
            />
          </div>
          <ActionButton type="submit" disabled={!selected}>
            Attach
          </ActionButton>
        </form>
      </div>

      {entries.length === 0 ? (
        <div className="rounded-xl border border-dashed border-white/10 bg-black/30 p-4 text-sm text-slate-400">
          No indicators linked yet.
        </div>
      ) : (
        <div className="space-y-3">
          {entries.map((entry) => {
            const isMissing = entry.status !== 'active'
            const params = entry.params || entry.snapshot?.params || {}
            const signals = Array.isArray(entry.signal_rules)
              ? entry.signal_rules
              : Array.isArray(entry.meta?.signal_rules)
                ? entry.meta.signal_rules
                : []
            const related = Array.isArray(entry.strategies) ? entry.strategies : []
            const otherStrategies = related.filter((s) => s.id && s.id !== strategy.id)

            const highlightTokens = []
            const pivotConfirm = params.pivot_breakout_confirmation_bars
            const marketProfileConfirm = params.market_profile_breakout_confirmation_bars
            const retestTolerance = params.market_profile_retest_tolerance_pct
            const binSize = params.bin_size
            const merged = params.market_profile_use_merged_value_areas

            if (pivotConfirm != null && pivotConfirm !== '') {
              highlightTokens.push(`Pivot confirm: ${pivotConfirm} bar${Number(pivotConfirm) === 1 ? '' : 's'}`)
            }
            if (marketProfileConfirm != null && marketProfileConfirm !== '') {
              highlightTokens.push(`MP confirm: ${marketProfileConfirm} bar${Number(marketProfileConfirm) === 1 ? '' : 's'}`)
            }
            if (retestTolerance != null && retestTolerance !== '') {
              const numeric = Number(retestTolerance)
              const pctLabel = Number.isFinite(numeric) ? `${(numeric * 100).toFixed(2)}%` : String(retestTolerance)
              highlightTokens.push(`Retest tolerance: ${pctLabel}`)
            }
            if (binSize != null && binSize !== '') {
              highlightTokens.push(`Bin size: ${binSize}`)
            }
            if (merged != null && merged !== '') {
              const mergedLabel = merged === true || String(merged).toLowerCase() === 'true'
                ? 'Merged value areas'
                : 'Session value areas'
              highlightTokens.push(mergedLabel)
            }

            return (
              <div
                key={entry.id}
                className={`rounded-2xl border p-4 transition ${
                  isMissing
                    ? 'border-rose-500/40 bg-rose-500/10 text-rose-100'
                    : 'border-white/10 bg-white/5 text-slate-100'
                }`}
              >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div className="min-w-0">
                    <h5 className="text-sm font-semibold text-white truncate">
                      {entry.name || entry.type || entry.id}
                    </h5>
                    <p className="text-xs text-slate-300">
                      {entry.type || entry.snapshot?.meta?.type || 'Custom'} • {signals.length} signal{signals.length === 1 ? '' : 's'} available
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span
                      className={`rounded-full px-3 py-1 text-[10px] uppercase tracking-[0.2em] ${
                        isMissing
                          ? 'border border-rose-400/60 bg-rose-500/20 text-rose-100'
                          : 'border border-white/15 bg-black/40 text-slate-200'
                      }`}
                    >
                      {isMissing ? 'Missing' : 'Active'}
                    </span>
                    <button
                      className="rounded-full border border-white/20 px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-slate-200 hover:border-rose-400/70 hover:text-rose-200"
                      type="button"
                      onClick={() => onDetach(entry.id)}
                    >
                      Remove
                    </button>
                  </div>
                </div>

                <dl className="mt-3 grid gap-3 text-[11px] text-slate-300 md:grid-cols-3">
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Signals</dt>
                    <dd className="mt-1 flex flex-wrap gap-1">
                      {signals.length ? (
                        signals.map(rule => renderSignalBadge(rule, entry.id))
                      ) : (
                        <span className="text-slate-500">No signals registered</span>
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Configuration</dt>
                    <dd className="mt-1 flex flex-wrap gap-1">
                      {highlightTokens.length ? (
                        highlightTokens.map((token) => (
                          <span
                            key={`${entry.id}-${token}`}
                            className="rounded-lg border border-white/12 bg-white/5 px-2 py-0.5 font-semibold text-[10px] uppercase tracking-[0.15em] text-slate-200"
                          >
                            {token}
                          </span>
                        ))
                      ) : (
                        <span className="text-slate-500">Default parameters</span>
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Usage</dt>
                    <dd className="mt-1 font-semibold text-slate-100">
                      {otherStrategies.length
                        ? `${otherStrategies.length} other strateg${otherStrategies.length === 1 ? 'y' : 'ies'}`
                        : 'Only used here'}
                    </dd>
                  </div>
                </dl>

                {otherStrategies.length > 0 && (
                  <div className="mt-3 rounded-xl border border-white/10 bg-black/30 p-3 text-xs text-slate-300">
                    <p className="font-semibold text-slate-200">Also used in:</p>
                    <ul className="mt-1 space-y-1">
                      {otherStrategies.map((item) => (
                        <li key={`${entry.id}-strategy-${item.id}`} className="flex items-center justify-between">
                          <span className="truncate text-[11px] text-slate-300">{item.name || item.id}</span>
                          <span className="text-[10px] uppercase tracking-[0.2em] text-slate-500">
                            {Array.isArray(item.rules) ? item.rules.length : 0} rules
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
