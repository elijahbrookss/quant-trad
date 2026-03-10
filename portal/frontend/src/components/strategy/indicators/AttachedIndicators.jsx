import React, { useRef, useState, useEffect, useMemo } from 'react'
import { ExternalLink, Unlink2 } from 'lucide-react'
import { Button } from '../../ui'
import { countIndicatorRuleUsage, requiresDetachConfirm } from '../utils/indicatorUsage.js'

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
  ActionButton,
}) => {
  const [selected, setSelected] = useState('')
  const [confirm, setConfirm] = useState(null) // { id, name, impact }
  const attachRef = useRef(null)

  useEffect(() => {
    setSelected('')
    setConfirm(null)
  }, [strategy?.id])

  const entries = Array.isArray(attached) ? attached : []
  const usageMap = useMemo(() => countIndicatorRuleUsage(strategy?.rules || []), [strategy?.rules])

  const handleAttach = async (event) => {
    event.preventDefault()
    if (!selected) return
    await onAttach(selected)
    setSelected('')
  }

  const handleFocusAttach = () => {
    if (!attachRef.current) return
    attachRef.current.scrollIntoView({ behavior: 'smooth', block: 'center' })
    const focusTarget = attachRef.current.querySelector('button')
    focusTarget?.focus()
  }

  const handleDetachRequest = (entry) => {
    const impact = usageMap.get(entry.id) || 0
    if (requiresDetachConfirm(entry.id, strategy?.rules)) {
      setConfirm({ id: entry.id, name: entry.name || entry.type || entry.id, impact })
      return
    }
    onDetach(entry.id)
  }

  const handleConfirmDetach = async () => {
    if (!confirm?.id) return
    await onDetach(confirm.id)
    setConfirm(null)
  }

  const renderSignalBadge = (rule, entryId) => {
    const label = rule?.label || rule?.id || 'Signal'
    return (
      <span
        key={`${entryId}-${label}`}
        className="inline-flex items-center gap-1 rounded-md border border-white/12 bg-white/5 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-200"
      >
        {rule?.signal_type ? rule.signal_type.toUpperCase() : label}
      </span>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2" ref={attachRef}>
        <form onSubmit={handleAttach} className="flex flex-1 items-center gap-2">
          <div className="flex-1">
            <DropdownSelect
              label="Attach signal source"
              value={selected}
              onChange={setSelected}
              placeholder="Search indicators…"
              options={availableIndicators.map((indicator) => ({
                value: indicator.id,
                label: indicator.name || indicator.type,
                description: indicator.type,
                badge: Array.isArray(indicator.signal_rules) ? `${indicator.signal_rules.length} signals` : null,
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
          <p>No indicators attached. Add signal sources from QuantLab, then attach them here.</p>
          <div className="mt-3">
            <ActionButton variant="ghost" onClick={handleFocusAttach}>
              Attach indicator
            </ActionButton>
          </div>
        </div>
      ) : (
        <div className="divide-y divide-white/5 rounded-xl border border-white/10 bg-black/30">
          {entries.map((entry) => {
            const signals = Array.isArray(entry.signal_rules)
              ? entry.signal_rules
              : Array.isArray(entry.meta?.signal_rules)
                ? entry.meta.signal_rules
                : []
            const impact = usageMap.get(entry.id) || 0
            return (
              <div key={entry.id} className="flex flex-wrap items-center gap-3 px-4 py-3">
                <div className="min-w-[200px] flex-1">
                  <div className="flex items-center gap-2">
                    <p className="text-sm font-semibold text-white truncate">{entry.name || entry.type || entry.id}</p>
                    <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[10px] uppercase tracking-[0.22em] text-slate-300">
                      {entry.type || entry.snapshot?.meta?.type || 'Custom'}
                    </span>
                  </div>
                  <div className="mt-1 flex flex-wrap items-center gap-1.5 text-[11px] text-slate-400">
                    {signals.length ? signals.map((rule) => renderSignalBadge(rule, entry.id)) : (
                      <span className="text-slate-500">No signals</span>
                    )}
                  </div>
                </div>

                <div className="flex items-center gap-1">
                  <a
                    href={`/quantlab/indicators/${entry.id}`}
                    target="_blank"
                    rel="noreferrer"
                    className="flex h-8 w-8 items-center justify-center rounded-md text-slate-400 transition hover:bg-white/5 hover:text-white"
                    title="Open in QuantLab"
                  >
                    <ExternalLink className="h-4 w-4" />
                  </a>
                  <button
                    className="flex h-8 w-8 items-center justify-center rounded-md text-slate-400 transition hover:bg-rose-500/10 hover:text-rose-400"
                    type="button"
                    onClick={() => handleDetachRequest(entry)}
                    title="Detach indicator"
                  >
                    <Unlink2 className="h-4 w-4" />
                  </button>
                </div>

                {impact > 0 && (
                  <p className="w-full text-[11px] text-amber-200">
                    Referenced by {impact} rule{impact === 1 ? '' : 's'} — detaching will break them.
                  </p>
                )}
              </div>
            )
          })}
        </div>
      )}

      {confirm && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/70 px-4 py-8">
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-[#111725] p-5 shadow-xl">
            <p className="text-sm font-semibold text-white">Detach indicator?</p>
            <p className="mt-1 text-xs text-slate-300">
              {confirm.impact
                ? `Used by ${confirm.impact} rule${confirm.impact === 1 ? '' : 's'}. Detaching will break them.`
                : 'Detach this indicator from the strategy.'}
            </p>
            <div className="mt-4 flex justify-end gap-2 text-sm">
              <Button variant="ghost" onClick={() => setConfirm(null)}>Cancel</Button>
              <Button variant="danger" onClick={handleConfirmDetach}>Detach anyway</Button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
