import React, { useMemo, useState } from 'react'
import { formatInstrumentNumber } from '../../../utils'
import { symbolsFromInstrumentSlots } from '../../../utils/instrumentSymbols.js'
import { Button } from '../../ui'
import { computeInstrumentRow } from '../utils/instrumentRows.js'

/**
 * Instruments tab showing instrument metadata and validation warnings.
 */
export const InstrumentsTab = ({
  strategy,
  instrumentMap,
  instrumentMessages,
  onAddInstrument,
  onRefreshMetadata,
  refreshStatus
}) => {
  const [expanded, setExpanded] = useState(null)

  const formatExpiry = (value) => {
    if (!value) return '—'
    const parsed = new Date(value)
    return Number.isNaN(parsed.valueOf()) ? '—' : parsed.toLocaleString()
  }
  const formatFeeRate = (value) => {
    const rate = Number(value)
    if (!Number.isFinite(rate)) return '—'
    return `${(rate * 100).toFixed(3)}%`
  }
  const formatRelative = (value) => {
    if (!value) return 'Never'
    const ts = typeof value === 'string' || typeof value === 'number' ? new Date(value) : value
    if (Number.isNaN(ts.valueOf())) return 'Unknown'
    const diff = Date.now() - ts.getTime()
    const minutes = Math.floor(diff / 60000)
    if (minutes < 1) return 'Just now'
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ago`
    const days = Math.floor(hours / 24)
    return `${days}d ago`
  }

  const toggleExpand = (key) => {
    setExpanded((prev) => (prev === key ? null : key))
  }
  const instruments = useMemo(() => symbolsFromInstrumentSlots(strategy.instrument_slots), [strategy.instrument_slots])

  const statusBadge = (status) => {
    if (status === 'valid') return { label: 'Valid', className: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-100' }
    if (status === 'missing') return { label: 'Missing fields', className: 'border-amber-500/40 bg-amber-500/10 text-amber-100' }
    return { label: 'Error', className: 'border-rose-500/40 bg-rose-500/10 text-rose-100' }
  }

  return (
    <>
      {instrumentMessages.length > 0 && (
        <div className="mb-4 flex gap-3 rounded-xl border border-amber-400/40 bg-amber-500/5 p-3 text-xs text-amber-100">
          <div className="text-lg leading-5">⚠️</div>
          <div>
            <p className="font-semibold text-amber-200">Metadata issues</p>
            <ul className="mt-1 list-disc space-y-1 pl-4">
              {instrumentMessages.map((entry, idx) => (
                <li key={`${entry.symbol || 'instrument'}-${idx}`}>
                  <span className="font-semibold">{entry.symbol || 'Symbol'}:</span>{' '}
                  {entry.message || 'No metadata stored'}
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}

      <div className="overflow-hidden rounded-xl border border-white/10 bg-black/30">
        <div className="grid grid-cols-[1.2fr_0.8fr_1fr_1fr_0.8fr_0.9fr_0.6fr] items-center gap-2 border-b border-white/5 px-3 py-2 text-[11px] uppercase tracking-[0.24em] text-slate-500">
          <span>Symbol</span>
          <span>Type</span>
          <span>Base / Quote</span>
          <span>Fees</span>
          <span>Status</span>
          <span>Updated</span>
          <span className="text-right">Actions</span>
        </div>

        {instruments.length === 0 ? (
          <div className="px-3 py-3 text-sm text-slate-400">No instruments configured.</div>
        ) : (
          instruments.map((symbol) => {
            const { key, record, hasMetadata, isRefreshing, staleLabel, status } = computeInstrumentRow({
              symbol,
              instrumentMap,
              instrumentMessages,
              refreshStatus,
            })
            const badge = statusBadge(status)
            const isOpen = expanded === key
            const updatedAt = refreshStatus?.[key]?.updatedAt || record?.updated_at
            return (
              <div key={key || symbol} className="border-b border-white/5 last:border-b-0">
                <div
                  role="button"
                  tabIndex={0}
                  onClick={() => toggleExpand(key)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault()
                      toggleExpand(key)
                    }
                  }}
                  className="grid w-full grid-cols-[1.2fr_0.8fr_1fr_1fr_0.8fr_0.9fr_0.6fr] items-center gap-2 px-3 py-1.5 text-left text-xs text-slate-200 hover:bg-white/[0.03] focus:outline-none"
                >
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold leading-none text-white">{symbol || '—'}</span>
                    <span className="text-[12px] text-slate-500">{isOpen ? '▾' : '▸'}</span>
                  </div>
                  <div className="text-slate-300">{record?.instrument_type || '—'}</div>
                  <div className="text-slate-300">
                    {record?.metadata?.instrument_fields?.base_currency || record?.base_currency || '—'} / {record?.quote_currency || record?.metadata?.instrument_fields?.quote_currency || '—'}
                  </div>
                  <div className="text-slate-300">
                    {hasMetadata ? `${formatFeeRate(record.maker_fee_rate)} / ${formatFeeRate(record.taker_fee_rate)}` : '—'}
                  </div>
                  <div>
                    <span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.2em] ${badge.className}`}>
                      {badge.label}
                    </span>
                  </div>
                  <div className="text-slate-400">{updatedAt ? formatRelative(updatedAt) : 'Needs refresh'}</div>
                  <div className="flex justify-end gap-1">
                    {onRefreshMetadata && (
                      <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); onRefreshMetadata(symbol) }} disabled={isRefreshing}>
                        {isRefreshing ? 'Refreshing…' : 'Refresh'}
                      </Button>
                    )}
                    <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); onAddInstrument(symbol) }}>
                      ⋯
                    </Button>
                  </div>
                </div>

                {isOpen && (
                  <div className="grid grid-cols-2 gap-3 border-t border-white/5 bg-white/[0.02] px-4 py-3 text-xs text-slate-200 md:grid-cols-3">
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Tick size</p>
                      <p className="text-sm text-white">{hasMetadata ? formatInstrumentNumber(record.tick_size) : '—'}</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Tick value</p>
                      <p className="text-sm text-white">{hasMetadata ? `${formatInstrumentNumber(record.tick_value)} ${record.quote_currency || ''}` : '—'}</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Contract size</p>
                      <p className="text-sm text-white">{hasMetadata ? formatInstrumentNumber(record.contract_size) : '—'}</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Min order</p>
                      <p className="text-sm text-white">{hasMetadata ? formatInstrumentNumber(record.min_order_size) : '—'}</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Expiry</p>
                      <p className="text-sm text-white">{formatExpiry(record?.expiry_ts)}</p>
                    </div>
                    <div className="flex items-center gap-2">
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Funding</p>
                      <span
                        className={`inline-flex h-4 w-4 items-center justify-center rounded-full border ${record?.has_funding ? 'border-emerald-500 bg-emerald-500/30' : 'border-white/15 bg-white/5'}`}
                        aria-label={record?.has_funding ? 'Funding enabled' : 'Funding unavailable'}
                      />
                    </div>
                    <div className="flex items-center gap-2">
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Shortable</p>
                      <span
                        className={`inline-flex h-4 w-4 items-center justify-center rounded-full border ${record?.can_short ? 'border-emerald-500 bg-emerald-500/30' : 'border-white/15 bg-white/5'}`}
                        aria-label={record?.can_short ? 'Shortable' : 'Not shortable'}
                      />
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Updated at</p>
                      <p className="text-sm text-white">{record?.updated_at ? new Date(record.updated_at).toLocaleString() : '—'}</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Provider</p>
                      <p className="text-sm text-white">{record?.datasource || strategy.datasource || '—'}</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Venue</p>
                      <p className="text-sm text-white">{record?.exchange || strategy.exchange || '—'}</p>
                    </div>
                  </div>
                )}
              </div>
            )
          })
        )}
      </div>
    </>
  )
}
