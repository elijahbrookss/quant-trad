import React from 'react'
import { formatInstrumentNumber } from '../../../utils'
import { symbolsFromInstrumentSlots } from '../../../utils/instrumentSymbols.js'
import { Button } from '../../ui'

/**
 * Instruments tab showing instrument metadata and validation warnings.
 */
export const InstrumentsTab = ({
  strategy,
  instrumentMap,
  instrumentMessages,
  onAddInstrument
}) => {
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

      <p className="mb-4 text-sm text-slate-400">
        Validate tick sizes, fees, and instrument types for accurate position sizing and cost calculations.
      </p>

      <div className="space-y-3">
        {symbolsFromInstrumentSlots(strategy.instrument_slots).map((symbol) => {
          const key = (symbol || '').toUpperCase()
          const record = key ? instrumentMap.get(key) : null
          const hasMetadata = record && (record.tick_size != null || record.tick_value != null || record.contract_size != null)

          return (
            <div key={key || symbol} className="rounded-xl border border-white/10 bg-black/30 p-4 text-sm text-slate-200">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Symbol</p>
                  <p className="text-lg font-semibold text-white">{symbol || '—'}</p>
                </div>
                <Button variant="ghost" size="sm" onClick={() => onAddInstrument(symbol)}>
                  {hasMetadata ? 'Update metadata' : 'Add metadata'}
                </Button>
              </div>

              {hasMetadata ? (
                <dl className="mt-3 grid gap-3 text-xs text-slate-300 md:grid-cols-2">
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Instrument type</dt>
                    <dd className="text-base text-white">{record.instrument_type || '—'}</dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Tick size</dt>
                    <dd className="text-base text-white">{formatInstrumentNumber(record.tick_size)}</dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Tick value</dt>
                    <dd className="text-base text-white">
                      {formatInstrumentNumber(record.tick_value)} {record.quote_currency || ''}
                    </dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Contract size</dt>
                    <dd className="text-base text-white">{formatInstrumentNumber(record.contract_size)}</dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Maker / Taker fees</dt>
                    <dd className="text-base text-white">
                      {record.maker_fee_rate != null ? `${(Number(record.maker_fee_rate) * 100).toFixed(2)}%` : '—'} /{' '}
                      {record.taker_fee_rate != null ? `${(Number(record.taker_fee_rate) * 100).toFixed(2)}%` : '—'}
                    </dd>
                  </div>
                  <div>
                    <dt className="uppercase tracking-[0.3em] text-slate-500">Min order size</dt>
                    <dd className="text-base text-white">{formatInstrumentNumber(record.min_order_size)}</dd>
                  </div>
                </dl>
              ) : (
                <p className="mt-3 text-sm text-slate-400">No tick or fee metadata stored yet.</p>
              )}
            </div>
          )
        })}
      </div>
    </>
  )
}
