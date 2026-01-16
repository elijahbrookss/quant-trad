import ActionButton from '../ui/ActionButton.jsx'
import useInstrumentForm from '../../../hooks/strategy/useInstrumentForm.js'

function InstrumentFormModal({ open, initialValues, onSubmit, onCancel, submitting, error }) {
  const { form, localError, handleChange, handleToggle, handleSubmit } = useInstrumentForm({
    open,
    initialValues,
    onSubmit,
  })

  if (!open) return null

  const errorMessage = localError || error

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-xl space-y-6 rounded-2xl border border-white/10 bg-[#14171f] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">Add instrument metadata</h3>
          <p className="text-sm text-slate-400">
            Define tick sizes, contract multipliers, and fee assumptions for this symbol.
          </p>
        </header>

        <form className="space-y-4" onSubmit={handleSubmit}>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Symbol</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.25em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.symbol}
                onChange={handleChange('symbol')}
                required
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Datasource</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.3em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.datasource}
                onChange={handleChange('datasource')}
                placeholder="e.g. CCXT"
              />
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Exchange</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.exchange}
                onChange={handleChange('exchange')}
                placeholder="e.g. binanceus"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Quote currency</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.2em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.quote_currency}
                onChange={handleChange('quote_currency')}
                placeholder="e.g. USDT"
              />
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Base currency</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.2em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.base_currency}
                onChange={handleChange('base_currency')}
                placeholder="e.g. BTC"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Instrument type</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm uppercase tracking-[0.2em] focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.instrument_type}
                onChange={handleChange('instrument_type')}
                placeholder="e.g. spot, future, swap"
              />
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <label className="flex items-center gap-3 rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border border-white/20 bg-black/60"
                checked={form.can_short}
                onChange={handleToggle('can_short')}
              />
              Can short
            </label>
            <label className="flex items-center gap-3 rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border border-white/20 bg-black/60"
                checked={form.has_funding}
                onChange={handleToggle('has_funding')}
              />
              Has funding
            </label>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <label className="flex items-center gap-3 rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border border-white/20 bg-black/60"
                checked={form.short_requires_borrow}
                onChange={handleToggle('short_requires_borrow')}
              />
              Short requires borrow
            </label>
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Expiry timestamp</label>
            <input
              type="datetime-local"
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              value={form.expiry_ts}
              onChange={handleChange('expiry_ts')}
            />
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Tick size</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.tick_size}
                onChange={handleChange('tick_size')}
                placeholder="0.0001"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Tick value</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.tick_value}
                onChange={handleChange('tick_value')}
                placeholder="e.g. 0.01"
              />
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Contract size</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.contract_size}
                onChange={handleChange('contract_size')}
                placeholder="1"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Min order size</label>
              <input
                type="number"
                step="any"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.min_order_size}
                onChange={handleChange('min_order_size')}
                placeholder="0.01"
              />
            </div>
          </div>

          {(!form.maker_fee_rate || !form.taker_fee_rate) && (
            <div className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-4">
              <div className="flex items-start gap-3">
                <svg className="mt-0.5 h-5 w-5 flex-shrink-0 text-amber-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
                <div className="flex-1">
                  <h4 className="text-sm font-semibold text-amber-300">Fee information required</h4>
                  <p className="mt-1 text-xs text-amber-200/80">
                    Trading fees were not provided by the exchange. Please enter accurate fee rates from a reliable source.
                    Fees are entered as <strong>decimals</strong>, not percentages.
                  </p>
                  <p className="mt-2 text-xs text-amber-200/60">
                    Example: Coinbase Advanced Trade typically charges 0.40% maker / 0.60% taker fees, which should be entered as 0.0040 and 0.0060 respectively.
                  </p>
                </div>
              </div>
            </div>
          )}

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Maker fee (decimal)
                <span className="ml-2 text-[10px] font-normal normal-case tracking-normal text-slate-500">
                  e.g., 0.0004 = 0.04%
                </span>
              </label>
              <input
                type="number"
                step="0.0001"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.maker_fee_rate}
                onChange={handleChange('maker_fee_rate')}
                placeholder="0.0004"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Taker fee (decimal)
                <span className="ml-2 text-[10px] font-normal normal-case tracking-normal text-slate-500">
                  e.g., 0.0006 = 0.06%
                </span>
              </label>
              <input
                type="number"
                step="0.0001"
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.taker_fee_rate}
                onChange={handleChange('taker_fee_rate')}
                placeholder="0.0006"
              />
            </div>
          </div>

          {errorMessage && <p className="text-xs text-rose-300">{errorMessage}</p>}

          <footer className="flex items-center justify-end gap-2">
            <ActionButton type="button" variant="ghost" onClick={onCancel} disabled={submitting}>
              Cancel
            </ActionButton>
            <ActionButton type="submit" disabled={submitting}>
              {submitting ? 'Saving…' : 'Save metadata'}
            </ActionButton>
          </footer>
        </form>
      </div>
    </div>
  )
}

export default InstrumentFormModal
