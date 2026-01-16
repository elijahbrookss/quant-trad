import { PlusCircle, Trash2 } from 'lucide-react'

export function WalletBalancesSection({
  walletBalances,
  onWalletBalanceChange,
  onWalletBalanceAdd,
  onWalletBalanceRemove,
  walletError,
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="text-xs font-medium text-slate-400">Initial Balances</label>
        <button
          type="button"
          onClick={onWalletBalanceAdd}
          className="inline-flex items-center gap-1.5 rounded-md border border-slate-800 bg-slate-950/50 px-2.5 py-1.5 text-xs font-medium text-slate-400 transition-colors hover:border-slate-700 hover:bg-slate-950 hover:text-slate-300"
        >
          <PlusCircle className="size-3.5" /> Add Currency
        </button>
      </div>
      <div className="space-y-2">
        {(walletBalances || []).map((row, index) => (
          <div key={row.id || `wallet-${index}`} className="flex flex-wrap items-center gap-2">
            <input
              type="text"
              value={row.currency}
              onChange={(event) => onWalletBalanceChange(index, { currency: event.target.value })}
              className="w-28 rounded-lg border border-slate-800 bg-slate-950/50 px-3 py-2 text-xs font-medium uppercase text-slate-200 transition-colors focus:border-slate-700 focus:bg-slate-950 focus:outline-none"
              placeholder="USDC"
            />
            <input
              type="number"
              step="any"
              value={row.amount}
              onChange={(event) => onWalletBalanceChange(index, { amount: event.target.value })}
              className="flex-1 min-w-[140px] rounded-lg border border-slate-800 bg-slate-950/50 px-3 py-2 text-xs tabular-nums text-slate-200 transition-colors focus:border-slate-700 focus:bg-slate-950 focus:outline-none"
              placeholder="10000.00"
            />
            <button
              type="button"
              onClick={() => onWalletBalanceRemove(index)}
              className="inline-flex items-center gap-1.5 rounded-md border border-rose-900/50 bg-rose-950/30 px-2.5 py-2 text-rose-300 transition-colors hover:border-rose-800/60 hover:bg-rose-950/50"
              aria-label="Remove"
            >
              <Trash2 className="size-3.5" />
            </button>
          </div>
        ))}
      </div>
      <p className="text-xs text-slate-500">Starting wallet balances for backtest simulation</p>
      {walletError ? (
        <div className="rounded-lg border border-rose-900/50 bg-rose-950/20 px-3 py-2 text-xs text-rose-300">
          {walletError}
        </div>
      ) : null}
    </div>
  )
}
