import { PlusCircle } from 'lucide-react'

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
        <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Wallet balances</label>
        <button
          type="button"
          onClick={onWalletBalanceAdd}
          className="inline-flex items-center gap-2 rounded-full border border-white/10 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300 hover:border-white/30"
        >
          <PlusCircle className="size-3" /> Add
        </button>
      </div>
      <div className="space-y-2">
        {(walletBalances || []).map((row, index) => (
          <div key={row.id || `wallet-${index}`} className="flex flex-wrap items-center gap-2">
            <input
              type="text"
              value={row.currency}
              onChange={(event) => onWalletBalanceChange(index, { currency: event.target.value })}
              className="w-28 rounded-xl border border-white/10 bg-[#0f1524] px-3 py-2 text-xs uppercase tracking-[0.2em] text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              placeholder="USDC"
            />
            <input
              type="number"
              step="any"
              value={row.amount}
              onChange={(event) => onWalletBalanceChange(index, { amount: event.target.value })}
              className="w-40 rounded-xl border border-white/10 bg-[#0f1524] px-3 py-2 text-xs text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              placeholder="500"
            />
            <button
              type="button"
              onClick={() => onWalletBalanceRemove(index)}
              className="rounded-full border border-white/10 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300 hover:border-white/30"
            >
              Remove
            </button>
          </div>
        ))}
      </div>
      <p className="text-[11px] text-slate-500">Provide starting balances (spot wallets are required).</p>
      {walletError ? <p className="text-xs text-rose-300">{walletError}</p> : null}
    </div>
  )
}
