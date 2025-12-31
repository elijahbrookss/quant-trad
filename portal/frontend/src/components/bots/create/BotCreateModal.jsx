import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { Bot, X } from 'lucide-react'
import { BotCreateForm } from './BotCreateForm.jsx'

export function BotCreateModal({
  open,
  onClose,
  form,
  strategies,
  strategiesLoading,
  strategyError,
  walletError,
  onSubmit,
  onChange,
  onBacktestRangeChange,
  onStrategyToggle,
  onWalletBalanceChange,
  onWalletBalanceAdd,
  onWalletBalanceRemove,
  error,
}) {
  const submitDisabled =
    !strategies.length ||
    !form.name ||
    !form.strategy_ids.length ||
    !(form.wallet_balances && form.wallet_balances.length) ||
    Boolean(walletError) ||
    (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end))

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-5xl rounded-3xl border border-white/10 bg-[#0b1020] p-6 shadow-2xl shadow-black/50">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-1">
              <DialogTitle className="flex items-center gap-2 text-lg font-semibold text-white">
                <Bot className="size-5 text-[color:var(--accent-text-strong)]" /> Create bot
              </DialogTitle>
              <p className="text-sm text-slate-400">Attach strategies and pick a run type to launch your backtest.</p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded-full p-2 text-slate-400 hover:bg-white/5 hover:text-white"
              aria-label="Close create bot"
            >
              <X className="size-5" />
            </button>
          </div>
          <div className="mt-4 rounded-2xl border border-white/5 bg-white/5 px-3 py-2 text-xs text-slate-300">
            <div className="flex flex-wrap items-center gap-3">
              <span className="rounded-full bg-white/10 px-2 py-1 text-[10px] uppercase tracking-[0.3em] text-white">
                Guided modal
              </span>
              <span>Timeframe and playback speed are inherited from the attached strategies and Bot Lens settings.</span>
            </div>
          </div>
          <div className="mt-6">
            <BotCreateForm
              form={form}
              strategies={strategies}
              strategiesLoading={strategiesLoading}
              strategyError={strategyError}
              walletError={walletError}
              onSubmit={onSubmit}
              onChange={onChange}
              onBacktestRangeChange={onBacktestRangeChange}
              onStrategyToggle={onStrategyToggle}
              onWalletBalanceChange={onWalletBalanceChange}
              onWalletBalanceAdd={onWalletBalanceAdd}
              onWalletBalanceRemove={onWalletBalanceRemove}
              submitDisabled={submitDisabled}
              error={error}
            />
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
