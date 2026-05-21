import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { X } from 'lucide-react'
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
  onStrategySelect,
  onVariantSelect,
  onWalletBalanceChange,
  onWalletBalanceAdd,
  onWalletBalanceRemove,
  error,
}) {
  const submitDisabled =
    !strategies.length ||
    !form.name ||
    !form.strategy_id ||
    !(form.wallet_balances && form.wallet_balances.length) ||
    Boolean(walletError) ||
    (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end))

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="flex max-h-[calc(100vh-2rem)] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-white/[0.06] bg-[#0b1019]/96 shadow-[0_30px_80px_rgba(0,0,0,0.45)]">
          <div className="flex items-start justify-between gap-4 border-b border-white/[0.06] px-6 pt-6 pb-4">
            <div className="space-y-1">
              <DialogTitle className="text-lg font-semibold text-slate-50">
                Create Bot
              </DialogTitle>
              <p className="text-sm text-slate-500">
                Configure one concrete execution run from an existing strategy.
              </p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-white/[0.06] bg-black/30 text-slate-400 transition-colors hover:border-white/[0.1] hover:bg-black/45 hover:text-slate-300"
              aria-label="Close"
            >
              <X className="size-4" />
            </button>
          </div>
          <div className="mt-6 overflow-y-auto px-6 pb-6">
            <BotCreateForm
              form={form}
              strategies={strategies}
              strategiesLoading={strategiesLoading}
              strategyError={strategyError}
              walletError={walletError}
              onSubmit={onSubmit}
              onChange={onChange}
              onBacktestRangeChange={onBacktestRangeChange}
              onStrategySelect={onStrategySelect}
              onVariantSelect={onVariantSelect}
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
