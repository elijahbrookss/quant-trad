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
        <DialogPanel className="flex max-h-[calc(100vh-2rem)] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-slate-800 bg-slate-950 shadow-2xl">
          <div className="flex items-start justify-between gap-4 border-b border-slate-800 px-6 pt-6 pb-4">
            <div className="space-y-1">
              <DialogTitle className="text-xl font-medium text-slate-50">
                Create Bot
              </DialogTitle>
              <p className="text-sm text-slate-400">
                Move from authored strategy design into one concrete bot configuration without changing compile or runtime semantics.
              </p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-slate-800 bg-slate-900/50 text-slate-400 transition-colors hover:border-slate-700 hover:bg-slate-900 hover:text-slate-300"
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
