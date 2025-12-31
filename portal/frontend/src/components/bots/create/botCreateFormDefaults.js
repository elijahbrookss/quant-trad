const createWalletRowId = () => {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `wallet-${Math.random().toString(36).slice(2, 10)}`
}

export const EMPTY_WALLET_ROW = () => ({ id: createWalletRowId(), currency: '', amount: '' })

export const normalizeWalletRow = (row = {}) => ({
  id: row.id || createWalletRowId(),
  currency: row.currency ?? '',
  amount: row.amount ?? '',
})

export function buildDefaultForm() {
  return {
    name: '',
    mode: 'walk-forward',
    run_type: 'backtest',
    backtest_start: '',
    backtest_end: '',
    strategy_ids: [],
    wallet_balances: [EMPTY_WALLET_ROW()],
  }
}
