import { useCallback, useMemo, useState } from 'react'
import { buildDefaultForm, EMPTY_WALLET_ROW, normalizeWalletRow } from './botCreateFormDefaults.js'

const MIN_WALLET_TOTAL = 10

const normalizeWalletRows = (rows) =>
  (Array.isArray(rows) ? rows : []).map((row) => normalizeWalletRow(row))

const normalizeDateValue = (value) => {
  if (!(value instanceof Date)) return ''
  const time = value.getTime()
  return Number.isNaN(time) ? '' : new Date(time).toISOString()
}

export function useBotCreateForm(initialForm = buildDefaultForm()) {
  const [form, setForm] = useState(initialForm)

  const { walletConfig, walletError } = useMemo(() => {
    const rows = normalizeWalletRows(form.wallet_balances)
    if (!rows.length) {
      return { walletConfig: null, walletError: 'Wallet balances are required.' }
    }
    const balances = {}
    let total = 0
    for (const row of rows) {
      const currency = String(row?.currency || '').trim().toUpperCase()
      const amountText = row?.amount
      if (!currency && (amountText === '' || amountText === null || amountText === undefined)) {
        continue
      }
      if (!currency) {
        return { walletConfig: null, walletError: 'Wallet balance currency is required.' }
      }
      const numeric = Number(amountText)
      if (!Number.isFinite(numeric)) {
        return { walletConfig: null, walletError: `wallet_config.balances.${currency} must be numeric.` }
      }
      if (numeric < 0) {
        return { walletConfig: null, walletError: `wallet_config.balances.${currency} must be non-negative.` }
      }
      balances[currency] = numeric
      total += numeric
    }
    if (!Object.keys(balances).length) {
      return { walletConfig: null, walletError: 'Wallet balances cannot be empty.' }
    }
    if (total < MIN_WALLET_TOTAL) {
      return { walletConfig: null, walletError: `Wallet total must be at least ${MIN_WALLET_TOTAL}.` }
    }
    return { walletConfig: { balances }, walletError: null }
  }, [form.wallet_balances])

  const handleChange = useCallback((event) => {
    const { name, value } = event.target
    setForm((prev) => {
      const next = { ...prev, [name]: value }
      if (name === 'run_type' && value !== 'backtest') {
        next.backtest_start = ''
        next.backtest_end = ''
      }
      return next
    })
  }, [])

  const handleBacktestRangeChange = useCallback((range) => {
    const [start, end] = Array.isArray(range) ? range : []
    const normStart = normalizeDateValue(start)
    const normEnd = normalizeDateValue(end)
    setForm((prev) => ({
      ...prev,
      backtest_start: normStart,
      backtest_end: normEnd,
    }))
  }, [])

  const handleStrategyToggle = useCallback((strategyId) => {
    setForm((prev) => {
      const next = prev.strategy_ids.includes(strategyId)
        ? prev.strategy_ids.filter((id) => id !== strategyId)
        : [...prev.strategy_ids, strategyId]
      return { ...prev, strategy_ids: next }
    })
  }, [])

  const handleWalletBalanceChange = useCallback((index, patch) => {
    setForm((prev) => {
      const rows = normalizeWalletRows(prev.wallet_balances).slice()
      if (!rows[index]) return prev
      rows[index] = { ...rows[index], ...patch }
      return { ...prev, wallet_balances: rows }
    })
  }, [])

  const handleWalletBalanceAdd = useCallback(() => {
    setForm((prev) => ({
      ...prev,
      wallet_balances: [...normalizeWalletRows(prev.wallet_balances), EMPTY_WALLET_ROW()],
    }))
  }, [])

  const handleWalletBalanceRemove = useCallback((index) => {
    setForm((prev) => {
      const rows = normalizeWalletRows(prev.wallet_balances).slice()
      rows.splice(index, 1)
      return { ...prev, wallet_balances: rows.length ? rows : [EMPTY_WALLET_ROW()] }
    })
  }, [])

  const resetForm = useCallback((overrides = {}) => {
    setForm({ ...buildDefaultForm(), ...overrides })
  }, [])

  return {
    form,
    setForm,
    walletConfig,
    walletError,
    handleChange,
    handleBacktestRangeChange,
    handleStrategyToggle,
    handleWalletBalanceChange,
    handleWalletBalanceAdd,
    handleWalletBalanceRemove,
    resetForm,
  }
}
