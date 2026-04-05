import { useCallback, useMemo } from 'react'

import { createBot } from '../../../adapters/bot.adapter.js'
import { buildDefaultForm } from './botCreateFormDefaults.js'
import { useBotCreateForm } from './useBotCreateForm.js'

const parseEnvText = (text) => {
  const next = {}
  for (const line of String(text || '').split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const idx = trimmed.indexOf('=')
    if (idx <= 0) continue
    const key = trimmed.slice(0, idx).trim()
    const value = trimmed.slice(idx + 1).trim()
    if (key) next[key] = value
  }
  return next
}

const normalizeVariantPayload = (variant, strategy) => ({
  strategy_variant_id: String(variant?.id || '').trim(),
  strategy_variant_name: String(variant?.name || '').trim(),
  resolved_params:
    variant?.param_overrides && typeof variant.param_overrides === 'object'
      ? { ...variant.param_overrides }
      : {},
  risk_config:
    strategy?.risk_config && typeof strategy.risk_config === 'object'
      ? { ...strategy.risk_config }
      : {},
})

const getStrategyVariants = (strategy) =>
  Array.isArray(strategy?.variants) ? strategy.variants : []

const getDefaultVariant = (strategy) => {
  const variants = getStrategyVariants(strategy)
  return variants.find((variant) => variant?.is_default) || variants[0] || null
}

const getVariantById = (strategy, variantId) => {
  const normalized = String(variantId || '').trim()
  if (!normalized) return null
  return getStrategyVariants(strategy).find((variant) => String(variant?.id || '').trim() === normalized) || null
}

export function useBotCreateController({
  strategies,
  fetchStrategyDetail,
  logger,
  onCreated,
  defaults = {},
} = {}) {
  const {
    form,
    setForm,
    walletConfig,
    walletError,
    handleChange,
    handleBacktestRangeChange,
    handleWalletBalanceChange,
    handleWalletBalanceAdd,
    handleWalletBalanceRemove,
    resetForm,
  } = useBotCreateForm(buildDefaultForm())

  const strategiesById = useMemo(() => {
    const map = new Map()
    for (const strategy of Array.isArray(strategies) ? strategies : []) {
      if (strategy?.id) {
        map.set(strategy.id, strategy)
      }
    }
    return map
  }, [strategies])

  const resolveStrategy = useCallback(
    async (strategyId) => {
      const normalizedId = String(strategyId || '').trim()
      if (!normalizedId) {
        return null
      }
      const existing = strategiesById.get(normalizedId) || null
      if (Array.isArray(existing?.variants)) {
        return existing
      }
      if (typeof fetchStrategyDetail !== 'function') {
        return existing
      }
      const detail = await fetchStrategyDetail(normalizedId)
      return detail || existing
    },
    [fetchStrategyDetail, strategiesById],
  )

  const applySelection = useCallback(
    async ({ strategyId = '', variantId = '', preserveName = false, base = {} } = {}) => {
      const strategy = await resolveStrategy(strategyId)
      const variant = getVariantById(strategy, variantId) || getDefaultVariant(strategy)
      resetForm({
        ...base,
        ...(preserveName ? { name: form.name } : {}),
        strategy_id: strategyId,
        ...(variant ? normalizeVariantPayload(variant, strategy) : {
          strategy_variant_id: '',
          strategy_variant_name: '',
          resolved_params: {},
          risk_config: strategy?.risk_config && typeof strategy.risk_config === 'object' ? { ...strategy.risk_config } : {},
        }),
      })
      return strategy
    },
    [form.name, resetForm, resolveStrategy],
  )

  const prepareForCreate = useCallback(
    async ({ strategyId = '', variantId = '', runType = 'backtest' } = {}) => {
      return applySelection({
        strategyId,
        variantId,
        base: {
          run_type: runType,
          snapshot_interval_ms: Number(defaults?.snapshotIntervalMs || 1000),
          bot_env: parseEnvText(defaults?.envText || ''),
        },
      })
    },
    [applySelection, defaults?.envText, defaults?.snapshotIntervalMs],
  )

  const handleStrategySelect = useCallback(
    async (strategyId) => {
      return applySelection({
        strategyId,
        preserveName: true,
        base: {
          ...form,
        },
      })
    },
    [applySelection, form],
  )

  const handleVariantSelect = useCallback(
    (variantId) => {
      const strategy = strategiesById.get(form.strategy_id) || null
      const variant = getVariantById(strategy, variantId)
      setForm((prev) => ({
        ...prev,
        ...(variant
          ? normalizeVariantPayload(variant, strategy)
          : {
              strategy_variant_id: '',
              strategy_variant_name: '',
              resolved_params: {},
              risk_config: strategy?.risk_config && typeof strategy.risk_config === 'object' ? { ...strategy.risk_config } : {},
            }),
      }))
    },
    [form.strategy_id, setForm, strategiesById],
  )

  const submit = useCallback(
    async (event) => {
      event.preventDefault()
      if (!form.name) {
        throw new Error('Bot name is required.')
      }
      if (!form.strategy_id) {
        throw new Error('Select a strategy for this bot.')
      }
      if (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end)) {
        throw new Error('Provide both a start and end date for backtests.')
      }
      if (walletError || !walletConfig) {
        throw new Error(walletError || 'Funding is required.')
      }

      const startISO = form.backtest_start ? new Date(form.backtest_start).toISOString() : undefined
      const endISO = form.backtest_end ? new Date(form.backtest_end).toISOString() : undefined
      const normalizedMode = form.run_type === 'backtest' ? form.mode : 'walk-forward'
      logger?.info?.('bot_create_request', {
        run_type: form.run_type,
        mode: normalizedMode,
        strategy_id: form.strategy_id,
        strategy_variant_id: form.strategy_variant_id || null,
        backtest_start: startISO,
        backtest_end: endISO,
      })

      const payloadBody = {
        ...form,
        snapshot_interval_ms: Number(form.snapshot_interval_ms || 1000),
        bot_env: form.bot_env || {},
        mode: normalizedMode,
        backtest_start: form.run_type === 'backtest' ? startISO : undefined,
        backtest_end: form.run_type === 'backtest' ? endISO : undefined,
        wallet_config: walletConfig,
      }
      delete payloadBody.wallet_balances

      const payload = await createBot(payloadBody)
      logger?.info?.('bot_create_success', { bot_id: payload?.id, strategy_id: form.strategy_id })
      onCreated?.(payload)
      await prepareForCreate({
        strategyId: form.strategy_id,
        variantId: form.strategy_variant_id,
        runType: form.run_type,
      })
      return payload
    },
    [form, logger, onCreated, prepareForCreate, walletConfig, walletError],
  )

  return {
    form,
    setForm,
    walletConfig,
    walletError,
    handleChange,
    handleBacktestRangeChange,
    handleWalletBalanceChange,
    handleWalletBalanceAdd,
    handleWalletBalanceRemove,
    handleStrategySelect,
    handleVariantSelect,
    prepareForCreate,
    submit,
  }
}

export default useBotCreateController
