import { useCallback, useEffect, useMemo, useState } from 'react'

import {
  attachStrategyIndicator,
  createStrategy,
  createStrategyVariant,
  createStrategyRule,
  deleteStrategyVariant,
  deleteStrategy,
  deleteStrategyRule,
  detachStrategyIndicator,
  updateStrategy,
  updateStrategyVariant,
  updateStrategyRule,
} from '../adapters/strategy.adapter.js'
import { createInstrument } from '../adapters/instrument.adapter.js'
import { StrategyGrid } from './strategy'
import StrategyDetails from './strategy/StrategyDetails.jsx'
import StrategyFormModal from './strategy/modals/StrategyFormModal.jsx'
import { RuleDrawer } from './strategy/rules/RuleDrawer.jsx'
import InstrumentFormModal from './strategy/modals/InstrumentFormModal.jsx'
import VariantFormModal from './strategy/modals/VariantFormModal.jsx'
import ActionButton from './strategy/ui/ActionButton.jsx'
import { useChartState } from '../contexts/ChartStateContext.jsx'
import { createLogger } from '../utils/logger.js'
import { templateKey, cloneATMTemplateSafe } from '../utils/strategy/atmTemplate.js'
import useStrategyData from '../hooks/strategy/useStrategyData.js'
import useStrategySelection from '../hooks/strategy/useStrategySelection.js'
import useIndicatorCache from '../hooks/strategy/useIndicatorCache.js'
import useInstrumentMetadata from '../hooks/strategy/useInstrumentMetadata.js'
import useStrategyPreview from '../hooks/strategy/useStrategyPreview.js'
import { extractRuleFlow } from './strategy/rules/ruleUtils.js'
import { BotCreateModal } from '../features/bots/create/BotCreateModal.jsx'
import { useBotCreateController } from '../features/bots/create/useBotCreateController.js'

const StrategyTab = ({ chartId }) => {
  const { getChart, updateChart } = useChartState()
  const chartSnapshot = getChart(chartId)
  const logger = useMemo(() => createLogger('StrategyTab', { chartId }), [chartId])
  const { info, error } = logger

  const [errorMessage, setErrorMessage] = useState(null)
  const [botCreateOpen, setBotCreateOpen] = useState(false)
  const [botCreateError, setBotCreateError] = useState(null)
  const [botCreateNotice, setBotCreateNotice] = useState(null)
  const [strategyModal, setStrategyModal] = useState({ open: false, strategy: null })
  const [ruleModal, setRuleModal] = useState({ open: false, rule: null, mode: 'create' })
  const [variantModal, setVariantModal] = useState({ open: false, variant: null, mode: 'create' })
  const [savingStrategy, setSavingStrategy] = useState(false)
  const [savingRule, setSavingRule] = useState(false)
  const [savingVariant, setSavingVariant] = useState(false)
  const [instrumentModal, setInstrumentModal] = useState({ open: false, defaults: null })
  const [savingInstrument, setSavingInstrument] = useState(false)
  const [instrumentError, setInstrumentError] = useState(null)
  const [quickUpdateStatus, setQuickUpdateStatus] = useState({ saving: false, error: null, savedAt: null })
  const [strategyListCollapsed, setStrategyListCollapsed] = useState(false)
  const {
    strategies,
    indicators,
    setIndicators,
    upsertStrategy,
    removeStrategy,
    atmTemplates,
    loading,
    error: dataError,
    refreshStrategies,
    refreshStrategyDetail,
    refreshTemplates,
  } = useStrategyData({ logger })
  const { selectedId, setSelectedId, selectedStrategy } = useStrategySelection(strategies)
  const { indicatorLookup, ensureIndicatorDetails } = useIndicatorCache({
    indicators,
    setIndicators,
    logger,
  })
  const { instrumentRefreshStatus, refreshInstrumentMetadata } = useInstrumentMetadata({
    selectedStrategy,
    refreshStrategies,
    refreshStrategyDetail,
    logger,
  })
  const selectedStrategyInstruments = useMemo(
    () => (Array.isArray(selectedStrategy?.instruments) ? selectedStrategy.instruments : []),
    [selectedStrategy],
  )
  const selectedInstrumentIds = useMemo(
    () => selectedStrategyInstruments.map((instrument) => instrument?.id).filter(Boolean),
    [selectedStrategyInstruments],
  )
  const {
    previewLoading,
    previewResult,
    previewInstrumentId,
    setPreviewInstrumentId,
    previewWindow,
    setPreviewWindow,
    runPreview,
  } = useStrategyPreview({
    chartId,
    chartSnapshot,
    updateChart,
    selectedStrategy,
    selectedInstrumentIds,
    logger,
    onError: setErrorMessage,
  })
  const instrumentMap = useMemo(() => {
    const map = new Map()
    selectedStrategyInstruments.forEach((instrument) => {
      const symbolKey = String(instrument?.symbol || '').toUpperCase()
      if (symbolKey) {
        map.set(symbolKey, instrument)
      }
    })
    return map
  }, [selectedStrategyInstruments])
  const {
    form: botCreateForm,
    walletError: botWalletError,
    handleChange: handleBotCreateChange,
    handleBacktestRangeChange: handleBotBacktestRangeChange,
    handleStrategySelect: handleBotStrategySelect,
    handleVariantSelect: handleBotVariantSelect,
    handleWalletBalanceChange: handleBotWalletBalanceChange,
    handleWalletBalanceAdd: handleBotWalletBalanceAdd,
    handleWalletBalanceRemove: handleBotWalletBalanceRemove,
    prepareForCreate: prepareBotCreate,
    submit: submitBotCreate,
  } = useBotCreateController({
    strategies,
    fetchStrategyDetail: refreshStrategyDetail,
    logger,
    onCreated: (payload) => {
      const createdStrategyName =
        strategies.find((strategy) => strategy.id === payload?.strategy_id)?.name
        || selectedStrategy?.name
        || 'the selected strategy'
      setBotCreateOpen(false)
      setBotCreateError(null)
      setBotCreateNotice(
        payload?.name
          ? `Bot '${payload.name}' created from ${createdStrategyName}.`
          : 'Bot created.',
      )
    },
  })

  useEffect(() => {
    setQuickUpdateStatus({ saving: false, error: null, savedAt: null })
  }, [selectedStrategy?.id])

  useEffect(() => {
    setBotCreateNotice(null)
  }, [selectedStrategy?.id])

  useEffect(() => {
    if (!selectedId) {
      return
    }
    refreshStrategyDetail(selectedId).catch((err) => {
      error('strategy_detail_refresh_failed', err)
    })
  }, [selectedId, refreshStrategyDetail, error])

  const displayError = errorMessage || dataError

  const availableATMTemplates = useMemo(() => {
    const seen = new Set()
    const uniqueTemplates = []

    const pushTemplate = (id, label, template) => {
      const normalized = cloneATMTemplateSafe(template)
      const resolvedLabel = normalized.name?.trim() || label
      const key = templateKey(normalized)
      if (!key || seen.has(key)) return
      seen.add(key)
      uniqueTemplates.push({ id, label: resolvedLabel, template: normalized })
    }

    atmTemplates.forEach((template) => {
      if (!template?.template) return
      pushTemplate(template.id, template.name, template.template)
    })
    strategies.forEach((strategy, index) => {
      if (!strategy?.atm_template) return
      const label = strategy.name ? `${strategy.name} ATM` : `Strategy ATM ${index + 1}`
      pushTemplate(`strategy-${strategy.id || index}`, label, strategy.atm_template)
    })

    return uniqueTemplates
  }, [atmTemplates, strategies])

  const openInstrumentModal = useCallback(
    (defaults = {}) => {
      setInstrumentError(null)

      // If a symbol is provided, try to populate from existing instrument data
      let existingInstrument = null
      if (defaults.symbol) {
        const symbolKey = String(defaults.symbol).toUpperCase()
        existingInstrument = instrumentMap.get(symbolKey)
      }

      setInstrumentModal({
        open: true,
        defaults: {
          // Always include these from defaults or strategy
          symbol: defaults.symbol || '',
          datasource: defaults.datasource || selectedStrategy?.datasource || '',
          exchange: defaults.exchange || selectedStrategy?.exchange || '',

          // Auto-populate from existing instrument if available
          ...(existingInstrument && {
            instrument_type: existingInstrument.instrument_type || '',
            tick_size: existingInstrument.tick_size ?? '',
            tick_value: existingInstrument.tick_value ?? '',
            contract_size: existingInstrument.contract_size ?? '',
            min_order_size: existingInstrument.min_order_size ?? '',
            base_currency: existingInstrument.metadata?.instrument_fields?.base_currency || '',
            quote_currency: existingInstrument.quote_currency || '',
            maker_fee_rate: existingInstrument.maker_fee_rate ?? '',
            taker_fee_rate: existingInstrument.taker_fee_rate ?? '',
            can_short: existingInstrument.can_short ?? false,
            short_requires_borrow: existingInstrument.short_requires_borrow ?? false,
            has_funding: existingInstrument.has_funding ?? false,
            expiry_ts: existingInstrument.expiry_ts || '',
          }),

          // Allow manual defaults to override existing values
          ...defaults,
        },
      })
    },
    [selectedStrategy, instrumentMap],
  )

  const closeInstrumentModal = useCallback(() => {
    setInstrumentModal({ open: false, defaults: null })
  }, [])

  const attachedIndicators = useMemo(() => {
    if (!selectedStrategy) {
      return []
    }
    const entries = Array.isArray(selectedStrategy.indicators)
      ? selectedStrategy.indicators
      : []
    return entries.map((entry) => {
      const lookupMeta = indicatorLookup.get(entry.id) || {}
      const mergedMeta = {
        ...entry.snapshot,
        ...entry.meta,
        ...lookupMeta,
      }
      return {
        ...mergedMeta,
        id: entry.id,
        status: entry.status || 'active',
        snapshot: entry.snapshot || {},
        strategies: lookupMeta.strategies || entry.meta?.strategies || [],
      }
    })
  }, [selectedStrategy, indicatorLookup])

  const indicatorsForRuleModal = useMemo(() => {
    if (!ruleModal?.rule) {
      return attachedIndicators
    }
    const existing = new Map(attachedIndicators.map((indicator) => [indicator.id, indicator]))
    const { trigger, guards } = extractRuleFlow(ruleModal.rule)
    const refs = [trigger, ...(Array.isArray(guards) ? guards : [])]
    for (const ref of refs) {
      const indicatorId = ref?.indicator_id
      if (!indicatorId || existing.has(indicatorId)) continue
      const meta = indicatorLookup.get(indicatorId)
      if (meta) {
        existing.set(indicatorId, meta)
      }
    }
    return [...existing.values()]
  }, [attachedIndicators, ruleModal?.rule, indicatorLookup])

  const openCreateStrategy = () => {
    setErrorMessage(null) // Clear any previous errors
    setStrategyModal({ open: true, strategy: null })
  }
  const openEditStrategy = (strategy) => {
    setErrorMessage(null) // Clear any previous errors
    setStrategyModal({ open: true, strategy })
  }
  const closeStrategyModal = () => {
    setErrorMessage(null) // Clear errors when closing
    setStrategyModal({ open: false, strategy: null })
  }

  const openRuleModal = (rule = null, options = {}) => {
    const mode = options.mode || (rule ? 'edit' : 'create')
    setRuleModal({ open: true, rule, mode })
  }
  const closeRuleModal = () => setRuleModal({ open: false, rule: null, mode: 'create' })

  const openCreateVariant = useCallback(() => {
    setErrorMessage(null)
    setVariantModal({ open: true, variant: null, mode: 'create' })
  }, [])

  const openEditVariant = useCallback((variant) => {
    setErrorMessage(null)
    setVariantModal({ open: true, variant, mode: 'edit' })
  }, [])

  const closeVariantModal = useCallback(() => {
    setVariantModal({ open: false, variant: null, mode: 'create' })
  }, [])

  const handleStrategySubmit = async (payload, options = {}) => {
    const { closeOnSuccess = true } = options || {}
    setSavingStrategy(true)
    setErrorMessage(null)
    try {
      let saved
      if (strategyModal.strategy) {
        saved = await updateStrategy(strategyModal.strategy.id, payload)
        info('strategy_updated', { strategyId: strategyModal.strategy.id })
      } else {
        saved = await createStrategy(payload)
        info('strategy_created', { name: payload.name })
      }
      upsertStrategy(saved)
      if (!strategyModal.strategy && saved?.id) {
        setSelectedId(saved.id)
      }
      await refreshTemplates()
      if (closeOnSuccess) {
        closeStrategyModal()
      }
      return saved
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to save strategy')
      error('strategy_save_failed', err)
      throw err
    } finally {
      setSavingStrategy(false)
    }
  }

  const handleQuickUpdate = useCallback(
    async (patch) => {
      if (!selectedStrategy) return
      setQuickUpdateStatus({ saving: true, error: null, savedAt: null })
      try {
        const saved = await updateStrategy(selectedStrategy.id, patch)
        upsertStrategy(saved)
        setQuickUpdateStatus({ saving: false, error: null, savedAt: Date.now() })
        info('strategy_quick_updated', { strategyId: selectedStrategy.id, fields: Object.keys(patch || {}) })
      } catch (err) {
        setQuickUpdateStatus({ saving: false, error: err?.message || 'Quick update failed', savedAt: null })
        error('strategy_quick_update_failed', err)
      }
    },
    [selectedStrategy, upsertStrategy, info, error],
  )

  const handleInstrumentSubmit = async (payload) => {
    setSavingInstrument(true)
    setInstrumentError(null)
    try {
      await createInstrument(payload)
      info('instrument_saved', { symbol: payload.symbol })
      await refreshStrategies()
      if (selectedStrategy?.id) {
        await refreshStrategyDetail(selectedStrategy.id)
      }
      closeInstrumentModal()
    } catch (err) {
      setInstrumentError(err?.message || 'Failed to save instrument metadata')
      error('instrument_save_failed', err)
    } finally {
      setSavingInstrument(false)
    }
  }

  const handleDeleteStrategy = async (strategy) => {
    if (!strategy) return
    setErrorMessage(null)
    try {
      await deleteStrategy(strategy.id)
      info('strategy_deleted', { strategyId: strategy.id })
      removeStrategy(strategy.id)
      if (selectedId === strategy.id) {
        setSelectedId(null)
      }
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to delete strategy')
      error('strategy_delete_failed', err)
    }
  }

  const handleAttachIndicator = async (indicatorId) => {
    if (!selectedStrategy) return
    setErrorMessage(null)
    try {
      const saved = await attachStrategyIndicator(selectedStrategy.id, indicatorId)
      info('strategy_indicator_attached', { strategyId: selectedStrategy.id, indicatorId })
      upsertStrategy(saved)
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to attach indicator')
      error('strategy_indicator_attach_failed', err)
    }
  }

  const handleDetachIndicator = async (indicatorId) => {
    if (!selectedStrategy) return
    setErrorMessage(null)
    try {
      const saved = await detachStrategyIndicator(selectedStrategy.id, indicatorId)
      info('strategy_indicator_detached', { strategyId: selectedStrategy.id, indicatorId })
      upsertStrategy(saved)
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to detach indicator')
      error('strategy_indicator_detach_failed', err)
    }
  }

  const handleRuleSubmit = async (payload) => {
    if (!selectedStrategy) return
    setSavingRule(true)
    setErrorMessage(null)
    try {
      let saved
      if (ruleModal.mode === 'edit' && ruleModal.rule?.id) {
        saved = await updateStrategyRule(selectedStrategy.id, ruleModal.rule.id, payload)
        info('strategy_rule_updated', { strategyId: selectedStrategy.id, ruleId: ruleModal.rule.id })
      } else {
        saved = await createStrategyRule(selectedStrategy.id, payload)
        info('strategy_rule_created', { strategyId: selectedStrategy.id })
      }
      upsertStrategy(saved)
      closeRuleModal()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to save rule')
      error('strategy_rule_save_failed', err)
    } finally {
      setSavingRule(false)
    }
  }

  const handleDeleteRule = async (rule) => {
    if (!selectedStrategy) return
    setErrorMessage(null)
    try {
      const saved = await deleteStrategyRule(selectedStrategy.id, rule.id)
      info('strategy_rule_deleted', { strategyId: selectedStrategy.id, ruleId: rule.id })
      upsertStrategy(saved)
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to delete rule')
      error('strategy_rule_delete_failed', err)
    }
  }

  const handleVariantSubmit = async (payload) => {
    if (!selectedStrategy) return
    setSavingVariant(true)
    setErrorMessage(null)
    try {
      if (variantModal.mode === 'edit' && variantModal.variant?.id) {
        await updateStrategyVariant(selectedStrategy.id, variantModal.variant.id, payload)
        info('strategy_variant_updated', { strategyId: selectedStrategy.id, variantId: variantModal.variant.id })
      } else {
        await createStrategyVariant(selectedStrategy.id, payload)
        info('strategy_variant_created', { strategyId: selectedStrategy.id, name: payload?.name })
      }
      await refreshStrategyDetail(selectedStrategy.id)
      closeVariantModal()
    } catch (err) {
      setErrorMessage(err?.message || 'Failed to save variant')
      error('strategy_variant_save_failed', err)
    } finally {
      setSavingVariant(false)
    }
  }

  const handleDeleteVariant = useCallback(
    async (variant) => {
      if (!selectedStrategy || !variant?.id || variant?.is_default) return
      setErrorMessage(null)
      try {
        await deleteStrategyVariant(selectedStrategy.id, variant.id)
        info('strategy_variant_deleted', { strategyId: selectedStrategy.id, variantId: variant.id })
        await refreshStrategyDetail(selectedStrategy.id)
      } catch (err) {
        setErrorMessage(err?.message || 'Failed to delete variant')
        error('strategy_variant_delete_failed', err)
      }
    },
    [selectedStrategy, refreshStrategyDetail, info, error],
  )

  const handleOpenBotCreate = useCallback(() => {
    if (!selectedStrategy?.id) return
    setBotCreateNotice(null)
    setBotCreateError(null)
    prepareBotCreate({
      strategyId: selectedStrategy.id,
      runType: 'backtest',
    }).then(() => {
      setBotCreateOpen(true)
    }).catch((err) => {
      error('strategy_bot_create_prepare_failed', err)
      setBotCreateError(err?.message || 'Unable to prepare bot create form')
    })
  }, [prepareBotCreate, selectedStrategy, error])

  const handleBotCreateSubmit = useCallback(
    async (event) => {
      setBotCreateError(null)
      try {
        await submitBotCreate(event)
      } catch (err) {
        error('strategy_page_bot_create_failed', err)
        setBotCreateError(err?.message || 'Unable to create bot')
      }
    },
    [submitBotCreate, error],
  )

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-[10px] uppercase tracking-[0.3em] text-slate-500">Strategy Studio</p>
          <h2 className="text-lg font-semibold text-white">Design, test, and wire strategies</h2>
          <p className="mt-0.5 text-xs text-slate-500">
            Build rules, validate signals, and prepare bots from a single workspace.
          </p>
        </div>
        <ActionButton variant="ghost" onClick={openCreateStrategy}>
          <svg className="mr-1 h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          New Strategy
        </ActionButton>
      </div>

      {/* Error Message */}
      {displayError && (
        <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2">
          <p className="text-xs text-rose-200">{displayError}</p>
        </div>
      )}

      {botCreateNotice ? (
        <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/10 px-3 py-2">
          <p className="text-xs text-emerald-200">{botCreateNotice}</p>
        </div>
      ) : null}

      <div className={`grid gap-5 ${strategyListCollapsed ? 'lg:grid-cols-[56px_minmax(0,1fr)]' : 'lg:grid-cols-[320px_minmax(0,1fr)]'}`}>
        <div className="space-y-3">
          <div className={`rounded-lg border border-white/[0.06] bg-black/40 ${strategyListCollapsed ? 'px-2 py-2' : 'px-3 py-2'}`}>
            <div className={`flex items-center ${strategyListCollapsed ? 'flex-col gap-2' : 'justify-between'}`}>
              {strategyListCollapsed ? (
                <span className="rounded bg-white/[0.06] px-1.5 py-0.5 text-[10px] text-slate-500">{strategies.length}</span>
              ) : (
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-slate-300">Strategies</span>
                  <span className="rounded bg-white/[0.06] px-1.5 py-0.5 text-[10px] text-slate-500">{strategies.length}</span>
                </div>
              )}
              <div className={`flex items-center ${strategyListCollapsed ? 'flex-col gap-1.5' : 'gap-1.5'}`}>
                <button
                  onClick={() => setStrategyListCollapsed((prev) => !prev)}
                  className="rounded p-1 text-slate-500 transition hover:bg-white/5 hover:text-slate-300"
                  title={strategyListCollapsed ? 'Expand strategy list' : 'Collapse strategy list'}
                >
                  {strategyListCollapsed ? (
                    <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                  ) : (
                    <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                    </svg>
                  )}
                </button>
                <button
                  onClick={openCreateStrategy}
                  className="rounded p-1 text-slate-500 transition hover:bg-white/5 hover:text-slate-300"
                  title="Add new strategy"
                >
                  <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                  </svg>
                </button>
              </div>
            </div>
          </div>

          {!strategyListCollapsed && (
            loading ? (
              <div className="flex items-center justify-center rounded-xl border border-dashed border-white/[0.06] bg-black/20 p-10">
                <div className="text-center">
                  <div className="mx-auto h-6 w-6 animate-spin rounded-full border-2 border-white/10 border-t-white/60"></div>
                  <p className="mt-3 text-xs text-slate-500">Loading strategies…</p>
                </div>
              </div>
            ) : (
              <StrategyGrid
                strategies={strategies}
                selectedId={selectedId}
                onSelect={setSelectedId}
                onEdit={openEditStrategy}
                onDelete={handleDeleteStrategy}
                layout="stacked"
              />
            )
          )}
        </div>

        <div className="space-y-3">
          {selectedStrategy ? (
            <StrategyDetails
              strategy={selectedStrategy}
              attachedIndicators={attachedIndicators}
              availableIndicators={indicators}
              indicatorLookup={indicatorLookup}
              onEdit={() => openEditStrategy(selectedStrategy)}
              onDelete={() => handleDeleteStrategy(selectedStrategy)}
              onAttachIndicator={handleAttachIndicator}
              onDetachIndicator={handleDetachIndicator}
              onAddRule={() => openRuleModal(null)}
              onEditRule={(rule) => openRuleModal(rule)}
              onDuplicateRule={(rule) => openRuleModal({ ...rule, name: `${rule?.name || 'Rule'} copy` }, { mode: 'create' })}
              onDeleteRule={handleDeleteRule}
              onAddVariant={openCreateVariant}
              onEditVariant={openEditVariant}
              onDeleteVariant={handleDeleteVariant}
              onRunPreview={runPreview}
              previewWindow={previewWindow}
              setPreviewWindow={setPreviewWindow}
              previewResult={previewResult}
              previewLoading={previewLoading}
              previewInstrumentId={previewInstrumentId}
              setPreviewInstrumentId={setPreviewInstrumentId}
              onAddInstrument={(defaults) => openInstrumentModal(defaults)}
              onRefreshInstrumentMetadata={refreshInstrumentMetadata}
              instrumentRefreshStatus={instrumentRefreshStatus}
              atmTemplates={atmTemplates}
              onQuickUpdate={handleQuickUpdate}
              quickUpdateStatus={quickUpdateStatus}
              onLaunchBot={handleOpenBotCreate}
            />
          ) : (
            <div className="flex items-center justify-center rounded-xl border border-dashed border-white/[0.06] bg-black/30 p-12 text-center">
              <div>
                <svg className="mx-auto h-10 w-10 text-slate-700" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                </svg>
                <p className="mt-3 text-xs text-slate-500">Select a strategy to view details</p>
              </div>
            </div>
          )}
        </div>
      </div>

      <StrategyFormModal
        open={strategyModal.open}
        initialValues={strategyModal.strategy}
        onSubmit={handleStrategySubmit}
        onCancel={closeStrategyModal}
        submitting={savingStrategy}
        availableATMTemplates={availableATMTemplates}
        error={errorMessage}
      />

      <RuleDrawer
        open={ruleModal.open}
        initialValues={ruleModal.rule}
        indicators={indicatorsForRuleModal}
        ensureIndicatorMeta={ensureIndicatorDetails}
        onSubmit={handleRuleSubmit}
        onCancel={closeRuleModal}
        submitting={savingRule}
      />

      <InstrumentFormModal
        open={instrumentModal.open}
        initialValues={instrumentModal.defaults}
        onSubmit={handleInstrumentSubmit}
        onCancel={closeInstrumentModal}
        submitting={savingInstrument}
        error={instrumentError}
      />

      <VariantFormModal
        open={variantModal.open}
        initialValues={variantModal.variant}
        onSubmit={handleVariantSubmit}
        onCancel={closeVariantModal}
        submitting={savingVariant}
        error={errorMessage}
        availableATMTemplates={availableATMTemplates}
      />

      <BotCreateModal
        open={botCreateOpen}
        onClose={() => {
          setBotCreateOpen(false)
          setBotCreateError(null)
        }}
        form={botCreateForm}
        strategies={strategies}
        strategiesLoading={loading}
        strategyError={dataError}
        walletError={botWalletError}
        onSubmit={handleBotCreateSubmit}
        onChange={handleBotCreateChange}
        onBacktestRangeChange={handleBotBacktestRangeChange}
        onStrategySelect={handleBotStrategySelect}
        onVariantSelect={handleBotVariantSelect}
        onWalletBalanceChange={handleBotWalletBalanceChange}
        onWalletBalanceAdd={handleBotWalletBalanceAdd}
        onWalletBalanceRemove={handleBotWalletBalanceRemove}
        error={botCreateError}
      />
    </div>
  )
}

export default StrategyTab
