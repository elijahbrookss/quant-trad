import { Fragment, useCallback, useEffect, useMemo, useState } from 'react'
import { Popover, PopoverButton, PopoverPanel, Transition } from '@headlessui/react'
import { MoreVertical, Pencil, Trash2 } from 'lucide-react'

import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols.js'
import { DateRangePickerComponent } from '../ChartComponent/DateTimePickerComponent.jsx'
import DropdownSelect from '../ChartComponent/DropdownSelect.jsx'
import { InstrumentsTab, ATMTab, RulesTab, OrderTriggersTab } from './index.js'
import ActionButton from './ui/ActionButton.jsx'
import TabButton from './ui/TabButton.jsx'
import TabPanel from './ui/TabPanel.jsx'

const EMPTY_LIST = Object.freeze([])

/**
 * Get display name for a symbol - prefers base_currency from instrument metadata
 */
const getSymbolDisplay = (symbol, instrumentMap) => {
  if (!symbol) return ''
  const str = String(symbol).toUpperCase()

  // Try to get base_currency from instrument metadata
  const instrument = instrumentMap?.get(str)
  const baseCurrency = instrument?.metadata?.instrument_fields?.base_currency || instrument?.base_currency
  if (baseCurrency) return baseCurrency

  // Fallback: for pairs like BTCUSDT, show as-is if short enough
  if (str.length <= 8) return str
  // Truncate long symbols
  return str.slice(0, 6) + '…'
}

const StrategyDetails = ({
  strategy,
  attachedIndicators,
  availableIndicators,
  indicatorLookup,
  onEdit,
  onDelete,
  onAttachIndicator,
  onDetachIndicator,
  onAddRule,
  onEditRule,
  onDuplicateRule,
  onDeleteRule,
  onRunPreview,
  previewWindow,
  setPreviewWindow,
  previewResult,
  previewLoading,
  previewInstrumentId,
  setPreviewInstrumentId,
  onAddInstrument = () => {},
  onRefreshInstrumentMetadata,
  instrumentRefreshStatus,
  atmTemplates,
  onQuickUpdate,
  quickUpdateStatus,
}) => {
  const hasStrategy = Boolean(strategy)
  const strategyInstruments = Array.isArray(strategy?.instruments) ? strategy.instruments : EMPTY_LIST
  const strategyInstrumentMessages = Array.isArray(strategy?.instrument_messages)
    ? strategy.instrument_messages
    : EMPTY_LIST
  const strategyDatasource = strategy?.datasource || ''
  const strategyExchange = strategy?.exchange || ''
  const strategySymbols = symbolsFromInstrumentSlots(strategy?.instrument_slots)

  const handleDateRangeChange = (range) => {
    setPreviewWindow((prev) => ({ ...prev, dateRange: range }))
  }

  const instrumentMap = useMemo(() => {
    const map = new Map()
    for (const entry of strategyInstruments) {
      const key = (entry.symbol || '').toUpperCase()
      if (key) {
        map.set(key, entry)
      }
    }
    return map
  }, [strategyInstruments])

  const instrumentMessages = strategyInstrumentMessages

  const ruleCount = Array.isArray(strategy?.rules) ? strategy.rules.length : 0
  const indicatorCount = Array.isArray(attachedIndicators) ? attachedIndicators.length : 0
  const atmTemplate = strategy?.atm_template || {}
  const atmTargets = Array.isArray(atmTemplate.take_profit_orders) ? atmTemplate.take_profit_orders : []

  const [quickBaseRisk, setQuickBaseRisk] = useState('')
  const [quickTemplateId, setQuickTemplateId] = useState('')
  const [quickSymbol, setQuickSymbol] = useState('')
  const [quickError, setQuickError] = useState(null)

  useEffect(() => {
    const currentRisk = strategy?.base_risk_per_trade
    setQuickBaseRisk(currentRisk === null || currentRisk === undefined ? '' : String(currentRisk))
    setQuickTemplateId(strategy?.atm_template_id || '')
    setQuickSymbol('')
    setQuickError(null)
  }, [strategy?.id, strategy?.base_risk_per_trade, strategy?.atm_template_id])

  const atmTemplateOptions = useMemo(() => {
    const options = []
    const seen = new Set()
    if (strategy?.atm_template_id) {
      const label = strategy?.atm_template?.name?.trim() || 'Current ATM template'
      options.push({ value: strategy.atm_template_id, label })
      seen.add(strategy.atm_template_id)
    }
    ;(atmTemplates || []).forEach((template) => {
      if (!template?.id || seen.has(template.id)) return
      options.push({ value: template.id, label: template.name || template.id })
      seen.add(template.id)
    })
    return options
  }, [atmTemplates, strategy?.atm_template, strategy?.atm_template_id])

  const normalizeSymbol = useCallback((value) => {
    if (!value) return ''
    return String(value).trim().toUpperCase().replace(/\s+/g, '')
  }, [])

  const handleQuickBaseRiskSave = useCallback(async () => {
    if (!onQuickUpdate) return
    const normalized = quickBaseRisk === '' ? null : Number(quickBaseRisk)
    if (quickBaseRisk !== '' && !Number.isFinite(normalized)) {
      setQuickError('Base risk must be a number.')
      return
    }
    const current = strategy?.base_risk_per_trade ?? null
    if ((current === null && normalized === null) || Number(current) === Number(normalized)) {
      setQuickError(null)
      return
    }
    setQuickError(null)
    await onQuickUpdate({ base_risk_per_trade: normalized })
  }, [onQuickUpdate, quickBaseRisk, strategy?.base_risk_per_trade])

  const handleTemplateChange = useCallback(
    async (event) => {
      if (!onQuickUpdate) return
      const next = event.target.value || null
      if ((strategy?.atm_template_id || null) === (next || null)) {
        return
      }
      setQuickTemplateId(next || '')
      setQuickError(null)
      await onQuickUpdate({ atm_template_id: next || null })
    },
    [onQuickUpdate, strategy?.atm_template_id],
  )

  const buildSlotPayload = useCallback((slots) => {
    return (slots || [])
      .map((slot) => ({
        symbol: normalizeSymbol(slot.symbol),
        enabled: slot.enabled !== false,
        ...(slot.risk_multiplier !== null && slot.risk_multiplier !== undefined
          ? { risk_multiplier: slot.risk_multiplier }
          : {}),
      }))
      .filter((slot) => slot.symbol)
  }, [normalizeSymbol])

  const handleQuickAddSymbol = useCallback(async () => {
    if (!onQuickUpdate) return
    const normalized = normalizeSymbol(quickSymbol)
    if (!normalized) {
      setQuickError('Enter a symbol to add.')
      return
    }
    const currentSlots = buildSlotPayload(strategy?.instrument_slots)
    if (currentSlots.some((slot) => slot.symbol === normalized)) {
      setQuickError('Symbol already added.')
      return
    }
    setQuickError(null)
    await onQuickUpdate({
      instrument_slots: [...currentSlots, { symbol: normalized, enabled: true }],
    })
    setQuickSymbol('')
  }, [onQuickUpdate, normalizeSymbol, quickSymbol, strategy?.instrument_slots, buildSlotPayload])

  const handleQuickRemoveSymbol = useCallback(
    async (symbol) => {
      if (!onQuickUpdate) return
      const normalized = normalizeSymbol(symbol)
      const currentSlots = buildSlotPayload(strategy?.instrument_slots)
      const nextSlots = currentSlots.filter((slot) => slot.symbol !== normalized)
      if (!nextSlots.length) {
        setQuickError('At least one instrument is required.')
        return
      }
      setQuickError(null)
      const nextRiskOverrides = { ...(strategy?.risk_overrides || {}) }
      delete nextRiskOverrides[normalized]
      await onQuickUpdate({
        instrument_slots: nextSlots,
        risk_overrides: nextRiskOverrides,
      })
    },
    [onQuickUpdate, normalizeSymbol, strategy?.instrument_slots, strategy?.risk_overrides, buildSlotPayload],
  )

  const handleQuickSymbolKey = useCallback(
    async (event) => {
      if (event.key !== 'Enter') return
      event.preventDefault()
      await handleQuickAddSymbol()
    },
    [handleQuickAddSymbol],
  )

  const [activeTab, setActiveTab] = useState('instruments')

  useEffect(() => {
    // Reset to instruments tab when strategy changes, or if there are instrument messages
    if (instrumentMessages.length > 0) {
      setActiveTab('instruments')
    } else {
      setActiveTab('instruments')
    }
  }, [strategy?.id, instrumentMessages.length])

  const handleAddInstrument = useCallback(
    (symbol) => {
      if (!symbol) return
      onAddInstrument({
        symbol,
        datasource: strategyDatasource,
        exchange: strategyExchange,
      })
    },
    [onAddInstrument, strategyDatasource, strategyExchange],
  )

  const handleSubmit = async (event) => {
    event.preventDefault()
    await onRunPreview(previewWindow)
  }

  if (!hasStrategy) {
    return (
      <div className="rounded-2xl border border-dashed border-white/10 bg-black/20 p-6 text-center text-sm text-slate-400">
        Select a strategy to manage indicators, rules, and signal evaluations.
      </div>
    )
  }

  // Computed values for header display
  const atmTemplateName = strategy.atm_template?.name?.trim() || 'Default ATM'

  return (
    <div className="space-y-4">
      {/* Compact Header */}
      <div className="rounded-xl border border-white/[0.08] bg-black/30">
        {/* Main header row */}
        <div className="flex items-center justify-between gap-4 px-5 py-4">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-3">
              <h2 className="truncate text-lg font-semibold text-white">{strategy.name}</h2>
              <span className="shrink-0 rounded bg-white/[0.06] px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-slate-400">
                {strategy.exchange || strategy.datasource || 'Exchange'}
              </span>
              {strategy.timeframe && (
                <span className="shrink-0 text-xs text-slate-500">{strategy.timeframe}</span>
              )}
            </div>
            {/* Inline stats */}
            <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-400">
              <span>{ruleCount} rule{ruleCount === 1 ? '' : 's'}</span>
              <span className="text-slate-600">•</span>
              <span>{indicatorCount} indicator{indicatorCount === 1 ? '' : 's'}</span>
              <span className="text-slate-600">•</span>
              <span>{atmTargets.length} TP target{atmTargets.length === 1 ? '' : 's'}</span>
            </div>
          </div>

          {/* Actions: Edit button + kebab menu */}
          <div className="flex items-center gap-2">
            <ActionButton variant="ghost" onClick={onEdit}>
              Edit
            </ActionButton>
            <Popover className="relative">
              {({ close }) => (
                <>
                  <PopoverButton
                    className="flex h-8 w-8 items-center justify-center rounded-md border border-white/10 text-slate-400 transition hover:bg-white/5 hover:text-white focus:outline-none"
                    title="More actions"
                  >
                    <MoreVertical className="h-4 w-4" />
                  </PopoverButton>
                  <Transition
                    as={Fragment}
                    enter="transition ease-out duration-100"
                    enterFrom="opacity-0 scale-95"
                    enterTo="opacity-100 scale-100"
                    leave="transition ease-in duration-75"
                    leaveFrom="opacity-100 scale-100"
                    leaveTo="opacity-0 scale-95"
                  >
                    <PopoverPanel className="absolute right-0 top-full z-50 mt-1 w-44 origin-top-right rounded-lg border border-white/10 bg-[#131a2b] p-1.5 shadow-xl">
                      <button
                        type="button"
                        onClick={() => {
                          onEdit?.()
                          close()
                        }}
                        className="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/5"
                      >
                        <Pencil className="h-3.5 w-3.5" />
                        Edit strategy
                      </button>
                      <div className="my-1 h-px bg-white/10" />
                      <button
                        type="button"
                        onClick={() => {
                          onDelete?.()
                          close()
                        }}
                        className="flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-left text-sm text-rose-300 transition hover:bg-rose-500/10"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                        Delete strategy
                      </button>
                    </PopoverPanel>
                  </Transition>
                </>
              )}
            </Popover>
          </div>
        </div>

        {/* Configuration row */}
        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 border-t border-white/[0.06] px-5 py-3 text-sm">
          {/* Base Risk */}
          <div className="flex items-center gap-2">
            <span className="text-slate-500">Risk:</span>
            <input
              type="number"
              min="0"
              step="0.01"
              value={quickBaseRisk}
              onChange={(e) => setQuickBaseRisk(e.target.value)}
              onBlur={handleQuickBaseRiskSave}
              onKeyDown={(e) => e.key === 'Enter' && handleQuickBaseRiskSave()}
              className="w-16 rounded bg-white/5 px-2 py-1 text-white transition hover:bg-white/10 focus:bg-white/10 focus:outline-none focus:ring-1 focus:ring-[color:var(--accent-alpha-40)]"
              placeholder="—"
            />
            <span className="text-slate-500">USD</span>
          </div>

          {/* Save status */}
          <div className="ml-auto text-xs">
            {quickUpdateStatus?.saving ? (
              <span className="text-slate-400">Saving...</span>
            ) : quickUpdateStatus?.error ? (
              <span className="text-rose-400">{quickUpdateStatus.error}</span>
            ) : quickUpdateStatus?.savedAt ? (
              <span className="text-emerald-400">Saved</span>
            ) : null}
          </div>
        </div>

        {/* Symbols row */}
        <div className="flex flex-wrap items-center gap-1.5 border-t border-white/[0.06] px-5 py-3">
          {(strategy?.instrument_slots || []).map((slot) => (
            <span
              key={`header-slot-${slot.symbol}`}
              className="group inline-flex items-center gap-1.5 rounded-md bg-white/[0.05] px-2.5 py-1 text-xs font-medium text-slate-300"
              title={slot.symbol}
            >
              {getSymbolDisplay(slot.symbol, instrumentMap)}
              <button
                type="button"
                onClick={() => handleQuickRemoveSymbol(slot.symbol)}
                className="text-slate-500 opacity-0 transition hover:text-rose-400 group-hover:opacity-100"
              >
                ×
              </button>
            </span>
          ))}
          <input
            type="text"
            placeholder="+ Add symbol"
            value={quickSymbol}
            onChange={(e) => setQuickSymbol(e.target.value)}
            onKeyDown={handleQuickSymbolKey}
            className="w-24 rounded-md bg-transparent px-2 py-1 text-xs text-slate-400 placeholder-slate-600 transition hover:bg-white/5 focus:w-28 focus:bg-white/5 focus:text-white focus:outline-none"
          />
          {quickError && <span className="text-xs text-rose-400">{quickError}</span>}
        </div>
      </div>

      {/* Missing indicators warning */}
      {Array.isArray(strategy.missing_indicators) && strategy.missing_indicators.length > 0 && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
          <div className="flex items-center gap-2">
            <svg className="h-4 w-4 flex-shrink-0 text-amber-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            <span className="font-medium text-amber-200">{strategy.missing_indicators.length} missing indicator(s)</span>
            <span className="text-amber-100/70">— detach or recreate to restore evaluations</span>
          </div>
        </div>
      )}

      {/* Tabs - consolidated (no Overview tab) */}
      <div className="rounded-xl border border-white/[0.08] bg-black/40">
        <div className="flex gap-1 border-b border-white/[0.06] px-1">
          <TabButton
            active={activeTab === 'instruments'}
            onClick={() => setActiveTab('instruments')}
          >
            Instruments
            {instrumentMessages.length > 0 && (
              <span className="ml-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-amber-500/20 text-[10px] text-amber-300">
                {instrumentMessages.length}
              </span>
            )}
          </TabButton>
          <TabButton active={activeTab === 'logic'} onClick={() => setActiveTab('logic')}>
            Decision Logic
          </TabButton>
          <TabButton active={activeTab === 'atm'} onClick={() => setActiveTab('atm')}>
            Risk & Execution
          </TabButton>
          <TabButton active={activeTab === 'preview'} onClick={() => setActiveTab('preview')}>
            Order Triggers
          </TabButton>
        </div>

        <TabPanel active={activeTab === 'instruments'}>
          <p className="px-6 pb-2 text-xs text-slate-400">Contracts used for sizing, fees, and execution.</p>
          <InstrumentsTab
            strategy={strategy}
            instrumentMap={instrumentMap}
            instrumentMessages={instrumentMessages}
            onAddInstrument={handleAddInstrument}
            onRefreshMetadata={onRefreshInstrumentMetadata}
            refreshStatus={instrumentRefreshStatus}
            ActionButton={ActionButton}
          />
        </TabPanel>

        <TabPanel active={activeTab === 'atm'}>
          <ATMTab
            template={strategy.atm_template}
            templateOptions={atmTemplateOptions}
            currentTemplateId={strategy.atm_template_id}
            onTemplateChange={handleTemplateChange}
          />
        </TabPanel>

        <TabPanel active={activeTab === 'logic'}>
          <p className="px-6 pb-2 text-xs text-slate-400">Attach indicators, inspect typed outputs, and compose trigger-to-action rule flows.</p>
          <RulesTab
            strategy={strategy}
            attachedIndicators={attachedIndicators}
            availableIndicators={availableIndicators}
            onAttachIndicator={onAttachIndicator}
            onDetachIndicator={onDetachIndicator}
            onAddRule={onAddRule}
            onEditRule={onEditRule}
            onDuplicateRule={onDuplicateRule}
            onDeleteRule={onDeleteRule}
            indicatorLookup={indicatorLookup}
            DropdownSelect={DropdownSelect}
            ActionButton={ActionButton}
          />
        </TabPanel>

        <TabPanel active={activeTab === 'preview'}>
          <p className="px-6 pb-2 text-xs text-slate-400">Preview when this strategy would attempt orders.</p>
          <OrderTriggersTab
            strategy={strategy}
            instruments={strategyInstruments}
            previewWindow={previewWindow}
            previewLoading={previewLoading}
            previewResult={previewResult}
            previewInstrumentId={previewInstrumentId}
            onInstrumentChange={setPreviewInstrumentId}
            onSubmit={handleSubmit}
            onDateRangeChange={handleDateRangeChange}
            DateRangePickerComponent={DateRangePickerComponent}
            onNavigateToRules={() => setActiveTab('logic')}
            onNavigateToExecution={() => setActiveTab('atm')}
          />
        </TabPanel>
      </div>
    </div>
  )
}


export default StrategyDetails
