import { useCallback, useEffect, useMemo, useState } from 'react'

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
  onDeleteRule,
  onCreateGlobalFilter,
  onUpdateGlobalFilter,
  onDeleteGlobalFilter,
  onCreateRuleFilter,
  onUpdateRuleFilter,
  onDeleteRuleFilter,
  onRunSignals,
  signalWindow,
  setSignalWindow,
  signalResult,
  signalsLoading,
  signalInstrumentId,
  setSignalInstrumentId,
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
    setSignalWindow((prev) => ({ ...prev, dateRange: range }))
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
    await onRunSignals(signalWindow)
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
      {/* Consolidated Header Card */}
      <div className="rounded-xl border border-white/[0.08] bg-black/40">
        {/* Top row: Name, exchange badge, actions */}
        <div className="flex items-center justify-between gap-3 border-b border-white/[0.06] px-4 py-3">
          <div className="flex items-center gap-3">
            <h2 className="text-base font-semibold text-white">{strategy.name}</h2>
            <span className="rounded bg-white/[0.06] px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-slate-400">
              {strategy.exchange || strategy.datasource || 'Exchange'}
            </span>
            <span className="text-xs text-slate-500">{strategy.timeframe}</span>
          </div>
          <div className="flex items-center gap-2">
            <ActionButton variant="ghost" onClick={onEdit}>
              Edit
            </ActionButton>
            <ActionButton variant="danger" onClick={onDelete}>
              Delete
            </ActionButton>
          </div>
        </div>

        {/* Statistics row - clickable cards that navigate to tabs */}
        <div className="grid grid-cols-3 gap-px bg-white/[0.04]">
          <button
            onClick={() => setActiveTab('logic')}
            className={`group flex flex-col items-center justify-center bg-black/40 px-4 py-3 transition hover:bg-white/[0.03] ${activeTab === 'logic' ? 'bg-white/[0.02]' : ''}`}
          >
            <div className="flex items-center gap-2">
              <svg className="h-4 w-4 text-slate-500 transition group-hover:text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
              </svg>
              <span className="text-xl font-semibold text-white">{ruleCount}</span>
            </div>
            <span className="mt-0.5 text-[10px] uppercase tracking-wider text-slate-500">Decision Logic</span>
          </button>
          <button
            onClick={() => setActiveTab('logic')}
            className={`group flex flex-col items-center justify-center bg-black/40 px-4 py-3 transition hover:bg-white/[0.03] ${activeTab === 'logic' ? 'bg-white/[0.02]' : ''}`}
          >
            <div className="flex items-center gap-2">
              <svg className="h-4 w-4 text-slate-500 transition group-hover:text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
              </svg>
              <span className="text-xl font-semibold text-white">{indicatorCount}</span>
            </div>
            <span className="mt-0.5 text-[10px] uppercase tracking-wider text-slate-500">Indicators</span>
          </button>
          <button
            onClick={() => setActiveTab('atm')}
            className={`group flex flex-col items-center justify-center bg-black/40 px-4 py-3 transition hover:bg-white/[0.03] ${activeTab === 'atm' ? 'bg-white/[0.02]' : ''}`}
          >
            <div className="flex items-center gap-2">
              <svg className="h-4 w-4 text-slate-500 transition group-hover:text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
              </svg>
              <span className="text-xl font-semibold text-white">{atmTargets.length}</span>
            </div>
            <span className="mt-0.5 text-[10px] uppercase tracking-wider text-slate-500">TP Targets</span>
          </button>
        </div>

        {/* Inline summary row - replaces Quick Edits */}
        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 border-t border-white/[0.06] px-4 py-2.5 text-xs">
          {/* Base Risk - editable on click */}
          <div className="group flex items-center gap-2">
            <span className="text-slate-500">Risk:</span>
            <div className="flex items-center gap-1">
              <input
                type="number"
                min="0"
                step="0.01"
                value={quickBaseRisk}
                onChange={(e) => setQuickBaseRisk(e.target.value)}
                onBlur={handleQuickBaseRiskSave}
                onKeyDown={(e) => e.key === 'Enter' && handleQuickBaseRiskSave()}
                className="w-16 rounded bg-transparent px-1 py-0.5 text-white transition hover:bg-white/5 focus:bg-white/10 focus:outline-none"
                placeholder="—"
              />
              <span className="text-slate-600">USD</span>
            </div>
          </div>

          <div className="h-3 w-px bg-white/10" />

          {/* ATM Template - dropdown */}
          <div className="flex items-center gap-2">
            <span className="text-slate-500">ATM:</span>
            <select
              value={quickTemplateId}
              onChange={handleTemplateChange}
              className="rounded bg-transparent px-1 py-0.5 text-white transition hover:bg-white/5 focus:bg-white/10 focus:outline-none"
            >
              <option value="" className="bg-slate-900">{atmTemplateName}</option>
              {atmTemplateOptions.filter(opt => opt.value !== strategy?.atm_template_id).map((opt) => (
                <option key={opt.value} value={opt.value} className="bg-slate-900">{opt.label}</option>
              ))}
            </select>
          </div>

          <div className="h-3 w-px bg-white/10" />

          {/* Symbols count with popover-style interaction */}
          <div className="flex items-center gap-2">
            <span className="text-slate-500">Symbols:</span>
            <span className="text-white">{strategySymbols.length}</span>
          </div>

          {/* Save status indicator */}
          <div className="ml-auto text-[10px]">
            {quickUpdateStatus?.saving ? (
              <span className="text-slate-400">Saving...</span>
            ) : quickUpdateStatus?.error ? (
              <span className="text-rose-400">{quickUpdateStatus.error}</span>
            ) : quickUpdateStatus?.savedAt ? (
              <span className="text-emerald-400">Saved</span>
            ) : null}
          </div>
        </div>

        {/* Symbols chips - compact display, uses base_currency when available */}
        <div className="flex flex-wrap items-center gap-1.5 border-t border-white/[0.06] px-4 py-2">
          {(strategy?.instrument_slots || []).map((slot) => (
            <span
              key={`header-slot-${slot.symbol}`}
              className="group inline-flex items-center gap-1 rounded bg-white/[0.04] px-2 py-0.5 text-[11px] text-slate-300"
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
          <div className="flex items-center gap-1">
            <input
              type="text"
              placeholder="+ Add"
              value={quickSymbol}
              onChange={(e) => setQuickSymbol(e.target.value)}
              onKeyDown={handleQuickSymbolKey}
              className="w-14 rounded bg-transparent px-1 py-0.5 text-[11px] text-slate-400 placeholder-slate-600 transition hover:bg-white/5 focus:w-20 focus:bg-white/10 focus:text-white focus:outline-none"
            />
          </div>
          {quickError && <span className="text-[10px] text-rose-400">{quickError}</span>}
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
          <TabButton active={activeTab === 'signals'} onClick={() => setActiveTab('signals')}>
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
          <p className="px-6 pb-2 text-xs text-slate-400">Define sizing, risk limits, and order management.</p>
          <ATMTab template={strategy.atm_template} />
        </TabPanel>

        <TabPanel active={activeTab === 'logic'}>
          <p className="px-6 pb-2 text-xs text-slate-400">Attach signal sources, define rule logic, and gate triggers with filters.</p>
          <RulesTab
            strategy={strategy}
            attachedIndicators={attachedIndicators}
            availableIndicators={availableIndicators}
            onAttachIndicator={onAttachIndicator}
            onDetachIndicator={onDetachIndicator}
            onAddRule={onAddRule}
            onEditRule={onEditRule}
            onDeleteRule={onDeleteRule}
            onCreateGlobalFilter={onCreateGlobalFilter}
            onUpdateGlobalFilter={onUpdateGlobalFilter}
            onDeleteGlobalFilter={onDeleteGlobalFilter}
            onCreateRuleFilter={onCreateRuleFilter}
            onUpdateRuleFilter={onUpdateRuleFilter}
            onDeleteRuleFilter={onDeleteRuleFilter}
            indicatorLookup={indicatorLookup}
            DropdownSelect={DropdownSelect}
            ActionButton={ActionButton}
          />
        </TabPanel>

        <TabPanel active={activeTab === 'signals'}>
          <p className="px-6 pb-2 text-xs text-slate-400">Preview when this strategy would attempt orders.</p>
          <OrderTriggersTab
            strategy={strategy}
            instruments={strategyInstruments}
            attachedIndicators={attachedIndicators}
            signalWindow={signalWindow}
            signalsLoading={signalsLoading}
            signalResult={signalResult}
            signalInstrumentId={signalInstrumentId}
            onInstrumentChange={setSignalInstrumentId}
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
