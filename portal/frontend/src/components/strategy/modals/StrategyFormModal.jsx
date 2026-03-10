import ATMConfigForm, { DEFAULT_ATM_TEMPLATE } from '../../atm/ATMConfigForm.jsx'
import ATMTemplateSummary from '../../atm/ATMTemplateSummary.jsx'
import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import { TimeframeSelect } from '../../ChartComponent/TimeframeSelectComponent.jsx'
import ActionButton from '../ui/ActionButton.jsx'
import { formatCurrency, formatNumber, parseNumericOr } from '../../../utils/strategy/formatting.js'
import { MIN_BASE_RISK, MIN_RISK_MULTIPLIER } from '../../../utils/strategy/formDefaults.js'
import InstrumentDetailsPanel from '../../InstrumentDetailsPanel.jsx'
import { normalizeSymbol } from '../../../utils/strategy/symbolValidation.js'
import useStrategyForm from '../../../hooks/strategy/useStrategyForm.js'

function StrategyFormModal({ open, initialValues, onSubmit, onCancel, submitting, availableATMTemplates = [], error = null }) {
  const {
    form,
    setForm,
    currentStep,
    setCurrentStep,
    atmMode,
    setAtmMode,
    selectedATMTemplateId,
    setSelectedATMTemplateId,
    touched,
    setTouched,
    showValidation,
    setShowValidation,
    prefetchingMeta,
    setPrefetchingMeta,
    atmPrefillWarning,
    setAtmPrefillWarning,
    providers,
    setProviders,
    providersLoading,
    setProvidersLoading,
    slotStatus,
    setSlotStatus,
    expandedSlots,
    setExpandedSlots,
    symbolsInput,
    setSymbolsInput,
    symbolValidation,
    setSymbolValidation,
    riskSettings,
    setRiskSettings,
    riskErrors,
    setRiskErrors,
    atmErrors,
    setAtmErrors,
    saveAnimationStage,
    setSaveAnimationStage,
    saveAnimationVisible,
    setSaveAnimationVisible,
    initializedKey,
    setInitializedKey,
    modalLogger,
    templateOptionsRef,
    providerOptions,
    getVenueOptions,
    validateStep1,
    step1Errors,
    isStep1Valid,
    showFieldError,
    markTouched,
    applyTickDefaults,
    lookupTickMetadata,
    templateOptions,
    selectedTemplate,
    handleChange,
    handleATMTemplateChange,
    slotIssues,
    parsedSymbolsPreview,
    updateSlots,
    handleAddSlot,
    handleRemoveSlot,
    handleSlotChange,
    updateRiskSettings,
    handleToggleSlot,
    handleReorderSlot,
    toggleSlotDetails,
    handleRefreshMetadata,
    handleATMTemplateSelect,
    validateTemplate,
    handleSubmit,
    venueOptions,
    handleStepAdvance,
    handleStepBack,
    steps
  } = useStrategyForm({ open, initialValues, onSubmit, availableATMTemplates })
  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="relative w-full max-w-4xl space-y-6 overflow-hidden rounded-2xl border border-white/10 bg-[#14171f] text-slate-100 shadow-xl">
        {saveAnimationVisible && (
          <div className="absolute inset-0 z-20 flex items-center justify-center bg-black/90 backdrop-blur-sm">
            {saveAnimationStage === 'saving' && (
              <div className="flex flex-col items-center gap-3 text-slate-200">
                <div className="flex h-14 w-14 items-center justify-center rounded-full border-2 border-emerald-400/40 border-t-transparent animate-spin" />
                <p className="text-lg font-semibold text-white animate-pulse">Saving strategy…</p>
                <p className="text-xs text-slate-400">Hold tight while we store your template.</p>
              </div>
            )}
            {saveAnimationStage === 'saved' && (
              <div className="flex flex-col items-center gap-3 text-emerald-100">
                <div className="relative">
                  <div className="absolute inset-0 animate-ping rounded-full bg-emerald-500/40" />
                  <div className="relative flex h-16 w-16 items-center justify-center rounded-full bg-emerald-500 text-2xl font-bold text-white shadow-lg shadow-emerald-500/40 transition-transform duration-300 ease-out">
                    ✓
                  </div>
                </div>
                <p className="text-lg font-semibold">Saved!</p>
                <p className="text-sm text-emerald-200/80">Preparing your review…</p>
              </div>
            )}
          </div>
        )}
        <header className="border-b border-white/5 px-6 py-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                {initialValues ? 'Edit strategy' : 'Create strategy'}
              </p>
              <h3 className="text-lg font-semibold text-white">{steps[currentStep].title}</h3>
              {steps[currentStep].description && (
                <p className="text-sm text-slate-400">{steps[currentStep].description}</p>
              )}
            </div>
            <div className="flex items-center gap-3 text-xs uppercase tracking-[0.2em] text-slate-400">
              {steps.map((step) => (
                <div
                  key={step.id}
                  className={`flex items-center gap-2 rounded-full px-3 py-1 ${
                    currentStep === step.id
                      ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                      : 'bg-white/5'
                  }`}
                >
                  <span
                    className={`flex h-6 w-6 items-center justify-center rounded-full text-xs font-bold ${
                      currentStep === step.id ? 'bg-[color:var(--accent-alpha-40)] text-white' : 'bg-white/10 text-slate-200'
                    }`}
                  >
                    {step.id + 1}
                  </span>
                  <span>{step.title}</span>
                </div>
              ))}
            </div>
          </div>
        </header>

        <form className="max-h-[70vh] space-y-6 overflow-y-auto px-6 py-4" onSubmit={handleSubmit}>
          {currentStep === 0 && (
            <div className="space-y-4">
              <div className="grid gap-4 md:grid-cols-2">
                <div>
                  <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Name</label>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={form.name}
                    onChange={handleChange('name')}
                    onBlur={() => markTouched('name')}
                  />
                </div>
                <div>
                  <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Description</label>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={form.description}
                    onChange={handleChange('description')}
                    placeholder="Optional context for your notes"
                    onBlur={() => markTouched('description')}
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                <TimeframeSelect
                  selected={form.timeframe || '15m'}
                  onChange={(value) => handleChange('timeframe')(value)}
                  variant="dropdown"
                  className="md:col-span-1"
                />
                <div className="md:col-span-1">
                  <DropdownSelect
                    label="Data Provider"
                    value={form.provider_id || ''}
                    onChange={handleChange('provider_id')}
                    options={providerOptions}
                    className="mt-1 w-full"
                  />
                  {showFieldError('provider_id') ? (
                    <p className="mt-1 text-xs text-rose-400">{step1Errors.provider_id}</p>
                  ) : null}
                </div>
                <div className="md:col-span-1">
                  <DropdownSelect
                    label="Venue"
                    value={form.venue_id || ''}
                    onChange={handleChange('venue_id')}
                    options={venueOptions}
                    className="mt-1 w-full"
                  />
                  {showFieldError('venue_id') ? (
                    <p className="mt-1 text-xs text-rose-400">{step1Errors.venue_id}</p>
                  ) : null}
                </div>
              </div>

              <div className="space-y-3 rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Symbols</p>
                    <p className="text-xs text-slate-500">Comma-separated list validated against the provider and venue.</p>
                  </div>
                  <div className="text-[11px] text-slate-400">Example: ES,CL,GC,BTCUSD</div>
                </div>
                <textarea
                  className="mt-2 w-full rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  rows={3}
                  value={symbolsInput}
                  onChange={(event) => setSymbolsInput(event.target.value)}
                  placeholder="ES,CL,GC,BTCUSD"
                />
                {showFieldError('instrument_slots') ? (
                  <p className="text-xs text-rose-400">{step1Errors.instrument_slots}</p>
                ) : (
                  <p className="text-[11px] text-slate-500">
                    We'll validate symbols and fetch metadata before moving to risk settings.
                  </p>
                )}

                {parsedSymbolsPreview.length > 0 && (
                  <div className="rounded-xl border border-white/5 bg-white/5 p-3 text-xs text-slate-300">
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Preview</p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {parsedSymbolsPreview.map((symbol, index) => {
                        const issue = slotIssues[index]
                        return (
                          <span
                            key={`${symbol}-${index}`}
                            className={`rounded-full px-3 py-1 ${issue ? 'bg-rose-500/20 text-rose-100' : 'bg-white/10 text-slate-100'}`}
                          >
                            {symbol}
                            {issue ? <span className="ml-2 text-[11px] text-rose-200">{issue}</span> : null}
                          </span>
                        )
                      })}
                    </div>
                  </div>
                )}

                {Object.keys(symbolValidation).length > 0 && (
                  <div className="space-y-2 rounded-xl border border-white/10 bg-white/5 p-3">
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Validation</p>
                    <div className="space-y-2">
                      {Object.entries(symbolValidation).map(([symbol, info]) => (
                        <div
                          key={symbol}
                          className="flex items-start justify-between rounded-lg bg-black/40 px-3 py-2 text-xs"
                        >
                          <div className="font-semibold text-white">{symbol}</div>
                          <div className="text-right">
                            {info.status === 'ok' ? (
                              <span className="text-emerald-300">Validated</span>
                            ) : (
                              <span className="text-rose-300">{info.message}</span>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 1 && (
            <div className="space-y-4">
              <div className="space-y-4 rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Stop Distance (1R Definition)</p>
                    <p className="text-xs text-slate-500">Defines how wide your stop is.</p>
                  </div>
                  <div className="text-[11px] text-slate-400">Risk drives sizing; keep position sizing off in the next step.</div>
                </div>
                <div className="grid gap-3 md:grid-cols-2">
                  <div>
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</label>
                    <input
                      className="mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                      type="number"
                      min={1}
                      value={riskSettings.atrPeriod ?? 14}
                      onChange={(event) => updateRiskSettings({ atrPeriod: Math.max(1, Number(event.target.value) || 14) })}
                    />
                    <p className="mt-1 text-[11px] text-slate-500">Rolling ATR length used to define 1R.</p>
                  </div>
                  <div>
                    <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.3em] text-slate-500">
                      <span>ATR multiplier</span>
                      <span
                        className="text-[11px] text-slate-500"
                        title="Scales ATR to set stop distance. Example: ATR 10 × 1.5 → 15pt stop."
                      >
                        ⓘ
                      </span>
                    </div>
                    <input
                      className="mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                      type="number"
                      step="0.1"
                      min={0}
                      value={riskSettings.atrMultiplier ?? 1}
                      onChange={(event) => updateRiskSettings({ atrMultiplier: Number(event.target.value) || 1 })}
                    />
                    <p className="mt-1 text-[11px] text-slate-500">Scales ATR to set your stop distance (1R).</p>
                  </div>
                </div>
              </div>

              <div className="space-y-4 rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Position Sizing</p>
                    <p className="text-xs text-slate-500">Controls how much money you risk per trade.</p>
                  </div>
                </div>

                <div className="grid gap-3 md:grid-cols-2">
                  <div className="flex flex-col justify-start">
                    <label className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Base Risk Per Trade ($)</label>
                    <input
                      className="mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                      type="number"
                      min={MIN_BASE_RISK}
                      step="0.01"
                      value={riskSettings.baseRiskPerTrade ?? ''}
                      onChange={(event) => {
                        const rawValue = event.target.value
                        const nextValue = rawValue === '' ? '' : Math.max(MIN_BASE_RISK, Number(rawValue) || 0)
                        setRiskErrors((prev) => ({ ...prev, baseRiskPerTrade: undefined }))
                        updateRiskSettings({ baseRiskPerTrade: nextValue })
                      }}
                    />
                    <p className="mt-1 text-[11px] text-slate-500">Dollar amount risked per trade before multipliers.</p>
                    {riskErrors.baseRiskPerTrade ? (
                      <p className="mt-1 text-[11px] text-rose-400">{riskErrors.baseRiskPerTrade}</p>
                    ) : null}
                  </div>
                  <div className="flex flex-col justify-start">
                    <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.3em] text-slate-500">
                      <span>Global risk multiplier</span>
                      <span
                        className="text-[11px] text-slate-500"
                        title="Scales position size. Example: $100 base risk × 2 → $200 risk."
                      >
                        ⓘ
                      </span>
                    </div>
                    <input
                      className="mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                      type="number"
                      step="0.1"
                      min={MIN_RISK_MULTIPLIER}
                      value={riskSettings.globalRiskMultiplier ?? ''}
                      onChange={(event) => {
                        const rawValue = event.target.value
                        const nextValue = rawValue === '' ? '' : Math.max(MIN_RISK_MULTIPLIER, Number(rawValue) || 0)
                        setRiskErrors((prev) => ({ ...prev, globalRiskMultiplier: undefined }))
                        updateRiskSettings({ globalRiskMultiplier: nextValue })
                      }}
                    />
                    <p className="mt-1 text-[11px] text-slate-500">Default multiplier for every symbol.</p>
                    {riskErrors.globalRiskMultiplier ? (
                      <p className="mt-1 text-[11px] text-rose-400">{riskErrors.globalRiskMultiplier}</p>
                    ) : null}
                  </div>
                </div>

                <div className="space-y-3 rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="flex items-center justify-between">
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Per-symbol risk overrides</p>
                  </div>

                  <div className="space-y-3">
                    {(form.instrument_slots || []).map((slot) => {
                      const status = slotStatus[slot.uid] || {}
                      const baseRiskValue = parseNumericOr(riskSettings.baseRiskPerTrade, 0)
                      const globalMultiplier = parseNumericOr(riskSettings.globalRiskMultiplier, 1)
                      const hasOverride =
                        slot.risk_multiplier !== '' &&
                        slot.risk_multiplier !== null &&
                        slot.risk_multiplier !== undefined
                      const overrideMultiplier = hasOverride ? parseNumericOr(slot.risk_multiplier, globalMultiplier) : null
                      const effectiveMultiplier = overrideMultiplier === null ? globalMultiplier : overrideMultiplier
                      const estimatedRisk = baseRiskValue * effectiveMultiplier
                      return (
                        <div key={slot.uid} className="space-y-2 rounded-xl border border-white/10 bg-black/30 p-3">
                          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                            <div className="space-y-1">
                              <p className="text-sm font-semibold text-white">{slot.symbol}</p>
                              <p className="text-[11px] text-slate-400">
                                Estimated risk per trade: {formatCurrency(estimatedRisk)}
                              </p>
                            </div>
                            <div className="flex items-center gap-2 md:min-w-[240px]">
                              <input
                                className="w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                                type="number"
                                step="0.1"
                                placeholder={String(riskSettings.globalRiskMultiplier || '')}
                                value={slot.risk_multiplier ?? ''}
                                onChange={(event) => handleSlotChange(slot.uid, { risk_multiplier: event.target.value })}
                              />
                            </div>
                          </div>
                          <div className="flex flex-wrap items-center gap-2 text-xs">
                            <ActionButton type="button" variant="ghost" className="text-xs" onClick={() => toggleSlotDetails(slot.uid)}>
                              {expandedSlots.includes(slot.uid) ? 'Hide details' : 'Show details'}
                            </ActionButton>
                            <ActionButton
                              type="button"
                              variant="subtle"
                              className="text-xs"
                              onClick={() => handleRefreshMetadata(slot)}
                              disabled={status.loading}
                            >
                              {status.loading ? 'Loading…' : 'Refresh metadata'}
                            </ActionButton>
                          </div>
                          {expandedSlots.includes(slot.uid) ? (
                            <div className="border-t border-white/5 pt-2">
                              <InstrumentDetailsPanel
                                symbol={normalizeSymbol(slot.symbol) || slot.symbol}
                                metadata={slot.metadata}
                                providerId={form.provider_id}
                                venueId={form.venue_id}
                                timeframe={form.timeframe}
                                status={status}
                                onRefresh={() => handleRefreshMetadata(slot)}
                              />
                            </div>
                          ) : null}
                        </div>
                      )
                    })}
                  </div>
                </div>
              </div>
            </div>
          )}

          {currentStep === 2 && (
            <div className="space-y-4">
              {atmPrefillWarning ? (
                <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
                  {atmPrefillWarning}
                </div>
              ) : null}

              {templateOptions.length > 0 && (
                <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
                  <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Reuse ATM template</p>
                      <p className="text-xs text-slate-500">Start from an existing configuration or build fresh below.</p>
                    </div>
                    <div className="flex flex-wrap gap-3 text-xs font-semibold uppercase tracking-[0.2em] text-slate-300">
                      <label className="flex items-center gap-2">
                        <input
                          type="radio"
                          className="h-4 w-4 accent-[color:var(--accent-text-strong)]"
                          checked={atmMode === 'existing'}
                          onChange={() => setAtmMode('existing')}
                        />
                        Use existing
                      </label>
                      <label className="flex items-center gap-2">
                        <input
                          type="radio"
                          className="h-4 w-4 accent-[color:var(--accent-text-strong)]"
                          checked={atmMode === 'new'}
                          onChange={() => setAtmMode('new')}
                        />
                        Create new
                      </label>
                    </div>
                  </div>

                  {atmMode === 'existing' && (
                    <div className="mt-3 space-y-3">
                      <DropdownSelect
                        label="Existing templates"
                        value={selectedATMTemplateId}
                        onChange={handleATMTemplateSelect}
                        options={templateOptions.map((option) => ({ value: option.value, label: option.label }))}
                        className="mt-1 w-full"
                      />
                    </div>
                  )}
                </div>
              )}

              <div className="space-y-3 rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">ATM template</p>
                    <p className="text-xs text-slate-500">Stops, targets, stop adjustments, and trailing in a compact layout.</p>
                  </div>
                  <ActionButton type="button" variant="subtle" onClick={() => handleATMTemplateChange(DEFAULT_ATM_TEMPLATE)}>
                    Reset
                  </ActionButton>
                </div>
                {atmMode !== 'existing' && (
                  <ATMConfigForm
                    value={form.atm_template}
                    onChange={handleATMTemplateChange}
                    errors={atmErrors}
                    hidePositionSizing
                    hideRiskSettings
                    collapsible
                  />
                )}
                {atmMode === 'existing' && selectedTemplate && (
                  <div className="mt-3">
                    <ATMTemplateSummary template={selectedTemplate.template} compact />
                  </div>
                )}
                {atmMode === 'existing' && !selectedTemplate && (
                  <p className="text-xs text-amber-200/80">
                    Select an existing template above or switch to "Create new" to edit the fields directly.
                  </p>
                )}
              </div>
            </div>
          )}
          {currentStep === 3 && (
            <div className="space-y-4">
              <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Basic setup</p>
                    <p className="text-xs text-slate-500">Core strategy metadata and symbols.</p>
                  </div>
                  <ActionButton type="button" variant="subtle" onClick={() => setCurrentStep(0)}>
                    ✎ Edit
                  </ActionButton>
                </div>
                <div className="mt-3 grid gap-3 md:grid-cols-2">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Name</p>
                    <p className="text-base text-white">{form.name || 'Untitled strategy'}</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Timeframe</p>
                    <p className="text-base text-white">{form.timeframe}</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Provider / Venue</p>
                    <p className="text-base text-white">
                      {form.provider_id || '—'} / {form.venue_id || '—'}
                    </p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Symbols</p>
                    <p className="text-base text-white">
                      {(form.instrument_slots || []).map((slot) => slot.symbol).join(', ') || '—'}
                    </p>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Risk & ATR</p>
                    <p className="text-xs text-slate-500">Sizing inputs carried into your ATM template.</p>
                  </div>
                  <ActionButton type="button" variant="subtle" onClick={() => setCurrentStep(1)}>
                    ✎ Edit
                  </ActionButton>
                </div>
                <div className="mt-3 grid gap-3 md:grid-cols-4">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Base risk per trade</p>
                    <p className="text-base text-white">{formatCurrency(riskSettings.baseRiskPerTrade || 0)}</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR period</p>
                    <p className="text-base text-white">{riskSettings.atrPeriod}</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">ATR multiplier (1R)</p>
                    <p className="text-base text-white">{riskSettings.atrMultiplier}</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Global risk multiplier</p>
                    <p className="text-base text-white">{riskSettings.globalRiskMultiplier}</p>
                  </div>
                </div>
                <div className="mt-4 space-y-2">
                  <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Per-symbol risk</p>
                  <div className="grid gap-2 md:grid-cols-2">
                    {(form.instrument_slots || []).map((slot) => {
                      const base = parseNumericOr(riskSettings.baseRiskPerTrade, 0)
                      const globalMultiplier = parseNumericOr(riskSettings.globalRiskMultiplier, 1)
                      const override = slot.risk_multiplier === '' || slot.risk_multiplier === null
                        ? null
                        : parseNumericOr(slot.risk_multiplier, globalMultiplier)
                      const effective = override === null ? globalMultiplier : override
                      return (
                        <div key={slot.uid} className="rounded-xl border border-white/5 bg-black/40 px-3 py-2 text-sm">
                          <div className="flex items-center justify-between text-white">
                            <span className="font-semibold">{slot.symbol}</span>
                            <span className="text-xs text-slate-300">× {formatNumber(effective)} R</span>
                          </div>
                          <p className="text-xs text-slate-400">
                            Estimated risk: {formatCurrency(base * effective)}
                          </p>
                        </div>
                      )
                    })}
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">ATM template</p>
                    <p className="text-xs text-slate-500">Saved stops, targets, adjustments, and trailing.</p>
                  </div>
                  <ActionButton type="button" variant="subtle" onClick={() => setCurrentStep(2)}>
                    ✎ Edit
                  </ActionButton>
                </div>
                <div className="mt-3">
                  <ATMTemplateSummary template={form.atm_template} compact />
                </div>
              </div>
            </div>
          )}

          {error && (
            <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 p-3">
              <p className="text-sm font-semibold text-rose-300">Failed to save strategy</p>
              <p className="mt-1 text-xs text-rose-200">{error}</p>
            </div>
          )}

          <footer className="flex items-center justify-between border-t border-white/5 pt-4">
            <div className="text-xs text-slate-500">
              Step {currentStep + 1} of {steps.length}
            </div>
            <div className="flex items-center gap-2">
              <ActionButton type="button" variant="ghost" onClick={onCancel}>
                Cancel
              </ActionButton>
              {currentStep > 0 && (
                <ActionButton type="button" variant="subtle" onClick={handleStepBack}>
                  Back
                </ActionButton>
              )}
              {currentStep < 2 && (
                <ActionButton type="button" onClick={handleStepAdvance} disabled={prefetchingMeta || providersLoading}>
                  {prefetchingMeta || providersLoading ? 'Loading…' : 'Next'}
                </ActionButton>
              )}
              {currentStep >= 2 && (
                <ActionButton type="submit" disabled={submitting}>
                  {submitting ? 'Saving…' : 'Save strategy'}
                </ActionButton>
              )}
              {currentStep === 3 && (
                <ActionButton type="button" onClick={onCancel}>
                  Close
                </ActionButton>
              )}
            </div>
          </footer>
        </form>
      </div>
    </div>
  )
}


export default StrategyFormModal
