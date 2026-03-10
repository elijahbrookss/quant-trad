import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { fetchProviders, fetchTickMetadata } from '../../adapters/provider.adapter.js'
import { DEFAULT_ATM_TEMPLATE, cloneATMTemplate } from '../../components/atm/ATMConfigForm.jsx'
import { createLogger } from '../../utils/logger.js'
import {
  STRATEGY_FORM_DEFAULT,
  MIN_RISK_MULTIPLIER,
  MIN_BASE_RISK,
  RISK_DEFAULTS,
} from '../../utils/strategy/formDefaults.js'
import { parseNumericOr } from '../../utils/strategy/formatting.js'
import { normalizeSymbol, parseSymbolInput } from '../../utils/strategy/symbolValidation.js'
import { newSlot, normaliseSlot, inflateSlots } from '../../utils/strategy/slotManagement.js'
import { templateKey, validateATMTemplate, stripInstrumentTemplateFields } from '../../utils/strategy/atmTemplate.js'

const useStrategyForm = ({ open, initialValues, onSubmit, availableATMTemplates = [] } = {}) => {
  const [form, setForm] = useState(() => ({
    ...STRATEGY_FORM_DEFAULT,
    atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
  }))
  const [currentStep, setCurrentStep] = useState(0)
  const [atmMode, setAtmMode] = useState('new')
  const [selectedATMTemplateId, setSelectedATMTemplateId] = useState('')
  const [touched, setTouched] = useState({})
  const [showValidation, setShowValidation] = useState(false)
  const [prefetchingMeta, setPrefetchingMeta] = useState(false)
  const [atmPrefillWarning, setAtmPrefillWarning] = useState(null)
  const [providers, setProviders] = useState([])
  const [providersLoading, setProvidersLoading] = useState(false)
  const [slotStatus, setSlotStatus] = useState({})
  const [expandedSlots, setExpandedSlots] = useState([])
  const [symbolsInput, setSymbolsInput] = useState('')
  const [symbolValidation, setSymbolValidation] = useState({})
  const [riskSettings, setRiskSettings] = useState(RISK_DEFAULTS)
  const [riskErrors, setRiskErrors] = useState({})
  const [atmErrors, setAtmErrors] = useState({})
  const [saveAnimationStage, setSaveAnimationStage] = useState('idle')
  const [saveAnimationVisible, setSaveAnimationVisible] = useState(false)
  const [initializedKey, setInitializedKey] = useState(null)
  const modalLogger = useMemo(() => createLogger('StrategyFormModal'), [])
  const templateOptionsRef = useRef([])

  const providerOptions = useMemo(
    () => (providers || []).map((provider) => ({ value: provider.id, label: provider.label })),
    [providers],
  )

  const getVenueOptions = useCallback(
    (providerId) => {
      const normalizedProvider = (providerId || '').toUpperCase()
      const provider = (providers || []).find((item) => item.id === normalizedProvider)
      return (provider?.venues || []).map((venue) => ({ value: venue.id, label: venue.label }))
    },
    [providers],
  )

  useEffect(() => {
    if (!open) return undefined
    let cancelled = false
    setProvidersLoading(true)
    fetchProviders()
      .then((response) => {
        if (cancelled) return
        const items = response?.providers || []
        setProviders(items)
        setForm((prev) => {
          if (prev.provider_id) return prev
          const firstProvider = items[0]
          const defaultVenue = (firstProvider?.venues || []).length === 1 ? firstProvider?.venues?.[0]?.id || '' : ''
          return {
            ...prev,
            provider_id: prev.provider_id || firstProvider?.id || '',
            venue_id: prev.venue_id || defaultVenue,
          }
        })
      })
      .catch((err) => {
        if (!cancelled) {
          modalLogger?.warn('provider_fetch_failed', err)
          const fallbackProviders = [
            { id: 'ALPACA', label: 'Alpaca', venues: [{ id: 'ALPACA', label: 'Alpaca' }] },
          ]
          setProviders(fallbackProviders)
          setForm((prev) => ({
            ...prev,
            provider_id: prev.provider_id || 'ALPACA',
            venue_id: prev.venue_id || 'ALPACA',
          }))
        }
      })
      .finally(() => {
        if (!cancelled) setProvidersLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, modalLogger])

  const validateStep1 = useCallback(
    (state) => {
      const errors = {}
      const providerId = (state.provider_id || '').toUpperCase()
      const venueId = (state.venue_id || '').toUpperCase()
      const slotIssues = {}
      const parsedSymbols = parseSymbolInput(symbolsInput)

      const hasProviderOptions = providerOptions.length > 0
      const validProviders = new Set(providerOptions.map((option) => option.value))
      if (!providerId) {
        errors.provider_id = 'Select a data provider to continue.'
      } else if (hasProviderOptions && !validProviders.has(providerId)) {
        errors.provider_id = 'Select a data provider to continue.'
      }

      const venueOptionsForProvider = getVenueOptions(providerId)
      const validVenues = new Set(venueOptionsForProvider.map((option) => option.value))
      if (!venueId) {
        errors.venue_id = 'Select an exchange for this provider.'
      } else if (venueOptionsForProvider.length && !validVenues.has(venueId)) {
        errors.venue_id = 'Choose a venue supported by the selected provider.'
      }

      const seenSymbols = new Set()
      parsedSymbols.forEach((symbol, index) => {
        const normalized = normalizeSymbol(symbol)
        if (!normalized) {
          slotIssues[index] = 'Symbol is required.'
          return
        }
        if (!/^[A-Za-z0-9][A-Za-z0-9\-/]*$/.test(normalized)) {
          slotIssues[index] = 'Use letters/numbers with - or / separators.'
          return
        }
        const key = normalized.toUpperCase()
        if (seenSymbols.has(key)) {
          slotIssues[index] = 'Symbols must be unique.'
        } else {
          seenSymbols.add(key)
        }
      })

      if (!parsedSymbols.length || !seenSymbols.size) {
        errors.instrument_slots = 'Add at least one symbol.'
      }
      if (Object.keys(slotIssues).length) {
        errors.slotIssues = slotIssues
      }

      return errors
    },
    [getVenueOptions, normalizeSymbol, parseSymbolInput, providerOptions, symbolsInput],
  )

  const step1Errors = useMemo(() => validateStep1(form), [form, validateStep1])
  const isStep1Valid = useMemo(() => Object.keys(step1Errors).length === 0, [step1Errors])
  const showFieldError = useCallback((field) => (showValidation || touched[field]) && step1Errors[field], [showValidation, step1Errors, touched])

  const markTouched = useCallback((field) => setTouched((prev) => ({ ...prev, [field]: true })), [])

  const applyTickDefaults = useCallback((defaults) => {
    if (!defaults) return
    setForm((prev) => {
      const template = cloneATMTemplate(prev.atm_template)
      const meta = { ...(template._meta || {}) }

      const applyField = (field) => {
        if (meta[`${field}_override`]) return
        const value = defaults[field]
        if (value === undefined || value === null) return
        template[field] = value
        meta[`${field}_override`] = true
      }

      applyField('tick_size')
      applyField('tick_value')
      applyField('contract_size')
      template._meta = meta
      return { ...prev, atm_template: template }
    })
  }, [])

  const strategyId = initialValues?.id || null
  const lookupTickMetadata = useCallback(async ({ symbol, provider_id, venue_id, timeframe }) => {
    const response = await fetchTickMetadata({
      provider_id,
      venue_id,
      symbol,
      timeframe,
      strategy_id: strategyId,
    })
    if (response?.errors) {
      const firstError = Object.values(response.errors).find(Boolean)
      const error = new Error(firstError || 'Tick metadata unavailable')
      error.payload = response.errors
      throw error
    }
    if (response?.metadata) return response.metadata
    return null
  }, [strategyId])

  const templateOptions = useMemo(
    () =>
      (availableATMTemplates || []).map((item, index) => {
        const template = cloneATMTemplate(item.template || DEFAULT_ATM_TEMPLATE)
        const label = template.name?.trim() || item.label || `ATM template ${index + 1}`
        if (!template.name) {
          template.name = label
        }
        return {
          value: item.id || `atm-${index + 1}`,
          label,
          template,
          key: templateKey(template),
        }
      }),
    [availableATMTemplates, templateKey],
  )

  const selectedTemplate = useMemo(
    () => templateOptions.find((option) => option.value === selectedATMTemplateId),
    [selectedATMTemplateId, templateOptions],
  )

  useEffect(() => {
    templateOptionsRef.current = templateOptions
  }, [templateOptions])

  useEffect(() => {
    setSymbolValidation({})
  }, [symbolsInput])

  useEffect(() => {
    if (!open) {
      setForm({
        ...STRATEGY_FORM_DEFAULT,
        atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
        instrument_slots: [newSlot('')],
      })
      setCurrentStep(0)
      setAtmMode('new')
      setSelectedATMTemplateId('')
      setTouched({})
      setShowValidation(false)
      setPrefetchingMeta(false)
      setAtmPrefillWarning(null)
      setSlotStatus({})
      setExpandedSlots([])
      setSymbolsInput('')
      setSymbolValidation({})
      setRiskSettings(RISK_DEFAULTS)
      setRiskErrors({})
      setAtmErrors({})
      setSaveAnimationVisible(false)
      setSaveAnimationStage('idle')
      setInitializedKey(null)
      return
    }

    const initKey = initialValues?.id ? `strategy-${initialValues.id}` : 'new'
    if (initializedKey === initKey) {
      return
    }

    const currentTemplateOptions = templateOptionsRef.current || []
    if (initialValues) {
      const initialSlots = (() => {
        if (Array.isArray(initialValues.instrument_slots)) return inflateSlots(initialValues.instrument_slots)
        return inflateSlots([])
      })()
      const fallbackTemplate = cloneATMTemplate(initialValues.atm_template || DEFAULT_ATM_TEMPLATE)
      const matchById = currentTemplateOptions.find((option) => option.value === initialValues.atm_template_id)
      const matchByKey = currentTemplateOptions.find(
        (option) => option.key && option.key === templateKey(initialValues.atm_template),
      )
      const match = matchById || matchByKey

      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        timeframe: initialValues.timeframe || '15m',
        provider_id: (initialValues.provider_id || '').toUpperCase() || '',
        venue_id: (initialValues.venue_id || '').toUpperCase() || '',
        instrument_slots: initialSlots,
        atm_template: match ? cloneATMTemplate(match.template) : fallbackTemplate,
      })
      setAtmMode(match ? 'existing' : 'new')
      setSelectedATMTemplateId(match?.value || initialValues.atm_template_id || '')
      setCurrentStep(0)
      setSymbolsInput(initialSlots.map((slot) => slot.symbol).filter(Boolean).join(', '))
      setRiskSettings((prev) => ({
        ...prev,
        atrPeriod: initialValues.atm_template?.initial_stop?.atr_period ?? prev.atrPeriod,
        atrMultiplier: initialValues.atm_template?.initial_stop?.atr_multiplier ?? prev.atrMultiplier,
        baseRiskPerTrade:
          initialValues.base_risk_per_trade !== undefined
            ? Math.max(MIN_BASE_RISK, initialValues.base_risk_per_trade || MIN_BASE_RISK)
            : prev.baseRiskPerTrade,
        globalRiskMultiplier:
          initialValues.global_risk_multiplier ?? initialValues.atm_template?.risk?.global_risk_multiplier ?? prev.globalRiskMultiplier,
      }))
    } else {
      setForm({
        ...STRATEGY_FORM_DEFAULT,
        atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
        instrument_slots: [newSlot('')],
      })
      setCurrentStep(0)
      setAtmMode('new')
      setSelectedATMTemplateId('')
      setSymbolsInput('')
      setRiskSettings(RISK_DEFAULTS)
    }
    setInitializedKey(initKey)
    setTouched({})
    setShowValidation(false)
    setAtmPrefillWarning(null)
    setSlotStatus({})
    setExpandedSlots([])
    setSymbolValidation({})
    setRiskErrors({})
  }, [open, initialValues, templateKey, inflateSlots, initializedKey])

  useEffect(() => {
    if (!open || atmMode !== 'existing') return
    if (selectedATMTemplateId) return
    if (!initialValues?.atm_template_id && !initialValues?.atm_template) return

    const matchById = templateOptions.find((option) => option.value === initialValues.atm_template_id)
    const matchByKey = templateOptions.find(
      (option) => option.key && option.key === templateKey(initialValues.atm_template),
    )
    const match = matchById || matchByKey
    if (!match) return

    setSelectedATMTemplateId(match.value)
    setForm((prev) => ({ ...prev, atm_template: cloneATMTemplate(match.template) }))
  }, [atmMode, initialValues, open, selectedATMTemplateId, templateKey, templateOptions])

  const handleChange = (field) => (input) => {
    let value = input
    if (input && typeof input === 'object' && 'target' in input) {
      value = input.target?.value ?? ''
    }
    markTouched(field)

    if (field === 'provider_id' && typeof value === 'string') {
      const nextProvider = value.toUpperCase()
      setForm((prev) => {
        const allowedVenues = new Set(getVenueOptions(nextProvider).map((option) => option.value))
        const nextVenue = allowedVenues.size === 1 ? [...allowedVenues][0] : allowedVenues.has(prev.venue_id) ? prev.venue_id : ''
        const clearedSlots = (prev.instrument_slots || []).map((slot) => ({ ...slot, metadata: {} }))
        return { ...prev, provider_id: nextProvider, venue_id: nextVenue, instrument_slots: clearedSlots }
      })
      setSlotStatus({})
      setSymbolValidation({})
      return
    }

    if (field === 'venue_id' && typeof value === 'string') {
      value = value.toUpperCase()
      setSlotStatus({})
      setForm((prev) => ({
        ...prev,
        venue_id: value,
        instrument_slots: (prev.instrument_slots || []).map((slot) => ({ ...slot, metadata: {} })),
      }))
      setSymbolValidation({})
      return
    }

    if (field === 'timeframe') {
      setSymbolValidation({})
    }

    setForm((prev) => ({ ...prev, [field]: value ?? '' }))
  }

  const handleATMTemplateChange = useCallback((template) => {
    setForm((prev) => ({
      ...prev,
      atm_template: cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE),
    }))
    setAtmErrors({})
    setAtmMode('new')
  }, [])

  const slotIssues = useMemo(() => step1Errors.slotIssues || {}, [step1Errors])
  const parsedSymbolsPreview = useMemo(() => parseSymbolInput(symbolsInput), [parseSymbolInput, symbolsInput])

  const updateSlots = useCallback((updater) => {
    setForm((prev) => {
      const current = Array.isArray(prev.instrument_slots) ? prev.instrument_slots : []
      const next = updater(current)
      return { ...prev, instrument_slots: next.length ? next : [newSlot('')] }
    })
  }, [])

  const handleAddSlot = () => {
    markTouched('instrument_slots')
    updateSlots((slots) => [...slots, newSlot('')])
  }

  const handleRemoveSlot = (uid) => {
    markTouched('instrument_slots')
    updateSlots((slots) => slots.filter((slot) => slot.uid !== uid))
    setSlotStatus((prev) => {
      const next = { ...prev }
      delete next[uid]
      return next
    })
    setExpandedSlots((prev) => prev.filter((id) => id !== uid))
  }

  const handleSlotChange = useCallback(
    (uid, updates) => {
      markTouched('instrument_slots')
      updateSlots((slots) => slots.map((slot) => (slot.uid === uid ? { ...slot, ...updates } : slot)))
    },
    [markTouched, updateSlots],
  )

  const updateRiskSettings = useCallback((patch) => {
    const normalizedPatch = { ...patch }
    if ('baseRiskPerTrade' in patch) {
      const rawBaseRisk = patch.baseRiskPerTrade
      normalizedPatch.baseRiskPerTrade =
        rawBaseRisk === '' ? '' : Math.max(MIN_BASE_RISK, Number(rawBaseRisk) || MIN_BASE_RISK)
    }
    setRiskSettings((prev) => {
      const next = { ...prev, ...normalizedPatch }
      setForm((current) => ({
        ...current,
        atm_template: cloneATMTemplate({
          ...current.atm_template,
          initial_stop: {
            ...(current.atm_template?.initial_stop || {}),
            mode: 'atr',
            atr_period: next.atrPeriod ?? current.atm_template?.initial_stop?.atr_period ?? 14,
            atr_multiplier: next.atrMultiplier ?? current.atm_template?.initial_stop?.atr_multiplier ?? 1.0,
          },
          risk: {
            ...(current.atm_template?.risk || {}),
            base_risk_per_trade: next.baseRiskPerTrade ?? current.atm_template?.risk?.base_risk_per_trade,
          },
        }),
      }))
      return next
    })
  }, [])

  const handleToggleSlot = (uid) => {
    markTouched('instrument_slots')
    updateSlots((slots) => slots.map((slot) => (slot.uid === uid ? { ...slot, enabled: !slot.enabled } : slot)))
  }

  const handleReorderSlot = (uid, direction) => {
    markTouched('instrument_slots')
    updateSlots((slots) => {
      const index = slots.findIndex((slot) => slot.uid === uid)
      if (index < 0) return slots
      const nextIndex = direction === 'up' ? index - 1 : index + 1
      if (nextIndex < 0 || nextIndex >= slots.length) return slots
      const next = [...slots]
      const [moved] = next.splice(index, 1)
      next.splice(nextIndex, 0, moved)
      return next
    })
  }

  const toggleSlotDetails = (uid) => {
    setExpandedSlots((prev) => (prev.includes(uid) ? prev.filter((id) => id !== uid) : [...prev, uid]))
  }

  const handleRefreshMetadata = useCallback(
    async (slot) => {
      if (!slot) return
      const symbol = normalizeSymbol(slot.symbol)
      if (!symbol) {
        setSlotStatus((prev) => ({ ...prev, [slot.uid]: { error: 'Enter a symbol before loading metadata.' } }))
        return
      }
      if (!form.provider_id || !form.venue_id) {
        setSlotStatus((prev) => ({ ...prev, [slot.uid]: { error: 'Select provider and venue first.' } }))
        return
      }
      setSlotStatus((prev) => ({ ...prev, [slot.uid]: { loading: true, error: null } }))
      try {
        const metadata = await lookupTickMetadata({
          symbol,
          provider_id: form.provider_id,
          venue_id: form.venue_id,
          timeframe: form.timeframe,
        })
        if (metadata) {
          handleSlotChange(slot.uid, {
            metadata: { ...metadata, provider_id: form.provider_id, venue_id: form.venue_id },
          })
          setSlotStatus((prev) => ({ ...prev, [slot.uid]: { loading: false, error: null, updatedAt: Date.now() } }))
        } else {
          setSlotStatus((prev) => ({ ...prev, [slot.uid]: { loading: false, error: 'No metadata returned.' } }))
        }
      } catch (err) {
        setSlotStatus((prev) => ({
          ...prev,
          [slot.uid]: { loading: false, error: err?.message || 'Metadata lookup failed.' },
        }))
      }
    },
    [form.provider_id, form.timeframe, form.venue_id, handleSlotChange, lookupTickMetadata, normalizeSymbol],
  )

  useEffect(() => {
    const allowedVenues = new Set(getVenueOptions(form.provider_id).map((option) => option.value))
    if (form.venue_id && !allowedVenues.has(form.venue_id)) {
      setForm((prev) => ({ ...prev, venue_id: '' }))
      setTouched((prev) => ({ ...prev, venue_id: true }))
    }
  }, [form.provider_id, form.venue_id, getVenueOptions])

  const handleATMTemplateSelect = (value) => {
    const option = templateOptions.find((candidate) => candidate.value === value)
    setSelectedATMTemplateId(value || '')
    if (option) {
      handleATMTemplateChange(option.template)
      setAtmMode('existing')
    }
  }

  const validateTemplate = useCallback(() => {
    const errors = validateATMTemplate(form.atm_template || DEFAULT_ATM_TEMPLATE)
    setAtmErrors(errors)
    return errors
  }, [form.atm_template])

  const handleSubmit = async (event) => {
    event.preventDefault()
    if (currentStep < 2) return
    const fallbackName = `Strategy ${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`
    const name = form.name.trim() || fallbackName
    const rawGlobalRisk = riskSettings.globalRiskMultiplier
    const globalRisk = rawGlobalRisk === '' ? null : Number(rawGlobalRisk)
    const baseRiskInput = riskSettings.baseRiskPerTrade
    const baseRiskValue = baseRiskInput === '' ? null : Number(baseRiskInput)
    const atmValidation = validateTemplate()
    if (Object.keys(atmValidation).length) return

    const templateName = (form.atm_template?.name || '').trim()
    const cleanedSlots = (form.instrument_slots || [])
      .map((slot, index) => {
        const symbol = normalizeSymbol(slot.symbol)
        if (!symbol) return null
        const overrideValue =
          slot.risk_multiplier === '' || slot.risk_multiplier === null || slot.risk_multiplier === undefined
            ? null
            : Number(slot.risk_multiplier)
        const payload = {
          symbol,
          enabled: Boolean(slot.enabled),
        }
        if (Number.isFinite(overrideValue)) {
          payload.risk_multiplier = overrideValue
        }
        if (slot.metadata && Object.keys(slot.metadata).length) {
          payload.metadata = slot.metadata
        }
        return payload
      })
      .filter(Boolean)
    const riskOverrides = cleanedSlots.reduce((acc, slot) => {
      if (slot.risk_multiplier !== undefined && slot.risk_multiplier !== null) {
        acc[slot.symbol] = slot.risk_multiplier
      }
      return acc
    }, {})
    const payload = {
      name,
      description: form.description.trim() || null,
      timeframe: form.timeframe.trim() || '15m',
      provider_id: (form.provider_id || '').trim().toUpperCase() || null,
      venue_id: (form.venue_id || '').trim().toUpperCase() || null,
      datasource: null,
      exchange: null,
      instrument_slots: cleanedSlots,
      atm_template_id: atmMode === 'existing' ? selectedATMTemplateId || null : null,
      base_risk_per_trade: Number.isFinite(baseRiskValue) ? baseRiskValue : null,
      global_risk_multiplier: Number.isFinite(globalRisk) ? globalRisk : null,
      risk_overrides: riskOverrides,
      atm_template: stripInstrumentTemplateFields(cloneATMTemplate({
        ...form.atm_template,
        name: templateName,
        initial_stop: {
          ...(form.atm_template?.initial_stop || {}),
          mode: 'atr',
          atr_period: riskSettings.atrPeriod ?? form.atm_template?.initial_stop?.atr_period ?? 14,
          atr_multiplier: riskSettings.atrMultiplier ?? form.atm_template?.initial_stop?.atr_multiplier ?? 1.0,
        },
        risk: {
          ...(form.atm_template?.risk || {}),
          base_risk_per_trade: Number.isFinite(baseRiskValue) ? baseRiskValue : form.atm_template?.risk?.base_risk_per_trade,
          global_risk_multiplier: Number.isFinite(globalRisk) ? globalRisk : form.atm_template?.risk?.global_risk_multiplier,
        },
      })),
    }
    const savingStartedAt = Date.now()
    const MIN_SAVING_DURATION_MS = 800
    const SAVED_HOLD_DURATION_MS = 700
    try {
      setSaveAnimationVisible(true)
      setSaveAnimationStage('saving')
      await onSubmit(payload, { closeOnSuccess: false })
      const elapsed = Date.now() - savingStartedAt
      const remainingSavingDelay = Math.max(0, MIN_SAVING_DURATION_MS - elapsed)
      setTimeout(() => {
        setSaveAnimationStage('saved')
        setTimeout(() => {
          setSaveAnimationVisible(false)
          setSaveAnimationStage('idle')
          setCurrentStep(3)
        }, SAVED_HOLD_DURATION_MS)
      }, remainingSavingDelay)
    } catch (err) {
      setSaveAnimationVisible(false)
      setSaveAnimationStage('idle')
      // Error messaging handled upstream via onSubmit
    }
  }

  const venueOptions = useMemo(() => getVenueOptions(form.provider_id), [form.provider_id, getVenueOptions])

  const handleStepAdvance = async (event) => {
    event.preventDefault()
    if (currentStep === 0) {
      setShowValidation(true)
      if (!isStep1Valid) return
      setPrefetchingMeta(true)
      const parsedSymbols = parseSymbolInput(symbolsInput)
      const validationResults = {}
      const hydratedSlots = []
      try {
        for (const symbol of parsedSymbols) {
          try {
            const metadata = await lookupTickMetadata({
              symbol,
              provider_id: form.provider_id,
              venue_id: form.venue_id,
              timeframe: form.timeframe,
            })
            const normalizedMeta = metadata
              ? { ...metadata, provider_id: form.provider_id, venue_id: form.venue_id }
              : null
            validationResults[symbol] = { status: 'ok', metadata: normalizedMeta }
            hydratedSlots.push(
              normaliseSlot({ symbol, metadata: normalizedMeta || {}, enabled: true, risk_multiplier: '' }, hydratedSlots.length),
            )
          } catch (err) {
            const message =
              err?.payload?.errors?.symbol || err?.message || 'Symbol not supported for this provider/venue/timeframe.'
            validationResults[symbol] = { status: 'error', message }
          }
        }
      } finally {
        setPrefetchingMeta(false)
      }

      setSymbolValidation(validationResults)
      const hasErrors = Object.values(validationResults).some((entry) => entry?.status === 'error')
      if (hasErrors) return
      const nextSlots = hydratedSlots.length ? hydratedSlots : [newSlot('')]
      setForm((prev) => ({ ...prev, instrument_slots: nextSlots }))
      if (nextSlots[0]?.metadata) {
        applyTickDefaults(nextSlots[0].metadata)
        setAtmPrefillWarning(null)
      }
      setSlotStatus({})
      setShowValidation(false)
      setCurrentStep(1)
      return
    }

    if (currentStep === 1) {
      const errors = {}
      const parsedGlobalRisk = parseNumericOr(riskSettings.globalRiskMultiplier, null)
      const parsedBaseRisk = parseNumericOr(riskSettings.baseRiskPerTrade, null)
      if (parsedBaseRisk === null || !Number.isFinite(parsedBaseRisk)) {
        errors.baseRiskPerTrade = 'Set a base risk per trade before continuing.'
      } else if (parsedBaseRisk < MIN_BASE_RISK) {
        errors.baseRiskPerTrade = `Base risk per trade must be at least $${MIN_BASE_RISK}.`
      }
      if (parsedGlobalRisk === null || !Number.isFinite(parsedGlobalRisk)) {
        errors.globalRiskMultiplier = 'Set a global risk multiplier before continuing.'
      } else if (parsedGlobalRisk < MIN_RISK_MULTIPLIER) {
        errors.globalRiskMultiplier = `Global risk multiplier must be at least ${MIN_RISK_MULTIPLIER}.`
      }

      setRiskErrors(errors)
      if (Object.keys(errors).length) return

      setCurrentStep(2)
      return
    }
  }

  const handleStepBack = (event) => {
    event.preventDefault()
    setCurrentStep((step) => Math.max(0, step - 1))
  }


  const steps = [
    { id: 0, title: 'Basic setup', description: '' },
    { id: 1, title: 'Risk & ATR', description: 'Define ATR-based R and per-symbol overrides.' },
    { id: 2, title: 'ATM template', description: 'Stops, targets, stop adjustments, and trailing.' },
    { id: 3, title: 'Review', description: 'Confirm the saved strategy and jump back to edit.' },
  ]


  return {
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
  }
}

export default useStrategyForm
