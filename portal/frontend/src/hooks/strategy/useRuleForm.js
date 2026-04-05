import { useCallback, useEffect, useMemo, useState } from 'react'

import { RULE_FORM_DEFAULT } from '../../utils/strategy/formDefaults.js'
import { extractRuleFlow } from '../../components/strategy/rules/ruleUtils.js'
import { indicatorHasAuthorableOutputs } from '../../utils/indicatorOutputs.js'

const EMPTY_GUARD = {
  type: 'ctx',
  variant: 'match',
  indicator_id: '',
  output_name: '',
  field: '',
  value_text: [],
  operator: '>',
  value: '',
  bars: '',
  event_key: '',
}

const defaultVariantForType = (type) => {
  if (type === 'signal') return 'seen'
  return 'match'
}

const isGuardComplete = (guard) => {
  if (!guard || typeof guard !== 'object') return false
  const contextValues = Array.isArray(guard.value_text) ? guard.value_text.filter(Boolean) : [guard.value_text].filter(Boolean)
  const numericValue = guard.value === '' ? null : Number(guard.value)
  const bars = Number(guard.bars)

  if (guard.type === 'ctx' && guard.variant === 'match') {
    return Boolean(guard.indicator_id && guard.output_name && guard.field && contextValues.length)
  }
  if (guard.type === 'ctx' && guard.variant === 'held') {
    return Boolean(guard.indicator_id && guard.output_name && guard.field && contextValues.length && Number.isFinite(bars) && bars > 0)
  }
  if (guard.type === 'metric' && guard.variant === 'match') {
    return Boolean(
      guard.indicator_id
      && guard.output_name
      && guard.field
      && guard.operator
      && guard.value !== ''
      && numericValue !== null
      && !Number.isNaN(numericValue)
    )
  }
  if (guard.type === 'metric' && guard.variant === 'held') {
    return Boolean(
      guard.indicator_id
      && guard.output_name
      && guard.field
      && guard.operator
      && guard.value !== ''
      && numericValue !== null
      && !Number.isNaN(numericValue)
      && Number.isFinite(bars)
      && bars > 0
    )
  }
  if (guard.type === 'signal' && (guard.variant === 'seen' || guard.variant === 'absent')) {
    return Boolean(guard.indicator_id && guard.output_name && guard.event_key && Number.isFinite(bars) && bars > 0)
  }
  return false
}

const mapInitialGuardToForm = (guard) => {
  if (!guard || typeof guard !== 'object') return { ...EMPTY_GUARD }

  if (guard.type === 'context_match') {
    return {
      ...EMPTY_GUARD,
      type: 'ctx',
      variant: 'match',
      indicator_id: guard.indicator_id || '',
      output_name: guard.output_name || '',
      field: guard.field || 'state',
      value_text: Array.isArray(guard.value) ? guard.value : guard.value ? [guard.value] : [],
    }
  }

  if (guard.type === 'metric_match') {
    return {
      ...EMPTY_GUARD,
      type: 'metric',
      variant: 'match',
      indicator_id: guard.indicator_id || '',
      output_name: guard.output_name || '',
      field: guard.field || '',
      operator: guard.operator || '>',
      value: guard.value ?? '',
    }
  }

  if (guard.type === 'holds_for_bars' && guard.guard?.type === 'context_match') {
    return {
      ...EMPTY_GUARD,
      type: 'ctx',
      variant: 'held',
      indicator_id: guard.guard.indicator_id || '',
      output_name: guard.guard.output_name || '',
      field: guard.guard.field || 'state',
      value_text: Array.isArray(guard.guard.value) ? guard.guard.value : guard.guard.value ? [guard.guard.value] : [],
      bars: guard.bars ?? '',
    }
  }

  if (guard.type === 'holds_for_bars' && guard.guard?.type === 'metric_match') {
    return {
      ...EMPTY_GUARD,
      type: 'metric',
      variant: 'held',
      indicator_id: guard.guard.indicator_id || '',
      output_name: guard.guard.output_name || '',
      field: guard.guard.field || '',
      operator: guard.guard.operator || '>',
      value: guard.guard.value ?? '',
      bars: guard.bars ?? '',
    }
  }

  if (guard.type === 'signal_seen_within_bars' || guard.type === 'signal_absent_within_bars') {
    return {
      ...EMPTY_GUARD,
      type: 'signal',
      variant: guard.type === 'signal_seen_within_bars' ? 'seen' : 'absent',
      indicator_id: guard.indicator_id || '',
      output_name: guard.output_name || '',
      event_key: guard.event_key || '',
      bars: guard.lookback_bars ?? '',
    }
  }

  return { ...EMPTY_GUARD }
}

const useRuleForm = ({
  open,
  indicators,
  ensureIndicatorMeta,
  initialValues,
  getDefaultName,
} = {}) => {
  const [form, setForm] = useState(RULE_FORM_DEFAULT)
  const [guardFieldFilters, setGuardFieldFilters] = useState([])

  const indicatorMap = useMemo(() => {
    const map = new Map()
    for (const indicator of indicators || []) {
      if (indicator?.id) {
        map.set(indicator.id, indicator)
      }
    }
    return map
  }, [indicators])

  const signalIndicators = useMemo(
    () => (indicators || []).filter((indicator) => indicatorHasAuthorableOutputs(
      indicator,
      'signal',
      {
        selectedOutputName: indicator?.id === form.trigger?.indicator_id ? form.trigger?.output_name : '',
      },
    )),
    [form.trigger?.indicator_id, form.trigger?.output_name, indicators],
  )

  useEffect(() => {
    if (!open) {
      setForm(RULE_FORM_DEFAULT)
      setGuardFieldFilters([])
      return
    }
    if (initialValues) {
      const flow = extractRuleFlow(initialValues)
      const guards = Array.isArray(flow.guards) ? flow.guards.map((guard) => mapInitialGuardToForm(guard)) : []
      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        intent: initialValues.intent || (initialValues.action === 'sell' ? 'enter_short' : 'enter_long'),
        priority: Number(initialValues.priority ?? 0) || 0,
        enabled: initialValues.enabled ?? true,
        trigger: {
          indicator_id: flow.trigger?.indicator_id || '',
          output_name: flow.trigger?.output_name || '',
          event_key: flow.trigger?.event_key || '',
        },
        guards,
      })
      setGuardFieldFilters(guards.map(() => ''))
      return
    }
    setForm({ ...RULE_FORM_DEFAULT })
    setGuardFieldFilters([])
  }, [open, initialValues])

  const trackedIndicatorIds = useMemo(() => {
    const ids = [form.trigger?.indicator_id, ...(form.guards || []).map((guard) => guard?.indicator_id)]
    return Array.from(new Set(ids.filter((value) => typeof value === 'string' && value.trim())))
  }, [form.trigger, form.guards])

  useEffect(() => {
    if (!open || typeof ensureIndicatorMeta !== 'function') return
    trackedIndicatorIds.forEach((indicatorId) => ensureIndicatorMeta(indicatorId))
  }, [open, trackedIndicatorIds, ensureIndicatorMeta])

  const canSubmit = Boolean(
    form.trigger?.indicator_id
    && form.trigger?.output_name
    && form.trigger?.event_key,
  )
  const incompleteGuardIndexes = useMemo(
    () => (form.guards || [])
      .map((guard, index) => (isGuardComplete(guard) ? null : index))
      .filter((value) => value !== null),
    [form.guards],
  )

  const updateTrigger = useCallback((updates) => {
    setForm((prev) => ({
      ...prev,
      trigger: {
        ...prev.trigger,
        ...updates,
      },
    }))
  }, [])

  const updateGuard = useCallback((index, updates) => {
    setForm((prev) => ({
      ...prev,
      guards: prev.guards.map((guard, guardIndex) => (
        guardIndex === index ? { ...guard, ...updates } : guard
      )),
    }))
  }, [])

  const addGuard = useCallback(() => {
    setForm((prev) => {
      return {
        ...prev,
        guards: [...(prev.guards || []), { ...EMPTY_GUARD }],
      }
    })
    setGuardFieldFilters((prev) => [...prev, ''])
  }, [])

  const removeGuard = useCallback((index) => {
    setForm((prev) => ({
      ...prev,
      guards: prev.guards.filter((_, guardIndex) => guardIndex !== index),
    }))
    setGuardFieldFilters((prev) => prev.filter((_, guardIndex) => guardIndex !== index))
  }, [])

  const duplicateGuard = useCallback((index) => {
    setForm((prev) => {
      const source = prev.guards[index]
      if (!source) return prev
      return {
        ...prev,
        guards: [...prev.guards, { ...source, value_text: [] }],
      }
    })
    setGuardFieldFilters((prev) => [...prev, ''])
  }, [])

  const handleFieldChange = (field) => (input) => {
    let value = input
    if (input && typeof input === 'object' && 'target' in input) {
      const target = input.target
      value = target.type === 'checkbox' ? target.checked : target.value
    }
    setForm((prev) => ({ ...prev, [field]: value }))
  }

  const handleTriggerIndicatorChange = (indicatorId) => {
    updateTrigger({
      indicator_id: indicatorId || '',
      output_name: '',
      event_key: '',
    })
    if (indicatorId && typeof ensureIndicatorMeta === 'function') {
      ensureIndicatorMeta(indicatorId)
    }
  }

  const handleTriggerOutputChange = (outputName) => {
    updateTrigger({
      output_name: outputName || '',
      event_key: '',
    })
  }

  const handleTriggerEventChange = (eventKey) => {
    updateTrigger({ event_key: eventKey || '' })
  }

  const handleGuardTypeChange = (index, type) => {
    const nextType = type || 'ctx'
    updateGuard(index, {
      ...EMPTY_GUARD,
      type: nextType,
      variant: defaultVariantForType(nextType),
      field: nextType === 'ctx' ? 'state' : '',
    })
    setGuardFieldFilters((prev) => prev.map((entry, guardIndex) => (guardIndex === index ? '' : entry)))
  }

  const handleGuardVariantChange = useCallback((index, variant) => {
    updateGuard(index, {
      variant: variant || 'match',
      bars: '',
      event_key: '',
    })
  }, [updateGuard])

  const handleGuardIndicatorChange = (index, indicatorId) => {
    setForm((prev) => ({
      ...prev,
      guards: prev.guards.map((guard, guardIndex) => (
        guardIndex === index
          ? {
            ...guard,
            indicator_id: indicatorId || '',
            output_name: '',
            field: guard.type === 'ctx' ? 'state' : '',
            value_text: [],
            value: '',
            event_key: '',
          }
          : guard
      )),
    }))
    setGuardFieldFilters((prev) => prev.map((entry, guardIndex) => (guardIndex === index ? '' : entry)))
    if (indicatorId && typeof ensureIndicatorMeta === 'function') {
      ensureIndicatorMeta(indicatorId)
    }
  }

  const handleGuardOutputChange = (index, outputName) => {
    setForm((prev) => ({
      ...prev,
      guards: prev.guards.map((guard, guardIndex) => (
        guardIndex === index
          ? {
            ...guard,
            output_name: outputName || '',
            field: guard.type === 'ctx' ? 'state' : '',
            value_text: [],
            value: '',
            event_key: '',
          }
          : guard
      )),
    }))
    setGuardFieldFilters((prev) => prev.map((entry, guardIndex) => (guardIndex === index ? '' : entry)))
  }

  const handleGuardFieldChange = (index, field, value) => {
    updateGuard(index, { [field]: value })
  }

  const handleGuardFieldFilterChange = useCallback((index, value) => {
    setGuardFieldFilters((prev) => prev.map((entry, guardIndex) => (guardIndex === index ? value : entry)))
  }, [])

  const clearGuardFieldFilter = useCallback((index) => {
    setGuardFieldFilters((prev) => prev.map((entry, guardIndex) => (guardIndex === index ? '' : entry)))
  }, [])

  const buildPayload = () => {
    if (!canSubmit) return null
    const trigger = {
      type: 'signal_match',
      indicator_id: form.trigger.indicator_id,
      output_name: form.trigger.output_name,
      event_key: form.trigger.event_key,
    }
    const guards = (form.guards || []).map((guard) => {
      const contextValues = Array.isArray(guard.value_text) ? guard.value_text.filter(Boolean) : [guard.value_text].filter(Boolean)
      const numericValue = guard.value === '' ? null : Number(guard.value)
      const bars = Number(guard.bars)

      if (guard.type === 'ctx' && guard.variant === 'match') {
        if (!isGuardComplete(guard)) return null
        return {
          type: 'context_match',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          field: guard.field,
          value: contextValues,
        }
      }

      if (guard.type === 'ctx' && guard.variant === 'held') {
        if (!isGuardComplete(guard)) return null
        return {
          type: 'holds_for_bars',
          bars,
          guard: {
            type: 'context_match',
            indicator_id: guard.indicator_id,
            output_name: guard.output_name,
            field: guard.field,
            value: contextValues,
          },
        }
      }

      if (guard.type === 'metric' && guard.variant === 'match') {
        if (!isGuardComplete(guard)) return null
        return {
          type: 'metric_match',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          field: guard.field,
          operator: guard.operator,
          value: numericValue,
        }
      }

      if (guard.type === 'metric' && guard.variant === 'held') {
        if (!isGuardComplete(guard)) return null
        return {
          type: 'holds_for_bars',
          bars,
          guard: {
            type: 'metric_match',
            indicator_id: guard.indicator_id,
            output_name: guard.output_name,
            field: guard.field,
            operator: guard.operator,
            value: numericValue,
          },
        }
      }

      if (guard.type === 'signal' && guard.variant === 'seen') {
        if (!isGuardComplete(guard)) return null
        return {
          type: 'signal_seen_within_bars',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          event_key: guard.event_key,
          lookback_bars: bars,
        }
      }

      if (guard.type === 'signal' && guard.variant === 'absent') {
        if (!isGuardComplete(guard)) return null
        return {
          type: 'signal_absent_within_bars',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          event_key: guard.event_key,
          lookback_bars: bars,
        }
      }

      return null
    }).filter(Boolean)

    const resolvedName = form.name.trim() || getDefaultName?.({
      intent: form.intent,
      trigger,
      guards,
      indicatorLookup: indicatorMap,
    }) || 'Rule'

    return {
      name: resolvedName,
      description: form.description.trim() || null,
      intent: form.intent,
      priority: Number.isFinite(Number(form.priority)) ? Number(form.priority) : 0,
      trigger,
      guards,
      enabled: Boolean(form.enabled),
    }
  }

  return {
    form,
    indicatorMap,
    signalIndicators,
    guardFieldFilters,
    canSubmit,
    incompleteGuardIndexes,
    addGuard,
    duplicateGuard,
    removeGuard,
    buildPayload,
    handleFieldChange,
    handleTriggerIndicatorChange,
    handleTriggerOutputChange,
    handleTriggerEventChange,
    handleGuardTypeChange,
    handleGuardVariantChange,
    handleGuardIndicatorChange,
    handleGuardOutputChange,
    handleGuardFieldChange,
    handleGuardFieldFilterChange,
    clearGuardFieldFilter,
  }
}

export default useRuleForm
