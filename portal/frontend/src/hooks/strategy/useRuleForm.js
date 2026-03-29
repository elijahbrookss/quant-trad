import { useCallback, useEffect, useMemo, useState } from 'react'

import { RULE_FORM_DEFAULT } from '../../utils/strategy/formDefaults.js'
import { extractRuleFlow } from '../../components/strategy/rules/ruleUtils.js'
import { indicatorHasAuthorableOutputs } from '../../utils/indicatorOutputs.js'

const EMPTY_GUARD = {
  type: 'context_match',
  indicator_id: '',
  output_name: '',
  field: '',
  value_text: '',
  operator: '>',
  value: '',
}

const useRuleForm = ({
  open,
  indicators,
  ensureIndicatorMeta,
  initialValues,
  getDefaultName,
} = {}) => {
  const [form, setForm] = useState(RULE_FORM_DEFAULT)

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
      return
    }
    if (initialValues) {
      const flow = extractRuleFlow(initialValues)
      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        intent: initialValues.intent || (initialValues.action === 'sell' ? 'enter_short' : 'enter_long'),
        priority: Number.isFinite(Number(initialValues.priority)) ? Number(initialValues.priority) : 0,
        trigger: {
          indicator_id: flow.trigger?.indicator_id || '',
          output_name: flow.trigger?.output_name || '',
          event_key: flow.trigger?.event_key || '',
        },
        guards: Array.isArray(flow.guards) ? flow.guards.slice(0, 2).map((guard) => ({
          ...EMPTY_GUARD,
          ...guard,
          value: guard?.value ?? '',
          value_text: guard?.type === 'context_match' ? String(guard?.value ?? '') : '',
        })) : [],
        enabled: Boolean(initialValues.enabled),
      })
      return
    }
    setForm({ ...RULE_FORM_DEFAULT })
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
      if ((prev.guards || []).length >= 2) {
        return prev
      }
      return {
        ...prev,
        guards: [...(prev.guards || []), { ...EMPTY_GUARD }],
      }
    })
  }, [])

  const removeGuard = useCallback((index) => {
    setForm((prev) => ({
      ...prev,
      guards: prev.guards.filter((_, guardIndex) => guardIndex !== index),
    }))
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
    updateGuard(index, {
      ...EMPTY_GUARD,
      type: type || 'context_match',
    })
  }

  const handleGuardIndicatorChange = (index, indicatorId) => {
    updateGuard(index, {
      indicator_id: indicatorId || '',
      output_name: '',
      field: '',
      value_text: '',
      value: '',
    })
    if (indicatorId && typeof ensureIndicatorMeta === 'function') {
      ensureIndicatorMeta(indicatorId)
    }
  }

  const handleGuardOutputChange = (index, outputName) => {
    updateGuard(index, {
      output_name: outputName || '',
      field: '',
      value_text: '',
      value: '',
    })
  }

  const handleGuardFieldChange = (index, field, value) => {
    updateGuard(index, { [field]: value })
  }

  const buildPayload = () => {
    if (!canSubmit) return null
    const trigger = {
      type: 'signal_match',
      indicator_id: form.trigger.indicator_id,
      output_name: form.trigger.output_name,
      event_key: form.trigger.event_key,
    }
    const guards = (form.guards || []).map((guard) => {
      if (guard.type === 'context_match') {
        return {
          type: 'context_match',
          indicator_id: guard.indicator_id,
          output_name: guard.output_name,
          field: guard.field || 'state',
          value: String(guard.value_text || '').trim(),
        }
      }
      return {
        type: 'metric_match',
        indicator_id: guard.indicator_id,
        output_name: guard.output_name,
        field: guard.field,
        operator: guard.operator,
        value: guard.value === '' ? null : Number(guard.value),
      }
    }).filter((guard) => {
      if (guard.type === 'context_match') {
        return guard.indicator_id && guard.output_name && guard.field && guard.value
      }
      return guard.indicator_id && guard.output_name && guard.field && guard.operator && guard.value !== null && !Number.isNaN(guard.value)
    })

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
    canSubmit,
    addGuard,
    removeGuard,
    buildPayload,
    handleFieldChange,
    handleTriggerIndicatorChange,
    handleTriggerOutputChange,
    handleTriggerEventChange,
    handleGuardTypeChange,
    handleGuardIndicatorChange,
    handleGuardOutputChange,
    handleGuardFieldChange,
  }
}

export default useRuleForm
