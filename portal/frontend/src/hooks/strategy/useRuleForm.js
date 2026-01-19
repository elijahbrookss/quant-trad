import { useCallback, useEffect, useMemo, useState } from 'react'

import { RULE_FORM_DEFAULT } from '../../utils/strategy/formDefaults.js'

const useRuleForm = ({ open, indicators, ensureIndicatorMeta, initialValues, onSubmit } = {}) => {
  const [form, setForm] = useState(RULE_FORM_DEFAULT)

  const indicatorMap = useMemo(() => {
    const map = new Map()
    for (const indicator of indicators || []) {
      map.set(indicator.id, indicator)
    }
    return map
  }, [indicators])

  const makeEmptyCondition = useCallback(
    () => ({ indicator_id: '', rule_id: '', signal_type: '', direction: '' }),
    [],
  )

  useEffect(() => {
    if (!open) {
      setForm(RULE_FORM_DEFAULT)
      return
    }

    if (initialValues) {
      const mappedConditions = Array.isArray(initialValues.conditions)
        ? initialValues.conditions.map((condition) => ({
            indicator_id: condition.indicator_id || '',
            rule_id: condition.rule_id || '',
            signal_type: condition.signal_type || '',
            direction: condition.direction || '',
          }))
        : []

      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        action: initialValues.action || 'buy',
        match: initialValues.match || 'all',
        conditions: mappedConditions.length ? mappedConditions : [makeEmptyCondition()],
        enabled: Boolean(initialValues.enabled),
      })
    } else {
      setForm({ ...RULE_FORM_DEFAULT, conditions: [makeEmptyCondition()] })
    }
  }, [open, initialValues, makeEmptyCondition])

  const trackedIndicatorIds = useMemo(
    () =>
      Array.from(
        new Set(
          (form.conditions || [])
            .map((condition) => condition.indicator_id)
            .filter((indicatorId) => typeof indicatorId === 'string' && indicatorId.trim().length > 0),
        ),
      ),
    [form.conditions],
  )

  useEffect(() => {
    if (!open || typeof ensureIndicatorMeta !== 'function' || !trackedIndicatorIds.length) {
      return
    }
    trackedIndicatorIds.forEach((indicatorId) => {
      ensureIndicatorMeta(indicatorId)
    })
  }, [open, trackedIndicatorIds, ensureIndicatorMeta])

  const canSubmit = form.conditions.some(
    (condition) => condition.indicator_id && condition.signal_type,
  )

  const updateCondition = (index, updates) => {
    setForm((prev) => {
      const nextConditions = prev.conditions.map((condition, idx) =>
        idx === index ? { ...condition, ...updates } : condition,
      )
      return { ...prev, conditions: nextConditions }
    })
  }

  const handleConditionIndicatorChange = (index) => (indicatorId) => {
    updateCondition(index, {
      indicator_id: indicatorId || '',
      rule_id: '',
      signal_type: '',
      direction: '',
    })
    if (indicatorId && typeof ensureIndicatorMeta === 'function') {
      ensureIndicatorMeta(indicatorId)
    }
  }

  const handleConditionRuleChange = (index) => (ruleId) => {
    setForm((prev) => {
      const nextConditions = [...prev.conditions]
      const current = nextConditions[index]
      const indicatorMeta = indicatorMap.get(current.indicator_id)
      const rules = Array.isArray(indicatorMeta?.signal_rules) ? indicatorMeta.signal_rules : []
      const selectedRule = rules.find((rule) => rule.id === ruleId)
      const defaultDirection = Array.isArray(selectedRule?.directions) && selectedRule.directions.length === 1
        ? selectedRule.directions[0].id
        : ''
      nextConditions[index] = {
        ...current,
        rule_id: ruleId || '',
        signal_type: selectedRule?.signal_type || '',
        direction: defaultDirection || '',
      }
      return { ...prev, conditions: nextConditions }
    })
  }

  const handleConditionDirectionChange = (index) => (direction) => {
    updateCondition(index, { direction: direction || '' })
  }

  const addCondition = () => {
    setForm((prev) => ({
      ...prev,
      conditions: [...prev.conditions, makeEmptyCondition()],
    }))
  }

  const removeCondition = (index) => {
    setForm((prev) => {
      const nextConditions = prev.conditions.filter((_, idx) => idx !== index)
      return {
        ...prev,
        conditions: nextConditions.length ? nextConditions : [makeEmptyCondition()],
      }
    })
  }

  const handleFieldChange = (field) => (input) => {
    let value = input
    if (input && typeof input === 'object' && 'target' in input) {
      const target = input.target
      if (target.type === 'checkbox') {
        value = target.checked
      } else {
        value = target.value
      }
    }
    setForm((prev) => ({ ...prev, [field]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const conditions = form.conditions
      .map((condition) => ({
        indicator_id: condition.indicator_id,
        signal_type: condition.signal_type,
        rule_id: condition.rule_id || null,
        direction: condition.direction || null,
      }))
      .filter((condition) => condition.indicator_id && condition.signal_type)

    if (!conditions.length) {
      return
    }

    const payload = {
      name: form.name.trim(),
      description: form.description.trim() || null,
      action: form.action,
      match: form.match,
      conditions,
      enabled: Boolean(form.enabled),
    }
    await onSubmit(payload)
  }

  return {
    form,
    indicatorMap,
    canSubmit,
    handleSubmit,
    handleFieldChange,
    addCondition,
    removeCondition,
    handleConditionIndicatorChange,
    handleConditionRuleChange,
    handleConditionDirectionChange,
  }
}

export default useRuleForm
