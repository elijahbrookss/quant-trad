import { DEFAULT_ATM_TEMPLATE, cloneATMTemplate } from '../../components/atm/ATMConfigForm.jsx'

const templateKey = (template) => {
  try {
    return JSON.stringify(template || {})
  } catch (err) {
    console.error('Failed to stringify ATM template', err)
    return ''
  }
}

const validateATMTemplate = (template) => {
  const errors = {}
  const normalized = cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE)
  const templateName = (normalized.name || '').trim()
  if (!templateName) {
    errors.name = 'Template name is required.'
  }

  const stopValue = normalized.stop_r_multiple
  const stopNumeric = Number(stopValue)
  if (stopValue === null || stopValue === undefined || Number.isNaN(stopNumeric)) {
    errors.stop_r_multiple = 'Enter a positive stop distance in R.'
  } else if (stopNumeric <= 0) {
    errors.stop_r_multiple = 'Stop distance must be positive.'
  }

  const targets = Array.isArray(normalized.take_profit_orders) ? normalized.take_profit_orders : []
  if (targets.length) {
    const total = targets.reduce((sum, target) => {
      const numeric = Number(target.size_percent)
      return Number.isFinite(numeric) ? sum + numeric : sum
    }, 0)
    if (Math.abs(total - 100) > 0.001) {
      errors.take_profit_orders = `Allocation must total 100%. Current: ${Math.round(total)}%.`
    }
  }

  return errors
}

const stripInstrumentTemplateFields = (template) => {
  if (!template) return template
  const cleaned = { ...template }
  const fields = [
    'tick_size',
    'tick_value',
    'contract_size',
    'maker_fee_rate',
    'taker_fee_rate',
    'quote_currency',
  ]
  fields.forEach((field) => {
    delete cleaned[field]
  })
  if (cleaned._meta && typeof cleaned._meta === 'object') {
    const meta = { ...cleaned._meta }
    fields.forEach((field) => {
      delete meta[`${field}_override`]
    })
    if (Object.keys(meta).length === 0) {
      delete cleaned._meta
    } else {
      cleaned._meta = meta
    }
  }
  return cleaned
}

const cloneATMTemplateSafe = (template) => cloneATMTemplate(template || DEFAULT_ATM_TEMPLATE)

export {
  templateKey,
  validateATMTemplate,
  stripInstrumentTemplateFields,
  cloneATMTemplateSafe,
}
