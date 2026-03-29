/**
 * Validation utilities for forms and data integrity.
 */

/**
 * Validate ATM template data.
 * @param {Object} template - ATM template object
 * @returns {Object} Validation errors object (empty if valid)
 */
export function validateATMTemplate(template) {
  const errors = {}

  // Validate template name
  if (!template.name || String(template.name).trim() === '') {
    errors.name = 'Template name is required'
  }

  // Validate stop R multiple (must be positive)
  if (template.stop_r_multiple != null) {
    const stopValue = Number(template.stop_r_multiple)
    if (!Number.isFinite(stopValue) || stopValue <= 0) {
      errors.stop_r_multiple = 'Stop distance must be positive'
    }
  }

  // Validate take profit targets
  if (Array.isArray(template.take_profit_orders)) {
    const targets = template.take_profit_orders

    // Validate R multiples are positive
    targets.forEach((target, idx) => {
      if (target.r_multiple != null) {
        const rValue = Number(target.r_multiple)
        if (!Number.isFinite(rValue) || rValue <= 0) {
          errors[`target_${idx}_r_multiple`] = 'R multiple must be positive'
        }
      }
    })

    // Validate sizes sum to 100% (if all have sizes)
    const allHaveSizes = targets.every(t => t.size_fraction != null || t.size_percent != null)
    if (allHaveSizes && targets.length > 0) {
      const totalPercent = targets.reduce((sum, target) => {
        const percent = target.size_percent ?? (target.size_fraction ?? 0) * 100
        return sum + percent
      }, 0)

      if (Math.abs(totalPercent - 100) > 0.1) {
        errors.target_sizes = `Target sizes must sum to 100% (currently ${totalPercent.toFixed(1)}%)`
      }
    }
  }

  return errors
}

/**
 * Validate strategy data.
 * @param {Object} strategy - Strategy object
 * @returns {Object} Validation errors object (empty if valid)
 */
export function validateStrategy(strategy) {
  const errors = {}

  // Validate name
  if (!strategy.name || String(strategy.name).trim() === '') {
    errors.name = 'Strategy name is required'
  }

  // Validate timeframe
  if (!strategy.timeframe || String(strategy.timeframe).trim() === '') {
    errors.timeframe = 'Timeframe is required'
  }

  // Validate instruments
  if (!Array.isArray(strategy.instrument_slots) || strategy.instrument_slots.length === 0) {
    errors.instrument_slots = 'At least one symbol is required'
  } else {
    const hasSymbol = strategy.instrument_slots.some((slot) => Boolean(slot?.symbol))
    if (!hasSymbol) {
      errors.instrument_slots = 'At least one symbol is required'
    }
  }

  // Validate risk parameters if present
  if (strategy.base_risk_per_trade != null) {
    const risk = Number(strategy.base_risk_per_trade)
    if (!Number.isFinite(risk) || risk <= 0) {
      errors.base_risk_per_trade = 'Base risk must be a positive number'
    }
  }

  if (strategy.global_risk_multiplier != null) {
    const multiplier = Number(strategy.global_risk_multiplier)
    if (!Number.isFinite(multiplier) || multiplier <= 0) {
      errors.global_risk_multiplier = 'Risk multiplier must be a positive number'
    }
  }

  return errors
}

/**
 * Validate rule data.
 * @param {Object} rule - Rule object
 * @returns {Object} Validation errors object (empty if valid)
 */
export function validateRule(rule) {
  const errors = {}

  if (!rule.name || String(rule.name).trim() === '') {
    errors.name = 'Rule name is required'
  }

  if (!rule.intent || String(rule.intent).trim() === '') {
    errors.intent = 'Intent is required'
  }

  const trigger = rule?.trigger && typeof rule.trigger === 'object' ? rule.trigger : null
  if (!trigger) {
    errors.trigger = 'Trigger is required'
  } else {
    if (!trigger.indicator_id) {
      errors.trigger_indicator_id = 'Trigger indicator is required'
    }
    if (!trigger.output_name) {
      errors.trigger_output_name = 'Trigger output is required'
    }
    if (!trigger.event_key) {
      errors.trigger_event_key = 'Trigger event is required'
    }
  }

  const guards = Array.isArray(rule.guards) ? rule.guards : []
  if (guards.length > 2) {
    errors.guards = 'At most two guards are supported in v1'
  }
  guards.forEach((guard, idx) => {
    if (!guard?.indicator_id) {
      errors[`guard_${idx}_indicator`] = 'Guard indicator is required'
    }
    if (!guard?.output_name) {
      errors[`guard_${idx}_output`] = 'Guard output is required'
    }
    if (guard?.type === 'context_match') {
      if (!guard.field) {
        errors[`guard_${idx}_field`] = 'Context field is required'
      }
      if (String(guard.value ?? '').trim() === '') {
        errors[`guard_${idx}_value`] = 'Context value is required'
      }
      return
    }
    if (guard?.type === 'metric_match') {
      if (!guard.field) {
        errors[`guard_${idx}_field`] = 'Metric field is required'
      }
      if (!guard.operator) {
        errors[`guard_${idx}_operator`] = 'Metric operator is required'
      }
      const value = Number(guard.value)
      if (!Number.isFinite(value)) {
        errors[`guard_${idx}_value`] = 'Metric value must be numeric'
      }
    }
  })

  return errors
}

/**
 * Validate instrument metadata.
 * @param {Object} instrument - Instrument object
 * @returns {Object} Validation errors object (empty if valid)
 */
export function validateInstrument(instrument) {
  const errors = {}

  if (!instrument.symbol || String(instrument.symbol).trim() === '') {
    errors.symbol = 'Symbol is required'
  }

  if (instrument.tick_size != null) {
    const tickSize = Number(instrument.tick_size)
    if (!Number.isFinite(tickSize) || tickSize <= 0) {
      errors.tick_size = 'Tick size must be a positive number'
    }
  }

  if (instrument.min_order_size != null) {
    const minSize = Number(instrument.min_order_size)
    if (!Number.isFinite(minSize) || minSize <= 0) {
      errors.min_order_size = 'Minimum order size must be a positive number'
    }
  }

  // Validate fee rates (must be decimals, not percentages)
  if (instrument.maker_fee_rate != null) {
    const makerFee = Number(instrument.maker_fee_rate)
    if (!Number.isFinite(makerFee)) {
      errors.maker_fee_rate = 'Maker fee must be a valid number'
    } else if (makerFee < 0) {
      errors.maker_fee_rate = 'Maker fee cannot be negative'
    } else if (makerFee > 0.01) {
      // Warning: fees > 1% are extremely high
      errors.maker_fee_rate = `Maker fee ${(makerFee * 100).toFixed(2)}% is unusually high. Typical fees are 0.01-0.10%. Enter as decimal (e.g., 0.0004 for 0.04%)`
    }
  }

  if (instrument.taker_fee_rate != null) {
    const takerFee = Number(instrument.taker_fee_rate)
    if (!Number.isFinite(takerFee)) {
      errors.taker_fee_rate = 'Taker fee must be a valid number'
    } else if (takerFee < 0) {
      errors.taker_fee_rate = 'Taker fee cannot be negative'
    } else if (takerFee > 0.01) {
      // Warning: fees > 1% are extremely high
      errors.taker_fee_rate = `Taker fee ${(takerFee * 100).toFixed(2)}% is unusually high. Typical fees are 0.01-0.10%. Enter as decimal (e.g., 0.0006 for 0.06%)`
    }
  }

  return errors
}

/**
 * Check if an object has any validation errors.
 * @param {Object} errors - Errors object from validation function
 * @returns {boolean} True if there are errors
 */
export function hasErrors(errors) {
  return Object.keys(errors).length > 0
}
