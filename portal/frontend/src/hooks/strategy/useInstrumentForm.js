import { useEffect, useState } from 'react'

import { INSTRUMENT_FORM_DEFAULT } from '../../utils/strategy/formDefaults.js'

const useInstrumentForm = ({ open, initialValues, onSubmit } = {}) => {
  const [form, setForm] = useState(INSTRUMENT_FORM_DEFAULT)
  const [localError, setLocalError] = useState(null)

  useEffect(() => {
    if (!open) {
      setForm(INSTRUMENT_FORM_DEFAULT)
      setLocalError(null)
      return
    }
    const seed = { ...(initialValues || {}) }
    if (seed.expiry_ts) {
      const parsed = new Date(seed.expiry_ts)
      seed.expiry_ts = Number.isNaN(parsed.valueOf()) ? '' : parsed.toISOString().slice(0, 16)
    }
    seed.can_short = Boolean(seed.can_short)
    seed.has_funding = Boolean(seed.has_funding)
    setForm({
      ...INSTRUMENT_FORM_DEFAULT,
      ...seed,
    })
    setLocalError(null)
  }, [open, initialValues])

  const handleChange = (field) => (event) => {
    const value = event?.target ? event.target.value : event
    setForm((prev) => ({ ...prev, [field]: value ?? '' }))
  }

  const handleToggle = (field) => (event) => {
    const checked = event?.target ? event.target.checked : Boolean(event)
    setForm((prev) => ({ ...prev, [field]: checked }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const providerId = (form.provider_id || form.datasource || '').trim().toUpperCase()
    const venueId = (form.venue_id || form.exchange || '').trim()
    const payload = {
      symbol: (form.symbol || '').trim().toUpperCase(),
      provider_id: providerId || null,
      venue_id: venueId ? venueId.toUpperCase() : null,
      datasource: providerId || null,
      exchange: venueId || null,
      instrument_type: (form.instrument_type || '').trim().toLowerCase() || null,
      base_currency: (form.base_currency || '').trim().toUpperCase() || null,
      quote_currency: (form.quote_currency || '').trim().toUpperCase() || null,
      can_short: Boolean(form.can_short),
      short_requires_borrow: Boolean(form.short_requires_borrow),
      has_funding: Boolean(form.has_funding),
      expiry_ts: form.expiry_ts ? new Date(form.expiry_ts).toISOString() : null,
    }
    const numericFields = [
      'tick_size',
      'tick_value',
      'contract_size',
      'min_order_size',
      'maker_fee_rate',
      'taker_fee_rate',
    ]
    numericFields.forEach((field) => {
      const parsed = parseFloat(form[field])
      payload[field] = Number.isFinite(parsed) ? parsed : null
    })

    if (!payload.symbol) {
      setLocalError('Symbol is required for instrument metadata')
      return
    }
    if (payload.tick_size == null && payload.tick_value == null) {
      setLocalError('Provide at least a tick size or tick value')
      return
    }
    setLocalError(null)
    await onSubmit?.(payload)
  }

  return {
    form,
    localError,
    handleChange,
    handleToggle,
    handleSubmit,
  }
}

export default useInstrumentForm
