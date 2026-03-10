import { normalizeSymbol } from './symbolValidation.js'

const slotId = () => (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2))

const newSlot = (symbol = '') => ({
  uid: slotId(),
  symbol,
  enabled: true,
  risk_multiplier: '',
  metadata: {},
})

const normaliseSlot = (value, index = 0) => {
  if (!value) return newSlot()
  const symbol = normalizeSymbol(typeof value === 'object' ? value.symbol : value)
  const riskValue =
    value && typeof value === 'object' && value.risk_multiplier !== undefined && value.risk_multiplier !== null
      ? String(value.risk_multiplier)
      : ''
  const metadata =
    value && typeof value === 'object' && value.metadata && typeof value.metadata === 'object'
      ? { ...value.metadata }
      : {}
  return {
    ...newSlot(symbol),
    enabled: value && typeof value === 'object' ? Boolean(value.enabled) : true,
    risk_multiplier: riskValue,
    metadata,
    uid: `${Date.now()}-${index}-${Math.random().toString(36).slice(2)}`,
  }
}

const inflateSlots = (rawSlots) => {
  const list = Array.isArray(rawSlots) ? rawSlots : []
  const mapped = list.map((slot, index) => normaliseSlot(slot, index))
  const cleaned = mapped.filter((slot, index) => slot.symbol || index === 0)
  return cleaned.length ? cleaned : [newSlot('')]
}

export {
  slotId,
  newSlot,
  normaliseSlot,
  inflateSlots,
}
