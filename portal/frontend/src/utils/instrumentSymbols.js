export const symbolsFromInstrumentSlots = (slots = []) => {
  if (!Array.isArray(slots)) return []
  const seen = new Set()
  const symbols = []
  for (const slot of slots) {
    const raw = typeof slot?.symbol === 'string' ? slot.symbol.trim() : ''
    if (!raw) continue
    const key = raw.toUpperCase()
    if (seen.has(key)) continue
    seen.add(key)
    symbols.push(raw)
  }
  return symbols
}

export const symbolsFromInstruments = (instruments = []) => {
  if (!Array.isArray(instruments)) return []
  const seen = new Set()
  const symbols = []
  for (const instrument of instruments) {
    const raw = typeof instrument?.symbol === 'string' ? instrument.symbol.trim() : ''
    if (!raw) continue
    const key = raw.toUpperCase()
    if (seen.has(key)) continue
    seen.add(key)
    symbols.push(raw)
  }
  return symbols
}
