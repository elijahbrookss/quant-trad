const normalizeSymbol = (rawSymbol) => {
  if (!rawSymbol) return ''
  const candidate = String(rawSymbol)
    .split(/[\s,;]+/)
    .map((token) => token.trim())
    .filter(Boolean)[0]
  if (!candidate) return ''
  return candidate
    .toUpperCase()
    .trim()
    .replace(/\s+/g, '')
    .replace(/,+$/, '')
    .replace(/^,/, '')
    .trim()
}

const parseSymbolInput = (input) => {
  if (!input) return []
  const parts = String(input)
    .split(/[\s,;]+/)
    .map((item) => normalizeSymbol(item))
    .filter(Boolean)
  const unique = []
  const seen = new Set()
  parts.forEach((symbol) => {
    const key = symbol.toUpperCase()
    if (seen.has(key)) return
    seen.add(key)
    unique.push(symbol)
  })
  return unique
}

const validateSymbolList = (symbols) => {
  const issues = {}
  const seen = new Set()
  symbols.forEach((symbol, index) => {
    const normalized = normalizeSymbol(symbol)
    if (!normalized) {
      issues[index] = 'Symbol is required.'
      return
    }
    if (!/^[A-Za-z0-9][A-Za-z0-9\-/]*$/.test(normalized)) {
      issues[index] = 'Use letters/numbers with - or / separators.'
      return
    }
    const key = normalized.toUpperCase()
    if (seen.has(key)) {
      issues[index] = 'Symbols must be unique.'
    } else {
      seen.add(key)
    }
  })
  return issues
}

export {
  normalizeSymbol,
  parseSymbolInput,
  validateSymbolList,
}
