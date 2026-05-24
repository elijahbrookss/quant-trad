export const PANE_DEFINITIONS = Object.freeze({
  price: Object.freeze({
    key: 'price',
    index: 0,
    stretchFactor: 0.76,
    label: 'Price',
    isMain: true,
    showLegend: false,
  }),
  volatility: Object.freeze({
    key: 'volatility',
    index: 1,
    stretchFactor: 0.18,
    label: 'Volatility',
    isMain: false,
    showLegend: true,
  }),
  oscillator: Object.freeze({
    key: 'oscillator',
    index: 2,
    stretchFactor: 0.14,
    label: 'Oscillator',
    isMain: false,
    showLegend: true,
  }),
})

export const normalizePaneKey = (paneKey) => {
  const normalized = String(paneKey || 'price').trim().toLowerCase()
  return normalized || 'price'
}

export const getPaneDefinition = (paneKey) => {
  const normalized = normalizePaneKey(paneKey)
  return PANE_DEFINITIONS[normalized] || PANE_DEFINITIONS.price
}

export const listPaneDefinitions = () => Object.values(PANE_DEFINITIONS)

export const collectActivePaneKeys = (...collections) => {
  const paneKeys = new Set(['price'])
  collections.forEach((collection) => {
    Object.keys(collection || {}).forEach((paneKey) => {
      paneKeys.add(normalizePaneKey(paneKey))
    })
  })
  return paneKeys
}
