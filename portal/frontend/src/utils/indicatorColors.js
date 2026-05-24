const normalizeColorMode = (value) => {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : ''
  return normalized === 'palette' ? 'palette' : 'single'
}

export const getIndicatorColorMode = (indicatorOrMeta) => (
  normalizeColorMode(indicatorOrMeta?.color_mode ?? indicatorOrMeta?.manifest?.color_mode)
)

export const getIndicatorColorPalettes = (indicatorOrMeta) => {
  const palettes = indicatorOrMeta?.color_palettes ?? indicatorOrMeta?.manifest?.color_palettes
  return Array.isArray(palettes) ? palettes : []
}

export const getSelectedIndicatorPalette = (indicatorOrMeta) => {
  const palettes = getIndicatorColorPalettes(indicatorOrMeta)
  if (!palettes.length) return null
  const selectedKey = typeof indicatorOrMeta?.color_palette === 'string' ? indicatorOrMeta.color_palette.trim() : ''
  return palettes.find((palette) => palette?.key === selectedKey) || palettes[0] || null
}

export const supportsCustomIndicatorColor = (indicatorOrMeta) => (
  getIndicatorColorMode(indicatorOrMeta) === 'single'
)

export const usesIndicatorPalette = (indicatorOrMeta) => (
  getIndicatorColorMode(indicatorOrMeta) === 'palette'
)

export const supportsIndicatorPaletteSelection = (indicatorOrMeta) => (
  usesIndicatorPalette(indicatorOrMeta) && getIndicatorColorPalettes(indicatorOrMeta).length > 0
)

export const getIndicatorSignalColor = (indicatorOrMeta) => {
  const palette = getSelectedIndicatorPalette(indicatorOrMeta)
  const paletteSignalColor = typeof palette?.signal_color === 'string' && palette.signal_color.trim()
    ? palette.signal_color.trim()
    : null
  if (paletteSignalColor) return paletteSignalColor
  const color = typeof indicatorOrMeta?.color === 'string' && indicatorOrMeta.color.trim()
    ? indicatorOrMeta.color.trim()
    : null
  return color
}

export const getPaletteOverlayColor = (indicatorOrMeta, overlayType) => {
  const palette = getSelectedIndicatorPalette(indicatorOrMeta)
  if (!palette || !overlayType) return null
  const overlayColors = palette?.overlay_colors
  if (!overlayColors || typeof overlayColors !== 'object') return null
  const color = overlayColors[overlayType]
  return typeof color === 'string' && color.trim() ? color.trim() : null
}
