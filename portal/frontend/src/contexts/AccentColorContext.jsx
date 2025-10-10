import { createContext, useContext, useEffect, useMemo, useState } from 'react'

// extremely/really dark purple (in the middle)
const DEFAULT_ACCENT = '#175a81ff'

const AccentColorContext = createContext({
  accentColor: DEFAULT_ACCENT,
  setAccentColor: () => {},
})

function normalizeHex(value) {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!/^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(trimmed)) {
    return null
  }
  if (trimmed.length === 4) {
    const [, r, g, b] = trimmed
    return `#${r}${r}${g}${g}${b}${b}`.toLowerCase()
  }
  return trimmed.toLowerCase()
}

function hexToRgbArray(hex) {
  const normalized = normalizeHex(hex)
  if (!normalized) return null
  const value = normalized.slice(1)
  const r = parseInt(value.slice(0, 2), 16)
  const g = parseInt(value.slice(2, 4), 16)
  const b = parseInt(value.slice(4, 6), 16)
  return [r, g, b]
}

function lighten(rgb, amount) {
  return rgb.map((channel) => Math.round(channel + (255 - channel) * amount))
}

function toRgbString(rgb) {
  return `rgb(${rgb.join(', ')})`
}

function toRgbaString(rgb, alpha) {
  return `rgba(${rgb.join(', ')}, ${alpha})`
}

function buildPalette(rgb) {
  const textStrong = lighten(rgb, 0.75)
  const textSoft = lighten(rgb, 0.6)
  const textMuted = lighten(rgb, 0.55)
  const textKicker = lighten(rgb, 0.4)
  const textBright = lighten(rgb, 0.85)

  return {
    '--accent-rgb': rgb.join(' '),
    '--accent-base': toRgbString(rgb),
    '--accent-alpha-05': toRgbaString(rgb, 0.05),
    '--accent-alpha-10': toRgbaString(rgb, 0.1),
    '--accent-alpha-15': toRgbaString(rgb, 0.15),
    '--accent-alpha-20': toRgbaString(rgb, 0.2),
    '--accent-alpha-25': toRgbaString(rgb, 0.25),
    '--accent-alpha-30': toRgbaString(rgb, 0.3),
    '--accent-alpha-40': toRgbaString(rgb, 0.4),
    '--accent-alpha-60': toRgbaString(rgb, 0.6),
    '--accent-alpha-70': toRgbaString(rgb, 0.7),
    '--accent-alpha-80': toRgbaString(rgb, 0.8),
    '--accent-alpha-85': toRgbaString(rgb, 0.85),
    '--accent-alpha-90': toRgbaString(rgb, 0.9),
    '--accent-text-strong': toRgbString(textStrong),
    '--accent-text-strong-alpha': toRgbaString(textStrong, 0.9),
    '--accent-text-soft': toRgbString(textSoft),
    '--accent-text-soft-alpha': toRgbaString(textSoft, 0.85),
    '--accent-text-muted': toRgbaString(textMuted, 0.7),
    '--accent-text-kicker': toRgbaString(textKicker, 0.8),
    '--accent-text-bright': toRgbString(textBright),
    '--accent-outline': toRgbaString(textSoft, 0.7),
    '--accent-outline-soft': toRgbaString(textSoft, 0.45),
    '--accent-ring': toRgbaString(rgb, 0.45),
    '--accent-ring-strong': toRgbaString(rgb, 0.7),
    '--accent-shadow-soft': toRgbaString(rgb, 0.3),
    '--accent-shadow-strong': toRgbaString(rgb, 0.55),
    '--accent-gradient-spot': toRgbaString(rgb, 0.14),
  }
}

function applyPalette(palette) {
  if (typeof document === 'undefined') return
  const root = document.documentElement
  Object.entries(palette).forEach(([key, value]) => {
    root.style.setProperty(key, value)
  })
}

export function AccentColorProvider({ defaultColor = DEFAULT_ACCENT, children }) {
  const [accentColor, setAccentColorState] = useState(() => {
    const normalized = normalizeHex(defaultColor)
    return normalized || DEFAULT_ACCENT
  })

  useEffect(() => {
    const rgb = hexToRgbArray(accentColor)
    if (!rgb) return
    const palette = buildPalette(rgb)
    applyPalette(palette)
  }, [accentColor])

  const value = useMemo(() => ({
    accentColor,
    setAccentColor: (next) => {
      const rgb = hexToRgbArray(next)
      if (!rgb) return
      const normalized = normalizeHex(next)
      if (!normalized) return
      setAccentColorState(normalized)
    },
  }), [accentColor])

  return (
    <AccentColorContext.Provider value={value}>
      {children}
    </AccentColorContext.Provider>
  )
}

export function useAccentColor() {
  return useContext(AccentColorContext)
}

export function setGlobalAccentColor(color) {
  const rgb = hexToRgbArray(color)
  if (!rgb) return false
  const palette = buildPalette(rgb)
  applyPalette(palette)
  return true
}

