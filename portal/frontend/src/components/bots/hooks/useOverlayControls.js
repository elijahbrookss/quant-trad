import { useEffect, useMemo, useState } from 'react'
import { BOTLENS_DEBUG } from '../chartDataUtils.js'

const toTitleCase = (value) => {
  if (!value) return ''
  return value
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(' ')
}

const resolveOverlayLabel = (overlay) => {
  if (!overlay) return 'Overlay'
  return overlay?.label || overlay?.ui?.label || toTitleCase(String(overlay.type || 'Overlay'))
}

const resolveOverlayColor = (overlay) => {
  return overlay?.color || overlay?.ui?.color || null
}

const isSuppressedOverlay = (overlay) => {
  const type = String(overlay?.type || '').trim().toLowerCase()
  const label = String(resolveOverlayLabel(overlay) || '').trim().toLowerCase()
  const tokens = `${type} ${label}`
  return /previous[\s_-]*zones?/.test(tokens)
}

export const resolveOverlayGroup = (overlay) => {
  const explicit = overlay?.ui?.group || overlay?.group
  const explicitGroup = String(explicit || '').trim().toLowerCase()
  if (explicitGroup === 'trade') return 'trade'
  if (explicitGroup === 'regime') return 'regime'
  if (explicitGroup === 'market' || explicitGroup === 'session') return 'market'
  if (explicitGroup === 'indicator' || explicitGroup === 'context') return 'indicator'

  const type = (overlay?.type || '').toString().toLowerCase()
  const label = resolveOverlayLabel(overlay).toLowerCase()
  const tokens = `${type} ${label}`

  if (tokens.includes('market_profile') || tokens.includes('market profile')) return 'market'
  if (tokens.includes('regime')) return 'regime'
  if (tokens.includes('atr') || tokens.includes('candle_stats') || tokens.includes('candle stats')) return 'indicator'

  const isTrade = ['trade', 'tp', 'sl', 'stop', 'target', 'ray', 'leg', 'exit', 'entry'].some((token) =>
    type.includes(token),
  )
  if (isTrade) return 'trade'

  return 'indicator'
}

export const useOverlayControls = ({ overlays = [], extraOptions = [] } = {}) => {
  const [visibility, setVisibility] = useState({})

  const overlayOptions = useMemo(() => {
    const seen = new Map()
    const push = (overlay) => {
      if (isSuppressedOverlay(overlay)) return
      const type = overlay?.type
      if (!type || seen.has(type)) return
      seen.set(type, {
        type,
        label: resolveOverlayLabel(overlay),
        color: resolveOverlayColor(overlay),
        group: resolveOverlayGroup(overlay),
        defaultVisible: overlay?.ui?.default_visible ?? null,
      })
    }
    for (const overlay of overlays) push(overlay)
    for (const option of extraOptions) push(option)
    const options = Array.from(seen.values())
    if (BOTLENS_DEBUG) {
      const labels = options.map((o) => `${o.type}:${o.label}`).join(', ')
      console.debug('[BotLens] overlay options resolved', { count: options.length, labels })
    }
    return options
  }, [extraOptions, overlays])

  useEffect(() => {
    if (!overlayOptions.length) {
      setVisibility((prev) => {
        if (!prev || Object.keys(prev).length === 0) {
          return prev
        }
        return {}
      })
      return
    }
    setVisibility((prev) => {
      const next = { ...prev }
      let changed = false
      for (const option of overlayOptions) {
        if (!(option.type in next)) {
          next[option.type] = option.defaultVisible ?? true
          changed = true
        }
      }
      for (const key of Object.keys(next)) {
        if (!overlayOptions.some((option) => option.type === key)) {
          delete next[key]
          changed = true
        }
      }
      return changed ? next : prev
    })
  }, [overlayOptions])

  const visibleOverlays = useMemo(() => {
    const visible = overlays.filter((overlay) => {
      if (isSuppressedOverlay(overlay)) return false
      const type = overlay?.type
      if (!type) return false
      return visibility[type] !== false
    })
    if (BOTLENS_DEBUG) {
      const summary = visible.reduce((acc, ov) => {
        const key = ov?.type || 'unknown'
        acc[key] = (acc[key] || 0) + 1
        return acc
      }, {})
      console.debug('[BotLens] visible overlays', { summary })
    }
    return visible
  }, [overlays, visibility])

  const toggleOverlay = (type) => {
    if (!type) return
    setVisibility((prev) => ({
      ...prev,
      [type]: prev[type] === false,
    }))
    if (BOTLENS_DEBUG) {
      console.debug('[BotLens] overlay toggled', { type })
    }
  }

  return {
    overlayOptions,
    visibility,
    visibleOverlays,
    toggleOverlay,
  }
}
