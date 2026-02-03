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
  return overlay?.ui?.label || toTitleCase(String(overlay.type || 'Overlay'))
}

const resolveOverlayColor = (overlay) => {
  return overlay?.ui?.color || null
}

export const useOverlayControls = ({ overlays = [] } = {}) => {
  const [visibility, setVisibility] = useState({})

  const overlayOptions = useMemo(() => {
    const seen = new Map()
    for (const overlay of overlays) {
      const type = overlay?.type
      if (!type || seen.has(type)) continue
      seen.set(type, {
        type,
        label: resolveOverlayLabel(overlay),
        color: resolveOverlayColor(overlay),
        defaultVisible: overlay?.ui?.default_visible ?? null,
      })
    }
    const options = Array.from(seen.values())
    if (BOTLENS_DEBUG) {
      const labels = options.map((o) => `${o.type}:${o.label}`).join(', ')
      console.debug('[BotLens] overlay options resolved', { count: options.length, labels })
    }
    return options
  }, [overlays])

  useEffect(() => {
    if (!overlayOptions.length) {
      setVisibility({})
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
