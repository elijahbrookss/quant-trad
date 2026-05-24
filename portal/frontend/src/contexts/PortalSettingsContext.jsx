import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'

import { setLogLevel } from '../utils/logger.js'

const STORAGE_KEY = 'quanttrad.portal.settings.v2'
const LEGACY_STORAGE_KEY = 'quanttrad.portal.settings.v1'
const VALID_LOG_LEVELS = new Set(['debug', 'info', 'warn', 'error'])
const VALID_LANDING_PAGES = new Set(['/quantlab', '/strategy', '/bots', '/reports'])
const VALID_DENSITY = new Set(['compact', 'comfortable'])
const VALID_MOTION = new Set(['full', 'reduced'])

function readDefaultLogLevel() {
  try {
    const stored = localStorage.getItem('LOG_LEVEL')
    if (stored && VALID_LOG_LEVELS.has(stored)) {
      return stored
    }
  } catch {
    // ignore storage access issues
  }
  return import.meta?.env?.MODE === 'production' ? 'warn' : 'debug'
}

const DEFAULT_SETTINGS = {
  accentColor: '#175a81',
  landingPage: '/quantlab',
  sidebarCollapsed: true,
  uiDensity: 'compact',
  motion: 'full',
  logLevel: readDefaultLogLevel(),
  particleField: true,
}

function normalizeHex(value) {
  if (typeof value !== 'string') return DEFAULT_SETTINGS.accentColor
  const trimmed = value.trim()
  if (!/^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(trimmed)) {
    return DEFAULT_SETTINGS.accentColor
  }
  if (trimmed.length === 4) {
    const [, r, g, b] = trimmed
    return `#${r}${r}${g}${g}${b}${b}`.toLowerCase()
  }
  return trimmed.toLowerCase()
}

function normalizeSettings(candidate = {}) {
  return {
    accentColor: normalizeHex(candidate?.accentColor),
    landingPage: VALID_LANDING_PAGES.has(candidate?.landingPage) ? candidate.landingPage : DEFAULT_SETTINGS.landingPage,
    sidebarCollapsed:
      typeof candidate?.sidebarCollapsed === 'boolean'
        ? candidate.sidebarCollapsed
        : DEFAULT_SETTINGS.sidebarCollapsed,
    uiDensity: VALID_DENSITY.has(candidate?.uiDensity) ? candidate.uiDensity : DEFAULT_SETTINGS.uiDensity,
    motion: VALID_MOTION.has(candidate?.motion) ? candidate.motion : DEFAULT_SETTINGS.motion,
    logLevel: VALID_LOG_LEVELS.has(candidate?.logLevel) ? candidate.logLevel : DEFAULT_SETTINGS.logLevel,
    particleField: typeof candidate?.particleField === 'boolean' ? candidate.particleField : DEFAULT_SETTINGS.particleField,
  }
}

function loadSettings() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY) || localStorage.getItem(LEGACY_STORAGE_KEY)
    if (!raw) return DEFAULT_SETTINGS
    return normalizeSettings(JSON.parse(raw))
  } catch {
    return DEFAULT_SETTINGS
  }
}

function persistSettings(next) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
}

function syncSettingsEffects(settings) {
  if (VALID_LOG_LEVELS.has(settings?.logLevel)) {
    setLogLevel(settings.logLevel)
    try {
      localStorage.setItem('LOG_LEVEL', settings.logLevel)
    } catch {
      // ignore storage access issues
    }
  }
}

const PortalSettingsContext = createContext({
  settings: DEFAULT_SETTINGS,
  updateSettings: () => {},
  resetSettings: () => {},
})

export function PortalSettingsProvider({ children }) {
  const [settings, setSettings] = useState(() => loadSettings())

  useEffect(() => {
    syncSettingsEffects(settings)
  }, [settings])

  const updateSettings = useCallback((patch) => {
    setSettings((prev) => {
      const next = normalizeSettings({ ...prev, ...(patch || {}) })
      persistSettings(next)
      return next
    })
  }, [])

  const resetSettings = useCallback(() => {
    persistSettings(DEFAULT_SETTINGS)
    setSettings(DEFAULT_SETTINGS)
  }, [])

  const value = useMemo(() => ({ settings, updateSettings, resetSettings }), [resetSettings, settings, updateSettings])

  return <PortalSettingsContext.Provider value={value}>{children}</PortalSettingsContext.Provider>
}

export function usePortalSettings() {
  return useContext(PortalSettingsContext)
}
