import { createContext, useContext, useMemo, useState } from 'react'

const STORAGE_KEY = 'quanttrad.portal.settings.v1'

const DEFAULT_SETTINGS = {
  accentColor: '#175a81',
  botDefaults: {
    snapshotIntervalMs: 1000,
    envText: '',
  },
}

function loadSettings() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return DEFAULT_SETTINGS
    const parsed = JSON.parse(raw)
    return {
      ...DEFAULT_SETTINGS,
      ...parsed,
      botDefaults: {
        ...DEFAULT_SETTINGS.botDefaults,
        ...(parsed?.botDefaults || {}),
      },
    }
  } catch {
    return DEFAULT_SETTINGS
  }
}

const PortalSettingsContext = createContext({
  settings: DEFAULT_SETTINGS,
  updateSettings: () => {},
})

export function PortalSettingsProvider({ children }) {
  const [settings, setSettings] = useState(() => loadSettings())

  const updateSettings = (patch) => {
    setSettings((prev) => {
      const next = {
        ...prev,
        ...patch,
        botDefaults: {
          ...(prev?.botDefaults || {}),
          ...(patch?.botDefaults || {}),
        },
      }
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
      return next
    })
  }

  const value = useMemo(() => ({ settings, updateSettings }), [settings])

  return <PortalSettingsContext.Provider value={value}>{children}</PortalSettingsContext.Provider>
}

export function usePortalSettings() {
  return useContext(PortalSettingsContext)
}
