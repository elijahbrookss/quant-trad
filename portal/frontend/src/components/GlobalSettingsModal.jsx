import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { RotateCcw, X } from 'lucide-react'

import { usePortalSettings } from '../contexts/PortalSettingsContext.jsx'
import { useAccentColor } from '../contexts/AccentColorContext.jsx'

const ACCENT_PRESETS = [
  { name: 'Harbor', value: '#175a81' },
  { name: 'Signal', value: '#0f766e' },
  { name: 'Brass', value: '#b6892d' },
  { name: 'Ember', value: '#c35a2d' },
]

const LANDING_OPTIONS = [
  { value: '/quantlab', label: 'QuantLab' },
  { value: '/strategy', label: 'Strategy' },
  { value: '/bots', label: 'Bots' },
  { value: '/reports', label: 'Reports' },
]

const NAV_OPTIONS = [
  { value: true, label: 'Compact rail' },
  { value: false, label: 'Expanded rail' },
]

const DENSITY_OPTIONS = [
  { value: 'compact', label: 'Compact' },
  { value: 'comfortable', label: 'Comfortable' },
]

const ANIMATION_OPTIONS = [
  { value: 'full', label: 'Standard' },
  { value: 'reduced', label: 'Reduced' },
]

const PARTICLE_FIELD_OPTIONS = [
  { value: true, label: 'Enabled' },
  { value: false, label: 'Disabled' },
]

const LOG_LEVEL_OPTIONS = [
  { value: 'debug', label: 'Debug' },
  { value: 'info', label: 'Info' },
  { value: 'warn', label: 'Warn' },
  { value: 'error', label: 'Error' },
]

function ChoiceRow({ label, helper, children }) {
  return (
    <section className="space-y-2 rounded-md border border-white/[0.08] bg-white/[0.03] p-4">
      <div className="space-y-1">
        <h4 className="text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-300">{label}</h4>
        {helper ? <p className="text-[11px] text-slate-500">{helper}</p> : null}
      </div>
      {children}
    </section>
  )
}

function OptionGrid({ options, currentValue, onSelect, columns = 'sm:grid-cols-2' }) {
  return (
    <div className={`grid gap-2 ${columns}`}>
      {options.map((option) => {
        const active = option.value === currentValue
        return (
          <button
            key={String(option.value)}
            type="button"
            onClick={() => onSelect(option.value)}
            className={[
              'rounded-md border px-3 py-2 text-left text-[12px] transition',
              active
                ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]'
                : 'border-white/[0.07] bg-[#080c12]/60 text-slate-300 hover:border-white/[0.14] hover:bg-white/[0.05] hover:text-slate-100',
            ].join(' ')}
          >
            {option.label}
          </button>
        )
      })}
    </div>
  )
}

export function GlobalSettingsModal({ open, onClose }) {
  const { settings, updateSettings, resetSettings } = usePortalSettings()
  const { setAccentColor } = useAccentColor()
  const accentValue = settings?.accentColor || '#175a81'

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/75 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="flex max-h-[calc(100vh-2rem)] w-full max-w-2xl flex-col overflow-hidden rounded-lg border border-white/[0.09] bg-[#0c1018]/90 shadow-[0_32px_80px_-20px_rgba(0,0,0,0.9)] backdrop-blur-2xl">

          {/* accent top strip */}
          <div className="h-px w-full bg-gradient-to-r from-transparent via-[color:var(--accent-alpha-60)] to-transparent" />

          <div className="flex items-start justify-between gap-4 border-b border-white/[0.07] px-6 pt-5 pb-4">
            <div className="space-y-1">
              <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-[color:var(--accent-text-kicker)]">Portal</p>
              <DialogTitle className="text-xl font-medium text-slate-50">Global Settings</DialogTitle>
              <p className="text-sm text-slate-400">
                Local portal preferences only. Bot runtime and execution controls stay in their own workflows.
              </p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-white/[0.08] bg-white/[0.04] text-slate-400 transition-colors hover:border-white/[0.14] hover:bg-white/[0.07] hover:text-slate-200"
              aria-label="Close"
            >
              <X className="size-4" />
            </button>
          </div>

          <div className="overflow-y-auto px-6 py-5">
            <div className="space-y-4">
              <ChoiceRow
                label="Accent"
                helper="Applied immediately in this browser."
              >
                <div className="flex items-center gap-3 rounded-md border border-white/[0.07] bg-[#080c12]/60 px-3 py-3">
                  <span
                    className="h-9 w-9 rounded-sm border border-white/10"
                    style={{ backgroundColor: accentValue }}
                  />
                  <div className="min-w-0 flex-1">
                    <div className="qt-mono text-[12px] text-slate-200">{accentValue}</div>
                    <div className="text-[11px] text-slate-500">Theme accent</div>
                  </div>
                  <input
                    type="color"
                    value={accentValue}
                    onChange={(event) => {
                      updateSettings({ accentColor: event.target.value })
                      setAccentColor(event.target.value)
                    }}
                    className="h-9 w-9 cursor-pointer rounded border border-white/[0.08] bg-[#0c1018]"
                  />
                </div>
                <div className="grid gap-2 sm:grid-cols-4">
                  {ACCENT_PRESETS.map((preset) => {
                    const active = preset.value === accentValue
                    return (
                      <button
                        key={preset.value}
                        type="button"
                        onClick={() => {
                          updateSettings({ accentColor: preset.value })
                          setAccentColor(preset.value)
                        }}
                        className={[
                          'flex items-center gap-2 rounded-md border px-3 py-2 text-left text-[12px] transition',
                          active
                            ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]'
                            : 'border-white/[0.07] bg-[#080c12]/60 text-slate-300 hover:border-white/[0.14] hover:bg-white/[0.05] hover:text-slate-100',
                        ].join(' ')}
                      >
                        <span
                          className="h-4 w-4 rounded-sm border border-white/10"
                          style={{ backgroundColor: preset.value }}
                        />
                        <span>{preset.name}</span>
                      </button>
                    )
                  })}
                </div>
              </ChoiceRow>

              <div className="grid gap-4 lg:grid-cols-2">
                <ChoiceRow
                  label="Landing View"
                  helper="Default route when the portal opens."
                >
                  <OptionGrid
                    options={LANDING_OPTIONS}
                    currentValue={settings?.landingPage}
                    onSelect={(value) => updateSettings({ landingPage: value })}
                    columns="grid-cols-2"
                  />
                </ChoiceRow>

                <ChoiceRow
                  label="Sidebar"
                  helper="How much navigation chrome to show by default."
                >
                  <OptionGrid
                    options={NAV_OPTIONS}
                    currentValue={Boolean(settings?.sidebarCollapsed)}
                    onSelect={(value) => updateSettings({ sidebarCollapsed: value })}
                  />
                </ChoiceRow>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <ChoiceRow
                  label="Density"
                  helper="Tighter or roomier layout spacing."
                >
                  <OptionGrid
                    options={DENSITY_OPTIONS}
                    currentValue={settings?.uiDensity}
                    onSelect={(value) => updateSettings({ uiDensity: value })}
                  />
                </ChoiceRow>

                <ChoiceRow
                  label="Animations"
                  helper="Reduce interface transitions and animated polish."
                >
                  <OptionGrid
                    options={ANIMATION_OPTIONS}
                    currentValue={settings?.motion}
                    onSelect={(value) => updateSettings({ motion: value })}
                  />
                </ChoiceRow>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <ChoiceRow
                  label="Background Field"
                  helper="Floating particle network behind the interface."
                >
                  <OptionGrid
                    options={PARTICLE_FIELD_OPTIONS}
                    currentValue={settings?.particleField !== false}
                    onSelect={(value) => updateSettings({ particleField: value })}
                  />
                </ChoiceRow>

                <ChoiceRow
                  label="Client Logs"
                  helper="Browser-side portal logging level."
                >
                  <OptionGrid
                    options={LOG_LEVEL_OPTIONS}
                    currentValue={settings?.logLevel}
                    onSelect={(value) => updateSettings({ logLevel: value })}
                    columns="grid-cols-2"
                  />
                </ChoiceRow>
              </div>
            </div>
          </div>

          <div className="flex items-center justify-between gap-3 border-t border-white/[0.07] px-6 py-4">
            <p className="text-[11px] text-slate-500">Stored locally for this browser.</p>
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={() => {
                  resetSettings()
                  setAccentColor('#175a81')
                }}
                className="inline-flex items-center gap-2 rounded-md border border-white/[0.08] bg-white/[0.04] px-3 py-2 text-[12px] text-slate-300 transition-colors hover:border-white/[0.14] hover:bg-white/[0.06] hover:text-slate-100"
              >
                <RotateCcw className="size-3.5" />
                Reset
              </button>
              <button
                type="button"
                onClick={onClose}
                className="rounded-md border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] px-4 py-2 text-[12px] font-medium text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)]"
              >
                Close
              </button>
            </div>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
