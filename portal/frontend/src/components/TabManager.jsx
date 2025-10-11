import { useEffect, useMemo, useState } from 'react'
import { IndicatorSection } from './IndicatorTab.jsx'
import { createLogger } from '../utils/logger.js'

const tabs = [
  { id: 'Indicators', blurb: 'Configure overlays, oscillators, and custom panes.' },
  { id: 'Signals', blurb: 'Future real-time signal routing and alert orchestration.' },
  { id: 'Strategies', blurb: 'Blueprint execution flows for live + backtest parity.' },
]

export const TabManager = ({ chartId }) => {
  const [activeTab, setActiveTab] = useState(tabs[0].id)

  const logger = useMemo(() => createLogger('TabManager', { chartId }), [chartId])
  const { info, debug } = logger

  useEffect(() => {
    debug('tab_manager_initialized', { activeTab })
  }, [activeTab, debug])

  // Debug: log tab changes and chartId
  const handleTabClick = (tab) => {
    setActiveTab(tab)
    info('tab_switched', { tab })
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.3em] text-slate-400">
        {tabs.map(({ id }) => {
          const isActive = activeTab === id
          return (
            <button
              key={id}
              onClick={() => handleTabClick(id)}
              className={`rounded-full border px-4 py-2 transition ${
                isActive
                  ? 'border-[color:var(--accent-alpha-70)] bg-[color:var(--accent-alpha-25)] text-[color:var(--accent-text-strong)] shadow-[0_12px_32px_-18px_var(--accent-shadow-strong)]'
                  : 'border-white/10 bg-white/5 text-slate-300 hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-strong)]'
              }`}
            >
              <span>{id}</span>
            </button>
          )
        })}
      </div>

      <div className="rounded-2xl border border-white/8 bg-[#1b1e28]/75 p-6">
        {tabs.map(({ id, blurb }) => (
          <p
            key={id}
            className={`text-xs text-slate-500 transition ${activeTab === id ? 'opacity-100' : 'hidden'}`}
          >
            {blurb}
          </p>
        ))}

        {activeTab === 'Indicators' && (
          <div className="mt-6">
            <IndicatorSection chartId={chartId} />
          </div>
        )}

        {activeTab === 'Signals' && (
          <div className="mt-6 space-y-4 text-sm text-slate-300">
            <p className="text-slate-400">
              Design the routing for future signal engines. Define which indicators feed each signal, throttle policies, and notification targets.
            </p>
            <div className="grid gap-4 lg:grid-cols-2">
              <div className="rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] p-4 text-[color:var(--accent-text-soft-alpha)]">
                <h4 className="text-sm font-semibold text-[color:var(--accent-text-strong)]">Live routing</h4>
                <p className="mt-2 text-xs">Map signals to webhooks, Discord channels, or automation web services. Future UI will surface connection health inline.</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-slate-300">
                <h4 className="text-sm font-semibold text-slate-100">Alert templates</h4>
                <p className="mt-2 text-xs">Pre-build alert payloads that pull in indicator context, risk tags, and strategy ownership metadata.</p>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'Strategies' && (
          <div className="mt-6 space-y-4 text-sm text-slate-300">
            <p className="text-slate-400">
              Assemble execution flows from QuantLab research into deployable strategy blueprints. Link to Ops Command for seamless rollouts.
            </p>
            <div className="grid gap-4 lg:grid-cols-2">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <h4 className="text-sm font-semibold text-slate-100">Playbook composer</h4>
                <p className="mt-2 text-xs text-slate-400">Chain signals, filters, and risk gates. Save variants for different market regimes.</p>
              </div>
              <div className="rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] p-4 text-[color:var(--accent-text-soft-alpha)]">
                <h4 className="text-sm font-semibold text-[color:var(--accent-text-strong)]">Execution sync</h4>
                <p className="mt-2 text-xs">Push strategies directly into the DevOps control plane for containerized rollout and monitoring.</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
