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
                  ? 'border-purple-400/60 bg-purple-500/20 text-purple-100 shadow-[0_10px_30px_-15px_rgba(168,85,247,0.6)]'
                  : 'border-white/10 bg-white/5 text-slate-400 hover:border-purple-400/30 hover:bg-purple-500/10 hover:text-purple-100'
              }`}
            >
              <span>{id}</span>
            </button>
          )
        })}
      </div>

      <div className="rounded-2xl border border-white/5 bg-[#0d0d11]/70 p-6">
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
              <div className="rounded-2xl border border-purple-500/20 bg-purple-500/5 p-4 text-purple-100/80">
                <h4 className="text-sm font-semibold text-purple-200">Live routing</h4>
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
              <div className="rounded-2xl border border-purple-500/20 bg-purple-500/5 p-4">
                <h4 className="text-sm font-semibold text-purple-200">Execution sync</h4>
                <p className="mt-2 text-xs text-purple-100/80">Push strategies directly into the DevOps control plane for containerized rollout and monitoring.</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
