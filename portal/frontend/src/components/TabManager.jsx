import { useEffect, useMemo, useState } from 'react'
import { IndicatorSection } from './IndicatorTab.jsx'
import StrategyTab from './StrategyTab.jsx'
import { BotPanel } from './bots/BotPanel.jsx'
import { createLogger } from '../utils/logger.js'

const tabs = [
  { id: 'Indicators', blurb: 'Configure overlays, oscillators, and custom panes.' },
  { id: 'Strategies', blurb: 'Blueprint execution flows for live + backtest parity.' },
  { id: 'Bots', blurb: 'Wire strategies into reusable bots for instant and walk-forward backtests.' },
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

        {activeTab === 'Bots' && (
          <div className="mt-6">
            <BotPanel />
          </div>
        )}

        {activeTab === 'Strategies' && (
          <div className="mt-6">
            <StrategyTab chartId={chartId} />
          </div>
        )}
      </div>
    </div>
  )
}
