import { useEffect, useMemo, useState } from 'react'
import { IndicatorSection } from './IndicatorTab.jsx'
import { createLogger } from '../utils/logger.js'

const tabs = ['Indicators', 'Signals', 'Strategies']

export const TabManager = ({ chartId }) => {
  const [activeTab, setActiveTab] = useState(tabs[0])

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
    <section className="rounded-3xl border border-slate-900/70 bg-slate-950/70 p-6 shadow-xl shadow-slate-950/40">
      <div className="flex flex-wrap items-center gap-2">
        {tabs.map((tab) => {
          const isActive = activeTab === tab
          return (
            <button
              key={tab}
              onClick={() => handleTabClick(tab)}
              className={`rounded-full px-4 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-300 ${
                isActive
                  ? 'bg-sky-500/20 text-sky-100 shadow-inner shadow-sky-500/30'
                  : 'bg-slate-900/80 text-slate-300 hover:bg-slate-900'
              }`}
              type="button"
            >
              {tab}
            </button>
          )
        })}
      </div>

      <div className="mt-6">
        {activeTab === 'Indicators' && (
          <IndicatorSection chartId={chartId}/>
        )}
        {activeTab === 'Signals' && (
          <div className="rounded-2xl border border-slate-900/60 bg-slate-950/80 p-6 text-sm text-slate-300">
            Generate signals from an indicator to see a curated timeline of entries and exits. Choose an indicator, run signal generation, and the results will appear here.
          </div>
        )}
        {activeTab === 'Strategies' && (
          <div className="rounded-2xl border border-slate-900/60 bg-slate-950/80 p-6 text-sm text-slate-300">
            Coming soon: bundle indicators and risk rules into beginner-friendly strategy templates you can paper trade instantly.
          </div>
        )}
      </div>
    </section>
  )
}
