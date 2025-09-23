import { useEffect, useMemo, useState } from 'react'
import { IndicatorSection } from './IndicatorTab.jsx'
import { createLogger } from '../utils/logger.js'

const tabs = [
  { key: 'Indicators', label: 'Indicators' },
  { key: 'Signals', label: 'Signals' },
  { key: 'Strategies', label: 'Strategies' },
]

export const TabManager = ({ chartId }) => {
  const [activeTab, setActiveTab] = useState(tabs[0].key)

  const logger = useMemo(() => createLogger('TabManager', { chartId }), [chartId])
  const { info, debug } = logger

  useEffect(() => {
    debug('tab_manager_initialized', { activeTab })
  }, [activeTab, debug])

  const handleTabClick = (tabKey) => {
    setActiveTab(tabKey)
    info('tab_switched', { tab: tabKey })
  }

  const surfaceClass = 'rounded-3xl border border-slate-800/70 bg-slate-950/60 p-6 shadow-[0_35px_65px_-40px_rgba(15,23,42,0.85)] backdrop-blur'

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-col gap-1">
        <h2 className="text-xl font-semibold text-slate-50">Workspace panels</h2>
        <p className="text-sm text-slate-500">One panel for indicators, signals, and playbooks.</p>
      </div>

      <div className="flex flex-wrap gap-2">
        {tabs.map((tab) => {
          const isActive = activeTab === tab.key
          return (
            <button
              key={tab.key}
              onClick={() => handleTabClick(tab.key)}
              className={`inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 ${
                isActive
                  ? 'border-sky-500/60 bg-sky-500/20 text-sky-100 shadow-[0_10px_20px_-12px_rgba(14,165,233,0.7)]'
                  : 'border-slate-800/70 bg-slate-900/50 text-slate-300 hover:border-slate-700 hover:text-slate-100'
              }`}
              type="button"
            >
              <span className={`h-1.5 w-1.5 rounded-full ${isActive ? 'bg-sky-200' : 'bg-slate-500'}`} />
              {tab.label}
            </button>
          )
        })}
      </div>

      <div className={surfaceClass}>
        {activeTab === 'Indicators' && (
          <IndicatorSection chartId={chartId} />
        )}
        {activeTab === 'Signals' && (
          <div className="space-y-4">
            <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 p-5 text-sm text-slate-300">
              <p className="font-medium text-slate-200">No signals recorded</p>
              <p className="mt-1 text-slate-400">Results populate automatically once signal engines respond for this chart.</p>
            </div>
          </div>
        )}
        {activeTab === 'Strategies' && (
          <div className="space-y-4">
            <div className="grid gap-3 text-sm text-slate-300">
              <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 p-5">
                <p className="font-medium text-slate-200">Coming soon</p>
                <p className="mt-1 text-slate-400">Structured playbooks for execution and review will land here.</p>
              </div>
              <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 p-5">
                <p className="font-medium text-slate-200">Roadmap</p>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-slate-400">
                  <li>Integrated risk checkpoints.</li>
                  <li>Repeatable execution templates.</li>
                  <li>Post-trade review capture.</li>
                </ul>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
