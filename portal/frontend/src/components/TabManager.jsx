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

  const surfaceClass = 'rounded-3xl border border-slate-800/70 bg-slate-950/60 p-6 shadow-[0_32px_60px_-45px_rgba(15,23,42,0.85)] backdrop-blur'

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-slate-50">Panels</h2>
        <span className="text-xs font-semibold uppercase tracking-[0.35em] text-slate-500">QuantLab</span>
      </div>

      <div className="flex flex-wrap gap-2">
        {tabs.map((tab) => {
          const isActive = activeTab === tab.key
          return (
            <button
              key={tab.key}
              onClick={() => handleTabClick(tab.key)}
              className={`inline-flex items-center gap-2 rounded-full border border-transparent px-4 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 ${
                isActive
                  ? 'border-slate-100 bg-slate-100 text-slate-900 shadow-[0_12px_25px_-15px_rgba(148,163,184,0.65)]'
                  : 'border-slate-800/70 bg-slate-900/60 text-slate-300 hover:text-slate-100'
              }`}
              type="button"
              aria-pressed={isActive}
            >
              <span className={`h-1.5 w-1.5 rounded-full ${isActive ? 'bg-slate-900' : 'bg-slate-500'}`} />
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
          <div className="grid gap-3 text-sm text-slate-300">
            <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 px-5 py-6">
              <div className="flex items-center justify-between text-xs uppercase tracking-[0.35em] text-slate-500">
                Queue
                <span className="text-sm font-semibold tracking-normal text-slate-200">0</span>
              </div>
            </div>
            <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 px-5 py-6 text-center text-sm font-semibold text-slate-400">
              No signals
            </div>
          </div>
        )}
        {activeTab === 'Strategies' && (
          <div className="grid gap-3 text-sm text-slate-300 sm:grid-cols-2">
            <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 px-5 py-6">
              <span className="text-xs uppercase tracking-[0.35em] text-slate-500">Win rate</span>
              <div className="mt-4 text-2xl font-semibold text-slate-200">--</div>
            </div>
            <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 px-5 py-6">
              <span className="text-xs uppercase tracking-[0.35em] text-slate-500">PnL</span>
              <div className="mt-4 text-2xl font-semibold text-slate-200">--</div>
            </div>
            <div className="rounded-2xl border border-slate-800/60 bg-slate-900/40 px-5 py-6 sm:col-span-2">
              <span className="text-xs uppercase tracking-[0.35em] text-slate-500">Trade log</span>
              <div className="mt-4 grid grid-cols-3 gap-3 text-center text-lg font-semibold text-slate-200">
                <span>--</span>
                <span>--</span>
                <span>--</span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
