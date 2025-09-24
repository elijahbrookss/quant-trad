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
  const [showEnabledOnly, setShowEnabledOnly] = useState(false)
  const [indicatorStats, setIndicatorStats] = useState({ total: 0, enabled: 0 })

  const logger = useMemo(() => createLogger('TabManager', { chartId }), [chartId])
  const { info, debug } = logger

  useEffect(() => {
    debug('tab_manager_initialized', { activeTab })
  }, [activeTab, debug])

  const handleTabClick = (tabKey) => {
    setActiveTab(tabKey)
    info('tab_switched', { tab: tabKey })
  }

  const surfaceClass = 'rounded-3xl border border-neutral-800/70 bg-neutral-950/80 p-6 shadow-[0_18px_45px_-30px_rgba(0,0,0,0.9)]'

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold text-neutral-100">Lab Console</h2>
          <p className="text-xs uppercase tracking-[0.28em] text-neutral-500">QuantLab</p>
        </div>
        <div className="flex items-center gap-3 text-xs text-neutral-400">
          <span className="rounded-full border border-neutral-800 bg-neutral-900/80 px-3 py-1 font-semibold text-neutral-200">
            {indicatorStats.enabled} active
          </span>
          <span className="text-neutral-500">of {indicatorStats.total} indicators</span>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        {tabs.map((tab) => {
          const isActive = activeTab === tab.key
          return (
            <button
              key={tab.key}
              onClick={() => handleTabClick(tab.key)}
              className={`inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500 ${
                isActive
                  ? 'border-neutral-500 bg-neutral-800 text-neutral-100 shadow'
                  : 'border-transparent bg-neutral-900 text-neutral-500 hover:border-neutral-700 hover:text-neutral-100'
              }`}
              type="button"
              aria-pressed={isActive}
            >
              <span className={`h-1.5 w-1.5 rounded-full ${isActive ? 'bg-neutral-200' : 'bg-neutral-700'}`} />
              {tab.label}
            </button>
          )
        })}

        {activeTab === 'Indicators' && (
          <label className="ml-auto inline-flex items-center gap-2 rounded-full border border-neutral-800 bg-neutral-950/70 px-3 py-1.5 text-xs text-neutral-400">
            <input
              type="checkbox"
              className="h-3.5 w-3.5 rounded border-neutral-700 bg-neutral-900 text-neutral-100 focus:ring-neutral-500"
              checked={showEnabledOnly}
              onChange={(event) => setShowEnabledOnly(event.target.checked)}
            />
            Show enabled only
          </label>
        )}
      </div>

      <div className={surfaceClass}>
        {activeTab === 'Indicators' && (
          <IndicatorSection
            chartId={chartId}
            filterEnabledOnly={showEnabledOnly}
            onStatsChange={setIndicatorStats}
          />
        )}
        {activeTab === 'Signals' && (
          <div className="grid gap-3 text-sm text-neutral-400">
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/70 px-5 py-6">
              <div className="flex items-center justify-between text-xs uppercase tracking-[0.28em] text-neutral-500">
                Queue
                <span className="text-sm font-semibold tracking-normal text-neutral-200">0</span>
              </div>
            </div>
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/70 px-5 py-6 text-center text-sm font-semibold text-neutral-500">
              No signals
            </div>
          </div>
        )}
        {activeTab === 'Strategies' && (
          <div className="grid gap-3 text-sm text-neutral-400 sm:grid-cols-2">
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/70 px-5 py-6">
              <span className="text-xs uppercase tracking-[0.28em] text-neutral-500">Win rate</span>
              <div className="mt-4 text-2xl font-semibold text-neutral-100">--</div>
            </div>
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/70 px-5 py-6">
              <span className="text-xs uppercase tracking-[0.28em] text-neutral-500">PnL</span>
              <div className="mt-4 text-2xl font-semibold text-neutral-100">--</div>
            </div>
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/70 px-5 py-6 sm:col-span-2">
              <span className="text-xs uppercase tracking-[0.28em] text-neutral-500">Trade log</span>
              <div className="mt-4 grid grid-cols-3 gap-3 text-center text-lg font-semibold text-neutral-200">
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
