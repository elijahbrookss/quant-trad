import { useEffect, useMemo, useState } from 'react'
import { Switch } from '@headlessui/react'
import { IndicatorSection } from './IndicatorTab.jsx'
import { useChartValue } from '../contexts/ChartStateContext'
import { createLogger } from '../utils/logger.js'

const tabs = [
  { key: 'Indicators', label: 'Indicators' },
  { key: 'Signals', label: 'Signals' },
  { key: 'Strategies', label: 'Strategies' },
]

export const TabManager = ({ chartId }) => {
  const [activeTab, setActiveTab] = useState(tabs[0].key)
  const [showEnabledOnly, setShowEnabledOnly] = useState(false)

  const logger = useMemo(() => createLogger('TabManager', { chartId }), [chartId])
  const { info, debug } = logger
  const chartSnapshot = useChartValue(chartId)
  const enabledCount = chartSnapshot?.indicators?.filter((i) => i?.enabled).length ?? 0
  const totalIndicators = chartSnapshot?.indicators?.length ?? 0

  useEffect(() => {
    debug('tab_manager_initialized', { activeTab })
  }, [activeTab, debug])

  useEffect(() => {
    debug('indicator_filter_state', { showEnabledOnly })
  }, [debug, showEnabledOnly])

  const handleTabClick = (tabKey) => {
    setActiveTab(tabKey)
    info('tab_switched', { tab: tabKey })
  }

  const surfaceClass = 'rounded-3xl border border-neutral-900 bg-neutral-900/60 p-6 shadow-[0_24px_80px_-50px_rgba(0,0,0,0.9)]'

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <h2 className="text-xl font-semibold text-neutral-100">Lab Console</h2>
          <p className="text-xs uppercase tracking-[0.3em] text-neutral-500">QuantLab</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="rounded-full border border-neutral-800 bg-neutral-900/60 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.2em] text-neutral-400">
            Enabled {enabledCount}
            <span className="text-neutral-600"> / {totalIndicators}</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-neutral-400">
            <span>Enabled only</span>
            <Switch
              checked={showEnabledOnly}
              onChange={setShowEnabledOnly}
              aria-label="Toggle enabled indicator filter"
              className={`${showEnabledOnly ? 'bg-emerald-500/80' : 'bg-neutral-700'} relative inline-flex h-5 w-9 items-center rounded-full transition`}
            >
              <span className={`${showEnabledOnly ? 'translate-x-5' : 'translate-x-1'} inline-block h-3 w-3 transform rounded-full bg-neutral-100 transition`} />
            </Switch>
          </div>
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        {tabs.map((tab) => {
          const isActive = activeTab === tab.key
          return (
            <button
              key={tab.key}
              onClick={() => handleTabClick(tab.key)}
              className={`inline-flex items-center gap-2 rounded-full border border-transparent px-4 py-2 text-sm font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500 ${
                isActive
                  ? 'border-neutral-700 bg-neutral-900 text-neutral-100 shadow-sm'
                  : 'border-neutral-800 bg-neutral-950/60 text-neutral-500 hover:border-neutral-700 hover:text-neutral-200'
              }`}
              type="button"
              aria-pressed={isActive}
            >
              <span className={`h-1.5 w-1.5 rounded-full ${isActive ? 'bg-neutral-300' : 'bg-neutral-700'}`} />
              {tab.label}
            </button>
          )
        })}
      </div>

      <div className={surfaceClass}>
        {activeTab === 'Indicators' && (
          <IndicatorSection
            chartId={chartId}
            showEnabledOnly={showEnabledOnly}
            onToggleEnabledOnly={setShowEnabledOnly}
          />
        )}
        {activeTab === 'Signals' && (
          <div className="grid gap-3 text-sm text-neutral-400">
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/80 px-5 py-6">
              <div className="flex items-center justify-between text-xs uppercase tracking-[0.28em] text-neutral-500">
                Queue
                <span className="text-sm font-semibold tracking-normal text-neutral-200">0</span>
              </div>
            </div>
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/80 px-5 py-6 text-center text-sm font-semibold text-neutral-500">
              No signals
            </div>
          </div>
        )}
        {activeTab === 'Strategies' && (
          <div className="grid gap-3 text-sm text-neutral-400 sm:grid-cols-2">
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/80 px-5 py-6">
              <span className="text-xs uppercase tracking-[0.28em] text-neutral-500">Win rate</span>
              <div className="mt-4 text-2xl font-semibold text-neutral-200">--</div>
            </div>
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/80 px-5 py-6">
              <span className="text-xs uppercase tracking-[0.28em] text-neutral-500">PnL</span>
              <div className="mt-4 text-2xl font-semibold text-neutral-200">--</div>
            </div>
            <div className="rounded-2xl border border-neutral-800 bg-neutral-900/80 px-5 py-6 sm:col-span-2">
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
