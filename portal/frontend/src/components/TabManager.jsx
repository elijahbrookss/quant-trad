import { useState } from 'react'
import { IndicatorSection } from './IndicatorTab.jsx'

const tabs = ['Indicators', 'Signals', 'Strategies']

export const TabManager = () => {
  const [activeTab, setActiveTab] = useState(tabs[0])

  return (
    <div className="p-6">
      {/* Top Tab Bar */}
      <div className="flex mb-4">
        {tabs.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-2 -mb-px border-b-2 transition-all cursor-pointer ${
              activeTab === tab
                ? 'border-white text-white font-semibold rounded-xs'
                : 'border-transparent text-white/25 hover:text-neutral-500 '
            }`}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      <div className="mt-4">
        {activeTab === 'Indicators' && (
          <div className="">
            <IndicatorSection />
          </div>
        )}
        {activeTab === 'Signals' && (
          <div className="">Signal section goes here.</div>
        )}
        {activeTab === 'Strategies' && (
          <div className="">Strategy section goes here.</div>
        )}
      </div>
    </div>
  )
}
