import { Activity, Boxes, Filter, RadioTower } from 'lucide-react'

import { ATLAS_FILTERS } from '../types/atlasTypes.js'

function formatCurrency(value) {
  const sign = value > 0 ? '+' : ''
  return `${sign}${Math.round(value).toLocaleString()}`
}

export function AtlasHud({ totals, filter, visibleCount, selectedArtifact }) {
  return (
    <header className="atlas-hud">
      <div className="atlas-title-block">
        <span className="atlas-kicker">System Memory</span>
        <h1>Atlas</h1>
      </div>

      <div className="atlas-hud-strip">
        <span><Boxes size={13} /> {visibleCount}/{totals.count}</span>
        <span><RadioTower size={13} /> {totals.districts}</span>
        <span><Activity size={13} /> {formatCurrency(totals.totalPnl)}</span>
        <span data-active={filter !== ATLAS_FILTERS.all}><Filter size={13} /> {filter}</span>
      </div>

      {selectedArtifact ? (
        <div className="atlas-selection-readout">
          <span>{selectedArtifact.run.strategy}</span>
          <strong>{selectedArtifact.id}</strong>
        </div>
      ) : null}
    </header>
  )
}
