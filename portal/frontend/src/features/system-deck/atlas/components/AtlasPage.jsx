import { AtlasConsole } from './AtlasConsole.jsx'
import { AtlasHud } from './AtlasHud.jsx'
import { ArtifactInspectionPanel } from './ArtifactInspectionPanel.jsx'
import { AtlasScene } from '../scene/AtlasScene.jsx'
import { useAtlasWorld } from '../hooks/useAtlasWorld.js'

import './Atlas.css'

export default function AtlasPage() {
  const atlas = useAtlasWorld()

  return (
    <div className="atlas-page">
      <AtlasScene
        artifacts={atlas.visibleArtifacts}
        districts={atlas.districtSummaries}
        selectedId={atlas.selectedArtifact?.id || null}
        focusTarget={atlas.focusTarget}
        focusKey={atlas.focusKey}
        resetKey={atlas.resetKey}
        onSelectArtifact={atlas.selectArtifact}
      />

      <AtlasHud
        totals={atlas.totals}
        filter={atlas.filter}
        visibleCount={atlas.visibleArtifacts.length}
        selectedArtifact={atlas.selectedArtifact}
      />

      <AtlasConsole onCommand={atlas.executeCommand} />

      <ArtifactInspectionPanel
        artifact={atlas.selectedArtifact}
        onClose={atlas.closeInspection}
        onFocus={atlas.focusArtifact}
      />
    </div>
  )
}
