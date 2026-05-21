import { ATLAS_FILTERS } from '../types/atlasTypes.js'

export function artifactMatchesFilter(artifact, filter) {
  if (!artifact) return false
  if (filter === ATLAS_FILTERS.profitable) return artifact.run.pnl >= 0
  if (filter === ATLAS_FILTERS.losing) return artifact.run.pnl < 0
  return true
}

export function getArtifactFocusTarget(artifact) {
  if (!artifact) return null
  return {
    x: artifact.position.x,
    y: Math.max(1.2, artifact.height * 0.45),
    z: artifact.position.z,
    height: artifact.height,
    seed: artifact.seed,
  }
}
