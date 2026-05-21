import { ATLAS_FILTERS } from '../types/atlasTypes.js'
import { getDistrictSummaries } from './WorldLayoutSystem.js'

export class ArtifactRegistry {
  constructor(artifacts) {
    this.artifacts = artifacts
    this.byId = new Map(artifacts.map((artifact) => [artifact.id, artifact]))
    this.byLowerId = new Map(artifacts.map((artifact) => [artifact.id.toLowerCase(), artifact]))
  }

  list() {
    return this.artifacts
  }

  findById(id) {
    const normalized = String(id || '').trim().toLowerCase()
    if (!normalized) return null
    if (this.byLowerId.has(normalized)) return this.byLowerId.get(normalized)
    const matches = this.artifacts.filter((artifact) => artifact.id.toLowerCase().includes(normalized))
    return matches.length === 1 ? matches[0] : null
  }

  latest() {
    return this.artifacts[0] || null
  }

  filter(filter) {
    if (filter === ATLAS_FILTERS.profitable) {
      return this.artifacts.filter((artifact) => artifact.run.pnl >= 0)
    }
    if (filter === ATLAS_FILTERS.losing) {
      return this.artifacts.filter((artifact) => artifact.run.pnl < 0)
    }
    return this.artifacts
  }

  districtSummaries() {
    return getDistrictSummaries(this.artifacts)
  }

  totals() {
    const totalPnl = this.artifacts.reduce((sum, artifact) => sum + artifact.run.pnl, 0)
    const profitable = this.artifacts.filter((artifact) => artifact.run.pnl >= 0).length
    return {
      count: this.artifacts.length,
      profitable,
      losing: this.artifacts.length - profitable,
      totalPnl,
      districts: this.districtSummaries().length,
    }
  }
}

export function createArtifactRegistry(artifacts) {
  return new ArtifactRegistry(artifacts)
}
