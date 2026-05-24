import test from 'node:test'
import assert from 'node:assert/strict'

import { processAtlasCommand } from '../src/features/system-deck/atlas/cli/AtlasCommandProcessor.js'
import { mockRunArtifacts } from '../src/features/system-deck/atlas/data/mockRunArtifacts.js'
import { buildAtlasArtifacts } from '../src/features/system-deck/atlas/systems/ArtifactGenerator.js'
import { createArtifactRegistry } from '../src/features/system-deck/atlas/systems/ArtifactRegistry.js'
import { ATLAS_FAMILIES, ATLAS_FILTERS } from '../src/features/system-deck/atlas/types/atlasTypes.js'

function buildRegistry() {
  const artifacts = buildAtlasArtifacts(mockRunArtifacts)
  return {
    artifacts,
    registry: createArtifactRegistry(artifacts),
  }
}

test('atlas artifact generation is deterministic from completed run artifacts', () => {
  const first = buildAtlasArtifacts(mockRunArtifacts)
  const second = buildAtlasArtifacts(mockRunArtifacts)

  assert.deepEqual(
    first.map((artifact) => ({
      id: artifact.id,
      family: artifact.family,
      position: artifact.position,
      height: artifact.height,
      damage: artifact.damage,
      windows: artifact.windows.length,
    })),
    second.map((artifact) => ({
      id: artifact.id,
      family: artifact.family,
      position: artifact.position,
      height: artifact.height,
      damage: artifact.damage,
      windows: artifact.windows.length,
    })),
  )
})

test('atlas maps run outcomes into structure height, damage, and ruins', () => {
  const { registry } = buildRegistry()
  const profitable = registry.findById('aurora')
  const failed = registry.findById('cinder')
  const dense = registry.findById('scalpgrid')
  const sparse = registry.findById('delta')

  assert.ok(profitable.height > failed.height)
  assert.equal(failed.family, ATLAS_FAMILIES.ruin)
  assert.ok(failed.damage > 0.9)
  assert.ok(dense.windows.length > sparse.windows.length)
})

test('atlas command processor selects, filters, and lists artifacts', () => {
  const { registry } = buildRegistry()

  const latest = processAtlasCommand('atlas latest', registry)
  assert.equal(latest.actions.selectId, 'run-2026-05-18-aurora-01')
  assert.equal(latest.actions.focusId, 'run-2026-05-18-aurora-01')

  const inspect = processAtlasCommand('atlas inspect aurora', registry)
  assert.equal(inspect.actions.selectId, 'run-2026-05-18-aurora-01')
  assert.match(inspect.lines.map((entry) => entry.text).join('\n'), /strategy=Aurora Breakout Ladder/)

  const losing = processAtlasCommand('atlas filter losing', registry)
  assert.equal(losing.actions.filter, ATLAS_FILTERS.losing)

  const districts = processAtlasCommand('atlas districts', registry)
  assert.ok(districts.lines.length >= 4)
  assert.match(districts.lines[0].text, /artifacts/)
})
