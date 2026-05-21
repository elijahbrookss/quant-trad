import { useCallback, useEffect, useMemo, useState } from 'react'

import { processAtlasCommand } from '../cli/AtlasCommandProcessor.js'
import { mockRunArtifacts } from '../data/mockRunArtifacts.js'
import { buildAtlasArtifacts } from '../systems/ArtifactGenerator.js'
import { createArtifactRegistry } from '../systems/ArtifactRegistry.js'
import { artifactMatchesFilter, getArtifactFocusTarget } from '../systems/SelectionSystem.js'
import { ATLAS_FILTERS } from '../types/atlasTypes.js'

export function useAtlasWorld() {
  const artifacts = useMemo(() => buildAtlasArtifacts(mockRunArtifacts), [])
  const registry = useMemo(() => createArtifactRegistry(artifacts), [artifacts])
  const [selectedId, setSelectedId] = useState(null)
  const [focusedId, setFocusedId] = useState(null)
  const [filter, setFilter] = useState(ATLAS_FILTERS.all)
  const [resetKey, setResetKey] = useState(0)
  const [focusKey, setFocusKey] = useState(0)

  const selectedArtifact = selectedId ? registry.findById(selectedId) : null
  const focusedArtifact = focusedId ? registry.findById(focusedId) : null
  const visibleArtifacts = useMemo(() => registry.filter(filter), [registry, filter])
  const districtSummaries = useMemo(() => registry.districtSummaries(), [registry])
  const totals = useMemo(() => registry.totals(), [registry])
  const focusTarget = useMemo(() => getArtifactFocusTarget(focusedArtifact), [focusedArtifact])

  useEffect(() => {
    if (selectedArtifact && !artifactMatchesFilter(selectedArtifact, filter)) {
      setSelectedId(null)
    }
  }, [filter, selectedArtifact])

  const selectArtifact = useCallback((artifactId) => {
    setSelectedId(artifactId)
    if (artifactId) {
      setFocusedId(artifactId)
      setFocusKey((current) => current + 1)
    }
  }, [])

  const closeInspection = useCallback(() => {
    setSelectedId(null)
  }, [])

  const focusArtifact = useCallback((artifactId) => {
    if (!artifactId) return
    setSelectedId(artifactId)
    setFocusedId(artifactId)
    setFocusKey((current) => current + 1)
  }, [])

  const resetView = useCallback(() => {
    setFocusedId(null)
    setSelectedId(null)
    setResetKey((current) => current + 1)
  }, [])

  const executeCommand = useCallback((input) => {
    const result = processAtlasCommand(input, registry)
    if (result.actions?.filter) setFilter(result.actions.filter)
    if (Object.prototype.hasOwnProperty.call(result.actions || {}, 'selectId')) {
      setSelectedId(result.actions.selectId)
    }
    if (result.actions?.focusId) {
      const artifact = registry.findById(result.actions.focusId)
      if (artifact && !artifactMatchesFilter(artifact, result.actions.filter || filter)) {
        setFilter(ATLAS_FILTERS.all)
      }
      setFocusedId(result.actions.focusId)
      setFocusKey((current) => current + 1)
    }
    if (result.actions?.resetView) resetView()
    return result
  }, [filter, registry, resetView])

  return {
    artifacts,
    visibleArtifacts,
    selectedArtifact,
    focusedArtifact,
    districtSummaries,
    totals,
    filter,
    focusTarget,
    focusKey,
    resetKey,
    selectArtifact,
    closeInspection,
    focusArtifact,
    resetView,
    executeCommand,
  }
}
