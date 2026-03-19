function normalizeRunId(value) {
  const normalized = String(value || '').trim()
  return normalized || null
}

export function chooseBotLensRunSelection({
  currentRunId,
  runs,
  activeRunId,
  selectionMode = 'auto',
  previousActiveRunId = null,
}) {
  const catalog = Array.isArray(runs) ? runs : []
  const normalizedCurrentRunId = normalizeRunId(currentRunId)
  const normalizedActiveRunId = normalizeRunId(activeRunId)
  const normalizedPreviousActiveRunId = normalizeRunId(previousActiveRunId)
  const activeCatalogRunId =
    normalizeRunId(catalog.find((entry) => entry?.is_active)?.run_id) ||
    normalizedActiveRunId ||
    normalizeRunId(catalog[0]?.run_id)

  if (selectionMode !== 'manual') {
    return {
      runId: activeCatalogRunId,
      selectionMode: 'auto',
    }
  }

  const currentExists = normalizedCurrentRunId
    ? catalog.some((entry) => normalizeRunId(entry?.run_id) === normalizedCurrentRunId)
    : false

  if (!currentExists) {
    return {
      runId: activeCatalogRunId,
      selectionMode: 'auto',
    }
  }

  if (
    normalizedCurrentRunId &&
    normalizedPreviousActiveRunId &&
    normalizedCurrentRunId === normalizedPreviousActiveRunId &&
    activeCatalogRunId &&
    activeCatalogRunId !== normalizedPreviousActiveRunId
  ) {
    return {
      runId: activeCatalogRunId,
      selectionMode: 'auto',
    }
  }

  return {
    runId: normalizedCurrentRunId,
    selectionMode: 'manual',
  }
}

