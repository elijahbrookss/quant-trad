export function normalizeIndicatorArtifactResponse(indicator, response, { defaultSource = 'indicator' } = {}) {
  const indicatorColor = typeof indicator?.color === 'string' && indicator.color.trim()
    ? indicator.color.trim()
    : null;
  const overlays = Array.isArray(response?.overlays)
    ? response.overlays
    : Array.isArray(response)
      ? response
      : response && response.type && response.payload
        ? [response]
        : [];

  return overlays
    .filter((entry) => entry && entry.type && entry.payload)
    .map((entry, index) => ({
      ...entry,
      ind_id: indicator.id,
      type: entry.type || indicator.type,
      payload: entry.payload,
      color: entry.color ?? indicatorColor,
      source: entry.source ?? defaultSource,
      overlay_id: entry.overlay_id || `${indicator.id}.${defaultSource}.${index}`,
      ui: {
        ...(entry.ui || {}),
        color: indicatorColor ?? entry?.ui?.color,
      },
    }));
}
