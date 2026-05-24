import {
  getIndicatorSignalColor,
  getPaletteOverlayColor,
  usesIndicatorPalette,
} from '../utils/indicatorColors.js'

const normalizeColor = (value) => (
  typeof value === 'string' && value.trim() ? value.trim() : null
);

export function normalizeIndicatorArtifactResponse(indicator, response, { defaultSource = 'indicator' } = {}) {
  const indicatorColor = normalizeColor(indicator?.color);
  const usesOverlayColorSet = usesIndicatorPalette(indicator);
  const overlays = Array.isArray(response?.overlays)
    ? response.overlays
    : Array.isArray(response)
      ? response
      : response && response.type && response.payload
        ? [response]
        : [];

  return overlays
    .filter((entry) => entry && entry.type && entry.payload)
    .map((entry, index) => {
      const paletteOverlayColor = getPaletteOverlayColor(indicator, entry?.type);
      const overlayColor = paletteOverlayColor
        ?? normalizeColor(entry?.ui?.color)
        ?? normalizeColor(entry?.color);
      const signalColor = entry?.source === 'signal'
        ? normalizeColor(getIndicatorSignalColor(indicator))
        : null;
      const colorPolicy = usesOverlayColorSet && overlayColor ? 'overlay' : 'indicator';
      const resolvedColor = signalColor || (
        colorPolicy === 'overlay'
          ? overlayColor
          : indicatorColor ?? overlayColor
      );
      return {
        ...entry,
        ind_id: indicator.id,
        type: entry.type || indicator.type,
        payload: entry.payload,
        color: resolvedColor,
        source: entry.source ?? defaultSource,
        overlay_id: entry.overlay_id || `${indicator.id}.${defaultSource}.${index}`,
        ui: {
          ...(entry.ui || {}),
          color: resolvedColor,
          color_policy: colorPolicy,
        },
      };
    });
}
