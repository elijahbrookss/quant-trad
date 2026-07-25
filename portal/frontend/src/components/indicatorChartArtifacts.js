import { applyIndicatorColors } from './indicatorSignals.js';
import {
  pruneIndicatorArtifactSliceCache,
  rebuildIndicatorArtifactsFromCache,
  seedIndicatorArtifactSliceCache,
} from './indicatorOverlaySlices.js';

export const DEFAULT_INDICATOR_COLOR = '#60a5fa';

export const buildColorMap = (list = []) => {
  if (!Array.isArray(list)) return {};
  return list.reduce((acc, indicator) => {
    if (!indicator?.id) return acc;
    const raw = typeof indicator?.color === 'string' ? indicator.color.trim() : '';
    acc[indicator.id] = raw || DEFAULT_INDICATOR_COLOR;
    return acc;
  }, {});
};

export const shallowEqualMap = (a = {}, b = {}) => {
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (const key of keysA) {
    if (a[key] !== b[key]) return false;
  }
  return true;
};

export const normalizedVisibilityMap = (value) => (
  value && typeof value === 'object' && !Array.isArray(value) ? value : {}
);

export const buildVisibleArtifactSets = (list = [], visibilityById = {}, activeInspection = null) => {
  const visibleIds = new Set(
    (list || [])
      .filter((indicator) => indicator?.enabled !== false && visibilityById?.[indicator.id] !== false)
      .map((indicator) => indicator?.id)
      .filter(Boolean),
  );
  const inspectionIndicatorId = activeInspection?.indicatorId;
  const indicatorIds = new Set(visibleIds);
  const inspectionIds = new Set();
  if (inspectionIndicatorId && visibleIds.has(inspectionIndicatorId)) {
    indicatorIds.delete(inspectionIndicatorId);
    inspectionIds.add(inspectionIndicatorId);
  }
  return {
    indicator: indicatorIds,
    signal: visibleIds,
    inspection: inspectionIds,
  };
};

export const buildVisibleOverlaysFromCache = (
  sliceCache,
  list,
  colors,
  visibilityMap,
  activeInspection = null,
  existingOverlays = [],
) => {
  const safeList = Array.isArray(list) ? list : [];
  const allowedIndicatorIds = new Set(safeList.map((indicator) => indicator?.id).filter(Boolean));
  const seededCache = seedIndicatorArtifactSliceCache(sliceCache || {}, existingOverlays || []);
  const prunedCache = pruneIndicatorArtifactSliceCache(seededCache, allowedIndicatorIds);
  const visibleOverlays = rebuildIndicatorArtifactsFromCache(
    prunedCache,
    buildVisibleArtifactSets(safeList, visibilityMap || {}, activeInspection),
  );
  return {
    sliceCache: prunedCache,
    overlays: applyIndicatorColors(visibleOverlays, colors || {}, safeList),
  };
};

export const indicatorIsVisibleOnChart = (indicator, visibilityById = {}) => (
  Boolean(indicator?.enabled !== false && visibilityById?.[indicator?.id] !== false)
);
