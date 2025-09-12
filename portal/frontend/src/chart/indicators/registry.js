import { PaneViewType } from '../paneViews/factory';

// Map indicator "type" → which pane-views it wants
const INDICATOR_PANEVIEWS = {
  // keep touch everywhere by default
  default: [PaneViewType.TOUCH],

  // examples (adjust to your actual types/strings)
  pivot_levels: [PaneViewType.TOUCH],
  market_profile: [PaneViewType.TOUCH, PaneViewType.VA_BOX],
  vwap: [PaneViewType.TOUCH],
  trendlines: [PaneViewType.TOUCH],
};

// expose which pane views a type uses
export function getPaneViewsFor(indicatorType) {
  return INDICATOR_PANEVIEWS[indicatorType] || INDICATOR_PANEVIEWS.default;
}

// Normalize backend payload → { priceLines, markers, touchPoints, boxes }
export function adaptPayload(indicatorType, payload, colorHex) {
  const priceLines = Array.isArray(payload?.price_lines) ? payload.price_lines : [];
  const markersAll = Array.isArray(payload?.markers) ? payload.markers : [];
  const boxes      = Array.isArray(payload?.boxes) ? payload.boxes : [];

  // touch markers → touchPoints (time in epoch seconds preferred in your chart)
  const touchPoints = markersAll
    .filter(m => m?.subtype === 'touch' && typeof m?.price === 'number' && m?.time != null)
    .map(m => ({
      time: typeof m.time === 'number' && m.time > 2e10 ? Math.floor(m.time / 1000) : m.time,
      price: Number(m.price),
      color: colorHex || m.color,
      size: m.size ?? 4,
    }));

  // non-touch markers remain markers
  const markers = markersAll
    .filter(m => m?.subtype !== 'touch')
    .map(m => ({
      ...m,
      time: (typeof m.time === 'number' && m.time > 2e10) ? Math.floor(m.time / 1000) : m.time,
    }));

  return { priceLines, markers, touchPoints, boxes };
}
