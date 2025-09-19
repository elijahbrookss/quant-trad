import { PaneViewType } from '../paneViews/factory';

const INDICATOR_PANEVIEWS = {
  default: [PaneViewType.TOUCH],
  pivot_level: [PaneViewType.TOUCH],
  market_profile: [PaneViewType.VA_BOX, PaneViewType.TOUCH],
  trendline: [PaneViewType.SEGMENT, PaneViewType.TOUCH],
  vwap: [PaneViewType.POLYLINE, PaneViewType.TOUCH],
};

export function getPaneViewsFor(type) {
  return INDICATOR_PANEVIEWS[type] || INDICATOR_PANEVIEWS.default;
}

const toSec = (t) => (typeof t === 'number' && t > 2e10 ? Math.floor(t/1000) : t);

export function adaptPayload(type, payload, colorHex) {
  const priceLines = Array.isArray(payload?.price_lines) ? payload.price_lines : [];
  const markersAll = Array.isArray(payload?.markers) ? payload.markers : [];
  const boxes      = Array.isArray(payload?.boxes) ? payload.boxes : [];
  const segments   = Array.isArray(payload?.segments) ? payload.segments : [];
  const polylines  = Array.isArray(payload?.polylines) ? payload.polylines : [];

  const touchPoints = markersAll
    .filter(m => m?.subtype === 'touch' && typeof m?.price === 'number' && m?.time != null)
    .map(m => ({ time: m.time, price: Number(m.price), color: colorHex || m.color, size: m.size ?? 4 }));

  const markers = markersAll.filter(m => m?.subtype !== 'touch')
    .map(m => ({ ...m, time: toSec(m.time) }));

  // normalize times for new types
  const normSegments = segments.map(s => ({
    ...s, x1: toSec(s.x1), x2: toSec(s.x2), color: colorHex || s.color
  }));
  const normPolylines = polylines.map(l => ({
    ...l,
    color: colorHex || l.color,
    points: (l.points || []).map(p => ({ time: toSec(p.time), price: Number(p.price) })),
  }));

  return { priceLines, markers, touchPoints, boxes, segments: normSegments, polylines: normPolylines };
}
