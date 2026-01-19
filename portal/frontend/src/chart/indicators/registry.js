import { PaneViewType } from '../paneViews/factory';

const INDICATOR_PANEVIEWS = {
  default: [PaneViewType.TOUCH],
  pivot_level: [PaneViewType.SIGNAL_BUBBLE, PaneViewType.TOUCH],
  market_profile: [PaneViewType.VA_BOX, PaneViewType.TOUCH],
  trendline: [PaneViewType.SEGMENT, PaneViewType.TOUCH],
  vwap: [PaneViewType.POLYLINE, PaneViewType.TOUCH],
  bot_trade_rays: [PaneViewType.SEGMENT],
};

export function getPaneViewsFor(type) {
  return INDICATOR_PANEVIEWS[type] || INDICATOR_PANEVIEWS.default;
}

const toSec = (value) => {
  if (value == null) return value;
  if (typeof value === 'number') {
    return value > 2e10 ? Math.floor(value / 1000) : value;
  }
  const parsed = Date.parse(value);
  if (Number.isFinite(parsed)) {
    return Math.floor(parsed / 1000);
  }
  return value;
};


const toFiniteNumber = (value) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const toRgba = (hex, alpha = 1.0) => {
  if (!hex || typeof hex !== 'string') return undefined;
  // If already rgba, just return it
  if (hex.startsWith('rgba(')) return hex;
  const normalized = hex.trim().replace('#', '');
  if (!(normalized.length === 3 || normalized.length === 6)) return undefined;
  const expand = (value) => value.split('').map((c) => c + c).join('');
  const full = normalized.length === 3 ? expand(normalized) : normalized;
  const r = parseInt(full.slice(0, 2), 16);
  const g = parseInt(full.slice(2, 4), 16);
  const b = parseInt(full.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

export function adaptPayload(type, payload, colorHex) {
  const priceLines = (Array.isArray(payload?.price_lines) ? payload.price_lines : [])
    .filter((line) => {
      // Filter out TP/SL lines from overlays - these are created from trade data in BotLensChart
      const title = (line?.title || '').toLowerCase();
      if (title.includes('tp') || title.includes('target') || title.includes('sl') || title.includes('stop')) {
        return false;
      }
      return true;
    })
    .map((line) => ({
      ...line,
      price: toFiniteNumber(line?.price),
    }))
    .filter((line) => line.price !== null);

  const markersAll = Array.isArray(payload?.markers) ? payload.markers : [];
  const boxesRaw   = Array.isArray(payload?.boxes) ? payload.boxes : [];
  const segments   = Array.isArray(payload?.segments) ? payload.segments : [];
  const polylines  = Array.isArray(payload?.polylines) ? payload.polylines : [];
  const bubbles    = Array.isArray(payload?.bubbles) ? payload.bubbles : [];

  const touchPoints = markersAll
    .filter((m) => m?.subtype === 'touch' && m?.time != null)
    .map((m) => ({
      time: toSec(m.time),
      price: toFiniteNumber(m.price),
      color: colorHex || m.color,
      size: m.size ?? 4,
    }))
    .filter((point) => Number.isFinite(point.time) && Number.isFinite(point.price));

  const markers = markersAll
    .filter((m) => m?.subtype !== 'touch' && m?.subtype !== 'bubble')
    .map((m) => ({ ...m, time: toSec(m.time) }))
    .filter((marker) => Number.isFinite(marker.time));

  const signalBubbles = bubbles
    .concat(markersAll.filter((m) => m?.subtype === 'bubble'))
    .map((b) => ({ ...b, time: toSec(b.time) }))
    .filter((bubble) => Number.isFinite(bubble.time));

  // normalize times for new types
  const normSegments = segments
    .map((s) => ({
      ...s,
      x1: toSec(s.x1),
      x2: toSec(s.x2),
      y1: toFiniteNumber(s.y1 ?? s.price ?? s.value),
      y2: toFiniteNumber(s.y2 ?? s.price ?? s.value),
      color: colorHex || s.color,
    }))
    .filter(
      (segment) =>
        Number.isFinite(segment.x1) &&
        Number.isFinite(segment.x2) &&
        (Number.isFinite(segment.y1) || Number.isFinite(segment.y2)),
    );

  const normPolylines = polylines
    .map((l) => ({
      ...l,
      color: colorHex || l.color,
      points: (l.points || [])
        .map((p) => ({ time: toSec(p.time), price: toFiniteNumber(p.price) }))
        .filter((pt) => Number.isFinite(pt.time) && Number.isFinite(pt.price)),
    }))
    .filter((line) => line.points.length > 0);

  const normBoxes = boxesRaw
    .map((box) => {
      const baseColor = colorHex || box.color;
      return {
        ...box,
        x1: toSec(box.x1 ?? box.start ?? box.start_date ?? box.startDate),
        x2: toSec(box.x2 ?? box.end ?? box.end_date ?? box.endDate),
        y1: toFiniteNumber(box.y1 ?? box.val ?? box.VAL),
        y2: toFiniteNumber(box.y2 ?? box.vah ?? box.VAH),
        // Apply subtle opacity to make boxes see-through
        color: toRgba(baseColor, 0.08) || box.color,
        border: box.border || {
          color: toRgba(baseColor, 0.25) || baseColor,
          width: 1,
        },
      };
    })
    .filter((box) => Number.isFinite(box.x1) && Number.isFinite(box.y1) && Number.isFinite(box.y2));

  return {
    priceLines,
    markers,
    touchPoints,
    boxes: normBoxes,
    segments: normSegments,
    polylines: normPolylines,
    bubbles: signalBubbles,
  };
}
