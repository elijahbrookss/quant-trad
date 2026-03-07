import { createTouchPaneView } from './touchPaneView';
import { createVABoxPaneView } from './vaBoxPaneView';
import { createSegmentPaneView } from './segmentPaneView';
import { createPolylinePaneView } from './polylinePaneView';
import { createSignalBubblePaneView } from './signalBubblePaneView';
import { createMarkerPaneView } from './markerPaneView';
import { createHighlightBandPaneView } from './highlightBandPaneView';

const toSec = (t) => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

export const PaneViewType = {
  TOUCH: 'touch',
  VA_BOX: 'va_box',
  SEGMENT: 'segment',
  POLYLINE: 'polyline',
  SIGNAL_BUBBLE: 'signal_bubble',
  MARKER: 'marker',
  HIGHLIGHT_BAND: 'highlight_band',
};

export class PaneViewManager {
  constructor(chart) {
    this.chart = chart;
    this.ts = chart.timeScale();
    this.series = new Map();
    this.views = new Map();
    this.vaBoxState = { boxes: [], lastSeriesTime: null, barSpacing: null };
    // Pre-initialize all overlay series so they sit below candles in z-order.
    // Candles must be added to the chart AFTER PaneViewManager is constructed.
    this.ensure(PaneViewType.VA_BOX);
    this.ensure(PaneViewType.SEGMENT);
    this.ensure(PaneViewType.POLYLINE);
    this.ensure(PaneViewType.HIGHLIGHT_BAND);
    this.ensure(PaneViewType.TOUCH);
    this.ensure(PaneViewType.SIGNAL_BUBBLE);
    this.ensure(PaneViewType.MARKER);
  }
  ensure(type) {
    if (this.series.has(type)) return;
    let view;
    if (type === PaneViewType.TOUCH)      view = createTouchPaneView(this.ts);
    else if (type === PaneViewType.VA_BOX)  view = createVABoxPaneView(this.ts, { hatchOverlap: true, outlineFront: true });
    else if (type === PaneViewType.SEGMENT) view = createSegmentPaneView(this.ts);
    else if (type === PaneViewType.POLYLINE) view = createPolylinePaneView(this.ts);
    else if (type === PaneViewType.SIGNAL_BUBBLE) view = createSignalBubblePaneView(this.ts);
    else if (type === PaneViewType.MARKER) view = createMarkerPaneView(this.ts);
    else if (type === PaneViewType.HIGHLIGHT_BAND) view = createHighlightBandPaneView(this.ts);
    else throw new Error(`Unknown pane view: ${type}`);
    const base = view.defaultOptions?.() ?? {};
    const s = this.chart.addCustomSeries(view, {
      ...base,
      priceScaleId: 'right',            // or mainSeries.priceScale().id()
      lastValueVisible: false,
      priceLineVisible: false,
    });
    this.views.set(type, view);
    this.series.set(type, s);
  }
  clearFrame() {
    for (const [type, view] of this.views.entries()) {
      if (type === PaneViewType.TOUCH)    { view.setRows?.([]);   this.series.get(type)?.setData([]); }
      if (type === PaneViewType.VA_BOX)   {
        this.vaBoxState.boxes = [];
        this._syncVABlocks();
      }
      if (type === PaneViewType.SEGMENT)  { view.setSegments?.([]); this.series.get(type)?.setData([]); }
      if (type === PaneViewType.POLYLINE) { view.setPolylines?.([]); this.series.get(type)?.setData([]); }
      if (type === PaneViewType.SIGNAL_BUBBLE) { view.setBubbles?.([]); this.series.get(type)?.setData([]); }
      if (type === PaneViewType.MARKER) { view.setMarkers?.([]); this.series.get(type)?.setData([]); }
    }
  }
  destroy() {
    for (const s of this.series.values()) {
      try { this.chart.removeSeries(s); }
      catch {
        // swallow errors when series already detached
      }
    }
    this.series.clear(); this.views.clear();
    this.vaBoxState = { boxes: [], lastSeriesTime: null, barSpacing: null };
  }

  setVABlocks(boxes, opts = {}){
    this.ensure(PaneViewType.VA_BOX);

    this.vaBoxState.boxes = Array.isArray(boxes) ? boxes : [];
    if (opts && Object.prototype.hasOwnProperty.call(opts, 'lastSeriesTime')) {
      this.vaBoxState.lastSeriesTime = opts.lastSeriesTime;
    }
    if (opts && Object.prototype.hasOwnProperty.call(opts, 'barSpacing')) {
      this.vaBoxState.barSpacing = opts.barSpacing;
    }

    this._syncVABlocks();
  }

  updateVABlockContext(opts = {}) {
    if (!this.views.has(PaneViewType.VA_BOX)) return;
    if (Object.prototype.hasOwnProperty.call(opts, 'lastSeriesTime')) {
      this.vaBoxState.lastSeriesTime = opts.lastSeriesTime;
    }
    if (Object.prototype.hasOwnProperty.call(opts, 'barSpacing')) {
      this.vaBoxState.barSpacing = opts.barSpacing;
    }
    this._syncVABlocks();
  }

  _syncVABlocks() {
    if (!this.views.has(PaneViewType.VA_BOX)) {
      return;
    }

    const view = this.views.get(PaneViewType.VA_BOX);
    const series = this.series.get(PaneViewType.VA_BOX);
    if (!view || !series) {
      return;
    }

    const boxes = this.vaBoxState.boxes || [];
    const { lastSeriesTime } = this.vaBoxState;

    view.setBoxes(boxes);

    const normalizedLast = toSec(lastSeriesTime);

    const seriesTimes = [...new Set(boxes.flatMap(b => [toSec(b.x1), toSec(b.x2), normalizedLast]))]
      .filter((t) => typeof t === 'number' && Number.isFinite(t))
      .sort((a, b) => a - b);

    series.setData(seriesTimes.map(t => ({ time: t, originalData: {} })));
  }
  setSegments(segs){ this.ensure(PaneViewType.SEGMENT);
    this.views.get(PaneViewType.SEGMENT).setSegments(segs || []);
    const times = [...new Set((segs||[]).flatMap(s => [toSec(s.x1), toSec(s.x2)]))]
       .filter(Number.isFinite).sort((a,b)=>a-b)
       .map(t => ({ time: t, originalData: {} }));
    this.series.get(PaneViewType.SEGMENT).setData(times);
  }
  setPolylines(lines){ this.ensure(PaneViewType.POLYLINE);
    this.views.get(PaneViewType.POLYLINE).setPolylines(lines || []);
    const times = [...new Set((lines||[]).flatMap(
       l => (l.points || []).map(p => toSec(p.time))
     ))]
       .filter(Number.isFinite).sort((a,b)=>a-b)
       .map(t => ({ time: t, originalData: {} }));
    this.series.get(PaneViewType.POLYLINE).setData(times);
  }
  setSignalBubbles(bubs){ this.ensure(PaneViewType.SIGNAL_BUBBLE);
    const normalized = (bubs || []).map(b => ({ ...b, time: toSec(b.time) }));
    this.views.get(PaneViewType.SIGNAL_BUBBLE).setBubbles(normalized);

    const grouped = new Map();
    for (const bubble of normalized) {
      const time = bubble?.time;
      if (!Number.isFinite(time)) continue;
      if (!grouped.has(time)) grouped.set(time, []);
      grouped.get(time).push(bubble);
    }

    const data = [...grouped.entries()]
      .sort((a,b) => a[0] - b[0])
      .map(([time, entries]) => ({ time, originalData: { bubbles: entries } }));

    this.series.get(PaneViewType.SIGNAL_BUBBLE).setData(data);
  }
    setTouchPoints(points) {
    // points: [{ time, price, color, size }]
    this.ensure(PaneViewType.TOUCH);
    // group by time → rows
    const byTime = new Map();
    for (const p of points || []) {
      const t = toSec(p?.time);
      if (!Number.isFinite(t)) continue;
      if (!byTime.has(t)) byTime.set(t, []);
      byTime.get(t).push({ price: Number(p.price), color: p.color, size: 3 });
    }
    const rows = [...byTime.entries()]
      .map(([time, pts]) => ({ time, originalData: { points: pts } }))
      .sort((a, b) => a.time - b.time);

    this.views.get(PaneViewType.TOUCH).setRows(rows);
    this.series.get(PaneViewType.TOUCH).setData(rows);
  }

  setMarkers(markers) {
    // markers: [{ time, price, color, size, shape, text, position }]
    this.ensure(PaneViewType.MARKER);
    const normalized = (markers || []).map(m => ({ ...m, time: toSec(m.time) }));
    this.views.get(PaneViewType.MARKER).setMarkers(normalized);

    // Group by time for series data
    const byTime = new Map();
    for (const m of normalized) {
      const t = m?.time;
      if (!Number.isFinite(t)) continue;
      if (!byTime.has(t)) byTime.set(t, []);
      byTime.get(t).push(m);
    }

    const data = [...byTime.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([time, entries]) => ({ time, originalData: { markers: entries } }));

    this.series.get(PaneViewType.MARKER).setData(data);
  }

  setHighlightBands(bands) {
    this.ensure(PaneViewType.HIGHLIGHT_BAND);
    const normalized = (bands || []).map((b) => ({
      ...b,
      x1: toSec(b.x1),
      x2: toSec(b.x2),
    }));
    this.views.get(PaneViewType.HIGHLIGHT_BAND).setBands(normalized);

    const times = [...new Set(normalized.flatMap((b) => [b.x1, b.x2]))]
      .filter(Number.isFinite)
      .sort((a, b) => a - b)
      .map((time) => ({ time, originalData: {} }));

    this.series.get(PaneViewType.HIGHLIGHT_BAND).setData(times);
  }


}
