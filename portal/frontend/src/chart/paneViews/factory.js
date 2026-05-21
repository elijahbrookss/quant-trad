import { createTouchPaneView } from './touchPaneView';
import { createVABoxPaneView } from './vaBoxPaneView';
import { createSegmentPaneView } from './segmentPaneView';
import { buildPolylineSeriesData, createPolylinePaneView } from './polylinePaneView';
import { createSignalBubblePaneView } from './signalBubblePaneView';
import { createMarkerPaneView } from './markerPaneView';
import { createHighlightBandPaneView } from './highlightBandPaneView';
import { getPaneDefinition, listPaneDefinitions, normalizePaneKey } from '../panes/registry.js';

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

const paneSignature = (paneKey, type) => `${String(paneKey || 'price')}::${type}`;

const resolvePaneConfig = (paneKey) => getPaneDefinition(paneKey);

export class PaneViewManager {
  constructor(chart) {
    this.chart = chart;
    this.ts = chart.timeScale();
    this.series = new Map();
    this.views = new Map();
    this.vaBoxState = new Map();
    // Pre-initialize all overlay series so they sit below candles in z-order.
    // Candles must be added to the chart AFTER PaneViewManager is constructed.
    this.ensure(PaneViewType.VA_BOX, 'price');
    this.ensure(PaneViewType.SEGMENT, 'price');
    this.ensure(PaneViewType.POLYLINE, 'price');
    this.ensure(PaneViewType.HIGHLIGHT_BAND, 'price');
    this.ensure(PaneViewType.TOUCH, 'price');
    this.ensure(PaneViewType.SIGNAL_BUBBLE, 'price');
    this.ensure(PaneViewType.MARKER, 'price');
    this.syncActivePanes(['price']);
  }
  _ensurePaneIndex(index) {
    while (this.chart.panes().length <= index) {
      this.chart.addPane();
    }
  }
  syncActivePanes(activePaneKeys = ['price']) {
    const requested = new Set((activePaneKeys || []).map((paneKey) => normalizePaneKey(paneKey)));
    requested.add('price');

    const requestedConfigs = [...requested]
      .map((paneKey) => ({ paneKey, config: resolvePaneConfig(paneKey) }))
      .sort((a, b) => b.config.index - a.config.index);

    for (const { paneKey, config } of requestedConfigs) {
      this._ensurePaneIndex(config.index);
    }

    const removablePanes = listPaneDefinitions()
      .filter((config) => config.key !== 'price' && !requested.has(config.key) && this.chart.panes().length > config.index)
      .sort((left, right) => right.index - left.index);

    removablePanes.forEach((config) => {
      const paneKey = config.key;
      try {
        this.chart.removePane(config.index);
      } catch {
        // Ignore pane removal failures and continue cleaning local state.
      }
      for (const signature of [...this.series.keys()]) {
        if (!signature.startsWith(`${paneKey}::`)) continue;
        this.series.delete(signature);
        this.views.delete(signature);
      }
      this.vaBoxState.delete(paneKey);
    });

    const hasAuxiliaryPane = [...requested].some((paneKey) => paneKey !== 'price');
    const pricePane = this.chart.panes()[0];
    if (pricePane && typeof pricePane.setStretchFactor === 'function') {
      pricePane.setStretchFactor(hasAuxiliaryPane ? getPaneDefinition('price').stretchFactor : 1);
    }

    [...requested]
      .filter((paneKey) => paneKey !== 'price')
      .forEach((paneKey) => {
        const config = resolvePaneConfig(paneKey);
        const paneApi = this.chart.panes()[config.index];
        if (paneApi && typeof paneApi.setStretchFactor === 'function') {
          paneApi.setStretchFactor(config.stretchFactor);
        }
      });
  }
  ensure(type, paneKey = 'price') {
    const signature = paneSignature(paneKey, type);
    if (this.series.has(signature)) return;
    const paneConfig = resolvePaneConfig(paneKey);
    this._ensurePaneIndex(paneConfig.index);
    const paneApi = this.chart.panes()[paneConfig.index];
    if (paneApi && typeof paneApi.setStretchFactor === 'function') {
      paneApi.setStretchFactor(paneConfig.stretchFactor);
    }
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
    }, paneConfig.index);
    this.views.set(signature, view);
    this.series.set(signature, s);
  }
  clearFrame() {
    for (const [signature, view] of this.views.entries()) {
      const [, type] = signature.split('::');
      if (type === PaneViewType.TOUCH)    { view.setRows?.([]);   this.series.get(signature)?.setData([]); }
      if (type === PaneViewType.VA_BOX)   {
        const [paneKey] = signature.split('::');
        this.vaBoxState.set(paneKey, { boxes: [], lastSeriesTime: null, barSpacing: null });
        this._syncVABlocks(paneKey);
      }
      if (type === PaneViewType.SEGMENT)  { view.setSegments?.([]); this.series.get(signature)?.setData([]); }
      if (type === PaneViewType.POLYLINE) { view.setPolylines?.([]); this.series.get(signature)?.setData([]); }
      if (type === PaneViewType.SIGNAL_BUBBLE) { view.setBubbles?.([]); this.series.get(signature)?.setData([]); }
      if (type === PaneViewType.MARKER) { view.setMarkers?.([]); this.series.get(signature)?.setData([]); }
      if (type === PaneViewType.HIGHLIGHT_BAND) { view.setBands?.([]); this.series.get(signature)?.setData([]); }
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
    this.vaBoxState = new Map();
  }

  setVABlocks(boxes, opts = {}, paneKey = 'price'){
    this.ensure(PaneViewType.VA_BOX, paneKey);

    const current = this.vaBoxState.get(paneKey) || { boxes: [], lastSeriesTime: null, barSpacing: null };
    current.boxes = Array.isArray(boxes) ? boxes : [];
    if (opts && Object.prototype.hasOwnProperty.call(opts, 'lastSeriesTime')) {
      current.lastSeriesTime = opts.lastSeriesTime;
    }
    if (opts && Object.prototype.hasOwnProperty.call(opts, 'barSpacing')) {
      current.barSpacing = opts.barSpacing;
    }
    this.vaBoxState.set(paneKey, current);

    this._syncVABlocks(paneKey);
  }

  updateVABlockContext(opts = {}, paneKey = 'price') {
    const signature = paneSignature(paneKey, PaneViewType.VA_BOX);
    if (!this.views.has(signature)) return;
    const current = this.vaBoxState.get(paneKey) || { boxes: [], lastSeriesTime: null, barSpacing: null };
    if (Object.prototype.hasOwnProperty.call(opts, 'lastSeriesTime')) {
      current.lastSeriesTime = opts.lastSeriesTime;
    }
    if (Object.prototype.hasOwnProperty.call(opts, 'barSpacing')) {
      current.barSpacing = opts.barSpacing;
    }
    this.vaBoxState.set(paneKey, current);
    this._syncVABlocks(paneKey);
  }

  _syncVABlocks(paneKey = 'price') {
    const signature = paneSignature(paneKey, PaneViewType.VA_BOX);
    if (!this.views.has(signature)) {
      return;
    }

    const view = this.views.get(signature);
    const series = this.series.get(signature);
    if (!view || !series) {
      return;
    }

    const state = this.vaBoxState.get(paneKey) || { boxes: [], lastSeriesTime: null, barSpacing: null };
    const boxes = state.boxes || [];
    const { lastSeriesTime } = state;

    view.setBoxes(boxes);

    const normalizedLast = toSec(lastSeriesTime);

    const seriesTimes = [...new Set(boxes.flatMap(b => [toSec(b.x1), toSec(b.x2), normalizedLast]))]
      .filter((t) => typeof t === 'number' && Number.isFinite(t))
      .sort((a, b) => a - b);

    series.setData(seriesTimes.map(t => ({ time: t, originalData: {} })));
  }
  setSegments(segs, paneKey = 'price'){ this.ensure(PaneViewType.SEGMENT, paneKey);
    const signature = paneSignature(paneKey, PaneViewType.SEGMENT);
    this.views.get(signature).setSegments(segs || []);
    const times = [...new Set((segs||[]).flatMap(s => [toSec(s.x1), toSec(s.x2)]))]
       .filter(Number.isFinite).sort((a,b)=>a-b)
       .map(t => ({ time: t, originalData: {} }));
    this.series.get(signature).setData(times);
  }
  setPolylines(lines, paneKey = 'price'){ this.ensure(PaneViewType.POLYLINE, paneKey);
    const signature = paneSignature(paneKey, PaneViewType.POLYLINE);
    this.views.get(signature).setPolylines(lines || []);
    this.series.get(signature).setData(buildPolylineSeriesData(lines || []));
  }
  setSignalBubbles(bubs, paneKey = 'price'){ this.ensure(PaneViewType.SIGNAL_BUBBLE, paneKey);
    const signature = paneSignature(paneKey, PaneViewType.SIGNAL_BUBBLE);
    const normalized = (bubs || []).map(b => ({ ...b, time: toSec(b.time) }));
    this.views.get(signature).setBubbles(normalized);

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

    this.series.get(signature).setData(data);
  }
    setTouchPoints(points, paneKey = 'price') {
    // points: [{ time, price, color, size }]
    this.ensure(PaneViewType.TOUCH, paneKey);
    const signature = paneSignature(paneKey, PaneViewType.TOUCH);
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

    this.views.get(signature).setRows(rows);
    this.series.get(signature).setData(rows);
  }

  setMarkers(markers, paneKey = 'price') {
    // markers: [{ time, price, color, size, shape, text, position }]
    this.ensure(PaneViewType.MARKER, paneKey);
    const signature = paneSignature(paneKey, PaneViewType.MARKER);
    const normalized = (markers || []).map(m => ({ ...m, time: toSec(m.time) }));
    this.views.get(signature).setMarkers(normalized);

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

    this.series.get(signature).setData(data);
  }

  setHighlightBands(bands, paneKey = 'price') {
    this.ensure(PaneViewType.HIGHLIGHT_BAND, paneKey);
    const signature = paneSignature(paneKey, PaneViewType.HIGHLIGHT_BAND);
    const normalized = (bands || []).map((b) => ({
      ...b,
      x1: toSec(b.x1),
      x2: toSec(b.x2),
    }));
    this.views.get(signature).setBands(normalized);

    const times = [...new Set(normalized.flatMap((b) => [b.x1, b.x2]))]
      .filter(Number.isFinite)
      .sort((a, b) => a - b)
      .map((time) => ({ time, originalData: {} }));

    this.series.get(signature).setData(times);
  }


}
