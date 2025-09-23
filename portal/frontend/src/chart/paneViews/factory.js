import { createTouchPaneView } from './touchPaneView';
import { createVABoxPaneView } from './vaBoxPaneView';
import { createSegmentPaneView } from './segmentPaneView';
import { createPolylinePaneView } from './polylinePaneView';
import { createSignalBubblePaneView } from './signalBubblePaneView';

const toSec = (t) => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

export const PaneViewType = {
  TOUCH: 'touch',
  VA_BOX: 'va_box',
  SEGMENT: 'segment',
  POLYLINE: 'polyline',
  SIGNAL_BUBBLE: 'signal_bubble',
};

export class PaneViewManager {
  constructor(chart) {
    this.chart = chart;
    this.ts = chart.timeScale();
    this.series = new Map();
    this.views = new Map();
    this.ensure(PaneViewType.VA_BOX); // create VA boxes first so they are in back
  }
  ensure(type) {
    if (this.series.has(type)) return;
    let view;
    if (type === PaneViewType.TOUCH)      view = createTouchPaneView(this.ts);
    else if (type === PaneViewType.VA_BOX)  view = createVABoxPaneView(this.ts, { extendRight: true, hatchOverlap: false, outlineFront: true });
    else if (type === PaneViewType.SEGMENT) view = createSegmentPaneView(this.ts);
    else if (type === PaneViewType.POLYLINE) view = createPolylinePaneView(this.ts);
    else if (type === PaneViewType.SIGNAL_BUBBLE) view = createSignalBubblePaneView(this.ts);
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
      if (type === PaneViewType.VA_BOX)   { view.setBoxes?.([]);  this.series.get(type)?.setData([]); }
      if (type === PaneViewType.SEGMENT)  { view.setSegments?.([]); this.series.get(type)?.setData([]); }
      if (type === PaneViewType.POLYLINE) { view.setPolylines?.([]); this.series.get(type)?.setData([]); }
      if (type === PaneViewType.SIGNAL_BUBBLE) { view.setBubbles?.([]); this.series.get(type)?.setData([]); }
    }
  }
  destroy() {
    for (const s of this.series.values()) { try { this.chart.removeSeries(s); } catch {} }
    this.series.clear(); this.views.clear();
  }

  setVABlocks(boxes){ this.ensure(PaneViewType.VA_BOX);
    const view = this.views.get(PaneViewType.VA_BOX);
    view.setBoxes(boxes || []);

    const rawTimes = [...new Set((boxes||[]).flatMap(b => [toSec(b.x1), toSec(b.x2)]))]
      .filter(Number.isFinite)
      .sort((a,b)=>a-b);

    let smallestStep = Infinity;
    for (let i = 1; i < rawTimes.length; i++) {
      const step = rawTimes[i] - rawTimes[i - 1];
      if (step > 0 && step < smallestStep) smallestStep = step;
    }

    const fallbackStep = 60; // 1 minute padding if we cannot infer spacing
    const extensionStep = Number.isFinite(smallestStep) && smallestStep > 0 ? smallestStep : fallbackStep;

    const rightEdge = Math.max(
      ...((boxes || [])
        .map(b => toSec(b.x2))
        .filter((t) => typeof t === 'number' && Number.isFinite(t))),
      -Infinity,
    );

    const paddedRightEdge = Number.isFinite(rightEdge)
      ? rightEdge + (extensionStep * 0.25)
      : null;

    const seriesTimes = rawTimes.slice();
    if (Number.isFinite(paddedRightEdge)) {
      seriesTimes.push(paddedRightEdge);
      seriesTimes.sort((a, b) => a - b);
    }

    const series = this.series.get(PaneViewType.VA_BOX);
    series.setData(seriesTimes.map(t => ({ time: t, originalData: {} })));

    view.setRightEdgeTime(Number.isFinite(paddedRightEdge) ? paddedRightEdge : (Number.isFinite(rightEdge) ? rightEdge : null));
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

  
}