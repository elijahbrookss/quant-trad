import { createTouchPaneView } from './touchPaneView';
import { createVABoxPaneView } from './vaBoxPaneView';
import { createSegmentPaneView } from './segmentPaneView';
import { createPolylinePaneView } from './polylinePaneView';

export const PaneViewType = {
  TOUCH: 'touch',
  VA_BOX: 'va_box',
  SEGMENT: 'segment',
  POLYLINE: 'polyline',
};

export class PaneViewManager {
  constructor(chart) {
    this.chart = chart;
    this.ts = chart.timeScale();
    this.series = new Map();
    this.views = new Map();
  }
  ensure(type) {
    if (this.series.has(type)) return;
    let view;
    if (type === PaneViewType.TOUCH)      view = createTouchPaneView(this.ts);
    else if (type === PaneViewType.VA_BOX)  view = createVABoxPaneView(this.ts, { extendRight:true, hatchOverlap:true });
    else if (type === PaneViewType.SEGMENT) view = createSegmentPaneView(this.ts);
    else if (type === PaneViewType.POLYLINE) view = createPolylinePaneView(this.ts);
    else throw new Error(`Unknown pane view: ${type}`);
    const s = this.chart.addCustomSeries(view, {});
    this.views.set(type, view);
    this.series.set(type, s);
  }
  clearFrame() {
    for (const [type, view] of this.views.entries()) {
      if (type === PaneViewType.TOUCH)    { view.setRows?.([]);   this.series.get(type)?.setData([]); }
      if (type === PaneViewType.VA_BOX)   { view.setBoxes?.([]);  this.series.get(type)?.setData([]); }
      if (type === PaneViewType.SEGMENT)  { view.setSegments?.([]); this.series.get(type)?.setData([]); }
      if (type === PaneViewType.POLYLINE) { view.setPolylines?.([]); this.series.get(type)?.setData([]); }
    }
  }
  destroy() {
    for (const s of this.series.values()) { try { this.chart.removeSeries(s); } catch {} }
    this.series.clear(); this.views.clear();
  }

  setVABlocks(boxes){ this.ensure(PaneViewType.VA_BOX);
    this.views.get(PaneViewType.VA_BOX).setBoxes(boxes || []);
    // seed unique ascending times
    const times = [...new Set((boxes||[]).flatMap(b => [b.x1, b.x2]))].sort((a,b)=>a-b)
      .map(t => ({ time: t, originalData: {} }));
    this.series.get(PaneViewType.VA_BOX).setData(times);
  }
  setSegments(segs){ this.ensure(PaneViewType.SEGMENT);
    this.views.get(PaneViewType.SEGMENT).setSegments(segs || []);
    const times = [...new Set((segs||[]).flatMap(s => [s.x1, s.x2]))].sort((a,b)=>a-b)
      .map(t => ({ time: t, originalData: {} }));
    this.series.get(PaneViewType.SEGMENT).setData(times);
  }
  setPolylines(lines){ this.ensure(PaneViewType.POLYLINE);
    this.views.get(PaneViewType.POLYLINE).setPolylines(lines || []);
    const times = [...new Set((lines||[]).flatMap(l => (l.points||[]).map(p => p.time)))]
      .sort((a,b)=>a-b).map(t => ({ time: t, originalData: {} }));
    this.series.get(PaneViewType.POLYLINE).setData(times);
  }
    setTouchPoints(points) {
    // points: [{ time, price, color, size }]
    this.ensure(PaneViewType.TOUCH);
    // group by time → rows
    const byTime = new Map();
    for (const p of points || []) {
      if (!Number.isFinite(p?.time)) continue;
      if (!byTime.has(p.time)) byTime.set(p.time, []);
      byTime.get(p.time).push({ price: Number(p.price), color: p.color, size: 3 });
    }
    const rows = [...byTime.entries()]
      .map(([time, pts]) => ({ time, originalData: { points: pts } }))
      .sort((a, b) => a.time - b.time);

    this.views.get(PaneViewType.TOUCH).setRows(rows);
    this.series.get(PaneViewType.TOUCH).setData(rows);
  }

  
}
