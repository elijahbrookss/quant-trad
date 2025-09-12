import { createTouchPaneView } from './touchPaneView';
import { createVABoxPaneView } from './vaBoxPaneView';

// Known pane-view types
export const PaneViewType = {
  TOUCH: 'touch',
  VA_BOX: 'va_box',
};

// Factory/manager per chart instance
export class PaneViewManager {
  constructor(chart) {
    this.chart = chart;
    this.ts = chart.timeScale();

    this.series = new Map(); // type -> series handle
    this.views  = new Map(); // type -> pane view instance
  }

  ensure(type) {
    if (this.series.has(type)) return;
    let view;
    switch (type) {
      case PaneViewType.TOUCH:
        view = createTouchPaneView(this.ts);
        break;
      case PaneViewType.VA_BOX:
        view = createVABoxPaneView(this.ts, { hatchOverlap: true, extendRight: true });
        break;
      default:
        throw new Error(`Unknown pane view: ${type}`);
    }
    const s = this.chart.addCustomSeries(view, {});
    this.views.set(type, view);
    this.series.set(type, s);
  }

  // ---- data writers
  setTouchPoints(points) {
    // points: [{ time, price, color, size }]
    this.ensure(PaneViewType.TOUCH);
    // group by time → rows
    const byTime = new Map();
    for (const p of points || []) {
      if (!Number.isFinite(p?.time)) continue;
      if (!byTime.has(p.time)) byTime.set(p.time, []);
      byTime.get(p.time).push({ price: Number(p.price), color: p.color, size: p.size ?? 4 });
    }
    const rows = [...byTime.entries()]
      .map(([time, pts]) => ({ time, originalData: { points: pts } }))
      .sort((a, b) => a.time - b.time);

    this.views.get(PaneViewType.TOUCH).setRows(rows);
    this.series.get(PaneViewType.TOUCH).setData(rows);
  }

    setVABlocks(boxes) {
      const type = PaneViewType.VA_BOX;
      if (!boxes || boxes.length === 0) {
        if (this.views.get(type)) {
          this.views.get(type).setBoxes([]);
          this.series.get(type)?.setData([]);
        }
        return;
      }

      this.ensure(type);
      this.views.get(type).setBoxes(boxes);

      // ---- build strictly ascending, unique seed times ----
      const toSec = (t) => {
        if (typeof t === 'number') return t > 2e10 ? Math.floor(t / 1000) : t;
        return Math.floor(new Date(t).getTime() / 1000);
      };

      const seen = new Set();
      const uniqTimes = [];
      for (const b of boxes) {
        const t = Math.min(toSec(b.x1), toSec(b.x2 ?? b.x1));
        if (!seen.has(t)) {
          seen.add(t);
          uniqTimes.push(t);
        }
      }
      uniqTimes.sort((a, b) => a - b);

      const seed = uniqTimes.map((t) => ({ time: t, originalData: {} }));
      this.series.get(type).setData(seed);
    }


  clearFrame() {
    if (this.views.get(PaneViewType.TOUCH)) {
      this.views.get(PaneViewType.TOUCH).setRows([]);
      this.series.get(PaneViewType.TOUCH)?.setData([]);
    }
    if (this.views.get(PaneViewType.VA_BOX)) {
      this.views.get(PaneViewType.VA_BOX).setBoxes([]);
      this.series.get(PaneViewType.VA_BOX)?.setData([]);
    }
  }

  destroy() {
    for (const s of this.series.values()) {
      try { this.chart.removeSeries(s); } catch {}
    }
    this.series.clear();
    this.views.clear();
  }
}
