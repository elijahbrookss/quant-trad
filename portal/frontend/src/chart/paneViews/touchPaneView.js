// One custom series row per time, with many dots in row.originalData.points
export function createTouchPaneView(timeScaleApi) {
  let rows = []; // [{ time, originalData: { points: [{ price, color, size }] } }]

  const renderer = {
    draw: (target, priceToCoordinate) => {
      const ctx = target.useMediaCoordinateSpace(({ context }) => context);
      if (!ctx) return;
      ctx.save();

      const toSec = t => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);
      for (const row of rows) {
        const x = timeScaleApi.timeToCoordinate(toSec(row.time));
        if (x == null) continue;
        const pts = row.originalData?.points || [];
        for (const pt of pts) {
          const y = priceToCoordinate(pt.price);
          if (y == null) continue;
          ctx.beginPath();
          ctx.arc(x, y, pt.size ?? 4, 0, Math.PI * 2);
          ctx.fillStyle = pt.color ?? '#6b7280';
          ctx.fill();
        }
      }

      ctx.restore();
    },
    drawBackground: () => {},
    hitTest: () => null,
  };

  return {
    renderer: () => renderer,
    update: () => {},
    priceValueBuilder: (item) => {
      const p = item?.originalData?.points?.[0]?.price ?? 0;
      return [p, p, p];
    },
    isWhitespace: (item) => !(item?.originalData?.points?.length),
    defaultOptions() {
      return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };
    },
    destroy: () => {},

    // our API
    setRows(next) { rows = Array.isArray(next) ? next : []; },
  };
}
