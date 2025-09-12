export function createVABoxPaneView(timeScaleApi) {
  let boxes = []; // [{ x1, x2, y1, y2, color, border? }]

  const renderer = {
    draw: (target, priceToCoordinate) => {
      const ctx = target.useMediaCoordinateSpace(({ context }) => context);
      if (!ctx) return;
      ctx.save();

      const toSec = t => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

      for (const b of boxes) {
        const x1 = timeScaleApi.timeToCoordinate(toSec(b.x1));
        const x2 = timeScaleApi.timeToCoordinate(toSec(b.x2));
        if (x1 == null || x2 == null) continue;

        const y1 = priceToCoordinate(b.y1);
        const y2 = priceToCoordinate(b.y2);
        if (y1 == null || y2 == null) continue;

        const left = Math.min(x1, x2);
        const width = Math.max(1, Math.abs(x2 - x1));
        const top = Math.min(y1, y2);
        const height = Math.abs(y2 - y1);

        ctx.fillStyle = b.color || 'rgba(156,163,175,0.18)';
        ctx.fillRect(left, top, width, height);

        if (b.border) {
          ctx.strokeStyle = b.border.color || 'rgba(100,116,139,.45)';
          ctx.lineWidth = b.border.width || 1;
          ctx.strokeRect(left + .5, top + .5, Math.max(0, width - 1), Math.max(0, height - 1));
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
    priceValueBuilder: () => [0,0,0],
    isWhitespace: () => false,
    defaultOptions() {
      return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };
    },
    destroy: () => {},

    setBoxes(arr) { boxes = Array.isArray(arr) ? arr : []; },
  };
}
