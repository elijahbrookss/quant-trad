export function createVABoxPaneView(timeScaleApi, opts = {}) {
  const { hatchOverlap = true, extendRight = true, outlineFront = false } = opts;
  let boxes = []; // [{ x1, x2, y1, y2, color, border? }]

  const toSec = t => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);
  const withAlpha = (rgba, a) =>
    rgba.replace(/rgba\((\d+),\s*(\d+),\s*(\d+),\s*[^)]+\)/, `rgba($1,$2,$3,${a})`);

  // --- shared draw routine that can render fill/border based on flags
  const paintRects = (ctx, priceToCoordinate, { fill = true, stroke = false, hatch = false }) => {
    const vr = timeScaleApi.getVisibleRange?.();
    const rightT = vr?.to; // epoch seconds
    const pxRects = [];

    for (const b of boxes) {
      const xLeft  = timeScaleApi.timeToCoordinate(toSec(b.x1));
      const xRight = extendRight && rightT != null
        ? timeScaleApi.timeToCoordinate(rightT)
        : timeScaleApi.timeToCoordinate(toSec(b.x2));
      if (xLeft == null || xRight == null) continue;

      const y1 = priceToCoordinate(b.y1);
      const y2 = priceToCoordinate(b.y2);
      if (y1 == null || y2 == null) continue;

      const left   = Math.min(xLeft, xRight);
      const width  = Math.max(1, Math.abs(xRight - xLeft));
      const top    = Math.min(y1, y2);
      const height = Math.abs(y2 - y1);
      const col    = b.color || 'rgba(156,163,175,0.18)';
      const brd    = b.border || { color: 'rgba(100,116,139,.45)', width: 1 };

      pxRects.push({ left, top, width, height, color: col, border: brd });

      if (fill) {
        ctx.fillStyle = col;
        ctx.fillRect(left, top, width, height);
      }
      if (stroke) {
        ctx.strokeStyle = brd.color;
        ctx.lineWidth = brd.width || 1;
        ctx.strokeRect(left + 0.5, top + 0.5, Math.max(0, width - 1), Math.max(0, height - 1));
      }
    }

    if (hatch && hatchOverlap && pxRects.length > 1) {
      for (let i = 0; i < pxRects.length; i++) {
        for (let j = i + 1; j < pxRects.length; j++) {
          const a = pxRects[i], b = pxRects[j];
          const L = Math.max(a.left, b.left);
          const R = Math.min(a.left + a.width, b.left + b.width);
          const T = Math.max(a.top, b.top);
          const B = Math.min(a.top + a.height, b.top + b.height);
          const w = R - L, h = B - T;
          if (w <= 1 || h <= 1) continue;

          ctx.save();
          ctx.beginPath();
          ctx.rect(L, T, w, h);
          ctx.clip();

          ctx.strokeStyle = withAlpha(a.border?.color || a.color || 'rgba(156,163,175,0.18)', 0.5);
          ctx.lineWidth = 1;
          const step = 6;
          for (let x = L - h; x < R; x += step) {
            ctx.beginPath();
            ctx.moveTo(x, T);
            ctx.lineTo(x + h, B);
            ctx.stroke();
          }
          ctx.restore();
        }
      }
    }
  };

  const renderer = {
// Draw boxes in the foreground (visible), but the series itself is created first → low z-order.
    draw: (target, priceToCoordinate) => {
      const ctx = target.useMediaCoordinateSpace(({ context }) => context);
      if (!ctx) return;
      ctx.save();
      // fill + hatch; keep outline off unless you set outlineFront=true
      paintRects(ctx, priceToCoordinate, { fill: true, stroke: !!outlineFront, hatch: true });
      ctx.restore();
    },
    // No background painting (prevents being hidden under pane/grid)
    drawBackground: () => {},
    hitTest: () => null,
  };

  return {
    renderer: () => renderer,
    update: () => {},
    priceValueBuilder: () => [0, 0, 0],
    isWhitespace: () => false,
    defaultOptions() { return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }; },
    destroy: () => {},
    setBoxes(arr) { boxes = Array.isArray(arr) ? arr : []; },
  };
}
