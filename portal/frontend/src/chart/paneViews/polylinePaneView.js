export function createPolylinePaneView(timeScaleApi) {
  let lines = []; // [{ points:[{time,price}], color, lineWidth, lineStyle }]

  const drawLine = (ctx, pts, priceToCoordinate, color, width, style) => {
    let started = false;
    if (style === 2) ctx.setLineDash([6,4]); else ctx.setLineDash([]);
    ctx.lineWidth = width ?? 1;
    ctx.strokeStyle = color || 'rgba(107,114,128,1)';

    ctx.beginPath();
    for (const p of pts) {
      const x = timeScaleApi.timeToCoordinate(p.time);
      const y = priceToCoordinate(p.price);
      if (x == null || y == null) continue;
      if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
    }
    if (started) ctx.stroke();
  };

  const renderer = {
    draw: (target, priceToCoordinate) => {
      const ctx = target.useMediaCoordinateSpace(({ context }) => context);
      if (!ctx) return;
      ctx.save();
      for (const l of lines) drawLine(ctx, l.points || [], priceToCoordinate, l.color, l.lineWidth, l.lineStyle);
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
    defaultOptions() { return { priceLineVisible:false, lastValueVisible:false, crosshairMarkerVisible:false }; },
    destroy: () => {},
    setPolylines(arr) { lines = Array.isArray(arr) ? arr : []; },
  };
}
