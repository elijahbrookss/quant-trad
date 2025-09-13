export function createSegmentPaneView(timeScaleApi) {
  let segs = []; // [{ x1,x2,y1,y2,color,lineWidth,lineStyle }]

  const renderer = {
    draw: (target, priceToCoordinate) => {
      const ctx = target.useMediaCoordinateSpace(({ context }) => context);
      if (!ctx) return;
      ctx.save();
      for (const s of segs) {
        const x1 = timeScaleApi.timeToCoordinate(s.x1);
        const x2 = timeScaleApi.timeToCoordinate(s.x2);
        const y1 = priceToCoordinate(s.y1);
        const y2 = priceToCoordinate(s.y2);
        if (x1 == null || x2 == null || y1 == null || y2 == null) continue;

        ctx.beginPath();
        if (s.lineStyle === 2) ctx.setLineDash([6, 4]); else ctx.setLineDash([]);
        ctx.strokeStyle = s.color || 'rgba(107,114,128,1)';
        ctx.lineWidth = s.lineWidth ?? 1;
        ctx.moveTo(x1, y1);
        ctx.lineTo(x2, y2);
        ctx.stroke();
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
    defaultOptions() { return { priceLineVisible:false, lastValueVisible:false, crosshairMarkerVisible:false }; },
    destroy: () => {},
    setSegments(arr) { segs = Array.isArray(arr) ? arr : []; },
  };
}
