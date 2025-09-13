export function createPolylinePaneView(timeScaleApi) {
  let lines = []; // [{ points:[{time,price}], color, lineWidth, lineStyle, role?, band?, side?, shade? }]

  const toRGBA = (c, a = 0.05) => {
    if (!c) return `rgba(107,114,128,${a})`;
    if (c.startsWith('rgba(')) return c.replace(/rgba\((\d+),\s*(\d+),\s*(\d+),\s*[^)]+\)/, `rgba($1,$2,$3,${a})`);
    if (c.startsWith('rgb('))  return c.replace(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/,       `rgba($1,$2,$3,${a})`);
    if (c[0] === '#') {
      const v = c.slice(1);
      const [r,g,b] = (v.length === 3)
        ? v.split('').map(x => parseInt(x + x, 16))
        : [parseInt(v.slice(0,2),16), parseInt(v.slice(2,4),16), parseInt(v.slice(4,6),16)];
      return `rgba(${r},${g},${b},${a})`;
    }
    return `rgba(107,114,128,${a})`;
  };

  const drawStroke = (ctx, pts, priceToCoordinate, color, width, style) => {
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
    ctx.setLineDash([]);
  };

  // Create a filled polygon between two time-aligned polylines
  const drawBandFill = (ctx, upper, lower, priceToCoordinate, color) => {
    // Build a time → {ux,uy} / {lx,ly} map; keep only times present in both
    const uMap = new Map();
    for (const p of upper.points || []) {
      const x = timeScaleApi.timeToCoordinate(p.time);
      const y = priceToCoordinate(p.price);
      if (x != null && y != null) uMap.set(p.time, { x, y });
    }
    const pair = [];
    for (const p of lower.points || []) {
      const x = timeScaleApi.timeToCoordinate(p.time);
      const y = priceToCoordinate(p.price);
      if (x != null && y != null && uMap.has(p.time)) {
        const u = uMap.get(p.time);
        pair.push({ ux: u.x, uy: u.y, lx: x, ly: y, t: p.time });
      }
    }
    if (pair.length < 2) return;

    // Sort by time just in case
    pair.sort((a, b) => a.t - b.t);

    ctx.save();
    ctx.beginPath();
    // trace along upper forward
    for (let i = 0; i < pair.length; i++) {
      const { ux, uy } = pair[i];
      if (i === 0) ctx.moveTo(ux, uy); else ctx.lineTo(ux, uy);
    }
    // then along lower backward
    for (let i = pair.length - 1; i >= 0; i--) {
      const { lx, ly } = pair[i];
      ctx.lineTo(lx, ly);
    }
    ctx.closePath();
    ctx.fillStyle = toRGBA(color, 0.02); // translucent fill
    ctx.fill();
    ctx.restore();
  };

  const renderer = {
    draw: (target, priceToCoordinate) => {
      const ctx = target.useMediaCoordinateSpace(({ context }) => context);
      if (!ctx) return;
      ctx.save();

      // 1) Group shadable lines by band (ignore main VWAP)
      const bands = new Map(); // bandId -> { upper, lower, color }
      for (const l of lines) {
        if (!l || !l.shade) continue;
        if (l.role === 'main') continue;
        const key = l.band ?? null;
        if (key == null) continue;
        const g = bands.get(key) || {};
        if (l.side === 'upper') g.upper = l;
        else if (l.side === 'lower') g.lower = l;
        if (!g.color) g.color = l.color;
        bands.set(key, g);
      }

      // 2) Draw fills first so strokes sit on top
      for (const [, g] of bands) {
        if (g.upper && g.lower) drawBandFill(ctx, g.upper, g.lower, priceToCoordinate, g.color);
      }

      // 3) Stroke all polylines (main + bands)
      for (const l of lines) {
        drawStroke(ctx, l.points || [], priceToCoordinate, l.color, l.lineWidth, l.lineStyle);
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
    setPolylines(arr) { lines = Array.isArray(arr) ? arr : []; },
  };
}
