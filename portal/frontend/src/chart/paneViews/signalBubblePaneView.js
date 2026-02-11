const toSec = (t) => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const hexToRgba = (hex, alpha) => {
  if (typeof hex !== 'string') return null;
  const value = hex.trim().replace('#', '');
  if (value.length !== 6) return null;
  const r = Number.parseInt(value.slice(0, 2), 16);
  const g = Number.parseInt(value.slice(2, 4), 16);
  const b = Number.parseInt(value.slice(4, 6), 16);
  if ([r, g, b].some((n) => Number.isNaN(n))) return null;
  const a = Math.min(Math.max(alpha, 0), 1);
  return `rgba(${r},${g},${b},${a.toFixed(2)})`;
};

const drawRoundedRect = (ctx, x, y, width, height, radius, fill, stroke) => {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
  ctx.fillStyle = fill;
  ctx.fill();
  ctx.lineWidth = 1;
  ctx.strokeStyle = stroke;
  ctx.stroke();
};

export function createSignalBubblePaneView(timeScaleApi) {
  let bubbles = [];

  const renderer = {
    draw(target, priceToCoordinate) {
      const { context: ctx, horizontalPixelRatio: hpr, verticalPixelRatio: vpr, bitmapSize } =
        target.useBitmapCoordinateSpace(({ context, horizontalPixelRatio, verticalPixelRatio, bitmapSize: size }) => ({
          context,
          horizontalPixelRatio,
          verticalPixelRatio,
          bitmapSize: size,
        }));
      if (!ctx) return;

      const widthPx = bitmapSize?.width ?? ctx.canvas.width;
      const heightPx = bitmapSize?.height ?? ctx.canvas.height;

      const padX = 8 * hpr;
      const padY = 5 * vpr;
      const radius = 8 * Math.min(hpr, vpr);
      const markerRadius = 2.5 * Math.min(hpr, vpr);
      const labelFontPx = 10 * vpr;
      const gap = 10 * vpr;

      ctx.save();
      ctx.textBaseline = 'top';
      ctx.font = `600 ${labelFontPx}px "Inter", "Segoe UI", sans-serif`;

      for (const bubble of bubbles) {
        const rawTime = toSec(bubble?.time);
        const px = timeScaleApi.timeToCoordinate(rawTime);
        if (px == null) continue;
        const py = priceToCoordinate(Number(bubble?.price));
        if (py == null) continue;

        const canvasX = px * hpr;
        if (canvasX < -24 * hpr || canvasX > widthPx + 24 * hpr) continue;

        const accent = bubble?.accentColor ?? '#38bdf8';
        const background = bubble?.backgroundColor ?? hexToRgba(accent, 0.16) ?? 'rgba(30,41,59,0.8)';
        const textColor = bubble?.textColor ?? '#eef2ff';

        const rawLabel = typeof bubble?.label === 'string' ? bubble.label.trim() : '';
        const label = rawLabel || 'Signal';
        const textWidth = ctx.measureText(label).width;
        const tagWidth = textWidth + padX * 2;
        const tagHeight = labelFontPx + padY * 2;

        const direction = bubble?.direction === 'below' ? 'below' : 'above';
        let x = canvasX - tagWidth / 2;
        x = clamp(x, 8 * hpr, Math.max(8 * hpr, widthPx - tagWidth - 8 * hpr));

        let y = direction === 'above'
          ? py * vpr - tagHeight - gap
          : py * vpr + gap;
        y = clamp(y, 8 * vpr, Math.max(8 * vpr, heightPx - tagHeight - 8 * vpr));

        drawRoundedRect(ctx, x, y, tagWidth, tagHeight, radius, background, accent);

        ctx.fillStyle = textColor;
        ctx.fillText(label, x + padX, y + padY);

        ctx.beginPath();
        ctx.fillStyle = accent;
        const markerY = direction === 'above' ? y + tagHeight + 3 * vpr : y - 3 * vpr;
        ctx.arc(clamp(canvasX, x + 4 * hpr, x + tagWidth - 4 * hpr), markerY, markerRadius, 0, Math.PI * 2);
        ctx.fill();
      }

      ctx.restore();
    },
    drawBackground() {},
    hitTest() { return null; },
  };

  return {
    renderer: () => renderer,
    update: () => {},
    priceValueBuilder: () => [NaN, NaN, NaN],
    isWhitespace: (item) => !(item?.originalData?.bubbles?.length),
    defaultOptions() {
      return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };
    },
    destroy: () => {},
    setBubbles(next) {
      bubbles = Array.isArray(next) ? next : [];
    },
  };
}
