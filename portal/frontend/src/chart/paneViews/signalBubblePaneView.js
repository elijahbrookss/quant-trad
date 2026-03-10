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

const abbreviateSignalLabel = (raw = '', max = 40) => {
  const label = String(raw || '').trim();
  if (!label) return 'SIG';
  return label.length > max ? `${label.slice(0, max)}...` : label;
};

const normalizeBubbleLines = (bubble) => {
  const lines = [];
  const label = abbreviateSignalLabel(bubble?.label || 'Signal', 42);
  if (label) lines.push(label);
  const meta = abbreviateSignalLabel(bubble?.meta || '', 42);
  if (meta) lines.push(meta);
  if (!lines.length) lines.push('Signal');
  return lines.slice(0, 2);
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

      const padX = 7 * hpr;
      const padY = 4 * vpr;
      const radius = 6 * Math.min(hpr, vpr);
      const markerRadius = 2.4 * Math.min(hpr, vpr);
      const labelFontPx = 11 * vpr;
      const metaFontPx = 10 * vpr;
      const gap = 8 * vpr;

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
        const background = bubble?.backgroundColor ?? hexToRgba(accent, 0.14) ?? 'rgba(15,23,42,0.82)';
        const textColor = bubble?.textColor ?? '#dbeafe';

        const lines = normalizeBubbleLines(bubble);
        const widths = [];
        for (let i = 0; i < lines.length; i += 1) {
          ctx.font = i === 0
            ? `600 ${labelFontPx}px "Inter", "Segoe UI", sans-serif`
            : `500 ${metaFontPx}px "Inter", "Segoe UI", sans-serif`;
          widths.push(ctx.measureText(lines[i]).width);
        }
        const textWidth = widths.length ? Math.max(...widths) : 0;
        const lineGap = 2 * vpr;
        const textHeight = labelFontPx + (lines.length > 1 ? (lineGap + metaFontPx) : 0);
        const tagWidth = textWidth + padX * 2;
        const tagHeight = textHeight + padY * 2;

        const direction = bubble?.direction === 'below' ? 'below' : 'above';
        let x = canvasX - tagWidth / 2;
        x = clamp(x, 8 * hpr, Math.max(8 * hpr, widthPx - tagWidth - 8 * hpr));

        let y = direction === 'above'
          ? py * vpr - tagHeight - gap
          : py * vpr + gap;
        y = clamp(y, 8 * vpr, Math.max(8 * vpr, heightPx - tagHeight - 8 * vpr));

        drawRoundedRect(ctx, x, y, tagWidth, tagHeight, radius, background, accent);

        ctx.fillStyle = textColor;
        ctx.font = `600 ${labelFontPx}px "Inter", "Segoe UI", sans-serif`;
        ctx.fillText(lines[0], x + padX, y + padY);
        if (lines.length > 1) {
          ctx.globalAlpha = 0.92;
          ctx.font = `500 ${metaFontPx}px "Inter", "Segoe UI", sans-serif`;
          ctx.fillText(lines[1], x + padX, y + padY + labelFontPx + lineGap);
          ctx.globalAlpha = 1;
        }

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
