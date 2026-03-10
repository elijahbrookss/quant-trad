const toSec = (t) => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

export function createHighlightBandPaneView(timeScaleApi) {
  let bands = [];

  const renderer = {
    draw(target) {
      const { context: ctx, mediaSize, horizontalPixelRatio: hpr, verticalPixelRatio: vpr } =
        target.useBitmapCoordinateSpace(({ context, mediaSize, horizontalPixelRatio, verticalPixelRatio }) => ({
          context,
          mediaSize,
          horizontalPixelRatio,
          verticalPixelRatio,
        }));

      if (!ctx) return;

      ctx.save();
      const fullHeight = mediaSize.height * vpr;

      for (const band of bands) {
        const x1 = timeScaleApi.timeToCoordinate(toSec(band?.x1));
        const x2 = timeScaleApi.timeToCoordinate(toSec(band?.x2));
        if (!Number.isFinite(x1) || !Number.isFinite(x2)) continue;

        const left = Math.min(x1, x2) * hpr;
        const right = Math.max(x1, x2) * hpr;
        const width = Math.max(1, right - left);

        ctx.fillStyle = band?.fillColor || 'rgba(148, 163, 184, 0.03)';
        ctx.fillRect(left, 0, width, fullHeight);

        ctx.strokeStyle = band?.borderColor || 'rgba(148, 163, 184, 0.35)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(left + 0.5, 0);
        ctx.lineTo(left + 0.5, fullHeight);
        ctx.moveTo(right - 0.5, 0);
        ctx.lineTo(right - 0.5, fullHeight);
        ctx.stroke();

        if (typeof band?.label === 'string' && band.label.trim()) {
          ctx.font = `${Math.max(10, 10 * Math.min(hpr, vpr))}px monospace`;
          ctx.textAlign = 'left';
          ctx.textBaseline = 'top';
          const padY = 8 * vpr;
          const text = band.label.trim();
          const metrics = ctx.measureText(text);
          const textW = metrics.width;
          const tagW = textW + 8 * hpr;
          const tagH = 16 * vpr;
          const tagX = left + 4 * hpr;
          const tagY = 4 * vpr;

          ctx.fillStyle = 'rgba(2, 6, 23, 0.62)';
          ctx.fillRect(tagX, tagY, tagW, tagH);
          ctx.fillStyle = 'rgba(226, 232, 240, 0.95)';
          ctx.fillText(text, tagX + 4 * hpr, tagY + padY * 0.25);
        }
      }

      ctx.restore();
    },
    drawBackground() {},
    hitTest() {
      return null;
    },
  };

  return {
    renderer: () => renderer,
    update: () => {},
    priceValueBuilder: () => [NaN, NaN, NaN],
    isWhitespace: () => false,
    defaultOptions() {
      return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };
    },
    destroy: () => {},
    setBands(next) {
      bands = Array.isArray(next) ? next : [];
    },
  };
}
