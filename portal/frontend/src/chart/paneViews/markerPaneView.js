const toSec = (t) => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

/**
 * Custom marker pane view that properly handles UTC timestamps.
 * Replaces createSeriesMarkers to avoid timezone offset bugs.
 */
export function createMarkerPaneView(timeScaleApi, priceScaleApi) {
  let markers = [];

  const drawCircle = (ctx, x, y, radius, color) => {
    ctx.beginPath();
    ctx.arc(x, y, radius, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
  };

  const drawSquare = (ctx, x, y, size, color) => {
    ctx.fillStyle = color;
    ctx.fillRect(x - size / 2, y - size / 2, size, size);
  };

  const drawArrowUp = (ctx, x, y, size, color) => {
    ctx.beginPath();
    ctx.moveTo(x, y - size);
    ctx.lineTo(x + size / 2, y);
    ctx.lineTo(x - size / 2, y);
    ctx.closePath();
    ctx.fillStyle = color;
    ctx.fill();
  };

  const drawArrowDown = (ctx, x, y, size, color) => {
    ctx.beginPath();
    ctx.moveTo(x, y + size);
    ctx.lineTo(x + size / 2, y);
    ctx.lineTo(x - size / 2, y);
    ctx.closePath();
    ctx.fillStyle = color;
    ctx.fill();
  };

  const drawText = (ctx, x, y, text, color, size) => {
    ctx.font = `${size}px monospace`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = color;
    ctx.fillText(text, x, y);
  };

  const renderer = {
    draw(target, priceToCoordinate) {
      const { context: ctx, horizontalPixelRatio: hpr, verticalPixelRatio: vpr } =
        target.useBitmapCoordinateSpace(({ context, horizontalPixelRatio, verticalPixelRatio }) => ({
          context,
          horizontalPixelRatio,
          verticalPixelRatio,
        }));

      if (!ctx) return;

      ctx.save();

      for (const marker of markers) {
        const rawTime = toSec(marker.time);
        const px = timeScaleApi.timeToCoordinate(rawTime);
        if (px == null) continue;

        const py = priceToCoordinate(Number(marker.price ?? marker.position ?? 0));
        if (py == null) continue;

        const canvasX = px * hpr;
        const canvasY = py * vpr;

        const color = marker.color || '#2196F3';
        const size = (marker.size || 8) * Math.min(hpr, vpr);
        const shape = marker.shape || 'circle';

        // Draw shape
        switch (shape) {
          case 'circle':
            drawCircle(ctx, canvasX, canvasY, size / 2, color);
            break;
          case 'square':
            drawSquare(ctx, canvasX, canvasY, size, color);
            break;
          case 'arrowUp':
            drawArrowUp(ctx, canvasX, canvasY, size, color);
            break;
          case 'arrowDown':
            drawArrowDown(ctx, canvasX, canvasY, size, color);
            break;
        }

        // Draw text if provided
        if (marker.text) {
          const textY = marker.position === 'aboveBar' ? canvasY - size * 1.5 : canvasY + size * 1.5;
          drawText(ctx, canvasX, textY, marker.text, color, size * 1.2);
        }
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
    isWhitespace: () => false,
    defaultOptions() {
      return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };
    },
    destroy: () => {},
    setMarkers(next) {
      markers = Array.isArray(next) ? next : [];
    },
  };
}
