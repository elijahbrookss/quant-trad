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

const drawRoundedBubble = (ctx, x, y, width, height, radius, pointer, accent, background, shadow) => {
  const r = Math.min(radius, width / 2, height / 2);
  const pointerWidth = pointer.width;
  const pointerHeight = pointer.height;
  const pointerHalf = pointerWidth / 2;
  const pointerBase = clamp(pointer.baseX, x + pointerHalf, x + width - pointerHalf);
  const direction = pointer.direction;

  ctx.save();
  ctx.beginPath();

  ctx.moveTo(x + r, y);

  if (direction === 'up') {
    ctx.lineTo(pointerBase - pointerHalf, y);
    ctx.lineTo(pointerBase, y - pointerHeight);
    ctx.lineTo(pointerBase + pointerHalf, y);
  }

  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);

  if (direction === 'down') {
    ctx.lineTo(pointerBase + pointerHalf, y + height);
    ctx.lineTo(pointerBase, y + height + pointerHeight);
    ctx.lineTo(pointerBase - pointerHalf, y + height);
  }

  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);

  ctx.closePath();

  if (shadow) {
    ctx.shadowColor = shadow;
    ctx.shadowBlur = 12;
    ctx.shadowOffsetY = 6;
  }

  ctx.fillStyle = background;
  ctx.fill();
  ctx.shadowColor = 'transparent';
  ctx.lineWidth = 1.5;
  ctx.strokeStyle = accent;
  ctx.stroke();
  ctx.restore();
};

const drawAccentBar = (ctx, x, y, width, accentColor) => {
  ctx.save();
  ctx.fillStyle = accentColor;
  ctx.fillRect(x, y, width, 3);
  ctx.restore();
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

      const padX = 12 * hpr;
      const padY = 10 * vpr;
      const pointerHeight = 12 * vpr;
      const pointerWidth = 18 * hpr;
      const radius = 12 * Math.min(hpr, vpr);
      const verticalGap = 16 * vpr;


      ctx.save();
      ctx.textBaseline = 'top';

      for (const bubble of bubbles) {
        const rawTime = toSec(bubble.time);
        const px = timeScaleApi.timeToCoordinate(rawTime);
        if (px == null) continue;

        const py = priceToCoordinate(Number(bubble.price));
        if (py == null) continue;

        const canvasX = px * hpr;
        const offscreenBuffer = 24 * hpr;
        if (canvasX < -offscreenBuffer || canvasX > widthPx + offscreenBuffer) {
          continue;
        }

        const accent = bubble.accentColor ?? '#38bdf8';
        const background = bubble.backgroundColor ?? hexToRgba(accent, 0.12) ?? 'rgba(30,41,59,0.78)';
        const textColor = bubble.textColor ?? '#f8fafc';
        const shadow = hexToRgba(accent, 0.2);

        const label = bubble.label ?? 'Signal';
        const bias = typeof bubble.bias === 'string' ? bubble.bias.trim() : '';
        const detailLine = typeof bubble.detail === 'string' ? bubble.detail : '';
        const detail = [detailLine, bias ? `Bias: ${bias}` : ''].filter(Boolean).join(' • ');
        const meta = bubble.meta ?? '';

        const direction = bubble.direction === 'below' ? 'below' : 'above';

        const headingFontSize = 12;
        const bodyFontSize = 10;

        ctx.font = `600 ${headingFontSize * vpr}px "Inter", "Segoe UI", sans-serif`;
        const headingWidth = ctx.measureText(label).width;
        const headingHeight = headingFontSize * 1.3 * vpr;

        ctx.font = `500 ${bodyFontSize * vpr}px "Inter", "Segoe UI", sans-serif`;
        const detailWidth = detail ? ctx.measureText(detail).width : 0;
        const metaWidth = meta ? ctx.measureText(meta).width : 0;
        const bodyHeight = (detail ? bodyFontSize * 1.25 * vpr : 0) + (meta ? bodyFontSize * 1.25 * vpr : 0);

        const textWidth = Math.max(headingWidth, detailWidth, metaWidth);
        const textHeight = headingHeight + bodyHeight;

        const bubbleWidth = textWidth + padX * 2;
        const bubbleHeight = textHeight + padY * 2;

        const pointerDir = direction === 'above' ? 'down' : 'up';

        let bubbleX = canvasX - bubbleWidth / 2;
        const minX = 12 * hpr;
        const maxX = widthPx - bubbleWidth - 12 * hpr;
        bubbleX = clamp(bubbleX, minX, Math.max(minX, maxX));

        let bubbleY;
        if (direction === 'above') {
          bubbleY = py * vpr - pointerHeight - verticalGap - bubbleHeight;
        } else {
          bubbleY = py * vpr + pointerHeight + verticalGap;
        }

        const minY = 12 * vpr;
        const maxY = heightPx - bubbleHeight - 12 * vpr;
        bubbleY = clamp(bubbleY, minY, Math.max(minY, maxY));

        let pointerBaseX = px * hpr;
        const pointerClampLeft = bubbleX + pointerWidth * 0.6;
        const pointerClampRight = bubbleX + bubbleWidth - pointerWidth * 0.6;
        pointerBaseX = clamp(pointerBaseX, pointerClampLeft, pointerClampRight);

        const pointer = {
          width: pointerWidth,
          height: pointerHeight,
          baseX: pointerBaseX,
          direction: pointerDir === 'down' ? 'down' : 'up',
        };

        drawRoundedBubble(ctx, bubbleX, bubbleY, bubbleWidth, bubbleHeight, radius, pointer, accent, background, shadow);
        if (pointer.direction === 'down') {
          drawAccentBar(ctx, bubbleX, bubbleY, bubbleWidth, accent);
        } else {
          drawAccentBar(ctx, bubbleX, bubbleY + bubbleHeight - 3, bubbleWidth, accent);
        }

        ctx.fillStyle = textColor;

        let cursorY = bubbleY + padY;
        ctx.font = `600 ${headingFontSize * vpr}px "Inter", "Segoe UI", sans-serif`;
        ctx.fillText(label, bubbleX + padX, cursorY);
        cursorY += headingHeight;

        ctx.font = `500 ${bodyFontSize * vpr}px "Inter", "Segoe UI", sans-serif`;
        if (detail) {
          ctx.fillStyle = textColor;
          ctx.fillText(detail, bubbleX + padX, cursorY);
          cursorY += bodyFontSize * 1.25 * vpr;
        }

        if (meta) {
          ctx.fillStyle = hexToRgba(accent, 0.9) ?? accent;
          ctx.fillText(meta, bubbleX + padX, cursorY);
        }

        ctx.beginPath();
        const pointerTipY = direction === 'above'
          ? bubbleY + bubbleHeight + pointerHeight
          : bubbleY - pointerHeight;
        const pointerTipX = pointerBaseX;
        ctx.fillStyle = accent;
        ctx.arc(pointerTipX, pointerTipY, 3 * Math.min(hpr, vpr), 0, Math.PI * 2);
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
