import { useCallback, useEffect, useRef } from 'react';
import { adaptPayload, getPaneViewsForOverlay } from '../../../chart/indicators/registry.js';

/**
 * useOverlaySync - Manages chart overlay synchronization
 *
 * Extracts overlay application logic from ChartComponent.
 * Handles price lines, markers, touch points, VA boxes, segments, polylines, and signal bubbles.
 * Part of ChartComponent refactoring to reduce complexity.
 */

// Helper functions
const toRgba = (hex, alpha = 0.12) => {
  if (typeof hex !== 'string') return null;
  const trimmed = hex.trim().replace('#', '');
  if (!(trimmed.length === 3 || trimmed.length === 6)) return null;

  const expand = (value) => value.split('').map((c) => c + c).join('');
  const normalized = trimmed.length === 3 ? expand(trimmed) : trimmed;

  const r = Number.parseInt(normalized.slice(0, 2), 16);
  const g = Number.parseInt(normalized.slice(2, 4), 16);
  const b = Number.parseInt(normalized.slice(4, 6), 16);

  if ([r, g, b].some((v) => Number.isNaN(v))) return null;

  const clampedAlpha = Math.min(Math.max(alpha, 0), 1);
  return `rgba(${r},${g},${b},${clampedAlpha})`;
};

const coalesce = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
};

const toFiniteNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const toIsoFromSeconds = (value) => {
  if (value == null || !Number.isFinite(Number(value))) return null;
  try {
    const ms = Number(value) * 1000;
    return new Date(ms).toISOString();
  } catch {
    return null;
  }
};

const formatPriceDisplay = (price, precision) => {
  if (price == null || !Number.isFinite(Number(price))) return 'n/a';
  const num = Number(price);
  const p = Number.isFinite(Number(precision)) ? Number(precision) : 2;
  return num.toFixed(Math.max(0, Math.min(p, 8)));
};

const buildVaBoxSummaryText = ({
  startSec,
  endSec,
  requestedEndSec,
  val,
  vah,
  poc,
  sessions,
  valueAreaId,
  precision,
}) => {
  const parts = [
    `start=${toIsoFromSeconds(startSec) ?? 'n/a'}`,
    `end=${toIsoFromSeconds(endSec) ?? 'n/a'}`,
    `VAL=${formatPriceDisplay(val, precision)}`,
    `VAH=${formatPriceDisplay(vah, precision)}`,
  ];

  if (poc != null) {
    parts.push(`POC=${formatPriceDisplay(poc, precision)}`);
  }
  if (sessions != null) {
    parts.push(`sessions=${sessions}`);
  }
  if (valueAreaId != null) {
    parts.push(`id=${valueAreaId}`);
  }
  if (requestedEndSec != null && requestedEndSec !== endSec) {
    parts.push('extended_to_last_bar=true');
  }

  return parts.join(', ');
};

export function useOverlaySync({
  chartRef,
  seriesRef,
  pvMgrRef,
  lastBarRef,
  barSpacingRef,
  logger,
  setDataLoading,
}) {
  // Overlay resource handles
  const overlayHandlesRef = useRef({ priceLines: [] });

  const syncOverlays = useCallback((overlays = []) => {
    setDataLoading(true);
    // Guard on required refs
    if (!seriesRef.current || !chartRef.current) return;

    // Helper: normalize time to seconds
    const toSec = (t) => {
      if (t == null) return t;
      if (typeof t !== 'number') return t;
      return t > 2e10 ? Math.floor(t / 1000) : t;
    };

    // 1) Clear existing price lines
    overlayHandlesRef.current.priceLines.forEach(h => {
      try {
        seriesRef.current.removePriceLine(h);
      } catch {
        // ignore if price line already cleared
      }
    });
    overlayHandlesRef.current.priceLines = [];

    pvMgrRef.current?.clearFrame();

    // 2) Build fresh markers and touch points
    const markers = [];
    const touchPoints = [];
    const boxes = [];
    const signalBubbles = [];
    const allSegments = [];
    const allPolylines = [];

    // 3) Walk overlays and apply
    for (const ov of overlays) {
      const { type, payload, color, ind_id: indicatorId } = ov || {};
      if (!payload) continue;

      const overlayLogger = logger.child({ indicatorId, indicatorType: type });
      overlayLogger.debug('overlay_payload_received', {
        priceLines: Array.isArray(payload.price_lines) ? payload.price_lines.length : 0,
        markers: Array.isArray(payload.markers) ? payload.markers.length : 0,
        boxes: Array.isArray(payload.boxes) ? payload.boxes.length : 0,
        segments: Array.isArray(payload.segments) ? payload.segments.length : 0,
        polylines: Array.isArray(payload.polylines) ? payload.polylines.length : 0,
      });

      const paneViews = getPaneViewsForOverlay(ov);
      const norm = adaptPayload(type, payload, color);
      overlayLogger.debug('overlay_adapted', {
        priceLines: Array.isArray(norm.priceLines) ? norm.priceLines.length : 0,
        markers: Array.isArray(norm.markers) ? norm.markers.length : 0,
        touchPoints: Array.isArray(norm.touchPoints) ? norm.touchPoints.length : 0,
        boxes: Array.isArray(norm.boxes) ? norm.boxes.length : 0,
        segments: Array.isArray(norm.segments) ? norm.segments.length : 0,
        polylines: Array.isArray(norm.polylines) ? norm.polylines.length : 0,
        bubbles: Array.isArray(norm.bubbles) ? norm.bubbles.length : 0,
      });
      const markerTimes = (norm.markers || []).map(m => m?.time).filter(t => Number.isFinite(t));
      const bubbleTimes = (norm.bubbles || []).map(b => b?.time).filter(t => Number.isFinite(t));
      if (markerTimes.length || bubbleTimes.length) {
        overlayLogger.debug('overlay_time_bounds', {
          markerMin: markerTimes.length ? Math.min(...markerTimes) : null,
          markerMax: markerTimes.length ? Math.max(...markerTimes) : null,
          bubbleMin: bubbleTimes.length ? Math.min(...bubbleTimes) : null,
          bubbleMax: bubbleTimes.length ? Math.max(...bubbleTimes) : null,
        });
      }

      // 3a) Price lines
      if (Array.isArray(payload.price_lines)) {
        for (const pl of payload.price_lines) {
          const handle = seriesRef.current.createPriceLine({
            price: pl.price,
            color: pl.color ?? undefined,
            lineWidth: pl.lineWidth ?? 1,
            lineStyle: pl.lineStyle ?? 0,
            axisLabelVisible: pl.axisLabelVisible ?? false,
            title: pl.title ?? type ?? '',
          });
          overlayHandlesRef.current.priceLines.push(handle);
        }
      }

      // 3b) Markers
      markers.push(...norm.markers);

      if (Array.isArray(norm.bubbles) && norm.bubbles.length) {
        if (color) {
          signalBubbles.push(...norm.bubbles.map(b => {
            const accentColor = color;
            const backgroundColor = toRgba(accentColor, 0.16) ?? undefined;
            return {
              ...b,
              accentColor,
              backgroundColor,
            };
          }));
        } else {
          signalBubbles.push(...norm.bubbles);
        }
      }

      // 3c) Touch points
      if (paneViews.includes('touch') && norm.touchPoints?.length) {
        touchPoints.push(...norm.touchPoints.map(p => ({
          ...p,
          time: toSec(p.time),
        })));
      }

      // 3d) VA Boxes
      if (paneViews.includes('va_box') && norm.boxes?.length) {
        const lastCandleSec = toSec(lastBarRef.current?.time);
        const baseIndex = boxes.length;
        const summaryEntries = [];
        const normalizedBoxes = norm.boxes.map((box, idxInGroup) => {
          const x1 = box.x1;
          const requestedX2 = box.x2;
          const extendBox = box.extend !== undefined ? Boolean(box.extend) : false;
          let x2 = requestedX2;

          if (!Number.isFinite(x2)) {
            if (extendBox && Number.isFinite(lastCandleSec)) {
              x2 = lastCandleSec;
            } else {
              x2 = x1;
            }
          } else if (extendBox && Number.isFinite(lastCandleSec) && lastCandleSec > x2) {
            overlayLogger.debug('va_box_span_extended', {
              boxIndex: baseIndex + idxInGroup,
              x1,
              originalX2: requestedX2,
              forcedX2: lastCandleSec,
            });
            x2 = lastCandleSec;
          }

          const pocValue = toFiniteNumber(
            coalesce(
              box.poc,
              box.POC,
              box?.meta?.poc,
              box?.metadata?.poc,
            ),
          );
          const sessions = coalesce(
            box.session_count,
            box.sessions,
            box.sessionCount,
            box?.meta?.session_count,
            box?.metadata?.session_count,
          );
          const valueAreaId = coalesce(
            box.value_area_id,
            box.valueAreaId,
            box.value_areaId,
            box.id,
            box?.meta?.value_area_id,
            box?.metadata?.value_area_id,
          );
          const label = coalesce(
            box.label,
            box.session_label,
            box.session,
            box.profile_label,
          );
          const sourceStart = coalesce(box.start, box.start_date, box.startDate);
          const sourceEnd = coalesce(box.end, box.end_date, box.endDate);

          const y1 = box.y1;
          const y2 = box.y2;
          const precision = Number.isFinite(Number(box.precision))
            ? Math.min(Math.max(Number(box.precision), 2), 8)
            : undefined;

          summaryEntries.push({
            index: baseIndex + idxInGroup + 1,
            startSec: x1,
            endSec: x2,
            requestedEndSec: requestedX2,
            val: Number.isFinite(y1) ? y1 : null,
            vah: Number.isFinite(y2) ? y2 : null,
            poc: pocValue,
            sessions,
            valueAreaId,
            label,
            sourceStart,
            sourceEnd,
            precision,
          });

          return {
            x1,
            x2,
            y1,
            y2,
            color: box.color,
            border: box.border,
            precision: box.precision,
          };
        }).filter(Boolean);
        boxes.push(...normalizedBoxes);
        normalizedBoxes.forEach((b, idx) => {
          const width = Number.isFinite(b.x2) && Number.isFinite(b.x1)
            ? Number(b.x2) - Number(b.x1)
            : null;
          overlayLogger.debug('va_box_applied', {
            boxIndex: baseIndex + idx,
            x1: b.x1,
            x2: b.x2,
            y1: b.y1,
            y2: b.y2,
            width,
          });
        });

        if (summaryEntries.length) {
          overlayLogger.info('va_box_summary', {
            appended: summaryEntries.length,
            total: boxes.length,
          });
          summaryEntries.forEach((entry) => {
            overlayLogger.info('va_box_summary_entry', {
              index: entry.index,
              detail: buildVaBoxSummaryText(entry),
              valueAreaId: entry.valueAreaId ?? null,
              label: entry.label ?? null,
              sourceStart: entry.sourceStart ?? null,
              sourceEnd: entry.sourceEnd ?? null,
            });
          });
        }
      }

      if (paneViews.includes('segment') && norm.segments?.length) {
        allSegments.push(...norm.segments);
      }
      if (paneViews.includes('polyline') && norm.polylines?.length) {
        allPolylines.push(...norm.polylines);
      }
    }

    // Group touch points by time, strictly 1 item per time
    const grouped = new Map();
    for (const p of touchPoints) {
      if (p.time == null || Number.isNaN(p.time)) continue;
      if (!grouped.has(p.time)) grouped.set(p.time, []);
      grouped.get(p.time).push({
        price:  p.originalData?.price ?? p.price,
        color:  p.originalData?.color ?? p.color,
        size:   (p.originalData?.size ?? p.size ?? 3),
      });
    }

    // 4) Sort markers for deterministic rendering
    markers.sort((a, b) => a.time - b.time);

    // 5) Apply markers via pane view manager (proper UTC support)
    try {
      pvMgrRef.current?.setMarkers(markers);
      pvMgrRef.current?.setTouchPoints(touchPoints);
      pvMgrRef.current?.setVABlocks(boxes, {
        lastSeriesTime: lastBarRef.current?.time,
        barSpacing: barSpacingRef.current,
      });
      pvMgrRef.current?.setSegments(allSegments);
      pvMgrRef.current?.setPolylines(allPolylines);
      pvMgrRef.current?.setSignalBubbles(signalBubbles);
    } catch (e) {
      logger.error('overlays_apply_failed', e);
    }

    // 6) Log summary for quick tracing
    logger.info('overlays_applied', {
      priceLines: overlayHandlesRef.current.priceLines.length,
      markers: markers.length,
      touchPoints: touchPoints.length,
      boxes: boxes.length,
      bubbles: signalBubbles.length,
      segments: allSegments.length,
      polylines: allPolylines.length,
    });

    setDataLoading(false);
  }, [chartRef, seriesRef, pvMgrRef, lastBarRef, barSpacingRef, logger, setDataLoading]);

  return {
    syncOverlays,
    overlayHandlesRef,
  };
}
