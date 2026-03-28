import { useCallback, useEffect, useRef } from 'react';
import { collectActivePaneKeys } from '../../../chart/panes/registry.js';
import { projectOverlayPayloads } from '../../../chart/overlays/projectOverlayPayloads.js';

/**
 * useOverlaySync - Manages chart overlay synchronization
 *
 * Extracts overlay application logic from ChartComponent.
 * Handles price lines, markers, touch points, VA boxes, segments, polylines, and signal bubbles.
 * Part of ChartComponent refactoring to reduce complexity.
 */

// Helper functions
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
  signalDetailsRef,
  setPaneLegendEntries,
  activeSignalSelection,
}) {
  // Overlay resource handles
  const overlayHandlesRef = useRef({ priceLines: [] });

  const syncOverlays = useCallback((overlays = []) => {
    setDataLoading(true);
    if (signalDetailsRef) signalDetailsRef.current = [];
    setPaneLegendEntries?.({});
    // Guard on required refs
    if (!seriesRef.current || !chartRef.current) return;

    // Helper: normalize time to seconds
    const toSec = (t) => {
      if (t == null) return t;
      if (typeof t === 'number') return t > 2e10 ? Math.floor(t / 1000) : t;
      if (typeof t === 'string') {
        const numeric = Number(t);
        if (Number.isFinite(numeric)) return numeric > 2e10 ? Math.floor(numeric / 1000) : numeric;
        const epochMs = Date.parse(t);
        if (Number.isFinite(epochMs)) return Math.floor(epochMs / 1000);
        return t;
      }
      if (typeof t === 'object') {
        if (typeof t.timestamp === 'function') {
          const ts = Number(t.timestamp());
          return Number.isFinite(ts) ? ts : t;
        }
        if (Number.isFinite(t.timestamp)) return Number(t.timestamp);
      }
      return t;
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

    const projected = projectOverlayPayloads({
      overlays,
      bubbleAlpha: 0.16,
      normalizeTime: toSec,
      activeSignalSelection,
      onOverlayProjected: ({ overlay, normalized }) => {
        const overlayLogger = logger.child({
          indicatorId: overlay?.ind_id,
          indicatorType: overlay?.type,
        });
        overlayLogger.debug('overlay_payload_received', {
          priceLines: Array.isArray(overlay?.payload?.price_lines) ? overlay.payload.price_lines.length : 0,
          markers: Array.isArray(overlay?.payload?.markers) ? overlay.payload.markers.length : 0,
          boxes: Array.isArray(overlay?.payload?.boxes) ? overlay.payload.boxes.length : 0,
          segments: Array.isArray(overlay?.payload?.segments) ? overlay.payload.segments.length : 0,
          polylines: Array.isArray(overlay?.payload?.polylines) ? overlay.payload.polylines.length : 0,
        });
        overlayLogger.debug('overlay_adapted', {
          priceLines: Array.isArray(normalized.priceLines) ? normalized.priceLines.length : 0,
          markers: Array.isArray(normalized.markers) ? normalized.markers.length : 0,
          touchPoints: Array.isArray(normalized.touchPoints) ? normalized.touchPoints.length : 0,
          boxes: Array.isArray(normalized.boxes) ? normalized.boxes.length : 0,
          segments: Array.isArray(normalized.segments) ? normalized.segments.length : 0,
          polylines: Array.isArray(normalized.polylines) ? normalized.polylines.length : 0,
          bubbles: Array.isArray(normalized.bubbles) ? normalized.bubbles.length : 0,
        });
      },
    });

    const markersByPane = projected.markersByPane;
    const touchPointsByPane = projected.touchPointsByPane;
    const signalBubblesByPane = projected.bubblesByPane;
    const segmentsByPane = projected.segmentsByPane;
    const polylinesByPane = projected.polylinesByPane;
    const signalDetails = projected.signalDetails;
    const legendEntriesByPane = projected.legendEntriesByPane || {};
    const boxesByPane = {};

    projected.priceLines.forEach((pl) => {
      const handle = seriesRef.current.createPriceLine({
        price: pl.price,
        color: pl.color ?? undefined,
        lineWidth: pl.lineWidth ?? 1,
        lineStyle: pl.lineStyle ?? 0,
        axisLabelVisible: pl.axisLabelVisible ?? false,
        title: pl.title ?? '',
      });
      overlayHandlesRef.current.priceLines.push(handle);
    });

    Object.entries(projected.boxesByPane || {}).forEach(([paneKey, paneBoxes]) => {
      const lastCandleSec = toSec(lastBarRef.current?.time);
      const normalizedBoxes = [];
      const summaryEntries = [];
      (paneBoxes || []).forEach((box, idxInGroup) => {
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
            index: idxInGroup + 1,
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

          normalizedBoxes.push({
            x1,
            x2,
            y1,
            y2,
            color: box.color,
            border: box.border,
            precision: box.precision,
          });
      });
      boxesByPane[paneKey] = normalizedBoxes;
      if (summaryEntries.length) {
        logger.info('va_box_summary', {
          appended: summaryEntries.length,
          total: normalizedBoxes.length,
          paneKey,
        });
        summaryEntries.forEach((entry) => {
          logger.info('va_box_summary_entry', {
            index: entry.index,
            detail: buildVaBoxSummaryText(entry),
            valueAreaId: entry.valueAreaId ?? null,
            label: entry.label ?? null,
            sourceStart: entry.sourceStart ?? null,
            sourceEnd: entry.sourceEnd ?? null,
          });
        });
      }
    });

    // Group touch points by time, strictly 1 item per time
    Object.values(markersByPane).forEach((entries) => entries.sort((a, b) => a.time - b.time));

    // 5) Apply markers via pane view manager (proper UTC support)
    try {
      const paneKeys = collectActivePaneKeys(
        markersByPane,
        touchPointsByPane,
        boxesByPane,
        segmentsByPane,
        polylinesByPane,
        signalBubblesByPane,
      );
      pvMgrRef.current?.syncActivePanes([...paneKeys]);
      paneKeys.forEach((paneKey) => {
        pvMgrRef.current?.setMarkers(markersByPane[paneKey] || [], paneKey);
        pvMgrRef.current?.setTouchPoints(touchPointsByPane[paneKey] || [], paneKey);
        pvMgrRef.current?.setVABlocks(
          boxesByPane[paneKey] || [],
          {
            lastSeriesTime: lastBarRef.current?.time,
            barSpacing: barSpacingRef.current,
          },
          paneKey,
        );
        pvMgrRef.current?.setSegments(segmentsByPane[paneKey] || [], paneKey);
        pvMgrRef.current?.setPolylines(polylinesByPane[paneKey] || [], paneKey);
        pvMgrRef.current?.setSignalBubbles(signalBubblesByPane[paneKey] || [], paneKey);
      });
      if (signalDetailsRef) {
        signalDetailsRef.current = signalDetails;
      }
      setPaneLegendEntries?.(legendEntriesByPane);
    } catch (e) {
      logger.error('overlays_apply_failed', e);
    }

    // 6) Log summary for quick tracing
    logger.info('overlays_applied', {
      priceLines: overlayHandlesRef.current.priceLines.length,
      markers: Object.values(markersByPane).reduce((total, entries) => total + entries.length, 0),
      touchPoints: Object.values(touchPointsByPane).reduce((total, entries) => total + entries.length, 0),
      boxes: Object.values(boxesByPane).reduce((total, entries) => total + entries.length, 0),
      bubbles: Object.values(signalBubblesByPane).reduce((total, entries) => total + entries.length, 0),
      segments: Object.values(segmentsByPane).reduce((total, entries) => total + entries.length, 0),
      polylines: Object.values(polylinesByPane).reduce((total, entries) => total + entries.length, 0),
    });

    setDataLoading(false);
  }, [activeSignalSelection, barSpacingRef, chartRef, lastBarRef, logger, pvMgrRef, seriesRef, setDataLoading, setPaneLegendEntries, signalDetailsRef]);

  return {
    syncOverlays,
    overlayHandlesRef,
  };
}
