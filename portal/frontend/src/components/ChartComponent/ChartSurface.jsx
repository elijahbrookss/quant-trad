import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Maximize2, Minimize2 } from 'lucide-react';
import SymbolPalette from '../SymbolPalette.jsx';
import HotkeyHint from '../HotkeyHint.jsx';
import LoadingOverlay from '../LoadingOverlay.jsx';
import { getPaneDefinition, listPaneDefinitions } from '../../chart/panes/registry.js';

const DEFAULT_ARM_TIMEOUT_MS = 10000;
const MIN_RECT_SIDE_PX = 4;

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const toEpochSeconds = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (!value || typeof value !== 'object') return null;
  if (typeof value.timestamp === 'number' && Number.isFinite(value.timestamp)) return value.timestamp;
  if (typeof value.timestamp === 'function') {
    const ts = Number(value.timestamp());
    return Number.isFinite(ts) ? ts : null;
  }
  if (
    Number.isFinite(value.year)
    && Number.isFinite(value.month)
    && Number.isFinite(value.day)
  ) {
    const ms = Date.UTC(value.year, value.month - 1, value.day, 0, 0, 0, 0);
    return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
  }
  return null;
};

const findFirstAtLeast = (arr, value) => {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < value) lo = mid + 1;
    else hi = mid;
  }
  return lo;
};

const findLastAtMost = (arr, value) => {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] <= value) lo = mid + 1;
    else hi = mid;
  }
  return lo - 1;
};

const countCandlesInRange = (sortedTimes, from, to) => {
  if (!Array.isArray(sortedTimes) || sortedTimes.length === 0) return 0;
  if (!Number.isFinite(from) || !Number.isFinite(to)) return 0;
  const low = Math.min(from, to);
  const high = Math.max(from, to);
  const start = findFirstAtLeast(sortedTimes, low);
  const end = findLastAtMost(sortedTimes, high);
  if (start >= sortedTimes.length || end < 0 || end < start) return 0;
  return end - start + 1;
};

const snapToNearestCandleTime = (sortedTimes, candidate) => {
  if (!Array.isArray(sortedTimes) || sortedTimes.length === 0) return null;
  if (!Number.isFinite(candidate)) return null;
  const idx = findFirstAtLeast(sortedTimes, candidate);
  if (idx <= 0) return sortedTimes[0];
  if (idx >= sortedTimes.length) return sortedTimes[sortedTimes.length - 1];
  const prev = sortedTimes[idx - 1];
  const next = sortedTimes[idx];
  return Math.abs(candidate - prev) <= Math.abs(next - candidate) ? prev : next;
};

const shouldIgnoreTarget = (target) => {
  if (!(target instanceof Element)) return false;
  return Boolean(
    target.closest(
      'button,input,textarea,select,a,[role="button"],[data-chart-ui-control="true"]',
    ),
  );
};

export function ChartSurface({
  shellRef,
  containerRef,
  chartRef,
  pvMgrRef,
  candleTimes,
  isFullscreen,
  toggleFullscreen,
  symbolDisplay,
  intervalDisplay,
  instrumentMeta,
  paneLegendEntries,
  chartStateNotice,
  windowSummary,
  palOpen,
  setPalOpen,
  applySymbol,
  loaderActive,
  loaderMessage,
}) {
  const [selectionArmed, setSelectionArmed] = useState(false);
  const [dragRect, setDragRect] = useState(null);
  const [savedRects, setSavedRects] = useState([]);
  const armTimeoutRef = useRef(null);
  const drawingRef = useRef(false);

  const clearArmTimeout = useCallback(() => {
    if (armTimeoutRef.current) {
      clearTimeout(armTimeoutRef.current);
      armTimeoutRef.current = null;
    }
  }, []);

  const disarmSelection = useCallback(() => {
    clearArmTimeout();
    drawingRef.current = false;
    setSelectionArmed(false);
    setDragRect(null);
  }, [clearArmTimeout]);

  const clearSelections = useCallback(() => {
    disarmSelection();
    setSavedRects([]);
  }, [disarmSelection]);

  useEffect(() => () => clearArmTimeout(), [clearArmTimeout]);

  const toRelativePoint = useCallback((clientX, clientY) => {
    const shell = shellRef?.current;
    if (!shell) return null;
    const rect = shell.getBoundingClientRect();
    return {
      x: clamp(clientX - rect.left, 0, rect.width),
      y: clamp(clientY - rect.top, 0, rect.height),
    };
  }, [shellRef]);

  const resolveTimeAtCoordinate = useCallback((x) => {
    const timeScaleApi = chartRef?.current?.timeScale?.();
    const rawTime = toEpochSeconds(timeScaleApi?.coordinateToTime?.(x));
    return snapToNearestCandleTime(candleTimes, rawTime);
  }, [chartRef, candleTimes]);

  useEffect(() => {
    const manager = pvMgrRef?.current;
    const bands = [...savedRects, ...(dragRect ? [{ id: 'draft', ...dragRect }] : [])]
      .filter((b) => Number.isFinite(b.startTime) && Number.isFinite(b.endTime))
      .map((b) => ({
        x1: b.startTime,
        x2: b.endTime,
        label: `${b.candleCount || 0} candles`,
        borderColor: 'rgba(148,163,184,0.35)',
        fillColor: 'rgba(148,163,184,0.03)',
      }));
    manager?.setHighlightBands?.(bands);
  }, [savedRects, dragRect, pvMgrRef]);

  useEffect(() => {
    const manager = pvMgrRef?.current;
    return () => {
      manager?.setHighlightBands?.([]);
    };
  }, [pvMgrRef]);

  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.key !== 'Escape') return;
      if (dragRect || selectionArmed || savedRects.length) {
        event.preventDefault();
      }
      if (dragRect || selectionArmed) {
        disarmSelection();
        return;
      }
      if (savedRects.length) {
        setSavedRects([]);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [dragRect, selectionArmed, savedRects.length, disarmSelection]);

  const beginDraw = useCallback((event) => {
    if (!selectionArmed) return;
    event.preventDefault();
    event.stopPropagation();

    const point = toRelativePoint(event.clientX, event.clientY);
    if (!point) return;
    const time = resolveTimeAtCoordinate(point.x);
    if (!Number.isFinite(time)) return;

    drawingRef.current = true;
    event.currentTarget?.setPointerCapture?.(event.pointerId);
    setDragRect({
      startX: point.x,
      endX: point.x,
      startTime: time,
      endTime: time,
      candleCount: 1,
    });
  }, [selectionArmed, resolveTimeAtCoordinate, toRelativePoint]);

  const moveDraw = useCallback((event) => {
    if (!drawingRef.current) return;
    event.preventDefault();
    event.stopPropagation();

    const point = toRelativePoint(event.clientX, event.clientY);
    if (!point) return;
    const time = resolveTimeAtCoordinate(point.x);
    if (!Number.isFinite(time)) return;

    setDragRect((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        endX: point.x,
        endTime: time,
        candleCount: countCandlesInRange(candleTimes, prev.startTime, time),
      };
    });
  }, [resolveTimeAtCoordinate, toRelativePoint, candleTimes]);

  const finishDraw = useCallback((event) => {
    if (!drawingRef.current) return;
    event.preventDefault();
    event.stopPropagation();

    drawingRef.current = false;
    event.currentTarget?.releasePointerCapture?.(event.pointerId);
    setDragRect((prev) => {
      if (!prev) return prev;
      const width = Math.abs(prev.endX - prev.startX);
      if (
        width >= MIN_RECT_SIDE_PX
        && Number.isFinite(prev.startTime)
        && Number.isFinite(prev.endTime)
      ) {
        setSavedRects((current) => [
          ...current,
          {
            id: `${Date.now()}-${current.length + 1}`,
            startTime: prev.startTime,
            endTime: prev.endTime,
            candleCount: prev.candleCount,
          },
        ]);
      }
      return null;
    });
    setSelectionArmed(false);
    clearArmTimeout();
  }, [clearArmTimeout]);

  const startSelectionArm = useCallback(() => {
    clearArmTimeout();
    setSelectionArmed(true);
    armTimeoutRef.current = setTimeout(() => {
      setSelectionArmed(false);
      setDragRect(null);
      armTimeoutRef.current = null;
    }, DEFAULT_ARM_TIMEOUT_MS);
  }, [clearArmTimeout]);

  const chartStateIsLoading = chartStateNotice?.state === 'loading';
  const effectiveLoaderActive = loaderActive || chartStateIsLoading;
  const effectiveLoaderMessage = chartStateIsLoading
    ? (chartStateNotice?.message || loaderMessage)
    : loaderMessage;

  const interactionLayerActive = selectionArmed || Boolean(dragRect);
  const legendSections = useMemo(
    () =>
      Object.entries(paneLegendEntries || {})
        .map(([paneKey, entries]) => {
          const pane = getPaneDefinition(paneKey);
          const filtered = (entries || []).filter((entry) => entry?.label);
          if (!pane?.showLegend || !filtered.length) return null;
          return {
            key: pane.key,
            label: pane.label,
            entries: filtered,
          };
        })
        .filter(Boolean),
    [paneLegendEntries],
  );
  const paneLegendLayout = useMemo(() => {
    if (!legendSections.length) return [];
    const paneSections = legendSections
      .map((section) => ({ section, pane: getPaneDefinition(section.key) }))
      .sort((left, right) => left.pane.index - right.pane.index);
    const activeAuxPanes = listPaneDefinitions()
      .filter((pane) => pane.key !== 'price' && paneSections.some((section) => section.pane.key === pane.key))
      .sort((left, right) => left.index - right.index);
    if (!activeAuxPanes.length) return [];

    const priceStretch = getPaneDefinition('price').stretchFactor;
    const totalStretch = priceStretch + activeAuxPanes.reduce((total, pane) => total + pane.stretchFactor, 0);
    let cursor = priceStretch;

    return paneSections.map(({ section, pane }) => {
      const topPercent = (cursor / totalStretch) * 100;
      cursor += pane.stretchFactor;
      return {
        ...section,
        top: `calc(${topPercent}% + 8px)`,
      };
    });
  }, [legendSections]);

  const chartShellClasses = useMemo(() => {
    const base =
      'group relative overflow-hidden border border-white/10 bg-[#0f1419] shadow-[0_0_40px_rgba(0,0,0,0.6)]';
    const cursor = interactionLayerActive ? 'cursor-crosshair' : '';
    const size = isFullscreen
      ? 'h-screen w-screen rounded-none'
      : 'h-[680px] rounded-2xl';
    return `${base} ${size} ${cursor}`.trim();
  }, [isFullscreen, interactionLayerActive]);

  return (
    <div
      ref={shellRef}
      className={chartShellClasses}
      onDoubleClick={(event) => {
        if (shouldIgnoreTarget(event.target)) return;
        event.preventDefault();
        startSelectionArm();
      }}
    >
      <div className="pointer-events-none absolute left-6 top-6 z-10 flex max-w-[70%] flex-col gap-1 text-slate-200 drop-shadow-[0_10px_30px_rgba(0,0,0,0.65)]">
        <div className="flex flex-wrap items-baseline gap-1.5">
          <span className="text-[17px] font-semibold tracking-[-0.02em] text-white">{symbolDisplay}</span>
          <span className="rounded-full border border-white/14 bg-black/48 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.24em] text-slate-100/92">
            {intervalDisplay}
          </span>
        </div>
        {instrumentMeta ? (
          <div className="text-[9px] font-semibold uppercase tracking-[0.28em] text-slate-300/78">
            {instrumentMeta}
          </div>
        ) : null}
      </div>
      {paneLegendLayout.length ? (
        <div className="pointer-events-none absolute inset-0 z-10 text-slate-200">
          {paneLegendLayout.map((section) => (
            <div
              key={section.key}
              className="absolute left-3 rounded-md border border-white/7 bg-black/26 px-1.5 py-1 backdrop-blur-[6px]"
              style={{ top: section.top }}
            >
              <div className="flex flex-wrap items-center gap-x-1.5 gap-y-1">
                {section.entries.map((entry) => (
                  <div key={`${section.key}:${entry.overlayType || entry.label}`} className="flex items-center gap-1">
                    <span
                      className="inline-block h-1.5 w-1.5 rounded-full"
                      style={{ backgroundColor: entry.color || '#cbd5e1' }}
                    />
                    <span className="text-[9px] font-medium text-slate-200/84">
                      {entry.label}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      ) : null}
      {savedRects.length || dragRect ? (
        <div className="pointer-events-none absolute left-1/2 top-3 z-10 -translate-x-1/2 opacity-0 transition-opacity duration-200 group-hover:opacity-100 focus-within:opacity-100">
          <button
            type="button"
            data-chart-ui-control="true"
            onClick={clearSelections}
            className="pointer-events-auto inline-flex items-center rounded-full border border-white/10 bg-black/34 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.2em] text-slate-100/86 transition hover:border-white/20 hover:bg-black/52 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent-ring-strong)]"
            title="Clear temporary selections (Esc)"
          >
            Clear marks
          </button>
        </div>
      ) : null}
      <div className="pointer-events-none absolute bottom-3 right-3 z-10 flex translate-y-1 flex-col items-end gap-2 opacity-0 transition group-hover:translate-y-0 group-hover:opacity-100 focus-within:translate-y-0 focus-within:opacity-100">
        <button
          type="button"
          data-chart-ui-control="true"
          aria-pressed={isFullscreen}
          onClick={toggleFullscreen}
          className="pointer-events-auto inline-flex h-8 w-8 items-center justify-center rounded-full border border-white/12 bg-black/42 text-slate-100/90 shadow-[0_8px_24px_rgba(0,0,0,0.28)] transition hover:border-white/24 hover:bg-black/58 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent-ring-strong)]"
          title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
        >
          {isFullscreen ? (
            <Minimize2 className="h-3.5 w-3.5" aria-hidden="true" />
          ) : (
            <Maximize2 className="h-3.5 w-3.5" aria-hidden="true" />
          )}
        </button>
      </div>
      <div ref={containerRef} className="h-full w-full" />
      <div
        className={`absolute inset-0 z-[6] ${interactionLayerActive ? 'pointer-events-auto' : 'pointer-events-none'}`}
        onPointerDown={beginDraw}
        onPointerMove={moveDraw}
        onPointerUp={finishDraw}
        onPointerCancel={finishDraw}
      />
      {chartStateNotice?.message && chartStateNotice.state !== 'ready' && chartStateNotice.state !== 'loading' ? (
        <div className="pointer-events-none absolute inset-0 z-[5] grid place-items-center px-6">
          <div className="relative max-w-xl rounded-2xl border border-white/8 bg-black/70 px-6 py-5 text-center text-sm text-slate-200 shadow-[0_26px_90px_rgba(0,0,0,0.7)] backdrop-blur">
            <div className="pointer-events-none absolute inset-2 rounded-2xl bg-[radial-gradient(circle_at_center,_rgba(255,255,255,0.06),_transparent_55%)]" />
            <p className="relative text-[11px] uppercase tracking-[0.32em] text-[color:var(--accent-text-soft)]">
              {chartStateNotice.state === 'empty'
                ? 'No Data'
                : chartStateNotice.state === 'error'
                  ? 'Issue'
                  : 'Status'}
            </p>
            <p className="relative mt-2 text-base font-semibold tracking-tight text-slate-50">
              {chartStateNotice.message}
            </p>
            {windowSummary ? (
              <p className="relative mt-2 text-xs text-slate-400">{windowSummary}</p>
            ) : null}
          </div>
        </div>
      ) : null}

      <SymbolPalette open={palOpen} onClose={() => setPalOpen(false)} onPick={applySymbol} />
      <HotkeyHint />
      <LoadingOverlay show={effectiveLoaderActive} message={effectiveLoaderMessage} className="right-3 top-3" />
    </div>
  );
}

export default ChartSurface;
