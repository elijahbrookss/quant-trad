import { useMemo } from 'react';
import { Maximize2, Minimize2 } from 'lucide-react';
import SymbolPalette from '../SymbolPalette.jsx';
import HotkeyHint from '../HotkeyHint.jsx';
import LoadingOverlay from '../LoadingOverlay.jsx';

/**
 * ChartSurface - Chart rendering surface with overlays and controls
 *
 * Isolated chart display component with symbol info, fullscreen button,
 * state notices, symbol palette, and loading overlay.
 * Part of ChartComponent refactoring to reduce complexity.
 */
export function ChartSurface({
  containerRef,
  isFullscreen,
  toggleFullscreen,
  symbolDisplay,
  intervalDisplay,
  instrumentMeta,
  chartStateNotice,
  windowSummary,
  palOpen,
  setPalOpen,
  applySymbol,
  loaderActive,
  loaderMessage,
}) {
  const chartShellClasses = useMemo(() => {
    const base =
      'relative overflow-hidden border border-white/10 bg-[#0f1419] shadow-[0_0_40px_rgba(0,0,0,0.6)]';
    const size = isFullscreen
      ? 'h-full w-full rounded-none'
      : 'h-[680px] rounded-2xl';
    return `${base} ${size}`;
  }, [isFullscreen]);

  return (
    <div className={chartShellClasses}>
      <div className="pointer-events-none absolute left-6 top-6 z-10 flex max-w-[70%] flex-col gap-1.5 text-slate-200 drop-shadow-[0_10px_30px_rgba(0,0,0,0.65)]">
        <div className="flex flex-wrap items-baseline gap-2">
          <span className="text-2xl font-semibold tracking-tight text-white">{symbolDisplay}</span>
          <span className="rounded-full border border-white/20 bg-black/60 px-3 py-0.5 text-[11px] font-semibold uppercase tracking-[0.35em] text-slate-100">
            {intervalDisplay}
          </span>
        </div>
        {instrumentMeta ? (
          <div className="text-[11px] font-semibold uppercase tracking-[0.32em] text-slate-300/90">
            {instrumentMeta}
          </div>
        ) : null}
      </div>
      <button
        type="button"
        aria-pressed={isFullscreen}
        onClick={toggleFullscreen}
        className="pointer-events-auto absolute right-6 top-6 z-10 inline-flex items-center gap-2 rounded-full border border-white/20 bg-black/60 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.25em] text-slate-100 shadow-lg shadow-black/30 transition hover:border-white/40 hover:bg-black/80 focus:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent-ring-strong)]"
      >
        {isFullscreen ? (
          <>
            <Minimize2 className="h-3.5 w-3.5" aria-hidden="true" />
            Exit Fullscreen
          </>
        ) : (
          <>
            <Maximize2 className="h-3.5 w-3.5" aria-hidden="true" />
            Fullscreen
          </>
        )}
      </button>
      <div ref={containerRef} className="h-full w-full" />
      {chartStateNotice?.message && chartStateNotice.state !== 'ready' ? (
        chartStateNotice.state === 'loading' ? (
          <div className="pointer-events-none absolute right-6 top-[52px] z-[5]">
            <div className="flex items-center gap-2.5 rounded-full border border-white/15 bg-black/70 px-4 py-2 text-xs font-medium text-slate-200 shadow-lg shadow-black/40 backdrop-blur-sm">
              <svg className="h-3.5 w-3.5 animate-spin text-[color:var(--accent-text-bright,#a5b4fc)]" viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" fill="none" opacity="0.3" />
                <path d="M22 12a10 10 0 0 1-10 10" stroke="currentColor" strokeWidth="3" fill="none" />
              </svg>
              <span>{chartStateNotice.message}</span>
            </div>
          </div>
        ) : (
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
        )
      ) : null}

      <SymbolPalette open={palOpen} onClose={() => setPalOpen(false)} onPick={applySymbol} />
      <HotkeyHint />
      <LoadingOverlay show={loaderActive} message={loaderMessage} />
    </div>
  );
}

export default ChartSurface;
