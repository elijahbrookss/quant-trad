import { Crosshair, Pause, RotateCw, ZoomIn, ZoomOut } from 'lucide-react'

export function PlaybackControls({
  canPause,
  canResume,
  onPause,
  onResume,
  action,
  playbackDisabled,
  simTimeLabel,
  onZoomIn,
  onZoomOut,
  onCenter,
  playbackDraft,
  playbackLabel,
  onPlaybackChange,
  speedSaving,
}) {
  return (
    <div className="relative">
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          {simTimeLabel ? (
            <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-200">
              {simTimeLabel}
            </div>
          ) : (
            <span className="text-xs text-slate-500">&nbsp;</span>
          )}
        </div>
        <div className="flex flex-1 justify-center">
          <div className="flex flex-wrap items-center gap-2">
            {canPause ? (
              <button
                type="button"
                onClick={onPause}
                disabled={action === 'pause' || playbackDisabled}
                className="inline-flex items-center gap-2 rounded-full border border-amber-500/30 px-4 py-2 text-sm text-amber-200 hover:bg-amber-500/10 disabled:opacity-40"
              >
                <Pause className="size-4" /> Pause walk-forward
              </button>
            ) : null}
            {canResume ? (
              <button
                type="button"
                onClick={onResume}
                disabled={action === 'resume' || playbackDisabled}
                className="inline-flex items-center gap-2 rounded-full border border-emerald-500/30 px-4 py-2 text-sm text-emerald-200 hover:bg-emerald-500/10 disabled:opacity-40"
              >
                <RotateCw className="size-4" /> Resume
              </button>
            ) : null}
          </div>
        </div>
        <div className="ml-auto flex items-center gap-3">
          <div className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-2 py-1 shadow">
            <button
              type="button"
              onClick={onZoomOut}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-white/5 text-slate-200 hover:bg-white/10"
              aria-label="Zoom out"
            >
              <ZoomOut className="size-4" />
            </button>
            <button
              type="button"
              onClick={onCenter}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-white/5 text-slate-200 hover:bg-white/10"
              aria-label="Center view"
            >
              <Crosshair className="size-4" />
            </button>
            <button
              type="button"
              onClick={onZoomIn}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-white/5 text-slate-200 hover:bg-white/10"
              aria-label="Zoom in"
            >
              <ZoomIn className="size-4" />
            </button>
          </div>
          <div
            className={`inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white shadow transition ${
              playbackDisabled ? 'pointer-events-none opacity-60' : ''
            }`}
          >
            <span className="text-[10px] uppercase tracking-[0.25em] text-slate-200">Speed</span>
            <input
              type="range"
              min="0"
              max="25"
              step="0.25"
              value={playbackDraft}
              onChange={onPlaybackChange}
              disabled={playbackDisabled}
              className="w-28 appearance-none bg-transparent [&::-webkit-slider-runnable-track]:h-1 [&::-webkit-slider-runnable-track]:rounded-full [&::-webkit-slider-runnable-track]:bg-white/30 [&::-webkit-slider-thumb]:mt-[-4px] [&::-webkit-slider-thumb]:h-3 [&::-webkit-slider-thumb]:w-3 [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:bg-white [&::-moz-range-track]:h-1 [&::-moz-range-track]:rounded-full [&::-moz-range-track]:bg-white/30 [&::-moz-range-thumb]:h-3 [&::-moz-range-thumb]:w-3 [&::-moz-range-thumb]:rounded-full [&::-moz-range-thumb]:border-0 [&::-moz-range-thumb]:bg-white"
            />
            <span className="text-xs font-semibold text-white">
              {playbackLabel}
              {speedSaving ? ' •' : ''}
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}
