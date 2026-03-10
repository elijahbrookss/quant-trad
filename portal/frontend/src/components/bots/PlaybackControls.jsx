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
      <div className="mb-3 flex items-center justify-between gap-4">
        {/* Left: Sim Time */}
        <div className="flex items-center">
          {simTimeLabel ? (
            <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200">
              {simTimeLabel}
            </div>
          ) : (
            <span className="text-xs text-slate-500">&nbsp;</span>
          )}
        </div>

        {/* Center: Pause/Resume */}
        <div className="flex items-center gap-2">
          {canPause ? (
            <button
              type="button"
              onClick={onPause}
              disabled={action === 'pause' || playbackDisabled}
              className="inline-flex items-center gap-2 rounded-full border border-amber-500/30 bg-amber-500/5 px-5 py-2.5 text-sm font-medium text-amber-200 transition-all hover:bg-amber-500/10 disabled:opacity-40"
            >
              <Pause className="size-4" /> Pause walk-forward
            </button>
          ) : null}
          {canResume ? (
            <button
              type="button"
              onClick={onResume}
              disabled={action === 'resume' || playbackDisabled}
              className="inline-flex items-center gap-2 rounded-full border border-emerald-500/30 bg-emerald-500/5 px-5 py-2.5 text-sm font-medium text-emerald-200 transition-all hover:bg-emerald-500/10 disabled:opacity-40"
            >
              <RotateCw className="size-4" /> Resume
            </button>
          ) : null}
        </div>

        {/* Right: Zoom controls and Speed */}
        <div className="flex items-center gap-3">
          <div className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-2 py-1.5 shadow">
            <button
              type="button"
              onClick={onZoomOut}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-white/5 text-slate-200 transition-colors hover:bg-white/10"
              aria-label="Zoom out"
            >
              <ZoomOut className="size-4" />
            </button>
            <button
              type="button"
              onClick={onCenter}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-white/5 text-slate-200 transition-colors hover:bg-white/10"
              aria-label="Center view"
            >
              <Crosshair className="size-4" />
            </button>
            <button
              type="button"
              onClick={onZoomIn}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-white/5 text-slate-200 transition-colors hover:bg-white/10"
              aria-label="Zoom in"
            >
              <ZoomIn className="size-4" />
            </button>
          </div>
          <div
            className={`inline-flex items-center gap-3 rounded-full border border-white/10 bg-white/5 px-4 py-2 text-xs text-white shadow transition ${
              playbackDisabled ? 'pointer-events-none opacity-60' : ''
            }`}
          >
            <span className="text-[10px] font-medium uppercase tracking-[0.25em] text-slate-400">Speed</span>
            <input
              type="range"
              min="0"
              max="25"
              step="0.25"
              value={playbackDraft}
              onChange={onPlaybackChange}
              disabled={playbackDisabled}
              className="w-32 appearance-none bg-transparent [&::-webkit-slider-runnable-track]:h-1 [&::-webkit-slider-runnable-track]:rounded-full [&::-webkit-slider-runnable-track]:bg-white/30 [&::-webkit-slider-thumb]:mt-[-4px] [&::-webkit-slider-thumb]:h-3 [&::-webkit-slider-thumb]:w-3 [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:bg-white [&::-moz-range-track]:h-1 [&::-moz-range-track]:rounded-full [&::-moz-range-track]:bg-white/30 [&::-moz-range-thumb]:h-3 [&::-moz-range-thumb]:w-3 [&::-moz-range-thumb]:rounded-full [&::-moz-range-thumb]:border-0 [&::-moz-range-thumb]:bg-white"
            />
            <span className="min-w-[3.5rem] text-sm font-semibold text-white">
              {playbackLabel}
              {speedSaving ? ' •' : ''}
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}
