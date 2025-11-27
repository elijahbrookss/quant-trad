export default function LoadingOverlay({ show, message = 'Loading…' }) {
  if (!show) return null;
  return (
    <div className="absolute inset-0 z-10 grid place-items-center rounded-[inherit] bg-[#050912]/90 text-slate-100 backdrop-blur-[6px]">
      <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-black/40 px-5 py-3 text-sm font-medium shadow-[0_20px_60px_-35px_rgba(0,0,0,0.8)]">
        <svg className="h-5 w-5 animate-spin text-[color:var(--accent-text-bright,#a5b4fc)]" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" fill="none" opacity="0.35" />
          <path d="M22 12a10 10 0 0 1-10 10" stroke="currentColor" strokeWidth="3" fill="none" />
        </svg>
        <span>{message}</span>
      </div>
    </div>
  );
}
