export default function LoadingOverlay({ show, message = 'Loading…' }) {
  if (!show) return null;
  return (
    <div className="absolute inset-0 z-10 grid place-items-center bg-black/35 backdrop-blur-sm">
      <div className="flex items-center gap-3 text-neutral-100">
        <svg className="h-5 w-5 animate-spin" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" fill="none" opacity="0.25"/>
          <path d="M22 12a10 10 0 0 1-10 10" stroke="currentColor" strokeWidth="3" fill="none" />
        </svg>
        <span className="text-sm">{message}</span>
      </div>
    </div>
  );
}
