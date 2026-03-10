import { useEffect, useState } from 'react';

export default function LoadingOverlay({ show, message = 'Loading…', className = '' }) {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (show) {
      const frame = requestAnimationFrame(() => setVisible(true));
      return () => cancelAnimationFrame(frame);
    }
    setVisible(false);
    return undefined;
  }, [show]);

  if (!show) return null;

  return (
    <div
      className={`pointer-events-none absolute z-10 transition-opacity duration-300 ${className || 'right-6 top-[52px]'} ${visible ? 'opacity-100' : 'opacity-0'}`}
    >
      <div className="pointer-events-auto flex items-center gap-2.5 rounded-full border border-white/15 bg-black/70 px-4 py-2 text-xs font-medium text-slate-200 shadow-lg shadow-black/40 backdrop-blur-sm">
        <svg className="h-3.5 w-3.5 animate-spin text-[color:var(--accent-text-bright,#a5b4fc)]" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" fill="none" opacity="0.3" />
          <path d="M22 12a10 10 0 0 1-10 10" stroke="currentColor" strokeWidth="3" fill="none" />
        </svg>
        <span>{message}</span>
      </div>
    </div>
  );
}
