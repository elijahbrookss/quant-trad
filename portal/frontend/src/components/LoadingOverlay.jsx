import { useEffect, useState } from 'react';

export default function LoadingOverlay({ show, message = 'Loading', className = '' }) {
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
      <div className="pointer-events-auto flex items-center gap-2 rounded-full border border-white/10 bg-black/42 px-2.5 py-1 text-[10px] font-medium tracking-[0.08em] text-slate-200/88 shadow-[0_8px_24px_rgba(0,0,0,0.28)] backdrop-blur-[8px]">
        <svg className="h-3 w-3 text-[color:var(--accent-text-bright,#a5b4fc)]" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="3.2" fill="currentColor" className="animate-pulse" />
          <circle cx="12" cy="12" r="8.5" stroke="currentColor" strokeWidth="1.4" fill="none" opacity="0.32" strokeDasharray="10 6" className="origin-center animate-spin" style={{ animationDuration: '2.6s' }} />
        </svg>
        <span className="leading-none">{message}</span>
      </div>
    </div>
  );
}
