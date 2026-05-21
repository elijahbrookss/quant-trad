import React from "react";
import { Eye, EyeOff } from "lucide-react";

/**
 * VisibilityToggle - Eye icon toggle for showing/hiding chart overlays
 * Replaces the "Enabled" switch with clearer visibility semantics
 */
export default function VisibilityToggle({
  visible = true,
  onChange,
  disabled = false,
  size = "md",
  className = "",
}) {
  const sizeClasses = {
    sm: "h-7 w-7",
    md: "h-9 w-9",
    lg: "h-10 w-10",
    xl: "h-10 w-10",
  };

  const iconSizeClasses = {
    sm: "size-3.5",
    md: "size-4",
    lg: "size-5",
    xl: "size-5",
  };

  const handleClick = () => {
    if (!disabled && typeof onChange === "function") {
      onChange(!visible);
    }
  };

  return (
    <button
      type="button"
      onClick={handleClick}
      disabled={disabled}
      className={`
        ${sizeClasses[size] || sizeClasses.md}
        inline-flex items-center justify-center rounded-[6px] border transition
        ${
          visible
            ? "border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] text-[color:var(--accent-text-soft)] hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-18)]"
            : "border-white/10 bg-white/5 text-slate-500 hover:border-white/20 hover:text-slate-400"
        }
        ${disabled ? "cursor-not-allowed opacity-50" : "cursor-pointer"}
        ${className}
      `}
      title={visible ? "Hide from chart" : "Show on chart"}
      aria-label={visible ? "Hide overlay from chart" : "Show overlay on chart"}
      aria-pressed={visible}
    >
      {visible ? (
        <Eye className={iconSizeClasses[size] || iconSizeClasses.md} />
      ) : (
        <EyeOff className={iconSizeClasses[size] || iconSizeClasses.md} />
      )}
    </button>
  );
}
