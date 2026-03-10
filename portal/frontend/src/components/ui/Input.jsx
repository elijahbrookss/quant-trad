import React from 'react'

/**
 * Input field component with consistent styling and validation display.
 *
 * @param {Object} props
 * @param {string} props.label - Input label text
 * @param {string} props.error - Error message to display
 * @param {string} props.hint - Hint text to display below input
 * @param {boolean} props.required - Whether field is required
 * @param {string} props.className - Additional CSS classes
 */
export const Input = ({
  label,
  error,
  hint,
  required = false,
  className = '',
  id,
  ...props
}) => {
  const inputId = id || `input-${label?.toLowerCase().replace(/\s+/g, '-')}`

  const baseInputClasses = 'w-full rounded-lg border bg-black/40 px-3 py-2 text-sm text-white placeholder-slate-500 transition focus:outline-none'
  const borderClasses = error
    ? 'border-rose-500/50 focus:border-rose-500'
    : 'border-white/10 focus:border-[color:var(--accent-alpha-40)]'

  const inputClasses = `${baseInputClasses} ${borderClasses} ${className}`

  return (
    <div className="space-y-2">
      {label && (
        <label htmlFor={inputId} className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
          {label}
          {required && <span className="ml-1 text-rose-400">*</span>}
        </label>
      )}
      <input
        id={inputId}
        className={inputClasses}
        aria-invalid={error ? 'true' : 'false'}
        aria-describedby={error ? `${inputId}-error` : hint ? `${inputId}-hint` : undefined}
        {...props}
      />
      {hint && !error && (
        <p id={`${inputId}-hint`} className="text-[11px] text-slate-500">
          {hint}
        </p>
      )}
      {error && (
        <p id={`${inputId}-error`} className="text-[11px] text-rose-400" role="alert">
          {error}
        </p>
      )}
    </div>
  )
}
