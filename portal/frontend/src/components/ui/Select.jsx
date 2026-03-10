import React from 'react'

/**
 * Select dropdown component with consistent styling.
 *
 * @param {Object} props
 * @param {string} props.label - Select label text
 * @param {string} props.error - Error message to display
 * @param {string} props.hint - Hint text to display below select
 * @param {boolean} props.required - Whether field is required
 * @param {Array} props.options - Array of {value, label} objects
 * @param {string} props.placeholder - Placeholder text
 * @param {string} props.className - Additional CSS classes
 */
export const Select = ({
  label,
  error,
  hint,
  required = false,
  options = [],
  placeholder,
  className = '',
  id,
  ...props
}) => {
  const selectId = id || `select-${label?.toLowerCase().replace(/\s+/g, '-')}`

  const baseSelectClasses = 'w-full rounded-lg border bg-black/40 px-3 py-2 text-sm text-white transition focus:outline-none'
  const borderClasses = error
    ? 'border-rose-500/50 focus:border-rose-500'
    : 'border-white/10 focus:border-[color:var(--accent-alpha-40)]'

  const selectClasses = `${baseSelectClasses} ${borderClasses} ${className}`

  return (
    <div className="space-y-2">
      {label && (
        <label htmlFor={selectId} className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
          {label}
          {required && <span className="ml-1 text-rose-400">*</span>}
        </label>
      )}
      <select
        id={selectId}
        className={selectClasses}
        aria-invalid={error ? 'true' : 'false'}
        aria-describedby={error ? `${selectId}-error` : hint ? `${selectId}-hint` : undefined}
        {...props}
      >
        {placeholder && (
          <option value="" disabled>
            {placeholder}
          </option>
        )}
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      {hint && !error && (
        <p id={`${selectId}-hint`} className="text-[11px] text-slate-500">
          {hint}
        </p>
      )}
      {error && (
        <p id={`${selectId}-error`} className="text-[11px] text-rose-400" role="alert">
          {error}
        </p>
      )}
    </div>
  )
}
