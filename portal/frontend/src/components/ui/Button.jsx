import React from 'react'

/**
 * Button component with consistent styling and variants.
 *
 * @param {Object} props
 * @param {'primary'|'ghost'|'danger'} props.variant - Button style variant
 * @param {'sm'|'md'|'lg'} props.size - Button size
 * @param {boolean} props.disabled - Whether button is disabled
 * @param {boolean} props.loading - Whether button is in loading state
 * @param {string} props.className - Additional CSS classes
 * @param {React.ReactNode} props.children - Button content
 * @param {React.ReactNode} props.icon - Optional icon element
 */
export const Button = ({
  variant = 'primary',
  size = 'md',
  disabled = false,
  loading = false,
  className = '',
  children,
  icon,
  type = 'button',
  ...props
}) => {
  const baseClasses = 'inline-flex items-center justify-center gap-2 rounded-lg font-medium transition focus:outline-none'

  const variantClasses = {
    primary: 'bg-[color:var(--accent-base)] text-white hover:bg-[color:var(--accent-alpha-80)] focus:ring-2 focus:ring-[color:var(--accent-ring)] disabled:opacity-50 disabled:cursor-not-allowed',
    ghost: 'border border-white/10 bg-transparent text-slate-300 hover:bg-white/5 disabled:opacity-50 disabled:cursor-not-allowed',
    danger: 'bg-rose-600 text-white hover:bg-rose-700 focus:ring-2 focus:ring-rose-500 disabled:opacity-50 disabled:cursor-not-allowed',
  }

  const sizeClasses = {
    sm: 'px-3 py-1.5 text-xs',
    md: 'px-4 py-2 text-sm',
    lg: 'px-6 py-3 text-base',
  }

  const classes = `${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`

  return (
    <button
      type={type}
      className={classes}
      disabled={disabled || loading}
      {...props}
    >
      {loading && (
        <svg className="h-4 w-4 animate-spin" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path
            className="opacity-75"
            fill="currentColor"
            d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
          ></path>
        </svg>
      )}
      {icon && !loading && icon}
      {children}
    </button>
  )
}
