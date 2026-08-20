import { resolveGreeting } from '../buildOverviewViewModel.js'

export function GreetingBanner({ nowEpochMs = Date.now() }) {
  const greeting = resolveGreeting(nowEpochMs)
  const dateLabel = new Date(nowEpochMs).toLocaleDateString(undefined, {
    weekday: 'long',
    month: 'long',
    day: 'numeric',
  })

  return (
    <div className="qt2-greeting qt2-greeting-compact">
      <span className="qt2-kicker">{dateLabel}</span>
      <p className="qt2-greeting-title">{greeting}, Operator</p>
    </div>
  )
}
