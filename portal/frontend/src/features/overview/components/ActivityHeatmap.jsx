function intensityLevel(total) {
  if (!total) return 0
  if (total <= 1) return 1
  if (total <= 3) return 2
  if (total <= 6) return 3
  return 4
}

export function ActivityHeatmap({ days = [], activityLabel = 'Persisted activity' }) {
  if (!days.length) {
    return <p className="qt2-empty">Activity projection unavailable.</p>
  }

  const firstDate = new Date(days[0].date + 'T00:00:00Z')
  const leadingPad = firstDate.getUTCDay()
  const cells = [...Array(leadingPad).fill(null), ...days]
  const weeks = []
  for (let index = 0; index < cells.length; index += 7) weeks.push(cells.slice(index, index + 7))

  return (
    <div>
      <div className="qt2-heatmap" role="img" aria-label={activityLabel + ' by UTC day'}>
        {weeks.map((week, weekIndex) => (
          <div key={weekIndex} className="qt2-heatmap-week">
            {week.map((day, dayIndex) =>
              day ? (
                <div
                  key={day.date}
                  className={'qt2-heatmap-cell qt2-heatmap-level-' + intensityLevel(day.total)}
                  title={day.date + ': ' + day.total + ' ' + activityLabel.toLowerCase()}
                />
              ) : (
                <div key={'pad-' + weekIndex + '-' + dayIndex} className="qt2-heatmap-cell qt2-heatmap-cell-pad" />
              ),
            )}
          </div>
        ))}
      </div>
      <div className="qt2-heatmap-legend">
        <span>Less</span>
        {[0, 1, 2, 3, 4].map((level) => <i key={level} className={'qt2-heatmap-cell qt2-heatmap-level-' + level} />)}
        <span>More</span>
      </div>
    </div>
  )
}
