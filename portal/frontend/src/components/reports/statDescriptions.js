/**
 * Stat and chart descriptions registry for report metrics.
 *
 * Each entry provides a brief, beginner-friendly explanation of what the stat
 * measures and why it matters. Keep descriptions concise (1-2 sentences).
 *
 * Format:
 *   key: "Plain English explanation of what this means and why it's useful."
 */

export const statDescriptions = {
  // Primary metrics
  total_return: "The percentage gain or loss on your initial investment. A 10% return means you made 10% profit on your starting balance.",
  net_pnl: "Your actual profit or loss in dollars after all fees and costs. This is the real money you made or lost.",
  max_drawdown: "The largest peak-to-trough drop in your account value. Lower is better - it shows how much pain you'd have experienced.",
  sharpe: "Measures return relative to risk taken. Above 1.0 is good, above 2.0 is excellent. Higher means better risk-adjusted performance.",

  // Risk metrics
  sortino: "Like Sharpe, but only penalizes downside volatility. Higher is better since it ignores 'good' volatility from gains.",
  calmar: "Annual return divided by max drawdown. Higher means better returns for the risk taken. Above 1.0 is generally good.",
  volatility: "How much your returns swing up and down. Lower volatility means smoother, more predictable returns.",
  exposure: "Percentage of time your money was actively invested in trades. Higher exposure means more time in the market.",

  // Trade performance
  win_rate: "Percentage of trades that were profitable. Above 50% means you win more often than you lose.",
  profit_factor: "Gross profits divided by gross losses. Above 1.0 means profitable, above 2.0 is very good.",
  expectancy: "Average amount you can expect to make per trade. Positive expectancy means the strategy is profitable over time.",
  payoff_ratio: "Average win size divided by average loss size. Higher means your winners are bigger than your losers.",

  // Summary stats
  total_trades: "Total number of completed trades in this backtest. More trades generally means more statistically reliable results.",
  avg_win: "The average profit on winning trades. Compare this to avg loss to understand your risk/reward profile.",
  avg_loss: "The average loss on losing trades. Smaller losses relative to wins is better for long-term profitability.",
  fees: "Total trading fees and commissions paid. High fees can significantly eat into profits over time.",

  // Additional metrics
  cagr: "Compound Annual Growth Rate - your annualized return accounting for compounding. Makes it easy to compare strategies of different durations.",
  best_day: "Your single best trading day. Useful for understanding the range of outcomes you might experience.",
  worst_day: "Your single worst trading day. Important for understanding downside risk and emotional preparation.",
  drawdown_duration: "How long it took to recover from the worst drawdown. Longer durations can be psychologically difficult.",
}

/**
 * Chart descriptions for visual components.
 *
 * Explains what each chart shows and how to interpret it.
 */
export const chartDescriptions = {
  // Main charts
  equity_curve: "Shows how your account balance changed over time. An upward slope means growth. Look for smooth, steady climbs rather than jagged spikes.",
  drawdown_curve: "Shows how far below your peak balance you were at any point. Deeper valleys mean bigger losses from highs. Ideally stays shallow.",
  rolling_sharpe: "Shows your risk-adjusted performance over rolling 20-day windows. Helps identify periods when the strategy performed well vs poorly.",
  returns_histogram: "Shows the distribution of your daily returns. A bell curve centered right of zero is ideal. Wide spreads indicate high volatility.",

  // Additional charts
  monthly_returns: "Color-coded calendar showing profit/loss each month. Green means gains, red means losses. Helps spot seasonal patterns.",
  hold_time_histogram: "Shows how long you typically hold trades. Useful for understanding if you're a quick scalper or longer-term position trader.",
  win_loss_streaks: "Shows consecutive winning and losing streaks. Long losing streaks can be emotionally tough even in profitable strategies.",

  // Breakdown sections
  direction_breakdown: "Compares performance of long (bullish) vs short (bearish) trades. Shows if your strategy works better in one direction.",
  instrument_breakdown: "Shows profit/loss by each traded symbol. Helps identify which instruments work best with your strategy.",
}

/**
 * Get description for a stat key.
 * Returns null if no description exists.
 */
export const getStatDescription = (key) => {
  return statDescriptions[key] || null
}

/**
 * Get description for a chart key.
 * Returns null if no description exists.
 */
export const getChartDescription = (key) => {
  return chartDescriptions[key] || null
}
