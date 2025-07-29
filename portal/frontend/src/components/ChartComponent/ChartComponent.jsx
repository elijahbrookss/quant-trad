import { useEffect, useRef, useState } from 'react'
import { createChart, CrosshairMode, LineStyle, CandlestickSeries } from 'lightweight-charts'
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent'
import { DateRangePickerComponent } from './DateTimePickerComponent'

function generateMockOHLC(count = 1000, basePrice = 100, volatility = 2, intervalSec = 3600) {
  const now = Math.floor(Date.now() / 1000)
  const data = []
  let lastClose = basePrice

  for (let i = 0; i < count; i++) {
    const time = now - (count - i) * intervalSec
    const open = lastClose
    const change = (Math.random() * 2 - 1) * volatility
    const close = open + change
    const high = Math.max(open, close) + Math.random() * (volatility * 0.5)
    const low = Math.min(open, close) - Math.random() * (volatility * 0.5)
    data.push({ time, open, high, low, close })
    lastClose = close
  }

  return data
}

export const ChartComponent = () => {
  const [symbol, setSymbol] = useState('AAPL')
  const [timeframe, setTimeframe] = useState('1h')

  // Default date range: last 45 days
  // Adjusted to ensure it doesn't exceed current time by 5 minutes
  const defaultEnd = new Date();
  defaultEnd.setMinutes(defaultEnd.getMinutes() - 5);
  const defaultStart = new Date();
  defaultStart.setDate(defaultStart.getDate() - 45);

  const [dateRange, setDateRange] = useState([defaultStart, defaultEnd]);

  const chartContainerRef = useRef()

  useEffect(() => {
    const chart = createChart(chartContainerRef.current, {
      layout: {
        textColor: '#DDD',
        background: { color: '#1E1E1E' },
      },
      grid: {
        vertLines: { color: "#444" },
        horzLines: { color: "#444" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          width: 8,
          color: '#C3BCDB45',
          style: LineStyle.Solid,
          labelBackgroundColor: '#000',
        },
        horzLine: {
          color: '#C3BCDB70',
          labelBackgroundColor: '#000',
        },
      }
    });

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      wickUpColor: 'rgb(54, 116, 217)',
      upColor: 'rgb(54, 116, 217)',
      wickDownColor: 'rgb(225, 50, 85)',
      downColor: 'rgb(225, 50, 85)',
      borderVisible: false,
    });

    candlestickSeries.setData(generateMockOHLC());
    chart.timeScale().fitContent();

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    }

    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };

  }, []);

  const handlePrintState = () => {
    console.log(
      'Selected Controls:',
      JSON.stringify(
        {
          symbol,
          timeframe,
          dateRange: {
            start: dateRange[0]?.toISOString(),
            end: dateRange[1]?.toISOString(),
          },
        },
        null,
        2
      )
    );
  };

  return (
    <>
      {/* Controls */}
      <div className="flex items-end space-x-4">
        <TimeframeSelect selected={timeframe} onChange={setTimeframe} />
        <SymbolInput value={symbol} onChange={setSymbol} />
        <DateRangePickerComponent dateRange={dateRange} setDateRange={setDateRange} />
        <button 
          className="mt-5.5 self-center border border-neutral-600 rounded-md p-2 hover:bg-neutral-700 transition-colors cursor-pointer"
          onClick={handlePrintState}
        >
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
        </button>
      </div>

      {/* Chart */}
      <div className="flex space-x-4 mt-5">
        <div className="flex-1 rounded-lg overflow-hidden bg-gray-800 h-[400px]">
          <div ref={chartContainerRef} className="h-full w-full bg-transparent" />
        </div>
      </div>
    </>
  )
}
