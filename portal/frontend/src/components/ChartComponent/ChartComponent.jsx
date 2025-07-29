import { useEffect, useRef, useState } from 'react'
import { createChart, CandlestickSeries } from 'lightweight-charts'
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent'
import { DateRangePickerComponent } from './DateTimePickerComponent'
import { options, seriesOptions } from './ChartOptions'
import { fetchCandleData } from '../../adapters/candle.adapter'

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
  const chartRef = useRef();
  const seriesRef = useRef();

  useEffect(() => {
    const chart = createChart(chartContainerRef.current, { ...options });
    chartRef.current = chart;

    const series = chart.addSeries(CandlestickSeries, { ...seriesOptions });
    seriesRef.current = series;

    loadChartData();

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


  const loadChartData = async () => {
    const response = await fetchCandleData({
      symbol,
      timeframe,
      start: dateRange[0]?.toISOString(),
      end: dateRange[1]?.toISOString(),
    });

    const formatted = response
      .filter(c => c && typeof c.time === "number")
      .map(c => ({
        time: c.time,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      }));

    console.log("Fetched candles:", formatted);

    if (seriesRef.current && formatted.length > 0) {
      seriesRef.current.setData(formatted);
      chartRef.current.timeScale().fitContent();
    }
  };

  const handlePrintState = () => {
    console.log('Chart State:', {
      symbol,
      timeframe,
      dateRange,
      chart: chartRef.current,
      series: seriesRef.current,
    });

    loadChartData();
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
