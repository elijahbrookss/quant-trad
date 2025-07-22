import { useState } from 'react'
import { ChartComponent } from './components/ChartComponent'
import { TimeframeSelect, SymbolInput } from './components/TimeframeSelectComponent'
import { DateRangePicker } from './components/DateTimePickerComponent'
import Datepicker from 'react-tailwindcss-datepicker'

function App() {
  const [symbol, setSymbol] = useState('AAPL')
  const [timeframe, setTimeframe] = useState('1h')  

  const defaultEnd = new Date().toISOString().split('T')[0];
  const defaultStart = new Date(
    Date.now() - 7 * 24 * 60 * 60 * 1000)
    .toISOString()
    .split('T')[0];

  const [dates, setDates] = useState({ startDate: defaultStart, endDate: defaultEnd });

  return (
<>
  <div className="bg-neutral-900 text-white min-h-screen p-5">

    {/* Title */}
    <h1 className="text-3xl font-bold text-center mt-10">
      QuantTrad
    </h1>

    {/* Chart + Indicators Section */}
    <div className="max-w-7xl mx-auto mt-10 p-5 bg-neutral-800 rounded-lg shadow-lg">

      {/* controls row */}
      <div className="flex space-x-4">
        <TimeframeSelect selected={timeframe} onChange={setTimeframe} />
        <SymbolInput     value={symbol}     onChange={setSymbol}     />

        <div className="w-1/4">
            <Datepicker
              primaryColor={"indigo"}
              value={dates} 
              onChange={setDates}
              showShortcuts={false}
          />
        </div>
      </div>

      {/* chart + sidebar row */}
      <div className="flex space-x-4 mt-5">

        {/* 1) Chart container — flex-1 so it grows, fixed height */}
        <div className="flex-1 rounded-lg overflow-hidden bg-gray-800 h-[400px]">
          <ChartComponent symbol={symbol} timeframe={timeframe} />
        </div>

        {/* 2) Indicators sidebar — fixed width, same height */}
        <div className="w-80 bg-neutral-900  shadow-2xl rounded-lg p-5 h-[400px] overflow-auto">
          <h2 className="text-xl font-semibold mb-4">Indicators</h2>
          {/* …your indicators here… */}
        </div>

      </div>
    </div>
  </div>
</>
  )
}

export default App
