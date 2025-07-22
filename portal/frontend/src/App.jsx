import { useState } from 'react'
import { ChartComponent } from './components/ChartComponent'
import { TimeframeSelect, SymbolInput} from './components/TimeframeSelectComponent'

function App() {
  const [symbol, setSymbol] = useState('AAPL')
  const [timeframe, setTimeframe] = useState('1h')

  return (
    <>
      <div className="bg-neutral-900 text-white min-h-screen p-5">
        <h1 className="text-3xl font-bold text-center mt-10">
          QuantTrad
        </h1>
        <div className="max-w-4xl mx-auto mt-10 p-5 bg-neutral-800 rounded-lg shadow-lg">
          <div className="flex justify-left space-x-4">
            <TimeframeSelect
              selected={timeframe}
              onChange={setTimeframe}
            />
            <SymbolInput
              value={symbol}
              onChange={setSymbol}
            />
          </div>
          <div className="flex justify-center mt-3 rounded-lg overflow-hidden h-120 bg-gray-800">
            <ChartComponent />
          </div>
        </div>
      </div>
    </>
  )
}

export default App
